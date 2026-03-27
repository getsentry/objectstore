//! Change lifecycle tracking and durable write-ahead log.
//!
//! When a storage mutation spans both the high-volume (HV) and long-term (LT)
//! backends, several non-atomic steps must happen in sequence: uploading to LT,
//! committing a tombstone in HV via compare-and-swap, and cleaning up
//! unreferenced blobs. A crash at any point can leave orphaned LT blobs.
//!
//! This module provides two layers of protection:
//!
//! 1. **In-process tracking** — [`ChangeGuard`] is an RAII guard that tracks
//!    the current [`ChangePhase`] of an operation. When dropped, it spawns a
//!    background task to clean up whichever blob is unreferenced based on the
//!    phase reached before the drop. This handles normal errors and early
//!    returns within a running process.
//!
//! 2. **Durable write-ahead log** — The [`ChangeLog`] trait records a
//!    [`Change`] to durable storage *before* any LT side effects begin. If the
//!    process crashes, a recovery scan reads outstanding entries and cleans up
//!    orphaned blobs. Recovery is garbage collection — it never replays CAS
//!    mutations or finishes incomplete operations.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::backend::common::{Backend, HighVolumeBackend, TieredMetadata};
use crate::error::Result;
use crate::id::ObjectId;

/// Initial delay for exponential backoff retries in background cleanup tasks.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// Maximum delay for exponential backoff retries in background cleanup tasks.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Maximum number of stale entries returned per [`ChangeLog::scan`] call.
const SCAN_COUNT: u16 = 100;
/// How often the heartbeat task re-records a change to extend its lifetime.
const REFRESH_INTERVAL: Duration = Duration::from_secs(10);
/// Time after which a change is considered expired if not completed or refreshed.
pub const EXPIRY: Duration = REFRESH_INTERVAL.saturating_mul(3);

/// Unique identifier for a change log entry.
///
/// Generated per-operation as a UUIDv7. In durable storage, scoped to the
/// owning service instance (e.g., `~oplog/{instance_id}/{change_id}`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangeId(uuid::Uuid);

impl ChangeId {
    /// Generates a new unique change ID.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl fmt::Display for ChangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Describes the LT blobs involved in a multi-step storage change.
///
/// Every mutating flow maps to: "I may have written a `new` LT blob and I
/// may be replacing an `old` LT blob." Recovery uses these fields to determine
/// which blobs are orphaned by reading the current HV state.
#[derive(Debug, Clone)]
pub struct Change {
    /// The logical object being mutated.
    ///
    /// Used by cleanup to query HV and determine which blob is currently referenced.
    pub id: ObjectId,
    /// The new LT blob written by this operation.
    ///
    /// Needs cleanup on failure (the CAS did not commit).
    pub new: Option<ObjectId>,
    /// The old LT blob being replaced.
    ///
    /// Needs cleanup on success (the CAS committed and the old blob is unreferenced).
    pub old: Option<ObjectId>,
}

/// Manager for multi-step storage changes, including backends and durable log.
///
/// Encapsulates the state and logic for recording changes, advancing their phases,
/// and performing cleanup on drop. The `TieredStorage` backend holds an instance
/// of this manager to use it for its multi-step operations.
#[derive(Debug)]
pub struct ChangeManager {
    /// The backend for small objects (≤ 1 MiB).
    pub(crate) high_volume: Box<dyn HighVolumeBackend>,
    /// The backend for large objects (> 1 MiB).
    pub(crate) long_term: Box<dyn Backend>,
    /// Durable write-ahead log for multi-step changes.
    pub(crate) changelog: Box<dyn ChangeLog>,
    /// Tracks outstanding background cleanup operations for graceful shutdown.
    pub(crate) tracker: TaskTracker,
}

impl ChangeManager {
    /// Creates a new `ChangeManager` with the given backends and changelog.
    pub fn new(
        high_volume: Box<dyn HighVolumeBackend>,
        long_term: Box<dyn Backend>,
        changelog: Box<dyn ChangeLog>,
    ) -> Arc<Self> {
        Arc::new(Self {
            high_volume,
            long_term,
            changelog,
            tracker: TaskTracker::new(),
        })
    }

    /// Records the change to the log and returns a guard.
    ///
    /// Generates a unique [`ChangeId`] and writes a durable log entry before
    /// returning. The caller may proceed with LT side effects immediately after.
    ///
    /// When the [`ChangeGuard`] is dropped, a background process is spawned to
    /// clean up any unreferenced objects in LT storage.
    pub async fn record(self: Arc<Self>, change: Change) -> Result<ChangeGuard> {
        let id = ChangeId::new();
        self.changelog.record(&id, &change).await?;

        let state = ChangeState {
            id: id.clone(),
            change: change.clone(),
            phase: ChangePhase::Recorded,
            manager: self.clone(),
            heartbeat: Some(self.clone().spawn_heartbeat(id, change)),
        };

        Ok(ChangeGuard { state: Some(state) })
    }

    fn spawn_heartbeat(self: Arc<Self>, id: ChangeId, change: Change) -> JoinHandle<()> {
        let token = self.tracker.token();

        tokio::spawn(async move {
            // Prevent shutdown while the change is active in this instance.
            let _token = token;

            loop {
                tokio::time::sleep(REFRESH_INTERVAL).await;
                if self.changelog.record(&id, &change).await.is_err() {
                    // `record` retries internally. If it fails repeatedly, stop the heartbeat and
                    // let recovery from a different instance handle the expired entry.
                    return;
                }
            }
        })
    }

    /// Polls the changelog for stale entries and runs cleanup for each.
    ///
    /// Runs as an infinite background loop. On each iteration:
    /// - If the scan fails, waits with exponential backoff before retrying.
    /// - If the scan returns entries, cleans them up sequentially then loops immediately.
    /// - If the scan is empty, waits [`REFRESH_INTERVAL`] before polling again.
    ///
    /// Spawn this at startup to recover from any orphaned objects after a crash.
    pub async fn recover(self: Arc<Self>) {
        let mut backoff = INITIAL_BACKOFF;

        loop {
            match self.changelog.scan(SCAN_COUNT).await {
                Err(e) => {
                    objectstore_log::error!(!!&e, "Failed to run changelog recovery");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.mul_f32(1.5)).min(MAX_BACKOFF);
                }
                Ok(entries) if entries.is_empty() => {
                    backoff = INITIAL_BACKOFF;
                    tokio::time::sleep(REFRESH_INTERVAL).await;
                }
                Ok(entries) => {
                    backoff = INITIAL_BACKOFF;
                    // NB: Intentionally sequential to reduce load on the system.
                    for (id, change) in entries {
                        let state = ChangeState {
                            id: id.clone(),
                            change: change.clone(),
                            phase: ChangePhase::Recovered,
                            manager: self.clone(),
                            heartbeat: Some(self.clone().spawn_heartbeat(id, change)),
                        };
                        state.cleanup().await;
                    }
                }
            }
        }
    }
}

/// Durable write-ahead log for multi-step storage changes.
///
/// Records in-progress changes that span both HV and LT backends so that
/// recovery can identify and clean up orphaned LT blobs after crashes.
/// The log is stored independently from the data backend (though it may
/// share infrastructure) and is scoped per service instance.
///
/// Recovery is garbage collection — it reads HV state to determine which
/// blobs are unreferenced and deletes them. It never replays CAS mutations
/// or finishes incomplete operations.
///
/// Implementations handle instance identity, heartbeats, and key prefixing
/// internally — callers interact only with entries.
#[async_trait::async_trait]
pub trait ChangeLog: fmt::Debug + Send + Sync {
    /// Records a change before any side effects begin (write-ahead).
    ///
    /// Must be durable before returning — the caller will proceed with
    /// LT writes immediately after.
    async fn record(&self, id: &ChangeId, change: &Change) -> Result<()>;

    /// Removes a completed change from the log.
    ///
    /// Called after all cleanup (LT blob deletion) is finished. Removing
    /// a nonexistent entry is not an error (idempotent).
    async fn remove(&self, id: &ChangeId) -> Result<()>;

    /// Returns up to `max` changes eligible for recovery.
    ///
    /// During normal operation, this returns an empty list. After a crash, it
    /// may discover stale entries from another instance, claim them, and
    /// return them for cleanup.
    async fn scan(&self, max: u16) -> Result<Vec<(ChangeId, Change)>>;
}

/// In-memory [`ChangeLog`] for tests and deployments without durable logging.
///
/// Stores entries in a `HashMap`. [`Clone`]-able so tests can hold a handle
/// for direct inspection while the service owns a boxed copy.
#[derive(Debug, Clone, Default)]
pub struct InMemoryChangeLog {
    entries: Arc<Mutex<HashMap<ChangeId, Change>>>,
}

impl InMemoryChangeLog {
    /// Returns `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().expect("lock poisoned").is_empty()
    }

    /// Returns a snapshot of all entries currently in the log.
    pub fn entries(&self) -> HashMap<ChangeId, Change> {
        self.entries.lock().expect("lock poisoned").clone()
    }
}

#[async_trait::async_trait]
impl ChangeLog for InMemoryChangeLog {
    async fn record(&self, id: &ChangeId, change: &Change) -> Result<()> {
        let mut entries = self.entries.lock().expect("lock poisoned");
        entries.insert(id.clone(), change.clone());
        Ok(())
    }

    async fn remove(&self, id: &ChangeId) -> Result<()> {
        let mut entries = self.entries.lock().expect("lock poisoned");
        entries.remove(id);
        Ok(())
    }

    async fn scan(&self, _max: u16) -> Result<Vec<(ChangeId, Change)>> {
        Ok(vec![])
    }
}

/// [`ChangeLog`] implementation that discards all entries.
///
/// Used as the default when no durable log is configured. Provides no
/// crash-recovery guarantees — orphan cleanup relies entirely on in-process
/// [`ChangeGuard`] drop logic.
#[derive(Debug, Default)]
pub struct NoopChangeLog;

#[async_trait::async_trait]
impl ChangeLog for NoopChangeLog {
    async fn record(&self, _id: &ChangeId, _change: &Change) -> Result<()> {
        Ok(())
    }

    async fn remove(&self, _id: &ChangeId) -> Result<()> {
        Ok(())
    }

    async fn scan(&self, _max: u16) -> Result<Vec<(ChangeId, Change)>> {
        Ok(vec![])
    }
}

/// Phase of a multi-step storage change.
#[derive(Debug, PartialEq)]
pub enum ChangePhase {
    /// The change was recovered from changelog and the phase is unknown.
    Recovered,
    /// The change is recorded in the log and LT upload has started.
    Recorded,
    /// LT upload has succeeded and the tombstone is being updated.
    Written,
    /// The tombstone update failed due to a conflict.
    Lost,
    /// The tombstone update succeeded.
    Updated,
    /// Cleanup complete.
    Completed,
}

impl ChangePhase {
    /// Returns the phase corresponding to the outcome of a compare-and-write operation.
    pub fn compare_and_write(succeeded: bool) -> Self {
        if succeeded { Self::Updated } else { Self::Lost }
    }
}

/// Internal state for a [`ChangeGuard`].
///
/// Logs an error if dropped in any phase other than `Completed`.
#[derive(Debug)]
struct ChangeState {
    id: ChangeId,
    change: Change,
    phase: ChangePhase,
    manager: Arc<ChangeManager>,
    heartbeat: Option<JoinHandle<()>>,
}

impl ChangeState {
    /// Marks the operation as completed, preventing any cleanup on drop.
    fn mark_completed(mut self) {
        self.phase = ChangePhase::Completed;
    }

    /// Determines tombstone state and runs cleanup for unreferenced objects.
    async fn cleanup(mut self) {
        let current = match self.phase {
            // For `Recovered`, we must first check the state of the tombstone.
            ChangePhase::Recovered => self.read_tombstone().await,
            ChangePhase::Recorded => self.change.old.clone(),
            // For `Written`, the CAS outcome is unknown — read HV to determine it.
            ChangePhase::Written => self.read_tombstone().await,
            ChangePhase::Lost => self.change.old.clone(),
            ChangePhase::Updated => self.change.new.clone(),
            ChangePhase::Completed => return, // unreachable
        };

        if current != self.change.old
            && let Some(ref old) = self.change.old
        {
            self.cleanup_lt(old).await;
        }

        if current != self.change.new
            && let Some(ref new) = self.change.new
        {
            self.cleanup_lt(new).await;
        }

        self.cleanup_log().await;
        self.mark_completed();
    }

    /// Reads the tombstone target for `id` from HV, retrying with exponential backoff on error.
    ///
    /// Returns `None` if the entry holds an inline object or is absent.
    async fn read_tombstone(&self) -> Option<ObjectId> {
        let mut delay = INITIAL_BACKOFF;
        loop {
            match self
                .manager
                .high_volume
                .get_tiered_metadata(&self.change.id)
                .await
            {
                Ok(TieredMetadata::Tombstone(t)) => return Some(t.target),
                Ok(TieredMetadata::Object(_)) => return None,
                Ok(TieredMetadata::NotFound) => return None,
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay.mul_f32(1.5)).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Deletes `target` from `lt`, retrying with exponential backoff until success.
    async fn cleanup_lt(&self, target: &ObjectId) {
        let mut delay = INITIAL_BACKOFF;
        while self.manager.long_term.delete_object(target).await.is_err() {
            tokio::time::sleep(delay).await;
            delay = (delay.mul_f32(1.5)).min(MAX_BACKOFF);
        }
    }

    /// Removes this change's log entry, retrying with exponential backoff until success.
    ///
    /// Aborts the heartbeat task first so it cannot re-record after removal.
    async fn cleanup_log(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }

        let mut delay = INITIAL_BACKOFF;
        while self.manager.changelog.remove(&self.id).await.is_err() {
            tokio::time::sleep(delay).await;
            delay = (delay.mul_f32(1.5)).min(MAX_BACKOFF);
        }
    }

    /// Returns `true` if the change entry is still held by this instance.
    fn is_valid(&self) -> bool {
        self.heartbeat.as_ref().is_some_and(|h| !h.is_finished())
    }
}

impl Drop for ChangeState {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }

        if self.phase != ChangePhase::Completed {
            objectstore_log::error!(
                change = ?self.change,
                phase = ?self.phase,
                "Operation dropped without completing cleanup"
            );
        }
    }
}

/// RAII guard that tracks cleanup state for a multi-step storage change.
///
/// When dropped in a non-`Completed` phase, determines the LT blob to clean up
/// and spawns a background task to delete it. If no tokio runtime is available
/// (e.g., during shutdown), the drop logs an error instead of panicking.
#[derive(Debug)]
pub struct ChangeGuard {
    state: Option<ChangeState>,
}

impl ChangeGuard {
    /// Advances the operation to the given phase. Zero-cost, no I/O.
    pub fn advance(&mut self, phase: ChangePhase) {
        if let Some(ref mut state) = self.state {
            state.phase = phase;
        }
    }

    /// Returns `true` if the change entry is still held by this instance.
    ///
    /// This can return false if the internal heartbeats failed to refresh the entry in the log,
    /// causing it to be claimed by another instance during recovery. Proceeding with tombstone
    /// writes during this time can lead to inconsistencies.
    pub fn is_valid(&self) -> bool {
        self.state.as_ref().is_some_and(|s| s.is_valid())
    }
}

impl Drop for ChangeGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take()
            && state.phase != ChangePhase::Completed
            && state.is_valid()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(state.cleanup());
        }

        // NB: Drop of `ChangeState` logs an error if cleanup is not scheduled.
    }
}

#[cfg(test)]
mod tests {
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;

    fn make_id(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".into(),
                scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
            },
            key.into(),
        )
    }

    #[tokio::test]
    async fn record_then_scan_returns_entry() {
        let log = InMemoryChangeLog::default();
        let id = ChangeId::new();
        let change = Change {
            id: make_id("object-key"),
            new: Some(make_id("object-key/rev1")),
            old: None,
        };

        log.record(&id, &change).await.unwrap();

        let entries = log.entries();
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&id));
    }

    #[tokio::test]
    async fn remove_then_scan_does_not_return_entry() {
        let log = InMemoryChangeLog::default();
        let id = ChangeId::new();
        let change = Change {
            id: make_id("object-key"),
            new: None,
            old: Some(make_id("object-key/rev1")),
        };

        log.record(&id, &change).await.unwrap();
        log.remove(&id).await.unwrap();

        assert!(log.is_empty());
    }

    #[tokio::test]
    async fn remove_nonexistent_entry_is_not_an_error() {
        let log = InMemoryChangeLog::default();
        let id = ChangeId::new();

        log.remove(&id).await.unwrap();
    }

    /// When the tokio runtime is dropped while an operation is in flight, the `ChangeGuard`
    /// drops outside any runtime and cannot schedule cleanup. The log entry must persist
    /// so that a future recovery pass can identify and clean up orphaned blobs.
    #[test]
    fn runtime_drop_while_pending_preserves_log_entry() {
        use crate::backend::in_memory::InMemoryBackend;

        let log = InMemoryChangeLog::default();
        let manager = ChangeManager::new(
            Box::new(InMemoryBackend::new("hv")),
            Box::new(InMemoryBackend::new("lt")),
            Box::new(log.clone()),
        );

        let guard = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            // Simulate a mid-flight operation that recorded its change but did not complete.
            rt.block_on(manager.record(Change {
                id: make_id("crash-test"),
                new: Some(make_id("crash-test/rev")),
                old: None,
            }))
            .unwrap()
            // Runtime drops here while `guard` is still alive outside it.
        };

        // Guard drops with no runtime active: cleanup cannot be scheduled.
        drop(guard);

        // Log entry must survive so recovery can clean up the orphaned blob.
        assert_eq!(log.entries().len(), 1, "log entry must persist");
    }
}
