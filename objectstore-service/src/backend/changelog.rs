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

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::backend::common::{Backend, HighVolumeBackend, TieredMetadata};
use crate::error::Result;
use crate::id::ObjectId;

/// Initial delay for exponential backoff retries in background cleanup tasks.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// Maximum delay for exponential backoff retries in background cleanup tasks.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Stream of changes claimed during a recovery scan.
///
/// Each item is a `(ChangeId, Change)` pair that has been atomically claimed
/// from durable storage. Items are yielded lazily — the CAS claim for each
/// entry happens as the caller polls the stream.
pub type ChangeStream = BoxStream<'static, (ChangeId, Change)>;

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

    /// Parses a `ChangeId` from its UUID string representation.
    pub(crate) fn from_uuid_str(s: &str) -> Option<Self> {
        uuid::Uuid::parse_str(s).ok().map(Self)
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
        let token = self.tracker.token();

        let id = ChangeId::new();
        self.changelog.record(&id, &change).await?;

        let state = ChangeState {
            id,
            change,
            phase: ChangePhase::Recorded,
            manager: self.clone(),
            _token: token,
        };

        Ok(ChangeGuard { state: Some(state) })
    }

    /// Scans the changelog for outstanding entries and runs cleanup for each.
    ///
    /// Spawn this into a background task at startup to recover from any orphaned objects after a
    /// crash. During normal operation, this should return an empty list and have no effect.
    pub async fn recover(self: Arc<Self>) -> Result<()> {
        // Hold one token for the duration of recovery to prevent premature shutdown.
        let _token = self.tracker.token();

        let mut stream =
            self.changelog.scan().await.inspect_err(|e| {
                objectstore_log::error!(!!e, "Failed to run changelog recovery")
            })?;

        // NB: Intentionally clean up sequentially to reduce load on the system.
        while let Some((id, change)) = stream.next().await {
            let state = ChangeState {
                id,
                change,
                phase: ChangePhase::Recovered,
                manager: self.clone(),
                _token: self.tracker.token(),
            };

            state.cleanup().await;
        }

        Ok(())
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

    /// Scans for outstanding changes and returns them as a lazy stream.
    ///
    /// For durable implementations this performs atomic claiming via
    /// compare-and-swap so that concurrent instances do not double-process
    /// the same entry. Items where the claim fails are silently skipped.
    async fn scan(&self) -> Result<ChangeStream>;
}

/// In-memory [`ChangeLog`] for tests and deployments without durable logging.
///
/// Stores entries in a `HashMap`. [`Clone`]-able so tests can hold a handle
/// for direct inspection while the service owns a boxed copy.
#[derive(Debug, Clone, Default)]
pub struct InMemoryChangeLog {
    entries: Arc<Mutex<HashMap<ChangeId, Change>>>,
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

    async fn scan(&self) -> Result<ChangeStream> {
        // TODO: Check if we should return anything here. This should probably yield empty since
        // there are no abandoned in-memory entries ever.
        let entries = self.entries.lock().expect("lock poisoned");
        let items: Vec<(ChangeId, Change)> = entries
            .iter()
            .map(|(id, change)| (id.clone(), change.clone()))
            .collect();
        Ok(futures_util::stream::iter(items).boxed())
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

    async fn scan(&self) -> Result<ChangeStream> {
        Ok(futures_util::stream::empty().boxed())
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
    _token: TaskTrackerToken,
}

impl ChangeState {
    /// Marks the operation as completed, preventing any cleanup on drop.
    fn mark_completed(mut self) {
        self.phase = ChangePhase::Completed;
    }

    /// Determines tombstone state and runs cleanup for unreferenced objects.
    async fn cleanup(self) {
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
    async fn cleanup_log(&self) {
        let mut delay = INITIAL_BACKOFF;
        while self.manager.changelog.remove(&self.id).await.is_err() {
            tokio::time::sleep(delay).await;
            delay = (delay.mul_f32(1.5)).min(MAX_BACKOFF);
        }
    }
}

impl Drop for ChangeState {
    fn drop(&mut self) {
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
    pub(crate) fn advance(&mut self, phase: ChangePhase) {
        if let Some(ref mut state) = self.state {
            state.phase = phase;
        }
    }
}

impl Drop for ChangeGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take()
            && state.phase != ChangePhase::Completed
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

        let entries: Vec<_> = log.scan().await.unwrap().collect().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, id);
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

        let entries: Vec<_> = log.scan().await.unwrap().collect().await;
        assert!(entries.is_empty());
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
        let rt = tokio::runtime::Runtime::new().unwrap();
        let entries: Vec<_> =
            rt.block_on(async { log.scan().await.unwrap().collect::<Vec<_>>().await });
        assert_eq!(entries.len(), 1, "log entry must persist");
    }
}
