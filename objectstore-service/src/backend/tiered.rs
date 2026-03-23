//! Two-tier storage backend with size-based routing and redirect tombstones.
//!
//! [`TieredStorage`] routes objects to a high-volume or long-term backend based
//! on size and maintains redirect tombstones so that reads never need to probe
//! both backends. See the [crate-level documentation](crate) for the high-level
//! motivation, and the [`TieredStorage`] struct docs for routing and tombstone
//! semantics.
//!
//! # Cross-Tier Consistency
//!
//! A single logical object may span both backends: a tombstone in HV pointing
//! to a payload in LT. Mutations keep the two in sync through compare-and-swap
//! on the high-volume backend (see [`HighVolumeBackend::compare_and_write`]).
//! Each operation reads the current HV revision, performs its work, then
//! atomically swaps the HV entry only if the revision is still current —
//! rolling back on conflict.
//!
//! ## Revision Keys
//!
//! Every large-object write stores its payload at a **revision key** in the
//! long-term backend: `{original_key}/{uuid}`. The UUID suffix is random (no
//! monotonicity is guaranteed), so each write targets a distinct LT path
//! regardless of whether another write to the same logical key is in progress.
//! The tombstone in HV then points to this specific revision. Because each
//! writer owns its own LT blob, the compare-and-swap on the tombstone becomes
//! an atomic pointer swap: the winner's revision is committed and the loser
//! can safely delete its own blob without affecting the winner.
//!
//! See `new_long_term_revision` for the key construction.
//!
//! ## Compare-and-Swap
//!
//! All mutating operations follow a common pattern of reading the current
//! revision, performing the upload, atomically swapping the revision (commit
//! point), and finally cleaning up old objects:
//!
//! ### Large-Object Write (> 1 MiB)
//!
//! 1. **Read HV** to capture the current revision (existing tombstone target,
//!    or absent).
//! 2. **Write payload to LT** at a unique revision key.
//! 3. **Compare-and-swap in HV**: write a tombstone pointing to the new
//!    revision, only if the current revision still matches step 1.
//!    - **OK** — delete the old LT blob, if any (best-effort).
//!    - **Conflict** — another writer won the race; delete our new LT blob.
//!    - **Error** — delete our new LT blob, then propagate the error.
//!
//! ### Small-Object Write (≤ 1 MiB)
//!
//! 1. **Write inline to HV**, skipping the write if a tombstone is present.
//!    - **OK** — done; the object is stored entirely in HV.
//!    - **Tombstone present** — a large object already occupies this key;
//!      continue:
//! 2. **Compare-and-swap in HV**: replace the tombstone with inline data, only
//!    if the tombstone's revision still matches.
//!    - **OK** — delete the old LT blob (best-effort).
//!    - **Conflict** — another writer won the race; they will clean up the
//!      LT blob and we have no new LT blob to clean up.
//!
//! ### Delete
//!
//! 1. **Delete from HV** if the entry is not a tombstone.
//!    - **OK** — done; there is no LT data to clean up.
//!    - **Tombstone present** — a large object is stored here; continue:
//! 2. **Compare-and-swap in HV**: remove the tombstone, only if its revision
//!    still matches.
//!    - **OK** — delete the LT blob (best-effort).
//!    - **Conflict** — another writer won the race; they will clean up.
//!
//! Tombstone removal is the commit point for deletes. If the subsequent LT
//! cleanup fails, an orphan blob remains but the object is already unreachable
//! through the normal read path.
//!
//! ## Last-Writer-Wins
//!
//! Concurrent mutations on the same key are inherently a race. Even a write
//! that returns `Ok` may be immediately overwritten by another caller — there
//! is no ordering guarantee and objectstore cannot provide a read-your-writes
//! promise.
//!
//! CAS conflicts are therefore **not errors**: the losing writer's data is
//! cleaned up and `Ok` is returned, because the result is indistinguishable
//! from having succeeded a moment earlier and then been overwritten.
//!
//! ### Idempotency
//!
//! `compare_and_write` is idempotent: if the row is already in the target state, it
//! returns `true` without re-applying the mutation. This is critical for retry
//! safety. If the server commits a write but the response is lost, a retry sees the
//! already-mutated state and still returns `true` — so callers do not mistakenly
//! treat a successful commit as a lost race and clean up data that was actually
//! persisted.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, HighVolumeBackend, MetadataResponse, PutResponse,
    TieredGet, TieredMetadata, TieredWrite, Tombstone,
};
use crate::backend::{HighVolumeStorageConfig, StorageConfig};
use crate::error::Result;
use crate::id::ObjectId;
use crate::stream::{ClientStream, SizedPeek};

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

/// Timeout for joining outstanding background cleanup operations on shutdown.
const BACKGROUND_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Counts outstanding background cleanup operations for graceful shutdown.
///
/// The counter is incremented when an [`OperationState`] is created and
/// decremented when it is dropped. [`BackgroundCounter::join`] waits until the
/// counter reaches zero or the timeout elapses.
#[derive(Debug, Clone)]
struct BackgroundCounter {
    inner: Arc<BackgroundCounterInner>,
}

#[derive(Debug)]
struct BackgroundCounterInner {
    outstanding: AtomicUsize,
    drained: Notify,
}

impl BackgroundCounter {
    fn new() -> Self {
        Self {
            inner: Arc::new(BackgroundCounterInner {
                outstanding: AtomicUsize::new(0),
                drained: Notify::new(),
            }),
        }
    }

    fn increment(&self) {
        self.inner.outstanding.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement(&self) {
        let prev = self.inner.outstanding.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            self.inner.drained.notify_waiters();
        }
    }

    async fn join(&self, timeout: Duration) {
        if self.inner.outstanding.load(Ordering::Acquire) == 0 {
            return;
        }
        let _ = tokio::time::timeout(timeout, async {
            loop {
                let notified = self.inner.drained.notified();
                if self.inner.outstanding.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await;

        let remaining = self.inner.outstanding.load(Ordering::Acquire);
        if remaining > 0 {
            objectstore_log::error!(
                remaining,
                "Cleanup operations still outstanding at shutdown"
            );
        }
    }
}

/// Phase of a multi-step storage operation.
///
/// Phases are ordered — the ordering determines which LT blob to clean up on drop.
#[derive(Debug, PartialEq, PartialOrd)]
enum OperationPhase {
    Registered,
    Written,
    Lost,
    Updated,
    Completed,
}

impl OperationPhase {
    /// Returns the phase corresponding to the outcome of a compare-and-write operation.
    fn compare_and_write(succeeded: bool) -> Self {
        if succeeded { Self::Updated } else { Self::Lost }
    }
}

/// Describes the LT blobs involved in a multi-step storage operation.
///
/// Every mutating flow maps to: "I may have written a `new_target` LT blob and I
/// may be replacing an `old_target` LT blob."
#[derive(Debug)]
struct Operation {
    /// The new LT blob written by this operation.
    ///
    /// Needs cleanup on failure (phase < [`OperationPhase::CasWon`]).
    new_target: Option<ObjectId>,
    /// The old LT blob being replaced.
    ///
    /// Needs cleanup on success (phase >= [`OperationPhase::CasWon`]).
    old_target: Option<ObjectId>,
}

/// Internal state for an [`OperationGuard`].
///
/// Increments the counter on construction and decrements it on drop. Logs an
/// error if dropped in any phase other than [`OperationPhase::Completed`].
#[derive(Debug)]
struct OperationState {
    operation: Operation,
    phase: OperationPhase,
    lt: Arc<dyn Backend>,
    counter: BackgroundCounter,
}

impl OperationState {
    fn new(operation: Operation, lt: Arc<dyn Backend>, counter: BackgroundCounter) -> Self {
        counter.increment();
        Self {
            operation,
            phase: OperationPhase::Registered,
            lt,
            counter,
        }
    }

    /// Returns the LT blob that should be deleted based on the current phase.
    ///
    /// Returns `None` if the phase is `Completed` or if no cleanup is needed.
    fn cleanup_target(&self) -> Option<ObjectId> {
        if self.phase == OperationPhase::Completed {
            return None;
        }
        if self.phase < OperationPhase::Updated {
            return self.operation.new_target.clone();
        }
        self.operation.old_target.clone()
    }

    /// Marks the operation as completed, suppressing the incomplete-cleanup error on drop.
    fn complete(mut self) {
        self.phase = OperationPhase::Completed;
    }
}

impl Drop for OperationState {
    fn drop(&mut self) {
        if self.phase != OperationPhase::Completed {
            objectstore_log::error!(
                operation = ?self.operation,
                phase = ?self.phase,
                "Operation dropped without completing cleanup"
            );
        }
        self.counter.decrement();
    }
}

/// RAII guard that tracks cleanup state for a multi-step storage operation.
///
/// When dropped in a non-`Completed` phase, determines the LT blob to clean up
/// and spawns a background task to delete it. If no tokio runtime is available
/// (e.g., during shutdown), the drop logs an error instead of panicking.
struct OperationGuard {
    state: Option<OperationState>,
}

impl OperationGuard {
    /// Advances the operation to the given phase. Zero-cost, no I/O.
    fn advance(&mut self, phase: OperationPhase) {
        if let Some(ref mut state) = self.state {
            state.phase = phase;
        }
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        let Some(mut state) = self.state.take() else {
            return;
        };

        if state.phase == OperationPhase::Completed {
            return;
        }

        let target = state.cleanup_target();

        let Some(target) = target else {
            // No LT blob to clean up for this (flow, phase) combination.
            state.phase = OperationPhase::Completed;
            return;
        };

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // No runtime available (e.g., dropped during shutdown).
            // state drops without Completed → logs error in OperationState::drop.
            return;
        };

        handle.spawn(async move {
            match state.lt.delete_object(&target).await {
                Ok(()) => state.complete(),
                Err(e) => {
                    objectstore_log::error!(
                        !!&e,
                        target = %target.as_storage_path(),
                        "Background LT cleanup failed"
                    );
                    // state drops without Completed → logs error in OperationState::drop
                }
            }
            // state drops here → decrements counter
        });
    }
}

/// Creates a new [`ObjectId`] with the same context but a unique revision key.
///
/// The new key has the format `{original_key}/{uuid_v7}`, producing a distinct
/// storage path for each large-object write. [`ObjectId::from_storage_path`] parses
/// the result back correctly because the key portion may contain `/`.
fn new_long_term_revision(id: &ObjectId) -> ObjectId {
    ObjectId {
        context: id.context.clone(),
        key: format!("{}/{}", id.key, uuid::Uuid::now_v7()),
    }
}

/// Configuration for [`TieredStorage`].
///
/// Composes two backends into a tiered routing setup: `high_volume` for small
/// objects and `long_term` for large objects. Nesting [`StorageConfig::Tiered`]
/// inside another tiered config is not supported.
///
/// # Example
///
/// ```yaml
/// storage:
///   type: tiered
///   high_volume:
///     type: bigtable
///     project_id: my-project
///     instance_name: objectstore
///     table_name: objectstore
///   long_term:
///     type: gcs
///     bucket: my-objectstore-bucket
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TieredStorageConfig {
    /// Backend for high-volume, small objects.
    ///
    /// Must be a backend that implements [`HighVolumeBackend`] (currently
    /// only BigTable).
    pub high_volume: HighVolumeStorageConfig,
    /// Backend for large, long-term objects.
    pub long_term: Box<StorageConfig>,
}

/// Two-tier storage backend that routes objects by size.
///
/// `TieredStorage` implements [`Backend`] and is intended to be used inside a
/// [`StorageService`](crate::StorageService), which wraps it with task spawning and panic
/// isolation.
///
/// # Size-Based Routing
///
/// Objects are routed at write time based on their size relative to a **1 MiB threshold**:
///
/// - Objects **≤ 1 MiB** go to the `high_volume` backend — optimized for low-latency reads
///   and writes of small objects (e.g. BigTable).
/// - Objects **> 1 MiB** go to the `long_term` backend — optimized for cost-efficient
///   storage of large objects (e.g. GCS).
///
/// # Redirect Tombstones
///
/// Because the [`ObjectId`] is backend-independent, reads must be able to find an object
/// without knowing which backend stores it. A naive approach would check the long-term
/// backend on every read miss in the high-volume backend — but that is slow and expensive.
///
/// Instead, when an object is stored in the long-term backend, a **redirect tombstone** is
/// written in the high-volume backend. It acts as a signpost: "the real data lives in the
/// other backend at this target." On reads, a single high-volume lookup either returns the
/// object directly or follows the tombstone to long-term storage, without probing both
/// backends.
///
/// How tombstones are physically stored is determined by the [`HighVolumeBackend`]
/// implementation — refer to the backend's own documentation for storage format details.
///
/// # Consistency
///
/// Consistency across the two backends is maintained through compare-and-swap
/// operations on the high-volume backend (see
/// [`HighVolumeBackend::compare_and_write`]), not distributed locks. Each
/// mutating operation reads the current high-volume revision, performs its
/// work, and then atomically swaps the high-volume entry only if the revision
/// is still current — rolling back on conflict.
///
/// See the [module-level documentation](self) for per-operation diagrams.
///
/// # Usage
///
/// `TieredStorage` handles only the routing and consistency logic. Wrap it in a
/// [`StorageService`](crate::service::StorageService) to add task spawning, panic isolation,
/// and concurrency limiting.
#[derive(Debug)]
pub struct TieredStorage {
    /// The backend for small objects (≤ 1 MiB).
    high_volume: Arc<dyn HighVolumeBackend>,
    /// The backend for large objects (> 1 MiB).
    long_term: Arc<dyn Backend>,
    /// Tracks outstanding background cleanup operations for graceful shutdown.
    counter: BackgroundCounter,
}

impl TieredStorage {
    /// Creates a new `TieredStorage` with the given backends.
    pub fn new(high_volume: Box<dyn HighVolumeBackend>, long_term: Box<dyn Backend>) -> Self {
        Self {
            high_volume: Arc::from(high_volume),
            long_term: Arc::from(long_term),
            counter: BackgroundCounter::new(),
        }
    }

    /// Creates an [`OperationGuard`] for the given operation.
    fn operation_guard(&self, operation: Operation) -> OperationGuard {
        OperationGuard {
            state: Some(OperationState::new(
                operation,
                Arc::clone(&self.long_term),
                self.counter.clone(),
            )),
        }
    }

    /// Returns the name of the backend corresponding to the given routing choice.
    fn backend_type(&self, choice: &BackendChoice) -> &'static str {
        match choice {
            BackendChoice::HighVolume => self.high_volume.name(),
            BackendChoice::LongTerm => self.long_term.name(),
        }
    }

    /// Puts an object into the high-volume backend.
    ///
    /// If a tombstone already exists, attempts to swap it for the new object.
    /// On success, the old long-term blob is cleaned up in the background.
    async fn put_high_volume(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<()> {
        let tombstone_opt = self
            .high_volume
            .put_non_tombstone(id, metadata, payload.clone())
            .await?;

        let Some(Tombstone { target, .. }) = tombstone_opt else {
            // No tombstone exists — write succeeded inline, no LT blob to manage.
            return Ok(());
        };

        // A tombstone exists — swap it for inline data.
        let mut guard = self.operation_guard(Operation {
            new_target: None,
            old_target: Some(target.clone()),
        });

        let write = TieredWrite::Object(metadata.clone(), payload);
        let written = self
            .high_volume
            .compare_and_write(id, Some(&target), write)
            .await?;

        // Update guard and let it schedule cleanup in the background.
        guard.advance(OperationPhase::compare_and_write(written));

        Ok(())
    }

    /// Puts an object into the long-term backend with a redirect tombstone in front.
    ///
    /// On success, the old long-term blob (if any) is cleaned up in the background.
    /// On failure or CAS conflict, the newly written LT blob is cleaned up in the background.
    async fn put_long_term(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<()> {
        // 1. Read current HV revision to establish the write precondition
        let current = match self.high_volume.get_tiered_metadata(id).await? {
            TieredMetadata::Tombstone(t) => Some(t.target),
            _ => None,
        };

        // 2. Write payload to long-term at a unique revision key.
        let new = new_long_term_revision(id);
        let tombstone = Tombstone {
            target: new.clone(),
            expiration_policy: metadata.expiration_policy,
        };

        let mut guard = self.operation_guard(Operation {
            new_target: Some(new.clone()),
            old_target: current.clone(),
        });

        self.long_term.put_object(&new, metadata, stream).await?;
        guard.advance(OperationPhase::Written);

        // 3. CAS commit: write tombstone only if HV state matches what we saw.
        let written = self
            .high_volume
            .compare_and_write(id, current.as_ref(), TieredWrite::Tombstone(tombstone))
            .await?;

        // Update guard and let it schedule cleanup in the background.
        guard.advance(OperationPhase::compare_and_write(written));

        Ok(())
    }
}

#[async_trait::async_trait]
impl Backend for TieredStorage {
    fn name(&self) -> &'static str {
        "tiered"
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        let start = Instant::now();
        if metadata.origin.is_none() {
            objectstore_metrics::count!("put.origin_missing", usecase = id.usecase().to_owned());
        }

        let peeked = SizedPeek::new(stream, BACKEND_SIZE_THRESHOLD).await?;
        objectstore_metrics::record!(
            "put.first_chunk.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            complete = if peeked.is_exhausted() { "yes" } else { "no" },
        );

        let (backend_choice, stored_size) = if peeked.is_exhausted() {
            let payload = peeked.into_bytes().await?;
            let payload_len = payload.len() as u64;
            self.put_high_volume(id, metadata, payload).await?;
            (BackendChoice::HighVolume, payload_len)
        } else {
            let (stored_size, stream) = counting_stream(peeked.into_stream());
            self.put_long_term(id, metadata, stream.boxed()).await?;
            (BackendChoice::LongTerm, stored_size.load(Ordering::Acquire))
        };

        let backend_ty = self.backend_type(&backend_choice);
        objectstore_metrics::record!(
            "put.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = backend_ty,
        );
        objectstore_metrics::record!(
            "put.size" = stored_size,
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = backend_ty,
        );

        Ok(())
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let start = Instant::now();

        let hv_result = self.high_volume.get_tiered_object(id).await?;
        let (result, backend_choice) = match hv_result {
            TieredGet::NotFound => (None, BackendChoice::HighVolume),
            TieredGet::Object(metadata, stream) => {
                (Some((metadata, stream)), BackendChoice::HighVolume)
            }
            TieredGet::Tombstone(tombstone) => (
                self.long_term.get_object(&tombstone.target).await?,
                BackendChoice::LongTerm,
            ),
        };

        let backend_type = self.backend_type(&backend_choice);
        objectstore_metrics::record!(
            "get.latency.pre-response" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = backend_type,
        );

        if let Some((ref metadata, _)) = result {
            if let Some(size) = metadata.size {
                objectstore_metrics::record!(
                    "get.size" = size,
                    usecase = id.usecase().to_owned(),
                    backend_choice = backend_choice.as_str(),
                    backend_type = backend_type,
                );
            } else {
                objectstore_log::warn!(backend_type, "Missing object size");
            }
        }

        Ok(result)
    }

    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        let start = Instant::now();

        let hv_result = self.high_volume.get_tiered_metadata(id).await?;
        let (result, backend_choice) = match hv_result {
            TieredMetadata::NotFound => (None, BackendChoice::HighVolume),
            TieredMetadata::Object(metadata) => (Some(metadata), BackendChoice::HighVolume),
            TieredMetadata::Tombstone(tombstone) => (
                self.long_term.get_metadata(&tombstone.target).await?,
                BackendChoice::LongTerm,
            ),
        };

        objectstore_metrics::record!(
            "head.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = self.backend_type(&backend_choice),
        );

        Ok(result)
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        let start = Instant::now();

        let mut backend_choice = BackendChoice::HighVolume;

        if let Some(tombstone) = self.high_volume.delete_non_tombstone(id).await? {
            backend_choice = BackendChoice::LongTerm;

            let mut guard = self.operation_guard(Operation {
                new_target: None,
                old_target: Some(tombstone.target.clone()),
            });

            // Remove the tombstone; the LT blob becomes unreachable at this point.
            let deleted = self
                .high_volume
                .compare_and_write(id, Some(&tombstone.target), TieredWrite::Delete)
                .await?;

            // Update guard and let it schedule cleanup in the background.
            guard.advance(OperationPhase::compare_and_write(deleted));
        }

        objectstore_metrics::record!(
            "delete.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = self.backend_type(&backend_choice),
        );

        Ok(())
    }

    async fn join(&self) {
        tokio::join!(
            self.high_volume.join(),
            self.long_term.join(),
            self.counter.join(BACKGROUND_JOIN_TIMEOUT),
        );
    }
}

#[derive(Debug)]
enum BackendChoice {
    HighVolume,
    LongTerm,
}

impl BackendChoice {
    fn as_str(&self) -> &'static str {
        match self {
            BackendChoice::HighVolume => "high-volume",
            BackendChoice::LongTerm => "long-term",
        }
    }
}

impl std::fmt::Display for BackendChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Wraps a stream to count the total bytes yielded by successful chunks.
///
/// Returns the shared counter and the wrapped stream. The counter is incremented
/// as the stream is consumed, so read it only after the stream is exhausted.
fn counting_stream<S, E>(stream: S) -> (Arc<AtomicU64>, impl Stream<Item = Result<Bytes, E>>)
where
    S: Stream<Item = Result<Bytes, E>>,
{
    let counter = Arc::new(AtomicU64::new(0));

    (
        counter.clone(),
        stream.inspect(move |res| {
            if let Ok(chunk) = res {
                counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;
    use crate::id::ObjectContext;
    use crate::stream;

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_id(key: &str) -> ObjectId {
        ObjectId::new(make_context(), key.into())
    }

    fn make_tiered_storage() -> (TieredStorage, InMemoryBackend, InMemoryBackend) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt.clone()));
        (storage, hv, lt)
    }

    // --- new_long_term_revision tests ---

    #[test]
    fn revision_id_preserves_context() {
        let id = make_id("my-key");
        let revised = new_long_term_revision(&id);
        assert_eq!(revised.context, id.context);
        assert!(
            revised.key.starts_with("my-key/"),
            "revised key should have /<uuid> suffix, got: {}",
            revised.key
        );
    }

    #[test]
    fn revision_id_roundtrips_storage_path() {
        let id = make_id("original");
        let revised = new_long_term_revision(&id);
        let path = revised.as_storage_path().to_string();
        let parsed = ObjectId::from_storage_path(&path)
            .unwrap_or_else(|| panic!("failed to parse '{path}'"));
        assert_eq!(parsed, revised);
    }

    #[test]
    fn revision_id_is_unique() {
        let id = make_id("base-key");
        let a = new_long_term_revision(&id);
        let b = new_long_term_revision(&id);
        assert_ne!(a.key, b.key, "two calls should produce different keys");
    }

    // --- Basic behavior ---

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let id = make_id("does-not-exist");

        assert!(storage.get_object(&id).await.unwrap().is_none());
        assert!(storage.get_metadata(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_succeeds() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let id = make_id("does-not-exist");

        storage.delete_object(&id).await.unwrap();
    }

    // --- Put routing ---

    #[tokio::test]
    async fn put_small_object_stores_inline() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("small");
        let payload = b"small payload".to_vec();

        storage
            .put_object(&id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap();

        assert!(hv.contains(&id), "expected in high-volume");
        assert!(!lt.contains(&id), "leaked to long-term");

        let (_, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body = stream::read_to_vec(s).await.unwrap();
        assert_eq!(body, payload);

        assert!(
            storage.get_metadata(&id).await.unwrap().is_some(),
            "get_metadata should return metadata for inline objects"
        );
    }

    #[tokio::test]
    async fn put_large_object_creates_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("large");
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB, over threshold
        let metadata_in = Metadata {
            content_type: "image/png".into(),
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            origin: Some("10.0.0.1".into()),
            ..Default::default()
        };

        storage
            .put_object(&id, &metadata_in, stream::single(payload.clone()))
            .await
            .unwrap();

        // Tombstone in HV: correct expiration_policy, target is a revision key.
        let tombstone = hv.get(&id).expect_tombstone();
        assert_eq!(tombstone.expiration_policy, metadata_in.expiration_policy);
        let lt_id = tombstone.target;
        assert!(
            lt_id.key().starts_with(id.key()),
            "tombstone target key should be a revision of the HV key, got: {}",
            lt_id.key()
        );

        // LT object at revision key with correct metadata.
        let (lt_meta, _) = lt.get(&lt_id).expect_object();
        assert_eq!(lt_meta.content_type, "image/png");
        assert_eq!(lt_meta.expiration_policy, metadata_in.expiration_policy);

        // get_object follows the tombstone and returns the correct payload.
        let (_, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body = stream::read_to_vec(s).await.unwrap();
        assert_eq!(body, payload);

        // get_metadata follows the tombstone and returns the correct content_type.
        let metadata = storage.get_metadata(&id).await.unwrap().unwrap();
        assert_eq!(metadata.content_type, "image/png");
    }

    // --- Put overwrites ---

    #[tokio::test]
    async fn reinsert_small_over_large_swaps_to_inline() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("reinsert-key");

        // First: insert a large object → creates tombstone in hv, payload in lt at lt_id
        let large_payload = vec![0xABu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(large_payload))
            .await
            .unwrap();

        let lt_id = hv.get(&id).expect_tombstone().target;

        // Re-insert a SMALL payload with the same key.
        // The CAS-swap puts the small object inline in HV and schedules background cleanup.
        let small_payload = vec![0xCDu8; 100]; // well under 1 MiB threshold
        storage
            .put_object(&id, &Default::default(), stream::single(small_payload))
            .await
            .unwrap();

        // The small object is now inline in high-volume.
        hv.get(&id).expect_object();

        // Drain background cleanup tasks before asserting LT state.
        storage.join().await;

        // The old long-term blob was cleaned up.
        lt.get(&lt_id).expect_not_found();
    }

    #[tokio::test]
    async fn overwrite_large_with_large_replaces_revision() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("overwrite-large");

        let payload1 = vec![0xAAu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(payload1))
            .await
            .unwrap();
        let lt_id_1 = hv.get(&id).expect_tombstone().target;

        let payload2 = vec![0xBBu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(payload2.clone()))
            .await
            .unwrap();
        let lt_id_2 = hv.get(&id).expect_tombstone().target;

        assert_ne!(
            lt_id_1, lt_id_2,
            "second write should create a new revision"
        );

        // Drain background cleanup tasks before asserting LT state.
        storage.join().await;

        lt.get(&lt_id_1).expect_not_found();
        lt.get(&lt_id_2).expect_object();

        let (_, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body = stream::read_to_vec(s).await.unwrap();
        assert_eq!(body, payload2);
    }

    // --- Delete ---

    #[tokio::test]
    async fn delete_small_object() {
        let (storage, hv, _lt) = make_tiered_storage();
        let id = make_id("delete-small");

        storage
            .put_object(&id, &Default::default(), stream::single("tiny"))
            .await
            .unwrap();

        storage.delete_object(&id).await.unwrap();

        hv.get(&id).expect_not_found();
        assert!(storage.get_object(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_large_object_cleans_up_both_backends() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("delete-both");
        let payload = vec![0u8; 2 * 1024 * 1024]; // 2 MiB

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // Capture lt_id before deleting (it lives at the revision key, not at id).
        let lt_id = hv.get(&id).expect_tombstone().target;

        storage.delete_object(&id).await.unwrap();

        // Drain background cleanup tasks before asserting LT state.
        storage.join().await;

        assert!(!hv.contains(&id), "tombstone not cleaned up");
        assert!(!lt.contains(&lt_id), "long-term object not cleaned up");
    }

    /// A backend wrapper that delegates everything except `delete_object`, which always fails.
    #[derive(Debug)]
    struct FailingDeleteBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl Backend for FailingDeleteBackend {
        fn name(&self) -> &'static str {
            "failing-delete"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            self.0.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, _id: &ObjectId) -> Result<DeleteResponse> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated long-term delete failure",
            )))
        }
    }

    /// When the long-term GCS cleanup fails after the tombstone is deleted, the
    /// delete still succeeds (GCS cleanup is best-effort). An orphan blob may
    /// remain in LT storage, which is accepted.
    #[tokio::test]
    async fn delete_succeeds_when_gcs_cleanup_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = FailingDeleteBackend(InMemoryBackend::new("lt"));
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt));

        let id = make_id("fail-delete");
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> goes to long-term
        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // Delete succeeds even though GCS cleanup fails (it is best-effort).
        let result = storage.delete_object(&id).await;
        assert!(
            result.is_ok(),
            "delete should succeed despite GCS cleanup failure"
        );

        // The tombstone in HV is gone (CAS-deleted first, before GCS cleanup).
        hv.get(&id).expect_not_found();

        // The orphaned GCS blob remains but the object is unreachable through the service.
        assert!(
            storage.get_object(&id).await.unwrap().is_none(),
            "object should be unreachable after tombstone is deleted"
        );
    }

    // --- CAS conflicts ---

    /// A backend wrapper that delegates everything except `compare_and_write`, which always
    /// returns `Ok(false)` to simulate a lost CAS race.
    #[derive(Debug)]
    struct ConflictingCasBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl Backend for ConflictingCasBackend {
        fn name(&self) -> &'static str {
            "conflicting-cas"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            self.0.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
            self.0.delete_object(id).await
        }
    }

    #[async_trait::async_trait]
    impl HighVolumeBackend for ConflictingCasBackend {
        async fn put_non_tombstone(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            payload: bytes::Bytes,
        ) -> Result<Option<Tombstone>> {
            self.0.put_non_tombstone(id, metadata, payload).await
        }

        async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet> {
            self.0.get_tiered_object(id).await
        }

        async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
            self.0.get_tiered_metadata(id).await
        }

        async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
            self.0.delete_non_tombstone(id).await
        }

        async fn compare_and_write(
            &self,
            _id: &ObjectId,
            _current: Option<&ObjectId>,
            _write: TieredWrite,
        ) -> Result<bool> {
            Ok(false) // always conflict
        }
    }

    /// After a large-object write loses the CAS race, the new LT blob must be
    /// cleaned up. The put still returns `Ok(())` — from the caller's view, a
    /// concurrent write won.
    #[tokio::test]
    async fn put_large_cas_conflict_cleans_up_new_blob() {
        let hv = ConflictingCasBackend(InMemoryBackend::new("hv"));
        let lt = InMemoryBackend::new("lt");
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt.clone()));

        let id = make_id("cas-conflict-large");
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> long-term path

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // Drain background cleanup tasks before asserting LT state.
        storage.join().await;

        assert!(
            lt.is_empty(),
            "LT blob should be cleaned up after CAS conflict"
        );
    }

    /// When swapping a tombstone for inline data, a CAS conflict means another
    /// writer won. The put still returns `Ok(())` — no LT blob was written, so
    /// there is nothing to clean up.
    #[tokio::test]
    async fn put_small_over_tombstone_cas_conflict_succeeds() {
        let inner = InMemoryBackend::new("hv");
        let id = make_id("cas-conflict-small");

        // Pre-seed a tombstone directly in the inner backend so put_non_tombstone
        // returns it instead of writing inline.
        let tombstone = Tombstone {
            target: make_id("lt-object"),
            expiration_policy: ExpirationPolicy::Manual,
        };
        inner
            .compare_and_write(&id, None, TieredWrite::Tombstone(tombstone))
            .await
            .unwrap();

        let lt = InMemoryBackend::new("lt");
        let hv = ConflictingCasBackend(inner);
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt));

        // Writing a small object over a tombstone should succeed even when CAS
        // conflicts — the other writer's write is accepted.
        storage
            .put_object(&id, &Default::default(), stream::single("tiny"))
            .await
            .unwrap();
    }

    // --- Failure / inconsistency ---

    /// A backend where `compare_and_write` always errors, but all other operations work normally.
    #[derive(Debug)]
    struct FailingCasBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl Backend for FailingCasBackend {
        fn name(&self) -> &'static str {
            "failing-cas"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            self.0.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
            self.0.delete_object(id).await
        }
    }

    #[async_trait::async_trait]
    impl HighVolumeBackend for FailingCasBackend {
        async fn put_non_tombstone(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            payload: bytes::Bytes,
        ) -> Result<Option<Tombstone>> {
            self.0.put_non_tombstone(id, metadata, payload).await
        }

        async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet> {
            self.0.get_tiered_object(id).await
        }

        async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
            self.0.get_tiered_metadata(id).await
        }

        async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
            self.0.delete_non_tombstone(id).await
        }

        async fn compare_and_write(
            &self,
            _id: &ObjectId,
            _current: Option<&ObjectId>,
            _write: TieredWrite,
        ) -> Result<bool> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated compare_and_write failure",
            )))
        }
    }

    /// If the tombstone write to the high-volume backend fails after the long-term
    /// write succeeds, the long-term object must be cleaned up so we never leave
    /// an unreachable orphan in long-term storage.
    #[tokio::test]
    async fn no_orphan_when_tombstone_write_fails() {
        let lt = InMemoryBackend::new("lt");
        let hv = FailingCasBackend(InMemoryBackend::new("hv"));
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt.clone()));

        let id = make_id("orphan-test");
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> long-term path
        let result = storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await;

        assert!(result.is_err());

        // Drain background cleanup tasks before asserting LT state.
        storage.join().await;

        assert!(lt.is_empty(), "long-term object not cleaned up");
    }

    /// If a tombstone exists in high-volume but the corresponding object is
    /// missing from long-term storage (e.g. due to a race condition or partial
    /// cleanup), reads should gracefully return None rather than error.
    #[tokio::test]
    async fn orphan_tombstone_returns_none() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("orphan-tombstone");
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // The object is at the revision key in LT, not at id.
        let lt_id = hv.get(&id).expect_tombstone().target;

        // Remove the long-term object, leaving an orphan tombstone in hv
        lt.remove(&lt_id);

        assert!(
            storage.get_object(&id).await.unwrap().is_none(),
            "orphan tombstone should resolve to None on get_object"
        );
        assert!(
            storage.get_metadata(&id).await.unwrap().is_none(),
            "orphan tombstone should resolve to None on get_metadata"
        );
    }

    // --- Redirect target ---

    /// A tombstone carrying an explicit `target` is followed correctly on reads and deletes,
    /// including when the target ObjectId differs from the HV ObjectId.
    #[tokio::test]
    async fn tombstone_target_is_used_for_reads_and_deletes() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt.clone()));

        let hv_id = make_id("hv-key");
        let lt_id = make_id("lt-key");
        let payload = vec![0xABu8; 100];

        // Write the object under the LT id and a tombstone pointing to it from HV.
        lt.put_object(&lt_id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap();
        let tombstone = Tombstone {
            target: lt_id.clone(),
            expiration_policy: ExpirationPolicy::Manual,
        };
        hv.compare_and_write(&hv_id, None, TieredWrite::Tombstone(tombstone))
            .await
            .unwrap();

        // get_object must follow the tombstone and find the object via the lt_id target.
        let (_, s) = storage.get_object(&hv_id).await.unwrap().unwrap();
        let body = stream::read_to_vec(s).await.unwrap();
        assert_eq!(body, payload);

        // delete_object must clean up both backends using the target.
        storage.delete_object(&hv_id).await.unwrap();
        storage.join().await;
        assert!(!hv.contains(&hv_id), "tombstone should be removed");
        assert!(!lt.contains(&lt_id), "lt object should be removed");
    }

    // --- Multi-chunk ---

    #[tokio::test]
    async fn multi_chunk_large_object_chains_buffered_and_remaining() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = make_id("multi-chunk");

        // Deliver a 2 MiB payload across multiple chunks that individually
        // fit under the threshold but collectively exceed it.
        let chunk_size = 512 * 1024; // 512 KiB per chunk
        let chunk_count = 4; // 4 × 512 KiB = 2 MiB total
        let stream: ClientStream = futures_util::stream::iter(
            (0..chunk_count).map(move |i| Ok(bytes::Bytes::from(vec![i as u8; chunk_size]))),
        )
        .boxed();

        storage
            .put_object(&id, &Default::default(), stream)
            .await
            .unwrap();

        // Should have been routed to long-term (over 1 MiB) at the revision key.
        let lt_id = hv.get(&id).expect_tombstone().target;
        let (_, lt_bytes) = lt.get(&lt_id).expect_object();
        assert_eq!(lt_bytes.len(), chunk_size * chunk_count);

        // Verify data integrity — each chunk's fill byte should appear in order.
        for i in 0..chunk_count {
            let offset = i * chunk_size;
            assert!(
                lt_bytes[offset..offset + chunk_size]
                    .iter()
                    .all(|&b| b == i as u8),
                "data mismatch in chunk {i}"
            );
        }
    }

    // --- OperationState::cleanup_target unit tests ---

    fn make_operation_state(
        new_target: Option<ObjectId>,
        old_target: Option<ObjectId>,
        phase: OperationPhase,
    ) -> OperationState {
        let lt = Arc::new(InMemoryBackend::new("lt"));
        let tracker = BackgroundCounter::new();
        let mut state = OperationState::new(
            Operation {
                new_target,
                old_target,
            },
            lt,
            tracker,
        );
        state.phase = phase;
        state
    }

    #[test]
    fn cleanup_target_write_large_registered_returns_new() {
        let new = make_id("new");
        let state = make_operation_state(Some(new.clone()), None, OperationPhase::Registered);
        assert_eq!(state.cleanup_target(), Some(new));
    }

    #[test]
    fn cleanup_target_write_large_lt_written_returns_new() {
        let new = make_id("new");
        let state = make_operation_state(Some(new.clone()), None, OperationPhase::Written);
        assert_eq!(state.cleanup_target(), Some(new));
    }

    #[test]
    fn cleanup_target_write_large_cas_lost_returns_new() {
        let new = make_id("new");
        let state = make_operation_state(
            Some(new.clone()),
            Some(make_id("old")),
            OperationPhase::Lost,
        );
        assert_eq!(state.cleanup_target(), Some(new));
    }

    #[test]
    fn cleanup_target_write_large_cas_won_returns_old() {
        let old = make_id("old");
        let state = make_operation_state(
            Some(make_id("new")),
            Some(old.clone()),
            OperationPhase::Updated,
        );
        assert_eq!(state.cleanup_target(), Some(old));
    }

    #[test]
    fn cleanup_target_fresh_write_cas_won_returns_none() {
        // WriteLarge (fresh, no old blob) + CasWon → no cleanup needed.
        let state = make_operation_state(Some(make_id("new")), None, OperationPhase::Updated);
        assert_eq!(state.cleanup_target(), None);
    }

    #[test]
    fn cleanup_target_inline_swap_cas_lost_returns_none() {
        // InlineSwap + CasLost → old blob still referenced by winner, no cleanup.
        let state = make_operation_state(None, Some(make_id("old")), OperationPhase::Lost);
        assert_eq!(state.cleanup_target(), None);
    }

    #[test]
    fn cleanup_target_inline_swap_cas_won_returns_old() {
        let old = make_id("old");
        let state = make_operation_state(None, Some(old.clone()), OperationPhase::Updated);
        assert_eq!(state.cleanup_target(), Some(old));
    }

    #[test]
    fn cleanup_target_delete_large_cas_won_returns_old() {
        let old = make_id("old");
        let state = make_operation_state(None, Some(old.clone()), OperationPhase::Updated);
        assert_eq!(state.cleanup_target(), Some(old));
    }

    #[test]
    fn cleanup_target_delete_large_cas_lost_returns_none() {
        // DeleteLarge + CasLost → winner handles old blob, nothing to do.
        let state = make_operation_state(None, Some(make_id("old")), OperationPhase::Lost);
        assert_eq!(state.cleanup_target(), None);
    }

    #[test]
    fn cleanup_target_completed_returns_none() {
        let state = make_operation_state(
            Some(make_id("new")),
            Some(make_id("old")),
            OperationPhase::Completed,
        );
        assert_eq!(state.cleanup_target(), None);
    }

    // --- OperationGuard drop safety tests ---

    /// Dropping a guard outside any tokio runtime must not panic.
    #[test]
    fn guard_dropped_outside_runtime_does_not_panic() {
        let lt = Arc::new(InMemoryBackend::new("lt"));
        let tracker = BackgroundCounter::new();

        // Construct a guard that would schedule cleanup (new_target is Some).
        // No tokio runtime is active in this plain #[test].
        let guard = OperationGuard {
            state: Some(OperationState::new(
                Operation {
                    new_target: Some(make_id("cleanup-target")),
                    old_target: None,
                },
                lt,
                tracker,
            )),
        };

        drop(guard); // Must not panic.
    }

    /// Dropping a guard with no cleanup needed (phase requires no action)
    /// marks the state Completed without spawning a background task.
    #[tokio::test]
    async fn guard_dropped_with_no_cleanup_needed_marks_completed() {
        let lt = Arc::new(InMemoryBackend::new("lt"));
        let tracker = BackgroundCounter::new();

        // InlineSwap + CasLost → no cleanup needed.
        let guard = OperationGuard {
            state: Some(OperationState::new(
                Operation {
                    new_target: None,
                    old_target: Some(make_id("old")),
                },
                lt,
                tracker.clone(),
            )),
        };

        // Advance to CasLost (no cleanup target for this combination).
        let mut guard = guard;
        guard.advance(OperationPhase::Lost);
        drop(guard);

        // Counter must be back to zero immediately (no background task spawned).
        assert_eq!(tracker.inner.outstanding.load(Ordering::Acquire), 0);
    }

    // --- BackgroundCounter drain tests ---

    /// `drain()` returns immediately when there are no outstanding operations.
    #[tokio::test]
    async fn join_returns_immediately_when_empty() {
        let tracker = BackgroundCounter::new();
        tokio::time::timeout(
            Duration::from_millis(100),
            tracker.join(Duration::from_secs(5)),
        )
        .await
        .expect("drain should return immediately with no outstanding ops");
    }

    /// `drain()` waits until all outstanding operations complete.
    #[tokio::test]
    async fn join_waits_for_outstanding_operations() {
        let tracker = BackgroundCounter::new();
        tracker.increment();

        // Spawn a task that decrements after a brief delay.
        let tracker_clone = tracker.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tracker_clone.decrement();
        });

        // drain() should wait for the decrement before returning.
        tokio::time::timeout(Duration::from_secs(1), tracker.join(Duration::from_secs(5)))
            .await
            .expect("drain should complete after decrement");

        assert_eq!(tracker.inner.outstanding.load(Ordering::Acquire), 0);
    }

    /// `drain()` returns after the timeout even if operations are still outstanding.
    #[tokio::test]
    async fn join_returns_after_timeout_with_remaining() {
        let tracker = BackgroundCounter::new();
        tracker.increment(); // Never decremented.

        // Use a very short timeout.
        let short = Duration::from_millis(50);
        tokio::time::timeout(Duration::from_secs(1), tracker.join(short))
            .await
            .expect("drain should return after timeout");

        // Outstanding count still non-zero — operation was never completed.
        assert_eq!(tracker.inner.outstanding.load(Ordering::Acquire), 1);

        // Clean up to avoid poisoning other tests.
        tracker.decrement();
    }
}
