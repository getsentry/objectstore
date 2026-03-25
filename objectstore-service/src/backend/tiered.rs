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
//! point), and cleaning up the now-unreferenced LT blob in the background:
//!
//! ### Large-Object Write (> 1 MiB)
//!
//! 1. **Read HV** to capture the current revision (existing tombstone target,
//!    or absent).
//! 2. **Write payload to LT** at a unique revision key.
//! 3. **Compare-and-swap in HV**: write a tombstone pointing to the new
//!    revision, only if the current revision still matches step 1.
//!    - **OK** — schedule background deletion of the old LT blob, if any.
//!    - **Conflict** — another writer won the race; schedule background deletion
//!      of our new LT blob.
//!    - **Error** — reload the tombstone and delete the unreferenced blob or
//!      blobs.
//!
//! ### Small-Object Write (≤ 1 MiB)
//!
//! 1. **Write inline to HV**, skipping the write if a tombstone is present.
//!    - **OK** — done; the object is stored entirely in HV.
//!    - **Tombstone present** — a large object already occupies this key;
//!      continue:
//! 2. **Compare-and-swap in HV**: replace the tombstone with inline data, only
//!    if the tombstone's revision still matches.
//!    - **OK** — schedule background deletion of the old LT blob.
//!    - **Conflict** — another writer won the race; they will clean up the
//!      LT blob and we have no new LT blob to clean up.
//!    - **Error** — reload the tombstone and delete the unreferenced blob if
//!      the write went through.
//!
//! ### Delete
//!
//! 1. **Delete from HV** if the entry is not a tombstone.
//!    - **OK** — done; there is no LT data to clean up.
//!    - **Tombstone present** — a large object is stored here; continue:
//! 2. **Compare-and-swap in HV**: remove the tombstone, only if its revision
//!    still matches.
//!    - **OK** — schedule background deletion of the LT blob.
//!    - **Conflict** — another writer won the race; they will clean up.
//!    - **Error** — reload the tombstone and delete the unreferenced blob if
//!      the write went through.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;
use serde::{Deserialize, Serialize};

use crate::backend::changelog::{Change, ChangeGuard, ChangeLog, ChangeManager, ChangePhase};
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
/// is still current — rolling back on conflict. Cleanup of unreferenced LT
/// blobs runs in background tasks so the caller returns as soon as the commit
/// point is reached. Call [`Backend::join`] during shutdown to wait for
/// outstanding cleanup.
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
    inner: Arc<ChangeManager>,
}

impl TieredStorage {
    /// Creates a new `TieredStorage` with the given backends and change log.
    pub fn new(
        high_volume: Box<dyn HighVolumeBackend>,
        long_term: Box<dyn Backend>,
        changelog: Box<dyn ChangeLog>,
    ) -> Self {
        let inner = ChangeManager::new(high_volume, long_term, changelog);
        // Note on cancellation: Our `join` method will wait for all tasks tracked by the spawned
        // recovery job, so we defer shutdown until recovery is complete or times out.
        tokio::spawn(inner.clone().recover());
        Self { inner }
    }

    /// Records the change to the log and returns a guard that cleans up on drop.
    async fn record_change(&self, change: Change) -> Result<ChangeGuard> {
        self.inner.clone().record(change).await
    }

    /// Returns the name of the backend corresponding to the given routing choice.
    fn backend_type(&self, choice: &BackendChoice) -> &'static str {
        match choice {
            BackendChoice::HighVolume => self.inner.high_volume.name(),
            BackendChoice::LongTerm => self.inner.long_term.name(),
        }
    }

    /// Puts an object into the high-volume backend.
    ///
    /// If a tombstone already exists, attempts to swap it for the new object and delete the old
    /// long-term object.
    async fn put_high_volume(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<()> {
        let tombstone_opt = self
            .inner
            .high_volume
            .put_non_tombstone(id, metadata, payload.clone())
            .await?;

        let Some(Tombstone { target, .. }) = tombstone_opt else {
            // No tombstone exists - write succeeded
            return Ok(());
        };

        // Tombstone exists — Swap it for inline data
        let mut guard = self
            .record_change(Change {
                id: id.clone(),
                new: None,
                old: Some(target.clone()),
            })
            .await?;

        let write = TieredWrite::Object(metadata.clone(), payload);
        guard.advance(ChangePhase::Written);

        let written = self
            .inner
            .high_volume
            .compare_and_write(id, Some(&target), write)
            .await?;

        // Update guard and let it schedule cleanup in the background.
        guard.advance(ChangePhase::compare_and_write(written));

        Ok(())
    }

    /// Puts an object into the long-term backend with a redirect tombstone in front.
    ///
    /// Deletes the previous long-term object if overwriting an existing tombstone. If the tombstone
    /// write fails, the new long-term object is cleaned up.
    async fn put_long_term(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<()> {
        // 1. Read current HV revision to establish the write precondition
        let current = match self.inner.high_volume.get_tiered_metadata(id).await? {
            TieredMetadata::Tombstone(t) => Some(t.target),
            _ => None,
        };

        // 2. Write payload to long-term at a unique revision key.
        let new = new_long_term_revision(id);
        let mut guard = self
            .record_change(Change {
                id: id.clone(),
                new: Some(new.clone()),
                old: current.clone(),
            })
            .await?;

        self.inner
            .long_term
            .put_object(&new, metadata, stream)
            .await?;
        guard.advance(ChangePhase::Written);

        // 3. CAS commit: write tombstone only if HV state matches what we saw.
        let tombstone = Tombstone {
            target: new.clone(),
            expiration_policy: metadata.expiration_policy,
        };
        let written = self
            .inner
            .high_volume
            .compare_and_write(id, current.as_ref(), TieredWrite::Tombstone(tombstone))
            .await?;

        // Update guard and let it schedule cleanup in the background.
        guard.advance(ChangePhase::compare_and_write(written));

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

        let hv_result = self.inner.high_volume.get_tiered_object(id).await?;
        let (result, backend_choice) = match hv_result {
            TieredGet::NotFound => (None, BackendChoice::HighVolume),
            TieredGet::Object(metadata, stream) => {
                (Some((metadata, stream)), BackendChoice::HighVolume)
            }
            TieredGet::Tombstone(tombstone) => (
                self.inner.long_term.get_object(&tombstone.target).await?,
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

        let hv_result = self.inner.high_volume.get_tiered_metadata(id).await?;
        let (result, backend_choice) = match hv_result {
            TieredMetadata::NotFound => (None, BackendChoice::HighVolume),
            TieredMetadata::Object(metadata) => (Some(metadata), BackendChoice::HighVolume),
            TieredMetadata::Tombstone(tombstone) => (
                self.inner.long_term.get_metadata(&tombstone.target).await?,
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

        if let Some(tombstone) = self.inner.high_volume.delete_non_tombstone(id).await? {
            backend_choice = BackendChoice::LongTerm;

            let mut guard = self
                .record_change(Change {
                    id: id.clone(),
                    new: None,
                    old: Some(tombstone.target.clone()),
                })
                .await?;
            guard.advance(ChangePhase::Written);

            // Remove the tombstone; the LT blob becomes unreachable at this point.
            let deleted = self
                .inner
                .high_volume
                .compare_and_write(id, Some(&tombstone.target), TieredWrite::Delete)
                .await?;

            // Update guard and let it schedule cleanup in the background.
            guard.advance(ChangePhase::compare_and_write(deleted));
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
        self.inner.tracker.close();
        tokio::join!(
            self.inner.high_volume.join(),
            self.inner.long_term.join(),
            self.inner.tracker.wait()
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

    use objectstore_types::metadata::{ExpirationPolicy, Metadata};
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::changelog::{InMemoryChangeLog, NoopChangeLog};
    use crate::backend::in_memory::InMemoryBackend;
    use crate::backend::testing::{Hooks, TestBackend};
    use crate::error::Error;
    use crate::id::ObjectContext;
    use crate::stream::{self, ClientStream};

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_id(key: &str) -> ObjectId {
        ObjectId::new(make_context(), key.into())
    }

    fn make_tiered_storage() -> (
        TieredStorage,
        InMemoryBackend,
        InMemoryBackend,
        InMemoryChangeLog,
    ) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        let changelog = InMemoryChangeLog::default();
        let storage = TieredStorage::new(
            Box::new(hv.clone()),
            Box::new(lt.clone()),
            Box::new(changelog.clone()),
        );
        (storage, hv, lt, changelog)
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
        let (storage, _hv, _lt, _) = make_tiered_storage();
        let id = make_id("does-not-exist");

        assert!(storage.get_object(&id).await.unwrap().is_none());
        assert!(storage.get_metadata(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_succeeds() {
        let (storage, _hv, _lt, _) = make_tiered_storage();
        let id = make_id("does-not-exist");

        storage.delete_object(&id).await.unwrap();
    }

    // --- Put routing ---

    #[tokio::test]
    async fn put_small_object_stores_inline() {
        let (storage, hv, lt, _) = make_tiered_storage();
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
        let (storage, hv, lt, _) = make_tiered_storage();
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
        let (storage, hv, lt, _) = make_tiered_storage();
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
        let (storage, hv, lt, _) = make_tiered_storage();
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
        let (storage, hv, _lt, _) = make_tiered_storage();
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
        let (storage, hv, lt, _) = make_tiered_storage();
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

    #[derive(Debug)]
    struct FailDelete;

    #[async_trait::async_trait]
    impl Hooks for FailDelete {
        async fn delete_object(
            &self,
            _inner: &InMemoryBackend,
            _id: &ObjectId,
        ) -> Result<DeleteResponse> {
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
        let lt = TestBackend::new(FailDelete);
        let log = NoopChangeLog;
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt), Box::new(log));

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

    #[derive(Debug)]
    struct CasConflict;

    #[async_trait::async_trait]
    impl Hooks for CasConflict {
        async fn compare_and_write(
            &self,
            _inner: &InMemoryBackend,
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
        let hv = TestBackend::new(CasConflict);
        let lt = InMemoryBackend::new("lt");
        let log = NoopChangeLog;
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt.clone()), Box::new(log));

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
        let hv = TestBackend::with_inner(inner, CasConflict);
        let log = NoopChangeLog;
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt), Box::new(log));

        // Writing a small object over a tombstone should succeed even when CAS
        // conflicts — the other writer's write is accepted.
        storage
            .put_object(&id, &Default::default(), stream::single("tiny"))
            .await
            .unwrap();
    }

    // --- Failure / inconsistency ---

    /// Simulates compare_and_write failure. If `true`, it fails after commit.
    #[derive(Debug)]
    struct FailCas(bool);

    #[async_trait::async_trait]
    impl Hooks for FailCas {
        async fn compare_and_write(
            &self,
            inner: &InMemoryBackend,
            id: &ObjectId,
            current: Option<&ObjectId>,
            write: TieredWrite,
        ) -> Result<bool> {
            if self.0 {
                // simulate a network error _after_ commit went through
                inner.compare_and_write(id, current, write).await?;
            }
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
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
        let hv = TestBackend::new(FailCas(false));
        let log = NoopChangeLog;
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt.clone()), Box::new(log));

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
        let (storage, hv, lt, _) = make_tiered_storage();
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
        let log = NoopChangeLog;
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt.clone()), Box::new(log));

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
        let (storage, hv, lt, _) = make_tiered_storage();
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

    // --- Written-phase cleanup ---

    /// When a large-object overwrite commits in HV but its response is lost, the guard drops in
    /// `Written` phase. Cleanup must read HV to determine the CAS outcome, then delete whichever
    /// LT blob is no longer referenced — here the old one, since the new tombstone committed.
    #[tokio::test]
    async fn written_cleanup_after_lost_cas_response() {
        let (storage, hv, lt, log) = make_tiered_storage();
        let id = make_id("obj");

        // First put: establishes tombstone
        let payload = vec![0xAAu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap();
        let tombstone1 = hv.get(&id).expect_tombstone().target;

        // Second put: Updates tombstone but fails immediately after committing
        let broken_storage = TieredStorage::new(
            Box::new(TestBackend::with_inner(hv.clone(), FailCas(true))),
            Box::new(lt.clone()),
            Box::new(log.clone()),
        );
        broken_storage
            .put_object(&id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap_err(); // must fail
        let tombstone2 = hv.get(&id).expect_tombstone().target;
        assert_ne!(tombstone1, tombstone2);

        // The first tombstone's target should be cleaned up, but the second should remain.
        broken_storage.join().await;
        lt.get(&tombstone1).expect_not_found();
        lt.get(&tombstone2).expect_object();

        // Now delete the new object with the same tombstone failure
        broken_storage.delete_object(&id).await.unwrap_err();
        hv.get(&id).expect_not_found();
        broken_storage.join().await;
        lt.get(&tombstone2).expect_not_found();

        // Create a fresh large object
        let id = make_id("obj2");
        storage
            .put_object(&id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap();
        let tombstone3 = hv.get(&id).expect_tombstone().target;

        // Overwrite it with a small object and check again for cleanup
        broken_storage
            .put_object(&id, &Default::default(), stream::single(&b"small"[..]))
            .await
            .unwrap_err(); // must fail
        hv.get(&id).expect_object();
        broken_storage.join().await;
        lt.get(&tombstone3).expect_not_found();
    }

    // --- ChangeGuard drop safety tests ---

    /// Dropping a guard outside any tokio runtime must not panic.
    #[test]
    fn guard_dropped_outside_runtime_does_not_panic() {
        let manager = ChangeManager::new(
            Box::new(InMemoryBackend::new("hv")),
            Box::new(InMemoryBackend::new("lt")),
            Box::new(NoopChangeLog),
        );

        let change = Change {
            id: make_id("object-key"),
            new: Some(make_id("cleanup-target")),
            old: None,
        };

        // Build the guard inside a temporary runtime, then let the runtime drop
        // so that no tokio context is active when the guard drops.
        let guard = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(manager.record(change)).unwrap()
        };

        drop(guard); // Must not panic.
    }

    /// `join` blocks until all in-flight guards have completed cleanup.
    ///
    /// Time is advanced manually so the test runs at virtual speed. The guard
    /// completes after 10 s; `join` must still be waiting at 9 s and done by 11 s.
    #[tokio::test(start_paused = true)]
    async fn join_waits_for_cleanup_to_complete() {
        let (storage, _hv, _lt, _) = make_tiered_storage();
        let change = Change {
            id: make_id("object-key"),
            new: None,
            old: None,
        };
        let mut guard = storage.record_change(change).await.unwrap();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            guard.advance(ChangePhase::Completed);
            drop(guard);
        });

        let join_future = tokio::spawn(async move { storage.join().await });

        tokio::time::sleep(Duration::from_secs(9)).await;
        assert!(!join_future.is_finished(), "finished before guard dropped");

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(join_future.is_finished(), "finish after guard drops");
    }

    // --- Changelog integration tests ---

    /// LT backend hook that completes the write, then pauses until resumed.
    ///
    /// Lets tests cancel the owning future after the blob is committed but
    /// before the HV tombstone is set.
    #[derive(Clone, Debug)]
    struct PauseAfterPut {
        paused: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Hooks for PauseAfterPut {
        async fn put_object(
            &self,
            inner: &InMemoryBackend,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            inner.put_object(id, metadata, stream).await?;
            self.paused.notify_one();
            self.resume.notified().await;
            Ok(())
        }
    }

    /// When a future is cancelled after the LT write but before the HV tombstone is set,
    /// the `ChangeGuard` cleans up the orphaned LT blob and removes the log entry.
    #[tokio::test]
    async fn dropped_future_triggers_cleanup_and_log_entry_removed() {
        let paused = Arc::new(tokio::sync::Notify::new());
        let hooks = PauseAfterPut {
            paused: Arc::clone(&paused),
            resume: Arc::new(tokio::sync::Notify::new()),
        };

        let lt_inner = InMemoryBackend::new("lt");
        let log = InMemoryChangeLog::default();
        let storage = TieredStorage::new(
            Box::new(InMemoryBackend::new("hv")),
            Box::new(TestBackend::with_inner(lt_inner.clone(), hooks)),
            Box::new(log.clone()),
        );

        let id = make_id("drop-test");
        let metadata = Metadata::default();
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB → long-term path

        // Drive the put until the LT write commits, then cancel before the HV tombstone is set.
        tokio::select! {
            result = storage.put_object(&id, &metadata, stream::single(payload)) => {
                panic!("expected put to pause before completing, got: {result:?}");
            }
            _ = paused.notified() => {
                // LT blob stored; cancelling drops the guard in Recorded phase.
            }
        }

        // ChangeGuard dropped → background cleanup task spawned; wait for it.
        storage.join().await;

        // The orphaned LT blob must have been deleted.
        assert!(lt_inner.is_empty(), "orphaned LT blob was not cleaned up");

        // The log entry must be gone once cleanup completes.
        let entries = log.scan().await.unwrap();
        assert!(
            entries.is_empty(),
            "changelog entry not removed after cleanup"
        );
    }

}
