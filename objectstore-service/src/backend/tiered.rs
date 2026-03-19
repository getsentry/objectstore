//! Two-tier storage backend with size-based routing and redirect tombstones.
//!
//! [`TieredStorage`] routes objects to a high-volume or long-term backend based
//! on size and maintains redirect tombstones so that reads never need to probe
//! both backends. See the [crate-level documentation](crate) for details.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;
use serde::{Deserialize, Serialize};

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
/// - Objects **≤ 1 MiB** go to the `high_volume_backend` — optimized for low-latency reads
///   and writes of small objects (e.g. BigTable).
/// - Objects **> 1 MiB** go to the `long_term_backend` — optimized for cost-efficient
///   storage of large objects (e.g. GCS).
///
/// # Redirect Tombstones
///
/// Because the [`ObjectId`] is backend-independent, reads must be able to find an object without
/// knowing which backend stores it. A naive approach would check the long-term backend on every
/// read miss in the high-volume backend — but that is slow and expensive.
///
/// Instead, when an object is stored in the long-term backend, a **redirect tombstone** is
/// written in the high-volume backend. It acts as a signpost: "the real data lives in the
/// other backend." How tombstones are physically stored is determined by the
/// [`HighVolumeBackend`] implementation — refer to the backend's own documentation for
/// storage format details.
///
/// # Consistency Without Locks
///
/// The tombstone system maintains consistency through operation ordering rather than
/// distributed locks. The invariant is: a redirect tombstone is always the **last thing
/// written** and the **last thing removed**.
///
/// - On **write**, the real object is persisted before the tombstone. If the tombstone write
///   fails, the real object is rolled back.
/// - On **delete**, the real object is removed before the tombstone. If the long-term delete
///   fails, the tombstone remains and the data stays reachable.
///
/// See the individual methods for per-operation tombstone behavior.
///
/// # Usage
///
/// `TieredStorage` handles only the routing and consistency logic. Wrap it in a
/// [`StorageService`](crate::service::StorageService) to add task spawning, panic isolation,
/// and concurrency limiting.
#[derive(Debug)]
pub struct TieredStorage {
    /// The backend for small objects (≤ 1 MiB).
    high_volume: Box<dyn HighVolumeBackend>,
    /// The backend for large objects (> 1 MiB).
    long_term: Box<dyn Backend>,
}

impl TieredStorage {
    /// Creates a new `TieredStorage` with the given backends.
    pub fn new(high_volume: Box<dyn HighVolumeBackend>, long_term: Box<dyn Backend>) -> Self {
        Self {
            high_volume,
            long_term,
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
    /// If a tombstone already exists, attempts to swap it for the new object and delete the old
    /// long-term object.
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
            // No tombstone exists - write succeeded
            return Ok(());
        };

        // Tombstone exists — Swap it for inline data
        let write = TieredWrite::Object(metadata.clone(), payload.clone());
        let written = self
            .high_volume
            .compare_and_write(id, Some(&target), write)
            .await?;

        // TODO: Schedule cleanups into background to ensure eventual cleanup
        if written {
            let _ = self.long_term.delete_object(&target).await;
        }

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
        let current = match self.high_volume.get_tiered_metadata(id).await? {
            TieredMetadata::Tombstone(t) => Some(t.target),
            _ => None,
        };

        // 2. Write payload to long-term at a unique revision key.
        let new = new_long_term_revision(id);
        self.long_term.put_object(&new, metadata, stream).await?;

        // 3. CAS commit: write tombstone only if HV state matches what we saw.
        let tombstone = Tombstone {
            target: new.clone(),
            expiration_policy: metadata.expiration_policy,
        };
        let written = self
            .high_volume
            .compare_and_write(id, current.as_ref(), TieredWrite::Tombstone(tombstone))
            .await;

        // TODO: Schedule cleanups into background to ensure eventual cleanup
        match written {
            Ok(true) => {
                // Tombstone committed. Clean up old GCS blob if overwriting.
                if let Some(current) = current {
                    let _ = self.long_term.delete_object(&current).await;
                }
            }
            Ok(false) => {
                // Someone else won the race. Clean up our GCS blob.
                let _ = self.long_term.delete_object(&new).await;
                // Return OK — from the caller's perspective, a write happened.
            }
            Err(e) => {
                // CAS error. Clean up our GCS blob before propagating.
                let _ = self.long_term.delete_object(&new).await;
                return Err(e);
            }
        }

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
            self.put_high_volume(id, metadata, payload.clone()).await?;
            (BackendChoice::HighVolume, payload.len() as u64)
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
                tracing::warn!(backend_type, "Missing object size");
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

            // Delete the tombstone first, then clean up GCS.
            let deleted = self
                .high_volume
                .compare_and_write(id, Some(&tombstone.target), TieredWrite::Delete)
                .await?;

            // TODO: Schedule cleanups into background to ensure eventual cleanup
            if deleted {
                let _ = self.long_term.delete_object(&tombstone.target).await;
            }
        }

        objectstore_metrics::record!(
            "delete.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
            backend_type = self.backend_type(&backend_choice),
        );

        Ok(())
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

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;
    use crate::id::ObjectContext;
    use crate::stream::{self};

    // --- revision_id tests ---

    #[test]
    fn revision_id_preserves_context() {
        let id = ObjectId {
            context: ObjectContext {
                usecase: "testing".to_string(),
                scopes: Scopes::from_iter([Scope::create("org", "17").unwrap()]),
            },
            key: "my-key".to_string(),
        };

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
        use crate::id::ObjectId;
        let id = ObjectId {
            context: ObjectContext {
                usecase: "attachments".to_string(),
                scopes: Scopes::from_iter([Scope::create("org", "42").unwrap()]),
            },
            key: "original".to_string(),
        };

        let revised = new_long_term_revision(&id);
        let path = revised.as_storage_path().to_string();
        let parsed = ObjectId::from_storage_path(&path)
            .unwrap_or_else(|| panic!("failed to parse '{path}'"));
        assert_eq!(parsed, revised);
    }

    #[test]
    fn revision_id_is_unique() {
        let id = ObjectId {
            context: ObjectContext {
                usecase: "testing".to_string(),
                scopes: Scopes::empty(),
            },
            key: "base-key".to_string(),
        };

        let a = new_long_term_revision(&id);
        let b = new_long_term_revision(&id);
        assert_ne!(a.key, b.key, "two calls should produce different keys");
    }

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_tiered_storage() -> (TieredStorage, InMemoryBackend, InMemoryBackend) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt.clone()));
        (storage, hv, lt)
    }

    // --- Basic behavior ---

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "does-not-exist".into());

        assert!(storage.get_object(&id).await.unwrap().is_none());
        assert!(storage.get_metadata(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_succeeds() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "does-not-exist".into());

        storage.delete_object(&id).await.unwrap();
    }

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let id = ObjectId::random(make_context());

        storage
            .put_object(&id, &Default::default(), stream::single("auto-keyed"))
            .await
            .unwrap();

        let (_, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = s.try_collect().await.unwrap();
        assert_eq!(body.as_ref(), b"auto-keyed");
    }

    // --- Size-based routing tests ---

    #[tokio::test]
    async fn small_object_goes_to_high_volume() {
        let (storage, hv, lt) = make_tiered_storage();
        let payload = vec![0u8; 100]; // 100 bytes, well under 1 MiB
        let id = ObjectId::new(make_context(), "small".into());

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        assert!(hv.contains(&id), "expected in high-volume");
        assert!(!lt.contains(&id), "leaked to long-term");
    }

    #[tokio::test]
    async fn large_object_goes_to_long_term_with_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();
        let payload_len = 2 * 1024 * 1024; // 2 MiB, over threshold
        let payload = vec![0xABu8; payload_len];
        let id = ObjectId::new(make_context(), "large".into());

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // A redirect tombstone should exist in high-volume pointing to a revision key.
        let tombstone = hv.get(&id).expect_tombstone();
        let lt_id = tombstone.target;
        assert!(
            lt_id.key().starts_with(id.key()),
            "tombstone target key should be a revision of the HV key"
        );

        // Real payload should be in long-term at the revision key.
        let (_, lt_bytes) = lt.get(&lt_id).expect_object();
        assert_eq!(lt_bytes.len(), payload_len);
    }

    #[tokio::test]
    async fn reinsert_small_over_large_swaps_to_inline() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "reinsert-key".into());

        // First: insert a large object → creates tombstone in hv, payload in lt at lt_id
        let large_payload = vec![0xABu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(large_payload))
            .await
            .unwrap();

        let lt_id = hv.get(&id).expect_tombstone().target;

        // Re-insert a SMALL payload with the same key.
        // The CAS-swap puts the small object inline in HV and cleans up the old GCS blob.
        let small_payload = vec![0xCDu8; 100]; // well under 1 MiB threshold
        storage
            .put_object(&id, &Default::default(), stream::single(small_payload))
            .await
            .unwrap();

        // The small object is now inline in high-volume.
        hv.get(&id).expect_object();

        // The old long-term blob was cleaned up.
        lt.get(&lt_id).expect_not_found();
    }

    #[tokio::test]
    async fn tombstone_inherits_expiration_policy() {
        let (storage, hv, lt) = make_tiered_storage();

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            origin: Some("10.0.0.1".into()),
            ..Default::default()
        };
        let payload = vec![0u8; 2 * 1024 * 1024]; // force long-term
        let id = ObjectId::new(make_context(), "expiry-test".into());

        storage
            .put_object(&id, &metadata_in, stream::single(payload))
            .await
            .unwrap();

        // The tombstone in hv should have expiration_policy inherited.
        // The target is a unique revision key, not `id` itself.
        let tombstone = hv.get(&id).expect_tombstone();
        assert_eq!(tombstone.expiration_policy, metadata_in.expiration_policy);
        let lt_id = tombstone.target.clone();
        assert_ne!(
            lt_id, id,
            "tombstone target must be a unique revision, not the HV id"
        );
        assert!(
            lt_id.key().starts_with(id.key()),
            "revision key should have original key as prefix"
        );

        // The long-term object should be at the revision key with the full metadata.
        let (lt_meta, _) = lt.get(&lt_id).expect_object();
        assert_eq!(lt_meta.content_type, "image/png");
        assert_eq!(lt_meta.expiration_policy, metadata_in.expiration_policy);
    }

    // --- Tombstone redirect tests ---

    #[tokio::test]
    async fn reads_follow_tombstone_redirect() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB, over threshold

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let id = ObjectId::new(make_context(), "redirect-read".into());

        storage
            .put_object(&id, &metadata_in, stream::single(payload.clone()))
            .await
            .unwrap();

        // get_object should transparently follow the tombstone
        let (_, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body = stream::read_to_vec(s).await.unwrap();
        assert_eq!(body, payload);

        // get_metadata should also follow the tombstone
        let metadata = storage.get_metadata(&id).await.unwrap().unwrap();
        assert_eq!(metadata.content_type, "image/png");
    }

    // --- Tombstone inconsistency tests ---

    /// A backend where `cas_put` always fails, but all other operations work normally.
    #[derive(Debug)]
    struct FailingTombstoneBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl Backend for FailingTombstoneBackend {
        fn name(&self) -> &'static str {
            "failing-tombstone"
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
    impl HighVolumeBackend for FailingTombstoneBackend {
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
                "simulated tombstone write failure",
            )))
        }
    }

    /// If the tombstone write to the high-volume backend fails after the long-term
    /// write succeeds, the long-term object must be cleaned up so we never leave
    /// an unreachable orphan in long-term storage.
    #[tokio::test]
    async fn no_orphan_when_tombstone_write_fails() {
        let lt = InMemoryBackend::new("lt");
        let hv = FailingTombstoneBackend(InMemoryBackend::new("hv"));
        let storage = TieredStorage::new(Box::new(hv), Box::new(lt.clone()));

        let id = ObjectId::new(make_context(), "orphan-test".into());
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> long-term path
        let result = storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await;

        assert!(result.is_err());
        assert!(lt.is_empty(), "long-term object not cleaned up");
    }

    /// If a tombstone exists in high-volume but the corresponding object is
    /// missing from long-term storage (e.g. due to a race condition or partial
    /// cleanup), reads should gracefully return None rather than error.
    #[tokio::test]
    async fn orphan_tombstone_returns_none() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "orphan-tombstone".into());
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

    // --- Delete tests ---

    #[tokio::test]
    async fn delete_cleans_up_both_backends() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "delete-both".into());
        let payload = vec![0u8; 2 * 1024 * 1024]; // 2 MiB

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // Capture lt_id before deleting (it lives at the revision key, not at id).
        let lt_id = hv.get(&id).expect_tombstone().target;

        storage.delete_object(&id).await.unwrap();

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

        let id = ObjectId::new(make_context(), "fail-delete".into());
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> goes to long-term
        storage
            .put_object(&id, &Default::default(), stream::single(payload.clone()))
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

    // --- Redirect target tests ---

    /// A tombstone carrying an explicit `target` is followed correctly on reads and deletes,
    /// including when the target ObjectId differs from the HV ObjectId.
    #[tokio::test]
    async fn tombstone_target_is_used_for_reads_and_deletes() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = TieredStorage::new(Box::new(hv.clone()), Box::new(lt.clone()));

        let hv_id = ObjectId::new(make_context(), "hv-key".into());
        let lt_id = ObjectId::new(make_context(), "lt-key".into());
        let payload = vec![0xABu8; 100];

        // Write the object under the LT id and a tombstone pointing to it from HV.
        lt.put_object(&lt_id, &Default::default(), stream::single(payload.clone()))
            .await
            .unwrap();
        let tombstone = Tombstone {
            target: lt_id.clone(),
            expiration_policy: objectstore_types::metadata::ExpirationPolicy::Manual,
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
        assert!(!hv.contains(&hv_id), "tombstone should be removed");
        assert!(!lt.contains(&lt_id), "lt object should be removed");
    }

    // --- Multi-chunk streaming tests ---

    #[tokio::test]
    async fn multi_chunk_large_object_chains_buffered_and_remaining() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "multi-chunk".into());

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
}
