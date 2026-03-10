//! Two-tier storage backend with size-based routing and redirect tombstones.
//!
//! [`TieredStorage`] routes objects to a high-volume or long-term backend based
//! on size and maintains redirect tombstones so that reads never need to probe
//! both backends. See the [crate-level documentation](crate) for details.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::backend::common::{BoxedBackend, DeleteOutcome, WriteOutcome};
use crate::error::Result;
use crate::id::{ObjectContext, ObjectId};
use crate::service::{DeleteResponse, GetResponse, InsertResponse, MetadataResponse};
use crate::stream::SizedPeek;

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

#[derive(Clone, Copy)]
enum BackendChoice {
    HighVolume,
    LongTerm,
}

impl std::fmt::Display for BackendChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BackendChoice::HighVolume => "high-volume",
            BackendChoice::LongTerm => "long-term",
        };
        f.write_str(s)
    }
}

/// Two-tier storage that routes objects by size.
///
/// Objects smaller than 1 MiB go to the high-volume backend; larger objects go
/// to the long-term backend with a redirect tombstone in the high-volume
/// backend. See [`StorageService`](crate::service::StorageService) for the
/// public API that wraps this with task spawning and panic isolation.
#[derive(Debug)]
pub(crate) struct TieredStorage {
    pub(crate) high_volume_backend: BoxedBackend,
    pub(crate) long_term_backend: BoxedBackend,
}

impl TieredStorage {
    pub(crate) async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> Result<InsertResponse> {
        if metadata.origin.is_none() {
            objectstore_metrics::counter!(
                "put.origin_missing": 1,
                "usecase" => context.usecase.as_str()
            );
        }

        let start = Instant::now();

        let peeked = SizedPeek::new(stream, BACKEND_SIZE_THRESHOLD).await?;
        let initial_backend = if peeked.is_exhausted() {
            BackendChoice::HighVolume
        } else {
            BackendChoice::LongTerm
        };

        objectstore_metrics::distribution!(
            "put.first_chunk.latency"@s: start.elapsed(),
            "usecase" => context.usecase.as_str(),
            "backend_choice" => initial_backend,
        );

        let id = ObjectId::optional(context, key);

        let (final_backend_choice, backend_ty, stored_size) = match initial_backend {
            BackendChoice::HighVolume => {
                // All data fits in the peek buffer. Extract the bytes so they can
                // be re-streamed to long-term storage if HV rejects with a tombstone.
                let stored_size = peeked.len() as u64;
                let bytes = peeked.into_bytes().await?;

                let outcome = self
                    .high_volume_backend
                    .put_non_tombstone(
                        &id,
                        metadata,
                        futures_util::stream::once(std::future::ready(Ok(bytes.clone()))).boxed(),
                    )
                    .await?;

                match outcome {
                    WriteOutcome::Written => (
                        BackendChoice::HighVolume,
                        self.high_volume_backend.name(),
                        stored_size,
                    ),
                    WriteOutcome::Tombstone => {
                        // A tombstone already exists in HV; write the data directly to
                        // long-term storage. No need to write the tombstone again.
                        self.long_term_backend
                            .put_object(
                                &id,
                                metadata,
                                futures_util::stream::once(std::future::ready(Ok(bytes))).boxed(),
                            )
                            .await?;
                        (
                            BackendChoice::LongTerm,
                            self.long_term_backend.name(),
                            stored_size,
                        )
                    }
                }
            }
            BackendChoice::LongTerm => {
                let stored_size = Arc::new(AtomicU64::new(0));
                let lt_stream = peeked
                    .into_stream()
                    .inspect({
                        let stored_size = Arc::clone(&stored_size);
                        move |res| {
                            if let Ok(chunk) = res {
                                stored_size.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                            }
                        }
                    })
                    .boxed();

                // Write the tombstone to HV first. If this fails, no data is written
                // to either backend — the operation fails cleanly with no orphan.
                let redirect_metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: metadata.expiration_policy,
                    ..Default::default()
                };
                self.high_volume_backend
                    .put_object(
                        &id,
                        &redirect_metadata,
                        futures_util::stream::empty().boxed(),
                    )
                    .await?;

                // Write data to long-term storage. On failure, an OrphanHV
                // remains — tombstone in HV, no data in LT. Reads return None;
                // deletes and re-inserts clean it up.
                self.long_term_backend
                    .put_object(&id, metadata, lt_stream)
                    .await?;

                (
                    BackendChoice::LongTerm,
                    self.long_term_backend.name(),
                    stored_size.load(Ordering::Acquire),
                )
            }
        };

        objectstore_metrics::distribution!(
            "put.latency"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => final_backend_choice,
            "backend_type" => backend_ty
        );
        objectstore_metrics::distribution!(
            "put.size"@b: stored_size,
            "usecase" => id.usecase(),
            "backend_choice" => final_backend_choice,
            "backend_type" => backend_ty
        );

        Ok(id)
    }

    pub(crate) async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();
        let mut result = self.high_volume_backend.get_metadata(id).await?;

        if result.as_ref().is_some_and(|m| m.is_tombstone()) {
            result = self.long_term_backend.get_metadata(id).await?;
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
        }

        objectstore_metrics::distribution!(
            "head.latency"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        Ok(result)
    }

    pub(crate) async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();
        let mut result = self.high_volume_backend.get_object(id).await?;

        if result.is_tombstone() {
            result = self.long_term_backend.get_object(id).await?;
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
        }

        objectstore_metrics::distribution!(
            "get.latency.pre-response"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        if let Some((metadata, _stream)) = &result {
            if let Some(size) = metadata.size {
                objectstore_metrics::distribution!(
                    "get.size"@b: size,
                    "usecase" => id.usecase(),
                    "backend_choice" => backend_choice,
                    "backend_type" => backend_type
                );
            } else {
                tracing::warn!(?backend_type, "Missing object size");
            }
        }

        Ok(result)
    }

    pub(crate) async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();

        let outcome = self.high_volume_backend.delete_non_tombstone(id).await?;
        if outcome == DeleteOutcome::Tombstone {
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
            // Delete the long-term object first, then clean up the tombstone.
            // This ordering ensures that if the long-term delete fails, the
            // tombstone remains and the data is still reachable (not orphaned).
            self.long_term_backend.delete_object(id).await?;
            self.high_volume_backend.delete_object(id).await?;
        }

        objectstore_metrics::distribution!(
            "delete.latency"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        Ok(())
    }
}

trait GetResponseExt {
    fn is_tombstone(&self) -> bool;
}

impl GetResponseExt for GetResponse {
    fn is_tombstone(&self) -> bool {
        self.as_ref().is_some_and(|(m, _)| m.is_tombstone())
    }
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
    use crate::stream::make_stream;

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_tiered_storage() -> (TieredStorage, InMemoryBackend, InMemoryBackend) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        let storage = TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(lt.clone()),
        };
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
    async fn insert_without_key_generates_unique_id() {
        let (storage, _hv, _lt) = make_tiered_storage();

        let id = storage
            .insert_object(
                make_context(),
                None,
                &Default::default(),
                make_stream(b"auto-keyed"),
            )
            .await
            .unwrap();

        assert!(uuid::Uuid::parse_str(id.key()).is_ok());

        let (_, stream) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.as_ref(), b"auto-keyed");
    }

    // --- Routing ---

    #[tokio::test]
    async fn small_object_goes_to_high_volume() {
        let (storage, hv, lt) = make_tiered_storage();
        let payload = vec![0u8; 100]; // 100 bytes, well under 1 MiB

        let id = storage
            .insert_object(
                make_context(),
                Some("small".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        assert!(hv.contains(&id), "expected in high-volume");
        assert!(!lt.contains(&id), "leaked to long-term");
    }

    #[tokio::test]
    async fn large_object_goes_to_long_term_with_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB, over threshold

        let id = storage
            .insert_object(
                make_context(),
                Some("large".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert_eq!(lt_bytes.len(), payload.len());
        assert!(!lt_meta.is_tombstone());

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
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

        let id = storage
            .insert_object(
                make_context(),
                Some("expiry-test".into()),
                &metadata_in,
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Tombstone in HV should have only expiration_policy copied.
        let (tombstone, _) = hv.get_stored(&id).unwrap();
        assert!(tombstone.is_tombstone());
        assert_eq!(tombstone.expiration_policy, metadata_in.expiration_policy);
        assert_eq!(tombstone.content_type, Metadata::default().content_type);
        assert!(tombstone.origin.is_none());

        // Long-term object should have the full metadata.
        let (lt_meta, _) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_meta.content_type, "image/png");
        assert_eq!(lt_meta.expiration_policy, metadata_in.expiration_policy);
    }

    /// A small object with a pre-existing tombstone at the same key is detected
    /// atomically by `put_non_tombstone` and routed to long-term storage.
    #[tokio::test]
    async fn reinsert_small_with_existing_tombstone_routes_to_long_term() {
        let (storage, hv, lt) = make_tiered_storage();

        // Establish a tombstone via a large insert.
        let id = storage
            .insert_object(
                make_context(),
                Some("reinsert-key".into()),
                &Default::default(),
                make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
            )
            .await
            .unwrap();

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        // Re-insert a small payload at the same key.
        let small_payload = vec![0xCDu8; 100];
        storage
            .insert_object(
                make_context(),
                Some("reinsert-key".into()),
                &Default::default(),
                make_stream(&small_payload),
            )
            .await
            .unwrap();

        // Small object goes to LT; tombstone in HV is preserved.
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), small_payload.len());
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
    }

    /// A small object at a key is overwritten by a large one: the large insert
    /// replaces the small HV entry with a tombstone and writes data to LT.
    #[tokio::test]
    async fn overwrite_small_with_large_no_prior_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();

        let id = storage
            .insert_object(
                make_context(),
                Some("overwrite-key".into()),
                &Default::default(),
                make_stream(&[0xAAu8; 100]),
            )
            .await
            .unwrap();

        assert!(hv.contains(&id));
        assert!(!lt.contains(&id));

        let large_payload = vec![0xBBu8; 2 * 1024 * 1024];
        storage
            .insert_object(
                make_context(),
                Some("overwrite-key".into()),
                &Default::default(),
                make_stream(&large_payload),
            )
            .await
            .unwrap();

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(
            hv_meta.is_tombstone(),
            "HV must have tombstone after large overwrite"
        );
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), large_payload.len());
    }

    /// After a large object is fully deleted, a small re-insert at the same key
    /// goes directly to HV (no tombstone present).
    #[tokio::test]
    async fn overwrite_large_with_small_after_delete() {
        let (storage, hv, lt) = make_tiered_storage();

        let id = storage
            .insert_object(
                make_context(),
                Some("cycle-key".into()),
                &Default::default(),
                make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
            )
            .await
            .unwrap();

        storage.delete_object(&id).await.unwrap();
        assert!(!hv.contains(&id));
        assert!(!lt.contains(&id));

        let small_payload = vec![0xCDu8; 100];
        storage
            .insert_object(
                make_context(),
                Some("cycle-key".into()),
                &Default::default(),
                make_stream(&small_payload),
            )
            .await
            .unwrap();

        let (hv_meta, hv_bytes) = hv.get_stored(&id).unwrap();
        assert!(
            !hv_meta.is_tombstone(),
            "small object must be in HV, not a tombstone"
        );
        assert_eq!(hv_bytes.len(), small_payload.len());
        assert!(!lt.contains(&id));
    }

    // --- Multi-chunk streaming ---

    #[tokio::test]
    async fn multi_chunk_large_object_chains_buffered_and_remaining() {
        let (storage, _hv, lt) = make_tiered_storage();

        // Deliver a 2 MiB payload across multiple chunks that individually
        // fit under the threshold but collectively exceed it.
        let chunk_size = 512 * 1024; // 512 KiB per chunk
        let chunk_count = 4; // 4 × 512 KiB = 2 MiB total
        let chunks: Vec<std::io::Result<bytes::Bytes>> = (0..chunk_count)
            .map(|i| Ok(bytes::Bytes::from(vec![i as u8; chunk_size])))
            .collect();
        let stream = futures_util::stream::iter(chunks).boxed();

        let id = storage
            .insert_object(
                make_context(),
                Some("multi-chunk".into()),
                &Default::default(),
                stream,
            )
            .await
            .unwrap();

        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), chunk_size * chunk_count);

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

    // --- Tombstone consistency ---
    //
    // These tests verify the invariant: every object in long-term storage must be
    // reachable via a redirect tombstone in the high-volume backend, and every
    // tombstone must either point to existing LT data or be safely recoverable.
    //
    // Operation  Scenario                                   Outcome                    Test
    // ---------  -----------------------------------------  -------------------------  ----
    // read       tombstone present, LT data present         consistent                 reads_follow_tombstone_redirect
    // read       tombstone present, LT data absent          headless tombstone         orphan_hv_returns_none
    // insert     HV put_non_tombstone fails (small)         consistent                 insert_small_hv_put_fails
    // insert     HV tombstone write fails (large)           consistent                 insert_large_hv_tombstone_write_fails
    // insert     LT data write fails after HV tombstone     headless tombstone         insert_large_lt_put_fails_leaves_orphan_hv
    // insert     pod kill after HV tombstone, before LT     headless tombstone         pod_kill_during_insert_large_after_hv_tombstone
    // delete     clean delete                               consistent                 delete_cleans_up_both_backends
    // delete     LT delete fails                            consistent                 tombstone_preserved_when_long_term_delete_fails
    // insert×2   concurrent large inserts (HV-first)        consistent                 race_concurrent_insert_insert_no_orphan
    // insert×2   small insert during large LT write         consistent                 race_small_insert_during_large_lt_write
    // insert+delete concurrent insert and delete race       ORPHAN VIOLATION           race_concurrent_insert_delete_causes_orphan_lt

    // Mock backends used by the consistency tests.

    /// A backend where `put_object` always fails; reads and deletes delegate normally.
    #[derive(Debug)]
    struct FailingPutBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for FailingPutBackend {
        fn name(&self) -> &'static str {
            "failing-put"
        }

        async fn put_object(
            &self,
            _id: &ObjectId,
            _metadata: &Metadata,
            _stream: PayloadStream,
        ) -> Result<()> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated put failure",
            )))
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            self.0.delete_object(id).await
        }
    }

    /// A backend where `delete_object` always fails; reads and puts delegate normally.
    #[derive(Debug)]
    struct FailingDeleteBackend(InMemoryBackend);

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for FailingDeleteBackend {
        fn name(&self) -> &'static str {
            "failing-delete"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: PayloadStream,
        ) -> Result<()> {
            self.0.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, _id: &ObjectId) -> Result<()> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated delete failure",
            )))
        }
    }

    /// A backend that notifies when `put_object` starts and waits to be resumed,
    /// allowing tests to observe and control intermediate write state.
    #[derive(Debug)]
    struct SyncBackend {
        inner: InMemoryBackend,
        put_started: std::sync::Arc<tokio::sync::Notify>,
        put_resume: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for SyncBackend {
        fn name(&self) -> &'static str {
            "sync"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: PayloadStream,
        ) -> Result<()> {
            self.put_started.notify_one();
            self.put_resume.notified().await;
            self.inner.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.inner.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            self.inner.delete_object(id).await
        }
    }

    // Reads

    #[tokio::test]
    async fn reads_follow_tombstone_redirect() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let payload = vec![0xCDu8; 2 * 1024 * 1024];
        let id = storage
            .insert_object(
                make_context(),
                Some("redirect-read".into()),
                &metadata_in,
                make_stream(&payload),
            )
            .await
            .unwrap();

        let (metadata, stream) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!metadata.is_tombstone());

        let metadata = storage.get_metadata(&id).await.unwrap().unwrap();
        assert!(!metadata.is_tombstone());
        assert_eq!(metadata.content_type, "image/png");
    }

    /// An OrphanHV (tombstone in HV, no corresponding data in LT) is a
    /// recoverable state: reads return None rather than an error.
    #[tokio::test]
    async fn orphan_hv_returns_none() {
        let (storage, _hv, lt) = make_tiered_storage();
        let id = storage
            .insert_object(
                make_context(),
                Some("orphan-tombstone".into()),
                &Default::default(),
                make_stream(&vec![0xCDu8; 2 * 1024 * 1024]),
            )
            .await
            .unwrap();

        lt.remove(&id); // simulate missing LT data

        assert!(storage.get_object(&id).await.unwrap().is_none());
        assert!(storage.get_metadata(&id).await.unwrap().is_none());
    }

    // Insert failures — clean

    /// Small-object write via `put_non_tombstone`: if HV fails, the insert fails
    /// cleanly with nothing written to either backend.
    #[tokio::test]
    async fn insert_small_hv_put_fails() {
        let lt = InMemoryBackend::new("lt");
        let storage = TieredStorage {
            high_volume_backend: Box::new(FailingPutBackend(InMemoryBackend::new("hv"))),
            long_term_backend: Box::new(lt.clone()),
        };

        let result = storage
            .insert_object(
                make_context(),
                Some("small-fail".into()),
                &Default::default(),
                make_stream(b"tiny"),
            )
            .await;

        assert!(result.is_err());
        assert!(lt.is_empty());
    }

    /// Large-object HV-first: if the tombstone write fails, the insert fails cleanly
    /// with nothing written to either backend.
    #[tokio::test]
    async fn insert_large_hv_tombstone_write_fails() {
        let lt = InMemoryBackend::new("lt");
        let storage = TieredStorage {
            high_volume_backend: Box::new(FailingPutBackend(InMemoryBackend::new("hv"))),
            long_term_backend: Box::new(lt.clone()),
        };

        let result = storage
            .insert_object(
                make_context(),
                Some("hv-fail-test".into()),
                &Default::default(),
                make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
            )
            .await;

        assert!(result.is_err());
        assert!(lt.is_empty());
    }

    // Insert failures — note: OrphanHV possible

    /// When the LT data write fails after the HV tombstone is written, an
    /// OrphanHV is left: tombstone in HV, no data in LT. Reads return None,
    /// deletes remove the tombstone, and re-inserts overwrite it.
    #[tokio::test]
    async fn insert_large_lt_put_fails_leaves_orphan_hv() {
        let hv = InMemoryBackend::new("hv");
        let lt_inner = InMemoryBackend::new("lt-inner");
        let storage = TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(FailingPutBackend(lt_inner.clone())),
        };

        let result = storage
            .insert_object(
                make_context(),
                Some("lt-fail-test".into()),
                &Default::default(),
                make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
            )
            .await;

        assert!(result.is_err());
        // OrphanHV: tombstone in HV, no data in LT.
        let id = ObjectId::new(make_context(), "lt-fail-test".into());
        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must be in HV");
        assert!(hv_meta.is_tombstone());
        assert!(lt_inner.is_empty());
    }

    /// If the process is killed after the HV tombstone is written but before the LT
    /// write completes, an OrphanHV is left: tombstone in HV, no data in LT.
    /// Reads return None; deletes and re-inserts recover the key.
    #[tokio::test]
    async fn pod_kill_during_insert_large_after_hv_tombstone() {
        let hv = InMemoryBackend::new("hv");
        let lt_inner = InMemoryBackend::new("lt");
        let put_started = std::sync::Arc::new(tokio::sync::Notify::new());
        let put_resume = std::sync::Arc::new(tokio::sync::Notify::new());

        let storage = std::sync::Arc::new(TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(SyncBackend {
                inner: lt_inner.clone(),
                put_started: std::sync::Arc::clone(&put_started),
                put_resume: std::sync::Arc::clone(&put_resume),
            }),
        });

        let id = ObjectId::new(make_context(), "pod-kill-key".into());
        let storage_task = std::sync::Arc::clone(&storage);
        let insert_task = tokio::spawn(async move {
            storage_task
                .insert_object(
                    make_context(),
                    Some("pod-kill-key".into()),
                    &Default::default(),
                    make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
                )
                .await
        });

        // Wait until the HV tombstone is written and the LT put is blocked.
        put_started.notified().await;
        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must exist");
        assert!(hv_meta.is_tombstone());

        // Simulate pod kill.
        insert_task.abort();
        put_resume.notify_one(); // unblock backend to avoid deadlock
        let _ = insert_task.await;

        // OrphanHV: tombstone in HV, no data in LT.
        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must survive pod kill");
        assert!(hv_meta.is_tombstone());
        assert!(lt_inner.is_empty());
        assert!(storage.get_object(&id).await.unwrap().is_none());
    }

    // Delete

    #[tokio::test]
    async fn delete_cleans_up_both_backends() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = storage
            .insert_object(
                make_context(),
                Some("delete-both".into()),
                &Default::default(),
                make_stream(&vec![0u8; 2 * 1024 * 1024]),
            )
            .await
            .unwrap();

        storage.delete_object(&id).await.unwrap();

        assert!(!hv.contains(&id));
        assert!(!lt.contains(&id));
    }

    /// When the LT delete fails, the tombstone is preserved so the object remains
    /// reachable — no data is orphaned.
    #[tokio::test]
    async fn tombstone_preserved_when_long_term_delete_fails() {
        let hv = InMemoryBackend::new("hv");
        let storage = TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(FailingDeleteBackend(InMemoryBackend::new("lt"))),
        };

        let id = storage
            .insert_object(
                make_context(),
                Some("fail-delete".into()),
                &Default::default(),
                make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
            )
            .await
            .unwrap();

        assert!(storage.delete_object(&id).await.is_err());

        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must be preserved");
        assert!(hv_meta.is_tombstone());

        let (metadata, stream) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), 2 * 1024 * 1024);
        assert!(!metadata.is_tombstone());
    }

    // Concurrent insert + insert — clean

    /// Two concurrent large inserts at the same key: both write (or overwrite) the
    /// tombstone in HV, both write to LT — last write wins, tombstone stays, no
    /// orphan.
    #[tokio::test]
    async fn race_concurrent_insert_insert_no_orphan() {
        let (storage, hv, lt) = make_tiered_storage();
        let storage = std::sync::Arc::new(storage);
        let payload = vec![0xABu8; 2 * 1024 * 1024];

        let s1 = std::sync::Arc::clone(&storage);
        let p1 = payload.clone();
        let t1 = tokio::spawn(async move {
            s1.insert_object(
                make_context(),
                Some("race-key".into()),
                &Default::default(),
                make_stream(&p1),
            )
            .await
        });

        let s2 = std::sync::Arc::clone(&storage);
        let t2 = tokio::spawn(async move {
            s2.insert_object(
                make_context(),
                Some("race-key".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
        });

        t1.await.unwrap().unwrap();
        t2.await.unwrap().unwrap();

        let id = ObjectId::new(make_context(), "race-key".into());
        let (hv_meta, _) = hv.get_stored(&id).expect("HV must have tombstone");
        assert!(hv_meta.is_tombstone());
        assert!(lt.contains(&id));
        assert!(storage.get_object(&id).await.unwrap().is_some());
    }

    /// A small insert that arrives while a large insert's LT write is in progress
    /// sees the HV tombstone via `put_non_tombstone` and routes its payload to LT.
    /// Both writes complete consistently.
    ///
    /// Interleaving:
    /// 1. Large insert writes tombstone to HV, blocks in LT.
    /// 2. Small insert: `put_non_tombstone` sees tombstone → routes payload to LT.
    /// 3. Both LT writes complete → tombstone in HV, data in LT.
    #[tokio::test]
    async fn race_small_insert_during_large_lt_write() {
        let hv = InMemoryBackend::new("hv");
        let lt_inner = InMemoryBackend::new("lt");
        let put_started = std::sync::Arc::new(tokio::sync::Notify::new());
        let put_resume = std::sync::Arc::new(tokio::sync::Notify::new());

        let storage = std::sync::Arc::new(TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(SyncBackend {
                inner: lt_inner.clone(),
                put_started: std::sync::Arc::clone(&put_started),
                put_resume: std::sync::Arc::clone(&put_resume),
            }),
        });

        let id = ObjectId::new(make_context(), "concurrent-key".into());

        // Start large insert: HV tombstone is written immediately, LT write blocks.
        let storage_large = std::sync::Arc::clone(&storage);
        let large_task = tokio::spawn(async move {
            storage_large
                .insert_object(
                    make_context(),
                    Some("concurrent-key".into()),
                    &Default::default(),
                    make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
                )
                .await
        });

        // Wait until large insert is blocked in LT (tombstone already in HV).
        put_started.notified().await;

        // Small insert arrives. put_non_tombstone sees the tombstone → routes to LT.
        // LT is still SyncBackend, so this write blocks too.
        let storage_small = std::sync::Arc::clone(&storage);
        let small_task = tokio::spawn(async move {
            storage_small
                .insert_object(
                    make_context(),
                    Some("concurrent-key".into()),
                    &Default::default(),
                    make_stream(b"small payload"),
                )
                .await
        });

        // Wait until small insert is also blocked in LT.
        put_started.notified().await;

        // Both tasks are waiting on put_resume. Yield to ensure both have reached
        // the await point, then wake them together.
        tokio::task::yield_now().await;
        put_resume.notify_waiters();

        large_task.await.unwrap().unwrap();
        small_task.await.unwrap().unwrap();

        // Both writes completed consistently: tombstone in HV, data in LT.
        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must be in HV");
        assert!(hv_meta.is_tombstone());
        assert!(lt_inner.contains(&id));
        assert!(storage.get_object(&id).await.unwrap().is_some());
    }

    // Concurrent insert + delete — VIOLATION: OrphanLT

    /// Known gap: a concurrent insert and delete can race to leave an OrphanLT.
    ///
    /// Interleaving (HV-first):
    /// 1. Insert writes tombstone to HV.
    /// 2. Delete sees tombstone → deletes LT (empty) → deletes HV tombstone.
    /// 3. Insert writes data to LT (tombstone already gone).
    ///
    /// Result: LT has data with no tombstone in HV — unreachable via the service.
    /// Requires per-key serialization to fix; out of scope.
    #[tokio::test]
    async fn race_concurrent_insert_delete_causes_orphan_lt() {
        let hv = InMemoryBackend::new("hv");
        let lt_inner = InMemoryBackend::new("lt");
        let put_started = std::sync::Arc::new(tokio::sync::Notify::new());
        let put_resume = std::sync::Arc::new(tokio::sync::Notify::new());

        let storage = std::sync::Arc::new(TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(SyncBackend {
                inner: lt_inner.clone(),
                put_started: std::sync::Arc::clone(&put_started),
                put_resume: std::sync::Arc::clone(&put_resume),
            }),
        });

        let id = ObjectId::new(make_context(), "race-del-key".into());

        let storage_insert = std::sync::Arc::clone(&storage);
        let insert_task = tokio::spawn(async move {
            storage_insert
                .insert_object(
                    make_context(),
                    Some("race-del-key".into()),
                    &Default::default(),
                    make_stream(&vec![0xABu8; 2 * 1024 * 1024]),
                )
                .await
        });

        // Wait until the HV tombstone is written and the LT put is blocked.
        put_started.notified().await;
        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone must exist");
        assert!(hv_meta.is_tombstone());

        // Delete runs concurrently: sees tombstone → deletes LT (empty) → removes tombstone.
        storage.delete_object(&id).await.unwrap();
        assert!(!hv.contains(&id));

        // Resume insert: LT write completes after the tombstone is gone.
        put_resume.notify_one();
        insert_task.await.unwrap().unwrap();

        // OrphanLT: data in LT, no tombstone in HV → unreachable.
        assert!(lt_inner.contains(&id));
        assert!(!hv.contains(&id));
        assert!(storage.get_object(&id).await.unwrap().is_none());
    }
}
