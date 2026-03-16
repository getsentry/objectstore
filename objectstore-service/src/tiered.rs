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

use crate::backend::common::{
    Backend, BoxedBackend, ConditionalOutcome, DeleteResponse, GetResponse, MetadataResponse,
    PutResponse,
};
use crate::error::Result;
use crate::id::ObjectId;
use crate::stream::{ClientStream, SizedPeek};

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

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
        if metadata.origin.is_none() {
            objectstore_metrics::count!("put.origin_missing", usecase = id.usecase().to_owned());
        }

        let start = Instant::now();

        let peeked = SizedPeek::new(stream, BACKEND_SIZE_THRESHOLD).await?;
        let backend_choice = if peeked.is_exhausted() {
            BackendChoice::HighVolume
        } else {
            BackendChoice::LongTerm
        };

        objectstore_metrics::record!(
            "put.first_chunk.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice.as_str(),
        );

        let (final_choice, stored_size) = match backend_choice {
            BackendChoice::HighVolume => {
                let payload = peeked.into_bytes().await?;
                let stored_size = payload.len() as u64;

                let outcome = self
                    .high_volume_backend
                    .put_non_tombstone(id, metadata, payload.clone())
                    .await?;

                if outcome == ConditionalOutcome::Tombstone {
                    // Tombstone already exists in HV — write to long-term instead.
                    // TODO: The new object's expiry may differ from the tombstone's,
                    // leaving them inconsistent. This is a known gap and will be fixed
                    // in a follow-up.
                    let stream = crate::stream::single(payload).boxed();
                    self.long_term_backend
                        .put_object(id, metadata, stream)
                        .await?;
                    (BackendChoice::LongTerm, stored_size)
                } else {
                    (BackendChoice::HighVolume, stored_size)
                }
            }
            BackendChoice::LongTerm => {
                let stored_size = Arc::new(AtomicU64::new(0));
                let stream = peeked
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

                // First write the object to long-term.
                self.long_term_backend
                    .put_object(id, metadata, stream)
                    .await?;

                // Then write the redirect tombstone to high-volume.
                let redirect_metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: metadata.expiration_policy,
                    ..Default::default()
                };
                let redirect_stream = futures_util::stream::empty().boxed();
                let redirect_result = self
                    .high_volume_backend
                    .put_object(id, &redirect_metadata, redirect_stream)
                    .await;

                if redirect_result.is_err() {
                    // Clean up on any kind of error.
                    self.long_term_backend.delete_object(id).await?;
                }
                redirect_result?;

                (BackendChoice::LongTerm, stored_size.load(Ordering::Acquire))
            }
        };

        let backend_ty = match final_choice {
            BackendChoice::HighVolume => self.high_volume_backend.name(),
            BackendChoice::LongTerm => self.long_term_backend.name(),
        };

        let usecase = id.usecase().to_owned();
        objectstore_metrics::record!(
            "put.latency" = start.elapsed(),
            usecase = usecase.clone(),
            backend_choice = final_choice.as_str(),
            backend_type = backend_ty,
        );
        objectstore_metrics::record!(
            "put.size" = stored_size,
            usecase = usecase,
            backend_choice = final_choice.as_str(),
            backend_type = backend_ty,
        );

        Ok(())
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();
        let mut result = self.high_volume_backend.get_object(id).await?;

        if result.is_tombstone() {
            result = self.long_term_backend.get_object(id).await?;
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
        }

        objectstore_metrics::record!(
            "get.latency.pre-response" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice,
            backend_type = backend_type,
        );

        if let Some((metadata, _stream)) = &result {
            if let Some(size) = metadata.size {
                objectstore_metrics::record!(
                    "get.size" = size,
                    usecase = id.usecase().to_owned(),
                    backend_choice = backend_choice,
                    backend_type = backend_type,
                );
            } else {
                tracing::warn!(?backend_type, "Missing object size");
            }
        }

        Ok(result)
    }

    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();
        let mut result = self.high_volume_backend.get_metadata(id).await?;

        if result.as_ref().is_some_and(|m| m.is_tombstone()) {
            result = self.long_term_backend.get_metadata(id).await?;
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
        }

        objectstore_metrics::record!(
            "head.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice,
            backend_type = backend_type,
        );

        Ok(result)
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.high_volume_backend.name();

        let outcome = self.high_volume_backend.delete_non_tombstone(id).await?;
        if outcome == ConditionalOutcome::Tombstone {
            backend_choice = "long-term";
            backend_type = self.long_term_backend.name();
            // Delete the long-term object first, then clean up the tombstone.
            // This ordering ensures that if the long-term delete fails, the
            // tombstone remains and the data is still reachable (not orphaned).
            self.long_term_backend.delete_object(id).await?;
            self.high_volume_backend.delete_object(id).await?;
        }

        objectstore_metrics::record!(
            "delete.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice,
            backend_type = backend_type,
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
    use crate::backend::common::BoxedBackend;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;
    use crate::id::ObjectContext;
    use crate::stream::{self, ClientStream, PayloadStream};

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
    async fn random_id_has_uuid_key() {
        let id = ObjectId::random(make_context());
        assert!(uuid::Uuid::parse_str(id.key()).is_ok());
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

        // Real payload should be in long-term
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert_eq!(lt_bytes.len(), payload_len);
        assert!(!lt_meta.is_tombstone());

        // A redirect tombstone should exist in high-volume
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
    }

    #[tokio::test]
    async fn reinsert_with_existing_tombstone_routes_to_long_term() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "reinsert-key".into());

        // First: insert a large object → creates tombstone in hv, payload in lt
        let large_payload = vec![0xABu8; 2 * 1024 * 1024];
        storage
            .put_object(&id, &Default::default(), stream::single(large_payload))
            .await
            .unwrap();

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        // Now re-insert a SMALL payload with the same key. The service should
        // detect the existing tombstone and route to long-term anyway.
        let small_payload = vec![0xCDu8; 100]; // well under 1 MiB threshold
        storage
            .put_object(&id, &Default::default(), stream::single(small_payload))
            .await
            .unwrap();

        // The small object should be in long-term (not high-volume)
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), 100);

        // The tombstone in hv should still be present
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
        let id = ObjectId::new(make_context(), "expiry-test".into());

        storage
            .put_object(&id, &metadata_in, stream::single(payload))
            .await
            .unwrap();

        // The tombstone in hv should have ONLY expiration_policy copied
        let (tombstone, _) = hv.get_stored(&id).unwrap();
        assert!(tombstone.is_tombstone());
        assert_eq!(tombstone.expiration_policy, metadata_in.expiration_policy);
        assert_eq!(tombstone.content_type, Metadata::default().content_type);
        assert!(tombstone.origin.is_none());

        // The long-term object should have the full metadata
        let (lt_meta, _) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_meta.content_type, "image/png");
        assert_eq!(lt_meta.expiration_policy, metadata_in.expiration_policy);
    }

    // --- Tombstone redirect tests ---

    #[tokio::test]
    async fn reads_follow_tombstone_redirect() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let payload_len = 2 * 1024 * 1024; // 2 MiB, over threshold
        let payload = vec![0xCDu8; payload_len];

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let id = ObjectId::new(make_context(), "redirect-read".into());

        storage
            .put_object(&id, &metadata_in, stream::single(payload))
            .await
            .unwrap();

        // get_object should transparently follow the tombstone
        let (metadata, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = s.try_collect().await.unwrap();
        assert_eq!(body.len(), payload_len);
        assert!(!metadata.is_tombstone());

        // get_metadata should also follow the tombstone
        let metadata = storage.get_metadata(&id).await.unwrap().unwrap();
        assert!(!metadata.is_tombstone());
        assert_eq!(metadata.content_type, "image/png");
    }

    // --- Tombstone inconsistency tests ---

    /// A backend where put_object always fails, but reads/deletes work normally.
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
            _stream: ClientStream,
        ) -> Result<()> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated tombstone write failure",
            )))
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            self.0.delete_object(id).await
        }
    }

    /// If the tombstone write to the high-volume backend fails after the long-term
    /// write succeeds, the long-term object must be cleaned up so we never leave
    /// an unreachable orphan in long-term storage.
    #[tokio::test]
    async fn no_orphan_when_tombstone_write_fails() {
        let lt = InMemoryBackend::new("lt");
        let hv: BoxedBackend = Box::new(FailingPutBackend(InMemoryBackend::new("hv")));
        let storage = TieredStorage {
            high_volume_backend: hv,
            long_term_backend: Box::new(lt.clone()),
        };

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
        let (storage, _hv, lt) = make_tiered_storage();
        let id = ObjectId::new(make_context(), "orphan-tombstone".into());
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        // Remove the long-term object, leaving an orphan tombstone in hv
        lt.remove(&id);

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

        storage.delete_object(&id).await.unwrap();

        assert!(!hv.contains(&id), "tombstone not cleaned up");
        assert!(!lt.contains(&id), "object not cleaned up");
    }

    /// A backend wrapper that delegates everything except `delete_object`, which always fails.
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
            stream: ClientStream,
        ) -> Result<()> {
            self.0.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, _id: &ObjectId) -> Result<()> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated long-term delete failure",
            )))
        }
    }

    /// When the long-term delete fails, the tombstone must be preserved so the
    /// object remains reachable and no data is orphaned.
    #[tokio::test]
    async fn tombstone_preserved_when_long_term_delete_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt: BoxedBackend = Box::new(FailingDeleteBackend(InMemoryBackend::new("lt")));
        let storage = TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: lt,
        };

        let id = ObjectId::new(make_context(), "fail-delete".into());
        let payload_len = 2 * 1024 * 1024; // 2 MiB -> goes to long-term
        let payload = vec![0xABu8; payload_len];
        storage
            .put_object(&id, &Default::default(), stream::single(payload))
            .await
            .unwrap();

        let result = storage.delete_object(&id).await;
        assert!(result.is_err());

        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone removed");
        assert!(hv_meta.is_tombstone());

        // The object should still be reachable through the service
        let (metadata, s) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = s.try_collect().await.unwrap();
        assert_eq!(body.len(), payload_len);
        assert!(!metadata.is_tombstone());
    }

    // --- Multi-chunk streaming tests ---

    #[tokio::test]
    async fn multi_chunk_large_object_chains_buffered_and_remaining() {
        let (storage, _hv, lt) = make_tiered_storage();
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

        // Should have been routed to long-term (over 1 MiB).
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
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
