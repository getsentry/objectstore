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
use crate::backend::common::{BoxedBackend, DeleteOutcome};
use crate::error::Result;
use crate::id::{ObjectContext, ObjectId};
use crate::service::{DeleteResponse, GetResponse, InsertResponse, MetadataResponse};
use crate::stream::SizedPeek;

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

impl TieredStorage {
    pub(crate) async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> Result<InsertResponse> {
        if metadata.origin.is_none() {
            objectstore_metrics::count!("put.origin_missing", usecase = context.usecase.clone());
        }

        let start = Instant::now();

        let peeked = SizedPeek::new(stream, BACKEND_SIZE_THRESHOLD).await?;
        let mut backend = if peeked.is_exhausted() {
            BackendChoice::HighVolume
        } else {
            BackendChoice::LongTerm
        };

        objectstore_metrics::record!(
            "put.first_chunk.latency" = start.elapsed(),
            usecase = context.usecase.clone(),
            backend_choice = backend.as_str(),
        );

        let has_key = key.is_some();
        let id = ObjectId::optional(context, key);

        // There might currently be a tombstone at the given path from a previously stored object.
        if has_key {
            let metadata = self.high_volume_backend.get_metadata(&id).await?;
            if metadata.is_some_and(|m| m.is_tombstone()) {
                // Write the object to the other backend and keep the tombstone in place
                backend = BackendChoice::LongTerm;
            }
        };

        // Capture before `match backend` consumes the value.
        let backend_choice = backend.as_str();

        let (backend_ty, stored_size) = match backend {
            BackendChoice::HighVolume => {
                let stored_size = peeked.len() as u64;
                let stream = peeked.into_stream().boxed();

                self.high_volume_backend
                    .put_object(&id, metadata, stream)
                    .await?;
                (self.high_volume_backend.name(), stored_size)
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

                // first write the object
                self.long_term_backend
                    .put_object(&id, metadata, stream)
                    .await?;

                let redirect_metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: metadata.expiration_policy,
                    ..Default::default()
                };
                let redirect_stream = futures_util::stream::empty().boxed();
                let redirect_request =
                    self.high_volume_backend
                        .put_object(&id, &redirect_metadata, redirect_stream);

                // then we write the tombstone
                let redirect_result = redirect_request.await;
                if redirect_result.is_err() {
                    // and clean up on any kind of error
                    self.long_term_backend.delete_object(&id).await?;
                }
                redirect_result?;

                (
                    self.long_term_backend.name(),
                    stored_size.load(Ordering::Acquire),
                )
            }
        };

        let usecase = id.usecase().to_owned();
        objectstore_metrics::record!(
            "put.latency" = start.elapsed(),
            usecase = usecase.clone(),
            backend_choice = backend_choice,
            backend_type = backend_ty,
        );
        objectstore_metrics::record!(
            "put.size" = stored_size,
            usecase = usecase,
            backend_choice = backend_choice,
            backend_type = backend_ty,
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

        objectstore_metrics::record!(
            "head.latency" = start.elapsed(),
            usecase = id.usecase().to_owned(),
            backend_choice = backend_choice,
            backend_type = backend_type,
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
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};
    use tokio::sync::Notify;

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

    const SMALL: &[u8] = &[0xAA; 100];
    const LARGE_SIZE: usize = 2 * 1024 * 1024;

    fn large_payload() -> Vec<u8> {
        vec![0xBB; LARGE_SIZE]
    }

    fn make_tiered_from(hv: &InMemoryBackend, lt: &InMemoryBackend) -> TieredStorage {
        TieredStorage {
            high_volume_backend: Box::new(hv.clone()),
            long_term_backend: Box::new(lt.clone()),
        }
    }

    fn make_tiered_storage() -> (TieredStorage, InMemoryBackend, InMemoryBackend) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        (make_tiered_from(&hv, &lt), hv, lt)
    }

    async fn insert_small(storage: &TieredStorage, key: &str) -> ObjectId {
        storage
            .insert_object(
                make_context(),
                Some(key.into()),
                &Default::default(),
                make_stream(SMALL),
            )
            .await
            .unwrap()
    }

    async fn insert_large(storage: &TieredStorage, key: &str) -> ObjectId {
        let payload = large_payload();
        storage
            .insert_object(
                make_context(),
                Some(key.into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap()
    }

    fn simulated_error(msg: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            msg.to_owned(),
        ))
    }

    // ==========================================
    // Correctness predicate
    // ==========================================
    //
    // Invariants for any given key:
    // - No OrphanLT: if LT has data, HV must have a tombstone pointing to it
    // - No DualData: HV and LT must not both contain non-tombstone data
    // - OrphanTombstone is safe: tombstone in HV with nothing in LT must
    //   return None on read
    //
    // `check_invariants` is the sync core that returns Err on violation.
    // `assert_consistent` wraps it and additionally verifies OrphanTombstone
    // reads return None.

    /// Core invariant check on the three booleans that characterize tiered state.
    fn check_invariants_core(
        hv_present: bool,
        hv_tombstone: bool,
        lt_present: bool,
        key: &str,
    ) -> std::result::Result<(), String> {
        match (hv_present, hv_tombstone, lt_present) {
            (false, _, false) => Ok(()),    // Empty
            (true, false, false) => Ok(()), // Small
            (true, true, true) => Ok(()),   // Large
            (true, true, false) => Ok(()),  // OrphanTombstone (async check in assert_consistent)
            (false, _, true) => Err(format!(
                "OrphanLT: data in LT for key {key:?} but nothing in HV"
            )),
            (true, false, true) => Err(format!(
                "DualData: non-tombstone in HV AND data in LT for key {key:?}"
            )),
        }
    }

    /// Checks the three consistency invariants. Returns `Err(msg)` on violation.
    fn check_invariants(
        hv: &InMemoryBackend,
        lt: &InMemoryBackend,
        id: &ObjectId,
    ) -> std::result::Result<(), String> {
        let hv_entry = hv.get_stored(id);
        let hv_present = hv_entry.is_some();
        let hv_tombstone = hv_entry.as_ref().is_some_and(|(m, _)| m.is_tombstone());
        let lt_present = lt.get_stored(id).is_some();
        check_invariants_core(hv_present, hv_tombstone, lt_present, id.key())
    }

    /// Panics if invariants are violated. For OrphanTombstone, additionally
    /// verifies that reads return None.
    async fn assert_consistent(
        storage: &TieredStorage,
        hv: &InMemoryBackend,
        lt: &InMemoryBackend,
        id: &ObjectId,
    ) {
        check_invariants(hv, lt, id).unwrap_or_else(|msg| panic!("{msg}"));

        // OrphanTombstone acceptance check: reads must return None.
        let is_orphan_tombstone =
            hv.get_stored(id).is_some_and(|(m, _)| m.is_tombstone()) && !lt.contains(id);
        if is_orphan_tombstone {
            assert!(
                storage.get_object(id).await.unwrap().is_none(),
                "OrphanTombstone: get_object should return None for key {:?}",
                id.key()
            );
            assert!(
                storage.get_metadata(id).await.unwrap().is_none(),
                "OrphanTombstone: get_metadata should return None for key {:?}",
                id.key()
            );
        }
    }

    // ==========================================
    // Shared mock backends
    // ==========================================

    /// Which backend operation should fail.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FailOn {
        Put,
        Get,
        GetMetadata,
        Delete,
    }

    /// Which side of the tiered storage should fail.
    #[derive(Debug, Clone, Copy)]
    enum FailSide {
        Hv,
        Lt,
    }

    /// A backend that delegates to an inner `InMemoryBackend` but fails on a
    /// configurable operation.
    #[derive(Debug)]
    struct SelectiveFailBackend {
        inner: InMemoryBackend,
        fail_on: FailOn,
    }

    impl SelectiveFailBackend {
        fn new(inner: InMemoryBackend, fail_on: FailOn) -> Self {
            Self { inner, fail_on }
        }
    }

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for SelectiveFailBackend {
        fn name(&self) -> &'static str {
            "selective-fail"
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: PayloadStream,
        ) -> Result<()> {
            if self.fail_on == FailOn::Put {
                return Err(simulated_error("selective-fail: put_object"));
            }
            self.inner.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            if self.fail_on == FailOn::Get {
                return Err(simulated_error("selective-fail: get_object"));
            }
            self.inner.get_object(id).await
        }

        async fn get_metadata(&self, id: &ObjectId) -> Result<Option<Metadata>> {
            if self.fail_on == FailOn::GetMetadata {
                return Err(simulated_error("selective-fail: get_metadata"));
            }
            self.inner.get_metadata(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            if self.fail_on == FailOn::Delete {
                return Err(simulated_error("selective-fail: delete_object"));
            }
            self.inner.delete_object(id).await
        }
    }

    fn make_failing_storage(
        hv: &InMemoryBackend,
        lt: &InMemoryBackend,
        fail_side: FailSide,
        fail_on: FailOn,
    ) -> TieredStorage {
        match fail_side {
            FailSide::Hv => TieredStorage {
                high_volume_backend: Box::new(SelectiveFailBackend::new(hv.clone(), fail_on)),
                long_term_backend: Box::new(lt.clone()),
            },
            FailSide::Lt => TieredStorage {
                high_volume_backend: Box::new(hv.clone()),
                long_term_backend: Box::new(SelectiveFailBackend::new(lt.clone(), fail_on)),
            },
        }
    }

    /// A backend that pauses on a specific operation until a `Notify` is signaled,
    /// then delegates to the inner `InMemoryBackend`.
    #[derive(Debug)]
    struct SyncBackend {
        inner: InMemoryBackend,
        sync_on: FailOn,
        notify: Arc<Notify>,
    }

    impl SyncBackend {
        fn new(inner: InMemoryBackend, sync_on: FailOn, notify: Arc<Notify>) -> Self {
            Self {
                inner,
                sync_on,
                notify,
            }
        }
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
            if self.sync_on == FailOn::Put {
                self.notify.notified().await;
            }
            self.inner.put_object(id, metadata, stream).await
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            if self.sync_on == FailOn::Get {
                self.notify.notified().await;
            }
            self.inner.get_object(id).await
        }

        async fn get_metadata(&self, id: &ObjectId) -> Result<Option<Metadata>> {
            if self.sync_on == FailOn::GetMetadata {
                self.notify.notified().await;
            }
            self.inner.get_metadata(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            if self.sync_on == FailOn::Delete {
                self.notify.notified().await;
            }
            self.inner.delete_object(id).await
        }
    }

    // ==========================================
    // Happy path: state transitions
    // ==========================================

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

    #[tokio::test]
    async fn small_object_goes_to_high_volume() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_small(&storage, "small").await;

        assert!(hv.contains(&id), "expected in high-volume");
        assert!(!lt.contains(&id), "leaked to long-term");
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn large_object_goes_to_long_term_with_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_large(&storage, "large").await;

        // Real payload should be in long-term
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert_eq!(lt_bytes.len(), LARGE_SIZE);
        assert!(!lt_meta.is_tombstone());

        // A redirect tombstone should exist in high-volume
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn reinsert_with_existing_tombstone_routes_to_long_term() {
        let (storage, hv, lt) = make_tiered_storage();

        // First: insert a large object -> creates tombstone in hv, payload in lt
        let id = insert_large(&storage, "reinsert-key").await;
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        // Now re-insert a SMALL payload with the same key. The service should
        // detect the existing tombstone and route to long-term anyway.
        insert_small(&storage, "reinsert-key").await;

        // The small object should be in long-term (not high-volume)
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), SMALL.len());

        // The tombstone in hv should still be present
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        assert_consistent(&storage, &hv, &lt, &id).await;
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
        let payload = large_payload();

        let id = storage
            .insert_object(
                make_context(),
                Some("expiry-test".into()),
                &metadata_in,
                make_stream(&payload),
            )
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

    #[tokio::test]
    async fn reads_follow_tombstone_redirect() {
        let (storage, _hv, _lt) = make_tiered_storage();
        let payload = large_payload();

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let id = storage
            .insert_object(
                make_context(),
                Some("redirect-read".into()),
                &metadata_in,
                make_stream(&payload),
            )
            .await
            .unwrap();

        // get_object should transparently follow the tombstone
        let (metadata, stream) = storage.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), LARGE_SIZE);
        assert!(!metadata.is_tombstone());

        // get_metadata should also follow the tombstone
        let metadata = storage.get_metadata(&id).await.unwrap().unwrap();
        assert!(!metadata.is_tombstone());
        assert_eq!(metadata.content_type, "image/png");
    }

    #[tokio::test]
    async fn delete_cleans_up_both_backends() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_large(&storage, "delete-both").await;

        storage.delete_object(&id).await.unwrap();

        assert!(!hv.contains(&id), "tombstone not cleaned up");
        assert!(!lt.contains(&id), "object not cleaned up");
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn orphan_tombstone_returns_none() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_large(&storage, "orphan-tombstone").await;

        // Remove the long-term object, leaving an orphan tombstone in hv
        lt.remove(&id);

        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn multi_chunk_large_object_chains_buffered_and_remaining() {
        let (storage, _hv, lt) = make_tiered_storage();

        // Deliver a 2 MiB payload across multiple chunks that individually
        // fit under the threshold but collectively exceed it.
        let chunk_size = 512 * 1024; // 512 KiB per chunk
        let chunk_count = 4; // 4 x 512 KiB = 2 MiB total
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

        // Should have been routed to long-term (over 1 MiB).
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), chunk_size * chunk_count);

        // Verify data integrity -- each chunk's fill byte should appear in order.
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

    #[tokio::test]
    async fn overwrite_small_with_large_no_prior_tombstone() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_small(&storage, "overwrite-key").await;

        let (hv_meta, hv_bytes) = hv.get_stored(&id).unwrap();
        assert!(!hv_meta.is_tombstone());
        assert_eq!(hv_bytes.len(), SMALL.len());
        assert!(!lt.contains(&id));

        insert_large(&storage, "overwrite-key").await;

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), LARGE_SIZE);
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn overwrite_large_with_small_after_delete() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_large(&storage, "reinsert-small").await;
        storage.delete_object(&id).await.unwrap();

        insert_small(&storage, "reinsert-small").await;

        let (hv_meta, hv_bytes) = hv.get_stored(&id).unwrap();
        assert!(!hv_meta.is_tombstone());
        assert_eq!(hv_bytes.len(), SMALL.len());
        assert!(!lt.contains(&id));
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn delete_small_only_object() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = insert_small(&storage, "delete-small").await;

        storage.delete_object(&id).await.unwrap();

        assert!(!hv.contains(&id));
        assert!(!lt.contains(&id));
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn exact_threshold_goes_to_high_volume() {
        let (storage, hv, lt) = make_tiered_storage();
        let payload = vec![0xDDu8; BACKEND_SIZE_THRESHOLD];

        let id = storage
            .insert_object(
                make_context(),
                Some("exact-threshold".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        let (hv_meta, hv_bytes) = hv.get_stored(&id).unwrap();
        assert!(!hv_meta.is_tombstone());
        assert_eq!(hv_bytes.len(), BACKEND_SIZE_THRESHOLD);
        assert!(!lt.contains(&id));
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    #[tokio::test]
    async fn empty_object_goes_to_high_volume() {
        let (storage, hv, lt) = make_tiered_storage();
        let id = storage
            .insert_object(
                make_context(),
                Some("empty-object".into()),
                &Default::default(),
                make_stream(&[]),
            )
            .await
            .unwrap();

        assert!(hv.contains(&id));
        assert!(!lt.contains(&id));
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    // ==========================================
    // Backend outages: error at each operation step
    // ==========================================

    /// If the tombstone write to the high-volume backend fails after the long-term
    /// write succeeds, the long-term object must be cleaned up so we never leave
    /// an unreachable orphan in long-term storage.
    #[tokio::test]
    async fn no_orphan_when_tombstone_write_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Put);

        let payload = large_payload();
        let result = storage
            .insert_object(
                make_context(),
                Some("orphan-test".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        assert!(result.is_err());
        assert!(lt.is_empty(), "long-term object not cleaned up");
    }

    /// When the long-term delete fails, the tombstone must be preserved so the
    /// object remains reachable and no data is orphaned.
    #[tokio::test]
    async fn tombstone_preserved_when_long_term_delete_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "fail-delete").await;

        let storage = make_failing_storage(&hv, &lt, FailSide::Lt, FailOn::Delete);
        let result = storage.delete_object(&id).await;
        assert!(result.is_err());

        let (hv_meta, _) = hv.get_stored(&id).expect("tombstone removed");
        assert!(hv_meta.is_tombstone());

        // The object should still be reachable through the setup storage
        let (metadata, stream) = setup.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), LARGE_SIZE);
        assert!(!metadata.is_tombstone());
    }

    // --- Insert large: outage tests ---

    /// HV.get_metadata fails before any writes during insert of a large object
    /// with an existing key. State should remain unchanged.
    #[tokio::test]
    async fn insert_large_hv_metadata_check_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "meta-fail").await;
        assert_consistent(&setup, &hv, &lt, &id).await;

        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::GetMetadata);
        let payload = large_payload();
        let result = storage
            .insert_object(
                make_context(),
                Some("meta-fail".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    /// LT.put_object fails before the tombstone write during large insert.
    /// State should remain unchanged (no writes succeed).
    #[tokio::test]
    async fn insert_large_lt_put_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = make_failing_storage(&hv, &lt, FailSide::Lt, FailOn::Put);

        let payload = large_payload();
        let result = storage
            .insert_object(
                make_context(),
                Some("lt-put-fail".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        assert!(result.is_err());
        assert!(hv.is_empty());
        assert!(lt.is_empty());
    }

    // --- Insert small: outage tests ---

    /// HV.put_object fails during small insert. Nothing should be written.
    #[tokio::test]
    async fn insert_small_hv_put_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Put);

        let result = storage
            .insert_object(
                make_context(),
                Some("small-put-fail".into()),
                &Default::default(),
                make_stream(SMALL),
            )
            .await;

        assert!(result.is_err());
        assert!(hv.is_empty());
        assert!(lt.is_empty());
    }

    // --- Get large: outage tests ---

    /// HV.get_object returns error when reading a large object.
    #[tokio::test]
    async fn get_large_hv_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "get-hv-fail").await;

        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Get);
        let result = storage.get_object(&id).await;
        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    /// HV returns tombstone successfully but LT.get_object fails.
    #[tokio::test]
    async fn get_large_lt_fails_after_tombstone() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "get-lt-fail").await;

        let storage = make_failing_storage(&hv, &lt, FailSide::Lt, FailOn::Get);
        let result = storage.get_object(&id).await;
        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    // --- Get small: outage tests ---

    /// HV.get_object returns error when reading a small object.
    #[tokio::test]
    async fn get_small_hv_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_small(&setup, "get-small-fail").await;

        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Get);
        let result = storage.get_object(&id).await;
        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    // --- Delete large: outage tests ---

    /// The metadata check in delete_non_tombstone fails for a large object.
    /// The tombstone and LT data should remain intact.
    #[tokio::test]
    async fn delete_large_hv_delete_non_tombstone_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "del-large-fail").await;

        // delete_non_tombstone calls get_metadata then delete_object.
        // Failing get_metadata prevents the delete path from proceeding.
        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::GetMetadata);
        let result = storage.delete_object(&id).await;
        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;

        let get_result = setup.get_object(&id).await.unwrap();
        assert!(get_result.is_some());
    }

    // --- Delete small: outage tests ---

    /// The delete_object call inside delete_non_tombstone fails for a small object.
    /// The HV data should remain.
    #[tokio::test]
    async fn delete_small_hv_delete_fails() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_small(&setup, "del-small-fail").await;

        // delete_non_tombstone calls get_metadata (succeeds), then delete_object (fails).
        let storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Delete);
        let result = storage.delete_object(&id).await;
        assert!(result.is_err());
        assert_consistent(&setup, &hv, &lt, &id).await;

        let get_result = setup.get_object(&id).await.unwrap();
        assert!(get_result.is_some());
    }

    // --- Insert large: double failure (G1) ---

    /// When the tombstone write to HV fails AND the subsequent LT cleanup also
    /// fails, an OrphanLT exists (data in LT, nothing in HV). This documents
    /// gap G1 from the consistency analysis.
    #[tokio::test]
    async fn insert_cleanup_double_failure_leaves_orphan_lt() {
        let lt_inner = InMemoryBackend::new("lt");
        // LT: put succeeds (data write), delete always fails (cleanup).
        let lt = SelectiveFailBackend::new(lt_inner.clone(), FailOn::Delete);
        // HV: put always fails (tombstone write).
        let hv_inner = InMemoryBackend::new("hv");
        let hv = SelectiveFailBackend::new(hv_inner.clone(), FailOn::Put);

        let storage = TieredStorage {
            high_volume_backend: Box::new(hv),
            long_term_backend: Box::new(lt),
        };

        let payload = large_payload();
        let result = storage
            .insert_object(
                make_context(),
                Some("double-fail".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        assert!(result.is_err(), "double failure should return an error");

        // TODO(consistency): Fix insert cleanup to not lose the original error
        // and to retry or accept the orphan gracefully.
        let id = ObjectId::new(make_context(), "double-fail".into());
        let violation = check_invariants(&hv_inner, &lt_inner, &id);
        assert!(
            violation.unwrap_err().contains("OrphanLT"),
            "double failure must produce OrphanLT"
        );
    }

    // --- Delete large: tombstone cleanup failure ---

    /// When deleting a large object, if the HV tombstone cleanup fails after
    /// the LT delete succeeds, an OrphanTombstone remains. Subsequent reads
    /// should return None (accepted state via assert_consistent).
    #[tokio::test]
    async fn delete_tombstone_cleanup_failure_leaves_orphan_tombstone() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "orphan-tombstone-delete").await;

        // HV delete always fails → tombstone cleanup will fail.
        let delete_storage = make_failing_storage(&hv, &lt, FailSide::Hv, FailOn::Delete);
        let result = delete_storage.delete_object(&id).await;
        assert!(result.is_err());

        assert!(!lt.contains(&id), "LT data should be deleted");
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone(), "tombstone should survive");

        // OrphanTombstone is accepted: assert_consistent verifies reads return None.
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    // ==========================================
    // Pod termination: drop between backend calls
    // ==========================================

    /// Insert large object: LT write completes, then the task is killed before
    /// the tombstone write to HV. Result: OrphanLT (data in LT, nothing in HV).
    /// This documents the known vulnerability.
    #[tokio::test]
    async fn pod_kill_during_insert_large_after_lt_write() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");

        // HV will block on put_object (the tombstone write), simulating a kill.
        let never_signal = Arc::new(Notify::new());
        let hv_sync = SyncBackend::new(hv.clone(), FailOn::Put, Arc::clone(&never_signal));
        let storage = TieredStorage {
            high_volume_backend: Box::new(hv_sync),
            long_term_backend: Box::new(lt.clone()),
        };

        let id = ObjectId::new(make_context(), "pod-kill-insert".into());
        let payload = large_payload();

        // Start the insert; it will block at the HV put (tombstone write).
        let insert_handle = tokio::spawn(async move {
            storage
                .insert_object(
                    make_context(),
                    Some("pod-kill-insert".into()),
                    &Default::default(),
                    make_stream(&payload),
                )
                .await
        });

        // Wait just enough for LT write to complete, then cancel.
        let timeout_result = tokio::time::timeout(Duration::from_millis(500), insert_handle).await;
        assert!(
            timeout_result.is_err(),
            "insert should have been blocked on tombstone write"
        );

        // TODO(consistency): The tombstone must be written atomically with the
        // LT data, or the incomplete insert must be rolled back on recovery.
        let violation = check_invariants(&hv, &lt, &id);
        assert!(
            violation.unwrap_err().contains("OrphanLT"),
            "pod kill after LT write must produce OrphanLT"
        );
    }

    /// Delete large object: LT delete completes, then the task is killed before
    /// the HV tombstone cleanup. Result: OrphanTombstone (tombstone in HV,
    /// nothing in LT). Reads return None (accepted state).
    #[tokio::test]
    async fn pod_kill_during_delete_large_after_lt_delete() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let setup = make_tiered_from(&hv, &lt);
        let id = insert_large(&setup, "pod-kill-delete").await;
        assert_consistent(&setup, &hv, &lt, &id).await;

        // Now set up a storage where HV blocks on delete_object (tombstone cleanup).
        let never_signal = Arc::new(Notify::new());
        let hv_sync = SyncBackend::new(hv.clone(), FailOn::Delete, Arc::clone(&never_signal));
        let storage = Arc::new(TieredStorage {
            high_volume_backend: Box::new(hv_sync),
            long_term_backend: Box::new(lt.clone()),
        });

        let storage_clone = Arc::clone(&storage);
        let id_clone = id.clone();
        let delete_handle =
            tokio::spawn(async move { storage_clone.delete_object(&id_clone).await });

        // Wait just enough for LT delete to complete, then cancel.
        let timeout_result = tokio::time::timeout(Duration::from_millis(500), delete_handle).await;
        assert!(
            timeout_result.is_err(),
            "delete should have been blocked on HV tombstone cleanup"
        );

        // After cancellation: tombstone in HV, nothing in LT -> OrphanTombstone.
        assert!(hv.contains(&id), "HV should still have the tombstone");
        assert!(!lt.contains(&id), "LT data should have been deleted");
        assert_consistent(&setup, &hv, &lt, &id).await;
    }

    // ==========================================
    // Concurrent races
    // ==========================================

    /// Two concurrent deletes on the same large object. Both should complete
    /// without error (one may no-op). Backend state should be Empty.
    #[tokio::test]
    async fn race_concurrent_delete_delete_is_safe() {
        let hv = InMemoryBackend::new("hv");
        let lt = InMemoryBackend::new("lt");
        let storage = Arc::new(make_tiered_from(&hv, &lt));
        let id = insert_large(&storage, "race-delete").await;

        // Spawn two concurrent deletes.
        let storage1 = Arc::clone(&storage);
        let id1 = id.clone();
        let handle1 = tokio::spawn(async move { storage1.delete_object(&id1).await });

        let storage2 = Arc::clone(&storage);
        let id2 = id.clone();
        let handle2 = tokio::spawn(async move { storage2.delete_object(&id2).await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        // Both should succeed (InMemoryBackend deletes are idempotent).
        r1.unwrap().unwrap();
        r2.unwrap().unwrap();

        assert!(!hv.contains(&id), "HV should be empty");
        assert!(!lt.contains(&id), "LT should be empty");
        assert_consistent(&storage, &hv, &lt, &id).await;
    }

    // --- Deterministic race reproduction ---
    //
    // These tests use mock backends with synchronization primitives to
    // deterministically reproduce the race conditions identified in the
    // tiered consistency analysis. They prove that the OrphanLT gaps are real.

    mod concurrent_races {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        use bytes::{Bytes, BytesMut};
        use futures_util::{StreamExt, TryStreamExt};
        use objectstore_types::metadata::Metadata;
        use tokio::sync::Notify;

        use super::*;

        type Store = HashMap<ObjectId, (Metadata, Bytes)>;

        fn check_store_invariants(
            hv_store: &Store,
            lt_store: &Store,
            id: &ObjectId,
        ) -> std::result::Result<(), String> {
            let hv_entry = hv_store.get(id);
            let hv_present = hv_entry.is_some();
            let hv_tombstone = hv_entry.is_some_and(|(m, _)| m.is_tombstone());
            let lt_present = lt_store.contains_key(id);
            check_invariants_core(hv_present, hv_tombstone, lt_present, id.key())
        }

        /// A backend backed by a shared HashMap with Notify-based sync hooks
        /// for precise interleaving control.
        #[derive(Debug)]
        struct SyncBackend {
            name: &'static str,
            store: Arc<Mutex<Store>>,
            put_wait: Option<Arc<Notify>>,
            put_signal: Option<Arc<Notify>>,
            get_metadata_signal: Option<Arc<Notify>>,
            delete_wait: Option<Arc<Notify>>,
            delete_signal: Option<Arc<Notify>>,
        }

        impl SyncBackend {
            fn new(name: &'static str, store: Arc<Mutex<Store>>) -> Self {
                Self {
                    name,
                    store,
                    put_wait: None,
                    put_signal: None,
                    get_metadata_signal: None,
                    delete_wait: None,
                    delete_signal: None,
                }
            }

            fn on_put(mut self, wait: Arc<Notify>, signal: Arc<Notify>) -> Self {
                self.put_wait = Some(wait);
                self.put_signal = Some(signal);
                self
            }

            fn on_put_wait(mut self, wait: Arc<Notify>) -> Self {
                self.put_wait = Some(wait);
                self
            }

            fn on_get_metadata_signal(mut self, signal: Arc<Notify>) -> Self {
                self.get_metadata_signal = Some(signal);
                self
            }

            fn on_delete(mut self, wait: Arc<Notify>, signal: Arc<Notify>) -> Self {
                self.delete_wait = Some(wait);
                self.delete_signal = Some(signal);
                self
            }

            fn on_delete_wait(mut self, wait: Arc<Notify>) -> Self {
                self.delete_wait = Some(wait);
                self
            }
        }

        #[async_trait::async_trait]
        impl crate::backend::common::Backend for SyncBackend {
            fn name(&self) -> &'static str {
                self.name
            }

            async fn put_object(
                &self,
                id: &ObjectId,
                metadata: &Metadata,
                stream: PayloadStream,
            ) -> crate::error::Result<()> {
                let bytes: BytesMut = stream.try_collect().await?;
                if let Some(wait) = &self.put_wait {
                    wait.notified().await;
                }
                self.store
                    .lock()
                    .unwrap()
                    .insert(id.clone(), (metadata.clone(), bytes.freeze()));
                if let Some(signal) = &self.put_signal {
                    signal.notify_one();
                }
                Ok(())
            }

            async fn get_object(
                &self,
                id: &ObjectId,
            ) -> crate::error::Result<Option<(Metadata, PayloadStream)>> {
                let entry = self.store.lock().unwrap().get(id).cloned();
                Ok(entry.map(|(metadata, bytes)| {
                    let mut metadata = metadata;
                    metadata.size = Some(bytes.len());
                    let stream = futures_util::stream::once(async move { Ok(bytes) }).boxed();
                    (metadata, stream)
                }))
            }

            async fn get_metadata(&self, id: &ObjectId) -> crate::error::Result<Option<Metadata>> {
                let result = self.store.lock().unwrap().get(id).map(|(m, b)| {
                    let mut m = m.clone();
                    m.size = Some(b.len());
                    m
                });
                if let Some(signal) = &self.get_metadata_signal {
                    signal.notify_one();
                }
                Ok(result)
            }

            async fn delete_object(&self, id: &ObjectId) -> crate::error::Result<()> {
                if let Some(wait) = &self.delete_wait {
                    wait.notified().await;
                }
                self.store.lock().unwrap().remove(id);
                if let Some(signal) = &self.delete_signal {
                    signal.notify_one();
                }
                Ok(())
            }
        }

        /// Race 1: Concurrent insert(large) + insert(small) → OrphanLT.
        ///
        /// ```text
        /// A: insert large "foo"     B: insert small "foo"
        /// A: peek -> LongTerm       B: peek -> HighVolume
        ///                            B: check HV -> no tombstone
        /// A: write data to LT
        /// A: write tombstone to HV
        ///                            B: write small to HV (overwrites tombstone!)
        /// Result: HV=small data, LT=large data (OrphanLT)
        /// ```
        #[tokio::test]
        async fn race_concurrent_insert_insert_causes_orphan_lt() {
            let shared_hv_store: Arc<Mutex<Store>> = Arc::new(Mutex::new(HashMap::new()));
            let shared_lt_store: Arc<Mutex<Store>> = Arc::new(Mutex::new(HashMap::new()));

            let a_tombstone_written = Arc::new(Notify::new());
            let b_metadata_checked = Arc::new(Notify::new());

            // A: HV put waits for B's metadata check, then signals when done.
            let storage_a = TieredStorage {
                high_volume_backend: Box::new(
                    SyncBackend::new("hv-a", Arc::clone(&shared_hv_store)).on_put(
                        Arc::clone(&b_metadata_checked),
                        Arc::clone(&a_tombstone_written),
                    ),
                ),
                long_term_backend: Box::new(SyncBackend::new("lt-a", Arc::clone(&shared_lt_store))),
            };

            // B: HV get_metadata signals when done; HV put waits for A's tombstone.
            let storage_b = TieredStorage {
                high_volume_backend: Box::new(
                    SyncBackend::new("hv-b", Arc::clone(&shared_hv_store))
                        .on_put_wait(Arc::clone(&a_tombstone_written))
                        .on_get_metadata_signal(Arc::clone(&b_metadata_checked)),
                ),
                long_term_backend: Box::new(SyncBackend::new("lt-b", Arc::clone(&shared_lt_store))),
            };

            let context = make_context();
            let key = "race-insert-insert";
            let large_payload = super::large_payload();
            let small_payload = SMALL;

            let task_a = {
                let ctx = context.clone();
                tokio::spawn(async move {
                    storage_a
                        .insert_object(
                            ctx,
                            Some(key.into()),
                            &Default::default(),
                            make_stream(&large_payload),
                        )
                        .await
                })
            };
            let task_b = {
                let ctx = context.clone();
                tokio::spawn(async move {
                    storage_b
                        .insert_object(
                            ctx,
                            Some(key.into()),
                            &Default::default(),
                            make_stream(small_payload),
                        )
                        .await
                })
            };

            task_a.await.unwrap().unwrap();
            task_b.await.unwrap().unwrap();

            // TODO(consistency): Prevent concurrent insert+insert from producing
            // OrphanLT, e.g. via per-key serialization or conditional writes.
            let id = ObjectId::new(context, key.into());
            let hv_store = shared_hv_store.lock().unwrap();
            let lt_store = shared_lt_store.lock().unwrap();
            let violation = check_store_invariants(&hv_store, &lt_store, &id);
            assert!(
                violation.unwrap_err().contains("DualData"),
                "concurrent insert+insert must violate consistency"
            );
        }

        /// Race 2: Concurrent insert(small) + delete → OrphanLT.
        ///
        /// ```text
        /// State: Large (HV=tombstone, LT=data)
        /// A: delete "foo"            B: insert small "foo"
        /// A: delete_non_tombstone -> Tombstone
        ///                            B: check HV -> IS tombstone -> route to LT
        /// A: delete from LT          B: write new data to LT
        /// A: delete tombstone from HV
        /// Result: HV=nothing, LT=new data (OrphanLT)
        /// ```
        #[tokio::test]
        async fn race_concurrent_insert_delete_causes_orphan_lt() {
            let shared_hv_store: Arc<Mutex<Store>> = Arc::new(Mutex::new(HashMap::new()));
            let shared_lt_store: Arc<Mutex<Store>> = Arc::new(Mutex::new(HashMap::new()));

            let context = make_context();
            let key = "race-insert-delete";
            let id = ObjectId::new(context.clone(), key.into());

            // Pre-seed Large state.
            {
                let tombstone_meta = Metadata {
                    is_redirect_tombstone: Some(true),
                    ..Default::default()
                };
                shared_hv_store
                    .lock()
                    .unwrap()
                    .insert(id.clone(), (tombstone_meta, Bytes::new()));
                shared_lt_store.lock().unwrap().insert(
                    id.clone(),
                    (Metadata::default(), Bytes::from(super::large_payload())),
                );
            }

            let b_checked_metadata = Arc::new(Notify::new());
            let a_deleted_lt = Arc::new(Notify::new());
            let b_wrote_lt = Arc::new(Notify::new());

            // A (delete): LT delete waits for B's metadata check, signals when done.
            //             HV delete waits for B's LT write.
            let storage_a = TieredStorage {
                high_volume_backend: Box::new(
                    SyncBackend::new("hv-a", Arc::clone(&shared_hv_store))
                        .on_delete_wait(Arc::clone(&b_wrote_lt)),
                ),
                long_term_backend: Box::new(
                    SyncBackend::new("lt-a", Arc::clone(&shared_lt_store))
                        .on_delete(Arc::clone(&b_checked_metadata), Arc::clone(&a_deleted_lt)),
                ),
            };

            // B (insert): HV get_metadata signals when done.
            //             LT put waits for A's LT delete, signals when done.
            let storage_b = TieredStorage {
                high_volume_backend: Box::new(
                    SyncBackend::new("hv-b", Arc::clone(&shared_hv_store))
                        .on_get_metadata_signal(Arc::clone(&b_checked_metadata)),
                ),
                long_term_backend: Box::new(
                    SyncBackend::new("lt-b", Arc::clone(&shared_lt_store))
                        .on_put(Arc::clone(&a_deleted_lt), Arc::clone(&b_wrote_lt)),
                ),
            };

            let task_a = tokio::spawn(async move { storage_a.delete_object(&id).await });
            let task_b = {
                let ctx = context.clone();
                tokio::spawn(async move {
                    storage_b
                        .insert_object(
                            ctx,
                            Some(key.into()),
                            &Default::default(),
                            make_stream(SMALL),
                        )
                        .await
                })
            };

            task_a.await.unwrap().unwrap();
            task_b.await.unwrap().unwrap();

            // TODO(consistency): Prevent concurrent insert+delete from producing
            // OrphanLT, e.g. via per-key serialization or conditional writes.
            let id = ObjectId::new(make_context(), key.into());
            let hv_store = shared_hv_store.lock().unwrap();
            let lt_store = shared_lt_store.lock().unwrap();
            let violation = check_store_invariants(&hv_store, &lt_store, &id);
            assert!(
                violation.unwrap_err().contains("OrphanLT"),
                "concurrent insert+delete must produce OrphanLT"
            );
        }
    }

    // ==========================================
    // Property-based fuzzing
    // ==========================================

    mod proptest_state_machine {
        use std::collections::HashMap;

        use bytes::BytesMut;
        use futures_util::TryStreamExt;
        use objectstore_types::scope::{Scope, Scopes};
        use proptest::prelude::*;

        use super::*;

        #[derive(Debug, Clone, PartialEq)]
        enum KeyState {
            Empty,
            Small(Vec<u8>),
            Large(Vec<u8>),
        }

        #[derive(Debug, Clone)]
        enum Op {
            Insert { key_idx: usize, small: bool },
            Get { key_idx: usize },
            Delete { key_idx: usize },
            GetMetadata { key_idx: usize },
        }

        const KEY_NAMES: &[&str] = &["key-a", "key-b", "key-c"];
        const SMALL_SIZE: usize = 100;
        const LARGE_SIZE: usize = 2 * 1024 * 1024;

        fn arb_op() -> impl Strategy<Value = Op> {
            let key_idx = 0..KEY_NAMES.len();
            prop_oneof![
                (key_idx.clone(), any::<bool>())
                    .prop_map(|(key_idx, small)| Op::Insert { key_idx, small }),
                key_idx.clone().prop_map(|key_idx| Op::Get { key_idx }),
                key_idx.clone().prop_map(|key_idx| Op::Delete { key_idx }),
                key_idx.prop_map(|key_idx| Op::GetMetadata { key_idx }),
            ]
        }

        fn make_payload(key_idx: usize, small: bool) -> Vec<u8> {
            let size = if small { SMALL_SIZE } else { LARGE_SIZE };
            vec![key_idx as u8; size]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn sequential_operations_maintain_invariants(ops in prop::collection::vec(arb_op(), 1..30)) {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async {
                    let hv = InMemoryBackend::new("proptest-hv");
                    let lt = InMemoryBackend::new("proptest-lt");
                    let storage = TieredStorage {
                        high_volume_backend: Box::new(hv.clone()),
                        long_term_backend: Box::new(lt.clone()),
                    };

                    let context = ObjectContext {
                        usecase: "proptest".into(),
                        scopes: Scopes::from_iter([
                            Scope::create("test", "prop").unwrap(),
                        ]),
                    };

                    let ids: Vec<ObjectId> = KEY_NAMES
                        .iter()
                        .map(|&name| ObjectId::new(context.clone(), name.into()))
                        .collect();

                    let mut expected: HashMap<usize, KeyState> = HashMap::new();
                    for i in 0..KEY_NAMES.len() {
                        expected.insert(i, KeyState::Empty);
                    }

                    for op in &ops {
                        match op {
                            Op::Insert { key_idx, small } => {
                                let payload = make_payload(*key_idx, *small);
                                let stream = crate::stream::make_stream(&payload);
                                storage
                                    .insert_object(
                                        context.clone(),
                                        Some(KEY_NAMES[*key_idx].into()),
                                        &Default::default(),
                                        stream,
                                    )
                                    .await
                                    .unwrap();
                                if *small {
                                    expected.insert(*key_idx, KeyState::Small(payload));
                                } else {
                                    expected.insert(*key_idx, KeyState::Large(payload));
                                }
                            }
                            Op::Get { key_idx } => {
                                let result = storage.get_object(&ids[*key_idx]).await.unwrap();
                                match expected.get(key_idx).unwrap() {
                                    KeyState::Empty => {
                                        assert!(result.is_none());
                                    }
                                    KeyState::Small(data) | KeyState::Large(data) => {
                                        let (metadata, stream) = result.unwrap();
                                        assert!(!metadata.is_tombstone());
                                        let body: BytesMut =
                                            stream.try_collect().await.unwrap();
                                        assert_eq!(body.len(), data.len());
                                    }
                                }
                            }
                            Op::Delete { key_idx } => {
                                storage.delete_object(&ids[*key_idx]).await.unwrap();
                                expected.insert(*key_idx, KeyState::Empty);
                            }
                            Op::GetMetadata { key_idx } => {
                                let result =
                                    storage.get_metadata(&ids[*key_idx]).await.unwrap();
                                match expected.get(key_idx).unwrap() {
                                    KeyState::Empty => assert!(result.is_none()),
                                    KeyState::Small(_) | KeyState::Large(_) => {
                                        assert!(!result.unwrap().is_tombstone());
                                    }
                                }
                            }
                        }

                        // After every operation, verify backend invariants.
                        for id in &ids {
                            assert_consistent(&storage, &hv, &lt, id).await;
                        }
                    }
                });
            }
        }
    }

    // ==========================================
    // Concurrent chaos fuzz testing
    // ==========================================

    mod chaos_fuzz {
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        use objectstore_types::metadata::Metadata;
        use objectstore_types::scope::{Scope, Scopes};

        use super::*;

        const KEY_NAMES: &[&str] = &["key-a", "key-b", "key-c"];
        const LARGE_SIZE: usize = super::BACKEND_SIZE_THRESHOLD + 1024;

        fn make_context() -> ObjectContext {
            ObjectContext {
                usecase: "chaos".into(),
                scopes: Scopes::from_iter([Scope::create("test", "chaos").unwrap()]),
            }
        }

        // -- ChaosConfig + ChaosBackend --

        #[derive(Debug, Clone)]
        struct ChaosConfig {
            put_error_pct: u8,
            get_error_pct: u8,
            get_metadata_error_pct: u8,
            delete_error_pct: u8,
        }

        #[derive(Debug)]
        struct ChaosBackend {
            inner: InMemoryBackend,
            config: ChaosConfig,
            call_counter: AtomicU64,
        }

        impl ChaosBackend {
            fn new(inner: InMemoryBackend, config: ChaosConfig) -> Self {
                Self {
                    inner,
                    config,
                    call_counter: AtomicU64::new(0),
                }
            }

            fn should_fail(&self, error_pct: u8) -> bool {
                if error_pct == 0 {
                    return false;
                }
                let count = self.call_counter.fetch_add(1, Ordering::Relaxed);
                let roll = (count.wrapping_mul(6_364_136_223_846_793_005)) % 100;
                roll < error_pct as u64
            }
        }

        #[async_trait::async_trait]
        impl crate::backend::common::Backend for ChaosBackend {
            fn name(&self) -> &'static str {
                "chaos"
            }

            async fn put_object(
                &self,
                id: &ObjectId,
                metadata: &Metadata,
                stream: PayloadStream,
            ) -> crate::error::Result<()> {
                if self.should_fail(self.config.put_error_pct) {
                    return Err(simulated_error("chaos: put_object"));
                }
                // Yield before the actual write to create interleaving
                // windows between the multi-step TieredStorage operations.
                tokio::task::yield_now().await;
                self.inner.put_object(id, metadata, stream).await
            }

            async fn get_object(
                &self,
                id: &ObjectId,
            ) -> crate::error::Result<Option<(Metadata, PayloadStream)>> {
                if self.should_fail(self.config.get_error_pct) {
                    return Err(simulated_error("chaos: get_object"));
                }
                tokio::task::yield_now().await;
                self.inner.get_object(id).await
            }

            async fn get_metadata(&self, id: &ObjectId) -> crate::error::Result<Option<Metadata>> {
                if self.should_fail(self.config.get_metadata_error_pct) {
                    return Err(simulated_error("chaos: get_metadata"));
                }
                tokio::task::yield_now().await;
                self.inner.get_metadata(id).await
            }

            async fn delete_object(&self, id: &ObjectId) -> crate::error::Result<()> {
                if self.should_fail(self.config.delete_error_pct) {
                    return Err(simulated_error("chaos: delete_object"));
                }
                tokio::task::yield_now().await;
                self.inner.delete_object(id).await
            }
        }

        /// Which write operation to perform concurrently.
        #[derive(Clone, Copy)]
        enum WriteOp {
            InsertSmall,
            InsertLarge,
            Delete,
        }

        /// Runs `rounds` independent rounds, each spawning concurrent writes
        /// to the *same* key, and returns a tally of invariant violations.
        ///
        /// All ops target a single key to maximize the chance of interleaving
        /// between the multi-step backend calls inside `TieredStorage`.
        async fn run_chaos_rounds(
            rounds: usize,
            ops: &[WriteOp],
            hv_config: ChaosConfig,
            lt_config: ChaosConfig,
        ) -> HashMap<&'static str, u32> {
            let mut violations: HashMap<&'static str, u32> = HashMap::new();
            let key = KEY_NAMES[0];

            for _ in 0..rounds {
                let hv = InMemoryBackend::new("chaos-hv");
                let lt = InMemoryBackend::new("chaos-lt");

                let storage = Arc::new(TieredStorage {
                    high_volume_backend: Box::new(ChaosBackend::new(hv.clone(), hv_config.clone())),
                    long_term_backend: Box::new(ChaosBackend::new(lt.clone(), lt_config.clone())),
                });

                let context = make_context();

                let handles: Vec<_> = ops
                    .iter()
                    .map(|&op| {
                        let storage = Arc::clone(&storage);
                        let ctx = context.clone();
                        tokio::spawn(async move {
                            match op {
                                WriteOp::InsertSmall => {
                                    let _ = storage
                                        .insert_object(
                                            ctx,
                                            Some(key.into()),
                                            &Default::default(),
                                            make_stream(SMALL),
                                        )
                                        .await;
                                }
                                WriteOp::InsertLarge => {
                                    let payload = vec![0xBB; LARGE_SIZE];
                                    let _ = storage
                                        .insert_object(
                                            ctx,
                                            Some(key.into()),
                                            &Default::default(),
                                            make_stream(&payload),
                                        )
                                        .await;
                                }
                                WriteOp::Delete => {
                                    let id = ObjectId::new(ctx, key.into());
                                    let _ = storage.delete_object(&id).await;
                                }
                            }
                        })
                    })
                    .collect();

                let results = futures_util::future::join_all(handles).await;
                for (i, result) in results.iter().enumerate() {
                    assert!(
                        result.is_ok(),
                        "Task {i} panicked: {:?}",
                        result.as_ref().unwrap_err()
                    );
                }

                let id = ObjectId::new(context, key.into());
                if let Err(msg) = check_invariants(&hv, &lt, &id) {
                    let label = if msg.contains("OrphanLT") {
                        "OrphanLT"
                    } else if msg.contains("DualData") {
                        "DualData"
                    } else {
                        "Unknown"
                    };
                    *violations.entry(label).or_default() += 1;
                }
            }

            violations
        }

        /// No-error config: yield points only, no injected failures.
        fn no_error_config() -> ChaosConfig {
            ChaosConfig {
                put_error_pct: 0,
                get_error_pct: 0,
                get_metadata_error_pct: 0,
                delete_error_pct: 0,
            }
        }

        const ROUNDS: usize = 2000;

        /// Concurrent insert(large) + insert(small) on the same key.
        ///
        /// The race window: A peeks and picks LongTerm, B peeks and picks
        /// HighVolume. B checks HV metadata (no tombstone yet). A writes LT
        /// + tombstone to HV. B overwrites HV with small data, destroying
        /// the tombstone → DualData.
        ///
        /// TODO(consistency): Prevent via per-key serialization or conditional
        /// writes. Flip to `assert!(!violations.contains_key(...))` when fixed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_insert_large_insert_small() {
            use WriteOp::*;
            let violations = run_chaos_rounds(
                ROUNDS,
                &[
                    InsertLarge,
                    InsertSmall,
                    InsertLarge,
                    InsertSmall,
                    InsertLarge,
                    InsertSmall,
                ],
                no_error_config(),
                no_error_config(),
            )
            .await;

            println!("insert_large+insert_small: {violations:?}");
            assert!(
                violations.contains_key("DualData") || violations.contains_key("OrphanLT"),
                "Expected consistency violations from concurrent large+small insert"
            );
        }

        /// Concurrent insert(small) + delete on a key that starts in Large
        /// state (pre-seeded by a preceding insert(large)).
        ///
        /// The race window: delete sees the tombstone and deletes LT data.
        /// Meanwhile insert checks HV metadata, sees the tombstone, and
        /// routes to LT. Delete then removes the HV tombstone. Result:
        /// new data in LT with nothing in HV → OrphanLT.
        ///
        /// TODO(consistency): Prevent via per-key serialization or conditional
        /// writes. Flip to `assert!(!violations.contains_key(...))` when fixed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_insert_delete_from_large_state() {
            use WriteOp::*;
            let violations = run_chaos_rounds(
                ROUNDS,
                // First op seeds Large state; remaining ops race.
                &[InsertLarge, InsertSmall, Delete, InsertSmall, Delete],
                no_error_config(),
                no_error_config(),
            )
            .await;

            println!("insert+delete from large: {violations:?}");
            assert!(
                violations.contains_key("OrphanLT") || violations.contains_key("DualData"),
                "Expected consistency violations from concurrent insert+delete"
            );
        }

        /// Concurrent large inserts with targeted error injection to trigger
        /// the double-failure cleanup path.
        ///
        /// The HV put (tombstone write) sometimes fails. When it does, the
        /// cleanup tries to delete the already-written LT data — but that
        /// also sometimes fails → OrphanLT.
        ///
        /// TODO(consistency): Fix insert cleanup to handle double failure.
        /// Flip to `assert!(!violations.contains_key(...))` when fixed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_inserts_with_tombstone_write_errors() {
            use WriteOp::*;
            // HV: put errors (tombstone write fails), no other errors.
            let hv_config = ChaosConfig {
                put_error_pct: 25,
                get_error_pct: 0,
                get_metadata_error_pct: 0,
                delete_error_pct: 0,
            };
            // LT: delete errors (cleanup fails), no other errors.
            let lt_config = ChaosConfig {
                put_error_pct: 0,
                get_error_pct: 0,
                get_metadata_error_pct: 0,
                delete_error_pct: 25,
            };
            let violations = run_chaos_rounds(
                ROUNDS,
                &[
                    InsertLarge,
                    InsertSmall,
                    InsertLarge,
                    InsertSmall,
                    InsertLarge,
                    InsertSmall,
                ],
                hv_config,
                lt_config,
            )
            .await;

            println!("inserts with tombstone errors: {violations:?}");
            assert!(
                violations.contains_key("OrphanLT") || violations.contains_key("DualData"),
                "Expected consistency violations from tombstone write + cleanup failures"
            );
        }
    }
}
