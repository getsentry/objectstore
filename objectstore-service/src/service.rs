//! Core storage service and configuration.
//!
//! [`StorageService`] is the main entry point for storing and retrieving
//! objects. Each operation runs in a separate tokio task for panic isolation.
//!
//! See the [crate-level documentation](crate) for full architecture details.

use std::future::Future;
use std::sync::Arc;

use objectstore_types::metadata::Metadata;
use objectstore_types::range::{ByteRange, ContentRange};

use crate::backend::common::Backend;
use crate::backend::counting::CountingBackend;
use crate::concurrency::ConcurrencyLimiter;
use crate::error::Result;
use crate::id::{ObjectContext, ObjectId};
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::stream::{ClientStream, PayloadStream};
use crate::streaming::StreamExecutor;

/// Service response for [`StorageService::get_object`].
pub type GetResponse = Option<(Metadata, Option<ContentRange>, PayloadStream)>;
/// Service response for [`StorageService::get_metadata`].
pub type MetadataResponse = Option<Metadata>;
/// Service response for [`StorageService::insert_object`].
pub type InsertResponse = ObjectId;
/// Service response for [`StorageService::delete_object`].
pub type DeleteResponse = ();

/// Default concurrency limit for [`StorageService`].
///
/// This value is used when no explicit limiter is set via
/// [`StorageService::with_concurrency`].
pub const DEFAULT_CONCURRENCY_LIMIT: u32 = 500;

/// Asynchronous storage service wrapping a single [`Backend`].
///
/// `StorageService` is the main entry point for storing and retrieving objects.
/// It delegates all storage operations to the backend supplied at construction,
/// adding task spawning, panic isolation, and a concurrency limit on top.
///
/// The typical backend is [`TieredStorage`](crate::backend::tiered::TieredStorage),
/// which provides size-based routing to high-volume and long-term backends along
/// with redirect tombstone management. Any type implementing [`Backend`] can be used.
///
/// # Lifecycle
///
/// After construction, call [`start`](StorageService::start) to start the
/// service's background processes.
///
/// # Run-to-Completion and Panic Isolation
///
/// Each operation runs to completion even if the caller is cancelled (e.g., on
/// client disconnect). This ensures that multi-step operations in the backend
/// are never left partially applied. Post-commit cleanup (e.g. deleting
/// unreferenced long-term blobs) runs in background tasks so callers are not
/// blocked. Call [`join`](StorageService::join) during shutdown to wait for
/// outstanding cleanup. Operations are also isolated from panics in backend
/// code — a failure in one operation does not bring down other in-flight work.
///
/// # Concurrency Limit
///
/// A [`ConcurrencyLimiter`] caps the number of in-flight backend operations.
/// Pass a custom limiter via
/// [`with_concurrency`](StorageService::with_concurrency); without one the
/// default is [`DEFAULT_CONCURRENCY_LIMIT`] permits with no queue.
#[derive(Clone, Debug)]
pub struct StorageService {
    inner: Arc<dyn Backend>,
    pub(crate) concurrency: ConcurrencyLimiter,
}

impl StorageService {
    /// Creates a new `StorageService` wrapping the given backend.
    ///
    /// The backend is wrapped in a [`CountingBackend`] which increments a COGS usage counter for
    /// each operation run. Single-object operations served directly by `StorageService` are covered
    /// as we batched operations served by [`StreamExecutor`]. See
    /// [`backend::counting`](crate::backend::counting) for details.
    pub fn new(backend: Box<dyn Backend>) -> Self {
        Self {
            inner: Arc::new(CountingBackend::new(backend)),
            concurrency: ConcurrencyLimiter::new(DEFAULT_CONCURRENCY_LIMIT),
        }
    }

    /// Replaces the default concurrency limiter.
    ///
    /// Must be called before [`start`](Self::start). Without this, the
    /// service uses a limiter with [`DEFAULT_CONCURRENCY_LIMIT`] permits
    /// and no queue.
    pub fn with_concurrency(mut self, limiter: ConcurrencyLimiter) -> Self {
        self.concurrency = limiter;
        self
    }

    /// Returns the number of backend tasks currently running.
    pub fn tasks_running(&self) -> u32 {
        self.concurrency.used_permits()
    }

    /// Returns the configured limit for concurrent backend tasks.
    pub fn tasks_limit(&self) -> u32 {
        self.concurrency.total_permits()
    }

    /// Prepares to stream multiple operations concurrently against this service.
    ///
    /// Each operation acquires a bulk permit individually via
    /// [`ConcurrencyLimiter::acquire_bulk`], which caps bulk traffic at a
    /// configurable percentage of execution slots while allowing operations
    /// to queue for permits instead of requiring upfront reservation.
    pub fn stream(&self) -> StreamExecutor {
        StreamExecutor {
            backend: Arc::clone(&self.inner),
            concurrency: self.concurrency.clone(),
        }
    }

    /// Starts background processes for the storage service.
    ///
    /// Spawns a task that emits concurrency gauges once per second:
    /// `service.concurrency.in_use`, `service.concurrency.queued`,
    /// `service.concurrency.bulk_in_use`, `service.concurrency.limit`,
    /// `service.concurrency.queue_limit`, and
    /// `service.concurrency.bulk_limit`.
    pub fn start(&self) {
        let concurrency = self.concurrency.clone();
        objectstore_metrics::gauge!("service.concurrency.limit" = concurrency.total_permits());
        objectstore_metrics::gauge!("service.concurrency.queue_limit" = concurrency.total_queue());
        objectstore_metrics::gauge!("service.concurrency.bulk_limit" = concurrency.total_bulk());

        tokio::spawn(async move {
            concurrency
                .run_emitter(|stats| async move {
                    objectstore_metrics::gauge!("service.concurrency.in_use" = stats.in_use);
                    objectstore_metrics::gauge!("service.concurrency.queued" = stats.queued);
                    objectstore_metrics::gauge!(
                        "service.concurrency.bulk_in_use" = stats.bulk_in_use
                    );
                })
                .await;
        });
    }

    /// Spawns a future in a separate task and awaits its result.
    ///
    /// # Observability
    ///
    /// This tracks two metrics:
    ///
    /// - `service.task.start` (counter) after acquiring a permit
    /// - `service.task.duration` (distribution) when the task completes
    ///
    /// Both are tagged with the given `operation` name and an `outcome`
    /// of `"success"` or `"error"`.
    ///
    /// # Errors
    ///
    /// - `AtCapacity` if the concurrency limit is reached
    /// - `Panic` if the spawned task panics (the panic message is captured for diagnostics)
    /// - `Dropped` if the task is dropped before sending its result.
    async fn spawn<T, F>(&self, operation: &'static str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let timer = objectstore_metrics::timer!("service.concurrency.wait");
        let permit = self.concurrency.acquire().await.inspect_err(|_| {
            objectstore_metrics::count!("service.concurrency.rejected");
            objectstore_log::warn!("Request rejected: service at capacity");
        })?;

        timer.record();
        crate::concurrency::spawn_metered(operation, permit, f).await
    }

    /// Creates or overwrites an object.
    ///
    /// The object is identified by the components of an [`ObjectId`]. The
    /// `context` is required, while the `key` can be assigned automatically if
    /// set to `None`.
    ///
    /// # Run-to-completion
    ///
    /// Once called, the operation runs to completion even if the returned future
    /// is dropped (e.g., on client disconnect). This guarantees that partially
    /// written objects in the backend are never left in an inconsistent state.
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: Metadata,
        stream: ClientStream,
    ) -> Result<InsertResponse> {
        metadata.validate()?;
        let id = ObjectId::optional(context, key);
        let inner = Arc::clone(&self.inner);
        self.spawn("insert", async move {
            inner.put_object(&id, &metadata, stream).await?;
            Ok(id)
        })
        .await
    }

    /// Retrieves only the metadata for an object, without the payload.
    pub async fn get_metadata(&self, id: ObjectId) -> Result<MetadataResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn("get_metadata", async move { inner.get_metadata(&id).await })
            .await
    }

    /// Streams (part of) the contents of an object.
    pub async fn get_object(&self, id: ObjectId, range: Option<ByteRange>) -> Result<GetResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn("get", async move { inner.get_object(&id, range).await })
            .await
    }

    /// Deletes an object, if it exists.
    ///
    /// # Run-to-completion
    ///
    /// Once called, the operation runs to completion even if the returned future
    /// is dropped. This guarantees that multi-step delete sequences in the backend
    /// are never left partially applied.
    pub async fn delete_object(&self, id: ObjectId) -> Result<DeleteResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn("delete", async move { inner.delete_object(&id).await })
            .await
    }

    /// Waits for all outstanding background operations to complete.
    ///
    /// Blocks until any pending background cleanup tasks finish, up to the
    /// backend's configured timeout. Should be called during graceful shutdown
    /// after the HTTP server has stopped accepting new requests.
    pub async fn join(&self) {
        self.inner.join().await;
    }

    // --- Multipart upload operations ---

    /// Initiates a new multipart upload.
    pub async fn initiate_multipart(
        &self,
        id: ObjectId,
        metadata: Metadata,
    ) -> Result<InitiateMultipartResponse> {
        metadata.validate()?;
        self.inner.as_multipart_upload_backend()?; // Fail before clone/spawn if unsupported
        let inner = self.inner.clone();
        self.spawn("initiate_multipart", async move {
            inner
                .as_multipart_upload_backend()?
                .initiate_multipart(&id, &metadata)
                .await
        })
        .await
    }

    /// Uploads a single part.
    ///
    /// Note that this requires a `content_length`.
    /// This grants us the broadest and most seamless compatibility when it comes to backends.
    /// For example, MinIO rejects `UploadPart` requests without a `Content-Length` on plain PUT
    /// requests.
    /// This can be worked around by using AWS SigV4 chunked streaming requests, which we could use
    /// if one day we'll have a usecase where the client doesn't know the part length upfront.
    pub async fn upload_part(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        part_number: PartNumber,
        content_length: u64,
        content_md5: Option<String>,
        body: ClientStream,
    ) -> Result<UploadPartResponse> {
        self.inner.as_multipart_upload_backend()?; // Fail before clone/spawn if unsupported
        let inner = self.inner.clone();
        self.spawn("upload_part", async move {
            inner
                .as_multipart_upload_backend()?
                .upload_part(
                    &id,
                    &upload_id,
                    part_number,
                    content_length,
                    content_md5.as_deref(),
                    body,
                )
                .await
        })
        .await
    }

    /// Lists the parts uploaded so far.
    pub async fn list_parts(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse> {
        self.inner.as_multipart_upload_backend()?; // Fail before clone/spawn if unsupported
        let inner = self.inner.clone();
        self.spawn("list_parts", async move {
            inner
                .as_multipart_upload_backend()?
                .list_parts(&id, &upload_id, max_parts, part_number_marker)
                .await
        })
        .await
    }

    /// Aborts a multipart upload.
    pub async fn abort_multipart(
        &self,
        id: ObjectId,
        upload_id: UploadId,
    ) -> Result<AbortMultipartResponse> {
        self.inner.as_multipart_upload_backend()?; // Fail before clone/spawn if unsupported
        let inner = self.inner.clone();
        self.spawn("abort_multipart", async move {
            inner
                .as_multipart_upload_backend()?
                .abort_multipart(&id, &upload_id)
                .await
        })
        .await
    }

    /// Finalizes a multipart upload.
    pub async fn complete_multipart(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartResponse> {
        self.inner.as_multipart_upload_backend()?; // Fail before clone/spawn if unsupported
        let inner = self.inner.clone();
        self.spawn("complete_multipart", async move {
            inner
                .as_multipart_upload_backend()?
                .complete_multipart(&id, &upload_id, parts)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::Metadata;
    use objectstore_types::range::ByteRange;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::bigtable::{BigTableBackend, BigTableConfig};
    use crate::backend::changelog::NoopChangeLog;
    use crate::backend::common::{HighVolumeBackend, PutResponse, TieredWrite};
    use crate::backend::gcs::{GcsBackend, GcsConfig};
    use crate::backend::in_memory::InMemoryBackend;
    use crate::backend::testing::{Hooks, TestBackend};
    use crate::backend::tiered::TieredStorage;
    use crate::error::Error;
    use crate::stream::{self, ClientStream};

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_service() -> StorageService {
        StorageService::new(Box::new(InMemoryBackend::new("in-memory")))
    }

    #[tokio::test]
    async fn insert_without_key_generates_unique_id() {
        let service = make_service();

        let id = service
            .insert_object(
                make_context(),
                None,
                Metadata::default(),
                stream::single("auto-keyed"),
            )
            .await
            .unwrap();

        assert!(uuid::Uuid::parse_str(id.key()).is_ok());
    }

    #[tokio::test]
    async fn stores_files() {
        let service = make_service();

        let key = service
            .insert_object(
                make_context(),
                Some("testing".into()),
                Metadata::default(),
                stream::single("oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, _, stream) = service.get_object(key, None).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn works_with_gcs() {
        let config = GcsConfig {
            endpoint: Some("http://localhost:8087".into()),
            bucket: "test-bucket".into(), // aligned with the env var in devservices and CI
        };

        let backend = GcsBackend::new(config).await.unwrap();
        let service = StorageService::new(Box::new(backend));

        let key = service
            .insert_object(
                make_context(),
                Some("testing".into()),
                Metadata::default(),
                stream::single("oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, _, stream) = service.get_object(key, None).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn tombstone_redirect_and_delete() {
        let bigtable_config = BigTableConfig {
            endpoint: Some("localhost:8086".into()),
            project_id: "testing".into(),
            instance_name: "objectstore".into(),
            table_name: "objectstore".into(),
            connections: None,
        };
        let gcs_config = GcsConfig {
            endpoint: Some("http://localhost:8087".into()),
            bucket: "test-bucket".into(),
        };

        let high_volume = Box::new(BigTableBackend::new(bigtable_config).await.unwrap());
        let long_term = Box::new(GcsBackend::new(gcs_config.clone()).await.unwrap());
        let backend = TieredStorage::new(high_volume, long_term, Box::new(NoopChangeLog));
        let service = StorageService::new(Box::new(backend));

        // A separate GCS backend to directly inspect the long-term storage.
        let gcs_backend = GcsBackend::new(gcs_config.clone()).await.unwrap();

        // Insert a >1 MiB object with a key.  This forces the long-term path:
        // the real payload goes to GCS, and a redirect tombstone is written to BigTable.
        let payload_len = 2 * 1024 * 1024;
        let payload = vec![0xAB; payload_len]; // 2 MiB
        let id = service
            .insert_object(
                make_context(),
                Some("delete-cleanup-test".into()),
                Metadata::default(),
                stream::single(payload),
            )
            .await
            .unwrap();

        // Sanity: the object is readable through the service (follows the tombstone).
        let (_, _, stream) = service.get_object(id.clone(), None).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload_len);

        // Delete through the service layer.
        service.delete_object(id.clone()).await.unwrap();

        // The tombstone in BigTable should be gone, so the service returns None.
        let after_delete = service.get_object(id.clone(), None).await.unwrap();
        assert!(after_delete.is_none(), "tombstone not deleted");

        // The real object in GCS must also be gone — no orphan.
        let orphan = gcs_backend.get_object(&id, None).await.unwrap();
        assert!(orphan.is_none(), "object leaked");
    }

    // --- Task spawning tests (public API) ---

    #[tokio::test]
    async fn basic_spawn_insert_and_get() {
        let service = make_service();

        let id = service
            .insert_object(
                make_context(),
                Some("test-key".into()),
                Metadata::default(),
                stream::single("hello world"),
            )
            .await
            .unwrap();

        let (_, _, stream) = service.get_object(id, None).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn basic_spawn_metadata_and_delete() {
        let service = make_service();

        let id = service
            .insert_object(
                make_context(),
                Some("meta-key".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await
            .unwrap();

        let metadata = service.get_metadata(id.clone()).await.unwrap();
        assert!(metadata.is_some());

        service.delete_object(id.clone()).await.unwrap();

        let after = service.get_object(id, None).await.unwrap();
        assert!(after.is_none());
    }

    #[derive(Debug)]
    struct PanicOnGet;

    #[async_trait::async_trait]
    impl Hooks for PanicOnGet {
        async fn get_object(
            &self,
            _inner: &InMemoryBackend,
            _id: &ObjectId,
            _range: Option<ByteRange>,
        ) -> Result<GetResponse> {
            panic!("intentional panic in get_object");
        }
    }

    #[tokio::test]
    async fn panic_in_backend_returns_task_failed() {
        let service = StorageService::new(Box::new(TestBackend::new(PanicOnGet)));

        let id = ObjectId::new(make_context(), "panic-test".into());
        let result = service.get_object(id, None).await;

        let Err(Error::Panic(msg)) = result else {
            panic!("expected Panic error");
        };
        assert!(msg.contains("intentional panic in get_object"), "{msg}");
    }

    /// In-memory backend with optional synchronization for `put_object`.
    ///
    /// When `pause` is enabled, each `put_object` call notifies `paused` and
    #[derive(Clone, Debug, Default)]
    struct GateOnPut {
        pause: bool,
        paused: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        on_put: Arc<tokio::sync::Notify>,
    }

    impl GateOnPut {
        fn with_pause() -> Self {
            Self {
                pause: true,
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl Hooks for GateOnPut {
        async fn put_object(
            &self,
            inner: &InMemoryBackend,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            if self.pause {
                self.paused.notify_one();
                self.resume.notified().await;
            }
            inner.put_object(id, metadata, stream).await?;
            self.on_put.notify_one();
            Ok(())
        }

        async fn compare_and_write(
            &self,
            inner: &InMemoryBackend,
            id: &ObjectId,
            current: Option<&ObjectId>,
            write: TieredWrite,
        ) -> Result<bool> {
            let notify = matches!(write, TieredWrite::Tombstone(_) | TieredWrite::Object(_, _));
            let result = inner.compare_and_write(id, current, write).await?;
            if notify {
                self.on_put.notify_one();
            }
            Ok(result)
        }
    }

    #[tokio::test]
    async fn receiver_drop_does_not_prevent_completion() {
        let hv = Box::new(TestBackend::new(GateOnPut::default()));
        let lt = Box::new(TestBackend::new(GateOnPut::with_pause()));
        let backend = TieredStorage::new(hv.clone(), lt.clone(), Box::new(NoopChangeLog));
        let service = StorageService::new(Box::new(backend));

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB → long-term path
        let request = service.insert_object(
            make_context(),
            Some("completion-test".into()),
            Metadata::default(),
            stream::single(payload),
        );

        // Start insert through the public API. select! drops the future once the
        // backend signals it has paused, simulating a client disconnect mid-write.
        let paused = Arc::clone(&lt.hooks.paused);
        tokio::select! {
            _ = request => panic!("insert should not complete while backend is paused"),
            _ = paused.notified() => {}
        }

        // The spawned task is now blocked inside put_object, and the caller
        // request (including the oneshot receiver) has been dropped. Unpause so
        // the task can finish writing.
        lt.hooks.resume.notify_one();

        // Wait for the tombstone write to the high-volume backend, which is the
        // last step of the long-term insert path.
        let on_put = Arc::clone(&hv.hooks.on_put);
        tokio::time::timeout(Duration::from_secs(5), on_put.notified())
            .await
            .expect("timed out waiting for tombstone write");

        // Verify the object was fully written despite the caller being dropped.
        // The tombstone in HV points to the revision key in LT.
        let id = ObjectId::new(make_context(), "completion-test".into());
        let tombstone = hv.inner.get(&id).expect_tombstone();
        let lt_id = tombstone.target;
        assert!(lt.inner.contains(&lt_id), "long-term object missing");
    }

    // --- Concurrency limit tests ---

    fn make_limited_service(limit: u32) -> (StorageService, TestBackend<GateOnPut>) {
        let backend = TestBackend::new(GateOnPut::with_pause());
        let service = StorageService::new(Box::new(backend.clone()))
            .with_concurrency(ConcurrencyLimiter::new(limit));
        (service, backend)
    }

    #[tokio::test]
    async fn at_capacity_rejects() {
        let (service, hv) = make_limited_service(1);

        // First insert blocks on the gated backend, holding the single permit.
        let svc = service.clone();
        let first = tokio::spawn(async move {
            svc.insert_object(
                make_context(),
                Some("first".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await
        });

        // Wait for the backend to signal it has paused (permit is held).
        hv.hooks.paused.notified().await;

        // Second insert should be rejected immediately.
        let result = service
            .insert_object(
                make_context(),
                Some("second".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await;

        assert!(
            matches!(result, Err(Error::AtCapacity)),
            "expected AtCapacity, got {result:?}"
        );

        // Unblock the first operation.
        hv.hooks.resume.notify_one();
        first.await.unwrap().unwrap();

        // Now that the permit is released, a new operation should succeed.
        service
            .get_metadata(ObjectId::new(make_context(), "first".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tasks_limit_returns_configured_limit() {
        let backend = Box::new(InMemoryBackend::new("cap"));
        let service = StorageService::new(backend).with_concurrency(ConcurrencyLimiter::new(7));
        assert_eq!(service.tasks_limit(), 7);
    }

    #[tokio::test]
    async fn tasks_running_tracks_in_flight() {
        let (service, hv) = make_limited_service(5);

        assert_eq!(service.tasks_running(), 0);

        // Kick off a request that blocks in the backend, holding a permit.
        let svc = service.clone();
        let _blocked = tokio::spawn(async move {
            svc.insert_object(
                make_context(),
                Some("in-use-test".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await
        });

        hv.hooks.paused.notified().await;
        assert_eq!(service.tasks_running(), 1);

        hv.hooks.resume.notify_one();
    }

    #[tokio::test]
    async fn permits_released_after_panic() {
        let service = StorageService::new(Box::new(TestBackend::new(PanicOnGet)))
            .with_concurrency(ConcurrencyLimiter::new(1));

        // First operation panics — the permit must still be released.
        let id = ObjectId::new(make_context(), "panic-permit".into());
        let result = service.get_object(id.clone(), None).await;
        assert!(matches!(result, Err(Error::Panic(_))));

        // Second operation should succeed in acquiring the permit (not AtCapacity).
        let result = service.get_object(id, None).await;
        assert!(
            !matches!(result, Err(Error::AtCapacity)),
            "permit was not released after panic"
        );
    }
}
