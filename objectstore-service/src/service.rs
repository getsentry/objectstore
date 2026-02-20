//! Core storage service and configuration.
//!
//! [`StorageService`] is the main entry point for storing and retrieving
//! objects. Each operation runs in a separate tokio task for panic isolation.
//! See the [crate-level documentation](crate) for the two-tier backend system,
//! redirect tombstones, and consistency guarantees.

use std::any::Any;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use futures_util::FutureExt;
use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::backend::common::BoxedBackend;
use crate::concurrency::ConcurrencyLimiter;
use crate::error::{Error, Result};
use crate::id::{ObjectContext, ObjectId};
use crate::tiered::TieredStorage;

/// Service response for [`StorageService::get_object`].
pub type GetResponse = Option<(Metadata, PayloadStream)>;
/// Service response for [`StorageService::get_metadata`].
pub type MetadataResponse = Option<Metadata>;
/// Service response for [`StorageService::insert_object`].
pub type InsertResponse = ObjectId;
/// Service response for [`StorageService::delete_object`].
pub type DeleteResponse = ();

/// Configuration to initialize a [`StorageService`].
#[derive(Debug, Clone)]
pub enum StorageConfig<'a> {
    /// Use a local filesystem as the storage backend.
    FileSystem {
        /// The path to the directory where files will be stored.
        path: &'a Path,
    },
    /// Use an S3-compatible storage backend.
    S3Compatible {
        /// Optional endpoint URL for the S3-compatible storage.
        endpoint: &'a str,
        /// The name of the bucket to use.
        bucket: &'a str,
    },
    /// Use Google Cloud Storage as storage backend.
    Gcs {
        /// Optional endpoint URL for the S3-compatible storage.
        ///
        /// Assumes an emulator without authentication if set.
        endpoint: Option<&'a str>,
        /// The name of the bucket to use.
        bucket: &'a str,
    },
    /// Use BigTable as storage backend.
    BigTable {
        /// Optional endpoint URL for the BigTable storage.
        ///
        /// Assumes an emulator without authentication if set.
        endpoint: Option<&'a str>,
        /// The Google Cloud project ID.
        project_id: &'a str,
        /// The BigTable instance name.
        instance_name: &'a str,
        /// The BigTable table name.
        table_name: &'a str,
        /// The number of concurrent connections to BigTable.
        ///
        /// Defaults to 2x the number of worker threads.
        connections: Option<usize>,
    },
}

/// Default concurrency limit for [`StorageService`].
///
/// This value is used when no explicit limit is set via
/// [`StorageService::with_concurrency_limit`].
pub const DEFAULT_CONCURRENCY_LIMIT: usize = 500;

/// Asynchronous storage service with a two-tier backend system.
///
/// `StorageService` is the main entry point for storing and retrieving objects.
/// It routes objects to a high-volume or long-term backend based on size (see
/// the [crate-level documentation](crate) for details) and maintains redirect
/// tombstones so that reads never need to probe both backends.
///
/// # Lifecycle
///
/// After construction, call [`start`](StorageService::start) to start the
/// service's background processes.
///
/// # Redirect Tombstones
///
/// Because the [`ObjectId`] is backend-independent, reads must be able to find
/// an object without knowing which backend stores it. A naive approach would
/// check the long-term backend on every read miss in the high-volume backend —
/// but that is slow and expensive.
///
/// Instead, when an object is stored in the long-term backend, the service
/// writes a **redirect tombstone** in the high-volume backend. A redirect
/// tombstone is an empty object with
/// [`is_redirect_tombstone: true`](objectstore_types::metadata::Metadata::is_redirect_tombstone)
/// in its metadata. It acts as a signpost: "the real data lives in the other
/// backend."
///
/// # Consistency Without Locks
///
/// The tombstone system maintains consistency through operation ordering rather
/// than distributed locks. The invariant is: a redirect tombstone is always the
/// **last thing written** and the **last thing removed**.
///
/// - On **write**, the real object is persisted before the tombstone. If the
///   tombstone write fails, the real object is rolled back.
/// - On **delete**, the real object is removed before the tombstone. If the
///   long-term delete fails, the tombstone remains and the data stays reachable.
///
/// This ensures that at every intermediate step, either the data is fully
/// reachable (tombstone points to data) or fully absent — never an orphan in
/// either direction.
///
/// See the individual methods for per-operation tombstone behavior.
///
/// # Run-to-Completion and Panic Isolation
///
/// Each operation runs to completion even if the caller is cancelled (e.g., on
/// client disconnect). This ensures that multi-step operations such as writing
/// redirect tombstones are never left partially applied. Operations are also
/// isolated from panics in backend code — a failure in one operation does not
/// bring down other in-flight work. See [`Error::Panic`].
///
/// # Concurrency Limit
///
/// A semaphore caps the number of in-flight backend operations. The limit is
/// configured via [`with_concurrency_limit`](StorageService::with_concurrency_limit);
/// without an explicit value the default is [`DEFAULT_CONCURRENCY_LIMIT`].
/// Operations that exceed the limit are rejected immediately with
/// [`Error::AtCapacity`].
#[derive(Clone, Debug)]
pub struct StorageService {
    inner: Arc<TieredStorage>,
    concurrency: ConcurrencyLimiter,
}

impl StorageService {
    /// Creates a new `StorageService` with the specified configuration.
    pub async fn new(
        high_volume_config: StorageConfig<'_>,
        long_term_config: StorageConfig<'_>,
    ) -> anyhow::Result<Self> {
        let high_volume_backend = create_backend(high_volume_config).await?;
        let long_term_backend = create_backend(long_term_config).await?;
        Ok(Self::from_backends(high_volume_backend, long_term_backend))
    }

    fn from_backends(high_volume_backend: BoxedBackend, long_term_backend: BoxedBackend) -> Self {
        Self {
            inner: Arc::new(TieredStorage {
                high_volume_backend,
                long_term_backend,
            }),
            concurrency: ConcurrencyLimiter::new(DEFAULT_CONCURRENCY_LIMIT),
        }
    }

    /// Sets the maximum number of concurrent backend operations.
    ///
    /// Must be called before [`run`](Self::run). Operations beyond this
    /// limit are rejected with [`Error::AtCapacity`].
    pub fn with_concurrency_limit(mut self, max: usize) -> Self {
        self.concurrency = ConcurrencyLimiter::new(max);
        self
    }

    /// Starts background processes for the storage service.
    ///
    /// Currently spawns a task that emits the `service.concurrency.in_use`
    /// gauge once per second.
    pub fn start(&self) {
        let concurrency = self.concurrency.clone();
        tokio::spawn(async move {
            concurrency
                .run_emitter(|permits| async move {
                    merni::gauge!("service.concurrency.in_use": permits);
                })
                .await;
        });
    }

    /// Spawns a future in a separate task and awaits its result.
    ///
    /// Returns [`Error::AtCapacity`] if the concurrency limit is reached,
    /// [`Error::Panic`] if the spawned task panics (the panic message
    /// is captured for diagnostics), or [`Error::Dropped`] if the task is
    /// dropped before sending its result.
    async fn spawn<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let permit = self.concurrency.try_acquire().inspect_err(|_| {
            merni::counter!("service.concurrency.rejected": 1);
        })?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(f)
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| Err(Error::Panic(extract_panic_message(payload))));
            let _ = tx.send(result);
            drop(permit);
        });
        rx.await.map_err(|_| Error::Dropped)?
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
    /// written objects are never left without their redirect tombstone.
    ///
    /// # Tombstone handling
    ///
    /// If the object has a caller-provided key and a redirect tombstone already
    /// exists at that key, the new write is routed to the long-term backend
    /// (preserving the existing tombstone as a redirect to the new data).
    ///
    /// For long-term writes, the real object is persisted first, then the
    /// tombstone. If the tombstone write fails, the real object is rolled back
    /// to avoid orphans.
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: Metadata,
        stream: PayloadStream,
    ) -> Result<InsertResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn(async move { inner.insert_object(context, key, &metadata, stream).await })
            .await
    }

    /// Retrieves only the metadata for an object, without the payload.
    ///
    /// # Tombstone handling
    ///
    /// Looks up the object in the high-volume backend first. If the result is a
    /// redirect tombstone, follows the redirect and fetches metadata from the
    /// long-term backend instead.
    pub async fn get_metadata(&self, id: ObjectId) -> Result<MetadataResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn(async move { inner.get_metadata(&id).await })
            .await
    }

    /// Streams the contents of an object.
    ///
    /// # Tombstone handling
    ///
    /// Looks up the object in the high-volume backend first. If the result is a
    /// redirect tombstone, follows the redirect and fetches the object from the
    /// long-term backend instead.
    pub async fn get_object(&self, id: ObjectId) -> Result<GetResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn(async move { inner.get_object(&id).await }).await
    }

    /// Deletes an object, if it exists.
    ///
    /// # Run-to-completion
    ///
    /// Once called, the operation runs to completion even if the returned future
    /// is dropped. This guarantees that the tombstone is only removed after the
    /// long-term object has been successfully deleted.
    ///
    /// # Tombstone handling
    ///
    /// Attempts to delete from the high-volume backend, but skips deletion if
    /// the entry is a redirect tombstone. When a tombstone is found, the
    /// long-term object is deleted first, then the tombstone. This ordering
    /// ensures that if the long-term delete fails, the tombstone remains and
    /// the data is still reachable.
    pub async fn delete_object(&self, id: ObjectId) -> Result<DeleteResponse> {
        let inner = Arc::clone(&self.inner);
        self.spawn(async move { inner.delete_object(&id).await })
            .await
    }
}

async fn create_backend(config: StorageConfig<'_>) -> anyhow::Result<BoxedBackend> {
    Ok(match config {
        StorageConfig::FileSystem { path } => {
            Box::new(crate::backend::local_fs::LocalFsBackend::new(path))
        }
        StorageConfig::S3Compatible { endpoint, bucket } => Box::new(
            crate::backend::s3_compatible::S3CompatibleBackend::without_token(endpoint, bucket),
        ),
        StorageConfig::Gcs { endpoint, bucket } => {
            Box::new(crate::backend::gcs::GcsBackend::new(endpoint, bucket).await?)
        }
        StorageConfig::BigTable {
            endpoint,
            project_id,
            instance_name,
            table_name,
            connections,
        } => Box::new(
            crate::backend::bigtable::BigTableBackend::new(
                endpoint,
                project_id,
                instance_name,
                table_name,
                connections,
            )
            .await?,
        ),
    })
}

/// Extracts a human-readable message from a panic payload.
fn extract_panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::Metadata;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend as _;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;
    use crate::stream::make_stream;

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_service() -> (StorageService, InMemoryBackend, InMemoryBackend) {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        let service = StorageService::from_backends(Box::new(hv.clone()), Box::new(lt.clone()));
        (service, hv, lt)
    }

    // --- Integration tests (real backends) ---

    #[tokio::test]
    async fn stores_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .insert_object(
                make_context(),
                Some("testing".into()),
                Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn works_with_gcs() {
        let config = StorageConfig::Gcs {
            endpoint: Some("http://localhost:8087"),
            bucket: "test-bucket", // aligned with the env var in devservices and CI
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .insert_object(
                make_context(),
                Some("testing".into()),
                Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn tombstone_redirect_and_delete() {
        let high_volume = StorageConfig::BigTable {
            endpoint: Some("localhost:8086"),
            project_id: "testing",
            instance_name: "objectstore",
            table_name: "objectstore",
            connections: None,
        };
        let long_term = StorageConfig::Gcs {
            endpoint: Some("http://localhost:8087"),
            bucket: "test-bucket",
        };
        let service = StorageService::new(high_volume, long_term).await.unwrap();

        // A separate GCS backend to directly inspect the long-term storage.
        let gcs_backend =
            crate::backend::gcs::GcsBackend::new(Some("http://localhost:8087"), "test-bucket")
                .await
                .unwrap();

        // Insert a >1 MiB object with a key.  This forces the long-term path:
        // the real payload goes to GCS, and a redirect tombstone is written to BigTable.
        let payload = vec![0xAB; 2 * 1024 * 1024]; // 2 MiB
        let id = service
            .insert_object(
                make_context(),
                Some("delete-cleanup-test".into()),
                Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Sanity: the object is readable through the service (follows the tombstone).
        let (_, stream) = service.get_object(id.clone()).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());

        // Delete through the service layer.
        service.delete_object(id.clone()).await.unwrap();

        // The tombstone in BigTable should be gone, so the service returns None.
        let after_delete = service.get_object(id.clone()).await.unwrap();
        assert!(after_delete.is_none(), "tombstone not deleted");

        // The real object in GCS must also be gone — no orphan.
        let orphan = gcs_backend.get_object(&id).await.unwrap();
        assert!(orphan.is_none(), "object leaked");
    }

    // --- Task spawning tests (public API) ---

    #[tokio::test]
    async fn basic_spawn_insert_and_get() {
        let (service, _hv, _lt) = make_service();

        let id = service
            .insert_object(
                make_context(),
                Some("test-key".into()),
                Metadata::default(),
                make_stream(b"hello world"),
            )
            .await
            .unwrap();

        let (_, stream) = service.get_object(id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn basic_spawn_metadata_and_delete() {
        let (service, _hv, _lt) = make_service();

        let id = service
            .insert_object(
                make_context(),
                Some("meta-key".into()),
                Metadata::default(),
                make_stream(b"data"),
            )
            .await
            .unwrap();

        let metadata = service.get_metadata(id.clone()).await.unwrap();
        assert!(metadata.is_some());

        service.delete_object(id.clone()).await.unwrap();

        let after = service.get_object(id).await.unwrap();
        assert!(after.is_none());
    }

    /// A backend that panics on `get_object` to verify panic isolation.
    #[derive(Debug)]
    struct PanickingBackend;

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for PanickingBackend {
        fn name(&self) -> &'static str {
            "panicking"
        }

        async fn put_object(
            &self,
            _id: &ObjectId,
            _metadata: &Metadata,
            _stream: PayloadStream,
        ) -> Result<()> {
            Ok(())
        }

        async fn get_object(&self, _id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            panic!("intentional panic in get_object");
        }

        async fn delete_object(&self, _id: &ObjectId) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn panic_in_backend_returns_task_failed() {
        let service =
            StorageService::from_backends(Box::new(PanickingBackend), Box::new(PanickingBackend));

        let id = ObjectId::new(make_context(), "panic-test".into());
        let result = service.get_object(id).await;

        let Err(Error::Panic(msg)) = result else {
            panic!("expected Panic error");
        };
        assert!(msg.contains("intentional panic in get_object"), "{msg}");
    }

    /// In-memory backend with optional synchronization for `put_object`.
    ///
    /// When `pause` is enabled, each `put_object` call notifies `paused` and
    /// then waits on `resume` before proceeding. After the write completes,
    /// `on_put` is always notified regardless of the `pause` setting.
    #[derive(Debug, Clone)]
    struct GatedBackend {
        inner: InMemoryBackend,
        pause: bool,
        paused: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        on_put: Arc<tokio::sync::Notify>,
    }

    impl GatedBackend {
        fn new(name: &'static str) -> Self {
            Self {
                inner: InMemoryBackend::new(name),
                pause: false,
                paused: Arc::new(tokio::sync::Notify::new()),
                resume: Arc::new(tokio::sync::Notify::new()),
                on_put: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn with_pause(mut self) -> Self {
            self.pause = true;
            self
        }
    }

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for GatedBackend {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: PayloadStream,
        ) -> Result<()> {
            if self.pause {
                self.paused.notify_one();
                self.resume.notified().await;
            }
            self.inner.put_object(id, metadata, stream).await?;
            self.on_put.notify_one();
            Ok(())
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.inner.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            self.inner.delete_object(id).await
        }
    }

    #[tokio::test]
    async fn receiver_drop_does_not_prevent_completion() {
        let hv = GatedBackend::new("gated-hv");
        let lt = GatedBackend::new("gated-lt").with_pause();
        let service = StorageService::from_backends(Box::new(hv.clone()), Box::new(lt.clone()));

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB → long-term path
        let request = service.insert_object(
            make_context(),
            Some("completion-test".into()),
            Metadata::default(),
            make_stream(&payload),
        );

        // Start insert through the public API. select! drops the future once the
        // backend signals it has paused, simulating a client disconnect mid-write.
        let paused = Arc::clone(&lt.paused);
        tokio::select! {
            _ = request => panic!("insert should not complete while backend is paused"),
            _ = paused.notified() => {}
        }

        // The spawned task is now blocked inside put_object, and the caller
        // request (including the oneshot receiver) has been dropped. Unpause so
        // the task can finish writing.
        lt.resume.notify_one();

        // Wait for the tombstone write to the high-volume backend, which is the
        // last step of the long-term insert path.
        let on_put = Arc::clone(&hv.on_put);
        tokio::time::timeout(Duration::from_secs(5), on_put.notified())
            .await
            .expect("timed out waiting for tombstone write");

        // Verify the object was fully written despite the caller being dropped.
        let id = ObjectId::new(make_context(), "completion-test".into());
        assert!(lt.inner.contains(&id), "long-term object missing");
        let (meta, _) = hv.inner.get_stored(&id).expect("tombstone missing");
        assert!(meta.is_tombstone(), "expected redirect tombstone");
    }

    // --- Concurrency limit tests ---

    fn make_limited_service(limit: usize) -> (StorageService, GatedBackend, GatedBackend) {
        let hv = GatedBackend::new("limited-hv").with_pause();
        let lt = GatedBackend::new("limited-lt");
        let service = StorageService::from_backends(Box::new(hv.clone()), Box::new(lt.clone()))
            .with_concurrency_limit(limit);
        (service, hv, lt)
    }

    #[tokio::test]
    async fn at_capacity_rejects() {
        let (service, hv, _lt) = make_limited_service(1);

        // First insert blocks on the gated backend, holding the single permit.
        let svc = service.clone();
        let first = tokio::spawn(async move {
            svc.insert_object(
                make_context(),
                Some("first".into()),
                Metadata::default(),
                make_stream(b"data"),
            )
            .await
        });

        // Wait for the backend to signal it has paused (permit is held).
        hv.paused.notified().await;

        // Second insert should be rejected immediately.
        let result = service
            .insert_object(
                make_context(),
                Some("second".into()),
                Metadata::default(),
                make_stream(b"data"),
            )
            .await;

        assert!(
            matches!(result, Err(Error::AtCapacity)),
            "expected AtCapacity, got {result:?}"
        );

        // Unblock the first operation.
        hv.resume.notify_one();
        first.await.unwrap().unwrap();

        // Now that the permit is released, a new operation should succeed.
        service
            .get_metadata(ObjectId::new(make_context(), "first".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn permits_released_after_panic() {
        let service =
            StorageService::from_backends(Box::new(PanickingBackend), Box::new(PanickingBackend))
                .with_concurrency_limit(1);

        // First operation panics — the permit must still be released.
        let id = ObjectId::new(make_context(), "panic-permit".into());
        let result = service.get_object(id.clone()).await;
        assert!(matches!(result, Err(Error::Panic(_))));

        // Second operation should succeed in acquiring the permit (not AtCapacity).
        let result = service.get_object(id).await;
        assert!(
            !matches!(result, Err(Error::AtCapacity)),
            "permit was not released after panic"
        );
    }
}
