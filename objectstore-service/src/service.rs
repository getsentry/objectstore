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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::BytesMut;
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::backend::common::{BoxedBackend, DeleteOutcome};
use crate::error::{Error, Result};
use crate::id::{ObjectContext, ObjectId};

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

enum BackendChoice {
    HighVolume,
    LongTerm,
}

/// Service response for [`StorageService::get_object`].
pub type GetResponse = Option<(Metadata, PayloadStream)>;
/// Service response for [`StorageService::get_metadata`].
pub type MetadataResponse = Option<Metadata>;
/// Service response for [`StorageService::insert_object`].
pub type InsertResponse = ObjectId;
/// Service response for [`StorageService::delete_object`].
pub type DeleteResponse = ();

/// Asynchronous storage service with a two-tier backend system.
///
/// `StorageService` is the main entry point for storing and retrieving objects.
/// It routes objects to a high-volume or long-term backend based on size (see
/// the [crate-level documentation](crate) for details) and maintains redirect
/// tombstones so that reads never need to probe both backends.
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
#[derive(Clone, Debug)]
pub struct StorageService(Arc<StorageServiceInner>);

#[derive(Debug)]
struct StorageServiceInner {
    high_volume_backend: BoxedBackend,
    long_term_backend: BoxedBackend,
}

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
        Self(Arc::new(StorageServiceInner {
            high_volume_backend,
            long_term_backend,
        }))
    }

    /// Spawns a future in a separate task and awaits its result.
    ///
    /// Returns [`Error::Panic`] if the spawned task panics (the panic message
    /// is captured for diagnostics) or [`Error::Cancelled`] if the task is
    /// dropped before sending its result.
    async fn spawn<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(f)
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| Err(Error::Panic(extract_panic_message(payload))));
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::Cancelled)?
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
        let this = self.clone();
        self.spawn(async move {
            this.insert_object_inner(context, key, &metadata, stream)
                .await
        })
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
        let this = self.clone();
        self.spawn(async move { this.get_metadata_inner(&id).await })
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
        let this = self.clone();
        self.spawn(async move { this.get_object_inner(&id).await })
            .await
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
        let this = self.clone();
        self.spawn(async move { this.delete_object_inner(&id).await })
            .await
    }

    // --- Private: inline business logic ---

    async fn insert_object_inner(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: &Metadata,
        mut stream: PayloadStream,
    ) -> Result<InsertResponse> {
        if metadata.origin.is_none() {
            merni::counter!(
                "put.origin_missing": 1,
                "usecase" => context.usecase.as_str()
            );
        }

        let start = Instant::now();

        let mut first_chunk = BytesMut::new();
        let mut backend = BackendChoice::HighVolume;
        while let Some(chunk) = stream.try_next().await? {
            first_chunk.extend_from_slice(&chunk);

            if first_chunk.len() > BACKEND_SIZE_THRESHOLD {
                backend = BackendChoice::LongTerm;
                break;
            }
        }

        let has_key = key.is_some();
        let id = ObjectId::optional(context, key);

        // There might currently be a tombstone at the given path from a previously stored object.
        if has_key {
            let metadata = self.0.high_volume_backend.get_metadata(&id).await?;
            if metadata.is_some_and(|m| m.is_tombstone()) {
                // Write the object to the other backend and keep the tombstone in place
                backend = BackendChoice::LongTerm;
            }
        };

        let (backend_choice, backend_ty, stored_size) = match backend {
            BackendChoice::HighVolume => {
                let stored_size = first_chunk.len() as u64;
                let stream = futures_util::stream::once(async { Ok(first_chunk.into()) }).boxed();

                self.0
                    .high_volume_backend
                    .put_object(&id, metadata, stream)
                    .await?;
                (
                    "high-volume",
                    self.0.high_volume_backend.name(),
                    stored_size,
                )
            }
            BackendChoice::LongTerm => {
                let stored_size = Arc::new(AtomicU64::new(0));
                let stream = futures_util::stream::once(async { Ok(first_chunk.into()) })
                    .chain(stream)
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
                self.0
                    .long_term_backend
                    .put_object(&id, metadata, stream)
                    .await?;

                let redirect_metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: metadata.expiration_policy,
                    ..Default::default()
                };
                let redirect_stream = futures_util::stream::empty().boxed();
                let redirect_request =
                    self.0
                        .high_volume_backend
                        .put_object(&id, &redirect_metadata, redirect_stream);

                // then we write the tombstone
                let redirect_result = redirect_request.await;
                if redirect_result.is_err() {
                    // and clean up on any kind of error
                    self.0.long_term_backend.delete_object(&id).await?;
                }
                redirect_result?;

                (
                    "long-term",
                    self.0.long_term_backend.name(),
                    stored_size.load(Ordering::Acquire),
                )
            }
        };

        merni::distribution!(
            "put.latency"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_ty
        );
        merni::distribution!(
            "put.size"@b: stored_size,
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_ty
        );

        Ok(id)
    }

    async fn get_metadata_inner(&self, id: &ObjectId) -> Result<MetadataResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.0.high_volume_backend.name();
        let mut result = self.0.high_volume_backend.get_metadata(id).await?;

        if result.as_ref().is_some_and(|m| m.is_tombstone()) {
            result = self.0.long_term_backend.get_metadata(id).await?;
            backend_choice = "long-term";
            backend_type = self.0.long_term_backend.name();
        }

        merni::distribution!(
            "head.latency"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        Ok(result)
    }

    async fn get_object_inner(&self, id: &ObjectId) -> Result<GetResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.0.high_volume_backend.name();
        let mut result = self.0.high_volume_backend.get_object(id).await?;

        if result.is_tombstone() {
            result = self.0.long_term_backend.get_object(id).await?;
            backend_choice = "long-term";
            backend_type = self.0.long_term_backend.name();
        }

        merni::distribution!(
            "get.latency.pre-response"@s: start.elapsed(),
            "usecase" => id.usecase(),
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        if let Some((metadata, _stream)) = &result {
            if let Some(size) = metadata.size {
                merni::distribution!(
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

    async fn delete_object_inner(&self, id: &ObjectId) -> Result<DeleteResponse> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.0.high_volume_backend.name();

        let outcome = self.0.high_volume_backend.delete_non_tombstone(id).await?;
        if outcome == DeleteOutcome::Tombstone {
            backend_choice = "long-term";
            backend_type = self.0.long_term_backend.name();
            // Delete the long-term object first, then clean up the tombstone.
            // This ordering ensures that if the long-term delete fails, the
            // tombstone remains and the data is still reachable (not orphaned).
            self.0.long_term_backend.delete_object(id).await?;
            self.0.high_volume_backend.delete_object(id).await?;
        }

        merni::distribution!(
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
    use std::time::Duration;

    use bytes::BytesMut;
    use futures_util::{StreamExt, TryStreamExt};
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend as _;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;

    fn make_stream(contents: &[u8]) -> PayloadStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

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
            .insert_object_inner(
                make_context(),
                Some("testing".into()),
                &Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object_inner(&key).await.unwrap().unwrap();
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
            .insert_object_inner(
                make_context(),
                Some("testing".into()),
                &Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object_inner(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    // --- Basic service behavior ---

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let (service, _hv, _lt) = make_service();
        let id = ObjectId::new(make_context(), "does-not-exist".into());

        assert!(service.get_object_inner(&id).await.unwrap().is_none());
        assert!(service.get_metadata_inner(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_succeeds() {
        let (service, _hv, _lt) = make_service();
        let id = ObjectId::new(make_context(), "does-not-exist".into());

        service.delete_object_inner(&id).await.unwrap();
    }

    #[tokio::test]
    async fn insert_without_key_generates_unique_id() {
        let (service, _hv, _lt) = make_service();

        let id = service
            .insert_object_inner(
                make_context(),
                None,
                &Default::default(),
                make_stream(b"auto-keyed"),
            )
            .await
            .unwrap();

        assert!(uuid::Uuid::parse_str(id.key()).is_ok());

        let (_, stream) = service.get_object_inner(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.as_ref(), b"auto-keyed");
    }

    // --- Size-based routing tests ---

    #[tokio::test]
    async fn small_object_goes_to_high_volume() {
        let (service, hv, lt) = make_service();
        let payload = vec![0u8; 100]; // 100 bytes, well under 1 MiB

        let id = service
            .insert_object_inner(
                make_context(),
                Some("small".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        assert!(hv.contains(&id), "small object not in high-volume backend");
        assert!(
            !lt.contains(&id),
            "small object leaked to long-term backend"
        );
    }

    #[tokio::test]
    async fn large_object_goes_to_long_term_with_tombstone() {
        let (service, hv, lt) = make_service();
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB, over threshold

        let id = service
            .insert_object_inner(
                make_context(),
                Some("large".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Real payload should be in long-term
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert_eq!(lt_bytes.len(), payload.len());
        assert!(!lt_meta.is_tombstone());

        // A redirect tombstone should exist in high-volume
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
    }

    #[tokio::test]
    async fn reinsert_with_existing_tombstone_routes_to_long_term() {
        let (service, hv, lt) = make_service();

        // First: insert a large object → creates tombstone in hv, payload in lt
        let large_payload = vec![0xABu8; 2 * 1024 * 1024];
        let id = service
            .insert_object_inner(
                make_context(),
                Some("reinsert-key".into()),
                &Default::default(),
                make_stream(&large_payload),
            )
            .await
            .unwrap();

        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());

        // Now re-insert a SMALL payload with the same key. The service should
        // detect the existing tombstone and route to long-term anyway.
        let small_payload = vec![0xCDu8; 100]; // well under 1 MiB threshold
        service
            .insert_object_inner(
                make_context(),
                Some("reinsert-key".into()),
                &Default::default(),
                make_stream(&small_payload),
            )
            .await
            .unwrap();

        // The small object should be in long-term (not high-volume)
        let (lt_meta, lt_bytes) = lt.get_stored(&id).unwrap();
        assert!(!lt_meta.is_tombstone());
        assert_eq!(lt_bytes.len(), small_payload.len());

        // The tombstone in hv should still be present
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(hv_meta.is_tombstone());
    }

    #[tokio::test]
    async fn tombstone_inherits_expiration_policy() {
        let (service, hv, lt) = make_service();

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            origin: Some("10.0.0.1".into()),
            ..Default::default()
        };
        let payload = vec![0u8; 2 * 1024 * 1024]; // force long-term

        let id = service
            .insert_object_inner(
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

    // --- Tombstone redirect tests ---

    #[tokio::test]
    async fn reads_follow_tombstone_redirect() {
        let (service, _hv, _lt) = make_service();
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let id = service
            .insert_object_inner(
                make_context(),
                Some("redirect-read".into()),
                &metadata_in,
                make_stream(&payload),
            )
            .await
            .unwrap();

        // get_object should transparently follow the tombstone
        let (metadata, stream) = service.get_object_inner(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!metadata.is_tombstone());

        // get_metadata should also follow the tombstone
        let metadata = service.get_metadata_inner(&id).await.unwrap().unwrap();
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
            _stream: PayloadStream,
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
        let service = StorageService::from_backends(hv, Box::new(lt.clone()));

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> long-term path
        let result = service
            .insert_object_inner(
                make_context(),
                Some("orphan-test".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        assert!(result.is_err());
        assert!(
            lt.is_empty(),
            "long-term object was not cleaned up after tombstone write failure"
        );
    }

    /// If a tombstone exists in high-volume but the corresponding object is
    /// missing from long-term storage (e.g. due to a race condition or partial
    /// cleanup), reads should gracefully return None rather than error.
    #[tokio::test]
    async fn orphan_tombstone_returns_none() {
        let (service, _hv, lt) = make_service();
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object_inner(
                make_context(),
                Some("orphan-tombstone".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Remove the long-term object, leaving an orphan tombstone in hv
        lt.remove(&id);

        assert!(
            service.get_object_inner(&id).await.unwrap().is_none(),
            "orphan tombstone should resolve to None on get_object"
        );
        assert!(
            service.get_metadata_inner(&id).await.unwrap().is_none(),
            "orphan tombstone should resolve to None on get_metadata"
        );
    }

    // --- Delete tests ---

    #[tokio::test]
    async fn delete_cleans_up_both_backends() {
        let (service, hv, lt) = make_service();
        let payload = vec![0u8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object_inner(
                make_context(),
                Some("delete-both".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        service.delete_object_inner(&id).await.unwrap();

        assert!(!hv.contains(&id), "tombstone not cleaned up from hv");
        assert!(!lt.contains(&id), "object not cleaned up from lt");
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
        let service = StorageService::from_backends(Box::new(hv.clone()), lt);

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> goes to long-term
        let id = service
            .insert_object_inner(
                make_context(),
                Some("fail-delete".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        let result = service.delete_object_inner(&id).await;
        assert!(result.is_err());

        // The tombstone in high-volume must still be present
        let (hv_meta, _) = hv.get_stored(&id).unwrap();
        assert!(
            hv_meta.is_tombstone(),
            "tombstone was removed despite long-term delete failure"
        );

        // The object should still be reachable through the service
        let (metadata, stream) = service.get_object_inner(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!metadata.is_tombstone());
    }

    // --- Integration test (real emulated backends) ---

    #[tokio::test]
    async fn test_tombstone_redirect_and_delete() {
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
            .insert_object_inner(
                make_context(),
                Some("delete-cleanup-test".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Sanity: the object is readable through the service (follows the tombstone).
        let (_, stream) = service.get_object_inner(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());

        // Delete through the service layer.
        service.delete_object_inner(&id).await.unwrap();

        // The tombstone in BigTable should be gone, so the service returns None.
        let after_delete = service.get_object_inner(&id).await.unwrap();
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

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected Panic error"),
        };
        match err {
            Error::Panic(msg) => {
                assert!(
                    msg.contains("intentional panic in get_object"),
                    "panic message should be captured, got: {msg}"
                );
            }
            other => panic!("expected Panic, got: {other}"),
        }
    }

    #[tokio::test]
    async fn receiver_drop_does_not_prevent_completion() {
        let (service, hv, lt) = make_service();
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB → long-term path

        // Spawn the insert manually so we can observe completion after dropping
        // the receiver. We replicate the spawn pattern but call the inner method
        // directly to control the oneshot ourselves.
        let svc = service.clone();
        let context = make_context();
        let stream = make_stream(&payload);

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<InsertResponse>>();
        tokio::spawn(async move {
            let result = svc
                .insert_object_inner(
                    context,
                    Some("completion-test".into()),
                    &Metadata::default(),
                    stream,
                )
                .await;
            let _ = tx.send(result);
        });

        // Drop the receiver immediately, simulating a cancelled web handler.
        drop(rx);

        // Poll until the spawned task completes instead of using a fixed sleep.
        let id = ObjectId::new(make_context(), "completion-test".into());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if lt.contains(&id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Verify the object was fully written despite the receiver being dropped.
        assert!(
            lt.contains(&id),
            "long-term object missing after receiver drop"
        );
        // High-volume backend should have the redirect tombstone.
        let (hv_meta, _) = hv
            .get_stored(&id)
            .expect("high-volume entry missing after receiver drop");
        assert!(
            hv_meta.is_tombstone(),
            "tombstone missing after receiver drop"
        );
    }
}
