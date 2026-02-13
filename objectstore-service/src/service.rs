//! Core storage service and configuration.
//!
//! This module contains [`StorageService`], the main entry point for storing and
//! retrieving objects, along with [`StorageConfig`] for backend initialization and
//! response type aliases for the service API.
//!
//! For an overview of the two-tier backend system, redirect tombstones, and
//! consistency guarantees, see the [crate-level documentation](crate).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::backend::common::{BoxedBackend, DeleteOutcome};
use crate::error::Result;
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

/// High-level asynchronous service for storing and retrieving objects.
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

    /// Creates or overwrites an object.
    ///
    /// The object is identified by the components of an [`ObjectId`]. The `context` is required,
    /// while the `key` can be assigned automatically if set to `None`.
    ///
    /// # Tombstone handling
    ///
    /// If the object has a caller-provided key and a redirect tombstone already exists
    /// at that key, the new write is routed to the long-term backend (preserving the
    /// existing tombstone as a redirect to the new data).
    ///
    /// For long-term writes, the real object is persisted first, then the tombstone.
    /// If the tombstone write fails, the real object is rolled back to avoid orphans.
    pub async fn insert_object(
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

    /// Retrieves only the metadata for an object, without downloading the payload.
    ///
    /// # Tombstone handling
    ///
    /// Looks up the object in the high-volume backend first. If the result is a
    /// redirect tombstone, follows the redirect and fetches metadata from the
    /// long-term backend instead.
    pub async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
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

    /// Streams the contents of an object stored at the given key.
    ///
    /// # Tombstone handling
    ///
    /// Looks up the object in the high-volume backend first. If the result is a
    /// redirect tombstone, follows the redirect and fetches the object from the
    /// long-term backend instead.
    pub async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
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

    /// Deletes an object stored at the given key, if it exists.
    ///
    /// # Tombstone handling
    ///
    /// Attempts to delete from the high-volume backend, but skips deletion if the
    /// entry is a redirect tombstone. When a tombstone is found, the long-term
    /// object is deleted first, then the tombstone. This ordering ensures that if
    /// the long-term delete fails, the tombstone remains and the data is still
    /// reachable.
    pub async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use futures_util::{StreamExt, TryStreamExt};

    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend as _;
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
                &Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
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
                &Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    fn make_localfs_service() -> (StorageService, tempfile::TempDir, tempfile::TempDir) {
        let hv_dir = tempfile::tempdir().unwrap();
        let lt_dir = tempfile::tempdir().unwrap();
        let hv = Box::new(crate::backend::local_fs::LocalFsBackend::new(hv_dir.path()));
        let lt = Box::new(crate::backend::local_fs::LocalFsBackend::new(lt_dir.path()));
        (StorageService::from_backends(hv, lt), hv_dir, lt_dir)
    }

    // --- Tombstone inconsistency tests ---

    /// A backend where put_object always fails, but reads/deletes work normally.
    #[derive(Debug)]
    struct FailingPutBackend(crate::backend::local_fs::LocalFsBackend);

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
        let lt_dir = tempfile::tempdir().unwrap();
        let lt_backend_for_inspection =
            crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());

        // High-volume backend always fails on put (simulating BigTable being down).
        // This means the tombstone write will fail after the long-term write succeeds.
        let hv: BoxedBackend = Box::new(FailingPutBackend(
            crate::backend::local_fs::LocalFsBackend::new(tempfile::tempdir().unwrap().path()),
        ));
        let lt: BoxedBackend =
            Box::new(crate::backend::local_fs::LocalFsBackend::new(lt_dir.path()));
        let service = StorageService::from_backends(hv, lt);

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> long-term path
        let result = service
            .insert_object(
                make_context(),
                Some("orphan-test".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await;

        // The insert should fail (tombstone write failed)
        assert!(result.is_err());

        // The long-term object must have been cleaned up — no orphan
        let id = ObjectId::from_parts(
            "testing".into(),
            Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
            "orphan-test".into(),
        );
        let orphan = lt_backend_for_inspection.get_object(&id).await.unwrap();
        assert!(
            orphan.is_none(),
            "long-term object was not cleaned up after tombstone write failure"
        );
    }

    /// If a tombstone exists in high-volume but the corresponding object is
    /// missing from long-term storage (e.g. due to a race condition or partial
    /// cleanup), reads should gracefully return None rather than error.
    #[tokio::test]
    async fn orphan_tombstone_returns_none_on_get() {
        let (service, _hv_dir, lt_dir) = make_localfs_service();
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object(
                make_context(),
                Some("orphan-tombstone".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Manually delete the long-term object, leaving an orphan tombstone
        let lt_backend = crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());
        lt_backend.delete_object(&id).await.unwrap();

        // get_object should gracefully return None, not error
        let result = service.get_object(&id).await.unwrap();
        assert!(
            result.is_none(),
            "orphan tombstone should resolve to None, not return the tombstone"
        );
    }

    /// Same as above but for get_metadata — an orphan tombstone should return
    /// None rather than exposing the tombstone metadata to callers.
    #[tokio::test]
    async fn orphan_tombstone_returns_none_on_get_metadata() {
        let (service, _hv_dir, lt_dir) = make_localfs_service();
        let payload = vec![0xEFu8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object(
                make_context(),
                Some("orphan-tombstone-meta".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Manually delete the long-term object
        let lt_backend = crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());
        lt_backend.delete_object(&id).await.unwrap();

        // get_metadata should gracefully return None
        let result = service.get_metadata(&id).await.unwrap();
        assert!(
            result.is_none(),
            "orphan tombstone metadata should resolve to None"
        );
    }

    // --- Size-based routing tests ---

    #[tokio::test]
    async fn small_object_goes_to_high_volume() {
        let (service, hv_dir, lt_dir) = make_localfs_service();
        let payload = vec![0u8; 100]; // 100 bytes, well under 1 MiB

        let id = service
            .insert_object(
                make_context(),
                Some("small".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Object should be in high-volume
        let hv_backend = crate::backend::local_fs::LocalFsBackend::new(hv_dir.path());
        let result = hv_backend.get_object(&id).await.unwrap();
        assert!(result.is_some(), "small object not in high-volume backend");

        // Object should NOT be in long-term
        let lt_backend = crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());
        let result = lt_backend.get_object(&id).await.unwrap();
        assert!(result.is_none(), "small object leaked to long-term backend");
    }

    #[tokio::test]
    async fn large_object_goes_to_long_term_with_tombstone() {
        let (service, hv_dir, lt_dir) = make_localfs_service();
        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB, over threshold

        let id = service
            .insert_object(
                make_context(),
                Some("large".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Real payload should be in long-term
        let lt_backend = crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());
        let (lt_meta, stream) = lt_backend.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!lt_meta.is_tombstone());

        // A redirect tombstone should exist in high-volume
        let hv_backend = crate::backend::local_fs::LocalFsBackend::new(hv_dir.path());
        let (hv_meta, _) = hv_backend.get_object(&id).await.unwrap().unwrap();
        assert!(hv_meta.is_tombstone());
    }

    #[tokio::test]
    async fn get_follows_tombstone_redirect() {
        let (service, _hv_dir, _lt_dir) = make_localfs_service();
        let payload = vec![0xCDu8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object(
                make_context(),
                Some("redirect-get".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // get_object through service should transparently follow the tombstone
        let (metadata, stream) = service.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!metadata.is_tombstone());
    }

    #[tokio::test]
    async fn get_metadata_follows_tombstone_redirect() {
        let (service, _hv_dir, _lt_dir) = make_localfs_service();
        let payload = vec![0xEFu8; 2 * 1024 * 1024]; // 2 MiB

        let metadata_in = Metadata {
            content_type: "image/png".into(),
            ..Default::default()
        };
        let id = service
            .insert_object(
                make_context(),
                Some("redirect-head".into()),
                &metadata_in,
                make_stream(&payload),
            )
            .await
            .unwrap();

        // get_metadata through service should follow tombstone
        let metadata = service.get_metadata(&id).await.unwrap().unwrap();
        assert!(!metadata.is_tombstone());
        assert_eq!(metadata.content_type, "image/png");
    }

    #[tokio::test]
    async fn delete_cleans_up_both_backends() {
        let (service, hv_dir, lt_dir) = make_localfs_service();
        let payload = vec![0u8; 2 * 1024 * 1024]; // 2 MiB

        let id = service
            .insert_object(
                make_context(),
                Some("delete-both".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        service.delete_object(&id).await.unwrap();

        // Both backends should be empty
        let hv_backend = crate::backend::local_fs::LocalFsBackend::new(hv_dir.path());
        assert!(hv_backend.get_object(&id).await.unwrap().is_none());

        let lt_backend = crate::backend::local_fs::LocalFsBackend::new(lt_dir.path());
        assert!(lt_backend.get_object(&id).await.unwrap().is_none());
    }

    // --- Tombstone preservation on failed long-term delete ---

    /// A backend wrapper that delegates everything except `delete_object`, which always fails.
    #[derive(Debug)]
    struct FailingDeleteBackend(crate::backend::local_fs::LocalFsBackend);

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

        async fn get_object(
            &self,
            id: &ObjectId,
        ) -> Result<Option<(Metadata, PayloadStream)>> {
            self.0.get_object(id).await
        }

        async fn delete_object(&self, _id: &ObjectId) -> Result<()> {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "simulated long-term delete failure",
            )))
        }
    }

    #[tokio::test]
    async fn tombstone_preserved_when_long_term_delete_fails() {
        let hv_dir = tempfile::tempdir().unwrap();
        let lt_dir = tempfile::tempdir().unwrap();
        let hv: BoxedBackend =
            Box::new(crate::backend::local_fs::LocalFsBackend::new(hv_dir.path()));
        let lt: BoxedBackend =
            Box::new(FailingDeleteBackend(crate::backend::local_fs::LocalFsBackend::new(
                lt_dir.path(),
            )));
        let service = StorageService::from_backends(hv, lt);

        let payload = vec![0xABu8; 2 * 1024 * 1024]; // 2 MiB -> goes to long-term
        let id = service
            .insert_object(
                make_context(),
                Some("fail-delete".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Delete should fail because long-term backend refuses deletes
        let result = service.delete_object(&id).await;
        assert!(result.is_err());

        // The tombstone in high-volume must still be present
        let hv_backend = crate::backend::local_fs::LocalFsBackend::new(hv_dir.path());
        let (hv_meta, _) = hv_backend.get_object(&id).await.unwrap().unwrap();
        assert!(
            hv_meta.is_tombstone(),
            "tombstone was removed despite long-term delete failure"
        );

        // The object should still be reachable through the service
        let (metadata, stream) = service.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());
        assert!(!metadata.is_tombstone());
    }

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
            .insert_object(
                make_context(),
                Some("delete-cleanup-test".into()),
                &Default::default(),
                make_stream(&payload),
            )
            .await
            .unwrap();

        // Sanity: the object is readable through the service (follows the tombstone).
        let (_, stream) = service.get_object(&id).await.unwrap().unwrap();
        let body: BytesMut = stream.try_collect().await.unwrap();
        assert_eq!(body.len(), payload.len());

        // Delete through the service layer.
        service.delete_object(&id).await.unwrap();

        // The tombstone in BigTable should be gone, so the service returns None.
        let after_delete = service.get_object(&id).await.unwrap();
        assert!(after_delete.is_none(), "tombstone not deleted");

        // The real object in GCS must also be gone — no orphan.
        let orphan = gcs_backend.get_object(&id).await.unwrap();
        assert!(orphan.is_none(), "object leaked");
    }
}
