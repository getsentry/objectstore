//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

// TODO(ja): Re-organize modules
mod backend;
pub mod id;

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use objectstore_types::Metadata;

use crate::backend::common::BoxedBackend;
use crate::id::{ObjectContext, ObjectId, ObjectKey};

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

enum BackendChoice {
    HighVolume,
    LongTerm,
}

// TODO(ja): Move into a submodule
/// High-level asynchronous service for storing and retrieving objects.
#[derive(Clone, Debug)]
pub struct StorageService(Arc<StorageServiceInner>);

#[derive(Debug)]
struct StorageServiceInner {
    high_volume_backend: BoxedBackend,
    long_term_backend: BoxedBackend,
}

/// Type alias for data streams used in service APIs.
pub type PayloadStream = BoxStream<'static, std::io::Result<Bytes>>;

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

/// Result type for get operations.
pub type GetResult = anyhow::Result<Option<(Metadata, PayloadStream)>>;
/// Result type for insert operations.
pub type InsertResult = anyhow::Result<ObjectId>;
/// Result type for delete operations.
pub type DeleteResult = anyhow::Result<()>;

/// Type alias to represent a stream of insert operations.
pub type PayloadMetadataStream =
    Pin<Box<dyn Stream<Item = Result<(Metadata, Bytes), anyhow::Error>> + Send>>;
/// Result type for batch insert operations.
pub type BatchInsertResult = anyhow::Result<Vec<InsertResult>>;
/// Result type for batch get operations.
/// TODO: change this
pub type BatchGetResult = anyhow::Result<Vec<GetResult>>;
/// Result type for batch delete operations.
pub type BatchDeleteResult = anyhow::Result<Vec<DeleteResult>>;

impl StorageService {
    /// Creates a new `StorageService` with the specified configuration.
    pub async fn new(
        high_volume_config: StorageConfig<'_>,
        long_term_config: StorageConfig<'_>,
    ) -> anyhow::Result<Self> {
        let high_volume_backend = create_backend(high_volume_config).await?;
        let long_term_backend = create_backend(long_term_config).await?;

        let inner = StorageServiceInner {
            high_volume_backend,
            long_term_backend,
        };
        Ok(Self(Arc::new(inner)))
    }

    /// Creates or overwrites an object.
    ///
    /// The object is identified by the components of an [`ObjectId`]. The `context` is required,
    /// while the `key` can be assigned automatically if set to `None`.
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: &Metadata,
        mut stream: PayloadStream,
    ) -> anyhow::Result<ObjectId> {
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
            let previously_stored_object = self.0.high_volume_backend.get_object(&id).await?;
            if is_tombstoned(&previously_stored_object) {
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

    /// Streams the contents of an object stored at the given key.
    pub async fn get_object(
        &self,
        id: &ObjectId,
    ) -> anyhow::Result<Option<(Metadata, PayloadStream)>> {
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.0.high_volume_backend.name();
        let mut result = self.0.high_volume_backend.get_object(id).await?;

        if is_tombstoned(&result) {
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
    pub async fn delete_object(&self, id: &ObjectId) -> anyhow::Result<()> {
        let start = Instant::now();

        if let Some((metadata, _stream)) = self.0.high_volume_backend.get_object(id).await? {
            if metadata.is_redirect_tombstone == Some(true) {
                self.0.long_term_backend.delete_object(id).await?;
            }
            self.0.high_volume_backend.delete_object(id).await?;
        }

        merni::distribution!(
            "delete.latency"@s: start.elapsed(),
            "usecase" => id.usecase()
        );

        Ok(())
    }
}

fn is_tombstoned(result: &Option<(Metadata, PayloadStream)>) -> bool {
    matches!(
        result,
        Some((
            Metadata {
                is_redirect_tombstone: Some(true),
                ..
            },
            _
        ))
    )
}

async fn create_backend(config: StorageConfig<'_>) -> anyhow::Result<BoxedBackend> {
    Ok(match config {
        StorageConfig::FileSystem { path } => {
            Box::new(backend::local_fs::LocalFsBackend::new(path))
        }
        StorageConfig::S3Compatible { endpoint, bucket } => Box::new(
            backend::s3_compatible::S3CompatibleBackend::without_token(endpoint, bucket),
        ),
        StorageConfig::Gcs { endpoint, bucket } => {
            Box::new(backend::gcs::GcsBackend::new(endpoint, bucket).await?)
        }
        StorageConfig::BigTable {
            endpoint,
            project_id,
            instance_name,
            table_name,
            connections,
        } => Box::new(
            backend::bigtable::BigTableBackend::new(
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
}
