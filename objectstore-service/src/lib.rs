//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
mod path;

use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::backend::{BackendStream, BoxedBackend};

pub use path::*;

/// The threshold up until which we will go to the "high volume" backend.
const BACKEND_SIZE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

enum BackendChoice {
    HighVolume,
    LongTerm,
}

/// High-level asynchronous service for storing and retrieving objects.
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

        let inner = StorageServiceInner {
            high_volume_backend,
            long_term_backend,
        };
        Ok(Self(Arc::new(inner)))
    }

    /// Stores or overwrites an object at the given key.
    pub async fn put_object(
        &self,
        path: ObjectPath,
        metadata: &Metadata,
        mut stream: BackendStream,
    ) -> anyhow::Result<ObjectPath> {
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

        let (backend_choice, backend_ty, stored_size) = match backend {
            BackendChoice::HighVolume => {
                let stored_size = first_chunk.len() as u64;
                let stream = futures_util::stream::once(async { Ok(first_chunk.into()) }).boxed();

                self.0
                    .high_volume_backend
                    .put_object(&path, metadata, stream)
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

                self.0
                    .long_term_backend
                    .put_object(&path, metadata, stream)
                    .await?;
                (
                    "long-term",
                    self.0.long_term_backend.name(),
                    stored_size.load(Ordering::Acquire),
                )
            }
        };

        merni::distribution!(
            "put.latency"@s: start.elapsed(),
            "usecase" => path.usecase,
            "backend_choice" => backend_choice,
            "backend_type" => backend_ty
        );
        merni::distribution!(
            "put.size"@b: stored_size,
            "usecase" => path.usecase,
            "backend_choice" => backend_choice,
            "backend_type" => backend_ty
        );

        Ok(path)
    }

    /// Streams the contents of an object stored at the given key.
    pub async fn get_object(
        &self,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        let result = self.0.high_volume_backend.get_object(path).await?;
        if result.is_some() {
            return Ok(result);
        }
        self.0.long_term_backend.get_object(path).await
    }

    /// Deletes an object stored at the given key, if it exists.
    pub async fn delete_object(&self, path: &ObjectPath) -> anyhow::Result<()> {
        // TODO: Don't evaluate `res2` if `res1` succeeds
        let res1 = self.0.high_volume_backend.delete_object(path).await;
        let res2 = self.0.long_term_backend.delete_object(path).await;
        res1.or(res2)
    }

    /// Updates the metadata for an object stored at the given key, if it exists.
    ///
    /// To erase a specific metadata field, set its value to the empty string.
    pub async fn patch_object(&self, path: &ObjectPath, metadata: &Metadata) -> anyhow::Result<()> {
        // TODO: Don't evaluate `res2` if `res1` succeeds
        let res1 = self
            .0
            .high_volume_backend
            .patch_object(path, metadata)
            .await;
        let res2 = self.0.long_term_backend.patch_object(path, metadata).await;
        res1.or(res2)
    }
}

async fn create_backend(config: StorageConfig<'_>) -> anyhow::Result<BoxedBackend> {
    Ok(match config {
        StorageConfig::FileSystem { path } => Box::new(backend::LocalFsBackend::new(path)),
        StorageConfig::S3Compatible { endpoint, bucket } => Box::new(
            backend::S3CompatibleBackend::without_token(endpoint, bucket),
        ),
        StorageConfig::Gcs { endpoint, bucket } => {
            Box::new(backend::GcsBackend::new(endpoint, bucket).await?)
        }
        StorageConfig::BigTable {
            endpoint,
            project_id,
            instance_name,
            table_name,
        } => Box::new(
            backend::BigTableBackend::new(endpoint, project_id, instance_name, table_name).await?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use futures_util::{StreamExt, TryStreamExt};

    use super::*;

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    fn make_path() -> ObjectPath {
        ObjectPath {
            usecase: "testing".into(),
            scope: "testing".into(),
            key: "testing".into(),
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
            .put_object(make_path(), &Default::default(), make_stream(b"oh hai!"))
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
            .put_object(make_path(), &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[ignore = "seadweedfs is not yet set up in CI"]
    #[tokio::test]
    async fn works_with_seaweed() {
        let config = StorageConfig::S3Compatible {
            endpoint: "http://localhost:8333",
            bucket: "whatever",
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .put_object(make_path(), &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }
}
