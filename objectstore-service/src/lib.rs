//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
mod metadata;

use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::{Metadata, Scope};

use std::path::Path;
use std::sync::Arc;

use crate::backend::{BackendStream, BoxedBackend};

pub use backend::BigTableConfig;
pub use metadata::*;

/// The threshold up until which we will go to the small backend.
const SMALL_THRESHOLD: usize = 50 * 1024; // 50 KiB

/// High-level asynchronous service for storing and retrieving objects.
#[derive(Clone, Debug)]
pub struct StorageService(Arc<StorageServiceInner>);

#[derive(Debug)]
struct StorageServiceInner {
    small_backend: BoxedBackend,
    large_backend: BoxedBackend,
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
        endpoint: Option<&'a str>,
        /// The name of the bucket to use.
        bucket: &'a str,
    },
    /// Use BigTable as storage backend.
    BigTable(BigTableConfig),
}

impl StorageService {
    /// Creates a new `StorageService` with the specified configuration.
    pub async fn new(
        small_config: StorageConfig<'_>,
        large_config: StorageConfig<'_>,
    ) -> anyhow::Result<Self> {
        let small_backend = create_backend(small_config).await?;
        let large_backend = create_backend(large_config).await?;

        let inner = StorageServiceInner {
            small_backend,
            large_backend,
        };
        Ok(Self(Arc::new(inner)))
    }

    /// Stores or overwrites an object at the given key.
    pub async fn put_object(
        &self,
        usecase: String,
        scope: Scope,
        metadata: &Metadata,
        mut stream: BackendStream,
    ) -> anyhow::Result<ScopedKey> {
        let mut first_chunk = BytesMut::new();
        let mut backend_id = 1; // 1 = small files backend
        while let Some(chunk) = stream.try_next().await? {
            first_chunk.extend_from_slice(&chunk);

            if first_chunk.len() > SMALL_THRESHOLD {
                backend_id = 2; // 2 = large files backend
                break;
            }
        }
        let stream = futures_util::stream::once(async { Ok(first_chunk.into()) })
            .chain(stream)
            .boxed();

        let key = ObjectKey::for_backend(backend_id);
        let key = ScopedKey {
            usecase,
            scope,
            key,
        };

        self.0
            .small_backend
            .put_object(&key, metadata, stream)
            .await?;
        Ok(key)
    }

    /// Streams the contents of an object stored at the given key.
    pub async fn get_object(
        &self,
        key: &ScopedKey,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        match key.key.backend {
            1 => self.0.small_backend.get_object(key).await,
            2 => self.0.large_backend.get_object(key).await,
            _ => anyhow::bail!("invalid backend"),
        }
    }

    /// Deletes an object stored at the given key, if it exists.
    pub async fn delete_object(&self, key: &ScopedKey) -> anyhow::Result<()> {
        match key.key.backend {
            1 => self.0.small_backend.delete_object(key).await,
            2 => self.0.large_backend.delete_object(key).await,
            _ => anyhow::bail!("invalid backend"),
        }
    }
}

async fn create_backend(config: StorageConfig<'_>) -> anyhow::Result<BoxedBackend> {
    Ok(match config {
        StorageConfig::FileSystem { path } => Box::new(backend::LocalFs::new(path)),
        StorageConfig::S3Compatible { endpoint, bucket } => {
            if let Some(endpoint) = endpoint {
                Box::new(backend::S3Compatible::without_token(endpoint, bucket))
            } else {
                backend::gcs(bucket).await?
            }
        }
        StorageConfig::BigTable(config) => Box::new(backend::BigTableBackend::new(config).await?),
    })
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use futures_util::{StreamExt, TryStreamExt};
    use objectstore_types::Scope;

    use super::*;

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    #[tokio::test]
    async fn stores_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .put_object(
                "testing".into(),
                Scope {
                    organization: 1234,
                    project: None,
                },
                &Default::default(),
                make_stream(b"oh hai!"),
            )
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[ignore = "gcs credentials are not yet set up in CI"]
    #[tokio::test]
    async fn works_with_gcs() {
        let config = StorageConfig::S3Compatible {
            endpoint: None,
            bucket: "sbx-warp-benchmark-bucket",
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .put_object(
                "testing".into(),
                Scope {
                    organization: 1234,
                    project: None,
                },
                &Default::default(),
                make_stream(b"oh hai!"),
            )
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
            endpoint: Some("http://localhost:8333"),
            bucket: "whatever",
        };
        let service = StorageService::new(config.clone(), config).await.unwrap();

        let key = service
            .put_object(
                "testing".into(),
                Scope {
                    organization: 1234,
                    project: None,
                },
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
