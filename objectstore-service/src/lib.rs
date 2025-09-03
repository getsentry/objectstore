//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
mod metadata;

use objectstore_types::Metadata;

use std::path::Path;
use std::sync::Arc;

use crate::backend::{BackendStream, BoxedBackend};

pub use backend::BigTableConfig;
pub use metadata::*;

/// High-level asynchronous service for storing and retrieving objects.
#[derive(Clone, Debug)]
pub struct StorageService(Arc<StorageServiceInner>);

#[derive(Debug)]
struct StorageServiceInner {
    backend: BoxedBackend,
}

/// Configuration to initialize a [`StorageService`].
#[derive(Debug)]
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
    pub async fn new(config: StorageConfig<'_>) -> anyhow::Result<Self> {
        let backend = match config {
            StorageConfig::FileSystem { path } => Box::new(backend::LocalFs::new(path)),
            StorageConfig::S3Compatible { endpoint, bucket } => {
                if let Some(endpoint) = endpoint {
                    Box::new(backend::S3Compatible::without_token(endpoint, bucket))
                } else {
                    backend::gcs(bucket).await?
                }
            }
            StorageConfig::BigTable(config) => {
                Box::new(backend::BigTableBackend::new(config).await?)
            }
        };

        let inner = StorageServiceInner { backend };
        Ok(Self(Arc::new(inner)))
    }

    /// Stores or overwrites an object at the given key.
    pub async fn put_object(
        &self,
        key: &ObjectKey,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()> {
        self.0.backend.put_object(&key.key, metadata, stream).await
    }

    /// Streams the contents of an object stored at the given key.
    pub async fn get_object(
        &self,
        key: &ObjectKey,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        self.0.backend.get_object(&key.key).await
    }

    /// Deletes an object stored at the given key, if it exists.
    pub async fn delete_object(&self, key: &ObjectKey) -> anyhow::Result<()> {
        self.0.backend.delete_object(&key.key).await
    }
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

    fn make_key(key: &str) -> ObjectKey {
        ObjectKey {
            usecase: "foo".into(),
            scope: Scope {
                organization: 1234,
                project: None,
            },
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn stores_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_object(&key, &Default::default(), make_stream(b"oh hai!"))
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
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_object(&key, &Default::default(), make_stream(b"oh hai!"))
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
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_object(&key, &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }
}
