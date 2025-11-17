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
use std::time::{Duration, Instant};

use crate::backend::common::{BackendStream, BoxedBackend};
use crate::backend::s3_compatible::S3CompatibleBackendConfig;

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
        /// The name of the region to use.
        region: &'a str,
        /// The name of the bucket to use.
        bucket: &'a str,
        /// Optional endpoint URL for the S3-compatible storage.
        endpoint: Option<&'a str>,
        /// Whether to use path-style URLs for the S3-compatible storage.
        use_path_style: bool,
        /// Optional access key for the S3-compatible storage.
        access_key: Option<&'a str>,
        /// Optional secret key for the S3-compatible storage.
        secret_key: Option<&'a str>,
        /// Optional security token for the S3-compatible storage.
        security_token: Option<&'a str>,
        /// Optional session token for the S3-compatible storage.
        session_token: Option<&'a str>,
        /// Optional request timeout for the S3-compatible storage.
        request_timeout_secs: Option<u64>,
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

        // There might currently be a tombstone at the given path from a previously stored object.
        let previously_stored_object = self.0.high_volume_backend.get_object(&path).await?;
        if is_tombstoned(&previously_stored_object) {
            // Write the object to the other backend and keep the tombstone in place
            backend = BackendChoice::LongTerm;
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

                // first write the object
                self.0
                    .long_term_backend
                    .put_object(&path, metadata, stream)
                    .await?;

                let redirect_metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: metadata.expiration_policy,
                    ..Default::default()
                };
                let redirect_stream = futures_util::stream::empty().boxed();
                let redirect_request = self.0.high_volume_backend.put_object(
                    &path,
                    &redirect_metadata,
                    redirect_stream,
                );

                // then we write the tombstone
                let redirect_result = redirect_request.await;
                if redirect_result.is_err() {
                    // and clean up on any kind of error
                    self.0.long_term_backend.delete_object(&path).await?;
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
        let start = Instant::now();

        let mut backend_choice = "high-volume";
        let mut backend_type = self.0.high_volume_backend.name();
        let mut result = self.0.high_volume_backend.get_object(path).await?;

        if is_tombstoned(&result) {
            result = self.0.long_term_backend.get_object(path).await?;
            backend_choice = "long-term";
            backend_type = self.0.long_term_backend.name();
        }

        merni::distribution!(
            "get.latency.pre-response"@s: start.elapsed(),
            "usecase" => path.usecase,
            "backend_choice" => backend_choice,
            "backend_type" => backend_type
        );

        if let Some((metadata, _stream)) = &result {
            if let Some(size) = metadata.size {
                merni::distribution!(
                    "get.size"@b: size,
                    "usecase" => path.usecase,
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
    pub async fn delete_object(&self, path: &ObjectPath) -> anyhow::Result<()> {
        let start = Instant::now();

        if let Some((metadata, _stream)) = self.0.high_volume_backend.get_object(path).await? {
            if metadata.is_redirect_tombstone == Some(true) {
                self.0.long_term_backend.delete_object(path).await?;
            }
            self.0.high_volume_backend.delete_object(path).await?;
        }

        merni::distribution!(
            "delete.latency"@s: start.elapsed(),
            "usecase" => path.usecase
        );

        Ok(())
    }
}

fn is_tombstoned(result: &Option<(Metadata, BackendStream)>) -> bool {
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
        StorageConfig::S3Compatible {
            endpoint,
            bucket,
            region,
            use_path_style,
            access_key,
            secret_key,
            security_token,
            session_token,
            request_timeout_secs,
        } => Box::new(backend::s3_compatible::S3CompatibleBackend::new(
            S3CompatibleBackendConfig {
                bucket: bucket.to_string(),
                region: region.to_string(),
                endpoint: endpoint.map(|s| s.to_string()),
                extra_headers: reqwest::header::HeaderMap::new(),
                request_timeout: request_timeout_secs.map(Duration::from_secs),
                path_style: Some(use_path_style),
                access_key: access_key.map(|s| s.to_string()),
                secret_key: secret_key.map(|s| s.to_string()),
                security_token: security_token.map(|s| s.to_string()),
                session_token: session_token.map(|s| s.to_string()),
            },
        )),
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
}
