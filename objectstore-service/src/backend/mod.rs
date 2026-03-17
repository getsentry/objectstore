//! Storage backend implementations.
//!
//! This module contains the [`Backend`](common::Backend) trait and its
//! implementations. Each backend adapts a specific storage system (BigTable,
//! GCS, local filesystem, S3-compatible) to a uniform interface that
//! [`StorageService`](crate::StorageService) consumes.
//!
//! Backend methods operate on single objects identified by an
//! [`ObjectId`](crate::id::ObjectId). Unlike the service-level API, backends
//! have no knowledge of the two-tier routing or redirect tombstones — those
//! concerns live in `StorageService`, which coordinates across two backend
//! instances.
//!
//! Backends are type-erased into a [`BoxedBackend`] so the service can work
//! with any combination.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use common::BoxedBackend;

pub mod bigtable;
pub mod common;
pub mod gcs;
pub mod in_memory;
pub mod local_fs;
pub mod s3_compatible;
pub mod tiered;

/// Storage backend configuration.
///
/// The `type` field in YAML or `__TYPE` in environment variables determines which variant is used.
///
/// Used to configure storage backends via [`from_config`].
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    /// Local filesystem storage backend (type `"filesystem"`).
    FileSystem(local_fs::FileSystemConfig),

    /// S3-compatible storage backend (type `"s3compatible"`).
    S3Compatible(s3_compatible::S3CompatibleConfig),

    /// [Google Cloud Storage] backend (type `"gcs"`).
    ///
    /// [Google Cloud Storage]: https://cloud.google.com/storage
    Gcs(gcs::GcsConfig),

    /// [Google Bigtable] backend (type `"bigtable"`).
    ///
    /// [Google Bigtable]: https://cloud.google.com/bigtable
    BigTable(bigtable::BigTableConfig),
}

/// Constructs a [`BoxedBackend`] from the given [`StorageConfig`].
pub async fn from_config(config: StorageConfig) -> Result<BoxedBackend> {
    Ok(match config {
        StorageConfig::FileSystem(c) => Box::new(local_fs::LocalFsBackend::new(c)),
        StorageConfig::S3Compatible(c) => {
            Box::new(s3_compatible::S3CompatibleBackend::without_token(c))
        }
        StorageConfig::Gcs(c) => Box::new(gcs::GcsBackend::new(c).await?),
        StorageConfig::BigTable(c) => Box::new(bigtable::BigTableBackend::new(c).await?),
    })
}
