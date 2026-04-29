//! Storage backend implementations.
//!
//! This module contains the [`Backend`](common::Backend) trait and its
//! implementations. Each backend adapts a specific storage system (BigTable,
//! GCS, local filesystem, S3-compatible) to a uniform interface that
//! [`StorageService`](crate::StorageService) consumes.
//!
//! Two-tier routing is encapsulated in [`TieredStorage`](tiered::TieredStorage)
//! and can be configured via [`StorageConfig::Tiered`].

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub mod bigtable;
pub mod changelog;
pub mod common;
pub mod gcs;
pub mod in_memory;
pub mod local_fs;
pub mod s3_compatible;
pub mod tiered;

#[cfg(test)]
pub(crate) mod testing;

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

    /// Tiered storage backend (type `"tiered"`).
    ///
    /// Routes objects across two backends based on size: small objects go to
    /// `high_volume`, large objects go to `long_term`. Nesting `Tiered` inside
    /// another `Tiered` is not supported and will return an error at startup.
    Tiered(tiered::TieredStorageConfig),
}

/// Constructs a type-erased [`Backend`](common::Backend) from the given [`StorageConfig`].
pub async fn from_config(config: StorageConfig) -> Result<Box<dyn common::Backend>> {
    Ok(match config {
        StorageConfig::Tiered(c) => {
            let hv = hv_from_config(c.high_volume).await?;
            let lt = from_leaf_config(*c.long_term).await?;
            let log = Box::new(changelog::NoopChangeLog);
            Box::new(tiered::TieredStorage::new(hv, lt, log))
        }
        // All non-Tiered variants are handled by from_leaf_config. A wildcard
        // is intentional here: any new leaf variant should fall through to
        // from_leaf_config, which will handle it or produce a compile error.
        _ => from_leaf_config(config).await?,
    })
}

async fn from_leaf_config(config: StorageConfig) -> Result<Box<dyn common::Backend>> {
    Ok(match config {
        StorageConfig::FileSystem(c) => Box::new(local_fs::LocalFsBackend::new(c)),
        StorageConfig::S3Compatible(c) => Box::new(s3_compatible::S3CompatibleBackend::new(c)?),
        StorageConfig::Gcs(c) => Box::new(gcs::GcsBackend::new(c).await?),
        StorageConfig::BigTable(c) => Box::new(bigtable::BigTableBackend::new(c).await?),
        StorageConfig::Tiered(_) => anyhow::bail!("nested tiered storage is not supported"),
    })
}

/// Configuration for the high-volume backend in a [`tiered::TieredStorageConfig`].
///
/// Only backends that implement [`common::HighVolumeBackend`] are valid here.
/// Currently this is limited to BigTable.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HighVolumeStorageConfig {
    /// [Google Bigtable] backend.
    ///
    /// [Google Bigtable]: https://cloud.google.com/bigtable
    BigTable(bigtable::BigTableConfig),
}

/// Constructs a type-erased [`common::HighVolumeBackend`] from the given config.
async fn hv_from_config(
    config: HighVolumeStorageConfig,
) -> anyhow::Result<Box<dyn common::HighVolumeBackend>> {
    Ok(match config {
        HighVolumeStorageConfig::BigTable(c) => Box::new(bigtable::BigTableBackend::new(c).await?),
    })
}
