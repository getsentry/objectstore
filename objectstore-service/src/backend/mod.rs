//! Storage backend implementations.
//!
//! This module contains the [`Backend`](common::Backend) trait and its
//! implementations. Each backend adapts a specific storage system (BigTable,
//! GCS, local filesystem, S3-compatible) to a uniform interface that
//! [`StorageService`](crate::StorageService) consumes.
//!
//! Two-tier routing is encapsulated in [`TieredStorage`](tiered::TieredStorage)
//! and can be configured via [`StorageConfig::Tiered`].

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::change_stream::{ChangeStream, ChangeStreamFactory};

pub mod bigtable;
pub mod changelog;
pub mod common;
pub mod counting;
mod extensions;
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

/// The role-independent facts about a leaf backend that service wiring needs.
struct LeafInfo {
    /// Suffix identifying the backend type in a `role:type` key.
    kind: &'static str,
    /// Whether the backend lacks reliable native expiration and needs the garbage collector.
    needs_gc: bool,
    /// Whether the backend has cost-tracking configuration.
    cogs: bool,
}

/// Returns the wiring facts for a leaf backend.
fn leaf_info(config: &StorageConfig) -> Result<LeafInfo> {
    Ok(match config {
        StorageConfig::FileSystem(c) => LeafInfo {
            kind: "filesystem",
            needs_gc: true,
            cogs: c.cogs.is_some(),
        },
        StorageConfig::S3Compatible(c) => LeafInfo {
            kind: "s3compatible",
            needs_gc: true,
            cogs: c.cogs.is_some(),
        },
        StorageConfig::Gcs(c) => LeafInfo {
            kind: "gcs",
            needs_gc: false,
            cogs: c.cogs.is_some(),
        },
        StorageConfig::BigTable(c) => LeafInfo {
            kind: "bigtable",
            needs_gc: false,
            cogs: c.cogs.is_some(),
        },
        StorageConfig::Tiered(_) => anyhow::bail!("nested tiered storage is not supported"),
    })
}

/// Returns the wiring facts for a high-volume backend.
fn high_volume_info(config: &HighVolumeStorageConfig) -> LeafInfo {
    match config {
        HighVolumeStorageConfig::BigTable(c) => LeafInfo {
            kind: "bigtable",
            needs_gc: false,
            cogs: c.cogs.is_some(),
        },
    }
}

/// Builds the key that identifies a backend's garbage-collector index.
fn gc_key(role: &str, kind: &str) -> String {
    format!("{role}:{kind}")
}

/// One leaf's garbage-collection requirements.
struct GcRequirement {
    /// The `role:backend-type` key identifying the leaf.
    key: String,
    /// Whether the backend needs the garbage collector.
    needs_gc: bool,
    /// Whether the backend has cost-tracking configuration.
    cogs: bool,
}

/// Returns the garbage-collection requirements for every leaf in the storage tree.
fn gc_requirements(config: &StorageConfig) -> Result<Vec<GcRequirement>> {
    let mut requirements = Vec::new();
    match config {
        StorageConfig::Tiered(c) => {
            let hv = high_volume_info(&c.high_volume);
            requirements.push(GcRequirement {
                key: gc_key("storage.high_volume", hv.kind),
                needs_gc: hv.needs_gc,
                cogs: hv.cogs,
            });

            let lt = leaf_info(&c.long_term)?;
            requirements.push(GcRequirement {
                key: gc_key("storage.long_term", lt.kind),
                needs_gc: lt.needs_gc,
                cogs: lt.cogs,
            });
        }
        leaf => {
            let info = leaf_info(leaf)?;
            requirements.push(GcRequirement {
                key: gc_key("storage", info.kind),
                needs_gc: info.needs_gc,
                cogs: info.cogs,
            });
        }
    }
    Ok(requirements)
}

/// Validates the service-wide garbage-collector configuration against the storage tree.
///
/// An empty map disables garbage collection entirely. Once any index is configured, every
/// backend without native expiration must have one, and no backend may have both a garbage
/// collector and cost tracking.
fn validate_gc(config: &StorageConfig, gc: &BTreeMap<String, Arc<dyn ChangeStream>>) -> Result<()> {
    let requirements = gc_requirements(config)?;

    if gc.is_empty() {
        if requirements.iter().any(|requirement| requirement.needs_gc) {
            objectstore_log::warn!(
                "no garbage collector is configured; expired objects in backends without \
                 native expiration will not be reclaimed"
            );
        }
        return Ok(());
    }

    for requirement in &requirements {
        let configured = gc.contains_key(&requirement.key);
        if requirement.needs_gc && configured && requirement.cogs {
            anyhow::bail!(
                "backend {} has both storage_cogs and a garbage collector configured; \
                 choose one",
                requirement.key
            );
        }
        if requirement.needs_gc && !configured {
            anyhow::bail!(
                "backend {} needs a garbage collector, but no garbage_collector entry \
                 exists for it",
                requirement.key
            );
        }
        if !requirement.needs_gc && configured {
            objectstore_log::warn!(
                key = requirement.key,
                "garbage collector configured for a backend that does not need one"
            );
        }
    }

    for key in gc.keys() {
        if !requirements
            .iter()
            .any(|requirement| requirement.key == *key)
        {
            objectstore_log::warn!(key, "garbage collector configured for an unknown backend");
        }
    }

    Ok(())
}

/// Constructs a type-erased [`Backend`](common::Backend) from the given [`StorageConfig`].
///
/// Backends that configured a [`ChangeStream`] get one from `streams`. Backends that need a
/// garbage collector get the stream from `gc`, keyed by `role:backend-type`. An empty `gc` map
/// disables garbage collection. If `gc` is non-empty, every backend without native expiration
/// must have an entry, and no backend may have both cost tracking and garbage collection.
pub async fn from_config(
    config: StorageConfig,
    streams: &ChangeStreamFactory,
    gc: &BTreeMap<String, Arc<dyn ChangeStream>>,
) -> Result<Box<dyn common::Backend>> {
    validate_gc(&config, gc)?;

    Ok(match config {
        StorageConfig::Tiered(c) => {
            let hv = hv_from_config(c.high_volume, streams, gc).await?;
            let lt = from_leaf_config(*c.long_term, streams, gc, "storage.long_term").await?;
            let log = Box::new(changelog::NoopChangeLog);
            let mut backend = tiered::TieredStorage::new(hv, lt, log);
            if let Some(limits) = c.concurrency {
                backend = backend.with_concurrency(limits);
            }
            Box::new(backend)
        }
        // All non-Tiered variants are handled by from_leaf_config. A wildcard
        // is intentional here: any new leaf variant should fall through to
        // from_leaf_config, which will handle it or produce a compile error.
        _ => from_leaf_config(config, streams, gc, "storage").await?,
    })
}

async fn from_leaf_config(
    config: StorageConfig,
    streams: &ChangeStreamFactory,
    gc: &BTreeMap<String, Arc<dyn ChangeStream>>,
    role: &str,
) -> Result<Box<dyn common::Backend>> {
    let info = leaf_info(&config)?;
    let key = gc_key(role, info.kind);

    let scoped;
    let streams = if info.needs_gc && !gc.is_empty() {
        let gc_stream = gc
            .get(&key)
            .with_context(|| format!("no garbage collector configured for {key}"))?;
        scoped = streams.clone().with_gc(Arc::clone(gc_stream));
        &scoped
    } else {
        streams
    };

    Ok(match config {
        StorageConfig::FileSystem(c) => Box::new(local_fs::LocalFsBackend::new(c, streams)),
        StorageConfig::S3Compatible(c) => Box::new(
            s3_compatible::S3CompatibleBackend::without_token(c, streams),
        ),
        StorageConfig::Gcs(c) => Box::new(gcs::GcsBackend::new(c, streams).await?),
        StorageConfig::BigTable(c) => Box::new(bigtable::BigTableBackend::new(c, streams).await?),
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
    streams: &ChangeStreamFactory,
    gc: &BTreeMap<String, Arc<dyn ChangeStream>>,
) -> Result<Box<dyn common::HighVolumeBackend>> {
    let info = high_volume_info(&config);
    let key = gc_key("storage.high_volume", info.kind);

    let scoped;
    let streams = if info.needs_gc && !gc.is_empty() {
        let gc_stream = gc
            .get(&key)
            .with_context(|| format!("no garbage collector configured for {key}"))?;
        scoped = streams.clone().with_gc(Arc::clone(gc_stream));
        &scoped
    } else {
        streams
    };

    Ok(match config {
        HighVolumeStorageConfig::BigTable(c) => {
            Box::new(bigtable::BigTableBackend::new(c, streams).await?)
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;
    use crate::change_stream::{CostTrackerStreamConfig, NoopStream};

    fn cogs() -> CostTrackerStreamConfig {
        CostTrackerStreamConfig {
            shared_resource_id: "test".into(),
            sample_rate: 1.0,
        }
    }

    fn fs(with_cogs: bool) -> StorageConfig {
        StorageConfig::FileSystem(local_fs::FileSystemConfig {
            path: PathBuf::from("data"),
            cogs: with_cogs.then(cogs),
        })
    }

    fn gcs(with_cogs: bool) -> StorageConfig {
        StorageConfig::Gcs(gcs::GcsConfig {
            endpoint: None,
            bucket: "bucket".into(),
            tcp_user_timeout: Duration::from_secs(30),
            cogs: with_cogs.then(cogs),
        })
    }

    fn bigtable(with_cogs: bool) -> HighVolumeStorageConfig {
        HighVolumeStorageConfig::BigTable(bigtable::BigTableConfig {
            endpoint: None,
            project_id: "project".into(),
            instance_name: "instance".into(),
            table_name: "table".into(),
            connections: None,
            rpc_timeout: Duration::from_secs(1),
            cogs: with_cogs.then(cogs),
        })
    }

    fn tiered(hv: HighVolumeStorageConfig, lt: StorageConfig) -> StorageConfig {
        StorageConfig::Tiered(tiered::TieredStorageConfig {
            high_volume: hv,
            long_term: Box::new(lt),
            concurrency: None,
        })
    }

    fn gc(keys: &[&str]) -> BTreeMap<String, Arc<dyn ChangeStream>> {
        keys.iter()
            .map(|key| {
                (
                    (*key).to_owned(),
                    Arc::new(NoopStream) as Arc<dyn ChangeStream>,
                )
            })
            .collect()
    }

    #[test]
    fn requirements_name_the_backend_role_and_type() {
        let requirements = gc_requirements(&tiered(bigtable(false), fs(false))).unwrap();

        assert_eq!(requirements.len(), 2);
        assert_eq!(requirements[0].key, "storage.high_volume:bigtable");
        assert!(!requirements[0].needs_gc);
        assert!(!requirements[0].cogs);
        assert_eq!(requirements[1].key, "storage.long_term:filesystem");
        assert!(requirements[1].needs_gc);
        assert!(!requirements[1].cogs);
    }

    #[test]
    fn a_single_backend_uses_the_storage_role() {
        let requirements = gc_requirements(&fs(false)).unwrap();

        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].key, "storage:filesystem");
        assert!(requirements[0].needs_gc);
        assert!(!requirements[0].cogs);
    }

    #[test]
    fn an_empty_gc_map_disables_gc() {
        validate_gc(&fs(false), &gc(&[])).unwrap();
        validate_gc(&fs(true), &gc(&[])).unwrap();
    }

    #[test]
    fn a_backend_without_native_expiry_requires_a_collector_when_gc_is_configured() {
        let error = validate_gc(&fs(false), &gc(&["storage:gcs"])).unwrap_err();

        assert!(error.to_string().contains("storage:filesystem"), "{error}");
    }

    #[test]
    fn both_cogs_and_gc_are_rejected() {
        let error = validate_gc(&fs(true), &gc(&["storage:filesystem"])).unwrap_err();

        assert!(error.to_string().contains("both"), "{error}");
    }

    #[test]
    fn gc_for_a_backend_with_native_expiry_is_tolerated() {
        validate_gc(&gcs(false), &gc(&["storage:gcs"])).unwrap();
    }

    #[test]
    fn an_unknown_gc_key_is_tolerated() {
        validate_gc(&gcs(false), &gc(&["storage:gcs", "storage:bogus"])).unwrap();
    }

    #[tokio::test]
    async fn from_config_rejects_both_cogs_and_gc() {
        let error = from_config(
            fs(true),
            &ChangeStreamFactory::default(),
            &gc(&["storage:filesystem"]),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("both"), "{error}");
    }

    #[tokio::test]
    async fn a_filesystem_backend_reports_writes_to_its_gc_index() {
        use objectstore_types::metadata::{ExpirationPolicy, Metadata};
        use objectstore_types::scope::{Scope, Scopes};
        use objectstore_types::time::Timestamp;

        use crate::change_stream::GcChangeStream;
        use crate::garbage_collector::GcStorage as _;
        use crate::garbage_collector::sqlite::{
            SqliteGarbageCollector, SqliteGarbageCollectorConfig,
        };
        use crate::id::ObjectContext;
        use crate::stream;

        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            SqliteGarbageCollector::new(&SqliteGarbageCollectorConfig {
                path: dir.path().join("gc.db").to_string_lossy().into_owned(),
            })
            .await
            .unwrap(),
        );
        let gc_stream: Arc<dyn ChangeStream> = Arc::new(GcChangeStream {
            storage: storage.clone(),
        });
        let gc = BTreeMap::from([("storage:filesystem".to_owned(), gc_stream)]);

        let backend = from_config(
            StorageConfig::FileSystem(local_fs::FileSystemConfig {
                path: dir.path().join("objects"),
                cogs: None,
            }),
            &ChangeStreamFactory::default(),
            &gc,
        )
        .await
        .unwrap();

        let now = Timestamp::now();
        let expires = now + Duration::from_hours(1);
        let id = crate::id::ObjectId::new(
            ObjectContext {
                usecase: "testing".parse().unwrap(),
                scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
            },
            "key".to_owned(),
        );
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(expires),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("payload"), now)
            .await
            .unwrap();

        assert_eq!(storage.list_expired(expires, 10).await.unwrap(), vec![id]);
    }
}
