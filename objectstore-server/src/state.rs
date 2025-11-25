use std::sync::Arc;

use objectstore_service::{StorageConfig, StorageService};

use crate::config::{Config, Storage};

pub type ServiceState = Arc<State>;

/// State that is made available in request handlers.
pub struct State {
    /// Service configuration.
    pub config: Config,
    /// Raw handle to the underlying storage service that does not enforce authorization checks.
    ///
    /// Consider using [`crate::auth::AuthAwareService`].
    pub authless_service: StorageService,
}

impl State {
    pub async fn new(config: Config) -> anyhow::Result<ServiceState> {
        let high_volume = map_storage_config(&config.high_volume_storage);
        let long_term = map_storage_config(&config.long_term_storage);
        let authless_service = StorageService::new(high_volume, long_term).await?;

        Ok(Arc::new(Self {
            config,
            authless_service,
        }))
    }
}

fn map_storage_config(config: &'_ Storage) -> StorageConfig<'_> {
    match config {
        Storage::FileSystem { path } => StorageConfig::FileSystem { path },
        Storage::S3Compatible { endpoint, bucket } => {
            StorageConfig::S3Compatible { endpoint, bucket }
        }
        Storage::Gcs { endpoint, bucket } => StorageConfig::Gcs {
            endpoint: endpoint.as_deref(),
            bucket,
        },
        Storage::BigTable {
            endpoint,
            project_id,
            instance_name,
            table_name,
            connections,
        } => StorageConfig::BigTable {
            endpoint: endpoint.as_deref(),
            project_id,
            instance_name,
            table_name,
            connections: *connections,
        },
    }
}
