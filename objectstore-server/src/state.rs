use std::sync::Arc;

use objectstore_service::{StorageConfig, StorageService};

use crate::config::{Config, Storage};

pub type ServiceState = Arc<State>;

pub struct State {
    pub config: Config,
    pub service: StorageService,
}

impl State {
    pub async fn new(config: Config) -> anyhow::Result<ServiceState> {
        let storage_config = match &config.storage {
            Storage::FileSystem { path } => StorageConfig::FileSystem { path },
            Storage::S3Compatible { endpoint, bucket } => StorageConfig::S3Compatible {
                endpoint: endpoint.as_deref(),
                bucket,
            },
        };
        let service = StorageService::new(storage_config).await?;

        Ok(Arc::new(Self { config, service }))
    }
}
