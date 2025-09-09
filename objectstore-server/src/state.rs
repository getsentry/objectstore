use std::sync::Arc;

use objectstore_service::{BigTableConfig, StorageConfig, StorageService};

use crate::config::{Config, Storage};

pub type ServiceState = Arc<State>;

pub struct State {
    pub config: Config,
    pub service: StorageService,
}

impl State {
    pub async fn new(config: Config) -> anyhow::Result<ServiceState> {
        let small_storage = map_storage_config(&config.small_storage);
        let large_storage = map_storage_config(&config.large_storage);
        let service = StorageService::new(small_storage, large_storage).await?;

        Ok(Arc::new(Self { config, service }))
    }
}

fn map_storage_config(config: &'_ Storage) -> StorageConfig<'_> {
    match config {
        Storage::FileSystem { path } => StorageConfig::FileSystem { path },
        Storage::S3Compatible { endpoint, bucket } => StorageConfig::S3Compatible {
            endpoint: endpoint.as_deref(),
            bucket,
        },
        Storage::BigTable {
            project_id,
            instance_name,
            table_name,
        } => StorageConfig::BigTable(BigTableConfig {
            project_id: project_id.clone(),
            instance_name: instance_name.clone(),
            table_name: table_name.clone(),
        }),
    }
}
