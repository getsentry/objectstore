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
        let high_volume = map_storage_config(&config.high_volume_storage);
        let long_term = map_storage_config(&config.long_term_storage);
        let service = StorageService::new(high_volume, long_term).await?;

        Ok(Arc::new(Self { config, service }))
    }
}

fn map_storage_config(config: &'_ Storage) -> StorageConfig<'_> {
    match config {
        Storage::FileSystem { path } => StorageConfig::FileSystem { path },
        Storage::S3Compatible {
            endpoint,
            bucket,
            region,
            use_path_style,
            access_key,
            secret_key,
            security_token,
            session_token,
            request_timeout_secs,
        } => StorageConfig::S3Compatible {
            endpoint: endpoint.as_deref(),
            bucket,
            region,
            use_path_style: match use_path_style {
                Some(value) => *value,
                None => false,
            },
            access_key: access_key.as_deref(),
            secret_key: secret_key.as_deref(),
            security_token: security_token.as_deref(),
            session_token: session_token.as_deref(),
            request_timeout_secs: *request_timeout_secs,
        },
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
