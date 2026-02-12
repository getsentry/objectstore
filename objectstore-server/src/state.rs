use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use objectstore_service::{PayloadStream, StorageConfig, StorageService};
use tokio::runtime::Handle;

use crate::auth::PublicKeyDirectory;
use crate::config::{Config, Storage};
use crate::rate_limits::{MeteredPayloadStream, RateLimiter};

/// Shared reference to the objectstore [`Services`].
pub type ServiceState = Arc<Services>;

/// Reference to the objectstore business logic.
///
/// This structure is created during server startup and shared with all HTTP request handlers. It
/// can be used to access the configured storage backends and other shared resources.
///
/// In request handlers, use `axum::extract::State<ServiceState>` to retrieve a shared reference to
/// this structure.
#[derive(Debug)]
pub struct Services {
    /// The server configuration.
    pub config: Config,
    /// Raw handle to the underlying storage service that does not enforce authorization checks.
    ///
    /// Consider using [`crate::auth::AuthAwareService`].
    pub service: StorageService,
    /// Directory for EdDSA public keys.
    ///
    /// The `kid` header field from incoming authorization tokens should correspond to a public key
    /// in this directory that can be used to verify the token.
    pub key_directory: PublicKeyDirectory,
    /// Stateful admission-based rate limiter for incoming requests.
    pub rate_limiter: RateLimiter,
}

impl Services {
    /// Spawns all services and background tasks for objectstore.
    ///
    /// This returns a [`ServiceState`], which is a shared reference to the services suitable for
    /// use in the web server.
    pub async fn spawn(config: Config) -> Result<ServiceState> {
        tokio::spawn(track_runtime_metrics(config.runtime.metrics_interval));

        let high_volume = map_storage_config(&config.high_volume_storage);
        let long_term = map_storage_config(&config.long_term_storage);
        let service = StorageService::new(high_volume, long_term).await?;

        let key_directory = PublicKeyDirectory::try_from(&config.auth)?;
        let rate_limiter = RateLimiter::new(config.rate_limits.clone());

        Ok(Arc::new(Self {
            config,
            service,
            key_directory,
            rate_limiter,
        }))
    }

    /// Utility to convert a [`PayloadStream`] into a [`MeteredPayloadStream`] for bandwidth rate
    /// limits and metrics.
    pub fn wrap_stream(&self, stream: PayloadStream) -> PayloadStream {
        MeteredPayloadStream::from(stream, self.rate_limiter.bytes_accumulator()).boxed()
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

/// Periodically captures and reports internal Tokio runtime metrics.
async fn track_runtime_metrics(interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    let metrics = Handle::current().metrics();

    loop {
        ticker.tick().await;
        tracing::trace!("Capturing runtime metrics");

        merni::gauge!("runtime.num_workers": metrics.num_workers());
        merni::gauge!("runtime.num_alive_tasks": metrics.num_alive_tasks());
        merni::gauge!("runtime.global_queue_depth": metrics.global_queue_depth());
        merni::gauge!("runtime.num_blocking_threads": metrics.num_blocking_threads());
        merni::gauge!("runtime.num_idle_blocking_threads": metrics.num_idle_blocking_threads());
        merni::gauge!("runtime.blocking_queue_depth": metrics.blocking_queue_depth());

        let registered_fds = metrics.io_driver_fd_registered_count();
        let deregistered_fds = metrics.io_driver_fd_deregistered_count();
        merni::gauge!("runtime.num_io_driver_fds": registered_fds - deregistered_fds);
    }
}
