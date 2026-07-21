//! Shared server state passed to all HTTP request handlers.
//!
//! [`Services`] is constructed once during startup by [`Services::spawn`] and then shared
//! across all request handlers as [`ServiceState`] (an `Arc<Services>`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::Stream;
use objectstore_service::concurrency::ConcurrencyLimiter;
use objectstore_service::id::ObjectContext;
use objectstore_service::{StorageService, backend};
use tokio::runtime::Handle;

use crate::auth::PublicKeyDirectory;
use crate::config::Config;
use crate::rate_limits::{MeteredPayloadStream, RateLimiter};
use crate::web::RequestCounter;

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
    /// Consider using [`crate::auth::AuthAwareService`] for auth-checked access.
    pub service: StorageService,
    /// Directory for EdDSA public keys.
    ///
    /// The `kid` header field from incoming authorization tokens should correspond to a public key
    /// in this directory that can be used to verify the token.
    pub key_directory: Arc<PublicKeyDirectory>,
    /// Stateful admission-based rate limiter for incoming requests.
    pub rate_limiter: RateLimiter,
    /// In-flight HTTP request counter with the configured limit.
    ///
    /// Shared with the web layer so the concurrency-limit middleware, the tracking
    /// layer, and any endpoint that reads the count all see the same atomic.
    pub request_counter: RequestCounter,
}

impl Services {
    /// Spawns all services and background tasks for objectstore.
    ///
    /// This returns a [`ServiceState`], which is a shared reference to the services suitable for
    /// use in the web server.
    pub async fn spawn(config: Config) -> Result<ServiceState> {
        tokio::spawn(track_runtime_metrics(config.runtime.metrics_interval));
        #[cfg(target_os = "linux")]
        tokio::spawn(track_allocator_metrics(config.runtime.metrics_interval));

        let backend = backend::from_config(config.storage.clone()).await?;
        let concurrency = ConcurrencyLimiter::new(config.service.max_concurrency)
            .with_queue(config.service.concurrency_queue)
            .with_timeout(config.service.concurrency_timeout)
            .with_bulk(config.service.bulk_concurrency_pct);
        let service = StorageService::new(backend).with_concurrency(concurrency);
        service.start();

        let key_directory = Arc::new(PublicKeyDirectory::from_config(&config.auth).await?);
        if config.auth.enforce && key_directory.keys.is_empty() {
            anyhow::bail!(
                "Auth enforcement is enabled but no keys are configured. Either disable auth enforcement (dev/test environments) or configure a public key."
            );
        }
        let rate_limiter = RateLimiter::new(config.rate_limits.clone());
        rate_limiter.start();

        let request_counter = RequestCounter::new(config.http.max_requests);
        tokio::spawn(request_counter.clone().run_emitter());

        Ok(Arc::new(Self {
            config,
            service,
            key_directory,
            rate_limiter,
            request_counter,
        }))
    }

    /// Wraps a byte stream with bandwidth metering for rate limiting.
    ///
    /// Works with any stream type — use for both [`objectstore_service::PayloadStream`]
    /// (outgoing, `E = io::Error`) and [`objectstore_service::ClientStream`]
    /// (incoming, `E = ClientError`).
    pub(crate) fn meter_stream<S, E>(
        &self,
        stream: S,
        context: &ObjectContext,
    ) -> MeteredPayloadStream<S>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    {
        MeteredPayloadStream::new(stream, self.rate_limiter.bandwidth_handle(context))
    }

    /// Records bandwidth usage for the given context without wrapping a stream.
    ///
    /// Used for cases where the payload size is known upfront (e.g. batch INSERT).
    pub fn record_bandwidth(&self, context: &ObjectContext, bytes: u64) {
        self.rate_limiter.record_bandwidth(context, bytes);
    }
}

/// Periodically captures and reports jemalloc stats.
#[cfg(target_os = "linux")]
async fn track_allocator_metrics(interval: Duration) {
    // INVARIANT: MIB resolution only fails if jemalloc is not the active allocator,
    // which would be a misconfigured build. Panic early to surface the problem.
    let epoch = tikv_jemalloc_ctl::epoch::mib().expect("jemalloc epoch MIB");
    let allocated = tikv_jemalloc_ctl::stats::allocated::mib().expect("jemalloc allocated MIB");
    let active = tikv_jemalloc_ctl::stats::active::mib().expect("jemalloc active MIB");
    let resident = tikv_jemalloc_ctl::stats::resident::mib().expect("jemalloc resident MIB");
    let mapped = tikv_jemalloc_ctl::stats::mapped::mib().expect("jemalloc mapped MIB");

    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;

        let Ok(_) = epoch.advance() else {
            continue;
        };

        if let Ok(allocated_bytes) = allocated.read() {
            // Bytes currently allocated by the application.
            objectstore_metrics::gauge!("jemalloc.allocated" = allocated_bytes);
        }
        if let Ok(active_bytes) = active.read() {
            // Bytes in active jemalloc pages (≥ allocated).
            objectstore_metrics::gauge!("jemalloc.active" = active_bytes);
        }
        if let Ok(resident_bytes) = resident.read() {
            // Bytes in resident pages mapped from the OS (≥ active).
            objectstore_metrics::gauge!("jemalloc.resident" = resident_bytes);
        }
        if let Ok(mapped_bytes) = mapped.read() {
            // Bytes in chunks mapped from the OS (≥ resident).
            objectstore_metrics::gauge!("jemalloc.mapped" = mapped_bytes);
        }
    }
}

/// Periodically captures and reports internal Tokio runtime metrics.
async fn track_runtime_metrics(interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    let metrics = Handle::current().metrics();

    loop {
        ticker.tick().await;
        objectstore_log::trace!("Capturing runtime metrics");

        objectstore_metrics::gauge!("runtime.num_workers" = metrics.num_workers());
        objectstore_metrics::gauge!("runtime.num_alive_tasks" = metrics.num_alive_tasks());
        objectstore_metrics::gauge!("runtime.global_queue_depth" = metrics.global_queue_depth());
        objectstore_metrics::gauge!(
            "runtime.num_blocking_threads" = metrics.num_blocking_threads()
        );
        objectstore_metrics::gauge!(
            "runtime.num_idle_blocking_threads" = metrics.num_idle_blocking_threads()
        );
        objectstore_metrics::gauge!(
            "runtime.blocking_queue_depth" = metrics.blocking_queue_depth()
        );

        let registered_fds = metrics.io_driver_fd_registered_count();
        let deregistered_fds = metrics.io_driver_fd_deregistered_count();
        objectstore_metrics::gauge!(
            "runtime.num_io_driver_fds" = registered_fds - deregistered_fds
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enforce_without_keys_fails_startup() {
        let config = Config {
            auth: crate::config::AuthZ {
                enforce: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = Services::spawn(config).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Auth enforcement is enabled but no keys are configured"),
        );
    }

    #[tokio::test]
    async fn no_enforce_without_keys_starts_ok() {
        let config = Config {
            auth: crate::config::AuthZ {
                enforce: false,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(Services::spawn(config).await.is_ok());
    }
}
