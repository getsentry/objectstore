//! Shared server state passed to all HTTP request handlers.
//!
//! [`Services`] is constructed once during startup by [`Services::spawn`] and then shared
//! across all request handlers as [`ServiceState`] (an `Arc<Services>`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::Stream;
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
    pub key_directory: PublicKeyDirectory,
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

        let backend = backend::from_config(config.storage.clone()).await?;
        let service =
            StorageService::new(backend).with_concurrency_limit(config.service.max_concurrency);
        service.start();

        let key_directory = PublicKeyDirectory::try_from(&config.auth)?;
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
        MeteredPayloadStream::new(stream, self.rate_limiter.bytes_accumulators(context))
    }

    /// Records bandwidth usage for the given context without wrapping a stream.
    ///
    /// Used for cases where the payload size is known upfront (e.g. batch INSERT).
    pub fn record_bandwidth(&self, context: &ObjectContext, bytes: u64) {
        self.rate_limiter.record_bandwidth(context, bytes);
    }
}

/// Periodically captures and reports internal Tokio runtime metrics.
async fn track_runtime_metrics(interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    let metrics = Handle::current().metrics();

    loop {
        ticker.tick().await;
        tracing::trace!("Capturing runtime metrics");

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
