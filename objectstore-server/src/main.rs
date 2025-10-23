//! The storage server component.
//!
//! This builds on top of the [`objectstore-service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use anyhow::Result;
use tokio::signal::unix::SignalKind;

use objectstore_server::config::Config;
use objectstore_server::http;
use objectstore_server::observability::{
    initialize_tracing, maybe_initialize_metrics, maybe_initialize_sentry,
};
use objectstore_server::state::State;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    let config = Config::from_env()?;

    // Sentry should be initialized before creating the async runtime
    let _sentry_guard = maybe_initialize_sentry(&config);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("main-rt")
        .enable_all()
        .worker_threads(config.runtime.worker_threads)
        .build()?;

    let _runtime_guard = runtime.enter();

    initialize_tracing(&config);
    tracing::info!("Starting service");
    tracing::debug!(?config);

    // Ensure a rustls crypto provider is installed, required on distroless.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let metrics_guard = maybe_initialize_metrics(&config)?;
    let metrics_handle = async move {
        match metrics_guard {
            Some(metrics_guard) => metrics_guard.flush(None).await,
            None => Ok(()),
        }
    };

    runtime.block_on(async move {
        let state = State::new(config).await?;
        let server_handle = tokio::spawn(http::server(state));

        elegant_departure::tokio::depart()
            .on_termination()
            .on_sigint()
            .on_signal(SignalKind::hangup())
            .on_signal(SignalKind::quit())
            .await;

        let (server_result, metrics_result) = tokio::join!(server_handle, metrics_handle);
        server_result??;
        metrics_result?;

        tracing::info!("shutdown complete");

        Ok(())
    })
}
