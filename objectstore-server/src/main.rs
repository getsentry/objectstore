//! The storage server component.
//!
//! This builds on top of the [`objectstore-service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use anyhow::Result;
use tokio::signal::unix::SignalKind;

use crate::config::Config;
use crate::observability::{initialize_tracing, maybe_initialize_metrics, maybe_initialize_sentry};
use crate::state::State;

mod config;
mod http;
mod observability;
mod state;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let _runtime_guard = runtime.enter();
    tracing::info!("test");

    let config = Config::from_env()?;
    tracing::debug!(?config, "Starting service");

    // Ensure a rustls crypto provider is installed, required on distroless.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let _sentry_guard = maybe_initialize_sentry(&config);
    let metrics_guard = maybe_initialize_metrics(&config)?;
    initialize_tracing(&config);

    runtime.block_on(async move {
        let state = State::new(config).await?;
        tokio::spawn(http::server(state));

        elegant_departure::tokio::depart()
            .on_termination()
            .on_sigint()
            .on_signal(SignalKind::hangup())
            .on_signal(SignalKind::quit())
            .await;

        if let Some(metrics_guard) = metrics_guard {
            metrics_guard.flush(None).await?;
        }

        tracing::info!("shutting down");

        Ok(())
    })
}
