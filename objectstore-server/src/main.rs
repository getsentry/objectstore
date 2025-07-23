//! The storage server component.
//!
//! This builds on top of the [`objectstore-service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use anyhow::Result;
use sentry::integrations::tracing as sentry_tracing;
use tokio::signal::unix::SignalKind;
use tracing::Level;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::Config;
use crate::state::State;

mod authentication;
mod config;
mod http;
mod state;

fn maybe_initialize_sentry(config: &Config) -> Option<sentry::ClientInitGuard> {
    config.sentry_dsn.as_ref().map(|sentry_dsn| {
        sentry::init(sentry::ClientOptions {
            dsn: sentry_dsn.parse().ok(),
            enable_logs: true,
            sample_rate: 1.0,
            traces_sample_rate: 1.0,
            ..Default::default()
        })
    })
}

fn initialize_tracing(config: &Config) {
    let sentry_layer = config.sentry_dsn.as_ref().map(|_| {
        sentry_tracing::layer().event_filter(|metadata| match *metadata.level() {
            Level::ERROR | Level::WARN => {
                sentry_tracing::EventFilter::Event | sentry_tracing::EventFilter::Log
            }
            Level::INFO => sentry_tracing::EventFilter::Log,
            Level::DEBUG | Level::TRACE => sentry_tracing::EventFilter::Ignore,
        })
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(sentry_layer)
        .with(EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;

    let _sentry_guard = maybe_initialize_sentry(&config);
    initialize_tracing(&config);

    tracing::debug!(?config, "Starting service");
    let state = State::new(config).await?;
    tokio::spawn(http::start_server(state));

    elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .await;

    tracing::info!("shutting down");

    Ok(())
}
