//! The storage server component.
//!
//! This builds on top of the [`objectstore-service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::env;

use anyhow::Result;
use sentry::integrations::tracing as sentry_tracing;
use tokio::signal::unix::SignalKind;
use tracing::Level;
use tracing::level_filters::LevelFilter;
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
    // Same as the default filter, except it converts warnings into events
    // and also sends everything at or above INFO as logs instead of breadcrumbs.
    let sentry_layer = config.sentry_dsn.as_ref().map(|_| {
        sentry_tracing::layer().event_filter(|metadata| match *metadata.level() {
            Level::ERROR | Level::WARN => {
                sentry_tracing::EventFilter::Event | sentry_tracing::EventFilter::Log
            }
            Level::INFO => sentry_tracing::EventFilter::Log,
            Level::DEBUG | Level::TRACE => sentry_tracing::EventFilter::Ignore,
        })
    });

    let (level, env_filter) = parse_rust_log();
    let format = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    tracing_subscriber::registry()
        .with(format.with_filter(LevelFilter::from(level)))
        .with(sentry_layer)
        .with(env_filter)
        .init();
}

fn parse_rust_log() -> (Level, EnvFilter) {
    // Try to parse RUST_LOG as a simple level filter and apply default levels internally.
    // Otherwise, use it literally if the user knows which overrides they want to run.
    let level = match env::var(EnvFilter::DEFAULT_ENV) {
        Ok(value) => match value.parse::<Level>() {
            Ok(level) => level,
            Err(_) => return (Level::TRACE, EnvFilter::new(value)),
        },
        Err(_) => Level::INFO,
    };

    // This is the maximum verbosity that will be logged, we filter this down to `level`.
    let env_filter = EnvFilter::new(
        "INFO,\
        tower_http=TRACE,\
        trust_dns_proto=WARN,\
        objectstore_server=TRACE,\
        objectstore_service=TRACE,\
        objectstore_types=TRACE,\
        ",
    );

    (level, env_filter)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;

    // Ensure a rustls crypto provider is installed, required on distroless.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

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
