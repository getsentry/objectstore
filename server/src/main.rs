//! The storage server component.
//!
//! This builds on top of the [`service`], and exposes the underlying
//! storage layer as both a `gRPC` service for use by the `client`, as well as
//! an `HTTP` layer which can serve files directly to *external clients*.

use std::sync::Arc;

use anyhow::Result;
use sentry::integrations::tracing as sentry_tracing;
use service::StorageService;
use tokio::signal::unix::SignalKind;
use tracing::Level;
use tracing_subscriber::prelude::*;

use crate::config::Config;

mod config;
mod grpc;
mod http;

fn maybe_initialize_sentry(config: &Arc<Config>) -> Option<sentry::ClientInitGuard> {
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

fn initialize_tracing(config: &Arc<Config>) {
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
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env()?);
    let service = StorageService::new(&config.data_path, config.gcs_bucket.as_deref()).await?;

    let _sentry_guard = maybe_initialize_sentry(&config);
    initialize_tracing(&config);

    tokio::spawn(http::start_server(Arc::clone(&config), service.clone()));
    tokio::spawn(grpc::start_server(config, service));

    elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .await;
    println!("shutting down");

    Ok(())
}
