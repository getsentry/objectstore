//! The storage server component.
//!
//! This builds on top of the [`service`], and exposes the underlying
//! storage layer as both a `gRPC` service for use by the `client`, as well as
//! an `HTTP` layer which can serve files directly to *external clients*.

use std::sync::Arc;

use anyhow::Result;
use service::StorageService;
use tokio::signal::unix::SignalKind;

use crate::config::Config;

mod config;
mod grpc;
mod http;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env()?);
    let service = StorageService::new(&config.data_path, config.gcs_bucket.as_deref()).await?;

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
