//! The storage server component.
//!
//! This builds on top of the [`service`], and exposes the underlying
//! storage layer as both a `gRPC` service for use by the `client`, as well as
//! an `HTTP` layer which can serve files directly to *external clients*.

use std::sync::Arc;

use anyhow::Result;
use service::StorageService;

use crate::config::Config;

mod config;
mod grpc;
mod http;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let service = Arc::new(StorageService::new(&config.path)?);

    let http_server = http::start_server(&config, Arc::clone(&service));
    let grpc_server = grpc::start_server(&config, service);

    let _ = tokio::join!(http_server, grpc_server);

    Ok(())
}
