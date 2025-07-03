//! The storage server component.
//!
//! This builds on top of the [`service`], and exposes the underlying
//! storage layer as both a `gRPC` service for use by the `client`, as well as
//! an `HTTP` layer which can serve files directly to *external clients*.

use anyhow::Result;
use tokio::signal::unix::SignalKind;
use tonic::transport::Server;

use crate::config::Config;

mod api;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;

    let addr = config.listen_addr();
    println!("Server listening on {addr}");

    let shutdown = elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit());

    Server::builder()
        .add_service(api::service(config)?)
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}
