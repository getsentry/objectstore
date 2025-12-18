use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpSocket};
use tokio::signal::unix::SignalKind;

use crate::config::Config;
use crate::state::Services;
use crate::web::app::App;

/// The maximum backlog for TCP listen sockets before refusing connections.
const TCP_LISTEN_BACKLOG: u32 = 1024;

/// Runs the objectstore HTTP server.
///
/// This function initializes the server, binds to the configured address, and runs until
/// termination is requested.
pub async fn server(config: Config) -> Result<()> {
    tracing::info!("Starting server");
    merni::counter!("server.start": 1);

    let listener = listen(&config).context("failed to start TCP listener")?;
    let state = Services::spawn(config).await?;

    let server_handle = tokio::spawn(async move {
        App::new(state)
            .graceful_shutdown(true)
            .serve(listener)
            .await
    });

    tokio::spawn(async move {
        elegant_departure::get_shutdown_guard().wait().await;
        tracing::info!("Shutting down ...");
    });

    elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .await;

    let server_result = server_handle.await.map_err(From::from).flatten();
    tracing::info!("Shutdown complete");
    server_result
}

fn listen(config: &Config) -> Result<TcpListener> {
    let addr = config.http_addr;
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;

    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuseport(true)?;
    socket.bind(addr)?;

    let listener = socket.listen(TCP_LISTEN_BACKLOG)?;
    tracing::info!("HTTP server listening on {addr}");

    Ok(listener)
}
