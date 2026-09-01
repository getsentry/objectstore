//! Sends a production-shaped service error to Sentry and waits for delivery.
//!
//! Configure this example with the same `OS__SENTRY__*` environment variables as the server. In
//! particular, `OS__SENTRY__DSN` must be set. An optional first argument selects the normal server
//! YAML configuration file.

use std::error::Error as _;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use anyhow::ensure;
use objectstore_server::config::Config;
use objectstore_service::concurrency::spawn_metered;
use objectstore_service::error::{ErrorKind, ResultExt as _};

const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> anyhow::Result<()> {
    let config_path = std::env::args_os().nth(1);
    let config = Config::load(config_path.as_deref().map(Path::new))?;

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    // Keep the guard alive until after the explicit flush below. This is the same Sentry
    // initialization used by the production server, including release, sampling, logs, tags,
    // environment, and server name.
    let _sentry_guard = objectstore_server::observability::init_sentry(&config)
        .ok_or_else(|| anyhow::anyhow!("OS__SENTRY__DSN must be configured"))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("sentry-test-rt")
        .enable_all()
        .worker_threads(config.runtime.worker_threads)
        .build()?;
    let _runtime_guard = runtime.enter();

    // This installs the same tracing-to-Sentry layer that reports service task failures in
    // production.
    objectstore_log::init(&config.logging);

    let endpoint = unused_local_endpoint()?;
    let error = runtime.block_on(async move {
        let result: objectstore_service::error::Result<()> =
            spawn_metered("sentry_test_backend_request", (), async move {
                reqwest::Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .context(ErrorKind::BackendFailure, "building the Sentry test client")?
                    .get(endpoint)
                    .send()
                    .await
                    .context(ErrorKind::BackendFailure, "sending the Sentry test request")?;
                Ok(())
            })
            .await;

        result.expect_err("request to a closed local port unexpectedly succeeded")
    });

    ensure!(error.kind() == ErrorKind::BackendFailure);
    ensure!(
        error.source().is_some(),
        "service error lost its reqwest source"
    );

    let client = sentry::Hub::current()
        .client()
        .ok_or_else(|| anyhow::anyhow!("Sentry client was not initialized"))?;
    ensure!(
        client.flush(Some(FLUSH_TIMEOUT)),
        "Sentry did not flush within {FLUSH_TIMEOUT:?}"
    );

    eprintln!("Sentry service-error event submitted and flushed");
    Ok(())
}

/// Reserves and releases a loopback port so the HTTP request produces a real connection error.
fn unused_local_endpoint() -> anyhow::Result<String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(format!("http://{address}/objectstore-sentry-test"))
}
