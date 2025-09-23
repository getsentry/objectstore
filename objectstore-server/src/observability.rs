use secrecy::ExposeSecret;
use sentry::integrations::tracing as sentry_tracing;
use tracing::Level;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::Config;

pub fn maybe_initialize_metrics(config: &Config) -> std::io::Result<Option<merni::DatadogFlusher>> {
    config
        .datadog_key
        .as_ref()
        .map(|api_key| {
            let mut builder =
                merni::datadog(api_key.expose_secret().as_str()).prefix("objectstore.");
            for (k, v) in &config.metric_tags {
                builder = builder.global_tag(k, v);
            }
            builder.try_init()
        })
        .transpose()
}

pub fn maybe_initialize_sentry(config: &Config) -> Option<sentry::ClientInitGuard> {
    config.sentry.as_ref().map(|sentry_config| {
        sentry::init(sentry::ClientOptions {
            dsn: sentry_config.dsn.expose_secret().parse().ok(),
            enable_logs: true,
            sample_rate: sentry_config.sample_rate.unwrap_or(1.0),
            traces_sample_rate: sentry_config.traces_sample_rate.unwrap_or(0.01),
            ..Default::default()
        })
    })
}

pub fn initialize_tracing(config: &Config) {
    // Same as the default filter, except it converts warnings into events
    // and also sends everything at or above INFO as logs instead of breadcrumbs.
    let sentry_layer = config.sentry.as_ref().map(|_| {
        sentry_tracing::layer().event_filter(|metadata| match *metadata.level() {
            Level::ERROR | Level::WARN => {
                sentry_tracing::EventFilter::Event | sentry_tracing::EventFilter::Log
            }
            Level::INFO | Level::DEBUG => sentry_tracing::EventFilter::Log,
            Level::TRACE => sentry_tracing::EventFilter::Ignore,
        })
    });

    let format = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    tracing_subscriber::registry()
        .with(format)
        .with(sentry_layer)
        .with(env_filter())
        .init();
}

fn env_filter() -> EnvFilter {
    match EnvFilter::try_from_default_env() {
        Ok(env_filter) => env_filter,
        // INFO by default. Use stricter levels for noisy crates. Use looser levels
        // for internal crates and essential dependencies.
        Err(_) => EnvFilter::new(
            "INFO,\
            objectstore=TRACE,\
            objectstore_service=TRACE,\
            objectstore_types=TRACE,\
            ",
        ),
    }
}
