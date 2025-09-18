use std::env;

use sentry::integrations::tracing as sentry_tracing;
use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::Config;

pub fn maybe_initialize_metrics(config: &Config) -> std::io::Result<Option<merni::DatadogFlusher>> {
    config
        .datadog_key
        .as_ref()
        .map(|api_key| {
            let mut builder = merni::datadog(api_key.as_str()).prefix("objectstore.");
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
            dsn: sentry_config.dsn.parse().ok(),
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

pub fn parse_rust_log() -> (Level, EnvFilter) {
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
