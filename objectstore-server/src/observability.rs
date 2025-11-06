use secrecy::ExposeSecret;
use sentry::integrations::tracing as sentry_tracing;
use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::{Config, LogFormat};

/// The full release name including the objectstore version and SHA.
const RELEASE: &str = std::env!("OBJECTSTORE_RELEASE");

pub fn init_metrics(config: &Config) -> std::io::Result<Option<merni::DatadogFlusher>> {
    let Some(ref api_key) = config.metrics.datadog_key else {
        return Ok(None);
    };

    let mut builder = merni::datadog(api_key.expose_secret().as_str()).prefix("objectstore.");
    for (k, v) in &config.metrics.tags {
        builder = builder.global_tag(k, v);
    }
    builder.try_init().map(Some)
}

pub fn init_sentry(config: &Config) -> Option<sentry::ClientInitGuard> {
    let config = &config.sentry;
    let dsn = config.dsn.as_ref()?;

    Some(sentry::init(sentry::ClientOptions {
        dsn: dsn.expose_secret().parse().ok(),
        release: Some(RELEASE.into()),
        environment: config.environment.clone(),
        server_name: config.server_name.clone(),
        sample_rate: config.sample_rate,
        traces_sampler: {
            let traces_sample_rate = config.traces_sample_rate;
            let inherit_sampling_decision = config.inherit_sampling_decision;
            Some(std::sync::Arc::new(move |ctx| {
                if let Some(sampled) = ctx.sampled()
                    && inherit_sampling_decision
                {
                    f32::from(sampled)
                } else {
                    traces_sample_rate
                }
            }))
        },
        enable_logs: true,
        debug: config.debug,
        ..Default::default()
    }))
}

pub fn init_tracing(config: &Config) {
    // Same as the default filter, except it converts warnings into events
    // and also sends everything at or above INFO as logs instead of breadcrumbs.
    let sentry_layer = config.sentry.is_enabled().then(|| {
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

    let format = match (config.logging.format, console::user_attended()) {
        (LogFormat::Auto, true) | (LogFormat::Pretty, _) => format.compact().without_time().boxed(),
        (LogFormat::Auto, false) | (LogFormat::Simplified, _) => format.with_ansi(false).boxed(),
        (LogFormat::Json, _) => format
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .with_file(true)
            .with_line_number(true)
            .boxed(),
    };

    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(env_filter) => env_filter,
        // INFO by default. Use stricter levels for noisy crates. Use looser levels
        // for internal crates and essential dependencies.
        Err(_) => EnvFilter::new(
            "INFO,\
            tower_http=DEBUG,\
            objectstore=TRACE,\
            objectstore_service=TRACE,\
            objectstore_types=TRACE,\
            ",
        ),
    };

    tracing_subscriber::registry()
        .with(format.with_filter(config.logging.level))
        .with(sentry_layer)
        .with(env_filter)
        .init();
}

/// Logs an error to the configured logger or `stderr` if not yet configured.
pub fn ensure_log_error(error: &anyhow::Error) {
    if tracing::Level::ERROR <= tracing::level_filters::STATIC_MAX_LEVEL
        && tracing::Level::ERROR <= LevelFilter::current()
    {
        tracing::error!(error = error.as_ref() as &dyn std::error::Error);
    } else {
        eprintln!("{error:?}");
    }
}
