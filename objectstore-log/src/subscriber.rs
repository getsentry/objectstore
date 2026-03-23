use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

use crate::Level;
use crate::config::{LogFormat, LoggingConfig};

/// Initializes the global tracing subscriber with structured logging.
///
/// Reads `RUST_LOG` for filter directives; falls back to `INFO`-level logging with `TRACE`-level
/// for internal objectstore crates. Log format (`pretty`, `simplified`, or `json`) is determined
/// by `config.format`, defaulting to pretty when a terminal is attached.
///
/// When built with the `sentry` feature, also attaches a Sentry tracing layer if the Sentry client
/// has been initialized. `ERROR` and `WARN` events become Sentry events; `INFO`/`DEBUG` become
/// Sentry logs; `TRACE` is ignored.
pub fn init(config: &LoggingConfig) {
    #[cfg(feature = "sentry")]
    let sentry_layer = sentry::Hub::current()
        .client()
        .filter(|c| c.is_enabled())
        .map(|_| {
            use sentry::integrations::tracing as sentry_tracing;
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

    let format: Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync> =
        match (config.format, console::user_attended()) {
            (LogFormat::Auto, true) | (LogFormat::Pretty, _) => {
                format.compact().without_time().boxed()
            }
            (LogFormat::Auto, false) | (LogFormat::Simplified, _) => {
                format.with_ansi(false).boxed()
            }
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
        Ok(filter) => filter,
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

    #[cfg(not(feature = "sentry"))]
    tracing_subscriber::registry()
        .with(format.with_filter(config.level))
        .with(env_filter)
        .init();

    #[cfg(feature = "sentry")]
    tracing_subscriber::registry()
        .with(format.with_filter(config.level))
        .with(sentry_layer)
        .with(env_filter)
        .init();
}
