//! Initialization of error reporting and distributed tracing.
//!
//! Call [`init_sentry`] and [`init_tracing`] during server startup.
//! Sentry must be initialized before the Tokio runtime is created so it can
//! instrument async tasks from the start.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use secrecy::ExposeSecret;
use sentry::integrations::tracing as sentry_tracing;
use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::{Config, LogFormat};

/// The full release name including the objectstore version and SHA.
const RELEASE: &str = std::env!("OBJECTSTORE_RELEASE");

#[derive(Clone, Debug, PartialEq, Eq)]
struct BufferedLog {
    level: Level,
    message: String,
}

#[derive(Debug, Default)]
struct BufferedLogsState {
    tracing_ready: bool,
    logs: VecDeque<BufferedLog>,
}

static BUFFERED_LOGS_STATE: OnceLock<Mutex<BufferedLogsState>> = OnceLock::new();

fn get_buffered_logs_state() -> std::sync::MutexGuard<'static, BufferedLogsState> {
    BUFFERED_LOGS_STATE
        .get_or_init(|| Mutex::new(BufferedLogsState::default()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn emit_log(level: Level, message: &str) {
    match level {
        Level::ERROR => tracing::error!("{message}"),
        Level::WARN => tracing::warn!("{message}"),
        Level::INFO => tracing::info!("{message}"),
        Level::DEBUG => tracing::debug!("{message}"),
        Level::TRACE => tracing::trace!("{message}"),
    }
}

/// Initializes the Sentry error-reporting client, if a DSN is configured.
///
/// Returns `None` when `config.sentry.dsn` is not set. The returned
/// [`sentry::ClientInitGuard`] must be kept alive for the duration of the process;
/// dropping it flushes the event queue and shuts down the Sentry client.
pub fn init_sentry(config: &Config) -> Option<sentry::ClientInitGuard> {
    let config = &config.sentry;
    let dsn = config.dsn.as_ref()?;

    let guard = sentry::init(sentry::ClientOptions {
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
    });

    sentry::configure_scope(|scope| {
        for (k, v) in &config.tags {
            scope.set_tag(k, v);
        }
    });

    Some(guard)
}

/// Initializes the global tracing subscriber with structured logging and optional Sentry integration.
///
/// Reads `RUST_LOG` for filter directives; falls back to `INFO`-level logging with `TRACE`-level
/// for internal objectstore crates. Log format (`pretty`, `simplified`, or `json`) is determined
/// by `config.logging.format`, defaulting to pretty when a terminal is attached.
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

    flush_buffered_logs();
}

/// Emits a log through tracing when ready, or buffers it until tracing is initialized.
pub fn log_or_buffer(level: Level, message: impl Into<String>) {
    let buffered_log = BufferedLog {
        level,
        message: message.into(),
    };

    let ready = {
        let mut state = get_buffered_logs_state();
        if state.tracing_ready {
            true
        } else {
            state.logs.push_back(buffered_log.clone());
            false
        }
    };

    if ready {
        emit_log(buffered_log.level, &buffered_log.message);
    }
}

fn flush_buffered_logs() {
    let buffered_logs = {
        let mut state = get_buffered_logs_state();
        state.tracing_ready = true;
        state.logs.drain(..).collect::<Vec<_>>()
    };

    for log in buffered_logs {
        emit_log(log.level, &log.message);
    }
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

#[cfg(test)]
pub(crate) static TEST_LOG_STATE_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn reset_buffered_logs() {
    let mut state = get_buffered_logs_state();
    *state = BufferedLogsState::default();
}

#[cfg(test)]
pub(crate) fn buffered_logs() -> Vec<(Level, String)> {
    get_buffered_logs_state()
        .logs
        .iter()
        .map(|log| (log.level, log.message.clone()))
        .collect()
}
