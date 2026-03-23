//! Logging macros and subscriber initialization for Objectstore.
//!
//! This crate provides three things:
//!
//! 1. Re-exports of [`tracing`] macros for structured logging — use them qualified as
//!    `objectstore_log::info!()`. Never import the macros; always qualify at the call site.
//! 2. [`LoggingConfig`] and [`LogFormat`] for configuring log level and output format, and
//!    (behind the `init` feature) an [`init`] function to wire up a `tracing-subscriber` stack.
//!    When built with the `sentry` feature, `init` also attaches a Sentry tracing layer if the
//!    Sentry client has already been initialized.
//! 3. [`event_dyn!`] for runtime-dispatched log levels and [`exception!`] for ergonomic error logging.
//!
//! # Usage
//!
//! ## Logging macros
//!
//! ```rust
//! objectstore_log::info!("server starting");
//! objectstore_log::warn!(status = "degraded", "storage unavailable");
//! objectstore_log::debug!(value = 42, "loaded configuration");
//! ```
//!
//! ## `exception!` — log an error at ERROR level
//!
//! Casts the expression to `&dyn std::error::Error` internally; any additional tracing fields or
//! message may follow:
//!
//! ```rust
//! # fn example() -> anyhow::Result<()> {
//! let err = anyhow::anyhow!("something broke");
//! objectstore_log::exception!(err.as_ref());
//! objectstore_log::exception!(err.as_ref(), "fatal startup error");
//! # Ok(())
//! # }
//! ```
//!
//! ## `event_dyn!` — dispatch log level at runtime
//!
//! ```rust
//! # use objectstore_log::Level;
//! let level = Level::WARN;
//! objectstore_log::event_dyn!(level, "dynamic level message");
//! objectstore_log::event_dyn!(level, field = "value", "with fields");
//! ```
//!
//! ## Subscriber initialization (requires `init` feature)
//!
//! ```rust,ignore
//! let config = objectstore_log::LoggingConfig::default();
//! objectstore_log::init(&config);
//! ```
//!
//! ## Span types and span macros
//!
//! Types and macros from the underlying [`tracing`] crate that are not re-exported individually
//! (such as [`tracing::Span`] and [`tracing::debug_span!`]) are accessible through the re-exported
//! `tracing` module:
//!
//! ```rust
//! use objectstore_log::tracing;
//! let span: tracing::Span = tracing::debug_span!("my_span");
//! ```

mod config;
mod macros;
#[cfg(feature = "init")]
mod subscriber;

pub use config::{FormatParseError, LogFormat, LoggingConfig};
#[cfg(feature = "init")]
pub use subscriber::init;

/// The underlying [`tracing`] crate, re-exported as a module.
///
/// Use this to access types and macros not individually re-exported, such as [`tracing::Span`],
/// [`tracing::debug_span!`], and [`tracing::field`]:
///
/// ```rust
/// use objectstore_log::tracing;
/// let _span: tracing::Span = tracing::debug_span!("op");
/// ```
pub use tracing;
pub use tracing::level_filters::LevelFilter;
pub use tracing::{Level, debug, error, info, trace, warn};

/// Logs `error` via the tracing subscriber if one is configured, or prints to `stderr` otherwise.
///
/// Use this in binary entry points where the subscriber may or may not have been initialized yet,
/// such as in `main` when a fatal error occurs before or during initialization.
pub fn ensure_log_error(error: &anyhow::Error) {
    if Level::ERROR <= tracing::level_filters::STATIC_MAX_LEVEL
        && Level::ERROR <= LevelFilter::current()
    {
        exception!(error.as_ref());
    } else {
        eprintln!("{error:?}");
    }
}
