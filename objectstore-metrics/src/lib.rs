//! Metrics macros and DogStatsD initialization for Objectstore.
//!
//! This crate provides three things:
//!
//! 1. [`count!`], [`gauge!`], and [`record!`] macros with rustfmt-friendly
//!    expression-based syntax.
//! 2. [`MetricsConfig`] and [`init`] for wiring up a DogStatsD exporter.
//! 3. [`with_capturing_test_client`] for asserting on emitted metrics in tests.
//!
//! # Usage
//!
//! ```rust
//! use std::time::Duration;
//! use objectstore_metrics::{count, gauge, record};
//!
//! let stored_size: u64 = 1024;
//! let elapsed = Duration::from_secs(1);
//! let route = "api/v1";
//!
//! count!("server.start");
//! gauge!("server.requests.in_flight" = 42usize);
//! record!("server.requests.duration" = elapsed, route = route);
//! ```
//!
//! # Tag syntax
//!
//! Tags use `ident = expr` syntax. Tag values must implement `Into<SharedString>`
//! (i.e., `&str`, `String`, or similar). For integer or `Display` types, call
//! `.to_string()`. Use `.as_str()` methods whenever available to avoid allocation.
//!
//! # `AsF64` trait
//!
//! [`AsF64`] converts gauge and histogram values to `f64`:
//!
//! - Standard numeric primitives (`f32`, `f64`, `i8`–`i32`, `u8`–`u32`) via `Into<f64>`.
//! - `u64` and `usize` via an `as f64` cast; values above 2^53 lose precision, which is
//!   acceptable for metric reporting.
//! - [`Duration`](std::time::Duration) as fractional seconds via `.as_secs_f64()`.

mod mock;

use std::collections::BTreeMap;

use metrics_exporter_dogstatsd::{AggregationMode, DogStatsDBuilder};
use serde::{Deserialize, Serialize};

/// Converts a value to `f64` for metric recording.
///
/// Implemented for `f64`, `f32`, [`Duration`](std::time::Duration),
/// `i8`–`i32`, `u8`–`u32`, `u64`, and `usize`.
///
/// `Duration` is converted to fractional seconds via `.as_secs_f64()`.
/// `u64` and `usize` use an `as f64` cast; values above 2^53 lose precision,
/// which is acceptable for metric reporting.
#[allow(clippy::wrong_self_convention)]
pub trait AsF64 {
    /// Converts this value to its `f64` representation.
    fn as_f64(self) -> f64;
}

macro_rules! impl_as_f64 {
    // Types where Into<f64> is available
    (into: $($t:ty),* $(,)?) => {$(
        impl AsF64 for $t {
            fn as_f64(self) -> f64 { self.into() }
        }
    )*};
    // Types where only `as f64` is available
    (cast: $($t:ty),* $(,)?) => {$(
        impl AsF64 for $t {
            fn as_f64(self) -> f64 { self as f64 }
        }
    )*};
}

impl_as_f64!(into: f32, f64, i8, i16, i32, u8, u16, u32);
impl_as_f64!(cast: u64, usize);

impl AsF64 for std::time::Duration {
    fn as_f64(self) -> f64 {
        self.as_secs_f64()
    }
}

/// Re-exports used by macro expansion. Not part of the public API.
#[doc(hidden)]
pub mod _macro_support {
    pub use crate::AsF64;
    pub use metrics;
}

/// Error type for metrics initialization.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build the DogStatsD exporter.
    #[error("failed to initialize metrics exporter: {0}")]
    Build(#[from] metrics_exporter_dogstatsd::BuildError),
}

/// Configuration for the DogStatsD metrics exporter.
///
/// When `addr` is `None`, metrics are no-ops (the global recorder is never installed).
///
/// # Environment Variables
///
/// - `OS__METRICS__ADDR` — StatsD address (e.g. `127.0.0.1:8125` or `unixgram:///tmp/statsd.sock`)
/// - `OS__METRICS__PREFIX` — global metric name prefix
/// - `OS__METRICS__BUFFER_SIZE` — maximum payload length in bytes
/// - `OS__METRICS__TAGS__KEY=value` — per-key global tags
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// Remote address to forward metrics to.
    ///
    /// When `None`, metrics are disabled (the global recorder is not installed and all
    /// metric calls are no-ops).
    ///
    /// For UDP, the address must be in the format `<host>:<port>` (e.g. `127.0.0.1:8125`).
    /// For Unix domain sockets, use the format `<scheme>://<path>`, where the scheme is
    /// either `unix` (stream, `SOCK_STREAM`) or `unixgram` (datagram, `SOCK_DGRAM`).
    ///
    /// # Default
    ///
    /// `None` (metrics disabled)
    ///
    /// # Environment Variable
    ///
    /// `OS__METRICS__ADDR`
    pub addr: Option<String>,

    /// Global prefix prepended to every metric name.
    ///
    /// The prefix is prepended to every metric name, with a `.` separator added automatically.
    ///
    /// # Default
    ///
    /// `"objectstore"`
    ///
    /// # Environment Variable
    ///
    /// `OS__METRICS__PREFIX`
    #[serde(default = "default_prefix")]
    pub prefix: String,

    /// Maximum payload length in bytes.
    ///
    /// Controls the maximum size per StatsD payload. Should match the Datadog Agent's
    /// `dogstatsd_buffer_size` setting. If `None`, the exporter uses its default
    /// (1432 bytes for UDP, 8192 bytes for Unix sockets).
    ///
    /// # Default
    ///
    /// `None` (exporter default)
    ///
    /// # Environment Variable
    ///
    /// `OS__METRICS__BUFFER_SIZE`
    pub buffer_size: Option<usize>,

    /// Global tags applied to all metrics.
    ///
    /// Key-value pairs attached to every emitted metric. Useful for identifying
    /// environment, region, or other deployment-specific dimensions.
    ///
    /// # Default
    ///
    /// Empty (no tags)
    ///
    /// # Environment Variables
    ///
    /// Each tag is set individually:
    /// - `OS__METRICS__TAGS__FOO=foo`
    /// - `OS__METRICS__TAGS__BAR=bar`
    ///
    /// # YAML Example
    ///
    /// ```yaml
    /// metrics:
    ///   tags:
    ///     foo: foo
    ///     bar: bar
    /// ```
    pub tags: BTreeMap<String, String>,
}

fn default_prefix() -> String {
    "objectstore".to_owned()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            addr: None,
            prefix: "objectstore".to_owned(),
            buffer_size: None,
            tags: BTreeMap::new(),
        }
    }
}

/// Initializes the global DogStatsD metrics exporter.
///
/// Returns `Ok(())` immediately when `config.addr` is `None` — in that case the
/// global recorder is never installed and all `metrics` calls are no-ops.
pub fn init(config: &MetricsConfig) -> Result<(), Error> {
    let Some(ref addr) = config.addr else {
        return Ok(());
    };

    tracing::info!("reporting metrics to statsd at {addr}");

    let global_labels: Vec<metrics::Label> = config
        .tags
        .iter()
        .map(|(k, v)| metrics::Label::new(k.clone(), v.clone()))
        .collect();

    let mut builder = DogStatsDBuilder::default()
        .with_remote_address(addr)?
        .with_telemetry(true)
        .with_aggregation_mode(AggregationMode::Aggressive)
        .send_histograms_as_distributions(true)
        .with_histogram_sampling(true)
        .set_global_prefix(&config.prefix)
        .with_global_labels(global_labels);

    if let Some(buffer_size) = config.buffer_size {
        builder = builder.with_maximum_payload_length(buffer_size)?;
    }

    builder.install()?;

    Ok(())
}

pub use mock::with_capturing_test_client;

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

/// Increments a counter metric.
///
/// # Syntax
///
/// ```rust
/// use objectstore_metrics::count;
///
/// // Shorthand: increments by 1
/// count!("server.start");
/// count!("server.requests", route = "/v1/test", method = "GET");
///
/// // Explicit increment value
/// count!("server.requests" += 5);
/// count!("server.requests" += 5, route = "/v1/test");
/// ```
///
/// Tag keys are identifiers; tag values must implement `Into<SharedString>`
/// (use `.to_string()` for integers or non-string types).
#[macro_export]
macro_rules! count {
    // Shorthand: increment by 1
    ($name:literal $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::counter!(
            $name $(, stringify!($tag) => $tv)*
        )
        .increment(1);
    };
    // Explicit increment value
    ($name:literal += $value:expr $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::counter!(
            $name $(, stringify!($tag) => $tv)*
        )
        .increment($value as u64);
    };
}

/// Sets, increments, or decrements a gauge metric.
///
/// # Syntax
///
/// ```rust
/// use objectstore_metrics::gauge;
///
/// gauge!("runtime.num_workers" = 4usize);
/// gauge!("connections" += 1usize);
/// gauge!("connections" -= 1usize);
/// gauge!("runtime.num_workers" = 4usize, pool = "default");
/// ```
///
/// Values are converted to `f64` via [`AsF64`]. Supported types
/// include `f64`, `Duration`, integer primitives, `u64`, and `usize`.
///
/// Tag keys are identifiers; tag values must implement `Into<SharedString>`.
#[macro_export]
macro_rules! gauge {
    // Set
    ($name:literal = $value:expr $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::gauge!(
            $name $(, stringify!($tag) => $tv)*
        )
        .set($crate::_macro_support::AsF64::as_f64($value));
    };
    // Increment
    ($name:literal += $value:expr $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::gauge!(
            $name $(, stringify!($tag) => $tv)*
        )
        .increment($crate::_macro_support::AsF64::as_f64($value));
    };
    // Decrement
    ($name:literal -= $value:expr $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::gauge!(
            $name $(, stringify!($tag) => $tv)*
        )
        .decrement($crate::_macro_support::AsF64::as_f64($value));
    };
}

/// Records a distribution (histogram) metric.
///
/// # Syntax
///
/// ```rust
/// use std::time::Duration;
/// use objectstore_metrics::record;
///
/// let elapsed = Duration::from_secs(1);
/// record!("server.requests.duration" = elapsed);
/// record!("server.requests.duration" = elapsed, route = "/v1/test");
/// record!("put.size" = 1024u64, usecase = "default");
/// ```
///
/// Values are converted to `f64` via [`AsF64`]. `Duration` is
/// converted to fractional seconds automatically.
///
/// Tag keys are identifiers; tag values must implement `Into<SharedString>`.
#[macro_export]
macro_rules! record {
    ($name:literal = $value:expr $(, $tag:ident = $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::histogram!(
            $name $(, stringify!($tag) => $tv)*
        )
        .record($crate::_macro_support::AsF64::as_f64($value));
    };
}
