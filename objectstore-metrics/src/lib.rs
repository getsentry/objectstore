//! Metrics macros and DogStatsD initialization for Objectstore.
//!
//! This crate provides three things:
//!
//! 1. [`counter!`], [`gauge!`], and [`distribution!`] macros that preserve
//!    a concise call-site syntax (`"name": value, "tag" => tag_value`).
//! 2. [`MetricsConfig`] and [`init`] for wiring up a DogStatsD exporter.
//! 3. [`with_capturing_test_client`] for asserting on emitted metrics in tests.
//!
//! # Usage
//!
//! ```rust
//! use std::time::Duration;
//! use objectstore_metrics::{counter, distribution, gauge};
//!
//! let count = 42_u64;
//! let elapsed = Duration::from_secs(1);
//! let route = "api/v1";
//!
//! counter!("server.start": 1);
//! gauge!("server.requests.in_flight": count);
//! distribution!("server.requests.duration"@s: elapsed, "route" => route);
//! ```
//!
//! # Unit annotations
//!
//! - `@s` converts a [`Duration`](std::time::Duration) to seconds via `.as_secs_f64()`.
//! - `@b` converts the value via `as f64` (identity for byte counts).
//! - No annotation also converts via `as f64`.

mod mock;

use std::collections::BTreeMap;

use metrics_exporter_dogstatsd::{AggregationMode, DogStatsDBuilder};
use serde::{Deserialize, Serialize};

/// Re-exports used by macro expansion. Not part of the public API.
#[doc(hidden)]
pub mod _macro_support {
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

/// Emits a counter metric.
///
/// # Syntax
///
/// ```rust
/// use objectstore_metrics::counter;
///
/// counter!("name": 1u64);
/// counter!("name": 1u64, "tag" => "value");
/// counter!("name": 1u64, "tag1" => "val1", "tag2" => "val2");
/// ```
#[macro_export]
macro_rules! counter {
    ($name:literal : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::counter!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .increment($value as u64);
    };
}

/// Emits a gauge metric.
///
/// # Syntax
///
/// ```rust
/// use objectstore_metrics::gauge;
///
/// gauge!("name": 1.0_f64);
/// gauge!("name"@b: 1024_u64);
/// gauge!("name": 1.0_f64, "tag" => "value");
/// ```
///
/// The `@b` unit annotation converts via `as f64` (identity for byte counts).
#[macro_export]
macro_rules! gauge {
    ($name:literal @b : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::gauge!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .set($value as f64);
    };
    ($name:literal : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::gauge!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .set($value as f64);
    };
}

/// Emits a distribution (histogram) metric.
///
/// # Syntax
///
/// ```rust
/// use std::time::Duration;
/// use objectstore_metrics::distribution;
///
/// distribution!("name": 1.0_f64);
/// distribution!("name"@s: Duration::from_secs(1));
/// distribution!("name"@b: 1024_u64);
/// distribution!("name"@s: Duration::from_secs(1), "tag" => "value");
/// ```
///
/// - `@s` converts a [`Duration`](std::time::Duration) to seconds via `.as_secs_f64()`.
/// - `@b` converts the value via `as f64` (identity for byte counts).
/// - No annotation converts via `as f64`.
#[macro_export]
macro_rules! distribution {
    ($name:literal @s : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::histogram!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .record($value.as_secs_f64());
    };
    ($name:literal @b : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::histogram!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .record($value as f64);
    };
    ($name:literal : $value:expr $(, $tag:literal => $tv:expr)* $(,)?) => {
        $crate::_macro_support::metrics::histogram!(
            $name
            $(, $tag => $crate::__label_value!($tv))*
        )
        .record($value as f64);
    };
}

/// Converts a tag value expression to a string suitable for a [`metrics::Label`].
///
/// This handles `&str`, `String`, integer types, and anything with a `Display` impl
/// by converting through `format!`. It relies on specialization-free dispatching:
/// `&str` and `String` pass through, everything else uses `format!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __label_value {
    ($e:expr) => {{
        // Use a trait-based dispatch that works for &str, String, and Display types.
        // The metrics crate accepts Into<SharedString> which covers &str, String, etc.
        $crate::_macro_support::metrics::SharedString::from(format!("{}", $e))
    }};
}
