//! Configuration for the objectstore server.
//!
//! This module provides the configuration system for the objectstore HTTP server. Configuration can
//! be loaded from multiple sources with the following precedence (highest to lowest):
//!
//! 1. Environment variables (prefixed with `OS__`)
//! 2. YAML configuration file (specified via `-c` or `--config` flag)
//! 3. Defaults
//!
//! See [`Config`] for a description of all configuration fields and their defaults.
//!
//! # Environment Variables
//!
//! Environment variables use `OS__` as a prefix and double underscores (`__`) to denote nested
//! configuration structures. For example:
//!
//! - `OS__HTTP_ADDR=0.0.0.0:8888` sets the HTTP server address
//! - `OS__LONG_TERM_STORAGE__TYPE=filesystem` sets the storage type
//! - `OS__LONG_TERM_STORAGE__PATH=/data` sets the directory name
//!
//! # YAML Configuration File
//!
//! Configuration can also be provided via a YAML file. The above configuration in YAML format would
//! look like this:
//!
//! ```yaml
//! http_addr: 0.0.0.0:8888
//!
//! long_term_storage:
//!   type: filesystem
//!   path: /data
//! ```

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use figment::providers::{Env, Format, Serialized, Yaml};
use objectstore_types::auth::Permission;
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;

use crate::killswitches::Killswitches;
use crate::rate_limits::RateLimits;

/// Environment variable prefix for all configuration options.
const ENV_PREFIX: &str = "OS__";

/// Newtype around `String` that may protect against accidental
/// logging of secrets in our configuration struct. Use with
/// [`secrecy::SecretBox`].
#[derive(Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ConfigSecret(String);

impl ConfigSecret {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for ConfigSecret {
    fn from(str: &str) -> Self {
        ConfigSecret(str.to_string())
    }
}

impl std::ops::Deref for ConfigSecret {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for ConfigSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[redacted]")
    }
}

impl CloneableSecret for ConfigSecret {}
impl SerializableSecret for ConfigSecret {}
impl Zeroize for ConfigSecret {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

/// Storage backend configuration.
///
/// The `type` field in YAML or `__TYPE` in environment variables determines which variant is used.
///
/// Used in: [`Config::high_volume_storage`], [`Config::long_term_storage`]
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Storage {
    /// Local filesystem storage backend (type `"filesystem"`).
    ///
    /// Stores objects as files on the local filesystem. Suitable for development, testing,
    /// and single-server deployments.
    ///
    /// # Example
    ///
    /// ```yaml
    /// long_term_storage:
    ///   type: filesystem
    ///   path: /data
    /// ```
    FileSystem {
        /// Directory path for storing objects.
        ///
        /// The directory will be created if it doesn't exist. Relative paths are resolved from
        /// the server's working directory.
        ///
        /// # Default
        ///
        /// `"data"` (relative to the server's working directory)
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__TYPE=filesystem`
        /// - `OS__HIGH_VOLUME_STORAGE__PATH=/path/to/storage`
        ///
        /// Or for long-term storage:
        /// - `OS__LONG_TERM_STORAGE__TYPE=filesystem`
        /// - `OS__LONG_TERM_STORAGE__PATH=/path/to/storage`
        path: PathBuf,
    },

    /// S3-compatible storage backend (type `"s3compatible"`).
    ///
    /// Supports [Amazon S3] and other S3-compatible services. Authentication is handled via
    /// environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) or IAM roles.
    ///
    /// [Amazon S3]: https://aws.amazon.com/s3/
    ///
    /// # Example
    ///
    /// ```yaml
    /// long_term_storage:
    ///   type: s3compatible
    ///   endpoint: https://s3.amazonaws.com
    ///   bucket: my-bucket
    /// ```
    S3Compatible {
        /// S3 endpoint URL.
        ///
        /// Examples: `https://s3.amazonaws.com`, `http://localhost:9000` (for MinIO)
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__TYPE=s3compatible`
        /// - `OS__HIGH_VOLUME_STORAGE__ENDPOINT=https://s3.amazonaws.com`
        ///
        /// Or for long-term storage:
        /// - `OS__LONG_TERM_STORAGE__TYPE=s3compatible`
        /// - `OS__LONG_TERM_STORAGE__ENDPOINT=https://s3.amazonaws.com`
        endpoint: String,

        /// S3 bucket name.
        ///
        /// The bucket must exist before starting the server.
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__BUCKET=my-bucket`
        /// - `OS__LONG_TERM_STORAGE__BUCKET=my-bucket`
        bucket: String,
    },

    /// [Google Cloud Storage] backend (type `"gcs"`).
    ///
    /// Stores objects in Google Cloud Storage (GCS). Authentication uses Application Default
    /// Credentials (ADC), which can be provided via the `GOOGLE_APPLICATION_CREDENTIALS`
    /// environment variable or GCE/GKE metadata service.
    ///
    /// **Note**: The bucket must be pre-created with the following lifecycle policy:
    /// - `daysSinceCustomTime`: 1 day
    /// - `action`: delete
    ///
    /// [Google Cloud Storage]: https://cloud.google.com/storage
    ///
    /// # Example
    ///
    /// ```yaml
    /// long_term_storage:
    ///   type: gcs
    ///   bucket: objectstore-bucket
    /// ```
    Gcs {
        /// Optional custom GCS endpoint URL.
        ///
        /// Useful for testing with emulators. If `None`, uses the default GCS endpoint.
        ///
        /// # Default
        ///
        /// `None` (uses default GCS endpoint)
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__TYPE=gcs`
        /// - `OS__HIGH_VOLUME_STORAGE__ENDPOINT=http://localhost:9000` (optional)
        ///
        /// Or for long-term storage:
        /// - `OS__LONG_TERM_STORAGE__TYPE=gcs`
        /// - `OS__LONG_TERM_STORAGE__ENDPOINT=http://localhost:9000` (optional)
        endpoint: Option<String>,

        /// GCS bucket name.
        ///
        /// The bucket must exist before starting the server.
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__BUCKET=my-gcs-bucket`
        /// - `OS__LONG_TERM_STORAGE__BUCKET=my-gcs-bucket`
        bucket: String,
    },

    /// [Google Bigtable] backend (type `"bigtable"`).
    ///
    /// Stores objects in Google Cloud Bigtable, a NoSQL wide-column database. This backend is
    /// optimized for high-throughput, low-latency workloads with small objects. Authentication uses
    /// Application Default Credentials (ADC).
    ///
    /// **Note**: The table must be pre-created with appropriate column families. Ensure to have the
    /// following column families:
    /// - `fg`: timestamp-based garbage collection (`maxage=1s`)
    /// - `fm`: manual garbage collection (`no GC policy`)
    ///
    /// [Google Bigtable]: https://cloud.google.com/bigtable
    ///
    /// # Example
    ///
    /// ```yaml
    /// high_volume_storage:
    ///   type: bigtable
    ///   project_id: my-project
    ///   instance_name: objectstore
    ///   table_name: objectstore
    /// ```
    BigTable {
        /// Optional custom Bigtable endpoint.
        ///
        /// Useful for testing with emulators. If `None`, uses the default Bigtable endpoint.
        ///
        /// # Default
        ///
        /// `None` (uses default Bigtable endpoint)
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__TYPE=bigtable`
        /// - `OS__HIGH_VOLUME_STORAGE__ENDPOINT=localhost:8086` (optional)
        ///
        /// Or for long-term storage:
        /// - `OS__LONG_TERM_STORAGE__TYPE=bigtable`
        /// - `OS__LONG_TERM_STORAGE__ENDPOINT=localhost:8086` (optional)
        endpoint: Option<String>,

        /// GCP project ID.
        ///
        /// The Google project ID (not project number) containing the Bigtable instance.
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__PROJECT_ID=my-project`
        /// - `OS__LONG_TERM_STORAGE__PROJECT_ID=my-project`
        project_id: String,

        /// Bigtable instance name.
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__INSTANCE_NAME=my-instance`
        /// - `OS__LONG_TERM_STORAGE__INSTANCE_NAME=my-instance`
        instance_name: String,

        /// Bigtable table name.
        ///
        /// The table must exist before starting the server.
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__TABLE_NAME=objectstore`
        /// - `OS__LONG_TERM_STORAGE__TABLE_NAME=objectstore`
        table_name: String,

        /// Optional number of connections to maintain to Bigtable.
        ///
        /// # Default
        ///
        /// `None` (infers connection count based on CPU count)
        ///
        /// # Environment Variables
        ///
        /// - `OS__HIGH_VOLUME_STORAGE__CONNECTIONS=16` (optional)
        /// - `OS__LONG_TERM_STORAGE__CONNECTIONS=16` (optional)
        connections: Option<usize>,
    },
}

/// Runtime configuration for the Tokio async runtime.
///
/// Controls the threading behavior of the server's async runtime.
///
/// Used in: [`Config::runtime`]
#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Runtime {
    /// Number of worker threads for the server runtime.
    ///
    /// This controls the size of the Tokio thread pool used to execute async tasks. More threads
    /// can improve concurrency for CPU-bound workloads, but too many threads can increase context
    /// switching overhead.
    ///
    /// Set this in accordance with the resources available to the server, especially in Kubernetes
    /// environments.
    ///
    /// # Default
    ///
    /// Defaults to the number of CPU cores on the host machine.
    ///
    /// # Environment Variable
    ///
    /// `OS__RUNTIME__WORKER_THREADS`
    ///
    /// # Considerations
    ///
    /// - For I/O-bound workloads, the default (number of CPU cores) is usually sufficient
    /// - For CPU-intensive workloads, consider matching or exceeding the number of cores
    /// - Setting this too high can lead to increased memory usage and context switching
    pub worker_threads: usize,

    /// Interval in seconds for reporting internal runtime metrics.
    ///
    /// Defaults to `10` seconds.
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// [Sentry](https://sentry.io/) error tracking and performance monitoring configuration.
///
/// Configures integration with Sentry for error tracking, performance monitoring, and distributed
/// tracing. Sentry is disabled by default and only enabled when a DSN is provided.
///
/// Used in: [`Config::sentry`]
#[derive(Debug, Deserialize, Serialize)]
pub struct Sentry {
    /// Sentry DSN (Data Source Name).
    ///
    /// When set, enables Sentry error tracking and performance monitoring. When `None`, Sentry
    /// integration is completely disabled.
    ///
    /// # Default
    ///
    /// `None` (Sentry disabled)
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__DSN`
    pub dsn: Option<SecretBox<ConfigSecret>>,

    /// Environment name for this deployment.
    ///
    /// Used to distinguish events from different environments (e.g., "production", "staging",
    /// "development"). This appears in the Sentry UI and can be used for filtering.
    ///
    /// # Default
    ///
    /// `None`
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__ENVIRONMENT`
    pub environment: Option<Cow<'static, str>>,

    /// Server name or identifier.
    ///
    /// Used to identify which server instance sent an event. Useful in multi-server deployments for
    /// tracking which instance encountered an error. Set to the hostname or pod name of the server.
    ///
    /// # Default
    ///
    /// `None`
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__SERVER_NAME`
    pub server_name: Option<Cow<'static, str>>,

    /// Error event sampling rate.
    ///
    /// Controls what percentage of error events are sent to Sentry. A value of `1.0` sends all
    /// errors, while `0.5` sends 50% of errors, and `0.0` sends no errors.
    ///
    /// # Default
    ///
    /// `1.0` (send all errors)
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__SAMPLE_RATE`
    pub sample_rate: f32,

    /// Performance trace sampling rate.
    ///
    /// Controls what percentage of transactions (traces) are sent to Sentry for performance
    /// monitoring. A value of `1.0` sends all traces, while `0.01` sends 1% of traces.
    ///
    /// **Important**: Performance traces can generate significant data volume in high-traffic
    /// systems. Start with a low rate (0.01-0.1) and adjust based on traffic and Sentry quota.
    ///
    /// # Default
    ///
    /// `0.01` (send 1% of traces)
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__TRACES_SAMPLE_RATE`
    pub traces_sample_rate: f32,

    /// Whether to inherit sampling decisions from incoming traces.
    ///
    /// When `true` (default), if an incoming request contains a distributed tracing header with a
    /// sampling decision (e.g., from an upstream service), that decision is honored. When `false`,
    /// the local `traces_sample_rate` is always used instead.
    ///
    /// When this is enabled, the calling service effectively controls the sampling decision for the
    /// entire trace. Set this to `false` if you want to have independent sampling control at the
    /// objectstore level.
    ///
    /// # Default
    ///
    /// `true`
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__INHERIT_SAMPLING_DECISION`
    pub inherit_sampling_decision: bool,

    /// Enable Sentry SDK debug mode.
    ///
    /// When enabled, the Sentry SDK will output debug information to stderr, which can be useful
    /// for troubleshooting Sentry integration issues. It is discouraged to enable this in
    /// production as it generates verbose logging.
    ///
    /// # Default
    ///
    /// `false`
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__DEBUG`
    pub debug: bool,

    /// Additional tags to attach to all Sentry events.
    ///
    /// Key-value pairs that are sent as tags with every event reported to Sentry. Useful for adding
    /// context such as deployment identifiers or environment details.
    ///
    /// # Default
    ///
    /// Empty (no tags)
    ///
    /// # Environment Variables
    ///
    /// Each tag is set individually:
    /// - `OS__SENTRY__TAGS__FOO=foo`
    /// - `OS__SENTRY__TAGS__BAR=bar`
    ///
    /// # YAML Example
    ///
    /// ```yaml
    /// sentry:
    ///   tags:
    ///     foo: foo
    ///     bar: bar
    /// ```
    pub tags: BTreeMap<String, String>,
}

impl Sentry {
    /// Returns whether Sentry integration is enabled.
    ///
    /// Sentry is considered enabled if a DSN is configured.
    pub fn is_enabled(&self) -> bool {
        self.dsn.is_some()
    }
}

impl Default for Sentry {
    fn default() -> Self {
        Self {
            dsn: None,
            environment: None,
            server_name: None,
            sample_rate: 1.0,
            traces_sample_rate: 0.01,
            inherit_sampling_decision: true,
            debug: false,
            tags: BTreeMap::new(),
        }
    }
}

/// Log output format.
///
/// Controls how log messages are formatted. The format can be explicitly specified or
/// auto-detected based on whether output is to a TTY.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Auto detect the best format.
    ///
    /// This chooses [`LogFormat::Pretty`] for TTY, otherwise [`LogFormat::Simplified`].
    Auto,

    /// Pretty printing with colors.
    ///
    /// ```text
    ///  INFO  objectstore::http > objectstore starting
    /// ```
    Pretty,

    /// Simplified plain text output.
    ///
    /// ```text
    /// 2020-12-04T12:10:32Z [objectstore::http] INFO: objectstore starting
    /// ```
    Simplified,

    /// Dump out JSON lines.
    ///
    /// ```text
    /// {"timestamp":"2020-12-04T12:11:08.729716Z","level":"INFO","logger":"objectstore::http","message":"objectstore starting","module_path":"objectstore::http","filename":"objectstore_service/src/http.rs","lineno":31}
    /// ```
    Json,
}

/// The logging format parse error.
#[derive(Clone, Debug)]
pub struct FormatParseError(String);

impl fmt::Display for FormatParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"error parsing "{}" as format: expected one of "auto", "pretty", "simplified", "json""#,
            self.0
        )
    }
}

impl std::str::FromStr for LogFormat {
    type Err = FormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "" => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("auto") => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("pretty") => LogFormat::Pretty,
            s if s.eq_ignore_ascii_case("simplified") => LogFormat::Simplified,
            s if s.eq_ignore_ascii_case("json") => LogFormat::Json,
            s => return Err(FormatParseError(s.into())),
        };

        Ok(result)
    }
}

impl std::error::Error for FormatParseError {}

mod display_fromstr {
    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: std::fmt::Display,
    {
        serializer.collect_str(&value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        use serde::Deserialize;
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Logging configuration.
///
/// Controls the verbosity and format of log output. Logs are always written to stderr.
///
/// Used in: [`Config::logging`]
#[derive(Debug, Deserialize, Serialize)]
pub struct Logging {
    /// Minimum log level to output.
    ///
    /// Controls which log messages are emitted based on their severity. Messages at or above this
    /// level will be output. Valid levels in increasing severity: TRACE, DEBUG, INFO, WARN, ERROR,
    /// OFF.
    ///
    /// The `RUST_LOG` environment variable provides more granular control per module if needed.
    ///
    /// **Important**: Levels `DEBUG` and `TRACE` are very verbose and can impact performance; use
    /// only for debugging.
    ///
    /// # Default
    ///
    /// `INFO`
    ///
    /// # Environment Variable
    ///
    /// `OS__LOGGING__LEVEL`
    ///
    /// # Considerations
    ///
    /// - `TRACE` and `DEBUG` can be very verbose and impact performance; use only for debugging
    /// - `INFO` is appropriate for production
    /// - `WARN` or `ERROR` can be used to reduce log volume in high-traffic systems
    /// -
    #[serde(with = "display_fromstr")]
    pub level: LevelFilter,

    /// Log output format.
    ///
    /// Determines how log messages are formatted. See [`LogFormat`] for available options and
    /// examples.
    ///
    /// # Default
    ///
    /// `Auto` (pretty for TTY, simplified otherwise)
    ///
    /// # Environment Variable
    ///
    /// `OS__LOGGING__FORMAT`
    pub format: LogFormat,
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            level: LevelFilter::INFO,
            format: LogFormat::Auto,
        }
    }
}

/// Metrics configuration.
///
/// Configures submission of internal metrics to Datadog.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Metrics {
    /// Datadog [API key] for metrics.
    ///
    /// When provided, enables metrics reporting to Datadog. Metrics include request counts,
    /// latencies, storage operations, and more. The key is kept secret and redacted from logs.
    ///
    /// # Default
    ///
    /// `None` (Datadog metrics disabled)
    ///
    /// # Environment Variable
    ///
    /// `OS__METRICS__DATADOG_KEY`
    ///
    /// [API key]: https://docs.datadoghq.com/account_management/api-app-keys/#api-keys
    pub datadog_key: Option<SecretBox<ConfigSecret>>,

    /// Global tags applied to all metrics.
    ///
    /// Key-value pairs that are attached to every metric sent to Datadog. Useful for
    /// identifying the environment, region, or other deployment-specific information.
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

/// A key that may be used to verify a request's `Authorization` header and its
/// associated permissions. May contain multiple key versions to facilitate rotation.
#[derive(Debug, Deserialize, Serialize)]
pub struct AuthZVerificationKey {
    /// Files that contain versions of this key's key material which may be used to verify
    /// signatures.
    ///
    /// If a key is being rotated, the old and new versions of that key should both be
    /// configured so objectstore can verify signatures while the updated key is still
    /// rolling out. Otherwise, this should only contain the most recent version of a key.
    pub key_files: Vec<PathBuf>,

    /// The maximum set of permissions that this key's signer is authorized to grant.
    ///
    /// If a request's `Authorization` header grants full permission but it was signed by
    /// a key that is only allowed to grant read permission, then the request only has
    /// read permission.
    #[serde(default)]
    pub max_permissions: HashSet<Permission>,
}

/// Configuration for content-based authorization.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct AuthZ {
    /// Whether to enforce content-based authorization or not.
    ///
    /// If this is set to `false`, checks are still performed but failures will not result
    /// in `403 Unauthorized` responses.
    pub enforce: bool,

    /// Keys that may be used to verify a request's `Authorization` header.
    ///
    /// This field is a container that is keyed on a key's ID. When verifying a JWT
    /// from the `Authorization` header, the `kid` field should be read from the JWT
    /// header and used to index into this map to select the appropriate key.
    #[serde(default)]
    pub keys: BTreeMap<String, AuthZVerificationKey>,
}

/// Main configuration struct for the objectstore server.
///
/// This is the top-level configuration that combines all server settings including networking,
/// storage backends, runtime, and observability options.
///
/// Configuration is loaded with the following precedence (highest to lowest):
/// 1. Environment variables (prefixed with `OS__`)
/// 2. YAML configuration file (if provided via `-c` flag)
/// 3. Default values
///
/// See individual field documentation for details on each configuration option, including
/// defaults and environment variables.
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    /// HTTP server bind address.
    ///
    /// The socket address (IP and port) where the HTTP server will listen for incoming
    /// connections. Supports both IPv4 and IPv6 addresses. Note that binding to `0.0.0.0`
    /// makes the server accessible from all network interfaces.
    ///
    /// # Default
    ///
    /// `0.0.0.0:8888` (listens on all network interfaces, port 8888)
    ///
    /// # Environment Variable
    ///
    /// `OS__HTTP_ADDR`
    pub http_addr: SocketAddr,

    /// Storage backend for high-volume, small objects.
    ///
    /// This backend is used for smaller objects in scenarios where high-throughput, low-latency
    /// access with many small objects is desired. Good candidates include Bigtable, local
    /// filesystem (for development), or fast SSDs. Can be set to the same backend as
    /// `long_term_storage` for simplicity.
    ///
    /// **Note**: Currently, objects up to 1 MiB are stored in this backend, while larger objects
    /// are stored in the [`long_term_storage`](`Config::long_term_storage`). This is subject to
    /// change in the future and more configuration options will be added to influence this
    /// decision.
    ///
    /// # Default
    ///
    /// Filesystem storage in `./data/high-volume` directory
    ///
    /// # Environment Variables
    ///
    /// - `OS__HIGH_VOLUME_STORAGE__TYPE` for the backend type. See [`Storage`] for available
    ///   options.
    ///
    /// # Example
    ///
    /// ```yaml
    /// high_volume_storage:
    ///   type: bigtable
    ///   project_id: my-project
    ///   instance_name: objectstore
    ///   table_name: objectstore
    /// ```
    pub high_volume_storage: Storage,

    /// Storage backend for large objects with long-term retention.
    ///
    /// This backend is used for larger objects in scenarios with lower throughput and higher
    /// latency requirements. Good candidates include S3, Google Cloud Storage, or other object
    /// storage systems. Can be set to the same backend as `high_volume_storage` for simplicity.
    ///
    /// **Note**: Currently, objects over 1 MiB are stored in this backend, while smaller objects
    /// are stored in the [`high_volume_storage`](`Config::high_volume_storage`). This is subject to
    /// change in the future and more configuration options will be added to influence this
    /// decision.
    ///
    /// # Default
    ///
    /// Filesystem storage in `./data/long-term` directory
    ///
    /// # Environment Variables
    ///
    /// - `OS__LONG_TERM_STORAGE__TYPE` - Backend type (filesystem, s3compatible, gcs, bigtable)
    /// - Additional fields depending on the type (see [`Storage`])
    ///
    /// # Example
    ///
    /// ```yaml
    /// long_term_storage:
    ///   type: gcs
    ///   bucket: my-objectstore-bucket
    /// ```
    pub long_term_storage: Storage,

    /// Configuration of the internal task runtime.
    ///
    /// Controls the thread pool size and behavior of the async runtime powering the server.
    /// See [`Runtime`] for configuration options.
    pub runtime: Runtime,

    /// Logging configuration.
    ///
    /// Controls log verbosity and output format. See [`Logging`] for configuration options.
    pub logging: Logging,

    /// Sentry error tracking configuration.
    ///
    /// Optional integration with Sentry for error tracking and performance monitoring.
    /// See [`Sentry`] for configuration options.
    pub sentry: Sentry,

    /// Internal metrics configuration.
    ///
    /// Optional configuration for submitting internal metrics to Datadog. See [`Metrics`] for
    /// configuration options.
    pub metrics: Metrics,

    /// Content-based authorization configuration.
    ///
    /// Controls the verification and enforcement of content-based access control based on the
    /// JWT in a request's `Authorization` header.
    pub auth: AuthZ,

    /// A list of matchers for requests to discard without processing.
    pub killswitches: Killswitches,

    /// Definitions for rate limits to enforce on incoming requests.
    pub rate_limits: RateLimits,

    /// Configuration for the [`StorageService`](objectstore_service::StorageService).
    pub service: Service,
}

/// Configuration for the [`StorageService`](objectstore_service::StorageService).
///
/// Controls operational parameters of the storage service layer that sits
/// between the HTTP server and the storage backends.
///
/// Used in: [`Config::service`]
///
/// # Environment Variables
///
/// - `OS__SERVICE__MAX_CONCURRENCY`
#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Service {
    /// Maximum number of concurrent backend operations.
    ///
    /// This caps the total number of in-flight storage operations (reads,
    /// writes, deletes) across all requests. Operations that exceed the limit
    /// are rejected with HTTP 429.
    ///
    /// # Default
    ///
    /// [`DEFAULT_CONCURRENCY_LIMIT`](objectstore_service::service::DEFAULT_CONCURRENCY_LIMIT)
    pub max_concurrency: usize,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            max_concurrency: objectstore_service::service::DEFAULT_CONCURRENCY_LIMIT,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8888".parse().unwrap(),

            high_volume_storage: Storage::FileSystem {
                path: PathBuf::from("data/high-volume"),
            },
            long_term_storage: Storage::FileSystem {
                path: PathBuf::from("data/long-term"),
            },

            runtime: Runtime::default(),
            logging: Logging::default(),
            sentry: Sentry::default(),
            metrics: Metrics::default(),
            auth: AuthZ::default(),
            killswitches: Killswitches::default(),
            rate_limits: RateLimits::default(),
            service: Service::default(),
        }
    }
}

impl Config {
    /// Loads configuration from the provided arguments.
    ///
    /// Configuration is merged in the following order (later sources override earlier ones):
    /// 1. Default values
    /// 2. YAML configuration file (if provided in `args`)
    /// 3. Environment variables (prefixed with `OS__`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The YAML configuration file cannot be read or parsed
    /// - Environment variables contain invalid values
    /// - Required fields are missing or invalid
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut figment = figment::Figment::from(Serialized::defaults(Config::default()));
        if let Some(path) = path {
            figment = figment.merge(Yaml::file(path));
        }
        let config = figment
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
            .extract()?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use secrecy::ExposeSecret;

    use crate::killswitches::Killswitch;
    use crate::rate_limits::{BandwidthLimits, RateLimits, ThroughputLimits, ThroughputRule};

    use super::*;

    #[test]
    fn configurable_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__LONG_TERM_STORAGE__TYPE", "s3compatible");
            jail.set_env("OS__LONG_TERM_STORAGE__ENDPOINT", "http://localhost:8888");
            jail.set_env("OS__LONG_TERM_STORAGE__BUCKET", "whatever");
            jail.set_env("OS__METRICS__TAGS__FOO", "bar");
            jail.set_env("OS__METRICS__TAGS__BAZ", "qux");
            jail.set_env("OS__SENTRY__DSN", "abcde");
            jail.set_env("OS__SENTRY__SAMPLE_RATE", "0.5");
            jail.set_env("OS__SENTRY__ENVIRONMENT", "production");
            jail.set_env("OS__SENTRY__SERVER_NAME", "objectstore-deadbeef");
            jail.set_env("OS__SENTRY__TRACES_SAMPLE_RATE", "0.5");
            jail.set_env("OS__SENTRY__TRACES_SAMPLE_RATE", "0.5");

            let config = Config::load(None).unwrap();

            let Storage::S3Compatible { endpoint, bucket } = &dbg!(&config).long_term_storage
            else {
                panic!("expected s3 storage");
            };
            assert_eq!(endpoint, "http://localhost:8888");
            assert_eq!(bucket, "whatever");
            assert_eq!(
                config.metrics.tags,
                [("foo".into(), "bar".into()), ("baz".into(), "qux".into())].into()
            );

            assert_eq!(config.sentry.dsn.unwrap().expose_secret().as_str(), "abcde");
            assert_eq!(config.sentry.environment.as_deref(), Some("production"));
            assert_eq!(
                config.sentry.server_name.as_deref(),
                Some("objectstore-deadbeef")
            );
            assert_eq!(config.sentry.sample_rate, 0.5);
            assert_eq!(config.sentry.traces_sample_rate, 0.5);

            Ok(())
        });
    }

    #[test]
    fn configurable_via_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
            long_term_storage:
                type: s3compatible
                endpoint: http://localhost:8888
                bucket: whatever
            sentry:
                dsn: abcde
                environment: production
                server_name: objectstore-deadbeef
                sample_rate: 0.5
                traces_sample_rate: 0.5
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();

            let Storage::S3Compatible { endpoint, bucket } = &dbg!(&config).long_term_storage
            else {
                panic!("expected s3 storage");
            };
            assert_eq!(endpoint, "http://localhost:8888");
            assert_eq!(bucket, "whatever");

            assert_eq!(config.sentry.dsn.unwrap().expose_secret().as_str(), "abcde");
            assert_eq!(config.sentry.environment.as_deref(), Some("production"));
            assert_eq!(
                config.sentry.server_name.as_deref(),
                Some("objectstore-deadbeef")
            );
            assert_eq!(config.sentry.sample_rate, 0.5);
            assert_eq!(config.sentry.traces_sample_rate, 0.5);

            Ok(())
        });
    }

    #[test]
    fn configured_with_env_and_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
            long_term_storage:
                type: s3compatible
                endpoint: http://localhost:8888
                bucket: whatever
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__LONG_TERM_STORAGE__ENDPOINT", "http://localhost:9001");

            let config = Config::load(Some(tempfile.path())).unwrap();

            let Storage::S3Compatible {
                endpoint,
                bucket: _bucket,
            } = &dbg!(&config).long_term_storage
            else {
                panic!("expected s3 storage");
            };
            // Env should overwrite the yaml config
            assert_eq!(endpoint, "http://localhost:9001");

            Ok(())
        });
    }

    #[test]
    fn configure_auth_with_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__AUTH__ENFORCE", "true");
            jail.set_env(
                "OS__AUTH__KEYS",
                r#"{kid1={key_files=["abcde","fghij","this is a test\n  multiline string\nend of string\n"],max_permissions=["object.read", "object.write"],}, kid2={key_files=["12345"],}}"#,
            );

            let config = Config::load(None).unwrap();

            assert!(config.auth.enforce);

            let kid1 = config.auth.keys.get("kid1").unwrap();
            assert_eq!(kid1.key_files[0], Path::new("abcde"));
            assert_eq!(kid1.key_files[1], Path::new("fghij"));
            assert_eq!(
                kid1.key_files[2],
                Path::new("this is a test\n  multiline string\nend of string\n"),
            );
            assert_eq!(
                kid1.max_permissions,
                HashSet::from([Permission::ObjectRead, Permission::ObjectWrite])
            );

            let kid2 = config.auth.keys.get("kid2").unwrap();
            assert_eq!(kid2.key_files[0], Path::new("12345"));
            assert_eq!(kid2.max_permissions, HashSet::new());

            Ok(())
        });
    }

    #[test]
    fn configure_auth_with_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
                auth:
                    enforce: true
                    keys:
                        kid1:
                            key_files:
                                - "abcde"
                                - "fghij"
                                - |
                                  this is a test
                                    multiline string
                                  end of string
                            max_permissions:
                                - "object.read"
                                - "object.write"
                        kid2:
                            key_files:
                                - "12345"
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();

            assert!(config.auth.enforce);

            let kid1 = config.auth.keys.get("kid1").unwrap();
            assert_eq!(kid1.key_files[0], Path::new("abcde"));
            assert_eq!(kid1.key_files[1], Path::new("fghij"));
            assert_eq!(
                kid1.key_files[2],
                Path::new("this is a test\n  multiline string\nend of string\n")
            );
            assert_eq!(
                kid1.max_permissions,
                HashSet::from([Permission::ObjectRead, Permission::ObjectWrite])
            );

            let kid2 = config.auth.keys.get("kid2").unwrap();
            assert_eq!(kid2.key_files[0], Path::new("12345"));
            assert_eq!(kid2.max_permissions, HashSet::new());

            Ok(())
        });
    }

    #[test]
    fn configure_killswitches_with_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
                killswitches:
                  - usecase: broken_usecase
                  - scopes:
                      org: "42"
                  - service: "test-*"
                  - scopes:
                      org: "42"
                      project: "4711"
                  - usecase: attachments
                    scopes:
                      org: "42"
                    service: "test-*"
                "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let expected = [
                Killswitch {
                    usecase: Some("broken_usecase".into()),
                    scopes: BTreeMap::new(),
                    service: None,
                    service_matcher: std::sync::OnceLock::new(),
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::from([("org".into(), "42".into())]),
                    service: None,
                    service_matcher: std::sync::OnceLock::new(),
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::new(),
                    service: Some("test-*".into()),
                    service_matcher: std::sync::OnceLock::new(),
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::from([
                        ("org".into(), "42".into()),
                        ("project".into(), "4711".into()),
                    ]),
                    service: None,
                    service_matcher: std::sync::OnceLock::new(),
                },
                Killswitch {
                    usecase: Some("attachments".into()),
                    scopes: BTreeMap::from([("org".into(), "42".into())]),
                    service: Some("test-*".into()),
                    service_matcher: std::sync::OnceLock::new(),
                },
            ];

            let config = Config::load(Some(tempfile.path())).unwrap();
            assert_eq!(&config.killswitches.0, &expected,);

            Ok(())
        });
    }

    #[test]
    fn configure_rate_limits_with_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
                rate_limits:
                  throughput:
                    global_rps: 1000
                    burst: 100
                    usecase_pct: 50
                    scope_pct: 25
                    rules:
                      - usecase: "high_priority"
                        scopes:
                          - ["org", "123"]
                        rps: 500
                      - scopes:
                          - ["org", "456"]
                          - ["project", "789"]
                        pct: 10
                  bandwidth:
                    global_bps: 1048576
                    usecase_pct: 50
                    scope_pct: 25
                "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let expected = RateLimits {
                throughput: ThroughputLimits {
                    global_rps: Some(1000),
                    burst: 100,
                    usecase_pct: Some(50),
                    scope_pct: Some(25),
                    rules: vec![
                        ThroughputRule {
                            usecase: Some("high_priority".to_string()),
                            scopes: vec![("org".to_string(), "123".to_string())],
                            rps: Some(500),
                            pct: None,
                        },
                        ThroughputRule {
                            usecase: None,
                            scopes: vec![
                                ("org".to_string(), "456".to_string()),
                                ("project".to_string(), "789".to_string()),
                            ],
                            rps: None,
                            pct: Some(10),
                        },
                    ],
                },
                bandwidth: BandwidthLimits {
                    global_bps: Some(1_048_576),
                    usecase_pct: Some(50),
                    scope_pct: Some(25),
                },
            };

            let config = Config::load(Some(tempfile.path())).unwrap();
            assert_eq!(config.rate_limits, expected);

            Ok(())
        });
    }
}
