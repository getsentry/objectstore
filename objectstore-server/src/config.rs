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
//! - `OS__STORAGE__TYPE=filesystem` sets the storage type
//! - `OS__STORAGE__PATH=/data` sets the directory path
//!
//! # YAML Configuration File
//!
//! Configuration can also be provided via a YAML file. The above configuration in YAML format would
//! look like this:
//!
//! ```yaml
//! http_addr: 0.0.0.0:8888
//!
//! storage:
//!   type: filesystem
//!   path: /data
//! ```
//!
//! # Variable References
//!
//! Any configuration value may be written as a reference, which is resolved after all
//! sources have been merged. `${file:PATH}` is replaced by that file's contents and
//! `${VAR_NAME}` by that environment variable:
//!
//! ```yaml
//! storage_cogs:
//!   type: kafka
//!   override_params:
//!     sasl.password: ${file:/var/secrets/kafka-password}
//! ```
//!
//! A reference must be the entire value: `${A}` works, `prefix-${A}` does not. Referencing
//! a file that cannot be read, or an environment variable that is not set, is an error at
//! startup.
//!
//! ## Relationship to `OS__` environment variables
//!
//! These are different mechanisms and neither replaces the other. They can even be used
//! together: `OS__SENTRY__DSN=${SENTRY_DSN}` will set the `sentry.dsn` YAML key to the
//! value of the `SENTRY_DSN` environment variable.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use figment::providers::{Env, Format, Serialized, Yaml};
use objectstore_service::backend::local_fs::FileSystemConfig;
use objectstore_service::change_stream::CostTrackerConfig;
use objectstore_service::resumable::ResumableTokenEncryption;
use objectstore_types::auth::Permission;
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

pub use objectstore_log::{LevelFilter, LogFormat, LoggingConfig};
pub use objectstore_service::backend::{MultipartUploadStorageConfig, StorageConfig};

use crate::killswitches::Killswitches;
use crate::rate_limits::RateLimits;
use crate::usecases::UseCases;

/// Environment variable prefix for all configuration options.
const ENV_PREFIX: &str = "OS__";

/// Newtype around `String` that may protect against accidental
/// logging of secrets in our configuration struct. Use with
/// [`secrecy::SecretBox`].
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigSecret(String);

impl ConfigSecret {
    /// Returns the secret value as a string slice.
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

    /// Whether to attach stack traces to captured errors and messages.
    ///
    /// When enabled, the attached stack trace starts where the event is captured.
    ///
    /// # Default
    ///
    /// `false`
    ///
    /// # Environment Variable
    ///
    /// `OS__SENTRY__ATTACH_STACKTRACE`
    pub attach_stacktrace: bool,

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
            attach_stacktrace: false,
            debug: false,
            tags: BTreeMap::new(),
        }
    }
}

// Logging configuration is defined in `objectstore_log::LoggingConfig`.
// Metrics configuration is defined in `objectstore_metrics::MetricsConfig`.

/// A key that may be used to verify a request's auth token and its associated
/// permissions. May contain multiple key versions to facilitate rotation.
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
    /// If a request's auth token grants full permission but it was signed by a key
    /// that is only allowed to grant read permission, then the request only has
    /// read permission.
    #[serde(default)]
    pub max_permissions: HashSet<Permission>,
}

/// Configuration for content-based authorization.
#[derive(Debug, Deserialize, Serialize)]
pub struct AuthZ {
    /// Whether to enforce content-based authorization or not.
    ///
    /// Defaults to `true`, resulting in `403 Unauthorized` responses for unauthorized requests. Set
    /// to `false` to permit unauthorized requests. Authorization checks are still performed if
    /// keys are configured, but only result in warnings.
    #[serde(default = "default_enforce")]
    pub enforce: bool,

    /// Keys that may be used to verify a request's auth token.
    ///
    /// The auth token is read from the `X-Os-Auth` header (preferred)
    /// or the standard `Authorization` header (fallback). This field is a
    /// container keyed on a key's ID. When verifying a JWT, the `kid` field
    /// should be read from the JWT header and used to index into this map to
    /// select the appropriate key.
    #[serde(default)]
    pub keys: BTreeMap<String, AuthZVerificationKey>,
}

fn default_enforce() -> bool {
    true
}

impl AuthZ {
    /// Returns whether content-based authorization is active.
    ///
    /// Authorization is considered active if enforcement is enabled or at least one key is
    /// configured. Without enforcement, authorization checks are still performed and reported but
    /// failures will not result in `403 Unauthorized`
    pub fn is_active(&self) -> bool {
        self.enforce || !self.keys.is_empty()
    }
}

impl Default for AuthZ {
    fn default() -> Self {
        Self {
            enforce: true,
            keys: BTreeMap::new(),
        }
    }
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

    /// Storage backend configuration.
    ///
    /// Configures the storage backend used by the server. Use `type: "filesystem"` for
    /// development, `type: "tiered"` for production two-tier routing (small objects to a
    /// high-volume backend, large objects to a long-term backend), or any other single backend
    /// type for simple deployments.
    ///
    /// # Default
    ///
    /// Filesystem storage in the `./data` directory
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE` — backend type (`filesystem`, `tiered`, `gcs`, `bigtable`,
    ///   `s3compatible`)
    /// - Additional fields depending on the type (see [`StorageConfig`])
    ///
    /// For tiered storage, sub-backend fields are nested under `high_volume` and `long_term`:
    /// - `OS__STORAGE__TYPE=tiered`
    /// - `OS__STORAGE__HIGH_VOLUME__TYPE=bigtable`
    /// - `OS__STORAGE__LONG_TERM__TYPE=gcs`
    ///
    /// # Example (tiered)
    ///
    /// ```yaml
    /// storage:
    ///   type: tiered
    ///   high_volume:
    ///     type: bigtable
    ///     project_id: my-project
    ///     instance_name: objectstore
    ///     table_name: objectstore
    ///   long_term:
    ///     type: gcs
    ///     bucket: my-objectstore-bucket
    /// ```
    pub storage: StorageConfig,

    /// Cost tracking sink for backends' change streams.
    ///
    /// A transport owns connections and a send queue, so it is configured once here and
    /// shared by every backend. What each backend reports, and how much of it, is
    /// configured per backend under [`storage`](Self::storage).
    ///
    /// Absent is the default, and disables reporting entirely.
    ///
    /// # Example
    ///
    /// ```yaml
    /// storage_cogs:
    ///   type: kafka
    ///   topic: shared-resources-inventory
    ///   bootstrap_servers: [kafka:9092]
    /// ```
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE_COGS__TYPE=kafka`
    /// - `OS__STORAGE_COGS__TOPIC=shared-resources-inventory`
    /// - `OS__STORAGE_COGS__BOOTSTRAP_SERVERS=kafka:9092`
    /// - `OS__STORAGE_COGS__OVERRIDE_PARAMS__<PROPERTY>=<value>`
    #[serde(default)]
    pub storage_cogs: Option<CostTrackerConfig>,

    /// Configuration of the internal task runtime.
    ///
    /// Controls the thread pool size and behavior of the async runtime powering the server.
    /// See [`Runtime`] for configuration options.
    pub runtime: Runtime,

    /// Logging configuration.
    ///
    /// Controls log verbosity and output format. See [`LoggingConfig`] for configuration options.
    pub logging: LoggingConfig,

    /// Sentry error tracking configuration.
    ///
    /// Optional integration with Sentry for error tracking and performance monitoring.
    /// See [`Sentry`] for configuration options.
    pub sentry: Sentry,

    /// Internal metrics configuration.
    ///
    /// Configures submission of internal metrics to a DogStatsD-compatible endpoint.
    /// See [`objectstore_metrics::MetricsConfig`] for configuration options.
    pub metrics: objectstore_metrics::MetricsConfig,

    /// Content-based authorization configuration.
    ///
    /// Controls the verification and enforcement of content-based access control based on the
    /// JWT in a request's `X-Os-Auth` or `Authorization` header.
    pub auth: AuthZ,

    /// A list of matchers for requests to discard without processing.
    pub killswitches: Killswitches,

    /// Definitions for rate limits to enforce on incoming requests.
    pub rate_limits: RateLimits,

    /// Per-use-case configuration.
    ///
    /// Controls properties of individual use cases such as which expiration
    /// policies are permitted and their maximum durations. Use cases not
    /// present in the map receive default configuration (all policies allowed,
    /// no duration caps).
    pub usecases: UseCases,

    /// Configuration for the [`StorageService`](objectstore_service::StorageService).
    pub service: Service,

    /// Configuration for the HTTP layer.
    ///
    /// Controls HTTP-level settings that operate before requests reach the
    /// storage service. See [`Http`] for configuration options.
    pub http: Http,
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
/// - `OS__SERVICE__CONCURRENCY_QUEUE`
/// - `OS__SERVICE__CONCURRENCY_TIMEOUT`
/// - `OS__SERVICE__BULK_CONCURRENCY_PCT`
/// - `OS__SERVICE__RESUMABLE_TOKEN_ENCRYPTION__ACTIVE_KEY_ID`
/// - `OS__SERVICE__RESUMABLE_TOKEN_ENCRYPTION__KEY_FILES`
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
    pub max_concurrency: u32,

    /// Maximum number of requests that may wait for a concurrency permit.
    ///
    /// When all `max_concurrency` execution slots are held, up to this many
    /// additional requests will park and wait (for at most
    /// `concurrency_timeout`) instead of being rejected immediately.
    /// Requests beyond that are rejected with HTTP 429.
    ///
    /// Sizing guidance: `concurrency_queue ≈ permit_release_rate ×
    /// acceptable_added_latency`.
    ///
    /// # Default
    ///
    /// `0`
    pub concurrency_queue: u32,

    /// Maximum time a caller may wait for a concurrency permit.
    ///
    /// Applies to both queued normal requests and bulk operations
    /// waiting for the bulk and execution semaphores.
    ///
    /// # Default
    ///
    /// `1s`
    #[serde(with = "humantime_serde")]
    pub concurrency_timeout: Duration,

    /// Percentage of `max_concurrency` available to bulk operations
    /// (e.g. parallelized batch requests).
    ///
    /// This sets a safe operating point: below this level there is
    /// little-to-no performance degradation, leaving room for more tasks
    /// to be admitted via the queue before rejection is necessary.
    ///
    /// Clamped to 1..=100. At 100, bulk operations can use all execution
    /// slots. Lower values leave headroom for single-object requests.
    ///
    /// # Default
    ///
    /// `60`
    pub bulk_concurrency_pct: u32,

    /// Persistent encryption keys for resumable-upload session tokens returned to clients.
    ///
    /// Tokens are always encrypted. When this is absent, Objectstore generates a fresh in-memory
    /// AES-256 key at startup, so resumable sessions become invalid after a restart. Configure a
    /// persistent keyring for sessions that must survive restarts. Keep old keys configured while
    /// their sessions may still be active; removing a key intentionally invalidates those sessions.
    ///
    /// ```yaml
    /// service:
    ///   resumable_token_encryption:
    ///     active_key_id: v1
    ///     key_files:
    ///       v1: /var/run/secrets/objectstore/resumable-upload-v1
    /// ```
    pub resumable_token_encryption: Option<ResumableTokenEncryptionConfig>,
}

impl Service {
    /// Loads and validates the configured resumable token encryption keys.
    pub(crate) fn resumable_token_encryption(&self) -> Result<Option<ResumableTokenEncryption>> {
        let Some(config) = &self.resumable_token_encryption else {
            return Ok(None);
        };

        let mut keys = BTreeMap::new();
        for (key_id, filename) in &config.key_files {
            let bytes = std::fs::read(filename).map_err(|error| {
                anyhow::anyhow!("reading resumable token key {filename:?}: {error}")
            })?;
            keys.insert(key_id.clone(), bytes);
        }

        ResumableTokenEncryption::new(config.active_key_id.clone(), keys).map(Some)
    }
}

/// AES-256-GCM keys used to protect externally visible resumable session tokens.
#[derive(Clone, Deserialize, Serialize)]
pub struct ResumableTokenEncryptionConfig {
    /// Key used to encrypt newly created sessions.
    pub active_key_id: String,
    /// Files containing raw, exactly 32-byte AES-256 keys, indexed by rotation ID.
    #[serde(default)]
    pub key_files: BTreeMap<String, PathBuf>,
}

impl fmt::Debug for ResumableTokenEncryptionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResumableTokenEncryptionConfig")
            .field("active_key_id", &self.active_key_id)
            .field("key_ids", &self.key_files.keys().collect::<Vec<_>>())
            .field("key_files", &self.key_files)
            .finish()
    }
}

impl Default for Service {
    fn default() -> Self {
        Self {
            max_concurrency: objectstore_service::service::DEFAULT_CONCURRENCY_LIMIT,
            concurrency_queue: 0,
            concurrency_timeout: Duration::from_secs(1),
            bulk_concurrency_pct: 60,
            resumable_token_encryption: None,
        }
    }
}

/// Default maximum number of concurrent in-flight HTTP requests.
///
/// Requests beyond this limit are rejected with HTTP 503.
pub const DEFAULT_MAX_HTTP_REQUESTS: usize = 10_000;

/// Configuration for the HTTP layer.
///
/// Controls behaviour at the HTTP request level, before requests reach the
/// storage service. Grouping these settings separately from [`Service`] keeps
/// HTTP-layer and service-layer concerns distinct and provides a natural home
/// for future HTTP-level settings (e.g. timeouts, body size limits).
///
/// Used in: [`Config::http`]
///
/// # Environment Variables
///
/// - `OS__HTTP__MAX_REQUESTS`
#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Http {
    /// Maximum number of concurrent in-flight HTTP requests.
    ///
    /// This is a flood protection limit. When the number of requests currently
    /// being processed reaches this value, new requests are rejected immediately
    /// with HTTP 503. Health and readiness endpoints (`/health`, `/ready`) are
    /// excluded from this limit.
    ///
    /// Unlike readiness-based backpressure, direct rejection responds in
    /// milliseconds and recovers the moment any in-flight request completes.
    ///
    /// # Default
    ///
    /// [`DEFAULT_MAX_HTTP_REQUESTS`]
    ///
    /// # Environment Variable
    ///
    /// `OS__HTTP__MAX_REQUESTS`
    pub max_requests: usize,
}

impl Default for Http {
    fn default() -> Self {
        Self {
            max_requests: DEFAULT_MAX_HTTP_REQUESTS,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8888".parse().unwrap(),

            storage: StorageConfig::FileSystem(FileSystemConfig {
                path: PathBuf::from("data"),
            }),

            storage_cogs: None,
            runtime: Runtime::default(),
            logging: LoggingConfig::default(),
            sentry: Sentry::default(),
            metrics: objectstore_metrics::MetricsConfig::default(),
            auth: AuthZ::default(),
            killswitches: Killswitches::default(),
            rate_limits: RateLimits::default(),
            usecases: UseCases::default(),
            service: Service::default(),
            http: Http::default(),
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
    /// Any value in the merged configuration may then be written as `${file:PATH}` or
    /// `${VAR_NAME}` to have it replaced by that file's contents or that environment
    /// variable — see [variable references](self#variable-references).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The YAML configuration file cannot be read or parsed
    /// - Environment variables contain invalid values
    /// - Required fields are missing or invalid
    /// - A `${file:PATH}` reference names a file that cannot be read, or a `${VAR_NAME}`
    ///   reference names an environment variable that is not set
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut figment = figment::Figment::from(Serialized::defaults(Config::default()));
        if let Some(path) = path {
            figment = figment.merge(Yaml::file(path));
        }

        // Merge first, then resolve variables against the merged value, so a reference is
        // resolved wherever it came from and whichever layer won.
        let merged: figment::value::Value = figment
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
            .extract()?;

        let base_path = path.and_then(Path::parent).unwrap_or(Path::new(""));

        // The file source must come first: `${file:x}` also matches the environment
        // source's `${` prefix, which would otherwise look up a variable named `file:x`.
        let mut source = (
            serde_vars::FileSource::new()
                .with_variable_prefix("${file:")
                .with_variable_suffix("}")
                .with_base_path(base_path),
            serde_vars::EnvSource::default()
                .with_variable_prefix("${")
                .with_variable_suffix("}"),
        );
        let config = serde_vars::deserialize(&merged, &mut source)?;

        Ok(config)
    }
}

#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "figment::Error is inherently large"
)]
mod tests {
    use std::io::Write;

    use objectstore_service::backend::{HighVolumeStorageConfig, MultipartUploadStorageConfig};
    use secrecy::ExposeSecret;

    use crate::killswitches::Killswitch;
    use crate::rate_limits::{BandwidthLimits, RateLimits, ThroughputLimits, ThroughputRule};

    use super::*;

    #[test]
    fn configurable_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__STORAGE__TYPE", "s3compatible");
            jail.set_env("OS__STORAGE__ENDPOINT", "http://localhost:8888");
            jail.set_env("OS__STORAGE__BUCKET", "whatever");
            jail.set_env("OS__METRICS__TAGS__FOO", "bar");
            jail.set_env("OS__METRICS__TAGS__BAZ", "qux");
            jail.set_env("OS__SENTRY__DSN", "abcde");
            jail.set_env("OS__SENTRY__SAMPLE_RATE", "0.5");
            jail.set_env("OS__SENTRY__ENVIRONMENT", "production");
            jail.set_env("OS__SENTRY__SERVER_NAME", "objectstore-deadbeef");
            jail.set_env("OS__SENTRY__TRACES_SAMPLE_RATE", "0.5");
            jail.set_env("OS__SENTRY__ATTACH_STACKTRACE", "true");

            let config = Config::load(None).unwrap();

            let StorageConfig::S3Compatible(c) = &dbg!(&config).storage else {
                panic!("expected s3 storage");
            };
            assert_eq!(c.endpoint, "http://localhost:8888");
            assert_eq!(c.bucket, "whatever");
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
            assert!(config.sentry.attach_stacktrace);

            Ok(())
        });
    }

    #[test]
    fn configurable_via_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
            storage:
                type: s3compatible
                endpoint: http://localhost:8888
                bucket: whatever
            sentry:
                dsn: abcde
                environment: production
                server_name: objectstore-deadbeef
                sample_rate: 0.5
                traces_sample_rate: 0.5
                attach_stacktrace: true
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();

            let StorageConfig::S3Compatible(c) = &dbg!(&config).storage else {
                panic!("expected s3 storage");
            };
            assert_eq!(c.endpoint, "http://localhost:8888");
            assert_eq!(c.bucket, "whatever");

            assert_eq!(config.sentry.dsn.unwrap().expose_secret().as_str(), "abcde");
            assert_eq!(config.sentry.environment.as_deref(), Some("production"));
            assert_eq!(
                config.sentry.server_name.as_deref(),
                Some("objectstore-deadbeef")
            );
            assert_eq!(config.sentry.sample_rate, 0.5);
            assert_eq!(config.sentry.traces_sample_rate, 0.5);
            assert!(config.sentry.attach_stacktrace);

            Ok(())
        });
    }

    #[test]
    fn resumable_token_encryption_loads_keys_from_files() {
        let mut key_file = tempfile::NamedTempFile::new().unwrap();
        key_file.write_all(&[7; 32]).unwrap();
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                format!(
                    "service:\n  resumable_token_encryption:\n    active_key_id: v1\n    key_files:\n      v1: \"{}\"\n",
                    key_file.path().display(),
                )
                .as_bytes(),
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();
            assert!(
                config
                    .service
                    .resumable_token_encryption()
                    .unwrap()
                    .is_some()
            );

            let debug = format!("{:?}", config.service);
            assert!(debug.contains("v1"));
            assert!(debug.contains(&key_file.path().display().to_string()));
            assert!(!debug.contains("07070707"));
            Ok(())
        });
    }

    #[test]
    fn resumable_token_encryption_rejects_invalid_configuration() {
        let mut valid = tempfile::NamedTempFile::new().unwrap();
        valid.write_all(&[7; 32]).unwrap();
        let mut short = tempfile::NamedTempFile::new().unwrap();
        short.write_all(&[7; 31]).unwrap();
        let missing = valid.path().with_extension("missing");
        for yaml in [
            "service:\n  resumable_token_encryption:\n    active_key_id: v1\n".to_owned(),
            format!(
                "service:\n  resumable_token_encryption:\n    active_key_id: missing\n    key_files:\n      v1: \"{}\"\n",
                valid.path().display(),
            ),
            format!(
                "service:\n  resumable_token_encryption:\n    active_key_id: bad_key\n    key_files:\n      'bad key': \"{}\"\n",
                valid.path().display(),
            ),
            format!(
                "service:\n  resumable_token_encryption:\n    active_key_id: v1\n    key_files:\n      v1: \"{}\"\n",
                short.path().display(),
            ),
            format!(
                "service:\n  resumable_token_encryption:\n    active_key_id: v1\n    key_files:\n      v1: \"{}\"\n",
                missing.display(),
            ),
        ] {
            let mut tempfile = tempfile::NamedTempFile::new().unwrap();
            tempfile.write_all(yaml.as_bytes()).unwrap();
            figment::Jail::expect_with(|_jail| {
                let config = Config::load(Some(tempfile.path())).unwrap();
                assert!(
                    config.service.resumable_token_encryption().is_err(),
                    "accepted {yaml}"
                );
                Ok(())
            });
        }
    }

    #[test]
    fn configured_with_env_and_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
            storage:
                type: s3compatible
                endpoint: http://localhost:8888
                bucket: whatever
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__STORAGE__ENDPOINT", "http://localhost:9001");

            let config = Config::load(Some(tempfile.path())).unwrap();

            let StorageConfig::S3Compatible(c) = &dbg!(&config).storage else {
                panic!("expected s3 storage");
            };
            // Env should overwrite the yaml config
            assert_eq!(c.endpoint, "http://localhost:9001");

            Ok(())
        });
    }

    #[test]
    fn tiered_storage_via_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
            storage:
                type: tiered
                high_volume:
                    type: bigtable
                    project_id: my-project
                    instance_name: objectstore
                    table_name: objectstore
                long_term:
                    type: gcs
                    bucket: my-objectstore-bucket
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();

            let StorageConfig::Tiered(c) = &dbg!(&config).storage else {
                panic!("expected tiered storage");
            };
            let HighVolumeStorageConfig::BigTable(hv) = &c.high_volume;
            assert_eq!(hv.project_id, "my-project");
            let MultipartUploadStorageConfig::Gcs(lt) = &c.long_term else {
                panic!("expected gcs long_term");
            };
            assert_eq!(lt.bucket, "my-objectstore-bucket");

            Ok(())
        });
    }

    #[test]
    fn tiered_storage_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__STORAGE__TYPE", "tiered");
            jail.set_env("OS__STORAGE__HIGH_VOLUME__TYPE", "bigtable");
            jail.set_env("OS__STORAGE__HIGH_VOLUME__PROJECT_ID", "my-project");
            jail.set_env("OS__STORAGE__HIGH_VOLUME__INSTANCE_NAME", "my-instance");
            jail.set_env("OS__STORAGE__HIGH_VOLUME__TABLE_NAME", "my-table");
            jail.set_env("OS__STORAGE__LONG_TERM__TYPE", "filesystem");
            jail.set_env("OS__STORAGE__LONG_TERM__PATH", "/data/lt");

            let config = Config::load(None).unwrap();

            let StorageConfig::Tiered(c) = &dbg!(&config).storage else {
                panic!("expected tiered storage");
            };
            let HighVolumeStorageConfig::BigTable(hv) = &c.high_volume;
            assert_eq!(hv.project_id, "my-project");
            assert_eq!(hv.instance_name, "my-instance");
            assert_eq!(hv.table_name, "my-table");
            let MultipartUploadStorageConfig::FileSystem(lt) = &c.long_term else {
                panic!("expected filesystem long_term");
            };
            assert_eq!(lt.path, Path::new("/data/lt"));

            Ok(())
        });
    }

    #[test]
    fn storage_cogs_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__STORAGE__TYPE", "bigtable");
            jail.set_env("OS__STORAGE__PROJECT_ID", "my-project");
            jail.set_env("OS__STORAGE__INSTANCE_NAME", "my-instance");
            jail.set_env("OS__STORAGE__TABLE_NAME", "my-table");
            jail.set_env(
                "OS__STORAGE__COGS__SHARED_RESOURCE_ID",
                "bigtable_objectstore",
            );
            jail.set_env("OS__STORAGE__COGS__SAMPLE_RATE", "0.5");
            jail.set_env("OS__STORAGE_COGS__TYPE", "kafka");
            jail.set_env("OS__STORAGE_COGS__TOPIC", "my-topic");
            jail.set_env("OS__STORAGE_COGS__BOOTSTRAP_SERVERS", "[kafka:9092]");

            let config = Config::load(None).unwrap();

            let StorageConfig::BigTable(storage) = &dbg!(&config).storage else {
                panic!("expected bigtable storage");
            };
            let stream = storage.cogs.as_ref().expect("change stream");
            assert_eq!(stream.shared_resource_id, "bigtable_objectstore");
            assert_eq!(stream.sample_rate, 0.5);

            let CostTrackerConfig::Kafka(kafka) =
                config.storage_cogs.as_ref().expect("kafka transport");
            assert_eq!(kafka.topic, "my-topic");
            assert_eq!(kafka.bootstrap_servers, ["kafka:9092"]);

            Ok(())
        });
    }

    #[test]
    fn a_backend_stream_defaults_to_reporting_everything_and_no_transport() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__STORAGE__TYPE", "gcs");
            jail.set_env("OS__STORAGE__BUCKET", "my-bucket");
            jail.set_env("OS__STORAGE__COGS__SHARED_RESOURCE_ID", "gcs_objectstore");

            let config = Config::load(None).unwrap();

            let StorageConfig::Gcs(storage) = &dbg!(&config).storage else {
                panic!("expected gcs storage");
            };
            let stream = storage.cogs.as_ref().expect("change stream");
            assert_eq!(stream.sample_rate, 1.0, "reports everything by default");
            assert!(
                config.storage_cogs.is_none(),
                "no transport is configured by default"
            );

            Ok(())
        });
    }

    #[test]
    fn serde_var_yaml_references() {
        let secrets = tempfile::tempdir().unwrap();
        let absolute_secret = secrets.path().join("kafka-password");
        std::fs::write(&absolute_secret, "hunter2").unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("relative-password"), "hunter3").unwrap();

        let config_path = dir.path().join("config.yml");
        std::fs::write(
            &config_path,
            format!(
                r#"
            storage_cogs:
                type: kafka
                override_params:
                    sasl.mechanism: SCRAM-SHA-256
                    not.a.reference: prod-${{NOT_A_VAR
                    from.env: ${{KAFKA_SASL_PASSWORD}}
                    from.relative.file: ${{file:relative-password}}
                    from.absolute.file: ${{file:{}}}
            "#,
                absolute_secret.display()
            ),
        )
        .unwrap();

        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFKA_SASL_PASSWORD", "hunter1");

            let config = Config::load(Some(&config_path)).unwrap();

            let CostTrackerConfig::Kafka(sink) =
                config.storage_cogs.as_ref().expect("kafka transport");
            assert_eq!(sink.override_params["from.env"], "hunter1");
            assert_eq!(sink.override_params["from.relative.file"], "hunter3");
            assert_eq!(
                sink.override_params["from.absolute.file"], "hunter2",
                "an absolute path ignores the config directory"
            );
            assert_eq!(sink.override_params["sasl.mechanism"], "SCRAM-SHA-256");
            assert_eq!(
                sink.override_params["not.a.reference"], "prod-${NOT_A_VAR",
                "a value that is not a reference is left alone"
            );

            Ok(())
        });
    }

    #[test]
    fn serde_vars_yaml_reference_failure() {
        let dir = tempfile::tempdir().unwrap();
        for (name, reference) in [
            ("missing-file.yml", "${file:nope}"),
            ("unset-var.yml", "${FAKE_VAR}"),
        ] {
            let config_path = dir.path().join(name);
            std::fs::write(
                &config_path,
                format!(
                    r#"
                storage_cogs:
                    type: kafka
                    override_params:
                        sasl.password: {reference}
                "#
                ),
            )
            .unwrap();

            figment::Jail::expect_with(|_jail| {
                assert!(Config::load(Some(&config_path)).is_err(), "{reference}");
                Ok(())
            });
        }
    }

    #[test]
    fn serde_vars_env_reference() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SENTRY_DSN", "https://public@example.invalid/1");
            jail.set_env("OS__SENTRY__DSN", "${SENTRY_DSN}");

            let config = Config::load(None).unwrap();

            assert_eq!(
                config.sentry.dsn.unwrap().expose_secret().as_str(),
                "https://public@example.invalid/1"
            );

            Ok(())
        });
    }

    #[test]
    fn metrics_addr_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__METRICS__ADDR", "127.0.0.1:8125");

            let config = Config::load(None).unwrap();
            assert_eq!(config.metrics.addr.as_deref(), Some("127.0.0.1:8125"));

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
    fn auth_enforce_defaults_to_true() {
        figment::Jail::expect_with(|_jail| {
            let config = Config::load(None).unwrap();
            assert!(config.auth.enforce);
            Ok(())
        });
    }

    #[test]
    fn auth_enforce_defaults_to_true_when_omitted_from_yaml() {
        let mut tempfile = tempfile::NamedTempFile::new().unwrap();
        tempfile
            .write_all(
                br#"
                auth:
                    keys: {}
            "#,
            )
            .unwrap();

        figment::Jail::expect_with(|_jail| {
            let config = Config::load(Some(tempfile.path())).unwrap();
            assert!(config.auth.enforce);
            Ok(())
        });
    }

    #[test]
    fn auth_enforce_can_be_disabled() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("OS__AUTH__ENFORCE", "false");
            let config = Config::load(None).unwrap();
            assert!(!config.auth.enforce);
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
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::from([("org".into(), "42".into())]),
                    service: None,
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::new(),
                    service: Some("test-*".into()),
                },
                Killswitch {
                    usecase: None,
                    scopes: BTreeMap::from([
                        ("org".into(), "42".into()),
                        ("project".into(), "4711".into()),
                    ]),
                    service: None,
                },
                Killswitch {
                    usecase: Some("attachments".into()),
                    scopes: BTreeMap::from([("org".into(), "42".into())]),
                    service: Some("test-*".into()),
                },
            ];

            let config = Config::load(Some(tempfile.path())).unwrap();
            assert_eq!(config.killswitches.as_slice(), &expected);

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
                    burst_ms: 2000
                    usecase_pct: 50
                    scope_pct: 25
                    report_only: true
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
                    burst_ms: 2000,
                    usecase_pct: Some(50),
                    scope_pct: Some(25),
                    report_only: true,
                },
            };

            let config = Config::load(Some(tempfile.path())).unwrap();
            assert_eq!(config.rate_limits, expected);

            Ok(())
        });
    }
}
