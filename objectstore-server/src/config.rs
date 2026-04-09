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

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use figment::providers::{Env, Format, Serialized, Yaml};
use objectstore_service::backend::local_fs::FileSystemConfig;
use objectstore_types::auth::Permission;
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

pub use objectstore_log::{LevelFilter, LogFormat, LoggingConfig};
pub use objectstore_service::backend::StorageConfig;

use crate::killswitches::Killswitches;
use crate::rate_limits::RateLimits;
use crate::usecases::UseCases;

/// Environment variable prefix for all configuration options.
const ENV_PREFIX: &str = "OS__";

/// Newtype around `String` that may protect against accidental
/// logging of secrets in our configuration struct. Use with
/// [`secrecy::SecretBox`].
#[derive(Clone, Default, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct AuthZ {
    /// Whether to enforce content-based authorization or not.
    ///
    /// If this is set to `false`, checks are still performed but failures will not result
    /// in `403 Unauthorized` responses.
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
#[expect(
    clippy::result_large_err,
    reason = "figment::Error is inherently large"
)]
mod tests {
    use std::io::Write;

    use objectstore_service::backend::HighVolumeStorageConfig;
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

            Ok(())
        });
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
            let StorageConfig::Gcs(lt) = c.long_term.as_ref() else {
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
            let StorageConfig::FileSystem(lt) = c.long_term.as_ref() else {
                panic!("expected filesystem long_term");
            };
            assert_eq!(lt.path, Path::new("/data/lt"));

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
