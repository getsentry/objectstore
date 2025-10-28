use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use argh::FromArgs;
use figment::providers::{Env, Format, Serialized, Yaml};
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;

const ENV_PREFIX: &str = "FSS_";

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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Storage {
    FileSystem {
        path: PathBuf,
    },
    S3Compatible {
        endpoint: String,
        bucket: String,
    },
    Gcs {
        endpoint: Option<String>,
        bucket: String,
    },
    BigTable {
        endpoint: Option<String>,
        project_id: String,
        instance_name: String,
        table_name: String,
        connections: Option<usize>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Runtime {
    /// Number of worker threads for the server runtime.
    pub worker_threads: usize,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sentry {
    pub dsn: Option<SecretBox<ConfigSecret>>,
    pub environment: Option<Cow<'static, str>>,
    pub server_name: Option<Cow<'static, str>>,
    pub sample_rate: f32,
    pub traces_sample_rate: f32,
    /// If true (default), then the SDK will inherit the sampling decision contained in the `sampled` flag of the incoming trace.
    /// If false, ignore the incoming sampling decision and always use `traces_sample_rate` instead.
    pub inherit_sampling_decision: bool,
    pub debug: bool,
}

impl Sentry {
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
        }
    }
}

/// Controls the log format.
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Logging {
    #[serde(with = "display_fromstr")]
    pub level: LevelFilter,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // server addr config
    pub http_addr: SocketAddr,

    // storage config
    pub high_volume_storage: Storage,
    pub long_term_storage: Storage,

    // server
    pub runtime: Runtime,

    // observability
    pub logging: Logging,
    pub sentry: Sentry,
    pub datadog_key: Option<SecretBox<ConfigSecret>>,
    pub metric_tags: BTreeMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8888".parse().unwrap(),

            high_volume_storage: Storage::FileSystem {
                path: PathBuf::from("data"),
            },
            long_term_storage: Storage::FileSystem {
                path: PathBuf::from("data"),
            },

            runtime: Runtime::default(),
            logging: Logging::default(),
            sentry: Sentry::default(),
            datadog_key: None,
            metric_tags: Default::default(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let args: Args = argh::from_env();
        Self::from_args(args)
    }

    pub fn from_args(args: Args) -> Result<Self> {
        let mut figment = figment::Figment::from(Serialized::defaults(Config::default()));
        if let Some(config_path) = &args.config {
            figment = figment.merge(Yaml::file(config_path));
        }
        let config = figment
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
            .extract()?;

        Ok(config)
    }
}

/// Command line arguments for the server.
#[derive(Debug, Default, FromArgs)]
pub struct Args {
    /// path to the yaml configuration file
    #[argh(option, short = 'c')]
    pub config: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn configurable_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("fss_long_term_storage__type", "s3compatible");
            jail.set_env("fss_long_term_storage__endpoint", "http://localhost:8888");
            jail.set_env("fss_long_term_storage__bucket", "whatever");
            jail.set_env("fss_metric_tags__foo", "bar");
            jail.set_env("fss_metric_tags__baz", "qux");
            jail.set_env("fss_sentry__dsn", "abcde");
            jail.set_env("fss_sentry__sample_rate", "0.5");
            jail.set_env("fss_sentry__environment", "production");
            jail.set_env("fss_sentry__server_name", "objectstore-deadbeef");
            jail.set_env("fss_sentry__traces_sample_rate", "0.5");
            jail.set_env("fss_sentry__traces_sample_rate", "0.5");

            let config = Config::from_args(Args::default()).unwrap();

            let Storage::S3Compatible { endpoint, bucket } = &dbg!(&config).long_term_storage
            else {
                panic!("expected s3 storage");
            };
            assert_eq!(endpoint, "http://localhost:8888");
            assert_eq!(bucket, "whatever");
            assert_eq!(
                config.metric_tags,
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

        let args = Args {
            config: Some(tempfile.path().into()),
        };
        let config = Config::from_args(args).unwrap();

        let Storage::S3Compatible { endpoint, bucket } = &dbg!(&config).long_term_storage else {
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
            jail.set_env("fss_long_term_storage__endpoint", "http://localhost:9001");

            let args = Args {
                config: Some(tempfile.path().into()),
            };
            let config = Config::from_args(args).unwrap();

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
}
