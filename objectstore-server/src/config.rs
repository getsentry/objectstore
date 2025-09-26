use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use argh::FromArgs;
use figment::providers::{Env, Format, Serialized, Yaml};
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

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
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sentry {
    pub dsn: SecretBox<ConfigSecret>,
    pub sample_rate: Option<f32>,
    pub traces_sample_rate: Option<f32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // server addr config
    pub http_addr: SocketAddr,

    // storage config
    pub high_volume_storage: Storage,
    pub long_term_storage: Storage,

    // others
    pub sentry: Option<Sentry>,
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

            sentry: None,
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

            let Sentry {
                dsn,
                sample_rate,
                traces_sample_rate,
            } = &dbg!(&config).sentry.as_ref().unwrap();
            assert_eq!(dsn.expose_secret().as_str(), "abcde");
            assert_eq!(sample_rate, &Some(0.5));
            assert_eq!(traces_sample_rate, &Some(0.5));

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

        let Sentry {
            dsn,
            sample_rate,
            traces_sample_rate,
        } = &dbg!(&config).sentry.as_ref().unwrap();
        assert_eq!(dsn.expose_secret().as_str(), "abcde");
        assert_eq!(sample_rate, &Some(0.5));
        assert_eq!(traces_sample_rate, &Some(0.5));
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
