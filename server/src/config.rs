use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use argh::FromArgs;
use figment::providers::{Env, Format, Serialized, Yaml};
use serde::{Deserialize, Serialize};

const ENV_PREFIX: &str = "FSS_";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Storage {
    FileSystem {
        path: PathBuf,
    },
    S3Compatible {
        endpoint: Option<String>,
        bucket: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // server addr config
    pub grpc_addr: SocketAddr,
    pub http_addr: SocketAddr,

    // storage config
    pub storage: Storage,

    // others
    pub sentry_dsn: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grpc_addr: "0.0.0.0:50051".parse().unwrap(),
            http_addr: "0.0.0.0:8888".parse().unwrap(),

            storage: Storage::FileSystem {
                path: PathBuf::from("data"),
            },

            sentry_dsn: None,
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
    use super::*;

    #[test]
    fn configurable_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("fss_storage__type", "s3compatible");
            jail.set_env("fss_storage__endpoint", "http://localhost:8888");
            jail.set_env("fss_storage__bucket", "whatever");

            let config = Config::from_args(Args::default()).unwrap();
            dbg!(config);

            Ok(())
        });
    }
}
