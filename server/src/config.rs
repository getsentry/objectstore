use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use argh::FromArgs;
use figment::providers::{Env, Format, Serialized, Yaml};
use serde::{Deserialize, Serialize};

const ENV_PREFIX: &str = "FSS_";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub listen_addr: String,
    pub path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:50051".to_owned(),
            path: PathBuf::from("data"),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let args: Args = argh::from_env();

        let mut figment = figment::Figment::from(Serialized::defaults(Config::default()));
        if let Some(config_path) = &args.config {
            figment = figment.merge(Yaml::file(config_path));
        }
        let config = figment.merge(Env::prefixed(ENV_PREFIX)).extract()?;

        Ok(config)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr.parse().expect("Invalid listen address")
    }
}

/// Command line arguments for the server.
#[derive(Debug, FromArgs)]
pub struct Args {
    /// path to the yaml configuration file
    #[argh(option, short = 'c')]
    pub config: Option<PathBuf>,
}
