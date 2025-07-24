//! This is a stresstest binary which can run different [`Workload`]s against
//! a backend storage service.
//!
//! The [`Workload`] currently supports configuring a *LogNormal* distribution
//! of file sizes, defined by the `p50` and `p99` of file sizes.
//! This models our real-world distribution of many small files, with a long
//! tail of larger files as well.
//!
//! It also allows configuring a distribution of actions, such as write, read or delete.
//! The goal is that we have a lot of *write-heavy* workloads which only write
//! blobs, but never read those.
//!
//! *Read* or *delete* actions are using a *zipfian* distribution, meaning that
//! more recently written blobs are the ones that will be read/deleted.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::path::PathBuf;

use anyhow::Context;
use argh::FromArgs;

use crate::config::Config;
use crate::http::HttpRemote;
use crate::stresstest::perform_stresstest;
use crate::workload::Workload;

mod config;
mod http;
mod stresstest;
mod workload;

/// Stresstester for our foundational storage service
#[derive(Debug, FromArgs)]
pub struct Args {
    /// path to the yaml configuration file
    #[argh(option, short = 'c')]
    pub config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = argh::from_env();

    let config_file = std::fs::File::open(args.config).context("failed to open config file")?;
    let config: Config =
        serde_yaml::from_reader(config_file).context("failed to parse config YAML")?;

    let remote = HttpRemote {
        remote: config.remote,
        jwt_secret: config.jwt_secret,
        client: reqwest::Client::new(),
    };
    let workloads = config
        .workloads
        .into_iter()
        .map(|w| {
            Workload::builder(w.name)
                .concurrency(w.concurrency)
                .size_distribution(w.file_sizes.p50.0, w.file_sizes.p99.0)
                .action_weights(w.actions.writes, w.actions.reads, w.actions.deletes)
                .build()
        })
        .collect();

    perform_stresstest(remote, workloads, config.duration).await?;

    Ok(())
}
