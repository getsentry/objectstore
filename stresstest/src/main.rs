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
use stresstest::Workload;
use stresstest::http::HttpRemote;

use crate::config::Config;

mod config;

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

    let remote = HttpRemote::new(&config.remote);

    let workloads = config
        .workloads
        .into_iter()
        .map(|w| {
            Workload::builder(w.name)
                .concurrency(w.concurrency)
                .organizations(w.organizations)
                .mode(w.mode)
                .size_distribution(w.file_sizes.p50.0, w.file_sizes.p99.0)
                .action_weights(w.actions.writes, w.actions.reads, w.actions.deletes)
                .build()
        })
        .collect();

    stresstest::run(remote, workloads, config.duration).await?;

    Ok(())
}
