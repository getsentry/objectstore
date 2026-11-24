//! This is a stresstest binary which can run different [`Workload`]s against
//! a backend storage service.
//!
//! The [`Workload`] supports two ways of configuring file sizes. A *LogNormal*
//! distribution, defined by the `p50` and `p99` of file sizes, models many
//! small files with a long tail of larger files. A histogram of [`SizeBuckets`]
//! replays a file size distribution that was measured in production.
//!
//! It also allows configuring a distribution of actions, such as write, read or delete.
//! The goal is that we have a lot of *write-heavy* workloads which only write
//! blobs, but never read those.
//!
//! *Read* or *delete* actions are using a *zipfian* distribution, meaning that
//! more recently written blobs are the ones that will be read/deleted.
//!
//! [`SizeBuckets`]: stresstest::workload::SizeBuckets
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::path::PathBuf;

use anyhow::Context;
use argh::FromArgs;
use objectstore_client::{SecretKey, TokenGenerator};
use stresstest::Workload;
use stresstest::http::HttpRemote;
use stresstest::stresstest::Stresstest;
use stresstest::workload::SizeBuckets;

use crate::config::Config;

mod config;

/// Stresstester for our foundational storage service
#[derive(Debug, FromArgs)]
pub struct Args {
    /// path to the yaml configuration file
    #[argh(option, short = 'c')]
    pub config: PathBuf,
}

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = argh::from_env();

    let config_file = std::fs::File::open(args.config).context("failed to open config file")?;
    let config: Config =
        serde_yaml::from_reader(config_file).context("failed to parse config YAML")?;

    let token = if let Some(auth) = &config.auth {
        let key = std::fs::read_to_string(&auth.key_path)
            .with_context(|| format!("failed to read private key from {:?}", auth.key_path))?;
        let generator = TokenGenerator::new(SecretKey {
            kid: auth.kid.clone(),
            secret_key: key,
        })?;
        Some(generator)
    } else {
        None
    };

    objectstore_metrics::init(&config.metrics)?;

    let remote = HttpRemote::new(&config.remote, token);
    let mut stresstest = Stresstest::new(remote)
        .duration(config.duration)
        .cleanup(config.cleanup);

    // Apply the configured TTL. `Default` keeps the stresstest's built-in TTL,
    // `Never` disables expiration (objects persist indefinitely), and `Fixed`
    // sets an explicit expiration.
    match config.ttl {
        config::TtlConfig::Default => {}
        config::TtlConfig::Never => stresstest = stresstest.ttl(None),
        config::TtlConfig::Fixed(ttl) => stresstest = stresstest.ttl(Some(ttl)),
    }

    for w in config.workloads {
        w.validate()?;

        let sizes = w.file_sizes;
        let mut workload = Workload::builder(&w.name)
            .concurrency(w.concurrency)
            .organizations(w.organizations)
            .mode(w.mode)
            .min_size(sizes.min.map_or(0, |b| b.0))
            .max_size(sizes.max.map(|b| b.0))
            .action_weights(w.actions.writes, w.actions.reads, w.actions.deletes);

        if let (Some(p50), Some(p99)) = (sizes.p50, sizes.p99) {
            workload = workload.size_distribution(p50.0, p99.0);
        }
        if let Some(buckets) = sizes.buckets {
            let buckets = SizeBuckets::new(buckets)
                .with_context(|| format!("workload '{}': invalid file size buckets", w.name))?;
            workload = workload.size_buckets(buckets);
        }

        stresstest = stresstest.workload(workload.build());
    }

    stresstest.run().await?;

    Ok(())
}
