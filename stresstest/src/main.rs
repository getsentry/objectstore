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

use std::time::Duration;

use crate::http::HttpRemote;
use crate::stresstest::perform_stresstest;
use crate::workload::Workload;

mod http;
mod stresstest;
mod workload;

#[tokio::main]
async fn main() {
    let remote = HttpRemote {
        master_url: "http://localhost:8888".into(),
        client: reqwest::Client::new(),
    };
    let workload = Workload::builder("testing")
        .concurrency(32)
        .size_distribution(16 * 1024, 1024 * 1024) // p50 = 16K, p99 = 1M
        .action_weights(98, 2, 0)
        .build();

    perform_stresstest(remote, vec![workload], Duration::from_secs(2))
        .await
        .unwrap();
}
