use std::time::Duration;

use bytesize::ByteSize;
use serde::Deserialize;
use stresstest::workload::WorkloadMode;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub remote: String,

    #[serde(default)]
    pub mode: Mode,

    #[serde(default, with = "humantime_serde")]
    pub duration: Option<Duration>,

    #[serde(default)]
    pub workloads: Vec<Workload>,

    #[serde(default)]
    pub cleanup: bool,

    // exists-bench fields
    pub count: Option<usize>,
    #[serde(default)]
    pub batch_size: usize,
    pub seed: Option<usize>,
    #[serde(default)]
    pub long_term_pct: u8,
    pub usecase: Option<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Stresstest,
    Exists,
}

#[derive(Debug, Deserialize)]
pub struct Workload {
    pub name: String,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_organizations")]
    pub organizations: u64,
    #[serde(default)]
    pub mode: WorkloadMode,
    pub file_sizes: FileSizes,
    #[serde(default)]
    pub actions: Actions,
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism().unwrap().get()
}

fn default_organizations() -> u64 {
    1
}

#[derive(Debug, Deserialize)]
pub struct FileSizes {
    pub p50: ByteSize,
    pub p99: ByteSize,
    pub max: Option<ByteSize>,
}

#[derive(Debug, Deserialize)]
pub struct Actions {
    pub writes: usize,
    pub reads: usize,
    pub deletes: usize,
}

impl Default for Actions {
    fn default() -> Self {
        Self {
            writes: 97,
            reads: 2,
            deletes: 1,
        }
    }
}
