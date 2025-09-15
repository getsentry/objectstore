use std::time::Duration;

use bytesize::ByteSize;
use serde::Deserialize;
use stresstest::workload::WorkloadMode;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub remote: String,

    #[serde(with = "humantime_serde")]
    pub duration: Duration,

    pub workloads: Vec<Workload>,
}

#[derive(Debug, Deserialize)]
pub struct Workload {
    pub name: String,
    #[serde(default)]
    pub concurrency: usize,
    #[serde(default)]
    pub mode: WorkloadMode,
    pub file_sizes: FileSizes,
    #[serde(default)]
    pub actions: Actions,
}

#[derive(Debug, Deserialize)]
pub struct FileSizes {
    pub p50: ByteSize,
    pub p99: ByteSize,
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
