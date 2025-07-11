use std::time::Duration;

use bytesize::ByteSize;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub remote: String,
    pub prefix: String,

    #[serde(with = "humantime_serde")]
    pub duration: Duration,

    pub workloads: Vec<Workload>,
}

#[derive(Debug, Deserialize)]
pub struct Workload {
    pub name: String,
    pub concurrency: usize,
    pub file_sizes: FileSizes,
    pub actions: Actions,
}

#[derive(Debug, Deserialize)]
pub struct FileSizes {
    pub p50: ByteSize,
    pub p99: ByteSize,
}

#[derive(Debug, Deserialize)]
pub struct Actions {
    pub writes: u8,
    pub reads: u8,
    pub deletes: u8,
}
