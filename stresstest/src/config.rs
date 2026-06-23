use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use bytesize::ByteSize;
use serde::Deserialize;
use stresstest::workload::{self, WorkloadMode};

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub kid: String,
    pub key_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub remote: String,

    #[serde(with = "humantime_serde")]
    pub duration: Duration,

    pub workloads: Vec<Workload>,

    #[serde(default)]
    pub cleanup: bool,

    #[serde(default)]
    pub auth: Option<Auth>,
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
    #[serde(default)]
    pub multipart: Option<MultipartConfig>,
}

#[derive(Debug, Deserialize)]
pub struct MultipartConfig {
    pub concurrency: usize,
    pub part_size: ByteSize,
}

impl Workload {
    pub fn validate(&self) -> anyhow::Result<()> {
        if matches!(self.mode, WorkloadMode::Batch) {
            if self.actions.reads > 0 || self.actions.deletes > 0 {
                bail!(
                    "workload '{}': batch mode only supports writes, but reads={} and deletes={} were configured",
                    self.name,
                    self.actions.reads,
                    self.actions.deletes
                );
            }
            if self.actions.writes == 0 {
                bail!(
                    "workload '{}': batch mode requires actions.writes > 0",
                    self.name
                );
            }
        }

        if let Some(mp) = &self.multipart {
            if matches!(self.mode, WorkloadMode::Batch) {
                bail!(
                    "workload '{}': multipart uploads are not supported in batch mode",
                    self.name
                );
            }
            if mp.part_size.0 < 5 * 1024 * 1024 {
                bail!(
                    "workload '{}': multipart part_size must be at least 5 MiB, got {}",
                    self.name,
                    mp.part_size
                );
            }
            if mp.concurrency == 0 {
                bail!(
                    "workload '{}': multipart concurrency must be at least 1",
                    self.name
                );
            }
        }

        Ok(())
    }

    pub fn multipart_config(&self) -> Option<workload::MultipartConfig> {
        self.multipart.as_ref().map(|mp| workload::MultipartConfig {
            concurrency: mp.concurrency,
            part_size: mp.part_size.0,
        })
    }
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
