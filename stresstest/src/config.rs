use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use bytesize::ByteSize;
use serde::{Deserialize, Deserializer};
use stresstest::workload::{self, WorkloadMode};

/// Time-to-live configuration for objects created during the stresstest.
///
/// Controls the [`ExpirationPolicy`](objectstore_client::ExpirationPolicy) that
/// objects are written with, which in turn decides whether Bigtable garbage
/// collects them:
///
/// - Omitted -> [`TtlConfig::Default`]: keep the built-in default TTL (1 hour).
/// - `manual`/`none`/`never` -> [`TtlConfig::Never`]: objects never expire. This
///   uses the `Manual` expiration policy, so objects are written to the
///   non-GC (`fm`) column family and persist until explicitly deleted.
/// - a humantime duration such as `24h` or `30m` -> [`TtlConfig::Fixed`]: objects
///   expire after that period.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum TtlConfig {
    /// Leave the stresstest's built-in default TTL in place.
    #[default]
    Default,
    /// Never expire objects (persist indefinitely).
    Never,
    /// Expire objects after a fixed duration.
    Fixed(Duration),
}

impl<'de> Deserialize<'de> for TtlConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.trim().to_ascii_lowercase().as_str() {
            "manual" | "none" | "never" | "off" => Ok(TtlConfig::Never),
            other => humantime::parse_duration(other)
                .map(TtlConfig::Fixed)
                .map_err(serde::de::Error::custom),
        }
    }
}

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

    /// Time-to-live for objects created during the stresstest.
    ///
    /// See [`TtlConfig`] for accepted values. Defaults to the built-in TTL
    /// (1 hour) when omitted; set to `never` to persist objects indefinitely.
    #[serde(default)]
    pub ttl: TtlConfig,

    #[serde(default)]
    pub auth: Option<Auth>,

    #[serde(default)]
    pub metrics: objectstore_metrics::MetricsConfig,
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
