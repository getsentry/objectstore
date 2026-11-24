use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use bytesize::ByteSize;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use stresstest::workload::WorkloadMode;

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
}

impl Workload {
    pub fn validate(&self) -> anyhow::Result<()> {
        let sizes = &self.file_sizes;
        let valid = match sizes.buckets {
            Some(_) => [sizes.min, sizes.p50, sizes.p99, sizes.max]
                .iter()
                .all(Option::is_none),
            None => sizes.p50.is_some() && sizes.p99.is_some(),
        };
        if !valid {
            bail!(
                "workload '{}': file_sizes needs either `buckets` alone, or `p50` and `p99` \
                 with optional `min` and `max`",
                self.name
            );
        }
        if let (Some(min), Some(p50), Some(p99)) = (sizes.min, sizes.p50, sizes.p99) {
            if min >= p50 || p99 < p50 {
                bail!(
                    "workload '{}': file sizes must satisfy min < p50 <= p99",
                    self.name
                );
            }
            if sizes.max.is_some_and(|max| max < min) {
                bail!(
                    "workload '{}': maximum file size is below the minimum",
                    self.name
                );
            }
        }
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

        Ok(())
    }
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism().unwrap().get()
}

fn default_organizations() -> u64 {
    1
}

/// File size distribution of a workload.
///
/// Sizes are either drawn from a lognormal distribution fitted to `p50` and `p99`, optionally
/// bounded by `min` and `max`, or from a histogram of size `buckets`. See
/// [`SizeBuckets`](stresstest::workload::SizeBuckets) for the meaning of buckets.
#[derive(Debug, Deserialize)]
pub struct FileSizes {
    pub min: Option<ByteSize>,
    pub p50: Option<ByteSize>,
    pub p99: Option<ByteSize>,
    pub max: Option<ByteSize>,
    /// Bucket sizes in bytes with the cumulative count of objects at or below each size, in the
    /// order they are written in the config.
    #[serde(default, deserialize_with = "deserialize_buckets")]
    pub buckets: Option<Vec<(u64, f64)>>,
}

/// Deserializes a map of sizes to counts into pairs, keeping the order of the map.
fn deserialize_buckets<'de, D>(deserializer: D) -> Result<Option<Vec<(u64, f64)>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BucketsVisitor;

    impl<'de> Visitor<'de> for BucketsVisitor {
        type Value = Vec<(u64, f64)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map of sizes to cumulative object counts")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut buckets = Vec::new();
            while let Some((size, count)) = map.next_entry::<ByteSize, f64>()? {
                buckets.push((size.0, count));
            }
            Ok(buckets)
        }
    }

    deserializer.deserialize_map(BucketsVisitor).map(Some)
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

#[cfg(test)]
mod tests {
    use stresstest::workload::SizeBuckets;

    use super::*;

    #[test]
    fn example_configs_are_valid() {
        for name in ["example.yaml", "production.example.yaml"] {
            let path = format!("{}/config/{name}", env!("CARGO_MANIFEST_DIR"));
            let config: Config =
                serde_yaml::from_reader(std::fs::File::open(path).unwrap()).unwrap();
            for workload in config.workloads {
                workload.validate().unwrap();
                if let Some(buckets) = workload.file_sizes.buckets {
                    SizeBuckets::new(buckets).unwrap();
                }
            }
        }
    }
}
