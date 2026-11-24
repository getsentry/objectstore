//! A module for defining a [`Workload`] that can be used to stress test remote storage services.

use std::pin::Pin;
use std::thread::available_parallelism;
use std::time::Instant;
use std::{fmt, io, task};

use anyhow::ensure;
use objectstore_client::Usecase;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use rand_distr::weighted::WeightedIndex;
use rand_distr::{Distribution, LogNormal, Zipf};
use serde::Deserialize;
use tokio::io::{AsyncRead, ReadBuf};

/// Defines how the workload schedules its operations.
#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadMode {
    /// The workload runs with fixed concurrency as fast as possible.
    ///
    /// Actions are used to determine the distribution of writes, reads and deletes.
    #[default]
    FixedConcurrency,

    /// The workload targets fixed ops per second, capped at 100 concurrent operations.
    ///
    /// Actions are used to determine the ops per second for each operation.
    FixedRate,

    /// The workload sends batch requests containing multiple write operations each.
    ///
    /// Actions' `writes` value determines how many put operations are included per batch request.
    /// Reads and deletes are not supported in this mode.
    Batch,
}

/// Object sizes as a histogram of size buckets.
///
/// Each bucket is the largest object size it holds, in bytes, with the number of objects at or
/// below that size. A bucket holds the sizes above the previous bucket's size, and the first
/// bucket starts at half its own size. Counts are cumulative and only their ratio to the last
/// count matters, so absolute counts, shares and percentages are equivalent.
///
/// Sampling picks a bucket with a probability proportional to the objects it adds over the
/// previous bucket, then draws a size log-uniformly within that bucket. Buckets that add no
/// objects are never picked.
///
/// ```
/// # use stresstest::workload::SizeBuckets;
/// // Half of the objects are in (512, 1024] bytes, the other half in (1024, 2048] bytes.
/// let buckets = SizeBuckets::new(vec![(1024, 50.0), (2048, 100.0)]).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct SizeBuckets(Vec<(u64, f64)>);

impl SizeBuckets {
    /// Creates size buckets from `(size, cumulative count)` pairs.
    ///
    /// # Errors
    ///
    /// Fails unless sizes are positive and strictly increasing, counts do not decrease, and the
    /// last count is greater than zero.
    pub fn new(buckets: Vec<(u64, f64)>) -> anyhow::Result<Self> {
        let (mut last_size, mut last_count) = (0, 0.0);
        for &(size, count) in &buckets {
            ensure!(
                size > last_size,
                "bucket sizes must be positive and strictly increasing, \
                 but {size} bytes follows {last_size} bytes"
            );
            ensure!(
                count >= last_count && count.is_finite(),
                "bucket counts are cumulative and must not decrease, \
                 but {count} at {size} bytes follows {last_count}"
            );
            (last_size, last_count) = (size, count);
        }
        ensure!(last_count > 0.0, "the last bucket count must be positive");

        Ok(Self(buckets))
    }
}

impl Distribution<u64> for SizeBuckets {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let buckets = &self.0;
        let total = buckets[buckets.len() - 1].1;

        // The rank of the sampled object in `(0, total]`, which falls into the first bucket with
        // at least that many objects.
        let rank = (1.0 - rng.random::<f64>()) * total;
        let index = buckets.partition_point(|&(_, count)| count < rank);

        let (size, count) = buckets[index];
        let (lower_size, lower_count) = match index {
            0 => (size as f64 / 2.0, 0.0),
            _ => (buckets[index - 1].0 as f64, buckets[index - 1].1),
        };

        // Spread the bucket's ranks log-uniformly over `(lower_size, size]`.
        let position = (rank - lower_count) / (count - lower_count);
        let sampled = lower_size * (size as f64 / lower_size).powf(position);
        (sampled.ceil() as u64).min(size)
    }
}

/// A builder for creating a [`Workload`].
#[derive(Debug)]
pub struct WorkloadBuilder {
    name: String,
    concurrency: usize,
    organizations: u64,
    mode: WorkloadMode,
    seed: u64,

    p50_size: u64,
    p99_size: u64,
    min_size: u64,
    max_size: Option<u64>,
    size_buckets: Option<SizeBuckets>,

    write_weight: usize,
    read_weight: usize,
    delete_weight: usize,
}

impl WorkloadBuilder {
    /// The maximum number of concurrent operations that can be performed within this workload.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// The number of organizations to distribute the workload across.
    pub fn organizations(mut self, organizations: u64) -> Self {
        self.organizations = organizations;
        self
    }

    /// How the workload schedules its operations.
    pub fn mode(mut self, mode: WorkloadMode) -> Self {
        self.mode = mode;
        self
    }

    /// Distribution of file sizes for the `write` action.
    pub fn size_distribution(mut self, p50: u64, p99: u64) -> Self {
        self.p50_size = p50;
        self.p99_size = p99;
        self
    }

    /// Sets the minimum payload size. This must be less than the median and at most the maximum.
    pub fn min_size(mut self, min_size: u64) -> Self {
        self.min_size = min_size;
        self
    }

    /// Sets an optional maximum payload size. Sampled sizes will be clamped to this value.
    pub fn max_size(mut self, max_size: Option<u64>) -> Self {
        self.max_size = max_size;
        self
    }

    /// Draws file sizes for the `write` action from a histogram of size buckets.
    ///
    /// This replaces the distribution set by [`size_distribution`](Self::size_distribution),
    /// including the minimum and maximum payload size.
    pub fn size_buckets(mut self, buckets: SizeBuckets) -> Self {
        self.size_buckets = Some(buckets);
        self
    }

    /// The ratio between writes, reads and deletes.
    pub fn action_weights(mut self, writes: usize, reads: usize, deletes: usize) -> Self {
        self.write_weight = writes;
        self.read_weight = reads;
        self.delete_weight = deletes;
        self
    }

    /// Creates the workload instance.
    pub fn build(self) -> Workload {
        let rng = SmallRng::seed_from_u64(self.seed);

        let sizes = match self.size_buckets {
            Some(buckets) => SizeDistribution::Buckets(buckets),
            None => {
                // Inspired by <https://stats.stackexchange.com/a/649432>
                // Fit the distribution above the floor so shifting the samples preserves p50 and
                // p99.
                let p50 = (self.p50_size - self.min_size) as f64;
                let p99 = (self.p99_size - self.min_size) as f64;
                let mu = p50.ln();
                let sigma = (p99.ln() - mu) / 2.3263;

                SizeDistribution::LogNormal {
                    distribution: LogNormal::new(mu, sigma).unwrap(),
                    min: self.min_size,
                    max: self.max_size,
                }
            }
        };
        let action_distribution =
            WeightedIndex::new([self.write_weight, self.read_weight, self.delete_weight]).unwrap();

        Workload {
            name: self.name,
            concurrency: self.concurrency,
            organizations: self.organizations,
            mode: self.mode,

            rng,
            sizes,
            action_distribution,

            start_time: None,
            totals: Totals::default(),

            existing_files: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct Totals {
    writes: usize,
    reads: usize,
    deletes: usize,
}

/// How a [`Workload`] generates payload sizes for the `write` action.
#[derive(Debug)]
enum SizeDistribution {
    /// A lognormal distribution, with `min` added to each sampled size and capped at `max`.
    LogNormal {
        distribution: LogNormal<f64>,
        min: u64,
        max: Option<u64>,
    },
    /// A histogram of size buckets.
    Buckets(SizeBuckets),
}

/// Specification of a stresstest that can be run against a remote storage service.
#[derive(Debug)]
pub struct Workload {
    /// Name of the workload for identification in logs and metrics.
    pub(crate) name: String,
    /// The maximum number of concurrent operations that can be performed within this workload.
    pub(crate) concurrency: usize,
    /// The number of organizations to distribute the workload across.
    pub(crate) organizations: u64,
    /// How the workload schedules its operations.
    pub(crate) mode: WorkloadMode,

    /// The RNG driving all our distributions.
    rng: SmallRng,
    /// The distribution that generates payload sizes for the `write` action.
    sizes: SizeDistribution,
    /// A distribution that generates actions, such as write/read/delete.
    action_distribution: WeightedIndex<usize>,

    start_time: Option<Instant>,
    totals: Totals,

    /// All the written files that we can then read or delete.
    existing_files: Vec<(InternalId, ExternalId)>,
}

impl Workload {
    /// Constructs a new workload builder with the given name.
    pub fn builder(name: impl Into<String>) -> WorkloadBuilder {
        WorkloadBuilder {
            name: name.into(),
            concurrency: available_parallelism().unwrap().get(),
            organizations: 1,
            mode: WorkloadMode::default(),
            seed: rand::random(),

            p50_size: 16 * 1024,
            p99_size: 1024 * 1024,
            min_size: 0,
            max_size: None,
            size_buckets: None,

            write_weight: 33,
            read_weight: 33,
            delete_weight: 33,
        }
    }

    fn get_payload(&self, seed: u64) -> Payload {
        let mut rng = SmallRng::seed_from_u64(seed);
        let len = match &self.sizes {
            SizeDistribution::Buckets(buckets) => buckets.sample(&mut rng),
            SizeDistribution::LogNormal {
                distribution,
                min,
                max,
            } => {
                let len = (distribution.sample(&mut rng) as u64).saturating_add(*min);
                max.map_or(len, |max| len.min(max))
            }
        };

        Payload { len, rng }
    }

    fn sample_readback(&mut self) -> Option<(InternalId, ExternalId)> {
        if self.existing_files.is_empty() {
            return None;
        }
        let len = self.existing_files.len();
        let zipf = Zipf::new(len as f64, 2.0).unwrap();
        let idx = len - self.rng.sample(zipf) as usize;

        Some(self.existing_files.remove(idx))
    }

    fn next_action_fixed_rate(&mut self) -> Option<Action> {
        let write_rate = self.action_distribution.weight(0).unwrap();
        let read_rate = self.action_distribution.weight(1).unwrap();
        let delete_rate = self.action_distribution.weight(2).unwrap();

        let elapsed = self.start_time.get_or_insert_with(Instant::now).elapsed();

        // Prioritize writes to create readback.
        if (self.totals.writes as f64) < (elapsed.as_secs_f64() * write_rate as f64) {
            self.totals.writes += 1;
            let seed = self.rng.next_u64();
            return Some(Action::Write(InternalId(seed), self.get_payload(seed)));
        }

        if (self.totals.reads as f64) < elapsed.as_secs_f64() * read_rate as f64
            && let Some((internal, external)) = self.sample_readback()
        {
            self.totals.reads += 1;
            return Some(Action::Read(
                internal,
                external,
                self.get_payload(internal.0),
            ));
        }

        if (self.totals.deletes as f64) < elapsed.as_secs_f64() * delete_rate as f64
            && let Some((_internal, external)) = self.sample_readback()
        {
            self.totals.deletes += 1;
            return Some(Action::Delete(external));
        }

        None
    }

    fn next_action_by_weight(&mut self) -> Action {
        loop {
            match self.action_distribution.sample(&mut self.rng) {
                0 => {
                    let seed = self.rng.next_u64();
                    return Action::Write(InternalId(seed), self.get_payload(seed));
                }
                1 => {
                    if let Some((internal, external)) = self.sample_readback() {
                        return Action::Read(internal, external, self.get_payload(internal.0));
                    };
                }
                _ => {
                    if let Some((_internal, external)) = self.sample_readback() {
                        return Action::Delete(external);
                    };
                }
            }
        }
    }

    pub(crate) fn next_action(&mut self) -> Option<Action> {
        match self.mode {
            WorkloadMode::FixedConcurrency | WorkloadMode::Batch => {
                Some(self.next_action_by_weight())
            }
            WorkloadMode::FixedRate => self.next_action_fixed_rate(),
        }
    }

    /// Generates `count` write payloads for use in batch mode.
    pub(crate) fn next_write_payloads(&mut self, count: usize) -> Vec<(InternalId, Payload)> {
        (0..count)
            .map(|_| {
                let seed = self.rng.next_u64();
                (InternalId(seed), self.get_payload(seed))
            })
            .collect()
    }

    /// Returns the number of writes per batch, derived from the `actions.writes` weight.
    pub(crate) fn batch_write_count(&self) -> usize {
        self.action_distribution.weight(0).unwrap()
    }

    pub(crate) fn next_organization_id(&mut self) -> u64 {
        self.rng.next_u64() % self.organizations
    }

    /// Adds a file to the internal store, so it can be yielded for reads or deletes.
    ///
    /// This function has to be called for files when a write or read has completed.
    /// (Files currently being read will not be concurrently deleted)
    pub(crate) fn push_file(&mut self, internal: InternalId, external: ExternalId) {
        self.existing_files.push((internal, external))
    }

    pub(crate) fn external_files(&self) -> impl Iterator<Item = &ExternalId> {
        self.existing_files
            .iter()
            .map(|(_internal, external)| external)
    }
}

/// Unique identifier for an object in the stresstest.
///
/// Objectstore assigns this an [`ExternalId`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InternalId(u64);

impl fmt::Display for InternalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for an object in the remote storage.
///
/// The identifier consists of the usecase, organization ID and the object key. These key maps to
/// [`InternalId`].
pub type ExternalId = (Usecase, u64, String);

/// An action that can be performed by the workload.
#[derive(Debug)]
pub enum Action {
    /// Write an object with the given internal ID and payload, returning an [`ExternalId`].
    Write(InternalId, Payload),
    /// Read an object with the given ID and expect the payload to match.
    Read(InternalId, ExternalId, Payload),
    /// Permanently delete an object with the given ID.
    Delete(ExternalId),
}

/// Randomized contents of an object.
///
/// Clone this instance to reuse it with deterministic contents across multiple reads.
/// Alternatively, the payload can be constructed from an [`InternalId`].
#[derive(Debug, Clone)]
pub struct Payload {
    /// The length of the payload in bytes.
    pub len: u64,
    /// The RNG used to fill the payload with random bytes.
    pub rng: SmallRng,
}

impl AsyncRead for Payload {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        let len_to_fill = (buf.remaining() as u64).min(self.len) as usize;

        let fill_buf = buf.initialize_unfilled_to(len_to_fill);
        self.rng.fill_bytes(fill_buf);

        self.len -= len_to_fill as u64;
        buf.advance(len_to_fill);

        task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_respect_size_bounds() {
        const MIN: u64 = 1024 * 1024 + 1;
        const MAX: u64 = 128 * 1024 * 1024;
        let workload = Workload::builder("test")
            .size_distribution(2 * 1024 * 1024, 50 * 1024 * 1024)
            .min_size(MIN)
            .max_size(Some(MAX))
            .build();
        for seed in 0..1000 {
            assert!((MIN..=MAX).contains(&workload.get_payload(seed).len));
        }
    }

    #[test]
    fn size_buckets_match_configured_shares() {
        // 10% up to 1 KiB, another 60% up to 2 KiB, nothing up to 4 KiB, the last 30% up to 8 KiB.
        let counts = vec![(1024, 10.0), (2048, 70.0), (4096, 70.0), (8192, 100.0)];
        let workload = Workload::builder("test")
            .size_buckets(SizeBuckets::new(counts.clone()).unwrap())
            .build();

        const SAMPLES: u64 = 100_000;
        let sizes: Vec<_> = (0..SAMPLES)
            .map(|seed| workload.get_payload(seed).len)
            .collect();

        assert!(sizes.iter().all(|size| (513..=8192).contains(size)));
        for (bound, count) in counts {
            let at_or_below = sizes.iter().filter(|&&size| size <= bound).count();
            let share = at_or_below as f64 / SAMPLES as f64;
            assert!((share - count / 100.0).abs() < 0.01, "{bound}: {share}");
        }
    }

    #[test]
    fn size_buckets_replace_percentile_settings() {
        // These settings cannot be fitted to a lognormal distribution, and buckets ignore them.
        let workload = Workload::builder("test")
            .size_distribution(100, 10)
            .min_size(1024)
            .size_buckets(SizeBuckets::new(vec![(64, 1.0)]).unwrap())
            .build();

        assert!((33..=64).contains(&workload.get_payload(0).len));
    }

    #[test]
    fn size_buckets_reject_invalid_input() {
        assert!(SizeBuckets::new(vec![]).is_err());
        assert!(SizeBuckets::new(vec![(1024, 0.0)]).is_err());
        assert!(SizeBuckets::new(vec![(2048, 1.0), (1024, 2.0)]).is_err());
        assert!(SizeBuckets::new(vec![(1024, 2.0), (2048, 1.0)]).is_err());
    }

    #[test]
    fn lognormal_percentiles_work() {
        let workload = Workload::builder("test")
            .size_distribution(100, 1000)
            .build();

        let mut sizes: Vec<_> = (0..100)
            .map(|seed| workload.get_payload(seed).len)
            .collect();
        sizes.sort_unstable();

        dbg!(sizes);
    }
}
