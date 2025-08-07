//! A module for defining a [`Workload`] that can be used to stress test remote storage services.

use std::pin::Pin;
use std::thread::available_parallelism;
use std::time::Instant;
use std::{fmt, io, task};

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::weighted::WeightedIndex;
use rand_distr::{Distribution, LogNormal, Zipf};
use serde::Deserialize;
use tokio::io::{AsyncRead, ReadBuf};

/// Defines how the workload schedules its operations.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadMode {
    /// The workload runs with fixed concurrency as fast as possible.
    ///
    /// Actions are used to determine the distribution of writes, reads and deletes.
    #[default]
    Weighted,

    /// The workload runs with fixed ops per second.
    ///
    /// Actions are used to determine the ops per second for each operation.
    Throughput,
}

/// A builder for creating a [`Workload`].
#[derive(Debug)]
pub struct WorkloadBuilder {
    name: String,
    concurrency: usize,
    mode: WorkloadMode,
    seed: u64,

    p50_size: u64,
    p99_size: u64,

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

    /// The mode of the workload, either `weighted` or `throughput`.
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

        // Inspired by <https://stats.stackexchange.com/a/649432>
        let p50 = self.p50_size as f64;
        let p99 = self.p99_size as f64;
        let mu = p50.ln();
        let sigma = (p99.ln() - mu) / 2.3263;

        let size_distribution = LogNormal::new(mu, sigma).unwrap();
        let action_distribution =
            WeightedIndex::new([self.write_weight, self.read_weight, self.delete_weight]).unwrap();

        Workload {
            name: self.name,
            concurrency: self.concurrency,
            mode: self.mode,

            rng,
            size_distribution,
            action_distribution,

            start_time: None,
            totals: Totals::default(),

            existing_files: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
struct Totals {
    writes: usize,
    reads: usize,
    deletes: usize,
}

/// Specification of a stresstest that can be run against a remote storage service.
#[derive(Debug)]
pub struct Workload {
    /// Name of the workload for identification in logs and metrics.
    pub(crate) name: String,
    /// The maximum number of concurrent operations that can be performed within this workload.
    pub(crate) concurrency: usize,
    /// The target throughput for the workload, in bytes per second. Overrides concurrency.
    pub(crate) mode: WorkloadMode,

    /// The RNG driving all our distributions.
    rng: SmallRng,
    /// A distribution that generates payload sizes for the `write` action.
    size_distribution: LogNormal<f64>,
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
            mode: WorkloadMode::default(),
            seed: rand::random(),

            p50_size: 16 * 1024,
            p99_size: 1024 * 1024,

            write_weight: 33,
            read_weight: 33,
            delete_weight: 33,
        }
    }

    fn get_payload(&self, seed: u64) -> Payload {
        let mut rng = SmallRng::seed_from_u64(seed);
        let len = self.size_distribution.sample(&mut rng) as u64;

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

    fn next_action_throughput(&mut self) -> Option<Action> {
        let write_throughput = self.action_distribution.weight(0).unwrap();
        let read_throughput = self.action_distribution.weight(1).unwrap();
        let delete_throughput = self.action_distribution.weight(2).unwrap();

        let elapsed = self.start_time.get_or_insert_with(Instant::now).elapsed();

        // Prioritize writes to create readback.
        if (self.totals.writes as f64) < (elapsed.as_secs_f64() * (write_throughput as f64)) {
            self.totals.writes += 1;
            let seed = self.rng.next_u64();
            return Some(Action::Write(InternalId(seed), self.get_payload(seed)));
        }

        if (self.totals.reads as f64) < elapsed.as_secs_f64() * (read_throughput as f64) {
            if let Some((internal, external)) = self.sample_readback() {
                self.totals.reads += 1;
                return Some(Action::Read(
                    internal,
                    external,
                    self.get_payload(internal.0),
                ));
            };
        }

        if (self.totals.deletes as f64) < elapsed.as_secs_f64() * (delete_throughput as f64) {
            if let Some((_internal, external)) = self.sample_readback() {
                self.totals.deletes += 1;
                return Some(Action::Delete(external));
            };
        }

        None
    }

    fn next_action_weighted(&mut self) -> Action {
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
            WorkloadMode::Weighted => Some(self.next_action_weighted()),
            WorkloadMode::Throughput => self.next_action_throughput(),
        }
    }

    /// Adds a file to the internal store, so it can be yielded for reads or deletes.
    ///
    /// This function has to be called for files when a write or read has completed.
    /// (Files currently being read will not be concurrently deleted)
    pub(crate) fn push_file(&mut self, internal: InternalId, external: ExternalId) {
        self.existing_files.push((internal, external))
    }

    pub(crate) fn external_files(&mut self) -> impl Iterator<Item = ExternalId> + use<> {
        std::mem::take(&mut self.existing_files)
            .into_iter()
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
/// These identifiers map to [`InternalId`]s.
pub type ExternalId = String;

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
