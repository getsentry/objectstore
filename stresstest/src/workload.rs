use std::io;
use std::thread::available_parallelism;

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::weighted::WeightedIndex;
use rand_distr::{Distribution, LogNormal, Zipf};

pub struct WorkloadBuilder {
    name: &'static str,
    concurrency: usize,
    seed: u64,

    p50_size: u64,
    p99_size: u64,

    write_weight: u8,
    read_weight: u8,
    delete_weight: u8,
}

impl WorkloadBuilder {
    // pub fn seed(mut self, seed: u64) -> Self {
    //     self.seed = seed;
    //     self
    // }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn size_distribution(mut self, p50: u64, p99: u64) -> Self {
        self.p50_size = p50;
        self.p99_size = p99;
        self
    }

    pub fn action_weights(mut self, writes: u8, reads: u8, deletes: u8) -> Self {
        self.write_weight = writes;
        self.read_weight = reads;
        self.delete_weight = deletes;
        self
    }

    pub fn build(self) -> Workload {
        let rng = SmallRng::seed_from_u64(self.seed);

        // Inspired by <https://stats.stackexchange.com/a/649432>
        let p50 = self.p50_size as f64;
        let p99 = self.p99_size as f64;
        let mu = p50.ln();
        let sigma = (p99.ln() - mu) / 2.3263;

        let size_distribution = LogNormal::new(mu, sigma).unwrap();
        let action_distribution =
            WeightedIndex::new(&[self.write_weight, self.read_weight, self.delete_weight]).unwrap();

        Workload {
            name: self.name,
            concurrency: self.concurrency,

            rng,
            size_distribution,
            action_distribution,

            existing_files: Default::default(),
        }
    }
}

pub struct Workload {
    pub name: &'static str,
    pub concurrency: usize,

    /// The RNG driving all our distributions.
    rng: SmallRng,
    /// A distribution that generates payload sizes for the `write` action.
    size_distribution: LogNormal<f64>,
    /// A distribution that generates actions, such as write/read/delete.
    action_distribution: WeightedIndex<u8>,

    /// All the written files that we can then read or delete.
    existing_files: Vec<(InternalId, ExternalId)>,
}

impl Workload {
    pub fn builder(name: &'static str) -> WorkloadBuilder {
        WorkloadBuilder {
            name,
            concurrency: available_parallelism().unwrap().get(),
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

    pub fn next_action(&mut self) -> Action {
        loop {
            match self.action_distribution.sample(&mut self.rng) {
                0 => {
                    let seed = self.rng.next_u64();
                    let payload = self.get_payload(seed);
                    return Action::Write(InternalId(seed), payload);
                }
                1 => {
                    let Some((internal, external)) = self.sample_readback() else {
                        continue;
                    };
                    let payload = self.get_payload(internal.0);
                    return Action::Read(internal, external, payload);
                }
                _ => {
                    let Some((_internal, external)) = self.sample_readback() else {
                        continue;
                    };
                    return Action::Delete(external);
                }
            }
        }
    }

    /// Adds a file to the internal store, so it can be yielded for reads or deletes.
    ///
    /// This function has to be called for files when a write or read has completed.
    /// (Files currently being read will not be concurrently deleted)
    pub fn push_file(&mut self, internal: InternalId, external: ExternalId) {
        self.existing_files.push((internal, external))
    }
}

#[derive(Eq, Hash, PartialEq)]
pub struct InternalId(u64);

pub type ExternalId = String;

pub enum Action {
    Write(InternalId, Payload),
    Read(InternalId, ExternalId, Payload),
    Delete(ExternalId),
}

pub struct Payload {
    pub len: u64,
    pub rng: SmallRng,
}

impl io::Read for Payload {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len_to_fill = (buf.len() as u64).min(self.len) as usize;

        let fill_buf = &mut buf[..len_to_fill];
        self.rng.fill_bytes(fill_buf);

        self.len -= len_to_fill as u64;
        Ok(len_to_fill)
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
