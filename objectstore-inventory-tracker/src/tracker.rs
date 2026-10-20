//! Record identity, sampling, and the emitting entry point.

use std::future::Future;
use std::time::{Duration, SystemTime};

use crate::producer::Producer;
use crate::record::{InventoryRecord, OpType, epoch_micros};

/// Hex characters, lowercase, matching the format consumers expect in `record_id`.
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Bytes of the hash reserved for the sampling decision.
const TOKEN_BYTES: usize = 8;
/// Bytes of the hash used to build the emitted record id, giving a 128-bit identifier.
const ID_BYTES: usize = 16;

// Don't need a dep just for this.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Emits inventory records for one shared resource.
///
/// # Hashing and sampling
///
/// `InventoryTracker` decides whether to emit a message for a given record based on the
/// configured `sample_rate` and a hash of the record ID passed in by the caller. A sample
/// rate of 1.0 means it will emit messages for 100% of records. A sample rate of 0.5
/// means it will emit messages for 50% of records. If a record is sampled out, no change
/// to that record will ever emit a message. If a record is included in the sample, every
/// change to that record will emit a message.
///
/// The hash is what `InventoryTracker` actually uses to populate the `record_id` message
/// field. Each message also includes the sample rate that was in effect at the time.
///
/// **The hash is a permanent wire contract.** Changing the algorithm, the byte ranges,
/// or introducing a salt renames every record, and consumers will double-count until the
/// old identifiers age out.
///
/// # Example
///
/// ```
/// use objectstore_inventory_tracker::{InventoryTracker, NoopProducer};
/// use std::time::SystemTime;
///
/// let tracker = InventoryTracker::new(NoopProducer, "example_resource", 1.0);
///
/// tracker.write(
///     "example_feature/org.123/project.456/objects/abc",
///     "example_feature",
///     4096,
///     SystemTime::now(),
///     None,
///     Some(123),
///     Some(456),
/// )?;
/// # Ok::<(), std::convert::Infallible>(())
/// ```
#[derive(Clone, Debug)]
pub struct InventoryTracker<P: Producer> {
    producer: P,
    shared_resource_id: String,
    sample_rate: f64,
    /// `sample_rate` as a point in the token space, so sampling is an integer comparison.
    sample_threshold: u64,
}

impl<P: Producer> InventoryTracker<P> {
    /// Creates a tracker emitting for `shared_resource_id` at `sample_rate`.
    ///
    /// `shared_resource_id` is meant to match a label on a provisioned storage backend so
    /// that downstream consumers can join the change stream dataset with, for instance,
    /// billing info.
    ///
    /// `sample_rate` is clamped to `[0, 1]`. A rate of `1.0` emits every record and
    /// short-circuits the sampling check entirely. A rate of `0.0` emits no records.
    pub fn new(producer: P, shared_resource_id: impl Into<String>, sample_rate: f64) -> Self {
        let sample_rate = if sample_rate.is_nan() {
            1.0
        } else {
            sample_rate.clamp(0.0, 1.0)
        };

        Self {
            producer,
            shared_resource_id: shared_resource_id.into(),
            sample_rate,
            // If a record's hash token is greater than or equal to this threshold, it is
            // skipped. A sample rate of 0 produces a threshold of 0 so all tokens are
            // skipped. A sample rate of 1.0 produces a threshold of `u64::MAX` so all
            // tokens will be included (except when the token _is_ `u64::MAX`, unlikely as
            // that may be. That edge case is handled in `sample()`).
            sample_threshold: (sample_rate * (u64::MAX as f64)) as u64,
        }
    }

    /// The storage resource this tracker emits for.
    pub fn shared_resource_id(&self) -> &str {
        &self.shared_resource_id
    }

    /// The fraction of records being emitted.
    pub fn sample_rate(&self) -> f64 {
        self.sample_rate
    }

    /// Decides whether `storage_key` is tracked, returning its record id if so.
    ///
    /// The decision is deterministic for a `storage_key`, so every operation on a record
    /// resolves the same way.
    ///
    /// It is also monotone in the rate: the set sampled at a lower rate is a subset of
    /// the set sampled at a higher one. Raising the rate is therefore safe for records
    /// already in flight.
    fn sample(&self, storage_key: &str) -> Option<String> {
        if self.sample_threshold == 0 {
            return None;
        }

        let hash = blake3::hash(storage_key.as_bytes());
        let bytes = hash.as_bytes();

        // This `sample_threshold < u64::MAX` check is handling an edge case. A sample
        // rate of 1.0 produces a sample threshold of `u64::MAX` which should include
        // everything. However, technically it will incorrectly skip hash tokens that
        // happen to equal `u64::MAX`, however unlikely that is. So, if our threshold is
        // `u64::MAX`, we skip this sampling check and just return the encoded key.
        if self.sample_threshold < u64::MAX {
            let token = u64::from_le_bytes(bytes[..TOKEN_BYTES].try_into().ok()?);
            if token >= self.sample_threshold {
                return None;
            }
        }

        Some(hex_encode(&bytes[TOKEN_BYTES..TOKEN_BYTES + ID_BYTES]))
    }

    /// Emits a `WRITE`: the record was created, or replaced with new contents.
    ///
    /// Does nothing and returns `Ok(())` if `storage_key` is not sampled.
    #[allow(clippy::too_many_arguments)]
    pub fn write(
        &self,
        storage_key: &str,
        app_feature: &str,
        size: u64,
        timestamp: SystemTime,
        expiration_time: Option<SystemTime>,
        organization_id: Option<u64>,
        project_id: Option<u64>,
    ) -> Result<(), P::Error> {
        let Some(record_id) = self.sample(storage_key) else {
            return Ok(());
        };

        self.emit(InventoryRecord {
            shared_resource_id: self.shared_resource_id.clone(),
            app_feature: app_feature.to_owned(),
            op_type: OpType::Write,
            record_id,
            timestamp: epoch_micros(timestamp),
            sample_rate: self.sample_rate,
            size: Some(size),
            expiration_time: expiration_time.map(epoch_micros),
            organization_id,
            project_id,
        })
    }

    /// Emits an `UPDATE`: metadata changed but the stored size did not.
    ///
    /// Does nothing and returns `Ok(())` if `storage_key` is not sampled.
    pub fn update(
        &self,
        storage_key: &str,
        app_feature: &str,
        timestamp: SystemTime,
        expiration_time: Option<SystemTime>,
        organization_id: Option<u64>,
        project_id: Option<u64>,
    ) -> Result<(), P::Error> {
        let Some(record_id) = self.sample(storage_key) else {
            return Ok(());
        };

        self.emit(InventoryRecord {
            shared_resource_id: self.shared_resource_id.clone(),
            app_feature: app_feature.to_owned(),
            op_type: OpType::Update,
            record_id,
            timestamp: epoch_micros(timestamp),
            sample_rate: self.sample_rate,
            // Omitted, which consumers read as "unchanged" and carry forward.
            size: None,
            expiration_time: expiration_time.map(epoch_micros),
            organization_id,
            project_id,
        })
    }

    /// Emits a `DELETE`: the record is gone.
    ///
    /// Does nothing and returns `Ok(())` if `storage_key` is not sampled.
    pub fn delete(
        &self,
        storage_key: &str,
        app_feature: &str,
        timestamp: SystemTime,
    ) -> Result<(), P::Error> {
        let Some(record_id) = self.sample(storage_key) else {
            return Ok(());
        };

        self.emit(InventoryRecord {
            shared_resource_id: self.shared_resource_id.clone(),
            app_feature: app_feature.to_owned(),
            op_type: OpType::Delete,
            record_id,
            timestamp: epoch_micros(timestamp),
            sample_rate: self.sample_rate,
            size: None,
            expiration_time: None,
            organization_id: None,
            project_id: None,
        })
    }

    /// Waits for emitted records to be delivered, or until `timeout` elapses.
    ///
    /// Call this during shutdown, within whatever budget the service allows for draining.
    /// Without it, records emitted moments before exit are still in a local queue and are
    /// lost with the process.
    pub fn join(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), P::Error>> + Send + use<P>
    where
        P: Clone + Send + Sync + 'static,
        P::Error: Send + 'static,
    {
        self.producer.join(timeout)
    }

    fn emit(&self, record: InventoryRecord) -> Result<(), P::Error> {
        let key = record.record_id.clone();
        // Serialization of this struct cannot fail: every field is a plain scalar or
        // string.
        let payload = serde_json::to_vec(&record).expect("inventory record is serializable");
        self.producer.send(key.as_bytes(), payload)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::test_utils::DummyProducer;

    use super::*;

    fn tracker(rate: f64) -> (DummyProducer, InventoryTracker<DummyProducer>) {
        let producer = DummyProducer::default();
        let tracker = InventoryTracker::new(producer.clone(), "example_resource", rate);
        (producer, tracker)
    }

    #[test]
    fn record_id_is_stable_for_a_given_key() {
        let (_, tracker) = tracker(1.0);
        let key = "example_feature/org.1/project.1/objects/abc";
        assert_eq!(tracker.sample(key), tracker.sample(key));

        let record_id = tracker.sample(key).unwrap();
        assert_eq!(record_id.len(), ID_BYTES * 2);
        assert!(
            record_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn distinct_keys_get_distinct_record_ids() {
        let (_, tracker) = tracker(1.0);
        let ids: HashSet<_> = (0..1000)
            .map(|i| tracker.sample(&format!("key/{i}")).unwrap())
            .collect();
        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn rate_of_one_samples_everything() {
        let (_, tracker) = tracker(1.0);
        for i in 0..1000 {
            assert!(tracker.sample(&format!("key/{i}")).is_some());
        }
    }

    #[test]
    fn sampling_is_uniform_across_key_prefixes() {
        let rate = 0.25;
        let (_, tracker) = tracker(rate);

        for prefix in ["attachments", "profiles", "preprod", "trace_attachments"] {
            let total = 20_000;
            let sampled = (0..total)
                .filter(|i| {
                    tracker
                        .sample(&format!("{prefix}/org.1/project.1/objects/{i}"))
                        .is_some()
                })
                .count();
            let observed = sampled as f64 / total as f64;
            assert!(
                (observed - rate).abs() < 0.02,
                "prefix {prefix} sampled at {observed}, expected ~{rate}"
            );
        }
    }

    #[test]
    fn sampling_is_monotone_in_the_rate() {
        let (_, low) = tracker(0.1);
        let (_, high) = tracker(0.5);
        let (_, full) = tracker(1.0);

        for i in 0..5000 {
            let key = format!("key/{i}");
            if low.sample(&key).is_some() {
                assert!(
                    high.sample(&key).is_some(),
                    "{key} dropped when raising to 0.5"
                );
                assert!(
                    full.sample(&key).is_some(),
                    "{key} dropped when raising to 1.0"
                );
            }
        }
    }

    #[test]
    fn record_id_is_independent_of_sample_rate() {
        let (_, low) = tracker(0.1);
        let (_, full) = tracker(1.0);

        for i in 0..2000 {
            let key = format!("key/{i}");
            if let Some(sampled) = low.sample(&key) {
                assert_eq!(sampled, full.sample(&key).unwrap());
            }
        }
    }

    #[test]
    fn emitted_message_is_keyed_on_the_record_id() {
        let (producer, tracker) = tracker(1.0);
        tracker
            .write(
                "some/key",
                "example_feature",
                10,
                SystemTime::now(),
                None,
                None,
                None,
            )
            .unwrap();

        let (message_key, _) = producer.raw().into_iter().next().unwrap();
        let record_id = &producer.records()[0].record_id;
        assert_eq!(message_key, record_id.as_bytes());
        assert_ne!(
            message_key, b"some/key",
            "the raw storage key must not be used as the message key"
        );
    }

    #[test]
    fn write_always_carries_a_size_and_delete_never_does() {
        let (producer, tracker) = tracker(1.0);
        let now = SystemTime::now();

        tracker
            .write("some/key", "f", 4096, now, None, None, None)
            .unwrap();
        tracker
            .update("some/key", "f", now, Some(now), None, None)
            .unwrap();
        tracker.delete("some/key", "f", now).unwrap();

        let records = producer.records();
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].size, Some(4096));
        assert_eq!(records[1].op_type, OpType::Update);
        assert_eq!(records[1].size, None, "update means size unchanged");
        assert_eq!(records[2].op_type, OpType::Delete);
        assert_eq!(records[2].size, None);
    }

    #[test]
    fn every_operation_on_a_key_reports_the_same_record_id() {
        let (producer, tracker) = tracker(1.0);
        let now = SystemTime::now();

        tracker
            .write("some/key", "f", 4096, now, None, None, None)
            .unwrap();
        tracker.delete("some/key", "f", now).unwrap();

        let records = producer.records();
        assert_eq!(records[0].record_id, records[1].record_id);
    }

    #[test]
    fn unsampled_keys_emit_nothing() {
        let (producer, tracker) = tracker(0.25);
        let now = SystemTime::now();

        let unsampled = (0..)
            .map(|i| format!("key/{i}"))
            .find(|key| tracker.sample(key).is_none())
            .expect("some key is not sampled");

        tracker
            .write(&unsampled, "f", 1, now, None, None, None)
            .unwrap();
        tracker
            .update(&unsampled, "f", now, None, None, None)
            .unwrap();
        tracker.delete(&unsampled, "f", now).unwrap();

        assert!(producer.records().is_empty());
    }

    #[test]
    fn sample_rate_is_stamped_on_every_record() {
        let (producer, tracker) = tracker(0.25);
        let sampled = (0..)
            .map(|i| format!("key/{i}"))
            .find(|key| tracker.sample(key).is_some())
            .expect("some key is sampled");

        tracker
            .write(&sampled, "f", 1, SystemTime::now(), None, None, None)
            .unwrap();

        assert_eq!(producer.records()[0].sample_rate, 0.25);
    }

    #[test]
    fn threshold_is_derived_from_the_rate() {
        for (rate, expected) in [
            (0.0, 0),
            (0.25, 1u64 << 62),
            (0.5, 1u64 << 63),
            (1.0, u64::MAX),
        ] {
            assert_eq!(tracker(rate).1.sample_threshold, expected, "rate {rate}");
        }
    }

    #[test]
    fn out_of_range_rates_are_clamped() {
        for high in [f64::NAN, f64::INFINITY, 2.0] {
            let (_, tracker) = tracker(high);
            assert_eq!(
                tracker.sample_rate(),
                1.0,
                "rate {high} should clamp to 1.0"
            );
        }
        for low in [f64::NEG_INFINITY, -1.0] {
            let (_, tracker) = tracker(low);
            assert_eq!(tracker.sample_rate(), 0.0, "rate {low} should clamp to 0.0");
        }
    }

    #[test]
    fn a_rate_of_zero_emits_nothing() {
        let (producer, tracker) = tracker(0.0);
        let now = SystemTime::now();

        for i in 0..1000 {
            let key = format!("key/{i}");
            tracker.write(&key, "f", 1, now, None, None, None).unwrap();
            tracker.update(&key, "f", now, None, None, None).unwrap();
            tracker.delete(&key, "f", now).unwrap();
        }

        assert!(producer.records().is_empty());
    }
}
