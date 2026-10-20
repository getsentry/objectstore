//! Emits per-record inventory change events for shared storage resources.
//!
//! Change events are published to the `shared-resources-inventory` Kafka topic. They
//! include (among other things):
//! - `shared_resource_id`: identifies the storage backend the record is hosted in. This
//!   is meant to be, for example, a specific GCS bucket or Bigtable instance.
//! - `record_id`: identifies each record. `InventoryTracker` populates this with a hash
//!   of the identifier passed in by the caller.
//! - `size`: the size of the record in bytes (including metadata).
//! - `expiration_time`: a timestamp (unixtime microseconds) describing when the record is
//!   meant to be deleted.
//!
//! # Usage
//!
//! ```
//! use objectstore_inventory_tracker::{InventoryTracker, NoopProducer};
//! use std::time::SystemTime;
//!
//! # let producer = NoopProducer;
//! // Create your `InventoryTracker`. This one is for the `my_gcs_bucket` bucket
//! // and has a sampling rate of `1.0` (unsampled).
//! let tracker = InventoryTracker::new(producer, "my_gcs_bucket", 1.0);
//!
//! // Emit a message indicating that `storage_key` has been written, 4096 bytes in size.
//! // Sampling and hashing of the key are handled internally.
//! let storage_key = "example_feature/org.1/project.1/objects/abc";
//! tracker.write(storage_key, "example_feature", 4096, SystemTime::now(), None, Some(1), Some(1))?;
//! # Ok::<(), std::convert::Infallible>(())
//! ```
//!
//! The `"my_gcs_bucket"` string in the above example is meant to correspond to a
//! label on the specific GCS bucket your service uses to store data. That way, an
//! inventory derived from your change stream can be joined with billing data to
//! analyze costs. If you have multiple buckets, or multiple storage backends, it's
//! recommended that you configure each of them with their own `InventoryTracker`.
//!
//! # Sampling
//!
//! [`InventoryTracker`] hashes the storage key it gets from the caller and uses part of
//! the digest to determine whether change messages should be emitted for a given object.
//! Each [`InventoryTracker`] instance is configured with its own sample rate so services
//! can enable, disable, or tune sampling as needed.
//!
//! Each change message includes the sampling rate that was in effect when the message was
//! emitted. Downstream consumers can use `1 / sample_rate` as a weight when computing
//! aggregates to approximate what the unsampled aggregate would have been.
//!
//! When sampling is used the resulting dataset will generally be representative. However,
//! it will not be complete, and with a low sample rate it's more likely that a small
//! trend or subpopulation of your data will be entirely missing from the sampled dataset.
//!
//! # Fail open
//!
//! [`Producer::send`] does not block, and it returns an error instead of waiting when the
//! local queue is full. Callers may count the error and move on.
//!
//! Sending is asynchronous, so messages handed over just before a process exits are still
//! sitting in a local queue. Await [`InventoryTracker::join`] during shutdown to deliver
//! them; anything still queued when the process goes away is lost.
//!
//! Losing change messages causes drift in downstream consumers. If your storage service
//! expires data, this drift will sort itself out as records age out. If your service
//! retains data indefinitely, consider writing a periodic reconciliation job.
//!
//! # Ordering
//!
//! Each change message has a `timestamp` field with microsecond resolution so that
//! multiple changes to the same record are ordered.
//!
//! NOTE: This crate can provide no guarantees that this ordering scheme will always
//! result in messages being serialized in Kafka in the same order the corresponding
//! operations were serialized in your service or its storage layer. If this is an issue,
//! consider writing a periodic reconciliation job or implementing synchronization outside
//! of this crate.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod producer;
mod record;
mod tracker;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(feature = "kafka")]
pub mod kafka;

pub use producer::{BoxError, NoopProducer, Producer, SharedProducer};
pub use record::{InventoryRecord, OpType, epoch_micros};
pub use tracker::InventoryTracker;
