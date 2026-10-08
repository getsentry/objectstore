# objectstore-inventory-tracker

Wrapper around a Kafka producer that a storage service can use to emit a
lightweight change stream.

Produces to Kafka via [`rdkafka`](https://docs.rs/rdkafka).
Message schema is defined in [sentry-kafka-schemas](https://github.com/getsentry/sentry-kafka-schemas).

## Why

Generalized cheap analytics. A stream consumer can merge each change message
into an external table to maintain an inventory of each record stored along with
its size and some attribution metadata. Various analyses (usage trends, Cost of
Goods Sold (COGS) rollups) can be run on the external table relatively cheaply
and without putting stress on the datastores actually used to serve production
traffic.

`InventoryTracker` is not meant to be used as a write-ahead log or subscription
channel for record changes. It was designed with back-of-house analytics in
mind; record IDs are hashed to enable sampling and strip any possible sensitive
information, and stored data is not included in the stream.

## Usage

```rust
use objectstore_inventory_tracker::{InventoryTracker, KafkaConfig, KafkaProducer};
use std::time::SystemTime;

// Create your Kafka producer.
let producer = KafkaProducer::try_new(KafkaConfig {
    topic: "shared-resources-inventory".into(),
    bootstrap_servers: vec!["localhost:9092".into()],
    override_params: Default::default(),
})?;

// Create your `InventoryTracker`. This one is for the `my_gcs_bucket` bucket
// and has a sampling rate of `1.0` (unsampled).
let tracker = InventoryTracker::new(producer, "my_gcs_bucket", 1.0);

// Emit a message indicating that the given key has been written with a size of
// 4096 bytes. Hashing and sampling of the key are handled internally; if the key
// is not sampled, this is a no-op.
let storage_key = "attachments/org.1/project.1/objects/abc";
tracker.write(storage_key, "attachments", 4096, SystemTime::now(), None, Some(1), Some(1))?;
```

The `"my_gcs_bucket"` string in the above example is meant to correspond to a
label on the specific GCS bucket your service uses to store data. That way, an
inventory derived from your change stream can be joined with billing data to
analyze costs. If you have multiple buckets, or multiple storage backends, it's
recommended that you configure each of them with their own `InventoryTracker`.

### Features

| Feature | Description |
|---|---|
| `kafka` | The `sentry_arroyo`-backed producer. Off by default, so the record types are usable without pulling in arroyo, librdkafka, and their native build. |
| `test-utils` | Exposes the `test_utils` module and its `DummyProducer`, so downstream crates can assert on emitted records without a broker. |
