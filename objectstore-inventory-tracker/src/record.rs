//! The wire format emitted onto the inventory topic.
//!
//! These types mirror the `shared-resources-inventory` schema registered in
//! [sentry-kafka-schemas]. The schema sets `additionalProperties: false`, so adding a
//! field here without a corresponding schema version bump produces messages that
//! consumers reject.
//!
//! [sentry-kafka-schemas]: https://github.com/getsentry/sentry-kafka-schemas

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The kind of change a record describes. `WRITE`, `UPDATE`, or `DELETE`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OpType {
    /// The record was created, or replaced with new contents.
    Write,
    /// An existing record's metadata changed without its stored size changing.
    ///
    /// Extending a record's expiration deadline is an example of an update operation.
    Update,
    /// The record was removed.
    Delete,
}

/// A single inventory change event.
///
/// Construct these through [`InventoryTracker`](crate::InventoryTracker) rather than
/// directly.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InventoryRecord {
    /// Identifies the shared resource this record belongs to.
    ///
    /// This is meant to match a label on a provisioned storage backend so that downstream
    /// consumers can join the change stream dataset with, for instance, billing info.
    pub shared_resource_id: String,

    /// The product feature this record is attributed to.
    pub app_feature: String,

    /// The type of operation that occurred.
    pub op_type: OpType,

    /// Opaque stable identifier, unique within `shared_resource_id`.
    ///
    /// A hash of the caller's storage key, derived by
    /// [`InventoryTracker`](crate::InventoryTracker). The raw key is never emitted.
    pub record_id: String,

    /// When the operation occurred, as Unix epoch microseconds.
    pub timestamp: i64,

    /// Fraction of records the producer is emitting for this resource, in `[0, 1]`.
    ///
    /// `InventoryTracker` will not emit messages for records that are sampled out so the
    /// consumer doesn't need to do any filtering. The reason the sample rate is included
    /// on messages is so that the consumer or downstream pipelines can apply a
    /// `1 / sample_rate` weight when calculating aggregates to account for changes to the
    /// sample rate.
    pub sample_rate: f64,

    /// Stored size in bytes. Always set for [`OpType::Write`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,

    /// When this record is set to expire, in Unix epoch microseconds if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_time: Option<i64>,

    /// ID of the organization that owns the record, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<u64>,

    /// ID of the project that owns the record, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<u64>,
}

/// Converts a [`SystemTime`] to Unix epoch microseconds.
pub fn epoch_micros(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        Err(err) => i64::try_from(err.duration().as_micros()).map_or(i64::MIN, |micros| -micros),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;

    fn record(op_type: OpType) -> InventoryRecord {
        InventoryRecord {
            shared_resource_id: "example_resource".into(),
            app_feature: "example_feature".into(),
            op_type,
            record_id: "3f7a1c2e9b4d5a6f8c0e1d2b3a4f5e6c".into(),
            timestamp: 1_785_283_200_000_000,
            sample_rate: 1.0,
            size: None,
            expiration_time: None,
            organization_id: None,
            project_id: None,
        }
    }

    #[test]
    fn write_serializes_to_expected_wire_format() {
        let mut rec = record(OpType::Write);
        rec.size = Some(524_288);
        rec.expiration_time = Some(1_785_888_000_000_000);
        rec.organization_id = Some(1);
        rec.project_id = Some(1);

        assert_eq!(
            serde_json::to_value(&rec).unwrap(),
            json!({
                "shared_resource_id": "example_resource",
                "app_feature": "example_feature",
                "op_type": "WRITE",
                "record_id": "3f7a1c2e9b4d5a6f8c0e1d2b3a4f5e6c",
                "timestamp": 1_785_283_200_000_000_i64,
                "sample_rate": 1.0,
                "size": 524_288,
                "expiration_time": 1_785_888_000_000_000_i64,
                "organization_id": 1,
                "project_id": 1,
            })
        );
    }

    #[test]
    fn absent_optional_fields_are_omitted() {
        let value = serde_json::to_value(record(OpType::Delete)).unwrap();
        let object = value.as_object().unwrap();

        assert_eq!(object["op_type"], "DELETE");
        for absent in ["size", "expiration_time", "organization_id", "project_id"] {
            assert!(!object.contains_key(absent), "{absent} should be omitted");
        }
        // The required fields survive that omission.
        for present in [
            "shared_resource_id",
            "app_feature",
            "op_type",
            "record_id",
            "timestamp",
            "sample_rate",
        ] {
            assert!(object.contains_key(present), "{present} is required");
        }
    }

    #[test]
    fn op_types_use_uppercase_spellings() {
        for (op_type, expected) in [
            (OpType::Write, "WRITE"),
            (OpType::Update, "UPDATE"),
            (OpType::Delete, "DELETE"),
        ] {
            assert_eq!(serde_json::to_value(op_type).unwrap(), json!(expected));
        }
    }

    #[test]
    fn epoch_micros_converts_both_directions() {
        assert_eq!(epoch_micros(UNIX_EPOCH), 0);
        assert_eq!(
            epoch_micros(UNIX_EPOCH + Duration::from_micros(1_785_283_200_000_000)),
            1_785_283_200_000_000
        );
        assert_eq!(
            epoch_micros(UNIX_EPOCH - Duration::from_micros(1_500)),
            -1_500
        );
    }

    #[test]
    fn epoch_micros_preserves_sub_second_precision() {
        let time = UNIX_EPOCH + Duration::new(1_785_283_200, 123_456_000);
        assert_eq!(epoch_micros(time), 1_785_283_200_123_456);
    }
}
