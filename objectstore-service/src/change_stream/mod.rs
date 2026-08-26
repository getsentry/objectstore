//! The change stream each storage backend publishes.

use serde::{Deserialize, Serialize};

/// What a single backend reports, and how much of it.
///
/// A backend without one reports nothing.
///
/// # Example
///
/// ```yaml
/// storage_cogs:
///   shared_resource_id: bigtable_objectstore
///   sample_rate: 1.0
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CostTrackerStreamConfig {
    /// Identifies the storage backend resource.
    ///
    /// This is meant to correspond to a `shared_resource_id` label on a provisioned
    /// storage resource so that change stream data can be joined with other data about
    /// the storage resource.
    pub shared_resource_id: String,

    /// Proportion of records to report, in `[0, 1]`.
    ///
    /// `1.0` reports every change. It can be lowered if the stream is under too much load
    /// but beware: when the sample rate decreases, records that used to be tracked will
    /// no longer be tracked. Stream consumers may have inconsistent state for them until
    /// they expire.
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
}

/// Reports everything by default.
fn default_sample_rate() -> f64 {
    1.0
}
