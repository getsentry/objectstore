//! The change stream each storage backend publishes.
//!
//! A backend describes its cost-tracking reporting with a [`CostTrackerStreamConfig`];
//! the service describes where those records go with a [`CostTrackerConfig`], shared by
//! every backend. [`ChangeStreamFactory`] pairs the two into a [`ChangeStream`].
//!
//! Behind the `storage-cogs` feature. Without it every backend gets a [`NoopStream`] and
//! the transport is left out of the binary.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::id::ObjectId;

#[cfg(feature = "storage-cogs")]
mod cost_tracker;
mod factory;

#[cfg(feature = "storage-cogs")]
pub use cost_tracker::CostTrackerStream;
pub use factory::ChangeStreamFactory;
#[cfg(feature = "storage-cogs")]
pub use factory::CostTrackerConfig;

#[cfg(all(test, feature = "storage-cogs"))]
pub(crate) use factory::dummy_factory;

/// How long a backend waits for reported records to be handed off during shutdown.
pub const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// Scope key holding the Sentry organization ID.
#[cfg(feature = "storage-cogs")]
const SCOPE_ORGANIZATION: &str = "org";
/// Scope key holding the Sentry project ID.
#[cfg(feature = "storage-cogs")]
const SCOPE_PROJECT: &str = "project";

/// What a single backend reports for cost tracking, and how much of it.
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

/// Publishes the changes a single backend makes to the objects it stores.
///
/// See [module docs](self).
#[async_trait::async_trait]
pub trait ChangeStream: fmt::Debug + Send + Sync + 'static {
    /// Reports that `id` now occupies `size` bytes. Used for new writes and overwrites.
    fn write(&self, id: &ObjectId, size: u64, expires_at: Option<SystemTime>);

    /// Reports that `id`'s expiration moved, with its stored size unchanged.
    fn update(&self, id: &ObjectId, expires_at: Option<SystemTime>);

    /// Reports that `id` was deleted explicitly. Does not account for automatic GC.
    fn delete(&self, id: &ObjectId);

    /// Blocks until reported records have been delivered, or `timeout` elapses.
    ///
    /// Call this during shutdown to drain the change stream queue.
    /// Waits for reported records to be delivered, or until `timeout` elapses.
    ///
    /// Awaited from [`Backend::join`](crate::backend::common::Backend::join) so records
    /// reported just before shutdown are not lost.
    async fn join(&self, timeout: Duration);
}

/// Drains `change_stream`, bounded by [`FLUSH_TIMEOUT`].
///
/// Backends call this from [`Backend::join`](crate::backend::common::Backend::join) so
/// records reported just before shutdown are not silently lost.
pub async fn flush_change_stream(change_stream: &Arc<dyn ChangeStream>) {
    change_stream.join(FLUSH_TIMEOUT).await;
}

/// A [`ChangeStream`] that reports nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopStream;

#[async_trait::async_trait]
impl ChangeStream for NoopStream {
    fn write(&self, _id: &ObjectId, _size: u64, _expires_at: Option<SystemTime>) {}

    fn update(&self, _id: &ObjectId, _expires_at: Option<SystemTime>) {}

    fn delete(&self, _id: &ObjectId) {}

    async fn join(&self, _timeout: Duration) {}
}

/// Reads a scope value off `id` as an integer, if present and well-formed.
#[cfg(feature = "storage-cogs")]
fn scope_id(id: &ObjectId, scope: &str) -> Option<u64> {
    id.scopes().get_value(scope)?.parse().ok()
}
