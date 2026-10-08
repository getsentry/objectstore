//! The change stream each storage backend publishes.
//!
//! A backend describes its cost-tracking reporting with a [`CostTrackerStreamConfig`];
//! the service describes where those records go with a [`CostTrackerConfig`], shared by
//! every backend. [`ChangeStreamFactory`] pairs the two into a [`ChangeStream`].
//!
//! [`ChangeTarget`] distinguishes published objects from resumable upload sessions.
//! Session creation reports the declared upload length and a seven-day inventory
//! expiry. Completion reports the published object followed by deletion of the session;
//! cancellation also deletes the session. Individual chunks do not emit changes.
//! The inventory expiry bounds accounting estimates; it does not enforce backend
//! session expiry or implement garbage collection.
//!
//! Cost tracking includes sessions by default. GCS disables session accounting in
//! the cost-tracking adapter while still publishing the generic lifecycle events.
//!
//! Behind the `storage-cogs` feature. Without it every backend gets a [`NoopStream`] and
//! the transport is left out of the binary.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use objectstore_types::time::Timestamp;

use crate::id::ObjectId;
use crate::resumable::Session;

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

/// Inventory lifetime assigned to a newly created resumable upload session.
///
/// This bounds estimated storage accounting for abandoned uploads. It does not
/// itself expire backend sessions or perform garbage collection.
pub(crate) const UPLOAD_SESSION_TTL: Duration = Duration::from_hours(7 * 24);

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

/// The physical object or resumable upload session affected by a change.
///
/// Sessions have their own identity and storage lifecycle, even when several uploads
/// target the same object. Completing an upload writes the object and deletes the session.
#[derive(Clone, Copy, Debug)]
pub enum ChangeTarget<'a> {
    /// A published object, including a backend's tombstone records.
    Object(&'a ObjectId),
    /// Temporary storage belonging to an incomplete resumable upload.
    Resumable(&'a Session),
}

impl<'a> ChangeTarget<'a> {
    /// Returns the object identity used to attribute the change to its owner.
    pub fn object_id(self) -> &'a ObjectId {
        match self {
            Self::Object(id) => id,
            Self::Resumable(session) => &session.object_id,
        }
    }
}

impl<'a> From<&'a ObjectId> for ChangeTarget<'a> {
    fn from(id: &'a ObjectId) -> Self {
        Self::Object(id)
    }
}

impl<'a> From<&'a Session> for ChangeTarget<'a> {
    fn from(session: &'a Session) -> Self {
        Self::Resumable(session)
    }
}

/// Publishes the changes a single backend makes to the objects and upload sessions it stores.
///
/// See [module docs](self).
#[async_trait::async_trait]
pub trait ChangeStream: fmt::Debug + Send + Sync + 'static {
    /// Reports a new target or overwrite with its size and expiration.
    ///
    /// Object sizes describe stored bytes. Session sizes estimate usage from the
    /// declared upload length; chunks do not emit size updates.
    fn write(&self, target: ChangeTarget<'_>, size: u64, expires_at: Option<Timestamp>);

    /// Reports that `target`'s expiration moved, with its size unchanged.
    fn update(&self, target: ChangeTarget<'_>, expires_at: Option<Timestamp>);

    /// Reports that `target` was deleted explicitly, including a completed session.
    /// Does not account for automatic GC.
    fn delete(&self, target: ChangeTarget<'_>);

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
    fn write(&self, _target: ChangeTarget<'_>, _size: u64, _expires_at: Option<Timestamp>) {}

    fn update(&self, _target: ChangeTarget<'_>, _expires_at: Option<Timestamp>) {}

    fn delete(&self, _target: ChangeTarget<'_>) {}

    async fn join(&self, _timeout: Duration) {}
}

/// Reads a scope value off `id` as an integer, if present and well-formed.
#[cfg(feature = "storage-cogs")]
fn scope_id(id: &ObjectId, scope: &str) -> Option<u64> {
    id.scopes().get_value(scope)?.parse().ok()
}
