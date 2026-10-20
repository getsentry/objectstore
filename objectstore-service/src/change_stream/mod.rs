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
use std::time::Duration;

use serde::{Deserialize, Serialize};

use objectstore_types::time::Timestamp;

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
/// Backends call `begin_*` before a storage operation that creates data or extends its
/// lifetime, and `commit_*` after a storage operation succeeds. A failed `begin_*` fails
/// the request before storage is touched. A `begin_*` is not always followed by a
/// `commit_*` (e.g. when the storage operation fails), so consumers that must never
/// underestimate what is stored should treat `begin_*` as an upper bound.
///
/// All methods but [`join`](Self::join) default to doing nothing. Backends await them
/// on the request path, so they add to request latency.
///
/// Consider spawning and independent task or using a queue in your implementation of the
/// methods if you only need best-effort delivery.
///
/// See [module docs](self).
#[async_trait::async_trait]
pub trait ChangeStream: fmt::Debug + Send + Sync + 'static {
    /// Announces that `id` is about to be written. Used for new writes and overwrites.
    async fn begin_write(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        let _ = (id, expires_at);
        Ok(())
    }

    /// Reports that `id` now occupies `size` bytes. Used for new writes and overwrites.
    async fn commit_write(&self, id: &ObjectId, size: u64, expires_at: Option<Timestamp>) {
        let _ = (id, size, expires_at);
    }

    /// Announces that `id`'s expiration is about to move out to `expires_at`.
    async fn begin_update(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        let _ = (id, expires_at);
        Ok(())
    }

    /// Reports that `id`'s expiration moved, with its stored size unchanged.
    async fn commit_update(&self, id: &ObjectId, expires_at: Option<Timestamp>) {
        let _ = (id, expires_at);
    }

    /// Reports that `id` was deleted explicitly. Does not account for automatic GC.
    async fn commit_delete(&self, id: &ObjectId) {
        let _ = id;
    }

    /// Blocks until reported records have been delivered, or `timeout` elapses.
    ///
    /// Call this during shutdown to drain the change stream queue.
    /// Waits for reported records to be delivered, or until `timeout` elapses.
    ///
    /// Awaited from [`Backend::join`](crate::backend::common::Backend::join) so records
    /// reported just before shutdown are not lost.
    async fn join(&self, timeout: Duration);
}

/// Why a [`ChangeStream`] could not begin a change.
///
/// The variant determines the [`ErrorKind`](crate::error::ErrorKind) of the failed
/// request. The source carries the details and can be a plain message:
///
/// ```
/// use objectstore_service::change_stream::ChangeStreamError;
///
/// let error = ChangeStreamError::Failure("constraint violated".into());
/// assert_eq!(error.to_string(), "change stream failed");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum ChangeStreamError {
    /// The sink is temporarily unavailable.
    #[error("change stream unavailable")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The sink did not respond in time.
    #[error("change stream timed out")]
    Timeout(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Any other failure.
    #[error("change stream failed")]
    Failure(#[source] Box<dyn std::error::Error + Send + Sync>),
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
    async fn join(&self, _timeout: Duration) {}
}

/// Reads a scope value off `id` as an integer, if present and well-formed.
#[cfg(feature = "storage-cogs")]
fn scope_id(id: &ObjectId, scope: &str) -> Option<u64> {
    id.scopes().get_value(scope)?.parse().ok()
}
