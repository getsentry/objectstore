//! Persistence of object expiration metadata, read by the garbage collector to
//! find objects whose lifetime has elapsed.
//!
//! [`GcStorage`] is the backend-agnostic interface. Implementations record the
//! expiration of each object when it is written and hand back the expired ids
//! that the collector is allowed to delete.

/// [`GcStorage`] implementation backed by SQLite.
pub mod sqlite;

use std::fmt::Debug;

use objectstore_types::time::Timestamp;

use crate::change_stream::ChangeStreamError;
use crate::id::ObjectId;

/// Stores object expiration metadata for the garbage collector.
///
/// Implementations are called on the write path (via [`upsert`](Self::upsert)
/// and [`remove`](Self::remove)) and by the collector itself (via
/// [`list_expired`](Self::list_expired)), so they must be safe to share across
/// tasks.
#[async_trait::async_trait]
pub trait GcStorage: Send + Sync + Debug {
    /// Records that `id` expires at `expires_at`, replacing any previous deadline.
    ///
    /// A later deadline replaces an earlier one; a shorter deadline is ignored, so concurrent
    /// writers cannot pull an expiration forward. `None` clears the recorded deadline and marks
    /// the object as never expiring.
    async fn upsert(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError>;

    /// Forgets `id` entirely.
    ///
    /// Best-effort: failures are not reported to the caller.
    async fn remove(&self, id: &ObjectId);

    /// Returns up to `limit` ids whose expiration is at or before `now`,
    /// ordered by earliest expiry first.
    ///
    /// Objects without an expiration are never returned.
    async fn list_expired(
        &self,
        now: Timestamp,
        limit: u64,
    ) -> Result<Vec<ObjectId>, ChangeStreamError>;
}
