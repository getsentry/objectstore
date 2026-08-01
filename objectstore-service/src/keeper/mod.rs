//! Keeper defines a trait to support extending an object retention for backends
//! that does not support them natively (e.g., S3-compatible backend and filesystem).

use objectstore_types::metadata::ExpirationPolicy;

use crate::error::Result;
use crate::id::ObjectId;

/// SQLite-backed implementation of the [`Keeper`] trait.
pub mod sqlite_backed;

/// Represents the computed expiry information for an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectExpiry<'a> {
    id: &'a ObjectId,
    expiration_policy: ExpirationPolicy,
}

/// Object retention keeper trait.
#[async_trait::async_trait]
pub trait Keeper: Send + Sync {
    /// Keep is the first step in the object retention lifecycle.
    /// Practically speaking, it would not be kept if the `expiration_policy` is `Manual`.
    /// The `expiration_policy` is set by the client at upload time via the
    /// [`x-sn-expiration`](crate::id::HEADER_EXPIRATION) header and persisted with the object.
    async fn keep(&self, id: &ObjectId, expiration_policy: ExpirationPolicy) -> Result<()>;

    /// Remove is the final step in the object retention lifecycle.
    /// It is called by a cleanup worker when the object is no longer needed.
    async fn remove(&self, id: &ObjectId) -> Result<()>;

    /// Marks an object as accessed. For `expiration_policy` of `TimeToIdle`, this will
    /// extend the object retention.
    async fn mark_accessed(&self, id: &ObjectId) -> Result<()>;
}
