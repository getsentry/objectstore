//! Keeper defines a trait to support extending an object retention for backends
//! that does not support them natively (e.g., S3-compatible backend and filesystem).

use std::str::FromStr;

use objectstore_types::metadata::ExpirationPolicy;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
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
pub trait Keeper: Send + Sync + std::fmt::Debug {
    /// Keep is the first step in the object retention lifecycle.
    /// Practically speaking, it would not be kept if the `expiration_policy` is `Manual`.
    /// The `expiration_policy` is set by the client at upload time via the supplied Metadata.
    async fn keep(&self, id: &ObjectId, expiration_policy: ExpirationPolicy) -> Result<()>;

    /// Remove is the final step in the object retention lifecycle.
    /// It is called by a cleanup worker when the object is no longer needed.
    async fn remove(&self, id: &ObjectId) -> Result<()>;

    /// Update an object to a new expiration policy (and thus, new expiration time).
    async fn update(&self, id: &ObjectId, expiration_policy: ExpirationPolicy) -> Result<()>;
}

/// Keeper backend of choice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeeperBackend {
    /// SQLite-backed keeper.
    Sqlite,
}

impl FromStr for KeeperBackend {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sqlite" => Ok(KeeperBackend::Sqlite),
            _ => Err(Error::generic(format!("unknown backend {}", s))),
        }
    }
}

/// Configuration for the keeper backend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeeperConfig {
    /// Specifies the backend to use.
    pub backend: KeeperBackend,
    /// Connection URL for the backend.
    /// Refer to each backend's documentation for details.
    pub connection_url: String,
}
