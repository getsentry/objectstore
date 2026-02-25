//! Shared trait definition and types for all backends.

use std::fmt::Debug;

use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::error::Result;
use crate::id::ObjectId;

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// Backend response for put operations.
pub(super) type PutResponse = ();
/// Backend response for get operations.
pub(super) type GetResponse = Option<(Metadata, PayloadStream)>;
/// Backend response for metadata-only get operations.
pub(super) type MetadataResponse = Option<Metadata>;
/// Backend response for delete operations.
pub(super) type DeleteResponse = ();

/// Response from [`Backend::delete_non_tombstone`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteOutcome {
    /// The entity was a redirect tombstone; it was left intact.
    Tombstone,
    /// The entity was a regular object (now deleted) or non-existent.
    Deleted,
}

/// A type-erased [`Backend`] instance.
pub type BoxedBackend = Box<dyn Backend>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    /// The backend name, used for diagnostics.
    fn name(&self) -> &'static str;

    /// Stores an object at the given path with the given metadata.
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> Result<PutResponse>;

    /// Retrieves an object at the given path, returning its metadata and a stream of bytes.
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse>;

    /// Retrieves only the metadata for an object, without the payload.
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        Ok(self
            .get_object(id)
            .await?
            .map(|(metadata, _stream)| metadata))
    }

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse>;

    /// Deletes the object only if it is NOT a redirect tombstone.
    ///
    /// Returns [`DeleteOutcome::Tombstone`] (leaving the row intact) when
    /// the object is a redirect tombstone, or [`DeleteOutcome::Deleted`]
    /// (after deleting it) for regular objects and non-existent rows.
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<DeleteOutcome> {
        let metadata = self.get_metadata(id).await?;
        if metadata.is_some_and(|m| m.is_tombstone()) {
            Ok(DeleteOutcome::Tombstone)
        } else {
            self.delete_object(id).await?;
            Ok(DeleteOutcome::Deleted)
        }
    }
}

/// Creates a reqwest client with required defaults.
pub fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .build()
        .expect("Client::new()")
}
