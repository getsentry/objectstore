//! Shared trait definition and types for all backends.

use std::fmt::Debug;

use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;

use bytes::Bytes;

use crate::error::Result;
use crate::id::ObjectId;
use crate::stream::{ClientStream, PayloadStream};

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// Backend response for put operations.
pub type PutResponse = ();
/// Backend response for get operations.
pub type GetResponse = Option<(Metadata, PayloadStream)>;
/// Backend response for metadata-only get operations.
pub type MetadataResponse = Option<Metadata>;
/// Backend response for delete operations.
pub type DeleteResponse = ();

/// Outcome of a tombstone-conditional operation.
///
/// Returned by [`Backend::put_if_not_tombstone`] and
/// [`Backend::delete_non_tombstone`] to indicate whether the operation
/// proceeded or was skipped because a redirect tombstone was present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationOutcome {
    /// The operation executed normally (write stored, delete removed).
    Executed,
    /// A redirect tombstone was found; the operation was skipped.
    Tombstone,
}

/// Trait implemented by all storage backends.
#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    /// The backend name, used for diagnostics.
    fn name(&self) -> &'static str;

    /// Stores an object at the given path with the given metadata.
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
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

    /// Writes the object only if NO redirect tombstone exists at this key.
    ///
    /// Returns [`OperationOutcome::Tombstone`] (skipping the write) when a
    /// redirect tombstone is present, or [`OperationOutcome::Executed`] after
    /// storing the object.
    ///
    /// Takes [`Bytes`] instead of a [`ClientStream`] because callers on this
    /// path have already fully buffered the payload.
    async fn put_if_not_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<OperationOutcome> {
        let existing = self.get_metadata(id).await?;
        if existing.is_some_and(|m| m.is_tombstone()) {
            Ok(OperationOutcome::Tombstone)
        } else {
            let stream = crate::stream::single(payload).boxed();
            self.put_object(id, metadata, stream).await?;
            Ok(OperationOutcome::Executed)
        }
    }

    /// Deletes the object only if it is NOT a redirect tombstone.
    ///
    /// Returns [`OperationOutcome::Tombstone`] (leaving the row intact) when
    /// the object is a redirect tombstone, or [`OperationOutcome::Executed`]
    /// (after deleting it) for regular objects and non-existent rows.
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<OperationOutcome> {
        let metadata = self.get_metadata(id).await?;
        if metadata.is_some_and(|m| m.is_tombstone()) {
            Ok(OperationOutcome::Tombstone)
        } else {
            self.delete_object(id).await?;
            Ok(OperationOutcome::Executed)
        }
    }
}

/// A type-erased [`Backend`] instance.
pub type BoxedBackend = Box<dyn Backend>;

/// Creates a reqwest client with required defaults.
///
/// Automatic decompression is disabled because backends store pre-compressed
/// payloads and manage `Content-Encoding` themselves.
pub(super) fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .no_zstd()
        .no_brotli()
        .no_gzip()
        .no_deflate()
        .build()
        .expect("Client::new()")
}
