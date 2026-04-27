//! Shared trait definition and types for all backends.

use std::fmt;

use objectstore_types::metadata::{ExpirationPolicy, Metadata};

use bytes::Bytes;

use crate::error::Result;
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
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

/// Trait implemented by all storage backends.
#[async_trait::async_trait]
pub trait Backend: fmt::Debug + Send + Sync + 'static {
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

    /// Waits for any outstanding background operations to complete before shutdown.
    ///
    /// The default implementation is a no-op. Backends that spawn background tasks
    /// (such as [`TieredStorage`](super::tiered::TieredStorage)) should override this
    /// to wait for those tasks to complete.
    async fn join(&self) {}
}

/// Trait for backends that support our S3-style multipart upload protocol.
#[async_trait::async_trait]
pub trait MultipartUploadBackend: Backend + fmt::Debug + Send + Sync + 'static {
    /// Initiates a new multipart upload at `id` with the given metadata.
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse>;

    /// Uploads a single part of the upload identified by `(id, upload_id)`.
    async fn upload_part(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        part_number: PartNumber,
        content_length: u64,
        body: ClientStream,
    ) -> Result<UploadPartResponse>;

    /// Lists the parts uploaded so far for `(id, upload_id)`.
    async fn list_parts(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse>;

    /// Aborts the upload identified by `(id, upload_id)`.
    async fn abort_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartResponse>;

    /// Finalizes the upload identified by `(id, upload_id)` with the given
    /// ordered list of parts.
    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartResponse>;
}

/// Trait for backends that support tombstone-conditional operations.
///
/// Only backends suitable for the high-volume tier of
/// [`TieredStorage`](super::tiered::TieredStorage) implement this trait.
/// The conditional methods provide atomic operations to avoid overwriting
/// redirect tombstones.
#[async_trait::async_trait]
pub trait HighVolumeBackend: Backend {
    /// Writes the object only if NO redirect tombstone exists at this key.
    ///
    /// Returns `None` after storing the object, or `Some(tombstone)` (skipping
    /// the write) when a redirect tombstone is present. The returned tombstone
    /// carries the target LT `ObjectId` so the caller can route without a
    /// second round trip.
    ///
    /// Takes [`Bytes`] instead of a [`ClientStream`] because callers on this
    /// path have already fully buffered the payload.
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<Option<Tombstone>>;

    /// Retrieves an object with explicit tombstone awareness.
    ///
    /// Returns [`TieredGet::Tombstone`] instead of synthesizing a tombstone
    /// object, making the caller's routing logic a compile-time distinction.
    async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet>;

    /// Retrieves only metadata with explicit tombstone awareness.
    ///
    /// Implementations should skip the payload column where possible to avoid
    /// fetching up to 1 MiB of data just to discover a tombstone.
    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata>;

    /// Deletes the object only if it is NOT a redirect tombstone.
    ///
    /// Returns `None` after deleting the row (or if the row was already absent),
    /// or `Some(tombstone)` (leaving the row intact) when the object is a
    /// redirect tombstone. The returned tombstone carries the target LT
    /// `ObjectId` so the caller can delete from long-term storage directly,
    /// without a second round trip.
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>>;

    /// Atomically mutates the row if the current redirect state matches.
    ///
    /// `current` determines the precondition:
    /// - `None`: succeeds only if no tombstone exists (row absent or inline).
    /// - `Some(target)`: succeeds only if a tombstone exists whose redirect
    ///   resolves to `target`.
    ///
    /// **This operation is idempotent:** if the object is already in the target
    /// state, it returns `true`. Whether the mutation runs again is up to the
    /// implementation.
    ///
    /// Returns `true` on success or idempotent match, `false` if a conflicting
    /// state was found (another writer won the race).
    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
    ) -> Result<bool>;
}

/// Information about a redirect tombstone in the high-volume backend.
#[derive(Clone, Debug, PartialEq)]
pub struct Tombstone {
    /// The [`ObjectId`] of the object in the long-term backend.
    ///
    /// For legacy tombstones with an empty `r` column, the HV backend resolves
    /// this to the HV `ObjectId` itself before surfacing the tombstone to callers.
    pub target: ObjectId,

    /// The expiration policy copied from the original object.
    pub expiration_policy: ExpirationPolicy,
}

/// Typed response from [`HighVolumeBackend::get_tiered_object`].
pub enum TieredGet {
    /// A real object was found.
    Object(Metadata, PayloadStream),
    /// A redirect tombstone was found; the real object lives in the long-term backend.
    Tombstone(Tombstone),
    /// No entry exists at this key.
    NotFound,
}

impl fmt::Debug for TieredGet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TieredGet::Object(metadata, _stream) => f
                .debug_tuple("Object")
                .field(metadata)
                .finish_non_exhaustive(),
            TieredGet::Tombstone(info) => f.debug_tuple("Tombstone").field(info).finish(),
            TieredGet::NotFound => write!(f, "NotFound"),
        }
    }
}

/// Typed metadata-only response from [`HighVolumeBackend::get_tiered_metadata`].
#[derive(Debug)]
pub enum TieredMetadata {
    /// Metadata for a real object was found.
    Object(Metadata),
    /// A redirect tombstone was found; the real object lives in the long-term backend.
    Tombstone(Tombstone),
    /// No entry exists at this key.
    NotFound,
}

/// The write operation performed by [`HighVolumeBackend::compare_and_write`].
#[derive(Clone, Debug)]
pub enum TieredWrite {
    /// Write a redirect tombstone.
    Tombstone(Tombstone),
    /// Write inline object data.
    Object(Metadata, Bytes),
    /// Delete the row entirely.
    Delete,
}

impl TieredWrite {
    /// Returns the tombstone target if this is a tombstone write, or `None` otherwise.
    pub fn target(&self) -> Option<&ObjectId> {
        match self {
            TieredWrite::Tombstone(t) => Some(&t.target),
            _ => None,
        }
    }
}

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
