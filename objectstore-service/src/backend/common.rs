//! Shared trait definition and types for all backends.

use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use objectstore_types::metadata::Metadata;
use objectstore_types::range::{ByteRange, ContentRange};
use objectstore_types::resumable::UploadProgress;
use objectstore_types::time::Timestamp;

use bytes::Bytes;

use crate::error::{ErrorKind, Result};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::resumable::BackendToken;
use crate::stream::{ClientStream, PayloadStream};

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// Backend response for put operations.
pub type PutResponse = ();
/// Backend response for get operations.
pub type GetResponse = Option<(Metadata, Option<ContentRange>, PayloadStream)>;
/// Backend response for metadata-only get operations.
pub type MetadataResponse = Option<Metadata>;
/// Backend response for delete operations.
pub type DeleteResponse = ();

/// The outcome of an expiry update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetExpiryResponse {
    /// The deadline was extended or already satisfied the request.
    ///
    /// Contains the resolved requested deadline, not necessarily the stored deadline.
    Satisfied(Timestamp),
    /// The object or redirect was observed to be absent or expired.
    NotFound,
    /// The update could not be satisfied.
    ///
    /// The entry is non-expiring, lacks required creation metadata, or conflicts
    /// with the conditional update. A failed conditional write does not establish
    /// absence, even if a concurrent deletion caused it to fail.
    Rejected,
}

/// The requested minimum deadline for an expiry update.
///
/// [`ExpiryTarget::At`] is already resolved. [`ExpiryTarget::FromCreation`]
/// is resolved by the backend from the creation time read by the update
/// operation itself. Zero durations and resolved deadlines in the past remain
/// valid minimum-deadline requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryTarget {
    /// An absolute deadline.
    At(Timestamp),
    /// A deadline relative to the object's creation time.
    FromCreation(Duration),
}

impl ExpiryTarget {
    /// Resolves this target against an optional creation time.
    ///
    /// Returns `None` when a creation-relative target has no creation time.
    /// Creation-relative deadlines use the timestamp's existing rounding and
    /// clamp to its maximum value on overflow.
    pub fn resolve(self, time_created: Option<Timestamp>) -> Option<Timestamp> {
        match self {
            Self::At(deadline) => Some(deadline),
            Self::FromCreation(duration) => {
                time_created.map(|created| created.saturating_add(duration))
            }
        }
    }
}

/// Trait implemented by all storage backends.
///
/// Object operations take `access_time`, the timestamp of the caller's operation.
/// Use it to decide whether an object or redirect has expired, so all steps of
/// an operation use the same time, including retries and calls to other backends.
/// An object is expired when its deadline is strictly earlier than `access_time`.
/// Writes preserve the creation time and deadline in the supplied metadata.
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
        access_time: Timestamp,
    ) -> Result<PutResponse>;

    /// Retrieves (part of) an object at the given path, returning its metadata, a description of
    /// the part being returned, and the payload.
    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse>;

    /// Retrieves only the metadata for an object, without the payload.
    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        Ok(self
            .get_object(id, access_time, None)
            .await?
            .map(|(metadata, _range, _stream)| metadata))
    }

    /// Extends the deadline of an existing object with expiration policy.
    ///
    /// This only changes the stored deadline: the expiration policy, duration,
    /// payload, and all other metadata remain unchanged.
    ///
    /// Returns [`SetExpiryResponse::Satisfied`] when extended or already satisfied,
    /// [`SetExpiryResponse::NotFound`] when observed absent or expired, or
    /// [`SetExpiryResponse::Rejected`] when ineligible or conflicting.
    /// Backend failures are returned as errors.
    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryTarget,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse>;

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId, access_time: Timestamp) -> Result<DeleteResponse>;

    /// Waits for any outstanding background operations to complete before shutdown.
    ///
    /// The default implementation is a no-op. Backends that spawn background tasks
    /// (such as [`TieredStorage`](super::tiered::TieredStorage)) should override this
    /// to wait for those tasks to complete.
    async fn join(&self) {}

    /// Borrows this backend as a [`MultipartUploadBackend`] if supported.
    ///
    /// The default returns an [`ErrorKind::Unsupported`]. Backends that implement
    /// [`MultipartUploadBackend`] should override this to return `Ok(self)`.
    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        Err(ErrorKind::Unsupported.into())
    }

    /// Creates a resumable upload session for the object at `id`.
    ///
    /// Object metadata and its total length are declared upfront and cannot be mutated
    /// during the upload.
    ///
    /// The returned string is opaque backend-defined state. [`StorageService`](crate::StorageService)
    /// protects it before exposing the session token outside the service layer.
    ///
    /// Returns `Ok(None)` when this backend cannot store the described object resumably. Declining
    /// is a routine outcome rather than an error, and the default implementation declines.
    ///
    /// # Errors
    ///
    /// Returns an error only when the backend supports resumable uploads but failed to open the
    /// session.
    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        total_length: NonZeroU64,
    ) -> Result<Option<BackendToken>> {
        let _ = (id, metadata, total_length);
        Ok(None)
    }

    /// Writes a chunk of `content_length` bytes at `offset` into an open session.
    ///
    /// A backend may acknowledge fewer bytes than the chunk supplied, for example by persisting
    /// only an aligned prefix. Callers must continue from the authoritative offset in the returned
    /// [`UploadProgress`], or query [`Self::upload_offset`] after an ambiguous failure. A backend
    /// may or may not accept a replay starting before its persisted offset.
    ///
    /// [`UploadProgress::Complete`] means the upload is terminal and the object is available
    /// through this backend's normal read methods. A backend that composes another backend must
    /// finish its own publication work before returning that outcome.
    ///
    /// A `content_length` of zero is valid. It writes nothing and reports the offset the backend
    /// holds.
    ///
    /// Returns [`ErrorKind::UnknownUploadSession`] when `token` does not identify an open session,
    /// and [`ErrorKind::ChunkExceedsUploadLength`] when the chunk would exceed the total length
    /// declared when the session was created.
    async fn put_chunk(
        &self,
        id: &ObjectId,
        token: &BackendToken,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        let _ = (id, token, offset, content_length, stream);
        Err(ErrorKind::Unsupported.into())
    }

    /// Reports how far the session has progressed.
    ///
    /// This can return [`UploadProgress::Complete`] repeatedly after the final chunk, including
    /// when its original response was lost. A composed backend may finish pending idempotent
    /// publication work before returning that terminal outcome.
    ///
    /// Returns [`ErrorKind::UnknownUploadSession`] when `token` does not identify a known session.
    async fn upload_offset(&self, id: &ObjectId, token: &BackendToken) -> Result<UploadProgress> {
        let _ = (id, token);
        Err(ErrorKind::Unsupported.into())
    }

    /// Cancels an upload session, discarding whatever was uploaded.
    ///
    /// Returns [`ErrorKind::UnknownUploadSession`] when `token` does not identify an open session.
    async fn cancel_upload(&self, id: &ObjectId, token: &BackendToken) -> Result<()> {
        let _ = (id, token);
        Err(ErrorKind::Unsupported.into())
    }
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
        content_md5: Option<&str>,
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
    ///
    /// Note that this returns `Result<Option<CompleteMultipartError>>`.
    /// It's therefore possible to get `Ok(Some(err))`, meaning that at the server level this will
    /// translate to HTTP `200 OK` with an error contained in the response body.
    /// We need to do it this way to mirror backends that also behave like this (namely S3 and
    /// GCS).
    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
        access_time: Timestamp,
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
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>>;

    /// Retrieves (part of) an object with explicit tombstone awareness.
    ///
    /// Returns [`TieredGet::Tombstone`] instead of synthesizing a tombstone
    /// object, making the caller's routing logic a compile-time distinction.
    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<TieredGet>;

    /// Retrieves only metadata with explicit tombstone awareness.
    ///
    /// Implementations should skip the payload column where possible to avoid
    /// fetching up to 1 MiB of data just to discover a tombstone.
    async fn get_tiered_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<TieredMetadata>;

    /// Deletes the object only if it is NOT a redirect tombstone.
    ///
    /// Returns `None` after deleting the row (or if the row was already absent),
    /// or `Some(tombstone)` (leaving the row intact) when the object is a
    /// redirect tombstone. The returned tombstone carries the target LT
    /// `ObjectId` so the caller can delete from long-term storage directly,
    /// without a second round trip.
    async fn delete_non_tombstone(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>>;

    /// Atomically mutates the row if the current redirect state matches.
    ///
    /// `current` determines the precondition:
    /// - `None`: succeeds only if no live tombstone exists (row absent, inline,
    ///   or tombstone present but logically expired).
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
        access_time: Timestamp,
    ) -> Result<bool>;

    /// Atomically updates an existing row if its kind and redirect target match.
    ///
    /// `current = None` requires a live inline object. `Some(target)` requires
    /// a live redirect to exactly that target. Updates never authorize creation
    /// of an absent row.
    ///
    /// Returns [`SetExpiryResponse::Satisfied`] when applied or already satisfied,
    /// [`SetExpiryResponse::NotFound`] when observed absent or expired, or
    /// [`SetExpiryResponse::Rejected`] for an ineligible entry or failed condition.
    /// Redirects can only resolve absolute targets because tombstones do not store
    /// creation time. Backend failures are returned as errors.
    async fn compare_and_update(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        update: TieredUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse>;
}

/// Information about a redirect tombstone in the high-volume backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tombstone {
    /// The [`ObjectId`] of the object in the long-term backend.
    ///
    /// For legacy tombstones with an empty `r` column, the HV backend resolves
    /// this to the HV `ObjectId` itself before surfacing the tombstone to callers.
    pub target: ObjectId,

    /// The concrete deadline stored on the redirect.
    pub time_expires: Option<Timestamp>,
}

impl Tombstone {
    /// Returns whether the tombstone has expired at the given time.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.time_expires.is_some_and(|deadline| deadline < now)
    }
}

/// Typed response from [`HighVolumeBackend::get_tiered_object`].
pub enum TieredGet {
    /// A real object was found.
    Object(Metadata, Option<ContentRange>, PayloadStream),
    /// A redirect tombstone was found; the real object lives in the long-term backend.
    Tombstone(Tombstone),
    /// No entry exists at this key.
    NotFound,
}

impl fmt::Debug for TieredGet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TieredGet::Object(metadata, content_range, _stream) => f
                .debug_tuple("Object")
                .field(metadata)
                .field(content_range)
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

/// The in-place operation performed by [`HighVolumeBackend::compare_and_update`].
#[derive(Clone, Debug)]
pub enum TieredUpdate {
    /// Extend the deadline while preserving all other stored data.
    SetExpiry(ExpiryTarget),
}

/// Creates a reqwest client with required defaults.
///
/// Automatic decompression is disabled because backends store pre-compressed
/// payloads and manage `Content-Encoding` themselves.
pub(super) fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .http1_only()
        .no_zstd()
        .no_brotli()
        .no_gzip()
        .no_deflate()
        .build()
        // INVARIANT: Building fails only if the TLS backend cannot be initialized, which
        // is checked at startup when the rustls crypto provider is installed.
        .expect("failed to build backend HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_target_resolution() {
        let created = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        assert_eq!(
            ExpiryTarget::FromCreation(Duration::ZERO).resolve(Some(created)),
            Some(created)
        );
        assert_eq!(ExpiryTarget::At(created).resolve(None), Some(created));
        assert_eq!(
            ExpiryTarget::FromCreation(Duration::ZERO).resolve(None),
            None
        );
        let max = Timestamp::from_unix_secs(253_402_300_799).unwrap();
        assert_eq!(
            ExpiryTarget::FromCreation(Duration::from_secs(1)).resolve(Some(max)),
            Some(max)
        );
    }
}
