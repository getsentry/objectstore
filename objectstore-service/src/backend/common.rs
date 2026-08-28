//! Shared trait definition and types for all backends.

use std::fmt;

use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::resumable::{CancelUploadResponse, CreateSessionResponse, SessionToken, UploadProgress};
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

    /// Retrieves (part of) an object at the given path, returning its metadata, a description of
    /// the part being returned, and the payload.
    async fn get_object(&self, id: &ObjectId, range: Option<ByteRange>) -> Result<GetResponse>;

    /// Retrieves only the metadata for an object, without the payload.
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        Ok(self
            .get_object(id, None)
            .await?
            .map(|(metadata, _range, _stream)| metadata))
    }

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse>;

    /// Waits for any outstanding background operations to complete before shutdown.
    ///
    /// The default implementation is a no-op. Backends that spawn background tasks
    /// (such as [`TieredStorage`](super::tiered::TieredStorage)) should override this
    /// to wait for those tasks to complete.
    async fn join(&self) {}

    /// Borrows this backend as a [`MultipartUploadBackend`] if supported.
    ///
    /// The default returns [`Error::NotImplemented`]. Backends that implement
    /// [`MultipartUploadBackend`] should override this to return `Ok(self)`.
    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        Err(Error::NotImplemented)
    }

    /// Opens a resumable upload session for the object at `id`.
    ///
    /// `total_length` is the complete size of the object in bytes, declared by the client
    /// when the session is created. It is a parameter of its own rather than part of
    /// `metadata`, because [`Metadata::size`] is materialized by the server and never
    /// trusted from a client. The backend needs it to recognize the final chunk, and a
    /// tiering backend needs it to decide where the object would be placed.
    ///
    /// `metadata` is fixed for the lifetime of the session and does not change afterwards.
    /// Compression is recorded rather than applied: the payload must already be compressed,
    /// since its total length has to be known at this point.
    ///
    /// Returns `Ok(None)` when this backend cannot store the described object resumably.
    /// Declining is a routine outcome, not an error — the server denies the session and the
    /// client falls back to a regular upload. The default implementation declines, so a
    /// backend opts in simply by overriding this method. There is deliberately no separate
    /// capability trait and no probe: support can depend on the size, the metadata and the
    /// routing result at once, all of which are only known here.
    ///
    /// # Errors
    ///
    /// Returns an error only when the backend supports resumable uploads but failed to open
    /// the session.
    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        total_length: u64,
    ) -> Result<CreateSessionResponse> {
        let _ = (id, metadata, total_length);
        Ok(None)
    }

    /// Writes a chunk of `content_length` bytes at `offset` into an open session.
    ///
    /// The application protocol requires the caller to declare `content_length` before the body
    /// is consumed, including over HTTP/2. Backends may rely on it without buffering the stream.
    ///
    /// `offset` must equal the offset the backend currently holds. Backends persist only
    /// aligned prefixes and discard the remainder, so the offset in the returned
    /// [`UploadProgress::Incomplete`] is authoritative and may be lower than
    /// `offset + content_length`.
    ///
    /// A session has a single writer. Concurrent chunk writes are not coordinated: one of
    /// them wins and the others fail with [`Error::UploadOffsetMismatch`].
    ///
    /// Once the chunk carrying the last byte is persisted, the backend assembles and commits
    /// the object and returns [`UploadProgress::Committed`].
    ///
    /// # Errors
    ///
    /// - [`Error::NotImplemented`] if this backend does not support resumable uploads. The
    ///   default implementation returns this, which is unreachable through the API because a
    ///   backend that declines in [`Self::create_upload_session`] never hands out a session.
    /// - [`Error::UploadOffsetMismatch`] if `offset` is not the offset the backend holds.
    /// - [`Error::UploadSessionGone`] if the session expired or was canceled.
    /// - [`Error::InvalidUploadRequest`] if the session is unusable, or the chunk would
    ///   exceed the length declared at creation.
    async fn put_chunk(
        &self,
        id: &ObjectId,
        session: &SessionToken,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        let _ = (id, session, offset, content_length, stream);
        Err(Error::NotImplemented)
    }

    /// Reports how far the session has progressed, committing the object if it is assembled.
    ///
    /// This is the recovery path: after any failed chunk the client calls this and continues
    /// from the returned offset. It is also the only read-shaped operation that mutates state.
    /// Making an object visible can outlive the request that triggered it, so a session whose
    /// payload fully landed may still be uncommitted; this operation must finish that work. It
    /// returns [`UploadProgress::Committed`] only after the object is committed and readable, or
    /// returns an error if committing fails. It must not return [`UploadProgress::Incomplete`]
    /// with the session's total length, because the client would have no bytes left to send.
    /// Callers must therefore treat it as a write.
    ///
    /// # Errors
    ///
    /// The same conditions as [`Self::put_chunk`], except for the offset mismatch.
    async fn upload_offset(&self, id: &ObjectId, session: &SessionToken) -> Result<UploadProgress> {
        let _ = (id, session);
        Err(Error::NotImplemented)
    }

    /// Cancels an upload session, discarding whatever was uploaded.
    ///
    /// Idempotent. Not required for correctness, since sessions expire on their own, but it
    /// lets a caller release an abandoned upload immediately.
    ///
    /// # Errors
    ///
    /// - [`Error::NotImplemented`] if this backend does not support resumable uploads.
    /// - [`Error::InvalidUploadRequest`] if the session token is unusable.
    async fn cancel_upload(
        &self,
        id: &ObjectId,
        session: &SessionToken,
    ) -> Result<CancelUploadResponse> {
        let _ = (id, session);
        Err(Error::NotImplemented)
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

    /// Retrieves (part of) an object with explicit tombstone awareness.
    ///
    /// Returns [`TieredGet::Tombstone`] instead of synthesizing a tombstone
    /// object, making the caller's routing logic a compile-time distinction.
    async fn get_tiered_object(&self, id: &ObjectId, range: Option<ByteRange>)
    -> Result<TieredGet>;

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
    ) -> Result<bool>;
}

/// Information about a redirect tombstone in the high-volume backend.
#[derive(Clone, Debug, PartialEq, Eq)]
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
