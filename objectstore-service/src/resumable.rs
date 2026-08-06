//! Shared types for Objectstore's resumable upload protocol.
//!
//! A resumable upload is a regular write whose payload arrives across several requests.
//! A session declares the object's total size and metadata upfront; chunks then arrive at
//! increasing byte offsets, and the backend commits the object itself once the last byte
//! lands. See [`objectstore_types::resumable`] for the wire-level types.
//!
//! Not every backend can support this. Session creation therefore asks the backend that
//! would store the object to open one, and a backend that cannot declines by returning
//! `None` from
//! [`Backend::create_upload_session`](crate::backend::common::Backend::create_upload_session).
//! Declining is a routine outcome rather than an error: the server denies the session and
//! the client falls back to a regular upload.

pub use objectstore_types::resumable::{InvalidSessionToken, SessionToken, UploadOffset};

/// How far a resumable upload has progressed.
///
/// Returned by both
/// [`Backend::put_chunk`](crate::backend::common::Backend::put_chunk) and
/// [`Backend::upload_offset`](crate::backend::common::Backend::upload_offset), because an
/// offset query commits an object that was assembled but not yet committed and therefore
/// has the same two outcomes as a chunk write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadProgress {
    /// More bytes are expected. The client continues from `offset`.
    ///
    /// This offset is authoritative and may be lower than the end of the chunk that was
    /// just written: backends persist only aligned prefixes and discard the remainder.
    Incomplete {
        /// The offset the backend has persisted.
        offset: u64,
    },
    /// The last byte arrived and the object is committed and readable.
    Committed,
}

/// Response for
/// [`Backend::create_upload_session`](crate::backend::common::Backend::create_upload_session).
///
/// `None` means the backend declines resumable uploads for this object.
pub type CreateSessionResponse = Option<SessionToken>;

/// Response for
/// [`Backend::terminate_upload`](crate::backend::common::Backend::terminate_upload).
pub type TerminateUploadResponse = ();
