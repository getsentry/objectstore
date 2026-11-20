//! Types supporting authenticated Resumable Upload Session tokens.
//!
//! Storage backends represent their opaque upload state as a [`BackendToken`]. At the service
//! boundary, [`Session`] combines that state with service-specific fields, and
//! [`crate::encryption::Cipher`] protects the serialized token before it is returned to the server.
//! After authentication, the service passes the structured [`Session`] to the backend.
//!
//! ```text
//! Storage backend       | objectstore-service                          | objectstore-server             |
//! BackendToken <------->| Session --------------- Cipher ------------->| EncryptedSessionToken          |
//! opaque backend state  | { ObjectId, upload_length, BackendToken }    | b64url encoded opaque envelope |
//! ```

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::id::ObjectId;

pub use objectstore_types::resumable::{
    SessionToken as EncryptedSessionToken, UploadOffset, UploadProgress,
};

/// Opaque session state encoded and decoded by a storage backend.
pub type BackendToken = String;

/// Identifies a resumable upload and carries its declared length.
///
/// The service encrypts this value before returning it to clients and authenticates it before
/// passing it to backend continuation operations.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Session {
    /// The `ObjectID` this upload is tied to to.
    #[serde(with = "crate::id::serde_storage_path")]
    pub object_id: ObjectId,
    /// Total length of the upload in bytes.
    pub upload_length: NonZeroU64,
    /// Opaque session state belonging to the receiving backend.
    pub backend_token: BackendToken,
}
