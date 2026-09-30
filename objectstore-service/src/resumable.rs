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

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::id::ObjectId;

pub use objectstore_types::resumable::{
    SessionToken as EncryptedSessionToken, UploadOffset, UploadProgress,
};

/// Opaque session state encoded and decoded by a storage backend.
pub type BackendToken = String;

/// Identifies a resumable upload and carries its declared length.
///
/// The service encrypts this value before returning it to clients and authenticates it before
/// passing it to backend continuation operations. Composed backends can derive an inner session
/// by replacing the object ID and backend token while retaining the shared upload information.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Session {
    /// Object being uploaded through the receiving backend.
    #[serde(
        serialize_with = "serialize_object_id",
        deserialize_with = "deserialize_object_id"
    )]
    pub object_id: ObjectId,
    /// Total length declared when the upload was created.
    pub upload_length: NonZeroU64,
    /// Opaque session state belonging to the receiving backend.
    pub backend_token: BackendToken,
}

fn serialize_object_id<S>(id: &ObjectId, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(&id.as_storage_path())
}

fn deserialize_object_id<'de, D>(deserializer: D) -> std::result::Result<ObjectId, D::Error>
where
    D: Deserializer<'de>,
{
    let path = String::deserialize(deserializer)?;
    ObjectId::from_storage_path(&path)
        .ok_or_else(|| de::Error::custom("invalid object storage path"))
}
