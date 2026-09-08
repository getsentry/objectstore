//! Types supporting authenticated Resumable Upload Session tokens.
//!
//! Storage backends represent their opaque upload state as a [`BackendToken`]. At the service
//! boundary, `SessionToken` combines that state with service-specific fields, and
//! [`crate::encryption::Cipher`] protects the serialized token before it is returned to the server.
//!
//! ```text
//! Storage backend       | objectstore-service                         | objectstore-server             |
//! BackendToken <------->| SessionToken ---------- Cipher ------------>| EncryptedSessionToken          |
//! opaque backend state  | { ObjectId, BackendToken }                  | b64url encoded opaque envelope |
//! ```

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::id::ObjectId;

pub use objectstore_types::resumable::{
    SessionToken as EncryptedSessionToken, UploadOffset, UploadProgress,
};

/// Opaque session state encoded and decoded by a storage backend.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct BackendToken(String);

impl BackendToken {
    /// Creates an opaque backend token from its encoded representation.
    pub fn new(token: String) -> Self {
        Self(token)
    }

    /// Returns the encoded token representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Structured token encrypted at the service boundary.
#[derive(Deserialize, Serialize)]
pub(crate) struct SessionToken {
    #[serde(
        serialize_with = "serialize_object_id",
        deserialize_with = "deserialize_object_id"
    )]
    pub(crate) object_id: ObjectId,
    pub(crate) backend_token: BackendToken,
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
