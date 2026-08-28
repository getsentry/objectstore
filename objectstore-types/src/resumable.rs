//! Types for the resumable upload protocol.
//!
//! A resumable upload declares the object's total size and metadata upfront, then
//! sends the payload as a sequence of chunks at increasing byte offsets. If a chunk
//! fails, the client asks the server which offset it holds and continues from there.
//! There is no finalize request: the server knows the total length from the session,
//! so it recognizes the chunk carrying the last byte and commits the object itself.
//!
//! Every request addresses the regular object endpoints with the session in the query
//! string. [`SessionToken`] serializes as unpadded base64url at that API boundary. Header
//! names are borrowed from [TUS] where they fit, but this is not a TUS implementation: there
//! is no version negotiation, no capability discovery, and no support for uploads of unknown
//! length.
//!
//! Key types:
//! - [`SessionToken`] — opaque identifier for an in-progress upload session.
//! - [`UploadOffset`] — the value of the [`HEADER_UPLOAD_OFFSET`] header.
//! - [`CreateSessionResponse`] — returned when a new session is created.
//! - [`CommitResponse`] — returned by the request that commits the object.
//!
//! [TUS]: https://tus.io/protocols/resumable-upload

use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Request header declaring the total size of the object, in bytes.
///
/// Required when creating a session. The server needs the total size to select a
/// backend and to recognize the final chunk.
pub const HEADER_UPLOAD_LENGTH: &str = "upload-length";

/// Header carrying the byte offset of a chunk, or the offset the server holds.
///
/// On a request this is the offset of the chunk's first byte, or `*` to query the
/// server's authoritative offset. On a response it is the offset the server has
/// persisted. See [`UploadOffset`].
pub const HEADER_UPLOAD_OFFSET: &str = "upload-offset";

/// The wildcard [`HEADER_UPLOAD_OFFSET`] value that queries the server's offset.
const OFFSET_WILDCARD: &str = "*";

/// Identifier for an in-progress resumable upload session.
///
/// The token is an opaque identifier whose contents are defined and interpreted by the storage
/// backend. At the API boundary it is serialized as unpadded base64url, keeping the opaque value
/// out of URL parsing and escaping rules.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionToken(String);

impl SessionToken {
    /// Returns the session token as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SessionToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for SessionToken {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for SessionToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(self.0.as_bytes()))
    }
}

impl<'de> Deserialize<'de> for SessionToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(&encoded)
            .map_err(serde::de::Error::custom)?;
        if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
            return Err(serde::de::Error::custom(
                "session token must use canonical unpadded base64url",
            ));
        }

        let token = String::from_utf8(bytes).map_err(serde::de::Error::custom)?;
        Ok(Self(token))
    }
}

/// The value of the [`HEADER_UPLOAD_OFFSET`] request header.
///
/// A concrete offset submits a chunk starting at that byte. The wildcard `*` submits
/// no payload and instead asks the server which offset it holds, which is also the
/// request that commits an object that was assembled but not yet committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadOffset {
    /// `Upload-Offset: <n>` — a chunk whose first byte sits at this offset.
    At(u64),
    /// `Upload-Offset: *` — a query for the server's authoritative offset.
    Unknown,
}

/// Error returned when an [`UploadOffset`] header value cannot be parsed.
#[derive(Debug, thiserror::Error)]
#[error("invalid {HEADER_UPLOAD_OFFSET} value: {0}")]
pub struct InvalidUploadOffset(String);

impl FromStr for UploadOffset {
    type Err = InvalidUploadOffset;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == OFFSET_WILDCARD {
            return Ok(Self::Unknown);
        }

        // Rejects the `+` sign and leading whitespace that `u64::from_str` would
        // otherwise be lenient about, keeping the header canonical.
        if !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(InvalidUploadOffset(s.to_owned()));
        }

        let offset = s.parse().map_err(|_| InvalidUploadOffset(s.to_owned()))?;
        Ok(Self::At(offset))
    }
}

impl fmt::Display for UploadOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::At(offset) => offset.fmt(f),
            Self::Unknown => f.write_str(OFFSET_WILDCARD),
        }
    }
}

/// Response from creating a resumable upload session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    /// The object key (server-generated or client-provided).
    pub key: String,
    /// The session token for subsequent requests, serialized as unpadded base64url.
    pub session: SessionToken,
}

/// Response from the request that commits the object.
///
/// This is either the chunk carrying the last byte, or an offset query against a
/// session whose object was already assembled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {
    /// The object key.
    pub key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_token_serializes_as_unpadded_base64url() -> Result<(), Box<dyn std::error::Error>> {
        let token = SessionToken::from("tok3n".to_owned());
        assert_eq!(serde_json::to_string(&token)?, r#""dG9rM24""#);

        let decoded: SessionToken = serde_json::from_str(r#""dG9rM24""#)?;
        assert_eq!(decoded, token);

        let opaque = SessionToken::from("../escape".to_owned());
        assert_eq!(serde_json::to_string(&opaque)?, r#""Li4vZXNjYXBl""#);
        let decoded: SessionToken = serde_json::from_str(r#""Li4vZXNjYXBl""#)?;
        assert_eq!(decoded, opaque);
        Ok(())
    }

    #[test]
    fn session_token_rejects_invalid_api_encodings() {
        for invalid in [r#""%%%""#, r#""dG9rM24=""#, r#""_w""#] {
            assert!(
                serde_json::from_str::<SessionToken>(invalid).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn upload_offset_parses_wildcard_and_offsets() -> Result<(), InvalidUploadOffset> {
        assert_eq!("*".parse::<UploadOffset>()?, UploadOffset::Unknown);
        assert_eq!("0".parse::<UploadOffset>()?, UploadOffset::At(0));
        assert_eq!("262144".parse::<UploadOffset>()?, UploadOffset::At(262144));
        Ok(())
    }

    #[test]
    fn upload_offset_rejects_malformed_values() {
        for invalid in ["", "-1", "+1", " 1", "1 ", "1.5", "0x10", "**", "abc"] {
            assert!(
                invalid.parse::<UploadOffset>().is_err(),
                "expected {invalid:?} to be rejected"
            );
        }
    }

    #[test]
    fn upload_offset_round_trips_through_display() -> Result<(), InvalidUploadOffset> {
        for offset in [
            UploadOffset::Unknown,
            UploadOffset::At(0),
            UploadOffset::At(7),
        ] {
            assert_eq!(offset.to_string().parse::<UploadOffset>()?, offset);
        }
        Ok(())
    }
}
