//! Types shared by resumable upload clients and servers.
//!
//! A resumable upload writes one object across multiple requests. The client first creates a
//! session, declaring the object's complete size with [`HEADER_UPLOAD_LENGTH`]. The server returns
//! a [`CreateSessionResponse`] containing an opaque [`SessionToken`] that identifies the upload.
//!
//! The client then sends chunks with [`HEADER_UPLOAD_OFFSET`] set to the byte position at which
//! each chunk starts. If an upload is interrupted, the client can send the wildcard offset
//! [`UploadOffset::Unknown`] to query the server's authoritative position before resuming. The
//! request that completes the upload returns a [`CompleteUploadResponse`].
//!
//! Session tokens contain backend state protected by the storage service, and clients must treat
//! their contents as opaque. The token bytes are encoded as unpadded base64url when the token is
//! placed in a request's `session` query parameter.

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Request header declaring the total size of the object, in bytes.
///
/// Required when creating a session.
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
/// Internally, this is an opaque byte string whose contents are defined and interpreted by the
/// storage service and backend. At the HTTP API boundary it serializes as canonical unpadded
/// base64url, so the serialized value can be placed directly in a subsequent request URL.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionToken(Vec<u8>);

impl SessionToken {
    /// Wraps opaque session-token bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the opaque token bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the token and returns its opaque bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Parses the canonical unpadded-base64url representation used by the HTTP API.
    pub fn from_base64url(encoded: &str) -> Result<Self, InvalidSessionToken> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| InvalidSessionToken)?;
        if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
            return Err(InvalidSessionToken);
        }
        Ok(Self(bytes))
    }

    /// Encodes this token for the HTTP API.
    pub fn to_base64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(&self.0)
    }
}

impl fmt::Debug for SessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionToken([redacted])")
    }
}

impl Serialize for SessionToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_base64url())
    }
}

impl<'de> Deserialize<'de> for SessionToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::from_base64url(&encoded).map_err(de::Error::custom)
    }
}

/// Error returned for a non-canonical or malformed external session token.
#[derive(Debug, thiserror::Error)]
#[error("session token must use unpadded base64url encoding")]
pub struct InvalidSessionToken;

/// The value of the [`HEADER_UPLOAD_OFFSET`] request header.
///
/// In a request, a concrete offset submits a chunk starting at that byte,
/// while [`UploadOffset::Unknown`] asks the server which offset it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadOffset {
    /// Denotes a chunk whose first byte sits at this offset.
    At(u64),
    /// Used to query the server for its authoritative offset.
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

/// How far a resumable upload has progressed.
///
/// Both a chunk write and an offset query can observe that an upload is complete, so both
/// operations have the same two outcomes. Completion is relative to the backend handling the
/// operation: it means the session is terminal and the object is available through that backend's
/// normal read methods. A backend that composes another backend must finish its own publication
/// work before returning [`UploadProgress::Complete`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadProgress {
    /// More bytes are expected. The client continues from `offset`.
    ///
    /// This offset is authoritative and may be lower than the end of the chunk that was just
    /// written: backends can persist only a prefix and discard the remainder. It must remain below
    /// the session's total length; once every byte has landed, the backend completes the upload or
    /// returns an error instead.
    Incomplete {
        /// The offset the backend has persisted.
        offset: u64,
    },
    /// The session is terminal and the object is available through the backend's normal reads.
    ///
    /// This is an observable status rather than a one-time event. A later offset query can return
    /// `Complete` again, for example when the response to the final chunk was lost.
    Complete,
}

/// Response from creating a resumable upload session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    /// The object key (server-generated or client-provided).
    pub key: String,
    /// The opaque session token that identifies the session.
    pub session: SessionToken,
}

/// Response from the request that completes the upload.
///
/// This is either the chunk carrying the last byte, or an offset query against a
/// session whose final chunk completed but whose response was not observed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteUploadResponse {
    /// The object key.
    pub key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_session_response_encodes_token_once() -> Result<(), serde_json::Error> {
        let response = CreateSessionResponse {
            key: "key".into(),
            session: SessionToken::new(b"../opaque +? \xc3\xbc"),
        };

        assert_eq!(
            serde_json::to_string(&response)?,
            r#"{"key":"key","session":"Li4vb3BhcXVlICs_IMO8"}"#
        );
        Ok(())
    }

    #[test]
    fn session_token_round_trips_arbitrary_bytes() -> Result<(), serde_json::Error> {
        let token = SessionToken::new([0, 1, 2, 0xfe, 0xff]);
        let json = serde_json::to_string(&token)?;
        assert_eq!(json, r#""AAEC_v8""#);
        assert_eq!(serde_json::from_str::<SessionToken>(&json)?, token);
        Ok(())
    }

    #[test]
    fn session_token_rejects_noncanonical_encodings() {
        for invalid in ["%%%", "dG9rM24="] {
            assert!(
                SessionToken::from_base64url(invalid).is_err(),
                "accepted {invalid:?}"
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
