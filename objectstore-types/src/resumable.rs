//! Types shared by resumable upload clients and servers.
//!
//! A resumable upload writes one object across multiple requests. The client first creates a
//! session, declaring the object's complete size with [`HEADER_UPLOAD_LENGTH`]. The server returns
//! a [`CreateSessionResponse`] containing an opaque [`SessionToken`] that identifies the upload.
//!
//! The client then sends chunks with [`HEADER_UPLOAD_OFFSET`] set to the byte position at which
//! each chunk starts. If an upload is interrupted, the client can send the wildcard offset
//! [`UploadOffset::Unknown`] to query the server's authoritative position before resuming. The
//! request that completes the object returns a [`CommitResponse`].
//!
//! Session tokens are defined by the storage backend and clients must treat their contents as
//! opaque. The token's UTF-8 bytes are encoded as unpadded base64url when the token is placed in a
//! request's `session` query parameter.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

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
/// The token is an opaque identifier whose contents are defined and interpreted by the storage
/// backend.
pub type SessionToken = String;

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

/// Response from creating a resumable upload session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    /// The object key (server-generated or client-provided).
    pub key: String,
    /// The opaque session token that identifies the session.
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
    fn create_session_response_serializes_token_verbatim() -> Result<(), serde_json::Error> {
        let response = CreateSessionResponse {
            key: "key".into(),
            session: "../opaque +? ü".into(),
        };

        assert_eq!(
            serde_json::to_string(&response)?,
            r#"{"key":"key","session":"../opaque +? ü"}"#
        );
        Ok(())
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
