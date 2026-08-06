//! Types for the resumable upload protocol.
//!
//! A resumable upload declares the object's total size and metadata upfront, then
//! sends the payload as a sequence of chunks at increasing byte offsets. If a chunk
//! fails, the client asks the server which offset it holds and continues from there.
//! There is no finalize request: the server knows the total length from the session,
//! so it recognizes the chunk carrying the last byte and commits the object itself.
//!
//! Every request addresses the regular object endpoints with the session in the query
//! string. Header names are borrowed from [TUS] where they fit, but this is not a TUS
//! implementation: there is no version negotiation, no capability discovery, and no
//! support for uploads of unknown length.
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
use std::path::{Component, Path};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

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
/// The token is opaque to the client: it is minted by the storage backend and carries
/// whatever that backend needs to continue or commit the upload without shared state.
/// It is neither signed nor encrypted, which is safe because the usecase, scopes and
/// key travel in the request path rather than in the token, so a request cannot address
/// an object other than the one it names.
///
/// Validated on construction: non-empty and free of path-traversal components (`..`,
/// leading `/`, etc.), so a backend can safely use it as a single path segment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SessionToken(String);

/// Error returned when a [`SessionToken`] fails validation.
#[derive(Debug, thiserror::Error)]
#[error("invalid session token: {0}")]
pub struct InvalidSessionToken(String);

impl SessionToken {
    /// Creates a new `SessionToken` after validating the input.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidSessionToken`] if the string is empty or contains a component
    /// that is not a plain path segment.
    pub fn new(s: String) -> Result<Self, InvalidSessionToken> {
        if s.is_empty() {
            return Err(InvalidSessionToken("must not be empty".into()));
        }
        for component in Path::new(&s).components() {
            if !matches!(component, Component::Normal(_)) {
                return Err(InvalidSessionToken(s));
            }
        }
        Ok(Self(s))
    }

    /// Returns the session token as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
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

impl<'de> Deserialize<'de> for SessionToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
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
    /// The session token for subsequent requests.
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
    fn session_token_accepts_opaque_values() -> Result<(), InvalidSessionToken> {
        assert_eq!(SessionToken::new("abc123".into())?.as_str(), "abc123");
        assert_eq!(
            SessionToken::new("eyJyZXZpc2lvbiI6ImEifQ".into())?.as_str(),
            "eyJyZXZpc2lvbiI6ImEifQ"
        );
        Ok(())
    }

    #[test]
    fn session_token_rejects_empty_and_traversal() {
        for invalid in ["", "..", "/abs", "a/../b", "./a"] {
            assert!(
                SessionToken::new(invalid.into()).is_err(),
                "expected {invalid:?} to be rejected"
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
