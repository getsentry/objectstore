//! Shared types for Objectstore's multipart upload protocol.

use std::fmt;
use std::ops::Deref;
use std::path::{Component, Path};
use std::time::SystemTime;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::Error;

/// Identifier for an in-progress multipart upload.
///
/// Validated on construction: non-empty and free of path-traversal components
/// (`..`, leading `/`, etc.), so it is always safe to use as a single path segment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct UploadId(String);

impl UploadId {
    /// Returns the upload ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Creates a new `UploadId` after validating the input.
    pub fn new(s: String) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::generic("upload_id must not be empty"));
        }
        for component in Path::new(&s).components() {
            if !matches!(component, Component::Normal(_)) {
                return Err(Error::generic(format!("invalid upload_id: {s}")));
            }
        }
        Ok(Self(s))
    }
}

impl Deref for UploadId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> Deserialize<'de> for UploadId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}
/// 1-indexed position of a part within its multipart upload.
pub type PartNumber = std::num::NonZeroU32;
/// Opaque per-part identifier returned by the backend after a successful part upload.
pub type ETag = String;

/// Description of one part in the response to
/// [`MultipartUploadBackend::list_parts`](crate::backend::common::MultipartUploadBackend::list_parts).
#[derive(Clone, Debug)]
pub struct Part {
    /// 1-indexed position of this part within the upload.
    pub part_number: PartNumber,
    /// Identifier returned when the part was uploaded.
    pub etag: ETag,
    /// Server-recorded time at which the part was uploaded.
    pub last_modified: SystemTime,
    /// Size of the part in bytes.
    pub size: u64,
}

/// Pair of (part number, ETag) that the client provides on
/// [`MultipartUploadBackend::complete_multipart`](crate::backend::common::MultipartUploadBackend::complete_multipart)
/// to identify the parts in order.
#[derive(Clone, Debug)]
pub struct CompletedPart {
    /// 1-indexed position of this part within the upload.
    pub part_number: PartNumber,
    /// Identifier returned when the part was uploaded.
    pub etag: ETag,
}

/// Error optionally returned by
/// [`MultipartUploadBackend::complete_multipart`](crate::backend::common::MultipartUploadBackend::complete_multipart).
#[derive(Clone, Debug)]
pub struct CompleteMultipartError {
    /// Error code or identifier.
    pub code: String,
    /// Human-readable error description.
    pub message: String,
}

/// Response for
/// [`MultipartUploadBackend::initiate_multipart`](crate::backend::common::MultipartUploadBackend::initiate_multipart).
pub type InitiateMultipartResponse = UploadId;

/// Response for
/// [`MultipartUploadBackend::upload_part`](crate::backend::common::MultipartUploadBackend::upload_part).
pub type UploadPartResponse = ETag;

/// Response for
/// [`MultipartUploadBackend::list_parts`](crate::backend::common::MultipartUploadBackend::list_parts).
#[derive(Clone, Debug)]
pub struct ListPartsResponse {
    /// Parts uploaded so far, in `part_number` order.
    pub parts: Vec<Part>,
    /// Set when the listing was truncated and more parts can be fetched
    /// using [`Self::next_part_number_marker`] as the next
    /// `part_number_marker`.
    pub is_truncated: bool,
    /// Marker to pass as the next `part_number_marker` when
    /// [`Self::is_truncated`] is `true`.
    pub next_part_number_marker: Option<PartNumber>,
}

/// Response for
/// [`MultipartUploadBackend::abort_multipart`](crate::backend::common::MultipartUploadBackend::abort_multipart).
pub type AbortMultipartResponse = ();

/// Response for
/// [`MultipartUploadBackend::complete_multipart`](crate::backend::common::MultipartUploadBackend::complete_multipart).
pub type CompleteMultipartResponse = Option<CompleteMultipartError>;
