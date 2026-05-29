//! Types for the multipart upload protocol.

use std::fmt;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::path::{Component, Path};
use std::time::SystemTime;

use serde::{Deserialize, Deserializer, Serialize};

/// 1-indexed position of a part within its multipart upload.
pub type PartNumber = NonZeroU32;

/// Opaque entity tag identifying a specific version of an uploaded part.
pub type ETag = String;

/// Identifier for an in-progress multipart upload.
///
/// Validated on construction: non-empty and free of path-traversal components
/// (`..`, leading `/`, etc.), so it is always safe to use as a single path segment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct UploadId(String);

/// Error returned when an [`UploadId`] fails validation.
#[derive(Debug, thiserror::Error)]
#[error("invalid upload_id: {0}")]
pub struct InvalidUploadId(String);

impl UploadId {
    /// Creates a new `UploadId` after validating the input.
    pub fn new(s: String) -> Result<Self, InvalidUploadId> {
        if s.is_empty() {
            return Err(InvalidUploadId("must not be empty".into()));
        }
        for component in Path::new(&s).components() {
            if !matches!(component, Component::Normal(_)) {
                return Err(InvalidUploadId(s));
            }
        }
        Ok(Self(s))
    }

    /// Returns the upload ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
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

/// Response from initiating a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitiateResponse {
    /// The object key (server-generated or user-provided).
    pub key: String,
    /// The upload session identifier for subsequent requests.
    pub upload_id: UploadId,
}

/// Response from uploading a single part.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartResponse {
    /// Opaque identifier of the uploaded part.
    pub etag: ETag,
}

/// Information about a single uploaded part, as returned by list-parts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartInfo {
    /// The part number.
    pub part_number: PartNumber,
    /// Opaque identifier of the part.
    pub etag: ETag,
    /// When the part was last modified.
    #[serde(with = "humantime_serde")]
    pub last_modified: SystemTime,
    /// Size of the part in bytes.
    pub size: u64,
}

impl From<PartInfo> for CompletePart {
    fn from(info: PartInfo) -> Self {
        Self {
            part_number: info.part_number,
            etag: info.etag,
        }
    }
}

/// Response from listing parts of a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPartsResponse {
    /// Parts uploaded so far.
    pub parts: Vec<PartInfo>,
    /// Whether the response was truncated.
    pub is_truncated: bool,
    /// Marker for the next page of results, if truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<PartNumber>,
}

/// A single part reference used in the complete request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletePart {
    /// The part number.
    pub part_number: PartNumber,
    /// The etag returned when this part was uploaded.
    pub etag: ETag,
}

/// Request body for completing a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    /// Ordered list of all parts that make up the object.
    pub parts: Vec<CompletePart>,
}

/// Successful response from completing a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteSuccessResponse {
    /// The object key.
    pub key: String,
}

/// Detail of an error that occurred during multipart completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteErrorDetail {
    /// Error code.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

/// Error response from completing a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteErrorResponse {
    /// The error detail.
    pub error: CompleteErrorDetail,
}
