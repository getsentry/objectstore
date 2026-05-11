//! Types for the multipart upload protocol.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// Identifier for a multipart upload session.
pub type UploadId = String;

/// Opaque entity tag identifying a specific version of an uploaded part.
pub type ETag = String;

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
    pub part_number: u32,
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
    pub next_part_number_marker: Option<u32>,
}

/// A single part reference used in the complete request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletePart {
    /// The part number.
    pub part_number: u32,
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
