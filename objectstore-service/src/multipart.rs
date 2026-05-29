//! Shared types for Objectstore's multipart upload protocol.

use std::time::SystemTime;

pub use objectstore_types::multipart::{ETag, InvalidUploadId, PartNumber, UploadId};

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
