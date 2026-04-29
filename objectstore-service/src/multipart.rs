//! Shared types for Objectstore's multipart upload protocol.

use std::time::SystemTime;

/// Identifier for an in-progress multipart upload.
pub type UploadId = String;
/// 1-indexed position of a part within its multipart upload.
pub type PartNumber = u32;
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

/// Response from
/// [`MultipartUploadBackend::list_parts`](crate::backend::common::MultipartUploadBackend::list_parts).
#[derive(Clone, Debug)]
pub struct ListedParts {
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

/// Backend response for
/// [`MultipartUploadBackend::initiate_multipart`](crate::backend::common::MultipartUploadBackend::initiate_multipart).
pub type InitiateMultipartResponse = UploadId;
/// Backend response for
/// [`MultipartUploadBackend::upload_part`](crate::backend::common::MultipartUploadBackend::upload_part).
pub type UploadPartResponse = ETag;
/// Backend response for
/// [`MultipartUploadBackend::list_parts`](crate::backend::common::MultipartUploadBackend::list_parts).
pub type ListPartsResponse = ListedParts;
/// Backend response for
/// [`MultipartUploadBackend::abort_multipart`](crate::backend::common::MultipartUploadBackend::abort_multipart).
pub type AbortMultipartResponse = ();
/// Backend response for
/// [`MultipartUploadBackend::complete_multipart`](crate::backend::common::MultipartUploadBackend::complete_multipart).
///
/// S3-compatible APIs can return HTTP 200 with an error body on complete-multipart.
/// This struct mirrors that behavior so callers can inspect the error without the
/// backend having to treat it as a hard failure.
#[derive(Clone, Debug)]
pub struct CompleteMultipartResponse {
    /// An error embedded in the 200 response body, if present.
    pub error: Option<CompleteMultipartError>,
}

/// An error embedded in a 200 response from a complete-multipart request.
#[derive(Clone, Debug)]
pub struct CompleteMultipartError {
    /// S3/GCS error code (e.g. `InternalError`).
    pub code: String,
    /// Human-readable error description.
    pub message: String,
}
