//! Operation kinds shared across the server and service.
//!
//! [`OperationKind`] is the canonical vocabulary used to tag metrics by the kind
//! of operation being performed, so that the server and service emit consistent
//! tag values instead of inventing their own strings.

/// The kind of operation being performed against the object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationKind {
    /// Download an object.
    Get,
    /// Metadata-only lookup for an object.
    Head,
    /// Create or overwrite an object.
    Insert,
    /// Delete an object.
    Delete,
    /// Initiate a multipart upload.
    InitiateMultipart,
    /// Upload a single part of a multipart upload.
    UploadPart,
    /// List the parts of a multipart upload.
    ListParts,
    /// Abort a multipart upload.
    AbortMultipart,
    /// Complete a multipart upload.
    CompleteMultipart,
    /// A whole batch request (e.g. rejected at the entry gate before its
    /// individual operations are examined).
    Batch,
}

impl OperationKind {
    /// Returns the canonical string used as a metric tag value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Insert => "insert",
            Self::Delete => "delete",
            Self::InitiateMultipart => "initiate_multipart",
            Self::UploadPart => "upload_part",
            Self::ListParts => "list_parts",
            Self::AbortMultipart => "abort_multipart",
            Self::CompleteMultipart => "complete_multipart",
            Self::Batch => "batch",
        }
    }
}

impl std::fmt::Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
