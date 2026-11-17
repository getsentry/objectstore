//! Wire types of the JSON layout of the batch endpoint.
//!
//! In this layout, the operation at position `i` of a request is described by a [`Descriptor`]
//! part named `"<i>"`. The server answers with a [`ResultPart`] named `"<i>"` per operation, in
//! completion order. A part named `"<i>.payload"` carries the object bytes of an insert in a
//! request, or of a successful get in a response, and directly follows the part it belongs to.

use serde::{Deserialize, Serialize};

use crate::api::ApiErrorResponse;
use crate::metadata::{ReadMetadata, WriteMetadata};

/// Maximum number of operations in one batch request.
pub const MAX_BATCH_OPERATIONS: usize = 1000;

/// Maximum size in bytes of the body of a part that carries object bytes.
pub const MAX_BATCH_PART_SIZE: usize = 1024 * 1024;

/// Maximum size in bytes of the body of a [`Descriptor`] part.
pub const MAX_DESCRIPTOR_SIZE: usize = 64 * 1024;

/// Suffix that marks a payload part.
pub const PAYLOAD_SUFFIX: &str = ".payload";

/// Returns the name of the payload part of the operation at `index`, for example `"0.payload"`.
///
/// ```
/// use objectstore_types::batch::payload_part_name;
///
/// assert_eq!(payload_part_name(3), "3.payload");
/// ```
pub fn payload_part_name(index: usize) -> String {
    format!("{index}{PAYLOAD_SUFFIX}")
}

/// The kind of a batch operation.
///
/// Kinds that this version does not know deserialize as [`OperationKind::Unknown`], so that a
/// client can still read results of operations added to the server later.
///
/// ```
/// use objectstore_types::batch::OperationKind;
///
/// let kind: OperationKind = serde_json::from_str(r#""copy""#)?;
/// assert_eq!(kind, OperationKind::Unknown);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationKind {
    /// Reads an object and its metadata.
    Get,
    /// Reads the metadata of an object.
    Head,
    /// Deletes an object.
    Delete,
    /// Stores an object.
    Insert,
    /// A kind that this version does not know.
    ///
    /// Only produced when deserializing. It serializes as `"unknown"`.
    #[serde(other)]
    Unknown,
}

impl OperationKind {
    /// Returns the name of the kind as used on the wire, for example `"insert"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Delete => "delete",
            Self::Insert => "insert",
            Self::Unknown => "unknown",
        }
    }
}

/// The JSON body of a descriptor part, which describes one operation of a request.
///
/// Unknown fields are ignored when deserializing.
///
/// ```
/// use objectstore_types::batch::Descriptor;
///
/// let json = r#"{"kind":"get","key":"a/b"}"#;
/// let descriptor: Descriptor = serde_json::from_str(json)?;
/// assert_eq!(descriptor, Descriptor::Get { key: "a/b".to_owned() });
/// assert_eq!(serde_json::to_string(&descriptor)?, json);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Descriptor {
    /// Reads the object with the given key.
    Get {
        /// The key of the object.
        key: String,
    },
    /// Reads the metadata of the object with the given key.
    Head {
        /// The key of the object.
        key: String,
    },
    /// Deletes the object with the given key.
    Delete {
        /// The key of the object.
        key: String,
    },
    /// Stores an object, whose bytes are in the payload part that follows.
    Insert {
        /// The key of the object. The server generates one if it is absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        /// The metadata of the object.
        #[serde(default)]
        metadata: WriteMetadata,
    },
}

/// The JSON body of a result part, which reports the outcome of one operation.
///
/// Unknown fields are ignored when deserializing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultPart {
    /// The HTTP status code of the operation.
    pub status: u16,
    /// The kind of the operation. Absent if the operation failed with an [`error`](Self::error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<OperationKind>,
    /// The key of the object. Absent if the operation failed with an [`error`](Self::error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The metadata of the object, for a successful get or head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ReadMetadata>,
    /// The error, for a failed operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_part_with_unknown_kind_is_readable() {
        let json = r#"{"status":200,"kind":"copy","key":"a"}"#;
        let result: ResultPart = serde_json::from_str(json).unwrap();

        assert_eq!(result.status, 200);
        assert_eq!(result.kind, Some(OperationKind::Unknown));
        assert_eq!(result.key.as_deref(), Some("a"));
    }
}
