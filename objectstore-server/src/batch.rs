//! Wire vocabulary of the batch endpoint: part header names and part names.
//!
//! The batch endpoint accepts two `multipart/form-data` layouts and answers in the layout of the
//! request (see [`BatchFormat`](crate::extractors::batch::BatchFormat)):
//!
//! - The **header layout** carries each operation in one part and its kind, key, index, status
//!   and metadata in part headers. Clients include [`HEADER_BATCH_OPERATION_KEY`] and
//!   [`HEADER_BATCH_OPERATION_KIND`] on each request part; the server echoes all three, including
//!   [`HEADER_BATCH_OPERATION_INDEX`], on each response part so clients can correlate results
//!   with their original operations.
//! - The **JSON layout** carries each operation in a JSON part named by its index (`"0"`, `"1"`,
//!   …) plus a payload part named [`payload_part_name`] (`"0.payload"`) for inserts and
//!   successful gets. Parts carry no headers besides `Content-Disposition` and `Content-Type`.

/// Zero-based position of this operation within the batch, set by the server on response parts.
///
/// Clients use this to match each response part back to its corresponding request operation.
pub const HEADER_BATCH_OPERATION_INDEX: &str = "x-sn-batch-operation-index";

/// Object key for this batch operation, required on request parts.
///
/// The key is escaped with [`encode_header_value`](objectstore_types::headers::encode_header_value)
/// so that keys containing characters a header value cannot carry — non-ASCII in particular — still
/// survive the transport.
pub const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";

/// Operation kind for this batch part: `"get"`, `"insert"`, or `"delete"`.
pub const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";

/// Suffix that marks a payload part in the JSON layout.
pub const PAYLOAD_SUFFIX: &str = ".payload";

/// Returns the name of the payload part for the operation at `index`, for example `"0.payload"`.
///
/// ```
/// use objectstore_server::batch::payload_part_name;
///
/// assert_eq!(payload_part_name(3), "3.payload");
/// ```
pub fn payload_part_name(index: usize) -> String {
    format!("{index}{PAYLOAD_SUFFIX}")
}
