//! HTTP header names used in batch request and response processing.
//!
//! These headers carry per-operation metadata in the multipart bodies of batch
//! requests and responses. Clients include [`HEADER_BATCH_OPERATION_KEY`] and
//! [`HEADER_BATCH_OPERATION_KIND`] on each request part; the server echoes all
//! three — including [`HEADER_BATCH_OPERATION_INDEX`] — on each response part
//! so clients can correlate results with their original operations.

/// Zero-based position of this operation within the batch, set by the server on response parts.
///
/// Clients use this to match each response part back to its corresponding request operation.
pub const HEADER_BATCH_OPERATION_INDEX: &str = "x-sn-batch-operation-index";

/// Base64-encoded object key for this batch operation, required on request parts.
///
/// The key is base64-encoded to allow arbitrary byte sequences in object keys without conflicting
/// with HTTP header encoding restrictions.
pub const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";

/// Operation kind for this batch part: `"get"`, `"insert"`, or `"delete"`.
pub const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
