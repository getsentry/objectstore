use std::sync::Arc;

/// Errors that can happen within the objectstore-client
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any error emitted from the underlying [`reqwest`] client.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// IO errors related to payload streaming.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Errors related to UTF-8 dcoding
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    /// Errors handling metadata, such as serializing it to/from HTTP headers.
    #[error(transparent)]
    Metadata(#[from] objectstore_types::metadata::Error),
    /// Error when scope validation fails.
    #[error("invalid scope: {0}")]
    InvalidScope(#[from] objectstore_types::scope::InvalidScopeError),
    /// Error when creating auth tokens, such as invalid keys.
    #[error(transparent)]
    TokenError(#[from] jsonwebtoken::errors::Error),
    /// Error when generating a pre-signed URL.
    #[error(transparent)]
    Presign(#[from] PresignError),
    /// Error when URL manipulation fails.
    #[error("{message}")]
    InvalidUrl {
        /// The URL error message.
        message: String,
    },
    /// Error when parsing a multipart response.
    #[error(transparent)]
    Multipart(#[from] multer::Error),
    /// Error when the server returned a malformed response.
    #[error("{0}")]
    MalformedResponse(String),
    /// Error that indicates that an entire batch request failed.
    #[error("batch request failed: {0}")]
    Batch(Arc<Error>),
    /// Error that indicates failure of an individual operation in a batch request.
    #[error("operation failed with HTTP status code {status}: {message}")]
    OperationFailure {
        /// The HTTP status code corresponding to the status of the operation.
        status: u16,
        /// The error message.
        message: String,
    },
}

/// Errors that can occur when generating a pre-signed URL.
#[derive(Debug, thiserror::Error)]
pub enum PresignError {
    /// The HTTP method is not supported for pre-signed URLs.
    #[error("unsupported method: {method}. Only GET and HEAD are supported")]
    UnsupportedMethod {
        /// The unsupported method that was provided.
        method: String,
    },
    /// Failed to parse the Ed25519 private key.
    #[error("failed to parse Ed25519 private key: {0}")]
    InvalidKey(#[from] ed25519_dalek::pkcs8::Error),
}

/// A convenience alias that defaults our [`Error`] type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
