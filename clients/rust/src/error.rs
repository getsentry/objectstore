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
    Metadata(#[from] objectstore_types::Error),
    /// Error when scope validation fails.
    #[error("invalid scope: {0}")]
    InvalidScope(#[from] objectstore_types::scope::InvalidScopeError),
    /// Error when creating auth tokens, such as invalid keys.
    #[error(transparent)]
    TokenError(#[from] jsonwebtoken::errors::Error),
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
    /// Error when parsing a batch response part.
    #[error("batch response error: {0}")]
    BatchResponse(String),
    // TODO: map reqwest::Error to this variant so that individual and `many` requests report
    // errors in the same way.
    /// Error that indicates failure of an individual operation in a `many` request.
    #[error("operation error (HTTP status code {status}): {message}")]
    OperationError {
        /// The HTTP status code corresponding to the status of the operation.
        status: u16,
        /// The error message.
        message: String,
    },
}

/// A convenience alias that defaults our [`Error`] type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
