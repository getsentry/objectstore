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
    /// Error parsing multipart response.
    #[error("failed to parse multipart response: {0}")]
    MultipartParse(#[from] multer::Error),
    /// Invalid multipart boundary.
    #[error("invalid or missing multipart boundary")]
    InvalidBoundary,
    /// Invalid header value.
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}

/// A convenience alias that defaults our [`Error`] type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
