//! Semantic errors for service and backend operations.
//!
//! [`Error`] deliberately exposes only a stable semantic [`ErrorKind`]. Human-readable context and
//! the source chain retain diagnostic detail without making backend implementation details part of
//! the service API.

use std::any::Any;
use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;

use objectstore_log::Level;
/// A panic captured from a service task.
#[derive(Debug)]
pub struct Panic {
    message: Cow<'static, str>,
}

impl Panic {
    /// Extracts a message from a panic payload.
    pub fn new(payload: Box<dyn Any + Send>) -> Self {
        let message = if let Some(s) = payload.downcast_ref::<&str>() {
            Cow::Borrowed(*s)
        } else if let Some(s) = payload.downcast_ref::<String>() {
            Cow::Owned(s.clone())
        } else {
            Cow::Borrowed("unknown panic")
        };
        Self { message }
    }
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for Panic {}

/// The client-visible semantic classification of a service error.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Object metadata supplied by a client is invalid.
    InvalidMetadata,
    /// A multipart upload identifier is invalid.
    InvalidUploadId,
    /// A client-provided request stream failed.
    ClientStream,
    /// A requested byte range cannot be resolved against the object size.
    RangeNotSatisfiable {
        /// Total object length in bytes.
        total: u64,
    },
    /// The service cannot accept more work.
    AtCapacity,
    /// The requested operation is unsupported.
    Unsupported,
    /// A storage backend operation failed.
    BackendFailure,
    /// A storage backend rejected the operation because it is rate limited.
    BackendRateLimited,
    /// A storage backend operation timed out.
    BackendTimeout,
    /// A storage backend is temporarily unavailable.
    BackendUnavailable,
    /// A service task panicked.
    Panic,
    /// Persisted or remote data is corrupt.
    CorruptData,
    /// An unexpected internal service failure occurred.
    Internal,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMetadata => f.write_str("invalid object metadata"),
            Self::InvalidUploadId => f.write_str("invalid upload id"),
            Self::ClientStream => f.write_str("invalid client stream"),
            Self::RangeNotSatisfiable { total } => {
                write!(f, "range not satisfiable (object size: {total} bytes)")
            }
            Self::AtCapacity => f.write_str("service at capacity"),
            Self::Unsupported => f.write_str("unsupported operation"),
            Self::BackendFailure => f.write_str("backend operation failed"),
            Self::BackendRateLimited => f.write_str("backend rate limited"),
            Self::BackendTimeout => f.write_str("backend timed out"),
            Self::BackendUnavailable => f.write_str("backend unavailable"),
            Self::CorruptData => f.write_str("corrupt stored data"),
            Self::Panic => f.write_str("service task panicked"),
            Self::Internal => f.write_str("internal service error"),
        }
    }
}

/// Opaque service error with a stable semantic kind.
///
/// Its string representation is the kind followed by `: ` and human-readable context when context
/// is present. The underlying source is retained separately through [`StdError::source`].
pub struct Error {
    kind: ErrorKind,
    context: Option<Cow<'static, str>>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    /// Returns this error's semantic kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Creates an error without an underlying source and with human-readable context.
    pub fn new(kind: ErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self::build(kind, Some(context.into()), None)
    }

    /// Creates an error with an underlying source.
    pub fn with_source<E>(kind: ErrorKind, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::build(kind, None, Some(Box::new(source)))
    }

    pub(crate) fn with_context<E>(
        kind: ErrorKind,
        context: impl Into<Cow<'static, str>>,
        source: E,
    ) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::build(kind, Some(context.into()), Some(Box::new(source)))
    }

    fn build(
        kind: ErrorKind,
        context: Option<Cow<'static, str>>,
        source: Option<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            context,
            source,
        }
    }

    /// Returns the appropriate log level for this error.
    pub fn level(&self) -> Level {
        match self.kind {
            ErrorKind::InvalidMetadata
            | ErrorKind::InvalidUploadId
            | ErrorKind::ClientStream
            | ErrorKind::RangeNotSatisfiable { .. } => Level::DEBUG,
            ErrorKind::AtCapacity
            | ErrorKind::BackendRateLimited
            | ErrorKind::BackendTimeout
            | ErrorKind::BackendUnavailable => Level::WARN,
            ErrorKind::Unsupported
            | ErrorKind::BackendFailure
            | ErrorKind::CorruptData
            | ErrorKind::Panic
            | ErrorKind::Internal => Level::ERROR,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)?;
        if let Some(context) = &self.context {
            write!(f, ": {context}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("source", &self.source)
            .finish()
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::build(kind, None, None)
    }
}

impl From<Panic> for Error {
    fn from(source: Panic) -> Self {
        Self::with_source(ErrorKind::Panic, source)
    }
}

/// Adds a semantic kind and optional context when converting an external error.
pub trait ResultExt<T> {
    /// Converts an external error into a service error with `kind` and human-readable context.
    ///
    /// The source error is retained, while the rendered service error contains the semantic kind
    /// and context.
    ///
    /// ```
    /// use objectstore_service::error::{ErrorKind, ResultExt as _};
    ///
    /// let result = std::fs::read("missing")
    ///     .context(ErrorKind::BackendFailure, "reading local object");
    /// let error = result.unwrap_err();
    /// assert_eq!(
    ///     error.to_string(),
    ///     "backend operation failed: reading local object"
    /// );
    /// ```
    fn context(self, kind: ErrorKind, context: impl Into<Cow<'static, str>>) -> Result<T>;

    /// Converts an external error into a service error with only `kind`.
    ///
    /// Use this when the source already identifies the failure or when the operation is expected to
    /// be infallible. The source error is still retained.
    ///
    /// ```
    /// use objectstore_service::error::{ErrorKind, ResultExt as _};
    ///
    /// let result = "invalid".parse::<u64>().kind(ErrorKind::InvalidMetadata);
    /// assert_eq!(result.unwrap_err().to_string(), "invalid object metadata");
    /// ```
    fn kind(self, kind: ErrorKind) -> Result<T>;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: StdError + Send + Sync + 'static,
{
    fn context(self, kind: ErrorKind, context: impl Into<Cow<'static, str>>) -> Result<T> {
        self.map_err(|source| Error::with_context(kind, context, source))
    }

    fn kind(self, kind: ErrorKind) -> Result<T> {
        self.map_err(|source| Error::with_source(kind, source))
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::with_source(ErrorKind::BackendFailure, source)
    }
}

impl From<reqwest::Error> for Error {
    fn from(source: reqwest::Error) -> Self {
        Self::with_source(ErrorKind::BackendFailure, source)
    }
}

impl From<gcp_auth::Error> for Error {
    fn from(source: gcp_auth::Error) -> Self {
        Self::with_source(ErrorKind::BackendFailure, source)
    }
}

impl From<crate::stream::ClientError> for Error {
    fn from(source: crate::stream::ClientError) -> Self {
        Self::with_source(ErrorKind::ClientStream, source)
    }
}

impl From<objectstore_types::multipart::InvalidUploadId> for Error {
    fn from(source: objectstore_types::multipart::InvalidUploadId) -> Self {
        Self::with_source(ErrorKind::InvalidUploadId, source)
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use super::{Error, ErrorKind, Panic};

    #[test]
    fn opaque_error_preserves_source() {
        let error = Error::with_source(ErrorKind::BackendFailure, io::Error::other("backend down"));
        let standard_error: &dyn std::error::Error = &error;

        assert_eq!(error.kind(), ErrorKind::BackendFailure);
        assert_eq!(standard_error.source().unwrap().to_string(), "backend down");
    }

    #[test]
    fn context_renders_after_kind() {
        let error = Error::with_context(
            ErrorKind::BackendFailure,
            "reading local object",
            io::Error::other("backend down"),
        );

        assert_eq!(
            error.to_string(),
            "backend operation failed: reading local object"
        );
    }

    #[test]
    fn error_kind_default_message_includes_range_size() {
        let error: Error = ErrorKind::RangeNotSatisfiable { total: 42 }.into();

        assert_eq!(
            error.to_string(),
            "range not satisfiable (object size: 42 bytes)"
        );
    }

    #[test]
    fn panic_uses_the_payload_message() {
        let panic = Panic::new(Box::new("task panicked"));
        let error: Error = panic.into();

        assert_eq!(error.kind(), ErrorKind::Panic);
        assert_eq!(error.to_string(), "service task panicked");
        assert_eq!(error.source().unwrap().to_string(), "task panicked");
    }
}
