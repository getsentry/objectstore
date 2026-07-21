//! Error types for service and backend operations.
//!
//! [`Error`] combines an [`ErrorKind`] (the *classification* of the failure,
//! independent of any HTTP semantics) with an [`anyhow::Error`] diagnostic.
//! The diagnostic preserves the original error and every context frame, while
//! the kind can be changed independently. [`Result`] is the corresponding alias.
//!
//! The default path is `?`: a foreign error converts via one of the [`From`]
//! impls into an [`ErrorKind::Internal`] error carrying the original diagnostic.
//! To override the classification or attach context without a `map_err`, use the
//! [`ResultExt`] extension trait's [`kind`](ResultExt::kind) and
//! [`context`](ResultExt::context) methods.

use std::any::Any;
use std::borrow::Cow;
use std::fmt;

use objectstore_log::Level;

use crate::stream::ClientError;

/// Structured error detail parsed from a backend HTTP error response.
///
/// Formats conditionally: includes only the fields that are non-empty.
#[derive(Debug)]
pub struct BackendDetail {
    /// Machine-readable error code (e.g., "InvalidArgument", "NoSuchKey").
    pub code: String,
    /// Human-readable error message from the response body.
    pub message: String,
}

impl BackendDetail {
    /// Creates a new [`BackendDetail`] with empty code and message.
    pub fn none() -> Self {
        Self {
            code: String::new(),
            message: String::new(),
        }
    }
}

impl fmt::Display for BackendDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code.is_empty(), self.message.is_empty()) {
            (false, false) => write!(f, "{} (backend code {})", self.message, self.code),
            (true, false) => write!(f, "{}", self.message),
            (false, true) => write!(f, "backend code {}", self.code),
            (true, true) => Ok(()),
        }
    }
}

/// The category of a service error.
///
/// These kinds describe the *cause* of a failure, independent of any HTTP
/// semantics. It is up to the API layer to decide how to map each kind onto a
/// status code, and which kinds to group together.
///
/// Users should rely on the kind for classification and handling, rather than matching on
/// specific variants.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Error originating from a client-supplied request body stream.
    ClientStream,
    /// Malformed client input (bad metadata, JSON, upload id, …).
    InvalidInput,
    /// The requested byte range is not satisfiable for the object's size.
    RangeNotSatisfiable,
    /// This service instance has reached its own concurrency limit and is
    /// shedding load.
    AtCapacity,
    /// A storage backend rejected the operation with a rate-limit response.
    BackendRateLimited,
    /// A storage backend timed out.
    BackendTimeout,
    /// A storage backend is temporarily unavailable.
    BackendUnavailable,
    /// Operation unsupported by this service instance.
    NotImplemented,
    /// Internal failure.
    Internal,
}

impl ErrorKind {
    /// Returns a short human-readable description of this kind.
    fn as_str(self) -> &'static str {
        match self {
            ErrorKind::ClientStream => "client stream error",
            ErrorKind::InvalidInput => "invalid input",
            ErrorKind::RangeNotSatisfiable => "range not satisfiable",
            ErrorKind::AtCapacity => "at capacity",
            ErrorKind::BackendRateLimited => "backend rate limited",
            ErrorKind::BackendTimeout => "backend timeout",
            ErrorKind::BackendUnavailable => "backend unavailable",
            ErrorKind::NotImplemented => "not implemented",
            ErrorKind::Internal => "internal error",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error type for service operations.
///
/// See the [module docs](self) for the overall design. Construct one either by
/// converting a foreign error (via `?` or [`ResultExt`]) or with [`Error::new`]
/// for failures without an underlying error.
pub struct Error {
    /// The classification of this error.
    pub kind: ErrorKind,
    /// The diagnostic error and its context chain.
    pub inner: anyhow::Error,
}

impl Error {
    /// Creates an error of the given kind using the kind's default message.
    ///
    /// Attach a more specific message with [`context`](Self::context).
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            inner: anyhow::Error::msg(kind.as_str()),
        }
    }

    fn from_source<E>(kind: ErrorKind, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            kind,
            inner: anyhow::Error::new(source),
        }
    }

    /// Creates a range error carrying the object's total size.
    pub fn range_not_satisfiable(total: u64) -> Self {
        Self::from_source(
            ErrorKind::RangeNotSatisfiable,
            RangeNotSatisfiableError { total },
        )
    }

    /// Returns the object size carried by a range error, if present.
    pub fn range_total(&self) -> Option<u64> {
        self.downcast_ref::<RangeNotSatisfiableError>()
            .map(|error| error.total)
    }

    /// Attaches another context frame to this error.
    #[must_use]
    pub fn context(mut self, context: impl Into<Cow<'static, str>>) -> Self {
        self.inner = self.inner.context(context.into());
        self
    }

    /// Changes this error's classification without changing its diagnostic.
    #[must_use]
    pub fn kind(mut self, kind: ErrorKind) -> Self {
        self.kind = kind;
        self
    }

    /// Creates an [`ErrorKind::Internal`] error from a panic payload, extracting the message.
    pub fn panic(payload: Box<dyn Any + Send>) -> Self {
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_owned()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_owned()
        };
        Self::new(ErrorKind::Internal).context(format!("service task panicked: {msg}"))
    }

    /// Iterates over the complete diagnostic chain, outermost context first.
    pub fn chain(&self) -> anyhow::Chain<'_> {
        self.inner.chain()
    }

    /// Downcasts any error or context value in the diagnostic chain.
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        self.inner.downcast_ref()
    }

    /// Returns the appropriate log level for this error.
    pub fn level(&self) -> Level {
        match self.kind {
            ErrorKind::ClientStream | ErrorKind::InvalidInput | ErrorKind::RangeNotSatisfiable => {
                Level::DEBUG
            }
            ErrorKind::AtCapacity
            | ErrorKind::BackendRateLimited
            | ErrorKind::BackendTimeout
            | ErrorKind::BackendUnavailable => Level::WARN,
            ErrorKind::NotImplemented | ErrorKind::Internal => Level::ERROR,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let inner: &(dyn std::error::Error + Send + Sync + 'static) = self.inner.as_ref();
        inner.source()
    }
}

/// The object's total size, carried in the diagnostic chain of an
/// [`ErrorKind::RangeNotSatisfiable`] error.
#[derive(Debug)]
struct RangeNotSatisfiableError {
    total: u64,
}

impl fmt::Display for RangeNotSatisfiableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "range not satisfiable (object size: {} bytes)",
            self.total
        )
    }
}

impl std::error::Error for RangeNotSatisfiableError {}

/// Generates `From<T> for Error` impls that classify `T` as [`ErrorKind::Internal`]
/// and preserve it in the diagnostic chain.
macro_rules! impl_internal_from {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(source: $ty) -> Self {
                    Self::from_source(ErrorKind::Internal, source)
                }
            }
        )*
    };
}

impl_internal_from!(
    std::io::Error,
    std::num::ParseIntError,
    std::num::TryFromIntError,
    std::str::Utf8Error,
    std::string::FromUtf8Error,
    std::time::SystemTimeError,
    serde_json::Error,
    reqwest::Error,
    reqwest::header::InvalidHeaderName,
    reqwest::header::InvalidHeaderValue,
    gcp_auth::Error,
    url::ParseError,
    quick_xml::DeError,
    quick_xml::SeError,
    bigtable_rs::bigtable::Error,
);

/// Converts an [`anyhow::Error`] into an [`ErrorKind::Internal`] error without
/// altering its diagnostic chain.
impl From<anyhow::Error> for Error {
    fn from(inner: anyhow::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            inner,
        }
    }
}

/// Converts a client-stream fault into an [`ErrorKind::ClientStream`] error.
impl From<ClientError> for Error {
    fn from(source: ClientError) -> Self {
        Self::from_source(ErrorKind::ClientStream, source)
    }
}

/// Converts a metadata parse error into an [`ErrorKind::InvalidInput`] error.
impl From<objectstore_types::metadata::Error> for Error {
    fn from(source: objectstore_types::metadata::Error) -> Self {
        Self::from_source(ErrorKind::InvalidInput, source)
    }
}

/// Converts an invalid upload id into an [`ErrorKind::InvalidInput`] error.
impl From<objectstore_types::multipart::InvalidUploadId> for Error {
    fn from(source: objectstore_types::multipart::InvalidUploadId) -> Self {
        Self::from_source(ErrorKind::InvalidInput, source)
    }
}

/// Extension trait for reclassifying and annotating a [`Result`]'s error as an
/// [`Error`], without a `map_err`.
pub trait ResultExt<T> {
    /// Sets the [`ErrorKind`] of the error without changing its diagnostic.
    fn kind(self, kind: ErrorKind) -> Result<T, Error>;

    /// Attaches another context frame to the error.
    fn context(self, context: impl Into<Cow<'static, str>>) -> Result<T, Error>;
}

impl<T, E: Into<Error>> ResultExt<T> for Result<T, E> {
    fn kind(self, kind: ErrorKind) -> Result<T, Error> {
        self.map_err(|error| error.into().kind(kind))
    }

    fn context(self, context: impl Into<Cow<'static, str>>) -> Result<T, Error> {
        self.map_err(|error| error.into().context(context))
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_accumulates_without_changing_kind() {
        let error = Err::<(), _>(std::io::Error::other("root cause"))
            .context("reading configuration")
            .kind(ErrorKind::BackendUnavailable)
            .context("starting backend")
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::BackendUnavailable);
        assert_eq!(error.to_string(), "starting backend");
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            ["starting backend", "reading configuration", "root cause",]
        );
        assert!(format!("{error:?}").contains("Caused by:"));
    }

    #[test]
    fn range_total_survives_context() {
        let error = Error::range_not_satisfiable(42).context("reading object range");

        assert_eq!(error.kind, ErrorKind::RangeNotSatisfiable);
        assert_eq!(error.range_total(), Some(42));
    }

    #[test]
    fn source_skips_displayed_outer_message() {
        let error = Error::from(std::io::Error::other("root")).context("outer");

        assert_eq!(error.to_string(), "outer");
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            "root"
        );
    }
}
