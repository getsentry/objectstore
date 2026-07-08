//! Error types for service and backend operations.
//!
//! [`Error`] is an `anyhow`-shaped struct: it carries an [`ErrorKind`] (the
//! *classification* of the failure, independent of any HTTP semantics), an
//! optional boxed [`source`](Error::source) error, and an optional
//! [`context`](Error::context) message. [`Result`] is the corresponding alias.
//!
//! The default path is `?`: a foreign error converts via one of the [`From`]
//! impls into an [`ErrorKind::Internal`] error carrying the original as its
//! source. To override the classification or attach context without a
//! `map_err`, use the [`ResultExt`] extension trait's [`kind`](ResultExt::kind)
//! and [`context`](ResultExt::context) methods.

use std::any::Any;
use std::borrow::Cow;
use std::fmt;

use objectstore_log::Level;

use crate::stream::ClientError;

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
    ///
    /// Used as the [`Error`] message when no explicit context is attached.
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
/// for source-less failures.
pub struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    pub(crate) context: Option<Cow<'static, str>>,
}

impl Error {
    /// Creates a source-less error of the given kind.
    ///
    /// Attach a message with [`context`](Self::context).
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            source: None,
            context: None,
        }
    }

    /// Attaches a context message to this error, replacing any existing one.
    #[must_use]
    pub fn context(mut self, context: impl Into<Cow<'static, str>>) -> Self {
        self.context = Some(context.into());
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

    /// Returns the classification of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the underlying source error, if any.
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|s| s as &(dyn std::error::Error + 'static))
    }

    /// Returns the context message, if any.
    pub fn context_str(&self) -> Option<&str> {
        self.context.as_deref()
    }

    /// Returns the appropriate log level for this error.
    pub fn level(&self) -> Level {
        match self.kind() {
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
        match &self.context {
            Some(context) => f.write_str(context),
            None => fmt::Display::fmt(&self.kind, f),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Error");
        dbg.field("kind", &self.kind);
        if let Some(context) = &self.context {
            dbg.field("context", context);
        }
        if let Some(source) = &self.source {
            dbg.field("source", source);
        }
        dbg.finish()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|s| s as &(dyn std::error::Error + 'static))
    }
}

/// The object's total size, carried as the [`source`](Error::source) of an
/// [`ErrorKind::RangeNotSatisfiable`] error.
///
/// Construct one at the call site and convert it with `?`/`.into()`: the
/// [`From`] impl below classifies it as [`ErrorKind::RangeNotSatisfiable`]. The
/// API layer downcasts back to this to build the `Content-Range` header of a
/// `416 Range Not Satisfiable` response.
#[derive(Debug)]
pub struct RangeNotSatisfiableError {
    /// Total size of the object in bytes.
    pub total: u64,
}

/// Converts into an [`ErrorKind::RangeNotSatisfiable`] error, preserving the
/// total size as a downcastable source.
impl From<RangeNotSatisfiableError> for Error {
    fn from(source: RangeNotSatisfiableError) -> Self {
        Self {
            kind: ErrorKind::RangeNotSatisfiable,
            source: Some(Box::new(source)),
            context: None,
        }
    }
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
/// and store it as the source.
macro_rules! impl_internal_from {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(source: $ty) -> Self {
                    Self {
                        kind: ErrorKind::Internal,
                        source: Some(Box::new(source)),
                        context: None,
                    }
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

/// Converts an [`anyhow::Error`] into an [`ErrorKind::Internal`] error, preserving
/// its cause chain as the source.
///
/// `anyhow::Error` does not implement [`std::error::Error`], so it cannot go through
/// the `impl_internal_from!` macro; it converts into a boxed error instead.
impl From<anyhow::Error> for Error {
    fn from(source: anyhow::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            source: Some(source.into()),
            context: None,
        }
    }
}

/// Converts a client-stream fault into an [`ErrorKind::ClientStream`] error.
impl From<ClientError> for Error {
    fn from(source: ClientError) -> Self {
        Self {
            kind: ErrorKind::ClientStream,
            source: Some(Box::new(source)),
            context: None,
        }
    }
}

/// Converts a metadata parse error into an [`ErrorKind::InvalidInput`] error.
impl From<objectstore_types::metadata::Error> for Error {
    fn from(source: objectstore_types::metadata::Error) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            source: Some(Box::new(source)),
            context: None,
        }
    }
}

/// Converts an invalid upload id into an [`ErrorKind::InvalidInput`] error.
impl From<objectstore_types::multipart::InvalidUploadId> for Error {
    fn from(source: objectstore_types::multipart::InvalidUploadId) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            source: Some(Box::new(source)),
            context: None,
        }
    }
}

/// Extension trait for reclassifying and annotating a [`Result`]'s error as an
/// [`Error`], without a `map_err`.
///
/// Both methods convert the existing error into an [`Error`] via its [`From`]
/// impl (foreign errors are boxed as the source with [`ErrorKind::Internal`];
/// an existing [`Error`] passes through unchanged), then override the relevant
/// field. Because the conversion of an existing [`Error`] is the identity, the
/// source is only ever boxed once — chaining `.kind(k).context(c)` does not
/// double-wrap.
pub trait ResultExt<T> {
    /// Sets the [`ErrorKind`] of the error.
    fn kind(self, kind: ErrorKind) -> Result<T, Error>;

    /// Attaches a context message to the error.
    fn context(self, context: impl Into<Cow<'static, str>>) -> Result<T, Error>;
}

impl<T, E: Into<Error>> ResultExt<T> for Result<T, E> {
    fn kind(self, kind: ErrorKind) -> Result<T, Error> {
        self.map_err(|e| {
            let mut err = e.into();
            err.kind = kind;
            err
        })
    }

    fn context(self, context: impl Into<Cow<'static, str>>) -> Result<T, Error> {
        self.map_err(|e| {
            let mut err = e.into();
            err.context = Some(context.into());
            err
        })
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
