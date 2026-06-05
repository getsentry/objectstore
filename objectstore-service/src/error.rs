//! Error types for service and backend operations.
//!
//! [`Error`] covers I/O, serialization, HTTP, metadata, authentication,
//! and backend-specific failures. [`Result`] is the corresponding alias.

use std::{borrow::Cow, fmt::Display};

use crate::stream::ClientError;

/// Error type for service operations.
#[derive(Debug)]
pub struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) description: Option<Cow<'static, str>>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    /// Returns the kind of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Attempts to downcast the source error to a concrete type.
    pub fn downcast_ref<T: std::error::Error + 'static>(&self) -> Option<&T> {
        self.source.as_ref()?.downcast_ref::<T>()
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_label = match &self.kind {
            ErrorKind::ClientStream => "client stream error",
            ErrorKind::Transient => "transient error",
            ErrorKind::BadRequest => "bad request",
            ErrorKind::NotImplemented => "not implemented",
            ErrorKind::TooManyRequests => "too many requests",
            ErrorKind::Internal => "internal error",
        };
        write!(f, "{kind_label}")?;
        if let Some(ref description) = self.description {
            write!(f, ": {description}")?;
        }
        Ok(())
    }
}

macro_rules! impl_converter {
    ($kind:path, $fn_name:ident, $msg_fn_name:ident) => {
        #[allow(dead_code)]
        impl Error {
            pub(crate) fn $fn_name(
                description: impl Into<Cow<'static, str>>,
                source: impl std::error::Error + Send + Sync + 'static,
            ) -> Self {
                Self {
                    kind: $kind,
                    description: Some(description.into()),
                    source: Some(Box::new(source)),
                }
            }

            pub(crate) fn $msg_fn_name(description: impl Into<Cow<'static, str>>) -> Self {
                Self {
                    kind: $kind,
                    description: Some(description.into()),
                    source: None,
                }
            }
        }
    };
}

impl_converter!(ErrorKind::ClientStream, client_stream, client_stream_msg);
impl_converter!(ErrorKind::Transient, transient, transient_msg);
impl_converter!(ErrorKind::BadRequest, bad_request, bad_request_msg);
impl_converter!(
    ErrorKind::NotImplemented,
    not_implemented,
    not_implemented_msg
);
impl_converter!(
    ErrorKind::TooManyRequests,
    too_many_requests,
    too_many_requests_msg
);
impl_converter!(ErrorKind::Internal, internal, internal_msg);

impl Error {
    pub(crate) fn from_reqwest(
        description: impl Into<Cow<'static, str>>,
        cause: reqwest::Error,
    ) -> Self {
        // NOTE: 404 (Not Found) and 409 (Conflict) don't map cleanly to any current ErrorKind.
        // Consider adding ErrorKind::NotFound and ErrorKind::Conflict if backends need to
        // distinguish these from generic internal errors.
        let kind = match cause.status() {
            Some(status) => match status.as_u16() {
                400 => ErrorKind::BadRequest,
                408 | 429 => ErrorKind::TooManyRequests,
                500 | 502 | 503 | 504 => ErrorKind::Transient,
                _ => ErrorKind::Internal,
            },
            None => ErrorKind::Transient,
        };
        Self {
            kind,
            description: Some(description.into()),
            source: Some(Box::new(cause)),
        }
    }
}

impl From<ClientError> for Error {
    fn from(source: ClientError) -> Self {
        Self {
            kind: ErrorKind::ClientStream,
            source: Some(Box::new(source)),
            description: None,
        }
    }
}

/// Classification of a service error.
#[derive(Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Error originating from a client-supplied input stream.
    ClientStream,
    /// Transient failure that may succeed on retry.
    Transient,
    /// Malformed or invalid client request.
    BadRequest,
    /// Functionality not implemented by this backend.
    NotImplemented,
    /// Service is at capacity.
    TooManyRequests,
    /// Internal service or backend failure.
    Internal,
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
