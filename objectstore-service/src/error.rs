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

impl Error {
    pub(crate) fn internal(
        description: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind: ErrorKind::Internal,
            description: Some(description.into()),
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn internal_msg(description: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: ErrorKind::Internal,
            description: Some(description.into()),
            source: None,
        }
    }

    pub(crate) fn bad_request(
        description: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind: ErrorKind::BadRequest,
            description: Some(description.into()),
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn bad_request_msg(description: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: ErrorKind::BadRequest,
            description: Some(description.into()),
            source: None,
        }
    }

    pub(crate) fn client_stream(source: ClientError) -> Self {
        Self {
            kind: ErrorKind::ClientStream,
            description: None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn not_implemented() -> Self {
        Self {
            kind: ErrorKind::NotImplemented,
            description: None,
            source: None,
        }
    }
}

impl From<ClientError> for Error {
    fn from(source: ClientError) -> Self {
        Self::client_stream(source)
    }
}

macro_rules! impl_from_internal {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(source: $ty) -> Self {
                    Self {
                        kind: ErrorKind::Internal,
                        description: None,
                        source: Some(Box::new(source)),
                    }
                }
            }
        )+
    };
}

impl_from_internal!(
    std::io::Error,
    serde_json::Error,
    reqwest::Error,
    gcp_auth::Error,
    objectstore_types::metadata::Error,
);

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
