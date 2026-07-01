//! Error types for service and backend operations.
//!
//! [`Error`] covers I/O, serialization, HTTP, metadata, authentication,
//! and backend-specific failures. [`Result`] is the corresponding alias.

use std::any::Any;
use std::borrow::Cow;
use std::fmt;

use objectstore_log::Level;
use reqwest::StatusCode;
use thiserror::Error as ThisError;

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

/// Error type for service operations.
#[derive(Debug, ThisError)]
pub enum Error {
    /// IO errors related to payload streaming or file operations.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Error originating from a client-supplied input stream.
    ///
    /// Indicates the client is at fault (e.g. dropped connection mid-upload) and should
    /// map to a 4xx response rather than a 5xx.
    #[error("error reading client stream: {0}")]
    Client(#[from] ClientError),

    /// Errors related to de/serialization.
    #[error("serde error: {context}")]
    Serde {
        /// Context describing what was being serialized/deserialized.
        context: &'static str,
        /// The underlying serde error.
        #[source]
        cause: serde_json::Error,
    },

    /// All errors stemming from the reqwest client, used in multiple backends to send requests to
    /// e.g. GCP APIs.
    /// These can be network errors encountered when sending the requests, but can also indicate
    /// errors returned by the API itself.
    #[error("reqwest error: {context}")]
    Reqwest {
        /// Context describing the request that failed.
        context: &'static str,
        /// The underlying reqwest error.
        #[source]
        cause: reqwest::Error,
    },

    /// An HTTP error response from a storage backend (e.g., GCS, S3).
    ///
    /// Unlike [`Reqwest`](Self::Reqwest), which covers transport-level failures, this variant
    /// captures application-level error responses where the server returned a 4xx/5xx status code
    /// along with a structured error body.
    #[error("{context} ({status}). {detail}")]
    BackendResponse {
        /// Context describing the request that failed.
        context: &'static str,
        /// The HTTP status code returned by the backend.
        status: StatusCode,
        /// Parsed error code and message from the response body.
        detail: BackendDetail,
    },

    /// Errors related to de/serialization and parsing of object metadata.
    #[error("metadata error: {0}")]
    Metadata(#[from] objectstore_types::metadata::Error),

    /// Errors encountered when attempting to authenticate with GCP.
    #[error("GCP authentication error: {0}")]
    GcpAuth(#[from] gcp_auth::Error),

    /// A spawned service task panicked.
    #[error("service task failed: {0}")]
    Panic(String),

    /// A spawned service task was dropped before it could deliver its result.
    ///
    /// This is an unexpected condition that can occur when the runtime drops the task for unknown
    /// reasons.
    #[error("task dropped")]
    Dropped,

    /// A redirect tombstone was encountered at a place where it is not supported.
    ///
    /// This indicates a caller bug — tombstone-aware reads must go through the
    /// [`HighVolumeBackend`](crate::backend::common::HighVolumeBackend) methods.
    #[error("unexpected tombstone")]
    UnexpectedTombstone,

    /// The requested byte range is not satisfiable for the object's size.
    #[error("range not satisfiable (object size: {total} bytes)")]
    RangeNotSatisfiable {
        /// Total size of the object in bytes.
        total: u64,
    },

    /// The service has reached its concurrency limit and cannot accept more operations.
    #[error("concurrency limit reached")]
    AtCapacity,

    /// Any other error stemming from one of the storage backends, which might be specific to that
    /// backend or to a certain operation.
    #[error("storage backend error: {context}")]
    Generic {
        /// Context describing the operation that failed.
        context: Cow<'static, str>,
        /// The underlying error, if available.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The functionality is not implemented by this instance of the service.
    #[error("not implemented")]
    NotImplemented,

    /// Invalid upload ID (e.g. path traversal attempt).
    #[error(transparent)]
    InvalidUploadId(#[from] objectstore_types::multipart::InvalidUploadId),
}

impl Error {
    /// Creates an [`Error::Panic`] from a panic payload, extracting the message.
    pub fn panic(payload: Box<dyn Any + Send>) -> Self {
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_owned()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_owned()
        };
        Self::Panic(msg)
    }

    /// Creates an [`Error::Reqwest`] from a reqwest error with context.
    pub fn reqwest(context: &'static str, cause: reqwest::Error) -> Self {
        Self::Reqwest { context, cause }
    }

    /// Creates an [`Error::Serde`] from a serde error with context.
    pub fn serde(context: &'static str, cause: serde_json::Error) -> Self {
        Self::Serde { context, cause }
    }

    /// Creates an [`Error::Generic`] with a context string and no cause.
    pub fn generic(context: impl Into<Cow<'static, str>>) -> Self {
        Self::Generic {
            context: context.into(),
            cause: None,
        }
    }

    /// Creates an [`Error::Generic`] with a context string and a cause.
    pub fn generic_cause(
        context: impl Into<Cow<'static, str>>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Generic {
            context: context.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Returns the appropriate log level for this error.
    pub fn level(&self) -> Level {
        match self {
            // Malformed client input at DEBUG level
            Self::Client(_) => Level::DEBUG,
            Self::Metadata(_) => Level::DEBUG,
            Self::RangeNotSatisfiable { .. } => Level::DEBUG,
            // Like rate limits, we treat capacity errors as warnings
            Self::AtCapacity => Level::WARN,
            // All other errors are service or backend failures
            Self::Io(_) => Level::ERROR,
            Self::Serde { .. } => Level::ERROR,
            Self::Reqwest { .. } => Level::ERROR,
            Self::BackendResponse { .. } => Level::ERROR,
            Self::GcpAuth(_) => Level::ERROR,
            Self::Panic(_) => Level::ERROR,
            Self::Dropped => Level::ERROR,
            Self::UnexpectedTombstone => Level::ERROR,
            Self::NotImplemented => Level::ERROR,
            Self::InvalidUploadId(_) => Level::DEBUG,
            Self::Generic { .. } => Level::ERROR,
        }
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
