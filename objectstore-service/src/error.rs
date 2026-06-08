//! Error types for service and backend operations.
//!
//! [`Error`] covers I/O, serialization, HTTP, metadata, authentication,
//! and backend-specific failures. [`Result`] is the corresponding alias.

#![allow(missing_docs)]

use std::any::Any;

use objectstore_log::Level;
use reqwest::StatusCode;
use thiserror::Error as ThisError;

use crate::stream::{self, ClientError};

/// Coarse categories for service errors.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Error originating from a client-supplied request body stream.
    ClientStream,
    /// Backend or network failure that may succeed if retried.
    Transient,
    /// Malformed or unsupported client input.
    BadRequest,
    /// Operation unsupported by this service instance.
    NotImplemented,
    /// Service or upstream capacity limit.
    TooManyRequests,
    /// Internal service or backend failure.
    Internal,
}

/// Reqwest failure with service-level classification.
#[derive(Debug, ThisError)]
#[error("reqwest error: {context}")]
pub struct ReqwestError {
    kind: ErrorKind,
    context: String,
    #[source]
    cause: reqwest::Error,
}

impl ReqwestError {
    fn transparent(context: impl Into<String>, cause: reqwest::Error) -> Self {
        let kind = classify_reqwest(&cause);
        Self {
            kind,
            context: context.into(),
            cause,
        }
    }

    fn internal(context: impl Into<String>, cause: reqwest::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            context: context.into(),
            cause,
        }
    }

    /// Returns the service-level classification for this reqwest error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the underlying reqwest error.
    pub fn cause(&self) -> &reqwest::Error {
        &self.cause
    }

    /// Returns whether the underlying reqwest failure is worth retrying.
    pub fn is_retryable(&self) -> bool {
        is_retryable_reqwest(&self.cause)
    }
}

/// Serde failure with service-level classification.
#[derive(Debug, ThisError)]
#[error("serde error: {context}")]
pub struct SerdeError {
    kind: ErrorKind,
    context: String,
    #[source]
    cause: serde_json::Error,
}

impl SerdeError {
    fn internal(context: impl Into<String>, cause: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            context: context.into(),
            cause,
        }
    }

    fn client(context: impl Into<String>, cause: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::BadRequest,
            context: context.into(),
            cause,
        }
    }

    /// Returns the service-level classification for this serde error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the underlying serde error.
    pub fn cause(&self) -> &serde_json::Error {
        &self.cause
    }
}

/// Metadata failure with service-level classification.
#[derive(Debug, ThisError)]
#[error("metadata error: {context}")]
pub struct MetadataError {
    kind: ErrorKind,
    context: String,
    #[source]
    cause: objectstore_types::metadata::Error,
}

impl MetadataError {
    fn internal(context: impl Into<String>, cause: objectstore_types::metadata::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            context: context.into(),
            cause,
        }
    }

    fn client(context: impl Into<String>, cause: objectstore_types::metadata::Error) -> Self {
        Self {
            kind: ErrorKind::BadRequest,
            context: context.into(),
            cause,
        }
    }

    /// Returns the service-level classification for this metadata error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the underlying metadata error.
    pub fn cause(&self) -> &objectstore_types::metadata::Error {
        &self.cause
    }
}

/// Error type for service operations.
#[derive(Debug, ThisError, derive_error_kind::ErrorKind)]
#[error_kind(ErrorKind)]
pub enum Error {
    /// IO errors related to payload streaming or file operations.
    #[error("i/o error: {0}")]
    #[error_kind(ErrorKind, Internal)]
    Io(std::io::Error),

    /// Error originating from a client-supplied input stream.
    ///
    /// Indicates the client is at fault (e.g. dropped connection mid-upload) and should
    /// map to a 4xx response rather than a 5xx.
    #[error("error reading client stream: {0}")]
    #[error_kind(ErrorKind, ClientStream)]
    Client(#[from] ClientError),

    /// Serde errors.
    #[error(transparent)]
    #[error_kind(transparent)]
    Serde(#[from] SerdeError),

    /// Reqwest errors from backend HTTP calls.
    #[error(transparent)]
    #[error_kind(transparent)]
    Reqwest(#[from] ReqwestError),

    /// Metadata errors.
    #[error(transparent)]
    #[error_kind(transparent)]
    Metadata(#[from] MetadataError),

    /// Errors encountered when attempting to authenticate with GCP.
    #[error("GCP authentication error: {0}")]
    #[error_kind(ErrorKind, Internal)]
    GcpAuth(#[from] gcp_auth::Error),

    /// A spawned service task panicked.
    #[error("service task failed: {0}")]
    #[error_kind(ErrorKind, Internal)]
    Panic(String),

    /// A spawned service task was dropped before it could deliver its result.
    ///
    /// This is an unexpected condition that can occur when the runtime drops the task for unknown
    /// reasons.
    #[error("task dropped")]
    #[error_kind(ErrorKind, Internal)]
    Dropped,

    /// A redirect tombstone was encountered at a place where it is not supported.
    ///
    /// This indicates a caller bug — tombstone-aware reads must go through the
    /// [`HighVolumeBackend`](crate::backend::common::HighVolumeBackend) methods.
    #[error("unexpected tombstone")]
    #[error_kind(ErrorKind, Internal)]
    UnexpectedTombstone,

    /// The requested byte range is not satisfiable for the object's size.
    #[error("range not satisfiable (object size: {total} bytes)")]
    #[error_kind(ErrorKind, BadRequest)]
    RangeNotSatisfiable {
        /// Total size of the object in bytes.
        total: u64,
    },

    /// The service has reached its concurrency limit and cannot accept more operations.
    #[error("concurrency limit reached")]
    #[error_kind(ErrorKind, TooManyRequests)]
    AtCapacity,

    /// Any other error stemming from one of the storage backends, which might be specific to that
    /// backend or to a certain operation.
    #[error("storage backend error: {context}")]
    #[error_kind(ErrorKind, Internal)]
    Generic {
        /// Context describing the operation that failed.
        context: String,
        /// The underlying error, if available.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The functionality is not implemented by this instance of the service.
    #[error("not implemented")]
    #[error_kind(ErrorKind, NotImplemented)]
    NotImplemented,

    /// Invalid upload ID (e.g. path traversal attempt).
    #[error(transparent)]
    #[error_kind(ErrorKind, BadRequest)]
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

    /// Creates an internal [`Error`] from a reqwest error with context.
    pub fn reqwest(context: impl Into<String>, cause: reqwest::Error) -> Self {
        if let Some(client_error) = stream::unpack_client_error(&cause) {
            return Self::Client(client_error);
        }

        Self::Reqwest(ReqwestError::internal(context, cause))
    }

    /// Creates an [`Error`] from a reqwest error, preserving its backend status classification.
    pub fn reqwest_transparent(context: impl Into<String>, cause: reqwest::Error) -> Self {
        if let Some(client_error) = stream::unpack_client_error(&cause) {
            return Self::Client(client_error);
        }

        Self::Reqwest(ReqwestError::transparent(context, cause))
    }

    /// Creates an [`Error::Serde`] from a serde error with context.
    pub fn serde(context: impl Into<String>, cause: serde_json::Error) -> Self {
        Self::Serde(SerdeError::internal(context, cause))
    }

    /// Creates a client-classified [`Error::Serde`] from a serde error with context.
    pub fn serde_client(context: impl Into<String>, cause: serde_json::Error) -> Self {
        Self::Serde(SerdeError::client(context, cause))
    }

    /// Creates an [`Error::Metadata`] from a metadata error with context.
    pub fn metadata(context: impl Into<String>, cause: objectstore_types::metadata::Error) -> Self {
        Self::Metadata(MetadataError::internal(context, cause))
    }

    /// Creates a client-classified [`Error::Metadata`] from a metadata error with context.
    pub fn metadata_client(
        context: impl Into<String>,
        cause: objectstore_types::metadata::Error,
    ) -> Self {
        Self::Metadata(MetadataError::client(context, cause))
    }

    /// Creates an [`Error::Generic`] with a context string and no cause.
    pub fn generic(context: impl Into<String>) -> Self {
        Self::Generic {
            context: context.into(),
            cause: None,
        }
    }

    /// Returns the appropriate log level for this error.
    pub fn level(&self) -> Level {
        match self {
            // Malformed client input at DEBUG level
            Self::Client(_) => Level::DEBUG,
            Self::RangeNotSatisfiable { .. } => Level::DEBUG,
            // Like rate limits, we treat capacity errors as warnings
            Self::AtCapacity => Level::WARN,
            // All other errors are service or backend failures
            Self::Io(_) => Level::ERROR,
            Self::Serde(error) => match error.kind() {
                ErrorKind::BadRequest => Level::DEBUG,
                ErrorKind::ClientStream
                | ErrorKind::Transient
                | ErrorKind::NotImplemented
                | ErrorKind::TooManyRequests
                | ErrorKind::Internal => Level::ERROR,
            },
            Self::Reqwest(error) => match error.kind() {
                ErrorKind::BadRequest => Level::DEBUG,
                ErrorKind::TooManyRequests => Level::WARN,
                ErrorKind::ClientStream
                | ErrorKind::Transient
                | ErrorKind::NotImplemented
                | ErrorKind::Internal => Level::ERROR,
            },
            Self::Metadata(error) => match error.kind() {
                ErrorKind::BadRequest => Level::DEBUG,
                ErrorKind::ClientStream
                | ErrorKind::Transient
                | ErrorKind::NotImplemented
                | ErrorKind::TooManyRequests
                | ErrorKind::Internal => Level::ERROR,
            },
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

fn classify_reqwest(cause: &reqwest::Error) -> ErrorKind {
    if let Some(status) = cause.status() {
        return match status {
            StatusCode::TOO_MANY_REQUESTS => ErrorKind::TooManyRequests,
            StatusCode::REQUEST_TIMEOUT
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Transient,
            status if status.is_client_error() => ErrorKind::BadRequest,
            _ => ErrorKind::Internal,
        };
    }

    if cause.is_timeout() || cause.is_connect() || cause.is_request() {
        ErrorKind::Transient
    } else {
        ErrorKind::Internal
    }
}

fn is_retryable_reqwest(cause: &reqwest::Error) -> bool {
    if let Some(status) = cause.status() {
        return matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::REQUEST_TIMEOUT
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
    }

    cause.is_timeout() || cause.is_connect() || cause.is_request()
}

impl From<std::io::Error> for Error {
    fn from(cause: std::io::Error) -> Self {
        match stream::unpack_client_error(&cause) {
            Some(client_error) => Self::Client(client_error),
            None => Self::Io(cause),
        }
    }
}

impl From<objectstore_types::metadata::Error> for Error {
    fn from(cause: objectstore_types::metadata::Error) -> Self {
        Self::metadata("metadata operation failed", cause)
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::io::AsyncWriteExt;

    use super::*;

    fn serde_error() -> serde_json::Error {
        serde_json::from_str::<serde_json::Value>("{").unwrap_err()
    }

    fn metadata_error() -> objectstore_types::metadata::Error {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            objectstore_types::metadata::HEADER_EXPIRATION,
            "garbage".parse().unwrap(),
        );
        objectstore_types::metadata::Metadata::from_headers(&headers, "").unwrap_err()
    }

    async fn reqwest_status_error(status: StatusCode) -> reqwest::Error {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 {} test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            status.as_u16()
        );
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        reqwest::get(format!("http://{addr}"))
            .await
            .unwrap()
            .error_for_status()
            .unwrap_err()
    }

    async fn reqwest_decode_error() -> reqwest::Error {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nnot-json",
                )
                .await
                .unwrap();
        });

        reqwest::get(format!("http://{addr}"))
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap_err()
    }

    #[test]
    fn direct_variant_kinds() {
        assert_eq!(
            Error::Client(ClientError::new(io::Error::other("client"))).kind(),
            ErrorKind::ClientStream
        );
        assert_eq!(Error::AtCapacity.kind(), ErrorKind::TooManyRequests);
        assert_eq!(Error::NotImplemented.kind(), ErrorKind::NotImplemented);
        assert_eq!(
            Error::RangeNotSatisfiable { total: 10 }.kind(),
            ErrorKind::BadRequest
        );
        assert_eq!(Error::generic("backend").kind(), ErrorKind::Internal);
    }

    #[test]
    fn serde_helpers_have_distinct_kinds() {
        assert_eq!(
            Error::serde("stored json", serde_error()).kind(),
            ErrorKind::Internal
        );
        assert_eq!(
            Error::serde_client("request json", serde_error()).kind(),
            ErrorKind::BadRequest
        );
    }

    #[test]
    fn metadata_helpers_have_distinct_kinds() {
        assert_eq!(
            Error::metadata("stored metadata", metadata_error()).kind(),
            ErrorKind::Internal
        );
        assert_eq!(
            Error::metadata_client("request metadata", metadata_error()).kind(),
            ErrorKind::BadRequest
        );
    }

    #[tokio::test]
    async fn reqwest_helper_classifies_reqwest_failures_as_internal() {
        assert_eq!(
            Error::reqwest(
                "bad request",
                reqwest_status_error(StatusCode::BAD_REQUEST).await
            )
            .kind(),
            ErrorKind::Internal
        );
        assert_eq!(
            Error::reqwest(
                "too many requests",
                reqwest_status_error(StatusCode::TOO_MANY_REQUESTS).await
            )
            .kind(),
            ErrorKind::Internal
        );
        assert_eq!(
            Error::reqwest(
                "internal server error",
                reqwest_status_error(StatusCode::INTERNAL_SERVER_ERROR).await
            )
            .kind(),
            ErrorKind::Internal
        );
    }

    #[tokio::test]
    async fn reqwest_helper_preserves_retryability() {
        let Error::Reqwest(error) = Error::reqwest(
            "too many requests",
            reqwest_status_error(StatusCode::TOO_MANY_REQUESTS).await,
        ) else {
            panic!("expected reqwest error");
        };
        assert!(error.is_retryable());

        let Error::Reqwest(error) = Error::reqwest(
            "internal server error",
            reqwest_status_error(StatusCode::INTERNAL_SERVER_ERROR).await,
        ) else {
            panic!("expected reqwest error");
        };
        assert!(error.is_retryable());

        let Error::Reqwest(error) = Error::reqwest(
            "bad request",
            reqwest_status_error(StatusCode::BAD_REQUEST).await,
        ) else {
            panic!("expected reqwest error");
        };
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn reqwest_transparent_helper_classifies_statuses() {
        assert_eq!(
            Error::reqwest_transparent(
                "bad request",
                reqwest_status_error(StatusCode::BAD_REQUEST).await
            )
            .kind(),
            ErrorKind::BadRequest
        );
        assert_eq!(
            Error::reqwest_transparent(
                "too many requests",
                reqwest_status_error(StatusCode::TOO_MANY_REQUESTS).await
            )
            .kind(),
            ErrorKind::TooManyRequests
        );
        assert_eq!(
            Error::reqwest_transparent(
                "internal server error",
                reqwest_status_error(StatusCode::INTERNAL_SERVER_ERROR).await
            )
            .kind(),
            ErrorKind::Transient
        );
    }

    #[tokio::test]
    async fn reqwest_transparent_helper_classifies_no_status_decode_error_as_internal() {
        assert_eq!(
            Error::reqwest_transparent("decode", reqwest_decode_error().await).kind(),
            ErrorKind::Internal
        );
    }

    #[test]
    fn io_helper_preserves_wrapped_client_error() {
        let client_error = ClientError::new(io::Error::other("client disconnected"));
        let error = Error::from(io::Error::other(client_error));
        assert_eq!(error.kind(), ErrorKind::ClientStream);
    }
}
