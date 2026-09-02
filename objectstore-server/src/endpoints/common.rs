//! Common types and utilities for API endpoints.

use std::error::Error;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use objectstore_service::error::{Error as ServiceError, ErrorKind as ServiceErrorKind};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::AuthError;
use crate::extractors::batch::BatchError;

/// Error type for API operations.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Errors indicating malformed or illegal requests.
    #[error("client error: {0}")]
    Client(String),

    /// Authorization/authentication errors.
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),

    /// Service errors, indicating that something went wrong when receiving or executing a request.
    #[error("service error: {0}")]
    Service(#[from] ServiceError),

    /// Errors encountered when parsing or executing a batch request.
    #[error("batch error: {0}")]
    Batch(#[from] BatchError),

    /// Internal server errors.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for API operations.
pub type ApiResult<T> = Result<T, ApiError>;

/// A JSON error response returned by the API.
#[derive(Serialize, Deserialize, Debug)]
pub struct ApiErrorResponse {
    /// The main error message.
    #[serde(default)]
    detail: Option<String>,
    /// Chain of error causes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
}

impl ApiErrorResponse {
    /// Creates an error response from an error, extracting the full cause chain.
    pub fn from_error<E: Error + ?Sized>(error: &E) -> Self {
        let detail = Some(error.to_string());

        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(s) = source {
            causes.push(s.to_string());
            source = s.source();
        }

        Self { detail, causes }
    }
}

impl ApiError {
    /// Returns the HTTP status code appropriate for this error variant.
    pub fn status(&self) -> StatusCode {
        match &self {
            ApiError::Client(_) => StatusCode::BAD_REQUEST,

            ApiError::Batch(BatchError::BadRequest(_))
            | ApiError::Batch(BatchError::Metadata(_))
            | ApiError::Batch(BatchError::Multipart(_)) => StatusCode::BAD_REQUEST,
            ApiError::Batch(BatchError::LimitExceeded(_)) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Batch(BatchError::RateLimited) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Batch(BatchError::ResponseSerialization { .. }) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnknownKey) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnsupportedPresignedMethod) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::NotPermitted) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::InternalError(_)) => StatusCode::INTERNAL_SERVER_ERROR,

            ApiError::Service(error) => match error.kind() {
                ServiceErrorKind::InvalidMetadata
                | ServiceErrorKind::InvalidUploadId
                | ServiceErrorKind::ClientStream
                | ServiceErrorKind::UnknownUploadSession
                | ServiceErrorKind::ChunkExceedsUploadLength { .. } => StatusCode::BAD_REQUEST,
                ServiceErrorKind::RangeNotSatisfiable { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
                ServiceErrorKind::UploadOffsetMismatch { .. } => StatusCode::CONFLICT,
                ServiceErrorKind::UploadSessionGone => StatusCode::GONE,
                ServiceErrorKind::AtCapacity => StatusCode::TOO_MANY_REQUESTS,
                ServiceErrorKind::Unsupported => StatusCode::NOT_IMPLEMENTED,
                ServiceErrorKind::BackendRateLimited => StatusCode::TOO_MANY_REQUESTS,
                ServiceErrorKind::BackendTimeout | ServiceErrorKind::BackendUnavailable => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                ServiceErrorKind::BackendFailure
                | ServiceErrorKind::CorruptData
                | ServiceErrorKind::Panic
                | ServiceErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },

            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Reports this error to error tracking if it indicates a server fault (5xx status).
    ///
    /// Call this exactly once wherever an `ApiError` is serialized into a client-visible
    /// response: standalone responses ([`IntoResponse`]) and batch response parts.
    pub fn capture(&self) {
        // Captured at the source in the service layer to prevent double-logging.
        if matches!(self, ApiError::Service(_)) {
            return;
        }

        if self.status().is_server_error() {
            objectstore_log::error!(!!self, "error handling request");
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        self.capture();
        let body = ApiErrorResponse::from_error(&self);
        (self.status(), Json(body)).into_response()
    }
}

/// Inserts `Accept-Ranges: bytes` into the response headers.
pub fn insert_accept_ranges(response: &mut Response) {
    response.headers_mut().insert(
        http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumable_service_errors_map_to_protocol_statuses() {
        let cases = [
            (
                ServiceErrorKind::UnknownUploadSession,
                StatusCode::BAD_REQUEST,
            ),
            (
                ServiceErrorKind::ChunkExceedsUploadLength {
                    offset: 8,
                    content_length: 4,
                    upload_length: 10,
                },
                StatusCode::BAD_REQUEST,
            ),
            (
                ServiceErrorKind::UploadOffsetMismatch { offset: 8 },
                StatusCode::CONFLICT,
            ),
            (ServiceErrorKind::UploadSessionGone, StatusCode::GONE),
            (ServiceErrorKind::Unsupported, StatusCode::NOT_IMPLEMENTED),
        ];

        for (kind, expected) in cases {
            assert_eq!(ApiError::Service(kind.into()).status(), expected);
        }
    }
}
