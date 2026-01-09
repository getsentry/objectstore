//! Error types for HTTP request handling.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

use objectstore_service::error::ServiceError;

use crate::auth::AuthError;

/// Top-level error for HTTP request handling.
///
/// Designed for flat matching - each variant maps to a specific HTTP status code.
#[derive(Debug, Error)]
pub enum RequestError {
    // === 400 Bad Request ===
    /// Invalid metadata in request headers.
    #[error("invalid metadata: {message}")]
    InvalidMetadata {
        /// Error message.
        message: String,
        /// Underlying metadata parsing error.
        #[source]
        cause: Option<objectstore_types::Error>,
    },

    /// Invalid request body or payload.
    #[error("invalid payload: {0}")]
    InvalidPayload(String),

    // === 401 Unauthorized ===
    /// Authentication token missing or invalid.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    // === 403 Forbidden ===
    /// Operation not permitted for this token.
    #[error("forbidden: {0}")]
    Forbidden(String),

    // === 413 Payload Too Large ===
    /// Request body exceeds size limit.
    #[error("payload too large: {0}")]
    PayloadTooLarge(String),

    // === 500 Internal Server Error ===
    /// Internal backend or service failure.
    #[error("internal error")]
    Internal(#[source] ServiceError),

    // === 502 Bad Gateway ===
    /// Upstream backend unavailable or returned error.
    #[error("backend unavailable")]
    BackendUnavailable(String),

    // === 503 Service Unavailable ===
    /// Service temporarily unavailable.
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl RequestError {
    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidMetadata { .. } | Self::InvalidPayload(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BackendUnavailable(_) => StatusCode::BAD_GATEWAY,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Relay-style JSON error response.
#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    /// Primary error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    /// Chain of underlying error causes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
}

impl ApiErrorResponse {
    /// Creates an error response from any error type.
    pub fn from_error<E: std::error::Error + ?Sized>(error: &E) -> Self {
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

impl IntoResponse for RequestError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        // Log server errors at error level, client errors at debug
        if status.is_server_error() {
            tracing::error!(
                error = &self as &dyn std::error::Error,
                "error handling request"
            );
        } else {
            tracing::debug!(
                error = &self as &dyn std::error::Error,
                "client error handling request"
            );
        }

        let body = ApiErrorResponse::from_error(&self);
        (status, axum::Json(body)).into_response()
    }
}

// === Conversions from other error types ===

impl From<AuthError> for RequestError {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::BadRequest(msg) => RequestError::InvalidPayload(msg.to_string()),
            AuthError::InitFailure(msg) | AuthError::InternalError(msg) => {
                RequestError::Internal(ServiceError::internal(msg))
            }
            AuthError::ValidationFailure(_) => RequestError::Unauthorized(err.to_string()),
            AuthError::VerificationFailure => {
                RequestError::Unauthorized("failed to verify token".to_string())
            }
            AuthError::NotPermitted => {
                RequestError::Forbidden("operation not allowed".to_string())
            }
        }
    }
}

impl From<ServiceError> for RequestError {
    fn from(err: ServiceError) -> Self {
        use objectstore_service::error::BackendError;

        match &err {
            ServiceError::StreamRead(_) => RequestError::InvalidPayload(err.to_string()),
            ServiceError::Backend(backend_err) => {
                // Map specific backend errors to appropriate HTTP codes
                match backend_err {
                    BackendError::Network { .. } | BackendError::InvalidResponse { .. } => {
                        RequestError::BackendUnavailable(backend_err.to_string())
                    }
                    _ => RequestError::Internal(err),
                }
            }
            ServiceError::Internal { .. } => RequestError::Internal(err),
        }
    }
}

impl From<objectstore_types::Error> for RequestError {
    fn from(err: objectstore_types::Error) -> Self {
        RequestError::InvalidMetadata {
            message: err.to_string(),
            cause: Some(err),
        }
    }
}
