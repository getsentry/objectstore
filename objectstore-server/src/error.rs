//! Error types for the objectstore API layer.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use objectstore_service::ServiceError;
use thiserror::Error;

use crate::auth::AuthError;

/// Error type for API operations, encompassing service, auth, and rate limiting errors.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Errors from the service layer (storage backends, streaming, etc.).
    #[error("service error: {0}")]
    Service(#[from] ServiceError),

    /// Authorization/authentication errors.
    #[error("authorization error: {0}")]
    Auth(#[from] AuthError),

    /// Errors related to metadata extraction or validation.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Rate limiting errors.
    #[error("rate limit exceeded")]
    RateLimited,
}

/// Result type for API operations.
pub type ApiResult<T> = Result<T, ApiError>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Service(err) => {
                // Log service errors as they're unexpected
                tracing::error!(
                    error = err as &dyn std::error::Error,
                    "service error handling request"
                );
                StatusCode::INTERNAL_SERVER_ERROR
            }
            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure)
            | ApiError::Auth(AuthError::NotPermitted) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::InitFailure(_))
            | ApiError::Auth(AuthError::InternalError(_)) => {
                tracing::error!(error = &self as &dyn std::error::Error, "auth system error");
                StatusCode::INTERNAL_SERVER_ERROR
            }
            ApiError::BadRequest(msg) => {
                tracing::debug!("bad request: {}", msg);
                StatusCode::BAD_REQUEST
            }
            ApiError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        };

        status.into_response()
    }
}
