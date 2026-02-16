//! Common types and utilities for API endpoints.

use std::error::Error;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use objectstore_service::ServiceError;
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
    pub fn status(&self) -> StatusCode {
        match &self {
            ApiError::Client(_) => StatusCode::BAD_REQUEST,

            ApiError::Batch(BatchError::BadRequest(_))
            | ApiError::Batch(BatchError::Metadata(_))
            | ApiError::Batch(BatchError::Multipart(_)) => StatusCode::BAD_REQUEST,
            ApiError::Batch(BatchError::LimitExceeded(_)) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Batch(BatchError::RateLimited) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Batch(BatchError::ResponseSerialization { .. }) => {
                tracing::error!(
                    error = self as &dyn Error,
                    "error serializing batch response"
                );
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::NotPermitted) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::InternalError(_)) => {
                tracing::error!(error = self as &dyn Error, "auth system error");
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Service(ServiceError::Metadata(_)) => StatusCode::BAD_REQUEST,

            ApiError::Service(_) => {
                tracing::error!(error = self as &dyn Error, "error handling request");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ApiErrorResponse::from_error(&self);
        (self.status(), Json(body)).into_response()
    }
}
