//! Common types and utilities for API endpoints.

use std::error::Error;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use objectstore_service::ServiceError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::AuthError;

/// Error type for API operations.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Errors indicating malformed or illegal requests.
    #[error("client error: {0}")]
    Client(String),

    /// Authorization/authentication errors.
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),

    /// Server errors, indicating that something went wrong when receiving or executing a request.
    #[error("server error: {0}")]
    Server(#[source] Box<dyn Error + Send + Sync>),
}

impl From<ServiceError> for ApiError {
    fn from(err: ServiceError) -> Self {
        ApiError::Server(Box::new(err))
    }
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

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Client(_) => StatusCode::BAD_REQUEST,

            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::NotPermitted) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::InternalError(_)) => {
                tracing::error!(error = &self as &dyn Error, "auth system error");
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Server(_) => {
                tracing::error!(error = &self as &dyn Error, "error handling request");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        let body = ApiErrorResponse::from_error(&self);
        (status, Json(body)).into_response()
    }
}
