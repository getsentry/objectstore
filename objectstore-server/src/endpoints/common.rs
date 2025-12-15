//!
//! This is mostly adapted from <https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs>

use axum::extract::multipart::MultipartError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use objectstore_service::DeleteResult;

pub enum AnyhowResponse {
    Error(anyhow::Error),
    Response(Response),
}

pub type ApiResult<T> = std::result::Result<T, AnyhowResponse>;

impl IntoResponse for AnyhowResponse {
    fn into_response(self) -> Response {
        match self {
            AnyhowResponse::Error(error) => {
                tracing::error!(
                    error = error.as_ref() as &dyn std::error::Error,
                    "error handling request"
                );

                // TODO: Support more nuanced return codes for validation errors etc. See
                // Relay's ApiErrorResponse and BadStoreRequest as examples.
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            AnyhowResponse::Response(response) => response,
        }
    }
}

impl From<Response> for AnyhowResponse {
    fn from(response: Response) -> Self {
        Self::Response(response)
    }
}

impl From<anyhow::Error> for AnyhowResponse {
    fn from(err: anyhow::Error) -> Self {
        Self::Error(err)
    }
}

impl From<MultipartError> for AnyhowResponse {
    fn from(value: MultipartError) -> Self {
        Self::Error(value.into())
    }
}
