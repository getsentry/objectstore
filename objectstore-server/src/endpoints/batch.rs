use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing;
use objectstore_service::id::ObjectContext;

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::{BatchRequest, Xt};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

async fn batch(
    _service: AuthAwareService,
    Xt(_context): Xt<ObjectContext>,
    _request: BatchRequest,
) -> ApiResult<Response> {
    Ok(StatusCode::NOT_IMPLEMENTED.into_response())
}
