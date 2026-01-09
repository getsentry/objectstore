use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing;
use objectstore_service::id::ObjectContext;

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::{BatchRequest, Xt};
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
}

async fn batch(
    _service: AuthAwareService,
    Xt(_context): Xt<ObjectContext>,
    _request: BatchRequest,
) -> ApiResult<Response> {
    Ok(StatusCode::NOT_IMPLEMENTED.into_response())
}
