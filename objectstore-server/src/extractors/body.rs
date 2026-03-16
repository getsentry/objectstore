//! Axum extractor for bandwidth-metered request bodies.

use std::convert::Infallible;

use axum::extract::{FromRequest, FromRequestParts, Path, Request};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::id::ObjectContext;
use objectstore_service::stream::{ClientError, ClientStream};

use super::id::ContextParams;
use crate::state::ServiceState;

/// An extractor that converts the request body into a metered [`ClientStream`].
///
/// Extracts the [`ObjectContext`] from the request path to attribute bandwidth to the correct
/// per-usecase and per-scope accumulators in addition to the global accumulator.
pub struct MeteredBody(pub ClientStream);

impl std::fmt::Debug for MeteredBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredBody").finish()
    }
}

impl FromRequest<ServiceState> for MeteredBody {
    type Rejection = Infallible;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = request.into_parts();
        let Path(params) =
            <Path<ContextParams> as FromRequestParts<ServiceState>>::from_request_parts(
                &mut parts, state,
            )
            .await
            .expect("MeteredBody must be used on routes with {usecase} and {scopes} path params");
        let context = ObjectContext {
            usecase: params.usecase,
            scopes: params.scopes,
        };
        let stream = body.into_data_stream().map_err(ClientError::new).boxed();
        let stream = state.meter_stream(stream, &context).boxed();
        Ok(Self(stream))
    }
}
