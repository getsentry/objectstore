use std::convert::Infallible;
use std::io;

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::PayloadStream;

use crate::state::ServiceState;

/// An extractor that converts the request body into a metered [`PayloadStream`].
///
/// This extractor reads the [`ObjectContext`](objectstore_service::id::ObjectContext) from request
/// extensions (inserted by the [`Xt<ObjectContext>`](super::Xt) or [`Xt<ObjectId>`](super::Xt)
/// extractors) to attribute bandwidth to the correct per-usecase and per-scope accumulators.
///
/// If no context is found in extensions (e.g. the handler doesn't use an `Xt` extractor),
/// only the global bandwidth accumulator is used.
pub struct MeteredBody(pub PayloadStream);

impl std::fmt::Debug for MeteredBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredBody").finish()
    }
}

impl FromRequest<ServiceState> for MeteredBody {
    type Rejection = Infallible;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let context = request.extensions().get().cloned();
        let stream = Body::from_request(request, state)
            .await
            .expect("Body extraction is infallible")
            .into_data_stream()
            .map_err(io::Error::other)
            .boxed();
        let stream = state.meter_stream(stream, context.as_ref());
        Ok(Self(stream))
    }
}
