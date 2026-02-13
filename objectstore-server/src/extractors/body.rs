use std::convert::Infallible;
use std::io;

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::PayloadStream;
use objectstore_service::id::ObjectContext;

use crate::state::ServiceState;

/// An extractor that converts the request body into a metered [`PayloadStream`].
///
/// This extractor reads the [`ObjectContext`] from request extensions (inserted by
/// [`Xt<ObjectId>`](super::Xt) or [`Xt<ObjectContext>`](super::Xt)) to set up per-key
/// bandwidth tracking. Since this extractor implements [`FromRequest`] (consuming the body),
/// it always runs after [`FromRequestParts`] extractors, so the context will always be available
/// when a handler also extracts an object ID or context.
pub struct MeteredBody(pub PayloadStream);

impl std::fmt::Debug for MeteredBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredBody").finish()
    }
}

impl FromRequest<ServiceState> for MeteredBody {
    type Rejection = Infallible;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let context = request.extensions().get::<ObjectContext>().cloned();
        let stream = Body::from_request(request, state)
            .await
            .expect("Body extraction is infallible")
            .into_data_stream()
            .map_err(io::Error::other)
            .boxed();
        let stream = match context {
            Some(ref ctx) => state.wrap_stream(stream, ctx),
            None => state.wrap_stream_global_only(stream),
        };
        Ok(Self(stream))
    }
}
