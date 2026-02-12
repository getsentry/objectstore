use std::convert::Infallible;
use std::io;

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::PayloadStream;

use crate::state::ServiceState;

/// An extractor that converts the request body into a metered [`PayloadStream`].
pub struct MeteredBody(pub PayloadStream);

impl std::fmt::Debug for MeteredBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredBody").finish()
    }
}

impl FromRequest<ServiceState> for MeteredBody {
    type Rejection = Infallible;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let stream = Body::from_request(request, state)
            .await
            .expect("Body extraction is infallible")
            .into_data_stream()
            .map_err(io::Error::other)
            .boxed();
        let stream = state.meter_ingress_stream(stream);
        Ok(Self(stream))
    }
}
