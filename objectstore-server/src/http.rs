#![allow(unused)]

use std::any::Any;
use std::io;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use axum_extra::middleware::option_layer;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::{ObjectKey, StorageService};
use objectstore_types::Metadata;
use sentry::integrations::tower as sentry_tower;
use serde::Serialize;
use uuid::Uuid;

use crate::authentication::{Claim, ExtractScope, Permission};
use crate::config::Config;
use crate::state::ServiceState;

pub async fn start_server(state: ServiceState) {
    let sentry_tower_service = state.config.sentry_dsn.as_ref().map(|_| {
        tower::ServiceBuilder::new()
            .layer(sentry_tower::NewSentryLayer::<Request>::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::new().enable_transaction())
    });
    let http_addr = state.config.http_addr;

    let app = Router::new()
        .route("/", put(put_blob))
        .route("/{*key}", get(get_blob).delete(delete_blob))
        .layer(option_layer(sentry_tower_service))
        .with_state(state)
        .into_make_service();

    tracing::info!("HTTP server listening on {http_addr}");
    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(guard.wait_owned())
        .await
        .unwrap();
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    key: String,
}

#[tracing::instrument(level = "trace", skip(state, body))]
async fn put_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    headers: HeaderMap,
    body: Body,
) -> error::Result<impl IntoResponse> {
    claim.ensure_permission(Permission::Write)?;
    let key = claim.into_key(Uuid::new_v4().to_string());
    let metadata = Metadata::from_headers(&headers, "")?;

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    state.service.put_object(&key, &metadata, stream).await?;

    Ok(Json(PutBlobResponse { key: key.key }))
}

#[tracing::instrument(level = "trace", skip(state))]
async fn get_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    Path(key): Path<String>,
) -> error::Result<Response> {
    claim.ensure_permission(Permission::Read)?;
    let key = claim.into_key(key);

    let Some((metadata, stream)) = state.service.get_object(&key).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("", false)?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

#[tracing::instrument(level = "trace", skip_all, fields(usecase, scope, key))]
async fn delete_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    Path(key): Path<String>,
) -> error::Result<impl IntoResponse> {
    claim.ensure_permission(Permission::Write)?;
    let key = claim.into_key(key);

    state.service.delete_object(&key).await?;

    Ok(())
}

mod error {
    // This is mostly adapted from <https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs>

    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};

    pub enum AnyhowResponse {
        Error(anyhow::Error),
        Response(Response),
    }

    pub type Result<T> = std::result::Result<T, AnyhowResponse>;

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
                    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
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
}
