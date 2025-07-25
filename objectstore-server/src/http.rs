#![allow(unused)]

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use axum_extra::middleware::option_layer;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::{ObjectKey, StorageService};
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
        .route("/", put(put_blob_no_key))
        .route("/{*key}", put(put_blob).get(get_blob).delete(delete_blob))
        .layer(option_layer(sentry_tower_service))
        .with_state(state)
        .into_make_service();

    tracing::info!("HTTP server listening on {http_addr}");
    let _guard = elegant_departure::get_shutdown_guard();
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let guard = elegant_departure::get_shutdown_guard();
            guard.wait().await;
        })
        .await
        .unwrap();
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    key: String,
}

#[tracing::instrument(skip_all, fields(usecase, scope))]
async fn put_blob_no_key(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    body: Body,
) -> error::Result<impl IntoResponse> {
    claim.ensure_permission(Permission::Write)?;
    let key = claim.into_key(Uuid::new_v4().to_string());

    let stream = body.into_data_stream().map_err(anyhow::Error::from).boxed();
    state.service.put_file(&key, stream).await?;

    Ok(Json(PutBlobResponse { key: key.key }))
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn put_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    Path(key): Path<String>,
    body: Body,
) -> error::Result<impl IntoResponse> {
    claim.ensure_permission(Permission::Write)?;
    let key = claim.into_key(key);

    let stream = body.into_data_stream().map_err(anyhow::Error::from).boxed();
    state.service.put_file(&key, stream).await?;

    Ok(Json(PutBlobResponse { key: key.key }))
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn get_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    Path(key): Path<String>,
) -> error::Result<Response> {
    claim.ensure_permission(Permission::Read)?;
    let key = claim.into_key(key);

    let Some(contents) = state.service.get_file(&key).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    Ok(Body::from_stream(contents).into_response())
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn delete_blob(
    State(state): State<ServiceState>,
    ExtractScope(claim): ExtractScope,
    Path(key): Path<String>,
) -> error::Result<impl IntoResponse> {
    claim.ensure_permission(Permission::Write)?;
    let key = claim.into_key(key);

    state.service.delete_file(&key).await?;

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
                AnyhowResponse::Error(_error) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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
