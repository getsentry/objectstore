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
use objectstore_service::StorageService;
use sentry::integrations::tower as sentry_tower;
use serde::Serialize;
use uuid::Uuid;

use crate::config::Config;

pub async fn start_server(config: Arc<Config>, service: StorageService) {
    let sentry_tower_service = config.sentry_dsn.as_ref().map(|_| {
        tower::ServiceBuilder::new()
            .layer(sentry_tower::NewSentryLayer::<Request>::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::new().enable_transaction())
    });

    let app = Router::new()
        .route("/{usecase}/{scope}", put(put_blob_no_key))
        .route(
            "/{usecase}/{scope}/{*key}",
            put(put_blob).get(get_blob).delete(delete_blob),
        )
        .layer(option_layer(sentry_tower_service))
        .with_state(service)
        .into_make_service();

    tracing::info!("HTTP server listening on {}", config.http_addr);
    let _guard = elegant_departure::get_shutdown_guard();
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap();
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
    State(service): State<StorageService>,
    Path((usecase, scope)): Path<(String, String)>,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let key = Uuid::new_v4();
    let key = format!("{usecase}/{scope}/{key}");

    let stream = body.into_data_stream().map_err(anyhow::Error::from).boxed();
    service.put_file(&key, stream).await?;
    Ok(Json(PutBlobResponse { key }))
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn put_blob(
    State(service): State<StorageService>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let key = format!("{usecase}/{scope}/{key}");

    let stream = body.into_data_stream().map_err(anyhow::Error::from).boxed();
    service.put_file(&key, stream).await?;
    Ok(Json(PutBlobResponse { key }))
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn get_blob(
    State(service): State<StorageService>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
) -> error::Result<Response> {
    let key = format!("{usecase}/{scope}/{key}");

    let Some(contents) = service.get_file(&key).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    Ok(Body::from_stream(contents).into_response())
}

#[tracing::instrument(skip_all, fields(usecase, scope, key))]
async fn delete_blob(
    State(service): State<StorageService>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
) -> error::Result<impl IntoResponse> {
    let key = format!("{usecase}/{scope}/{key}");

    service.delete_file(&key).await?;

    Ok(())
}

mod error {
    // This is mostly adapted from <https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs>

    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};

    pub struct AnyhowError(anyhow::Error);

    pub type Result<T> = std::result::Result<T, AnyhowError>;

    impl IntoResponse for AnyhowError {
        fn into_response(self) -> Response {
            eprintln!("{:?}", self.0); // TODO: this is just temporary
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }

    impl<E> From<E> for AnyhowError
    where
        E: Into<anyhow::Error>,
    {
        fn from(err: E) -> Self {
            Self(err.into())
        }
    }
}
