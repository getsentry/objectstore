#![allow(unused)]

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use serde::Serialize;
use service::StorageService;
use uuid::Uuid;

use crate::config::Config;

pub async fn start_server(config: Arc<Config>, service: Arc<StorageService>) {
    let app = Router::new()
        .route("/{usecase}/{scope}", put(put_blob_no_key))
        .route("/{usecase}/{scope}/{*key}", put(put_blob).get(get_blob))
        .with_state(Arc::clone(&service))
        .into_make_service();

    println!("HTTP server listening on {}", config.http_addr);
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
    println!("HTTP server shut down");
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    key: String,
}

async fn put_blob_no_key(
    State(service): State<Arc<StorageService>>,
    Path((usecase, scope)): Path<(String, String)>,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let key = Uuid::new_v4();
    let key = format!("{usecase}/{scope}/{key}");

    let contents = to_bytes(body, usize::MAX).await?;
    service.put_file(&key, &contents)?;

    Ok(Json(PutBlobResponse { key }))
}

async fn put_blob(
    State(service): State<Arc<StorageService>>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let key = format!("{usecase}/{scope}/{key}");

    let contents = to_bytes(body, usize::MAX).await?;
    service.put_file(&key, &contents)?;

    Ok(Json(PutBlobResponse { key }))
}

async fn get_blob(
    State(service): State<Arc<StorageService>>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
) -> error::Result<Response> {
    let key = format!("{usecase}/{scope}/{key}");

    let Some(contents) = service.get_file(&key)? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    Ok(contents.into_response())
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
