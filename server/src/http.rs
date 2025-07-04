#![allow(unused)]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::routing::put;
use service::StorageService;

use crate::config::Config;

pub async fn start_server(config: Arc<Config>, service: Arc<StorageService>) {
    let app = Router::new()
        .route("/{usecase}/{scope}/{*key}", put(put_blob).get(get_blob))
        .with_state(Arc::clone(&service))
        .into_make_service();

    println!("HTTP server listening on {}", config.http_addr);
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(elegant_departure::tokio::depart().on_termination())
        .await
        .unwrap();
    println!("HTTP server shut down");
}

async fn put_blob(
    State(service): State<Arc<StorageService>>,
    Path((usecase, scope, key)): Path<(String, String, Option<String>)>,
    body: Body,
) {
}

async fn get_blob(
    State(service): State<Arc<StorageService>>,
    Path((usecase, scope, key)): Path<(String, String, String)>,
) {
}
