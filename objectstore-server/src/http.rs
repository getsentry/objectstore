use std::any::Any;
use std::io;
use std::net::SocketAddr;

use anyhow::Result;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::ObjectPath;
use objectstore_types::Metadata;
use sentry::integrations::tower as sentry_tower;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpSocket};
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;

use crate::config::Config;
use crate::state::ServiceState;

const TCP_LISTEN_BACKLOG: u32 = 1024;

#[derive(Deserialize, Debug)]
struct ContextParams {
    pub scope: String,
    pub usecase: String,
}

fn make_app(state: ServiceState) -> axum::Router {
    let middleware = ServiceBuilder::new()
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(sentry_tower::NewSentryLayer::<Request>::new_from_top())
        .layer(sentry_tower::SentryHttpLayer::new().enable_transaction())
        .layer(TraceLayer::new_for_http().on_failure(DefaultOnFailure::new().level(Level::DEBUG)));

    let routes = Router::new().route("/", put(put_object_nokey)).route(
        "/{*key}",
        put(put_object).get(get_object).delete(delete_object),
    );

    routes.layer(middleware).with_state(state)
}

/// Handler function for the [`CatchPanicLayer`] middleware.
fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    let detail = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "no error details".to_owned()
    };

    tracing::error!("panic in web handler: {detail}");

    let response = (StatusCode::INTERNAL_SERVER_ERROR, detail);
    response.into_response()
}

fn listen(config: &Config) -> Result<TcpListener> {
    let addr = config.http_addr;
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;

    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuseport(true)?;
    socket.bind(addr)?;

    let listener = socket.listen(TCP_LISTEN_BACKLOG)?;
    tracing::info!("HTTP server listening on {addr}");

    Ok(listener)
}

async fn serve(listener: TcpListener, app: axum::Router) -> Result<()> {
    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(guard.wait_owned())
        .await?;

    Ok(())
}

pub async fn server(state: ServiceState) -> Result<()> {
    let listener = listen(&state.config)?;

    let app = make_app(state);
    serve(listener, app).await
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    key: String,
}

#[tracing::instrument(level = "info", skip(state, body))]
async fn put_object_nokey(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    headers: HeaderMap,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key: uuid::Uuid::new_v4().to_string(),
    };
    let metadata = Metadata::from_headers(&headers, "")?;

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let key = state.service.put_object(path, &metadata, stream).await?;

    Ok(Json(PutBlobResponse {
        key: key.key.to_string(),
    }))
}

#[tracing::instrument(level = "info", skip(state, body))]
async fn put_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> error::Result<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };
    let metadata = Metadata::from_headers(&headers, "")?;

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let key = state.service.put_object(path, &metadata, stream).await?;

    Ok(Json(PutBlobResponse {
        key: key.key.to_string(),
    }))
}

#[tracing::instrument(level = "info", skip(state))]
async fn get_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
) -> error::Result<Response> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };

    let Some((metadata, stream)) = state.service.get_object(&path).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("", false)?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

#[tracing::instrument(level = "info", skip(state))]
async fn delete_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
) -> error::Result<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };

    state.service.delete_object(&path).await?;

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
