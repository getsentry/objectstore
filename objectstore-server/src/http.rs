use std::any::Any;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{ConnectInfo, MatchedPath, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{RequestExt, ServiceExt};
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use tokio::net::{TcpListener, TcpSocket};
use tokio::time::Instant;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::metrics::InFlightRequestsLayer;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;

use crate::config::Config;
use crate::endpoints;
use crate::state::ServiceState;

/// The maximum backlog for TCP listen sockets before refusing connections.
const TCP_LISTEN_BACKLOG: u32 = 1024;

/// Interval for emitting the in-flight requests gauge metric.
const IN_FLIGHT_INTERVAL: Duration = Duration::from_secs(1);

/// The value for the `Server` HTTP header.
const SERVER: &str = concat!("objectstore/", env!("CARGO_PKG_VERSION"));

/// Runs the objectstore HTTP server.
///
/// This function creates a future that runs the server. The future must be spawned or awaited for
/// the server to continue running.
pub async fn server(state: ServiceState) -> Result<()> {
    merni::counter!("server.start": 1);
    let listener = listen(&state.config)?;

    App::new(state)
        .graceful_shutdown(true)
        .serve(listener)
        .await
}

/// The objectstore web server application.
#[derive(Debug)]
pub struct App {
    router: axum::Router,
    in_flight_requests: InFlightRequestsCounter,
    graceful_shutdown: bool,
}

impl App {
    /// Creates a new application router for the given service state.
    ///
    /// The applications sets up middlewares and routes for the objectstore web API. Use
    /// [`serve`](Self::serve) to run the server future.
    pub fn new(state: ServiceState) -> Self {
        let (in_flight_layer, in_flight_requests) = InFlightRequestsLayer::pair();

        // Build the router middleware into a single service which runs _after_ routing. Service
        // builder order defines layers added first will be called first. This means:
        //  - Requests go from top to bottom
        //  - Responses go from bottom to top
        let middleware = ServiceBuilder::new()
            .layer(axum::middleware::from_fn(emit_request_metrics))
            .layer(in_flight_layer)
            .layer(CatchPanicLayer::custom(handle_panic))
            .layer(SetResponseHeaderLayer::overriding(
                header::SERVER,
                HeaderValue::from_static(SERVER),
            ))
            .layer(NewSentryLayer::new_from_top())
            .layer(SentryHttpLayer::new().enable_transaction())
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(make_http_span)
                    .on_failure(DefaultOnFailure::new().level(Level::DEBUG)),
            );

        let router = endpoints::routes().layer(middleware).with_state(state);

        App {
            router,
            in_flight_requests,
            graceful_shutdown: false,
        }
    }

    /// Enables or disables graceful shutdown for the server.
    ///
    /// By default, graceful shutdown is disabled.
    pub fn graceful_shutdown(mut self, enable: bool) -> Self {
        self.graceful_shutdown = enable;
        self
    }

    /// Runs the web server until graceful shutdown is triggered.
    ///
    /// This function creates a future that runs the server. The future must be spawned or awaited for
    /// the server to continue running.
    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        let Self {
            router,
            in_flight_requests,
            graceful_shutdown,
        } = self;

        let service =
            ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(router);

        let server = async move {
            if graceful_shutdown {
                let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
                axum::serve(listener, service)
                    .with_graceful_shutdown(guard.wait_owned())
                    .await
            } else {
                axum::serve(listener, service).await
            }
        };

        let emitter = in_flight_requests.run_emitter(IN_FLIGHT_INTERVAL, |count| async move {
            merni::gauge!("server.requests.in_flight": count);
        });

        let (serve_result, _) = tokio::join!(server, emitter);
        serve_result?;

        Ok(())
    }
}

/// Create a tracing span for an HTTP request.
///
/// As opposed to `DefaultMakeSpan`, this also records the client IP address if available.
fn make_http_span(request: &Request) -> tracing::Span {
    let span = tracing::debug_span!(
        "request",
        method = %request.method(),
        uri = %request.uri(),
        version = ?request.version(),
        client_addr = tracing::field::Empty,
    );

    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
    {
        span.record("client_addr", tracing::field::display(addr.ip()));
    }

    span
}

/// A panic handler that logs the panic and turns it into a 500 response.
///
/// Use with the [`CatchPanicLayer`] middleware.
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

/// A middleware that logs web request timings as metrics.
///
/// Use this with [`from_fn`](axum::middleware::from_fn).
async fn emit_request_metrics(mut request: Request, next: Next) -> Response {
    let request_start = Instant::now();

    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let method = request.method().clone();
    merni::counter!("server.requests": 1, "route" => route, "method" => method.as_str());

    let response = next.run(request).await;

    merni::distribution!(
        "server.requests.duration"@s: request_start.elapsed(),
        "route" => route,
        "method" => method.as_str(),
        "status" => response.status().as_str()
    );

    response
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
