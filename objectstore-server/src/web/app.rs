use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use axum::ServiceExt;
use axum::extract::Request;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::metrics::InFlightRequestsLayer;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;

use crate::endpoints;
use crate::state::ServiceState;
use crate::web::middleware as m;

/// Interval for emitting the in-flight requests gauge metric.
const IN_FLIGHT_INTERVAL: Duration = Duration::from_secs(1);

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
            .layer(axum::middleware::from_fn(m::emit_request_metrics))
            .layer(in_flight_layer)
            .layer(CatchPanicLayer::custom(m::handle_panic))
            .layer(m::set_server_header())
            .layer(NewSentryLayer::new_from_top())
            .layer(SentryHttpLayer::new().enable_transaction())
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(m::make_http_span)
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

        let guard = if graceful_shutdown {
            Some(elegant_departure::get_shutdown_guard())
        } else {
            None
        };

        let server = async {
            if let Some(ref guard) = guard {
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
