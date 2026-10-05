use std::net::SocketAddr;

use anyhow::Result;
use axum::ServiceExt;
use axum::extract::Request;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;

use crate::endpoints;
use crate::state::ServiceState;
use crate::web::middleware as m;

/// The objectstore web server application.
#[derive(Debug)]
pub struct App {
    router: axum::Router,
    graceful_shutdown: bool,
}

impl App {
    /// Creates a new application router for the given service state.
    ///
    /// The applications sets up middlewares and routes for the objectstore web API. Use
    /// [`serve`](Self::serve) to run the server future.
    pub fn new(state: ServiceState) -> Self {
        // Build the router middleware into a single service which runs _after_ routing. Service
        // builder order defines layers added first will be called first. This means:
        //  - Requests go from top to bottom
        //  - Responses go from bottom to top
        let middleware = ServiceBuilder::new()
            .layer(axum::middleware::from_fn(m::capture_request_time))
            .layer(NewSentryLayer::new_from_top())
            .layer(SentryHttpLayer::new().enable_transaction())
            .layer(axum::middleware::from_fn(m::emit_request_metrics))
            .layer(axum::middleware::from_fn(m::bind_sentry_body))
            .layer(axum::middleware::from_fn_with_state(
                state.request_counter.clone(),
                m::limit_web_concurrency,
            ))
            .layer(state.request_counter.layer())
            .layer(CatchPanicLayer::custom(m::handle_panic))
            .layer(m::set_server_header());

        let router = endpoints::routes()
            .layer(middleware)
            .with_state(state.clone());

        App {
            router,
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
            graceful_shutdown,
        } = self;

        let service =
            ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(router);

        if graceful_shutdown {
            let guard = elegant_departure::get_shutdown_guard();
            axum::serve(listener, service)
                .with_graceful_shutdown(guard.wait_owned())
                .await?;
        } else {
            axum::serve(listener, service).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::StatusCode;
    use objectstore_log::tracing;
    use objectstore_service::backend::local_fs::FileSystemConfig;
    use sentry::protocol::{Context, EnvelopeItem, TraceContext, Transaction};
    use tower::ServiceExt;
    use tracing_subscriber::prelude::*;

    use super::*;
    use crate::config::{AuthZ, Config, StorageConfig};
    use crate::state::Services;

    fn trace<'a>(transaction: &'a Transaction<'_>) -> &'a TraceContext {
        let Some(Context::Trace(trace)) = transaction.contexts.get("trace") else {
            panic!("transaction is missing its trace context");
        };
        trace
    }

    #[test]
    fn body_task_preserves_sentry_context() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _subscriber = tracing_subscriber::registry()
            .with(
                sentry::integrations::tracing::layer()
                    .span_filter(|metadata| *metadata.level() != tracing::Level::TRACE),
            )
            .set_default();
        let trace_id = "11111111111111111111111111111111";
        let caller_span = "aaaaaaaaaaaaaaaa";
        let envelopes = sentry::test::with_captured_envelopes_options(
            || {
                runtime.block_on(async {
                    let directory = tempfile::tempdir().unwrap();
                    let state = Services::spawn(Config {
                        storage: StorageConfig::FileSystem(FileSystemConfig {
                            path: directory.path().into(),
                            cogs: None,
                        }),
                        auth: AuthZ {
                            enforce: false,
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                    let request = Request::builder()
                        .method("POST")
                        .uri("/v1/objects:batch/test/org=1/")
                        .header("sentry-trace", format!("{trace_id}-{caller_span}-1"))
                        .header("content-type", "multipart/form-data; boundary=boundary")
                        .body(Body::from(concat!(
                            "--boundary\r\n",
                            "x-sn-batch-operation-key: missing\r\n",
                            "x-sn-batch-operation-kind: head\r\n",
                            "\r\n\r\n--boundary--\r\n",
                        )))
                        .unwrap();
                    let response = App::new(state).router.oneshot(request).await.unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    // Polling the batch body starts the task after the HTTP transaction ends.
                    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                    assert!(String::from_utf8_lossy(&body).contains("404 Not Found"));
                })
            },
            sentry::ClientOptions {
                traces_sample_rate: 1.0,
                ..Default::default()
            },
        );
        let transactions: Vec<_> = envelopes
            .iter()
            .flat_map(|envelope| envelope.items())
            .filter_map(|item| match item {
                EnvelopeItem::Transaction(transaction) => Some(transaction),
                _ => None,
            })
            .collect();
        let [http, task] = transactions.as_slice() else {
            panic!("expected exactly one HTTP and one task transaction");
        };
        assert_eq!(trace(http).op.as_deref(), Some("http.server"));
        assert_eq!(trace(task).op.as_deref(), Some("tokio.task"));
        assert_eq!(task.name.as_deref(), Some("head"));
        assert_eq!(trace(http).parent_span_id.unwrap().to_string(), caller_span);
        assert_eq!(
            trace(task).parent_span_id,
            Some(trace(http).span_id),
            "task parent must be the emitted HTTP span"
        );
        for transaction in transactions {
            assert_eq!(trace(transaction).trace_id.to_string(), trace_id);
            assert_eq!(
                transaction.tags.get("usecase").map(String::as_str),
                Some("test")
            );
            assert_eq!(
                transaction.tags.get("scope.org").map(String::as_str),
                Some("1")
            );
        }
    }
}
