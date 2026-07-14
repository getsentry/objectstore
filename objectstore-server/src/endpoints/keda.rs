//! KEDA autoscaling metrics endpoint.
//!
//! Exposes a `/keda` endpoint in Prometheus text format (version 0.0.4) with:
//!
//! - **Monotonic counters** (`objectstore_bytes_total`, `objectstore_requests_total`):
//!   cumulative totals since startup; use `irate(counter[window])` in KEDA queries for an
//!   unsmoothed, immediately responsive rate.

use std::fmt::Write as _;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/keda", routing::get(keda))
}

async fn keda(State(state): State<ServiceState>) -> impl IntoResponse {
    let bw_limit = state.rate_limiter.bandwidth_limit();
    let bw_total = state.rate_limiter.bandwidth_total_bytes();
    let tp_limit = state.rate_limiter.throughput_limit();
    let tp_total = state.rate_limiter.throughput_total_admitted();
    let req_in_flight = state.request_counter.count();
    let req_limit = state.request_counter.limit();
    let tasks_running = state.service.tasks_running();
    let tasks_limit = state.service.tasks_limit();

    let mut output = format!(
        "# HELP objectstore_bytes_total Total bytes transferred since startup\n\
         # TYPE objectstore_bytes_total counter\n\
         objectstore_bytes_total {bw_total}\n\
         # HELP objectstore_requests_total Total admitted requests since startup\n\
         # TYPE objectstore_requests_total counter\n\
         objectstore_requests_total {tp_total}\n\
         # HELP objectstore_requests_in_flight Current in-flight HTTP requests\n\
         # TYPE objectstore_requests_in_flight gauge\n\
         objectstore_requests_in_flight {req_in_flight}\n\
         # HELP objectstore_requests_limit Configured maximum concurrent HTTP requests\n\
         # TYPE objectstore_requests_limit gauge\n\
         objectstore_requests_limit {req_limit}\n\
         # HELP objectstore_tasks_running Current running backend tasks\n\
         # TYPE objectstore_tasks_running gauge\n\
         objectstore_tasks_running {tasks_running}\n\
         # HELP objectstore_tasks_limit Configured maximum concurrent backend tasks\n\
         # TYPE objectstore_tasks_limit gauge\n\
         objectstore_tasks_limit {tasks_limit}\n"
    );

    if let Some(limit) = bw_limit {
        write!(
            output,
            "# HELP objectstore_bandwidth_limit Configured global bandwidth limit in bytes/s\n\
             # TYPE objectstore_bandwidth_limit gauge\n\
             objectstore_bandwidth_limit {limit}\n"
        )
        .expect("writing to String is infallible");
    }

    if let Some(limit) = tp_limit {
        write!(
            output,
            "# HELP objectstore_throughput_limit Configured global throughput limit in requests/s\n\
             # TYPE objectstore_throughput_limit gauge\n\
             objectstore_throughput_limit {limit}\n"
        )
        .expect("writing to String is infallible");
    }

    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}
