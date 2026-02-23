//! KEDA autoscaling metrics endpoint.
//!
//! Exposes a `/keda` endpoint in Prometheus text format (version 0.0.4) with pre-computed
//! gauges for every rate-limited resource. Each scrape is a self-contained snapshot — no
//! `irate()` or Prometheus scrape-interval arithmetic needed.

use std::fmt::Write as _;

use axum::Extension;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Router, routing};
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;

use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/keda", routing::get(keda))
}

async fn keda(
    State(state): State<ServiceState>,
    Extension(counter): Extension<InFlightRequestsCounter>,
) -> impl IntoResponse {
    let bw_ewma = state.rate_limiter.bandwidth_ewma();
    let bw_limit = state.rate_limiter.bandwidth_limit();
    let tp_rps = state.rate_limiter.throughput_rps();
    let tp_limit = state.rate_limiter.throughput_limit();
    let req_in_flight = counter.get();
    let req_limit = state.config.http.max_requests;
    let tasks_in_use = state.service.tasks_in_use();
    let tasks_capacity = state.service.tasks_capacity();

    let mut output = format!(
        "# HELP objectstore_bandwidth_ewma Current bandwidth in bytes/s (EWMA)\n\
         # TYPE objectstore_bandwidth_ewma gauge\n\
         objectstore_bandwidth_ewma {bw_ewma}\n\
         # HELP objectstore_throughput_rps Current admitted request rate in requests/s (EWMA)\n\
         # TYPE objectstore_throughput_rps gauge\n\
         objectstore_throughput_rps {tp_rps}\n\
         # HELP objectstore_requests_in_flight Current in-flight HTTP requests\n\
         # TYPE objectstore_requests_in_flight gauge\n\
         objectstore_requests_in_flight {req_in_flight}\n\
         # HELP objectstore_requests_limit Configured maximum concurrent HTTP requests\n\
         # TYPE objectstore_requests_limit gauge\n\
         objectstore_requests_limit {req_limit}\n\
         # HELP objectstore_tasks_in_use Current in-flight backend tasks\n\
         # TYPE objectstore_tasks_in_use gauge\n\
         objectstore_tasks_in_use {tasks_in_use}\n\
         # HELP objectstore_tasks_limit Configured maximum concurrent backend tasks\n\
         # TYPE objectstore_tasks_limit gauge\n\
         objectstore_tasks_limit {tasks_capacity}\n"
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
