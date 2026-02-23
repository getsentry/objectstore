//! KEDA autoscaling metrics endpoint.
//!
//! Exposes a `/keda` endpoint in Prometheus text format (version 0.0.4) with pre-computed
//! gauges for every rate-limited resource. Each scrape is a self-contained snapshot — no
//! `irate()` or Prometheus scrape-interval arithmetic needed.

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

    let mut output = String::new();

    output.push_str("# HELP objectstore_bandwidth_ewma Current bandwidth in bytes/s (EWMA)\n");
    output.push_str("# TYPE objectstore_bandwidth_ewma gauge\n");
    output.push_str(&format!("objectstore_bandwidth_ewma {bw_ewma}\n"));

    if let Some(limit) = bw_limit {
        output.push_str(
            "# HELP objectstore_bandwidth_limit Configured global bandwidth limit in bytes/s\n",
        );
        output.push_str("# TYPE objectstore_bandwidth_limit gauge\n");
        output.push_str(&format!("objectstore_bandwidth_limit {limit}\n"));
    }

    output.push_str(
        "# HELP objectstore_throughput_rps Current admitted request rate in requests/s (EWMA)\n",
    );
    output.push_str("# TYPE objectstore_throughput_rps gauge\n");
    output.push_str(&format!("objectstore_throughput_rps {tp_rps}\n"));

    if let Some(limit) = tp_limit {
        output.push_str(
            "# HELP objectstore_throughput_limit Configured global throughput limit in requests/s\n",
        );
        output.push_str("# TYPE objectstore_throughput_limit gauge\n");
        output.push_str(&format!("objectstore_throughput_limit {limit}\n"));
    }

    output.push_str("# HELP objectstore_requests_in_flight Current in-flight HTTP requests\n");
    output.push_str("# TYPE objectstore_requests_in_flight gauge\n");
    output.push_str(&format!("objectstore_requests_in_flight {req_in_flight}\n"));

    output.push_str(
        "# HELP objectstore_requests_limit Configured maximum concurrent HTTP requests\n",
    );
    output.push_str("# TYPE objectstore_requests_limit gauge\n");
    output.push_str(&format!("objectstore_requests_limit {req_limit}\n"));

    output.push_str("# HELP objectstore_tasks_in_use Current in-flight backend tasks\n");
    output.push_str("# TYPE objectstore_tasks_in_use gauge\n");
    output.push_str(&format!("objectstore_tasks_in_use {tasks_in_use}\n"));

    output.push_str("# HELP objectstore_tasks_limit Configured maximum concurrent backend tasks\n");
    output.push_str("# TYPE objectstore_tasks_limit gauge\n");
    output.push_str(&format!("objectstore_tasks_limit {tasks_capacity}\n"));

    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}
