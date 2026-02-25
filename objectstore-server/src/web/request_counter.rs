//! In-flight HTTP request counter with a configurable limit.
//!
//! [`RequestCounter`] pairs a [`tower_http`] in-flight counter with the configured
//! maximum, so both the tracking layer and the concurrency-limit middleware share a
//! single, self-contained type.

use std::time::Duration;

use tower_http::metrics::InFlightRequestsLayer;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;

/// Interval for the periodic in-flight gauge metric emitter.
const EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Tracks in-flight HTTP requests and enforces a configured maximum.
///
/// Clone this to share the same underlying atomic across the tower layer, the
/// concurrency-limit middleware, and any endpoint that needs to read the count.
/// Call [`layer`](Self::layer) to obtain the [`InFlightRequestsLayer`] that
/// increments the counter for every request.
#[derive(Clone, Debug)]
pub struct RequestCounter {
    counter: InFlightRequestsCounter,
    limit: usize,
}

impl RequestCounter {
    /// Creates a new counter with the given maximum.
    pub fn new(limit: usize) -> Self {
        Self {
            counter: InFlightRequestsCounter::new(),
            limit,
        }
    }

    /// Returns a [`InFlightRequestsLayer`] that increments this counter for every request.
    pub fn layer(&self) -> InFlightRequestsLayer {
        InFlightRequestsLayer::new(self.counter.clone())
    }

    /// Returns the number of requests currently in flight.
    pub fn count(&self) -> usize {
        self.counter.get()
    }

    /// Returns the configured maximum number of concurrent requests.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Periodically calls `emit` with the current in-flight count.
    ///
    /// Runs forever; intended to be spawned as a background task.
    pub async fn run_emitter(self) {
        self.counter
            .run_emitter(EMITTER_INTERVAL, |count| async move {
                merni::gauge!("server.requests.in_flight": count);
            })
            .await;
    }
}
