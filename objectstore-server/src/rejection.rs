//! Rejection reason classification and metrics emission.
//!
//! [`RejectionReason`] classifies why a request was rejected, providing a single
//! point for emitting the `server.rejected` counter metric with a `reason` tag.

/// Classifies the reason a request was rejected by the server.
///
/// Used to emit a unified `server.rejected` counter tagged with `reason`,
/// providing visibility into each rejection category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// Request was blocked by a configured killswitch.
    Killswitch,
    /// Request exceeded a throughput or bandwidth rate limit.
    RateLimit,
    /// Request failed authentication or authorization checks.
    Auth,
    /// An internal server error prevented the request from being handled.
    Internal,
    /// Request was malformed or violated API constraints.
    BadRequest,
    /// The backend task concurrency limit was reached.
    TaskConcurrency,
    /// The web server in-flight request concurrency limit was reached.
    WebConcurrency,
}

impl RejectionReason {
    /// Returns the string used as the `reason` metric tag value.
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectionReason::Killswitch => "killswitch",
            RejectionReason::RateLimit => "rate_limit",
            RejectionReason::Auth => "auth",
            RejectionReason::Internal => "internal",
            RejectionReason::BadRequest => "bad_request",
            RejectionReason::TaskConcurrency => "task_concurrency",
            RejectionReason::WebConcurrency => "web_concurrency",
        }
    }

    /// Emits a `server.rejected` counter tagged with this rejection reason.
    pub fn emit(&self) {
        merni::counter!("server.rejected": 1, "reason" => self.as_str());
    }
}
