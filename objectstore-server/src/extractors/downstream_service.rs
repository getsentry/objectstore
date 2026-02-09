use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::state::ServiceState;

/// Header used to identify the downstream service making the request, for use in killswitches and
/// logging.
const HEADER_SERVICE: &str = "x-downstream-service";

/// Extractor for the downstream service identifier from the request header.
///
/// This extracts the `x-downstream-service` header value, which is used to identify which
/// Kubernetes service or downstream system is making the request. This can be used for
/// killswitches, rate limiting, and logging.
#[derive(Debug, Clone)]
pub struct DownstreamService(pub Option<String>);

impl DownstreamService {
    /// Returns the downstream service identifier, if present.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

impl FromRequestParts<ServiceState> for DownstreamService {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let service = parts
            .headers
            .get(HEADER_SERVICE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref service) = service {
            sentry::configure_scope(|s| {
                s.set_tag("downstream_service", service);
            });
        }

        Ok(DownstreamService(service))
    }
}
