//! Downstream service extractor from the `x-downstream-service` request header.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

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

impl std::fmt::Display for DownstreamService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_deref().unwrap_or("unknown"))
    }
}

/// Strips a Kubernetes ReplicaSet hash and pod suffix from a service name.
///
/// Deployment pods are named `<deployment>-<replicaset-hash>-<pod-suffix>`, where the hash
/// and suffix are random lowercase-alphanumeric strings. These create extremely high metric
/// cardinality and carry no useful information, so they are dropped here.
///
/// The suffix is only stripped when the trailing two `-`-separated segments match the shape
/// `<hash>-<5-char suffix>` (both non-empty, lowercase alphanumeric). Values that do not
/// match are returned unchanged.
fn strip_pod_suffix(service: &str) -> &str {
    let is_hash = |segment: &str| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    };

    let Some((rest, pod_suffix)) = service.rsplit_once('-') else {
        return service;
    };
    if pod_suffix.len() != 5 || !is_hash(pod_suffix) {
        return service;
    }

    let Some((deployment, replicaset_hash)) = rest.rsplit_once('-') else {
        return service;
    };
    if deployment.is_empty() || !is_hash(replicaset_hash) {
        return service;
    }

    deployment
}

impl<S: Send + Sync> FromRequestParts<S> for DownstreamService {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let service = parts
            .headers
            .get(HEADER_SERVICE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        if let Some(ref service) = service {
            sentry::configure_scope(|s| {
                s.set_tag("downstream_service", service);
            });
        }

        let normalized = service.as_deref().map(strip_pod_suffix).map(str::to_owned);
        Ok(DownstreamService(normalized))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_typical_pod_name() {
        assert_eq!(
            strip_pod_suffix("getsentry-incinerator-7d8f9c5b6d-abc12"),
            "getsentry-incinerator"
        );
    }

    #[test]
    fn strips_single_segment_deployment() {
        assert_eq!(strip_pod_suffix("service-7d8f9c5b6d-abcde"), "service");
    }

    #[test]
    fn keeps_plain_name() {
        assert_eq!(strip_pod_suffix("relay"), "relay");
    }

    #[test]
    fn keeps_name_without_pod_suffix() {
        assert_eq!(
            strip_pod_suffix("getsentry-incinerator"),
            "getsentry-incinerator"
        );
    }

    #[test]
    fn keeps_short_trailing_segment() {
        assert_eq!(strip_pod_suffix("my-service"), "my-service");
    }

    #[test]
    fn keeps_uppercase_suffix() {
        assert_eq!(
            strip_pod_suffix("my-svc-7d8f9c5b6d-ABC12"),
            "my-svc-7d8f9c5b6d-ABC12"
        );
    }

    #[test]
    fn keeps_empty_deployment() {
        assert_eq!(strip_pod_suffix("-abcde-fghij"), "-abcde-fghij");
    }
}
