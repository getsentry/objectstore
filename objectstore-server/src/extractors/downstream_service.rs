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

/// Strips a Kubernetes pod suffix from a service name, leaving the base workload name.
///
/// Deployment pods are named `<deployment>-<replicaset-hash>-<pod-suffix>` and StatefulSet
/// pods `<name>-<ordinal>`. The hash/suffix and ordinal change on every rollout, creating
/// useless metric cardinality. Segment lengths follow the Kubernetes naming rules to limit
/// false positives; anything else is returned unchanged.
fn strip_pod_suffix(service: &str) -> &str {
    let is_hash = |segment: &str| {
        segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    };

    let Some((rest, last)) = service.rsplit_once('-') else {
        return service;
    };
    if rest.is_empty() {
        return service;
    }

    // Deployment: the pod suffix is 5 chars and the ReplicaSet hash is 7-10 chars.
    if last.len() == 5
        && is_hash(last)
        && let Some((deployment, hash)) = rest.rsplit_once('-')
        && !deployment.is_empty()
        && (7..=10).contains(&hash.len())
        && is_hash(hash)
    {
        return deployment;
    }

    // StatefulSet: the pod suffix is a numeric ordinal.
    if !last.is_empty() && last.bytes().all(|b| b.is_ascii_digit()) {
        return rest;
    }

    service
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
            // Tag the raw pod name intentionally: it pinpoints the exact instance when
            // debugging, and tag cardinality is not a concern here.
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

    #[test]
    fn keeps_wrong_length_hash() {
        // Hash segment is just outside the 7-10 char range, so it is not a ReplicaSet hash.
        assert_eq!(strip_pod_suffix("foo-sixchr-abcde"), "foo-sixchr-abcde");
        assert_eq!(
            strip_pod_suffix("foo-abcdefghijk-abcde"),
            "foo-abcdefghijk-abcde"
        );
    }

    #[test]
    fn strips_min_length_hash() {
        // A 7-char hash is the shortest that is still treated as a ReplicaSet hash.
        assert_eq!(strip_pod_suffix("svc-abcdefg-hijkl"), "svc");
    }

    #[test]
    fn strips_statefulset_ordinal() {
        assert_eq!(
            strip_pod_suffix("getsentry-incinerator-0"),
            "getsentry-incinerator"
        );
        assert_eq!(strip_pod_suffix("web-12"), "web");
    }
}
