use axum::extract::FromRequestParts;
use axum::http::{header, request::Parts};

use crate::auth::{AuthAwareService, AuthContext};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Custom header (preferred), query parameter, or standard `Authorization`
/// header (fallback) for Objectstore authentication. This allows proxy setups
/// (e.g. Django) to use `Authorization` for their own auth while forwarding an
/// Objectstore token separately.
const OBJECTSTORE_AUTH_KEY: &str = "x-objectstore-auth";

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let token = extract_token(parts);

        let enforce = state.config.auth.enforce;
        // Attempt to decode / verify the JWT, logging failure
        let auth_result = AuthContext::from_encoded_jwt(token.as_deref(), &state.key_directory)
            .inspect_err(|err| err.log(None, None, enforce));

        // If auth enforcement is enabled, `from_encoded_jwt()` must have succeeded.
        // If auth enforcement is disabled, we'll pass the context along if it succeeded but will
        // still proceed with `None` if it failed.
        let auth_context = match enforce {
            true => Some(auth_result?),
            false => auth_result.ok(),
        };

        AuthAwareService::new(
            state.service.clone(),
            auth_context,
            state.config.auth.enforce,
        )
    }
}

/// Extracts a bearer token from (in order):
/// 1. The `X-Objectstore-Auth` header
/// 2. The `x-objectstore-auth` query parameter
/// 3. The standard `Authorization` header
fn extract_token(parts: &Parts) -> Option<String> {
    // 1. Custom header
    if let Some(token) = parts
        .headers
        .get(OBJECTSTORE_AUTH_KEY)
        .and_then(|v| v.to_str().ok())
        .and_then(strip_bearer)
    {
        return Some(token.to_owned());
    }

    // 2. Query parameter
    if let Some(token) = parts
        .uri
        .query()
        .into_iter()
        .flat_map(|q| form_urlencoded::parse(q.as_bytes()))
        .find(|(key, _)| key.eq_ignore_ascii_case(OBJECTSTORE_AUTH_KEY))
        .and_then(|(_, value)| strip_bearer(&value).map(str::to_owned))
    {
        return Some(token);
    }

    // 3. Standard Authorization header
    parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(strip_bearer)
        .map(str::to_owned)
}

fn strip_bearer(header_value: &str) -> Option<&str> {
    let (prefix, tail) = header_value.split_at_checked(BEARER_PREFIX.len())?;
    if prefix.eq_ignore_ascii_case(BEARER_PREFIX) {
        Some(tail)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_bearer() {
        // Prefix matches
        assert_eq!(strip_bearer("Bearer tokenvalue"), Some("tokenvalue"));
        assert_eq!(strip_bearer("bearer tokenvalue"), Some("tokenvalue"));
        assert_eq!(strip_bearer("BEARER tokenvalue"), Some("tokenvalue"));

        // Prefix doesn't match
        assert_eq!(strip_bearer("Token tokenvalue"), None);
        assert_eq!(strip_bearer("Bearer"), None);

        // No character boundary at end of expected prefix
        assert_eq!(strip_bearer("Bearer⚠️tokenvalue"), None);
    }
}
