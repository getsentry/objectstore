use axum::extract::FromRequestParts;
use axum::http::{StatusCode, header, request::Parts};

use crate::auth::{AuthAwareService, AuthContext};
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let inner = state.service.clone();
        if !state.config.auth.enforce {
            return Ok(AuthAwareService::new(inner, None));
        }

        let encoded_token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        let context =
            AuthContext::from_encoded_jwt(encoded_token, &state.config.auth).map_err(|err| {
                tracing::debug!("Authorization rejected: `{:?}`", err);
                StatusCode::UNAUTHORIZED
            })?;

        Ok(AuthAwareService::new(state.service.clone(), Some(context)))
    }
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
