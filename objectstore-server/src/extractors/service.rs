use axum::extract::FromRequestParts;
use axum::http::{header, request::Parts};

use crate::auth::{AuthAwareService, AuthContext};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let encoded_token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        let enforce = state.config.auth.enforce;
        // Attempt to decode / verify the JWT, logging failure
        let auth_result = AuthContext::from_encoded_jwt(encoded_token, &state.key_directory)
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
