use axum::extract::FromRequestParts;
use axum::http::{header, request::Parts};

use crate::auth::presigned::extract_presigned_params;
use crate::auth::{AuthAwareService, AuthContext, AuthError};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Custom header for Objectstore authentication. Checked before the standard
/// `Authorization` header so that proxy setups (e.g. Django) can use
/// `Authorization` for their own auth while forwarding an Objectstore token in
/// this header.
const OBJECTSTORE_AUTH_HEADER: &str = "x-os-auth";

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let enforce = state.config.auth.enforce;

        let encoded_jwt = parts
            .headers
            .get(OBJECTSTORE_AUTH_HEADER)
            .or_else(|| parts.headers.get(header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        let presigned_params = extract_presigned_params(&parts.uri);

        let auth_result = match (presigned_params, encoded_jwt) {
            // Pre-signed URL params take precedence
            (Some(ref params), _) => AuthContext::from_presigned_url(
                params,
                &parts.method,
                &parts.uri,
                &state.key_directory,
            ),
            // Fall back to header-based JWT auth
            (None, Some(jwt)) => AuthContext::from_encoded_jwt(jwt, &state.key_directory),
            (None, None) => Err(AuthError::BadRequest("No authorization provided")),
        };

        if let Err(err) = &auth_result {
            err.log(None, None, enforce);
        }

        // If auth enforcement is enabled, `auth_context` must be `Ok(context)`.
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
