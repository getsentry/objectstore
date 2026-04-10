use axum::extract::FromRequestParts;
use axum::http::{Method, header, request::Parts};

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

        // 1. Try header-based JWT auth (preferred)
        let encoded_token = parts
            .headers
            .get(OBJECTSTORE_AUTH_HEADER)
            .or_else(|| parts.headers.get(header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        // 2. If no header token, try pre-signed URL params
        let auth_result = if encoded_token.is_none()
            && let Some(presigned_params) = extract_presigned_params(&parts.uri)
        {
            if parts.method != Method::GET && parts.method != Method::HEAD {
                return Err(ApiError::Auth(AuthError::BadRequest(
                    "Pre-signed URLs are only valid for GET and HEAD requests",
                )));
            }
            AuthContext::from_presigned_url(&presigned_params, &parts.uri, &state.key_directory)
        } else {
            // 3. Fall through to JWT verification
            AuthContext::from_encoded_jwt(encoded_token, &state.key_directory)
        };

        if let Err(err) = &auth_result {
            err.log(None, None, enforce);
        }

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
