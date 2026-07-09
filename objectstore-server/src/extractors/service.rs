use std::time::SystemTime;

use axum::extract::{FromRequestParts, OriginalUri, Query};
use axum::http::{Method, header, request::Parts};
use objectstore_types::presign::PARAM_SIG;

use crate::auth::{AuthAwareService, AuthContext, AuthError, PresignParams};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Custom header for Objectstore authentication. Checked before the standard
/// `Authorization` header so that proxy setups (e.g. Django) can use
/// `Authorization` for their own auth while forwarding an Objectstore token in
/// this header.
const OBJECTSTORE_AUTH_HEADER: &str = "x-os-auth";

impl AuthAwareService {
    fn from_token(parts: &mut Parts, state: &ServiceState) -> Result<AuthContext, AuthError> {
        let token = parts
            .headers
            .get(OBJECTSTORE_AUTH_HEADER)
            .or_else(|| parts.headers.get(header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        AuthContext::from_encoded_jwt(token, &state.key_directory)
    }

    async fn from_presigned_request(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<AuthContext, AuthError> {
        if !matches!(
            &parts.method,
            &Method::GET | &Method::HEAD | &Method::DELETE
        ) {
            return Err(AuthError::UnsupportedPresignedMethod);
        }

        let Query(params) = Query::<PresignParams>::from_request_parts(parts, state)
            .await
            .map_err(|_| {
                AuthError::BadRequest("presigned URL has missing or invalid parameters")
            })?;

        // The client signs the full public path, but `Router::nest` strips the `/v1`
        // prefix from `parts.uri`. Recover the original path from `OriginalUri`.
        let path = parts
            .extensions
            .get::<OriginalUri>()
            .ok_or(AuthError::InternalError(
                "OriginalUri extension missing".into(),
            ))?
            .0
            .path();

        AuthContext::from_presigned_request(
            &parts.method,
            path,
            parts.uri.query(),
            &params,
            &state.key_directory,
            SystemTime::now(),
        )
    }
}

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let enforce = state.config.auth.enforce;
        if !state.config.auth.is_active() {
            return Ok(AuthAwareService::new(
                state.service.clone(),
                AuthContext::Disabled,
                enforce,
                state.key_directory.clone(),
            ));
        }

        let auth_result = if has_signature(parts.uri.query()) {
            AuthAwareService::from_presigned_request(parts, state).await
        } else {
            AuthAwareService::from_token(parts, state)
        }
        .inspect_err(|e| e.log(!enforce));

        // If enforcement is disabled, proceed without an auth context even on failure
        let auth = match auth_result {
            Ok(auth) => auth,
            Err(error) if enforce => return Err(ApiError::Auth(error)),
            Err(_) => AuthContext::Disabled,
        };

        Ok(AuthAwareService::new(
            state.service.clone(),
            auth,
            enforce,
            state.key_directory.clone(),
        ))
    }
}

/// Returns whether the query string carries a pre-signed URL signature (`os-sig`).
fn has_signature(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        query
            .split('&')
            .any(|pair| pair.split_once('=').map_or(pair, |(key, _)| key) == PARAM_SIG)
    })
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

    #[test]
    fn test_has_presign_signature() {
        assert!(has_signature(Some("os_sig=abc")));
        assert!(has_signature(Some("os_kid=relay&os_sig=abc")));

        assert!(!has_signature(Some("OS_SIG=abc")));
        assert!(!has_signature(None));
        assert!(!has_signature(Some("os_kid=relay")));
    }
}
