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

        // A pre-signed URL carries its signature in the query string. If present, verify it
        // instead of looking for a JWT.
        let auth_result = if has_presign_signature(parts.uri.query()) {
            // Pre-signed URLs are only accepted for read-like methods. Other methods are
            // rejected outright (see the `PresignUnsupportedMethod` status mapping for the
            // 501-vs-401 note).
            if !matches!(parts.method, Method::GET | Method::HEAD | Method::DELETE) {
                let error = AuthError::PresignUnsupportedMethod;
                error.log(!enforce);
                return if enforce {
                    Err(ApiError::Auth(error))
                } else {
                    Ok(AuthAwareService::new(
                        state.service.clone(),
                        AuthContext::Disabled,
                        enforce,
                        state.key_directory.clone(),
                    ))
                };
            }

            let Query(params) = Query::<PresignParams>::from_request_parts(parts, state)
                .await
                .map_err(|_| {
                    AuthError::BadRequest("presigned URL has missing or invalid parameters")
                })?;

            // The client signs the full public path, but `Router::nest` strips the `/v1`
            // prefix from `parts.uri`. Recover the original path from `OriginalUri`.
            let path = match parts.extensions.get::<OriginalUri>() {
                Some(original) => original.0.path(),
                None => parts.uri.path(),
            };

            AuthContext::from_presigned_request(
                &parts.method,
                path,
                parts.uri.query(),
                &params,
                &state.key_directory,
                SystemTime::now(),
            )
            .inspect_err(|e| e.log(!enforce))
        } else {
            let token = parts
                .headers
                .get(OBJECTSTORE_AUTH_HEADER)
                .or_else(|| parts.headers.get(header::AUTHORIZATION))
                .and_then(|v| v.to_str().ok())
                .and_then(strip_bearer);

            // Attempt to decode / verify the JWT, logging failure
            AuthContext::from_encoded_jwt(token, &state.key_directory)
                .inspect_err(|e| e.log(!enforce))
        };

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
///
/// Objectstore query parameter keys are always matched case-insensitively (see
/// [`objectstore_types::presign`]), so this also detects `OS-SIG`, `Os-Sig`, etc.
fn has_presign_signature(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        query.split('&').any(|pair| {
            pair.split_once('=')
                .map_or(pair, |(key, _)| key)
                .eq_ignore_ascii_case(PARAM_SIG)
        })
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
    fn test_has_presign_signature_case_insensitive() {
        assert!(has_presign_signature(Some("os-sig=abc")));
        assert!(has_presign_signature(Some("OS-SIG=abc")));
        assert!(has_presign_signature(Some("Os-Sig=abc")));
        assert!(has_presign_signature(Some("os-kid=relay&OS-SIG=abc")));

        assert!(!has_presign_signature(None));
        assert!(!has_presign_signature(Some("os-kid=relay")));
    }
}
