use std::time::SystemTime;

use axum::extract::{FromRequestParts, OriginalUri, Query};
use axum::http::{Method, header, request::Parts};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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

/// Query parameter carrying a base64url-encoded JWT, as an alternative to the
/// `x-os-auth` header. This lets callers embed an auth token directly in a URL
/// (e.g. a read-only download link). The header takes precedence when both are
/// present.
const AUTH_QUERY_PARAM: &str = "os_auth";

impl AuthAwareService {
    fn from_token(parts: &mut Parts, state: &ServiceState) -> Result<AuthContext, AuthError> {
        let header_token = parts
            .headers
            .get(OBJECTSTORE_AUTH_HEADER)
            .or_else(|| parts.headers.get(header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer);

        // Fall back to the `os_auth` query parameter when no header token is
        // present. The value is a base64url-encoded JWT.
        let query_token = match header_token {
            Some(_) => None,
            None => token_from_query(parts.uri.query())?,
        };

        let token = header_token.or(query_token.as_deref());

        AuthContext::from_encoded_jwt(token, &state.key_directory)
    }

    async fn from_presigned_request(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<AuthContext, AuthError> {
        if !matches!(&parts.method, &Method::GET | &Method::HEAD) {
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

        Ok(AuthAwareService::new(state.service.clone(), auth, enforce))
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

/// Extracts and base64url-decodes the JWT carried in the `os_auth` query
/// parameter, if present.
///
/// Returns `Ok(None)` when the parameter is absent, and
/// [`AuthError::BadRequest`] when the value is not valid base64url or does not
/// decode to a UTF-8 string.
fn token_from_query(query: Option<&str>) -> Result<Option<String>, AuthError> {
    let Some(value) = query.and_then(|query| {
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == AUTH_QUERY_PARAM).then_some(value)
        })
    }) else {
        return Ok(None);
    };

    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| AuthError::BadRequest("os_auth query parameter is not valid base64url"))?;
    let token = String::from_utf8(bytes)
        .map_err(|_| AuthError::BadRequest("os_auth query parameter is not a valid token"))?;

    Ok(Some(token))
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

    #[test]
    fn test_token_from_query() {
        let encoded = URL_SAFE_NO_PAD.encode("header.payload.signature");

        // Present, valid base64url.
        assert_eq!(
            token_from_query(Some(&format!("{AUTH_QUERY_PARAM}={encoded}"))).unwrap(),
            Some("header.payload.signature".to_owned())
        );
        // Present alongside other params.
        assert_eq!(
            token_from_query(Some(&format!("foo=bar&{AUTH_QUERY_PARAM}={encoded}"))).unwrap(),
            Some("header.payload.signature".to_owned())
        );

        // Absent.
        assert_eq!(token_from_query(None).unwrap(), None);
        assert_eq!(token_from_query(Some("foo=bar")).unwrap(), None);

        // Present but not valid base64url.
        assert!(token_from_query(Some(&format!("{AUTH_QUERY_PARAM}=not base64!!"))).is_err());
    }
}
