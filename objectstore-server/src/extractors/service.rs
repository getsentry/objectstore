use axum::extract::FromRequestParts;
use axum::extract::Query;
use axum::http::{header, request::Parts};
use objectstore_service::id::ObjectId;
use serde::Deserialize;

use crate::auth::{AuthAwareService, AuthContext, JwtAuthContext};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Custom header for Objectstore authentication. Checked before the standard
/// `Authorization` header so that proxy setups (e.g. Django) can use
/// `Authorization` for their own auth while forwarding an Objectstore token in
/// this header.
pub(crate) const OBJECTSTORE_AUTH_HEADER: &str = "x-os-auth";

#[derive(Debug)]
pub(crate) struct StrictAuthContext(pub JwtAuthContext);

#[derive(Debug, Deserialize)]
struct PresignedSignatureQuery {
    #[serde(rename = "X-Os-Signature")]
    signature: Option<String>,
}

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let encoded_token = encoded_auth_token(parts).map(str::to_owned);

        if encoded_token.is_none()
            && let Some(auth_context) = presigned_auth_context(parts, state).await?
        {
            return AuthAwareService::new(state.service.clone(), Some(auth_context), true);
        }

        let enforce = state.config.auth.enforce;
        // Attempt to decode / verify the JWT, logging failure
        let auth_result =
            AuthContext::from_encoded_jwt(encoded_token.as_deref(), &state.key_directory)
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

impl FromRequestParts<ServiceState> for StrictAuthContext {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let encoded_token = encoded_auth_token(parts).map(str::to_owned);
        let auth_context =
            JwtAuthContext::from_encoded_jwt(encoded_token.as_deref(), &state.key_directory)
                .inspect_err(|err| err.log(None, None, true))?;

        Ok(Self(auth_context))
    }
}

fn encoded_auth_token(parts: &Parts) -> Option<&str> {
    parts
        .headers
        .get(OBJECTSTORE_AUTH_HEADER)
        .or_else(|| parts.headers.get(header::AUTHORIZATION))
        .and_then(|v| v.to_str().ok())
        .and_then(strip_bearer)
}

async fn presigned_auth_context(
    parts: &mut Parts,
    state: &ServiceState,
) -> Result<Option<AuthContext>, ApiError> {
    let Query(query) = Query::<PresignedSignatureQuery>::from_request_parts(parts, state)
        .await
        .map_err(|_| crate::auth::AuthError::BadRequest("Invalid query string"))?;

    let Some(signature) = query.signature else {
        return Ok(None);
    };

    let id = parts
        .extensions
        .get::<ObjectId>()
        .ok_or(crate::auth::AuthError::NotPermitted)?;
    let expires_at =
        state
            .presigned_key_directory
            .verify_object_read(&signature, id, &parts.method)?;

    Ok(Some(AuthContext::from_presigned_object_read(
        id.clone(),
        expires_at,
    )))
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
