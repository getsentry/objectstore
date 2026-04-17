use axum::extract::FromRequestParts;
use axum::http::{header, request::Parts};

use crate::auth::{AuthAwareService, AuthContext};
use crate::endpoints::common::ApiError;
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Custom header for Objectstore authentication. Checked before the standard
/// `Authorization` header so that proxy setups (e.g. Django) can use
/// `Authorization` for their own auth while forwarding an Objectstore token in
/// this header.
const OBJECTSTORE_AUTH_HEADER: &str = "x-os-auth";

/// Query parameter name for Objectstore authentication. Used as a fallback when
/// headers cannot be set (e.g. browser-initiated requests). The value should be
/// the raw JWT without a `Bearer` prefix.
const OBJECTSTORE_AUTH_QUERY_PARAM: &str = "X-Os-Auth";

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        // Precedence: X-Os-Auth header > X-Os-Auth query param > Authorization header.
        let encoded_token = parts
            .headers
            .get(OBJECTSTORE_AUTH_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer)
            .or_else(|| extract_query_param_value(parts.uri.query(), OBJECTSTORE_AUTH_QUERY_PARAM))
            .or_else(|| {
                parts
                    .headers
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(strip_bearer)
            });

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

fn extract_query_param_value<'a>(query: Option<&'a str>, param: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == param && !value.is_empty()).then_some(value)
    })
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
    fn test_extract_query_param_value() {
        assert_eq!(extract_query_param_value(None, "X-Os-Auth"), None);
        assert_eq!(extract_query_param_value(Some(""), "X-Os-Auth"), None);
        assert_eq!(
            extract_query_param_value(Some("other=val"), "X-Os-Auth"),
            None
        );

        assert_eq!(
            extract_query_param_value(Some("X-Os-Auth=mytoken"), "X-Os-Auth"),
            Some("mytoken")
        );
        assert_eq!(
            extract_query_param_value(Some("foo=bar&X-Os-Auth=mytoken"), "X-Os-Auth"),
            Some("mytoken")
        );
        assert_eq!(
            extract_query_param_value(Some("X-Os-Auth=mytoken&foo=bar"), "X-Os-Auth"),
            Some("mytoken")
        );

        // Empty value
        assert_eq!(
            extract_query_param_value(Some("X-Os-Auth="), "X-Os-Auth"),
            None
        );
        // No `=` sign
        assert_eq!(
            extract_query_param_value(Some("X-Os-Auth"), "X-Os-Auth"),
            None
        );
    }
}
