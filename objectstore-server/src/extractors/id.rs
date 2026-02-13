use std::borrow::Cow;

use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::scope::{EMPTY_SCOPES, Scope, Scopes};
use serde::{Deserialize, de};

use crate::extractors::Xt;
use crate::extractors::downstream_service::DownstreamService;
use crate::state::ServiceState;

#[derive(Debug)]
pub enum ObjectRejection {
    Path(PathRejection),
    Killswitched,
    RateLimited,
}

impl IntoResponse for ObjectRejection {
    fn into_response(self) -> Response {
        match self {
            ObjectRejection::Path(rejection) => rejection.into_response(),
            ObjectRejection::Killswitched => (
                axum::http::StatusCode::FORBIDDEN,
                "Object access is disabled for this scope through killswitches",
            )
                .into_response(),
            ObjectRejection::RateLimited => (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "Object access is rate limited",
            )
                .into_response(),
        }
    }
}

impl From<PathRejection> for ObjectRejection {
    fn from(rejection: PathRejection) -> Self {
        ObjectRejection::Path(rejection)
    }
}

impl FromRequestParts<ServiceState> for Xt<ObjectId> {
    type Rejection = ObjectRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let Path(params) = Path::<ObjectParams>::from_request_parts(parts, state).await?;
        let id = ObjectId::from_parts(params.usecase, params.scopes, params.key);

        populate_sentry_context(id.context());
        sentry::configure_scope(|s| s.set_extra("key", id.key().into()));

        let service = DownstreamService::from_request_parts(parts, state)
            .await
            .unwrap();

        if state
            .config
            .killswitches
            .matches(id.context(), service.as_str())
        {
            tracing::debug!("Request rejected due to killswitches");
            return Err(ObjectRejection::Killswitched);
        }

        if !state.rate_limiter.check(id.context()) {
            tracing::debug!("Request rejected due to rate limits");
            return Err(ObjectRejection::RateLimited);
        }

        Ok(Xt(id))
    }
}

/// Path parameters used for object-level endpoints.
///
/// This is meant to be used with the axum `Path` extractor.
#[derive(Clone, Debug, Deserialize)]
struct ObjectParams {
    usecase: String,
    #[serde(deserialize_with = "deserialize_scopes")]
    scopes: Scopes,
    key: String,
}

/// Deserializes a `Scopes` instance from a string representation.
///
/// The string representation is a semicolon-separated list of `key=value` pairs, following the
/// Matrix URIs proposal. An empty scopes string (`"_"`) represents no scopes.
fn deserialize_scopes<'de, D>(deserializer: D) -> Result<Scopes, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = Cow::<str>::deserialize(deserializer)?;
    if s == EMPTY_SCOPES {
        return Ok(Scopes::empty());
    }

    let scopes = s
        .split(';')
        .map(|s| {
            let (key, value) = s
                .split_once("=")
                .ok_or_else(|| de::Error::custom("scope must be 'key=value'"))?;

            Scope::create(key, value).map_err(de::Error::custom)
        })
        .collect::<Result<_, _>>()?;

    Ok(scopes)
}

impl FromRequestParts<ServiceState> for Xt<ObjectContext> {
    type Rejection = ObjectRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let Path(params) = Path::<ContextParams>::from_request_parts(parts, state).await?;
        let context = ObjectContext {
            usecase: params.usecase,
            scopes: params.scopes,
        };

        populate_sentry_context(&context);

        let service = DownstreamService::from_request_parts(parts, state)
            .await
            .unwrap();

        if state
            .config
            .killswitches
            .matches(&context, service.as_str())
        {
            tracing::debug!("Request rejected due to killswitches");
            return Err(ObjectRejection::Killswitched);
        }

        if !state.rate_limiter.check(&context) {
            tracing::debug!("Request rejected due to rate limits");
            return Err(ObjectRejection::RateLimited);
        }

        Ok(Xt(context))
    }
}

/// Path parameters used for collection-level endpoints without a key.
///
/// This is meant to be used with the axum `Path` extractor.
#[derive(Clone, Debug, Deserialize)]
struct ContextParams {
    usecase: String,
    #[serde(deserialize_with = "deserialize_scopes")]
    scopes: Scopes,
}

fn populate_sentry_context(context: &ObjectContext) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", &context.usecase);
        for scope in &context.scopes {
            s.set_tag(&format!("scope.{}", scope.name()), scope.value());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{CowStrDeserializer, Error as DeError};
    use serde::de::IntoDeserializer;
    use std::borrow::Cow;

    fn deser_scopes(input: &str) -> Result<Scopes, DeError> {
        let deserializer: CowStrDeserializer<DeError> = Cow::Borrowed(input).into_deserializer();
        deserialize_scopes(deserializer)
    }

    #[test]
    fn parse_single_scope() {
        let scopes = deser_scopes("org=123").unwrap();
        assert_eq!(scopes.get_value("org"), Some("123"));
    }

    #[test]
    fn parse_multiple_scopes() {
        let scopes = deser_scopes("org=123;project=456").unwrap();
        assert_eq!(scopes.get_value("org"), Some("123"));
        assert_eq!(scopes.get_value("project"), Some("456"));
    }

    #[test]
    fn parse_empty_scopes() {
        let scopes = deser_scopes("_").unwrap();
        assert!(scopes.is_empty());
    }

    #[test]
    fn parse_missing_equals() {
        let result = deser_scopes("org123");
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_scope_chars() {
        let result = deser_scopes("org=hello world");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_key_or_value() {
        assert!(deser_scopes("=value").is_err());
        assert!(deser_scopes("key=").is_err());
    }
}
