use std::borrow::Cow;

use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::scope::{EMPTY_SCOPES, Scope, Scopes};
use serde::{Deserialize, de};

use crate::extractors::Xt;
use crate::state::ServiceState;

impl FromRequestParts<ServiceState> for Xt<ObjectId> {
    type Rejection = PathRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let Path(params) = Path::<ObjectParams>::from_request_parts(parts, state).await?;
        let id = ObjectId::from_parts(params.usecase, params.scopes, params.key);

        populate_sentry_context(id.context());
        sentry::configure_scope(|s| s.set_extra("key", id.key().into()));

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
    type Rejection = PathRejection;

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
