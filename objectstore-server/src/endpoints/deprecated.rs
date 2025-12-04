use std::time::SystemTime;
use std::{fmt, io};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::id::{ObjectId, Scope, Scopes};
use objectstore_types::Metadata;
use serde::de;

use crate::endpoints::helpers;
use crate::endpoints::objects::InsertObjectResponse;
use crate::error::ApiResult;
use crate::state::ServiceState;

/// Magic URL segment that separates objectstore context from an object's user-provided key.
const PATH_CONTEXT_SEPARATOR: &str = "objects";

pub fn router() -> Router<ServiceState> {
    let routes = axum::routing::post(deprecated_insert)
        .put(deprecated_insert)
        .get(deprecated_get)
        .delete(deprecated_delete);

    Router::new().route("/{*path}", routes)
}

async fn deprecated_insert(
    State(state): State<ServiceState>,
    Path(optional_id): Path<OptionalObjectId>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let (expected_method, response_status) = match optional_id.key {
        Some(_) => (Method::PUT, StatusCode::OK),
        None => (Method::POST, StatusCode::CREATED),
    };

    // TODO: For now allow PUT everywhere. Remove the second condition when all clients are updated.
    if method != expected_method && method == Method::POST {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }

    let id = optional_id.create_key();
    helpers::populate_sentry_scope(&id);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_path = state.service.put_object(id, &metadata, stream).await?;
    let response = Json(InsertObjectResponse {
        key: response_path.key.to_string(),
    });

    Ok((response_status, response).into_response())
}

async fn deprecated_get(
    State(state): State<ServiceState>,
    Path(optional_id): Path<OptionalObjectId>,
) -> ApiResult<Response> {
    let id = optional_id.require_key()?;
    helpers::populate_sentry_scope(&id);
    let Some((metadata, stream)) = state.service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn deprecated_delete(
    State(state): State<ServiceState>,
    Path(optional_id): Path<OptionalObjectId>,
) -> ApiResult<impl IntoResponse> {
    let id = optional_id.require_key()?;
    helpers::populate_sentry_scope(&id);

    state.service.delete_object(&id).await?;

    Ok(StatusCode::NO_CONTENT)
}

struct OptionalObjectId {
    usecase: String,
    scopes: Scopes,
    key: Option<String>,
}

impl OptionalObjectId {
    /// Converts to an [`ObjectId`], generating a unique `key` if none was provided.
    pub fn create_key(self) -> ObjectId {
        ObjectId::optional(self.usecase, self.scopes, self.key)
    }

    /// Converts to an [`ObjectId`], returning an error if no `key` was provided.
    pub fn require_key(self) -> Result<ObjectId> {
        Ok(ObjectId {
            usecase: self.usecase,
            scopes: self.scopes,
            key: self
                .key
                .context("object key is required but was not provided")?,
        })
    }
}

struct OptionalObjectIdVisitor;

impl<'de> serde::de::Visitor<'de> for OptionalObjectIdVisitor {
    type Value = OptionalObjectId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a string of the following format: `{{usecase}}/{{scope1}}/.../{PATH_CONTEXT_SEPARATOR}/{{key}}`"
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Some((usecase, mut remainder)) = s.split_once('/') else {
            return Err(E::custom("path is empty or contains no '/'"));
        };

        let mut scopes = vec![];

        loop {
            let Some((scope_str, tail)) = remainder.split_once('/') else {
                return Err(E::custom("missing object key"));
            };

            remainder = tail;
            if scope_str == PATH_CONTEXT_SEPARATOR {
                break;
            } else if scope_str.is_empty() {
                return Err(E::custom("scope must not be empty"));
            }

            let Some((key, value)) = scope_str.split_once('.') else {
                return Err(E::custom("scope must be 'key.value'"));
            };

            let scope = Scope::create(key, value).map_err(E::custom)?;
            scopes.push(scope);
        }

        let key = Some(remainder).filter(|s| !s.is_empty());

        Ok(OptionalObjectId {
            usecase: usecase.to_owned(),
            scopes: scopes.into_iter().collect(),
            key: key.map(|k| k.to_owned()),
        })
    }
}

impl<'de> de::Deserialize<'de> for OptionalObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(OptionalObjectIdVisitor)
    }
}
