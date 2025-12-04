//! Contains all HTTP endpoint handlers.

use std::borrow::Cow;
use std::time::SystemTime;
use std::{fmt, io};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::id::{ObjectId, Scope, Scopes};
use objectstore_types::Metadata;
use serde::{Deserialize, Serialize, de};

use crate::error::ApiResult;
use crate::state::ServiceState;

pub fn routes() -> Router<ServiceState> {
    let routes_v1 = Router::new()
        .route("/objects/{usecase}/{scopes}", routing::post(objects_post))
        .route("/objects/{usecase}/{scopes}/", routing::post(objects_post))
        .route(
            "/objects/{usecase}/{scopes}/{*key}",
            routing::get(object_get)
                .head(object_head)
                .put(object_put)
                // TODO(ja): Implement PATCH (metadata update w/o body)
                // .patch(object_patch)
                .delete(object_delete),
        )
        // legacy
        .route(
            "/{*path}",
            routing::post(deprecated_insert)
                .put(deprecated_insert)
                .get(deprecated_get)
                .delete(deprecated_delete),
        );

    Router::new()
        .route("/health", routing::get(health))
        .nest("/v1/", routes_v1)
}

/// TODO(ja): Doc
/// TODO(ja): Move to extractors
#[derive(Clone, Debug)]
pub struct PathScopes(pub Scopes);

impl PathScopes {
    /// TODO(ja): Doc
    pub fn into_scopes(self) -> Scopes {
        self.0
    }
}

impl<'de> de::Deserialize<'de> for PathScopes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;

        // TODO(ja): Align what our syntax for *no scopes* is.
        if s == "_" {
            return Ok(PathScopes(Scopes::empty()));
        }

        let scopes = s
            .split(';')
            .map(|s| {
                let (key, value) = s
                    .split_once("=")
                    .ok_or_else(|| de::Error::custom("scope must be 'key=value'"))?;

                Scope::create(key, value).map_err(de::Error::custom)
            })
            .collect::<Result<Scopes, _>>()?;

        Ok(Self(scopes))
    }
}

/// TODO(ja): Doc
#[derive(Clone, Debug, Deserialize)]
struct ObjectsParams {
    usecase: String,
    // TODO(ja): Use serde(remote)
    scopes: PathScopes,
}

impl ObjectsParams {
    /// TODO(ja): Doc
    pub fn into_object_id(self) -> ObjectId {
        ObjectId::random(self.usecase, self.scopes.into_scopes())
    }
}

/// TODO(ja): Doc
#[derive(Clone, Debug, Deserialize)]
struct ObjectParams {
    usecase: String,
    scopes: PathScopes,
    key: String,
}

impl ObjectParams {
    /// TODO(ja): Doc
    pub fn into_object_id(self) -> ObjectId {
        ObjectId {
            usecase: self.usecase,
            scopes: self.scopes.into_scopes(),
            key: self.key,
        }
    }
}

// TODO(ja): Create axum extractors for these so we can auto-populate the scope on extraction.
fn populate_sentry_scope(path: &ObjectId) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", &path.usecase);
        s.set_extra("scope", path.scopes.as_storage_path().to_string().into());
        s.set_extra("key", path.key.clone().into());
    });
}

// ----------------------------------------------------
// NEW ROUTES. TODO(ja): Move into subfile
// ----------------------------------------------------

#[derive(Debug, Serialize)]
struct InsertObjectResponse {
    key: String,
}

async fn objects_post(
    State(state): State<ServiceState>,
    Path(params): Path<ObjectsParams>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    populate_sentry_scope(&id);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_path = state.service.put_object(id, &metadata, stream).await?;
    let response = Json(InsertObjectResponse {
        key: response_path.key.to_string(),
    });

    Ok((StatusCode::CREATED, response).into_response())
}

async fn object_get(
    State(state): State<ServiceState>,
    Path(params): Path<ObjectsParams>,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    populate_sentry_scope(&id);

    let Some((metadata, stream)) = state.service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn object_head(
    State(state): State<ServiceState>,
    Path(params): Path<ObjectParams>,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    populate_sentry_scope(&id);

    let Some((metadata, _stream)) = state.service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;

    Ok((StatusCode::NO_CONTENT, headers).into_response())
}

async fn object_put(
    State(state): State<ServiceState>,
    Path(params): Path<ObjectParams>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    populate_sentry_scope(&id);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_path = state.service.put_object(id, &metadata, stream).await?;
    let response = Json(InsertObjectResponse {
        key: response_path.key.to_string(),
    });

    Ok((StatusCode::OK, response).into_response())
}

async fn object_delete(
    State(state): State<ServiceState>,
    Path(params): Path<ObjectParams>,
) -> ApiResult<impl IntoResponse> {
    let id = params.into_object_id();
    populate_sentry_scope(&id);

    state.service.delete_object(&id).await?;

    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> impl IntoResponse {
    "OK"
}

// ----------------------------------------------------
// OLD ROUTES. TODO(ja): Move into subfile, remove eventually
// ----------------------------------------------------

/// Magic URL segment that separates objectstore context from an object's user-provided key.
const PATH_CONTEXT_SEPARATOR: &str = "objects";

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
    populate_sentry_scope(&id);

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
    populate_sentry_scope(&id);
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
    populate_sentry_scope(&id);

    state.service.delete_object(&id).await?;

    Ok(StatusCode::NO_CONTENT)
}
