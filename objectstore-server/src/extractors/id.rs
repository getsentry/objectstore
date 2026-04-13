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
            return Err(ObjectRejection::Killswitched);
        }

        if !state.rate_limiter.check(id.context()) {
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

/// Parses an [`ObjectId`] from an HTTP request path matching the object endpoint shape.
///
/// This accepts paths with arbitrary prefixes as long as they contain an `/objects/` segment,
/// such as `/v1/objects/{usecase}/{scopes}/{key}`.
pub(crate) fn object_id_from_uri_path(path: &str) -> Option<ObjectId> {
    let rest = path
        .find("/objects/")
        .map(|i| &path[i + "/objects/".len()..])?;

    let mut parts = rest.splitn(3, '/');
    let usecase = parts.next()?.to_string();
    let scopes = parse_scopes_str(parts.next()?).ok()?;
    let key = parts.next()?.to_string();

    if usecase.is_empty() || key.is_empty() {
        return None;
    }

    Some(ObjectId::from_parts(usecase, scopes, key))
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
    parse_scopes_str(&s).map_err(de::Error::custom)
}

fn parse_scopes_str(scopes_str: &str) -> Result<Scopes, String> {
    if scopes_str == EMPTY_SCOPES {
        return Ok(Scopes::empty());
    }

    scopes_str
        .split(';')
        .map(|scope| {
            let (key, value) = scope
                .split_once('=')
                .ok_or_else(|| "scope must be 'key=value'".to_string())?;

            Scope::create(key, value).map_err(|err| err.to_string())
        })
        .collect()
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
            return Err(ObjectRejection::Killswitched);
        }

        if !state.rate_limiter.check(&context) {
            return Err(ObjectRejection::RateLimited);
        }

        Ok(Xt(context))
    }
}

/// Path parameters for extracting an [`ObjectContext`] from a request path.
///
/// Works on both collection-level (`/objects/{usecase}/{scopes}`) and object-level
/// (`/objects/{usecase}/{scopes}/{*key}`) routes — the extra `key` parameter is ignored.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct ContextParams {
    pub usecase: String,
    #[serde(deserialize_with = "deserialize_scopes")]
    pub scopes: Scopes,
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
    use serde::de::IntoDeserializer;
    use serde::de::value::{CowStrDeserializer, Error as DeError};
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

    #[test]
    fn parse_object_id_from_uri_path_with_prefix() {
        let id = object_id_from_uri_path("/api/prefix/v1/objects/myusecase/org=1;project=2/a/b")
            .unwrap();

        assert_eq!(id.usecase(), "myusecase");
        assert_eq!(id.scopes().get_value("org"), Some("1"));
        assert_eq!(id.scopes().get_value("project"), Some("2"));
        assert_eq!(id.key(), "a/b");
    }

    #[test]
    fn parse_object_id_from_uri_path_rejects_missing_key() {
        assert!(object_id_from_uri_path("/v1/objects/myusecase/org=1/").is_none());
    }

    // --- Extractor integration tests ---

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, post};
    use objectstore_service::StorageService;
    use objectstore_service::backend::in_memory::InMemoryBackend;
    use tower::ServiceExt;

    use crate::auth::PublicKeyDirectory;
    use crate::config::Config;
    use crate::killswitches::{Killswitch, Killswitches};
    use crate::rate_limits::{RateLimiter, RateLimits, ThroughputLimits};
    use crate::state::{ServiceState, Services};
    use crate::web::RequestCounter;

    async fn test_state(config: Config) -> ServiceState {
        let service = StorageService::new(Box::new(InMemoryBackend::new("in-memory")));
        let key_directory = PublicKeyDirectory::try_from(&config.auth).unwrap();
        let rate_limiter = RateLimiter::new(config.rate_limits.clone());

        Arc::new(Services {
            config,
            service,
            key_directory,
            rate_limiter,
            request_counter: RequestCounter::new(0),
        })
    }

    async fn handle_object_id(Xt(id): Xt<ObjectId>) -> String {
        format!(
            "usecase={} key={} scopes_empty={}",
            id.context().usecase,
            id.key(),
            id.context().scopes.is_empty(),
        )
    }

    async fn handle_object_context(Xt(ctx): Xt<ObjectContext>) -> String {
        format!(
            "usecase={} scopes_empty={}",
            ctx.usecase,
            ctx.scopes.is_empty(),
        )
    }

    fn test_router(state: ServiceState) -> Router {
        Router::new()
            .route("/objects/{usecase}/{scopes}/{*key}", get(handle_object_id))
            .route("/objects/{usecase}/{scopes}/", post(handle_object_context))
            .with_state(state)
    }

    async fn response_body(response: axum::http::Response<Body>) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    // Extraction tests

    #[tokio::test]
    async fn extract_object_id_parses_path() {
        let state = test_state(Config::default()).await;
        let app = test_router(state);

        let request = Request::builder()
            .uri("/objects/myusecase/org=123;project=456/my-key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        assert!(body.contains("usecase=myusecase"));
        assert!(body.contains("key=my-key"));
        assert!(body.contains("scopes_empty=false"));
    }

    #[tokio::test]
    async fn extract_object_id_with_empty_scopes() {
        let state = test_state(Config::default()).await;
        let app = test_router(state);

        let request = Request::builder()
            .uri("/objects/myusecase/_/my-key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        assert!(body.contains("scopes_empty=true"));
    }

    #[tokio::test]
    async fn extract_object_context_parses_path() {
        let state = test_state(Config::default()).await;
        let app = test_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/myusecase/org=123;project=456/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        assert!(body.contains("usecase=myusecase"));
        assert!(body.contains("scopes_empty=false"));
    }

    #[tokio::test]
    async fn extract_object_context_with_empty_scopes() {
        let state = test_state(Config::default()).await;
        let app = test_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/myusecase/_/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        assert!(body.contains("scopes_empty=true"));
    }

    #[tokio::test]
    async fn extract_object_id_invalid_scopes() {
        let state = test_state(Config::default()).await;
        let app = test_router(state);

        let request = Request::builder()
            .uri("/objects/myusecase/invalid-no-equals/key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // Killswitch tests

    #[tokio::test]
    async fn extract_object_id_killswitched() {
        let config = Config {
            killswitches: Killswitches(vec![Killswitch {
                usecase: Some("blocked".into()),
                scopes: BTreeMap::new(),
                service: None,
                service_matcher: Default::default(),
            }]),
            ..Config::default()
        };
        let state = test_state(config).await;
        let app = test_router(state);

        let request = Request::builder()
            .uri("/objects/blocked/org=1/key")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let request = Request::builder()
            .uri("/objects/allowed/org=1/key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn extract_object_context_killswitched() {
        let config = Config {
            killswitches: Killswitches(vec![Killswitch {
                usecase: Some("blocked".into()),
                scopes: BTreeMap::new(),
                service: None,
                service_matcher: Default::default(),
            }]),
            ..Config::default()
        };
        let state = test_state(config).await;
        let app = test_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/blocked/org=1/")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/allowed/org=1/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn extract_object_id_killswitched_with_service() {
        let config = Config {
            killswitches: Killswitches(vec![Killswitch {
                usecase: None,
                scopes: BTreeMap::new(),
                service: Some("test-*".into()),
                service_matcher: Default::default(),
            }]),
            ..Config::default()
        };
        let state = test_state(config).await;
        let app = test_router(state);

        // Matching service header → 403
        let request = Request::builder()
            .uri("/objects/any/org=1/key")
            .header("x-downstream-service", "test-service")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // Non-matching service header → 200
        let request = Request::builder()
            .uri("/objects/any/org=1/key")
            .header("x-downstream-service", "other-service")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // No service header → 200
        let request = Request::builder()
            .uri("/objects/any/org=1/key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Rate limiter tests

    #[tokio::test]
    async fn extract_object_id_rate_limited() {
        let config = Config {
            rate_limits: RateLimits {
                throughput: ThroughputLimits {
                    global_rps: Some(1),
                    burst: 0,
                    ..ThroughputLimits::default()
                },
                ..RateLimits::default()
            },
            ..Config::default()
        };
        let state = test_state(config).await;
        let app = test_router(state);

        let request = Request::builder()
            .uri("/objects/test/org=1/key")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = Request::builder()
            .uri("/objects/test/org=1/key")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn extract_object_context_rate_limited() {
        let config = Config {
            rate_limits: RateLimits {
                throughput: ThroughputLimits {
                    global_rps: Some(1),
                    burst: 0,
                    ..ThroughputLimits::default()
                },
                ..RateLimits::default()
            },
            ..Config::default()
        };
        let state = test_state(config).await;
        let app = test_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/test/org=1/")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = Request::builder()
            .method("POST")
            .uri("/objects/test/org=1/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
