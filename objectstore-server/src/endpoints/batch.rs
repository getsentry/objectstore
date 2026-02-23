use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::{IntoResponse, Response};
use axum::routing;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use objectstore_service::batch::{BatchExecutor, Operation, OperationResult, Outcome};
use objectstore_service::id::{ObjectContext, ObjectKey};
use objectstore_types::auth::Permission;
use objectstore_types::metadata::Metadata;

use crate::auth::AuthAwareService;
use crate::batch::{HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND};
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchOperationStream};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

struct GetResponse {
    pub key: ObjectKey,
    pub result: ApiResult<Option<(Metadata, Bytes)>>,
}

impl std::fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetResponse")
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Debug)]
struct InsertResponse {
    pub key: ObjectKey,
    pub result: ApiResult<objectstore_service::service::InsertResponse>,
}

#[derive(Debug)]
struct DeleteResponse {
    pub key: ObjectKey,
    pub result: ApiResult<objectstore_service::service::DeleteResponse>,
}

#[derive(Debug)]
struct ErrorResponse {
    pub key: Option<ObjectKey>,
    pub kind: Option<&'static str>,
    pub error: ApiError,
}

#[derive(Debug)]
enum OperationResponse {
    Get(GetResponse),
    Insert(InsertResponse),
    Delete(DeleteResponse),
    Error(ErrorResponse),
}

async fn batch(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    requests: BatchOperationStream,
) -> Response {
    // Attempt to acquire a window from available service permits.
    // If no permits are available, reject the entire batch immediately.
    let executor = match BatchExecutor::new(&state.service) {
        Ok(e) => e,
        Err(objectstore_service::error::Error::AtCapacity) => {
            return ApiError::Service(objectstore_service::error::Error::AtCapacity)
                .into_response();
        }
        Err(e) => return ApiError::Service(e).into_response(),
    };

    merni::gauge!("service.batch.window": executor.window());

    let captured_errors: Arc<Mutex<Vec<OperationResponse>>> = Arc::new(Mutex::new(Vec::new()));

    // Build a clean stream of validated operations. Parse errors, rate-limit
    // rejections, and auth failures are captured in `captured_errors` rather
    // than propagating as stream items, so the executor receives a clean
    // `Stream<Item = Operation>`.
    let clean_stream: BoxStream<'static, Operation> = {
        let captured = Arc::clone(&captured_errors);
        let state_ref = Arc::clone(&state);
        let context_ref = context.clone();

        async_stream::stream! {
            let captured = captured;
            let state = state_ref;
            let context = context_ref;
            // Move the AuthAwareService into this generator so it lives for the
            // duration of validation without needing Clone or Arc.
            let service = service;
            let mut raw = requests.0;

            while let Some(result) = raw.next().await {
                let mut op = match result {
                    Ok(op) => op,
                    Err(e) => {
                        captured.lock().unwrap().push(OperationResponse::Error(ErrorResponse {
                            key: None,
                            kind: None,
                            error: e.into(),
                        }));
                        continue;
                    }
                };

                let key = op.key().to_owned();
                let kind = op.kind();

                // Rate limit check.
                if !state.rate_limiter.check(&context) {
                    tracing::debug!("Batch operation rejected due to rate limits");
                    captured.lock().unwrap().push(OperationResponse::Error(ErrorResponse {
                        key: Some(key),
                        kind: Some(kind),
                        error: BatchError::RateLimited.into(),
                    }));
                    continue;
                }

                // Auth check.
                let perm = match &op {
                    Operation::Get(_) => Permission::ObjectRead,
                    Operation::Insert(_) => Permission::ObjectWrite,
                    Operation::Delete(_) => Permission::ObjectDelete,
                };
                if let Err(e) = service.check_permission(perm, &context) {
                    captured.lock().unwrap().push(OperationResponse::Error(ErrorResponse {
                        key: Some(key),
                        kind: Some(kind),
                        error: e,
                    }));
                    continue;
                }

                // For inserts: stamp time_created and record bandwidth now that the
                // operation is known to be valid.
                if let Operation::Insert(ref mut insert) = op {
                    insert.metadata.time_created = Some(SystemTime::now());
                    state.record_bandwidth(&context, insert.payload.len() as u64);
                }

                yield op;
            }
        }
        .boxed()
    };

    // Run the validated operations through the concurrent executor.
    let state_for_conv = Arc::clone(&state);
    let context_for_conv = context.clone();

    let executor_stream: BoxStream<'static, OperationResponse> = executor
        .execute(state.service.clone(), context, clean_stream)
        .then(move |outcome| {
            let state = Arc::clone(&state_for_conv);
            let context = context_for_conv.clone();
            async move { convert_outcome(outcome, &state, &context).await }
        })
        .boxed();

    // Yield executor outcomes first, then append any captured validation errors.
    let responses: BoxStream<'static, OperationResponse> = async_stream::stream! {
        let captured = captured_errors;
        let stream = executor_stream;
        futures::pin_mut!(stream);
        while let Some(r) = stream.next().await {
            yield r;
        }
        let drained: Vec<_> = captured.lock().unwrap().drain(..).collect();
        for error in drained {
            yield error;
        }
    }
    .boxed();

    let r = rand::random::<u128>();
    responses.into_multipart_response(r)
}

/// Converts a service-level [`Outcome`] into an endpoint [`OperationResponse`].
///
/// For get operations this collects the payload stream and applies bandwidth metering.
async fn convert_outcome(
    outcome: Outcome,
    state: &crate::state::Services,
    context: &ObjectContext,
) -> OperationResponse {
    let key = outcome.id.key;
    match outcome.result {
        OperationResult::Got(Ok(Some((metadata, stream)))) => {
            let metered_stream = state.meter_stream(stream, context);
            match metered_stream.try_collect::<BytesMut>().await {
                Ok(bytes) => OperationResponse::Get(GetResponse {
                    key,
                    result: Ok(Some((metadata, bytes.freeze()))),
                }),
                Err(e) => OperationResponse::Get(GetResponse {
                    key,
                    result: Err(ApiError::Service(e.into())),
                }),
            }
        }
        OperationResult::Got(Ok(None)) => OperationResponse::Get(GetResponse {
            key,
            result: Ok(None),
        }),
        OperationResult::Got(Err(e)) => OperationResponse::Get(GetResponse {
            key,
            result: Err(e.into()),
        }),
        OperationResult::Inserted(result) => OperationResponse::Insert(InsertResponse {
            key,
            result: result.map_err(Into::into),
        }),
        OperationResult::Deleted(result) => OperationResponse::Delete(DeleteResponse {
            key,
            result: result.map_err(Into::into),
        }),
    }
}

fn insert_key_header(headers: &mut HeaderMap, key: &ObjectKey) {
    let encoded = BASE64_STANDARD.encode(key.as_bytes());
    headers.insert(
        HEADER_BATCH_OPERATION_KEY,
        encoded
            .parse()
            .expect("base64 encoded string is always a valid header value"),
    );
}

fn insert_kind_header(headers: &mut HeaderMap, kind: &str) {
    headers.insert(
        HEADER_BATCH_OPERATION_KIND,
        kind.parse()
            .expect("operation kind is always a valid header value"),
    );
}

fn insert_status_header(headers: &mut HeaderMap, status: StatusCode) {
    let status_str = format!(
        "{} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .trim()
    .to_owned();

    headers.insert(
        HEADER_BATCH_OPERATION_STATUS,
        status_str.parse().expect("always a valid header value"),
    );
}

fn create_success_part(
    key: &ObjectKey,
    kind: &str,
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Bytes,
    additional_headers: Option<HeaderMap>,
) -> Part {
    let mut headers = HeaderMap::new();
    insert_key_header(&mut headers, key);
    insert_kind_header(&mut headers, kind);
    insert_status_header(&mut headers, status);
    if let Some(additional) = additional_headers {
        headers.extend(additional);
    }
    Part::new(body, headers, content_type)
}

fn create_error_part(key: Option<&ObjectKey>, kind: Option<&str>, error: &ApiError) -> Part {
    let mut headers = HeaderMap::new();
    if let Some(key) = key {
        insert_key_header(&mut headers, key);
    }
    if let Some(kind) = kind {
        insert_kind_header(&mut headers, kind);
    }
    insert_status_header(&mut headers, error.status());

    let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(error))
        .inspect_err(|err| {
            tracing::error!(
                error = err as &dyn std::error::Error,
                "error serializing ApiErrorResponse, this should never happen"
            )
        })
        .unwrap_or_default();
    Part::new(Bytes::from(error_body), headers, None)
}

impl From<OperationResponse> for Part {
    fn from(value: OperationResponse) -> Self {
        match value {
            OperationResponse::Get(GetResponse { key, result }) => match result {
                Ok(Some((metadata, bytes))) => {
                    let mut metadata_headers = match metadata.to_headers("") {
                        Ok(headers) => headers,
                        Err(err) => {
                            let err = BatchError::ResponseSerialization {
                                context: "serializing object metadata".to_owned(),
                                cause: Box::new(err),
                            }
                            .into();
                            return create_error_part(Some(&key), Some("get"), &err);
                        }
                    };
                    create_success_part(
                        &key,
                        "get",
                        StatusCode::OK,
                        metadata_headers.remove(CONTENT_TYPE),
                        bytes,
                        Some(metadata_headers),
                    )
                }
                Ok(None) => create_success_part(
                    &key,
                    "get",
                    StatusCode::NOT_FOUND,
                    None,
                    Bytes::new(),
                    None,
                ),
                Err(error) => create_error_part(Some(&key), Some("get"), &error),
            },
            OperationResponse::Insert(InsertResponse { key, result }) => match result {
                // XXX: this could actually be either StatusCode::OK or StatusCode::CREATED, the service
                // layer doesn't allow us to distinguish between them currently
                Ok(_) => create_success_part(
                    &key,
                    "insert",
                    StatusCode::CREATED,
                    None,
                    Bytes::new(),
                    None,
                ),
                Err(error) => create_error_part(Some(&key), Some("insert"), &error),
            },
            OperationResponse::Delete(DeleteResponse { key, result }) => match result {
                Ok(_) => create_success_part(
                    &key,
                    "delete",
                    StatusCode::NO_CONTENT,
                    None,
                    Bytes::new(),
                    None,
                ),
                Err(error) => create_error_part(Some(&key), Some("delete"), &error),
            },
            OperationResponse::Error(ErrorResponse { key, kind, error }) => {
                create_error_part(key.as_ref(), kind, &error)
            }
        }
    }
}
