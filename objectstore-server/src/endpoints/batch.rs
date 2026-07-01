use std::sync::Arc;
use std::time::SystemTime;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::{IntoResponse, Response};
use axum::routing;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use objectstore_service::id::{ObjectContext, ObjectKey};
use objectstore_service::operation::OperationKind;
use objectstore_service::streaming::{OpResponse, Operation};
use percent_encoding::NON_ALPHANUMERIC;

use crate::auth::AuthAwareService;
use crate::batch::{
    HEADER_BATCH_OPERATION_INDEX, HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND,
};
use crate::endpoints::OpRoute;
use crate::endpoints::common::{ApiError, ApiErrorResponse};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchOperationStream};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route(
            "/objects:batch/{usecase}/{scopes}/",
            routing::post(batch).op(OperationKind::Batch),
        )
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

/// Applies a per-operation check to a stream of indexed operations.
///
/// Errors already in the stream pass through unchanged. For each `Ok(op)`,
/// `check` is called: if it returns `Err(e)` the item becomes `Err(e)`;
/// otherwise the item remains `Ok(op)`.
fn validate<S, E, F>(
    stream: S,
    check: F,
) -> impl futures::Stream<Item = (usize, Result<Operation, E>)> + Send + 'static
where
    S: futures::Stream<Item = (usize, Result<Operation, E>)> + Send + 'static,
    F: Fn(&Operation) -> Result<(), E> + Send + 'static,
{
    stream.map(move |(idx, item)| {
        (
            idx,
            item.and_then(|op| {
                check(&op)?;
                Ok(op)
            }),
        )
    })
}

async fn batch(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    requests: BatchOperationStream,
) -> Response {
    let batch = match state.service.stream() {
        Ok(b) => b,
        Err(e) => return ApiError::Service(e).into_response(),
    };

    objectstore_metrics::gauge!("service.batch.window" = batch.window());

    // Step 1: parse multipart fields → (idx, Result<Operation, ApiError>)
    let parsed = requests.0.map(|r| r.map_err(ApiError::from)).enumerate();

    // Step 2: rate-limit check
    let rate_limited = validate(parsed, {
        let state = Arc::clone(&state);
        let context = context.clone();
        move |op| {
            if state.rate_limiter.check(op.kind(), &context, None) {
                Ok(())
            } else {
                Err(ApiError::from(BatchError::RateLimited))
            }
        }
    });

    // Step 3: auth check
    let authorized = validate(rate_limited, {
        let context = context.clone();
        move |op| service.check_permission(op.permission(), &context)
    });

    // Step 4: use case policy validation
    let policy_checked = validate(authorized, {
        let state = Arc::clone(&state);
        let usecase = context.usecase.clone();
        move |op| {
            if let Operation::Insert(ins) = op {
                state
                    .config
                    .usecases
                    .validate(&usecase, &ins.metadata)
                    .map_err(|e| ApiError::Client(e.to_string()))?;
            }
            Ok(())
        }
    });

    // Step 5: stamp inserts with time_created and record bandwidth
    let stamped = policy_checked.map({
        let state = Arc::clone(&state);
        let context = context.clone();
        move |(idx, mut item)| {
            if let Ok(Operation::Insert(ins)) = &mut item {
                ins.metadata.time_created = Some(SystemTime::now());
                state.record_bandwidth(&context, ins.payload.len() as u64);
            }
            (idx, item)
        }
    });

    // Step 6: execute concurrently, then convert each result to a multipart Part
    let state_ref = Arc::clone(&state);
    let context_ref = context.clone();
    let responses = batch.execute(context, stamped).then(move |(idx, result)| {
        let state = Arc::clone(&state_ref);
        let context = context_ref.clone();
        async move { convert_to_part(idx, result, &state, &context).await }
    });

    responses.into_multipart_response(rand::random())
}

/// Converts a single operation result to a multipart [`Part`].
///
/// For get operations this collects the payload stream and applies bandwidth metering.
/// The `x-sn-batch-operation-index` header is set on every part.
async fn convert_to_part(
    idx: usize,
    result: Result<OpResponse, ApiError>,
    state: &crate::state::Services,
    context: &ObjectContext,
) -> Part {
    match result {
        Ok(OpResponse::Got {
            key,
            response: Some((metadata, _content_range, stream)),
        }) => got_to_part(idx, key, metadata, stream, state, context)
            .await
            .unwrap_or_else(|e| create_error_part(idx, &e)),
        Ok(OpResponse::Got {
            key,
            response: None,
        }) => create_success_part(
            idx,
            &key,
            "get",
            StatusCode::NOT_FOUND,
            None,
            Bytes::new(),
            None,
        ),
        Ok(OpResponse::Inserted { id }) => create_success_part(
            idx,
            &id.key,
            "insert",
            // XXX: this could actually be either StatusCode::OK or StatusCode::CREATED, the service
            // layer doesn't allow us to distinguish between them currently
            StatusCode::CREATED,
            None,
            Bytes::new(),
            None,
        ),
        Ok(OpResponse::Deleted { key }) => create_success_part(
            idx,
            &key,
            "delete",
            StatusCode::NO_CONTENT,
            None,
            Bytes::new(),
            None,
        ),
        Ok(OpResponse::Head {
            key,
            metadata: Some(metadata),
        }) => {
            let metadata_headers = metadata.to_headers("").map_err(|err| {
                ApiError::from(BatchError::ResponseSerialization {
                    context: "serializing object metadata".to_owned(),
                    cause: Box::new(err),
                })
            });
            match metadata_headers {
                Ok(headers) => create_success_part(
                    idx,
                    &key,
                    "head",
                    StatusCode::NO_CONTENT,
                    None,
                    Bytes::new(),
                    Some(headers),
                ),
                Err(e) => create_error_part(idx, &e),
            }
        }
        Ok(OpResponse::Head {
            key,
            metadata: None,
        }) => create_success_part(
            idx,
            &key,
            "head",
            StatusCode::NOT_FOUND,
            None,
            Bytes::new(),
            None,
        ),
        Err(error) => create_error_part(idx, &error),
    }
}

async fn got_to_part(
    idx: usize,
    key: ObjectKey,
    metadata: objectstore_types::metadata::Metadata,
    stream: objectstore_service::PayloadStream,
    state: &crate::state::Services,
    context: &ObjectContext,
) -> Result<Part, ApiError> {
    let bytes = state
        .meter_stream(stream, context)
        .try_collect::<BytesMut>()
        .await
        .map_err(|e| ApiError::Service(e.into()))?
        .freeze();

    let mut metadata_headers = metadata.to_headers("").map_err(|err| {
        ApiError::from(BatchError::ResponseSerialization {
            context: "serializing object metadata".to_owned(),
            cause: Box::new(err),
        })
    })?;

    let content_type = metadata_headers.remove(CONTENT_TYPE);
    Ok(create_success_part(
        idx,
        &key,
        "get",
        StatusCode::OK,
        content_type,
        bytes,
        Some(metadata_headers),
    ))
}

fn insert_index_header(headers: &mut HeaderMap, idx: usize) {
    headers.insert(
        HEADER_BATCH_OPERATION_INDEX,
        idx.to_string()
            .parse()
            .expect("usize display is always a valid header value"),
    );
}

fn insert_key_header(headers: &mut HeaderMap, key: &ObjectKey) {
    let encoded = percent_encoding::percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string();
    headers.insert(
        HEADER_BATCH_OPERATION_KEY,
        encoded
            .parse()
            .expect("percent-encoded string is always a valid header value"),
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
    idx: usize,
    key: &ObjectKey,
    kind: &str,
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Bytes,
    additional_headers: Option<HeaderMap>,
) -> Part {
    let mut headers = HeaderMap::new();
    insert_index_header(&mut headers, idx);
    insert_key_header(&mut headers, key);
    insert_kind_header(&mut headers, kind);
    insert_status_header(&mut headers, status);
    if let Some(additional) = additional_headers {
        headers.extend(additional);
    }
    Part::new(body, headers, content_type)
}

fn create_error_part(idx: usize, error: &ApiError) -> Part {
    let mut headers = HeaderMap::new();
    insert_index_header(&mut headers, idx);
    insert_status_header(&mut headers, error.status());

    let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(error))
        .inspect_err(|err| objectstore_log::error!(!!err, "Failed to serialize ApiErrorResponse"))
        .unwrap_or_default();
    Part::new(Bytes::from(error_body), headers, None)
}
