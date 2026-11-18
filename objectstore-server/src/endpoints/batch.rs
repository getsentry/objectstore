//! The batch endpoint, which executes many object operations in one multipart request.
//!
//! The response uses the layout of the request (see [`BatchFormat`]) and streams one result per
//! operation in completion order. In the JSON layout, each result is a JSON part named by the
//! operation index, followed by a payload part for a successful get.

use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::Response;
use axum::routing;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use objectstore_service::id::{ObjectContext, ObjectKey};
use objectstore_service::streaming::{OpResponse, Operation};
use objectstore_types::headers;
use objectstore_types::metadata::{DEFAULT_CONTENT_TYPE, Metadata, ReadMetadata};
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::batch::{
    HEADER_BATCH_OPERATION_INDEX, HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND,
    payload_part_name,
};
use crate::endpoints::common::{ApiError, ApiErrorResponse};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchFormat, BatchOperationStream};
use crate::extractors::request_time::RequestTime;
use crate::multipart::{IntoMultipartResponse, Part};
use crate::state::{ServiceState, Services};

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
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
    RequestTime(access_time): RequestTime,
    requests: BatchOperationStream,
) -> Response {
    let batch = state.service.stream();
    let BatchOperationStream { format, operations } = requests;

    // Step 1: parse multipart fields → (idx, Result<Operation, ApiError>)
    let parsed = operations.map(|r| r.map_err(ApiError::from)).enumerate();

    // Step 2: rate-limit check
    let rate_limited = validate(parsed, {
        let state = Arc::clone(&state);
        let context = context.clone();
        move |_op| {
            state
                .rate_limiter
                .check(&context, None)
                .map_err(|rejection| ApiError::from(BatchError::RateLimited(rejection)))
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
                state.config.usecases.validate(&usecase, &ins.metadata)?;
            }
            Ok(())
        }
    });

    // Step 5: record bandwidth for inserts
    let metered = policy_checked.map({
        let state = Arc::clone(&state);
        let context = context.clone();
        move |(idx, item)| {
            if let Ok(Operation::Insert(ins)) = &item {
                state.record_bandwidth(&context, ins.payload.len() as u64);
            }
            (idx, item)
        }
    });

    // Step 6: execute concurrently, then convert each result to multipart parts
    let results = batch.execute(context.clone(), metered, access_time);
    match format {
        BatchFormat::Headers => results
            .then(move |(idx, result)| {
                let state = Arc::clone(&state);
                let context = context.clone();
                async move { convert_to_part(idx, result, &state, &context).await }
            })
            .into_multipart_response(rand::random()),
        BatchFormat::Json => results
            .then(move |(idx, result)| {
                let state = Arc::clone(&state);
                let context = context.clone();
                async move { convert_to_json_parts(idx, result, &state, &context).await }
            })
            .flat_map(futures::stream::iter)
            .into_multipart_response(rand::random()),
    }
}

/// The JSON body of a result part in the JSON layout.
#[derive(Debug, Default, Serialize)]
struct OperationResult {
    /// The HTTP status code of the operation.
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<ObjectKey>,
    /// The object's metadata, for a successful get or head.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<ReadMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ApiErrorResponse>,
}

impl OperationResult {
    fn failure(error: &ApiError) -> Self {
        // Capture the error explicitly, as it is not converted via `IntoResponse` here.
        error.capture();
        Self {
            status: error.status().as_u16(),
            error: Some(ApiErrorResponse::from_error(error)),
            ..Default::default()
        }
    }
}

/// Returns the status of a successful operation.
fn response_status(response: &OpResponse) -> StatusCode {
    match response {
        OpResponse::Got { response: None, .. } | OpResponse::Head { metadata: None, .. } => {
            StatusCode::NOT_FOUND
        }
        OpResponse::Got { .. } | OpResponse::Head { .. } => StatusCode::OK,
        // XXX: this could actually be either StatusCode::OK or StatusCode::CREATED, the service
        // layer doesn't allow us to distinguish between them currently
        OpResponse::Inserted { .. } => StatusCode::CREATED,
        OpResponse::Deleted { .. } => StatusCode::NO_CONTENT,
    }
}

/// Converts a single operation result to the parts of the JSON layout.
///
/// Every result becomes a JSON part named `"<idx>"`. A successful get also gets a payload part
/// named `"<idx>.payload"` with filename `"<idx>"`. For gets, this collects the payload stream
/// and applies bandwidth metering.
async fn convert_to_json_parts(
    idx: usize,
    result: Result<OpResponse, ApiError>,
    state: &Services,
    context: &ObjectContext,
) -> impl Iterator<Item = Part> + use<> {
    let response = match result {
        Ok(response) => response,
        Err(error) => return json_parts(idx, OperationResult::failure(&error), None),
    };

    let mut result = OperationResult {
        status: response_status(&response).as_u16(),
        kind: Some(response.kind()),
        key: Some(response.key().clone()),
        ..Default::default()
    };

    let mut payload = None;
    match response {
        OpResponse::Got {
            response: Some((metadata, _, stream)),
            ..
        } => match collect_payload(stream, state, context).await {
            Ok(bytes) => {
                payload = Some((bytes, payload_content_type(&metadata)));
                result.metadata = Some(metadata.into());
            }
            Err(error) => return json_parts(idx, OperationResult::failure(&error), None),
        },
        OpResponse::Head {
            metadata: Some(metadata),
            ..
        } => result.metadata = Some(metadata.into()),
        _ => {}
    }

    json_parts(idx, result, payload)
}

/// Builds the result part of `idx` and, for a get, its payload part.
fn json_parts(
    idx: usize,
    result: OperationResult,
    payload: Option<(Bytes, HeaderValue)>,
) -> impl Iterator<Item = Part> {
    let (body, payload) = match serde_json::to_vec(&result) {
        Ok(body) => (body, payload),
        Err(error) => {
            objectstore_log::error!(!!&error, "failed to serialize batch result");
            (br#"{"status":500}"#.to_vec(), None)
        }
    };

    // INVARIANT: decimal indices and `<idx>.payload` are valid part names and filenames.
    let name = idx.to_string();
    let json = HeaderValue::from_static("application/json");
    let result_part =
        Part::named(&name, body.into(), None, Some(json)).expect("decimal part name is valid");

    let payload_part = payload.map(|(bytes, content_type)| {
        Part::named(
            &payload_part_name(idx),
            bytes,
            Some(&name),
            Some(content_type),
        )
        .expect("payload part name and filename are valid")
    });

    std::iter::once(result_part).chain(payload_part)
}

/// Returns the `Content-Type` of a get payload part.
///
/// Encoded objects use `application/octet-stream`, so that no client mistakes the encoded bytes
/// for the object's type. The real type is always in the result's `metadata`.
fn payload_content_type(metadata: &Metadata) -> HeaderValue {
    let content_type = match metadata.compression {
        Some(_) => DEFAULT_CONTENT_TYPE,
        None => &metadata.content_type,
    };

    // Stored content types are validated media types, so this falls back only defensively.
    HeaderValue::from_str(content_type)
        .unwrap_or_else(|_| HeaderValue::from_static(DEFAULT_CONTENT_TYPE))
}

/// Collects a get payload into memory and records its bandwidth.
async fn collect_payload(
    stream: objectstore_service::PayloadStream,
    state: &Services,
    context: &ObjectContext,
) -> Result<Bytes, ApiError> {
    state
        .meter_stream(stream, context)
        .try_collect::<BytesMut>()
        .await
        .map(BytesMut::freeze)
        .map_err(|e| {
            objectstore_log::error!(!!&e, "failed to collect payload stream");
            ApiError::Service(e.into())
        })
}

/// Converts a single operation result to a multipart [`Part`] of the header layout.
///
/// For get operations this collects the payload stream and applies bandwidth metering.
/// The `x-sn-batch-operation-index` header is set on every part.
async fn convert_to_part(
    idx: usize,
    result: Result<OpResponse, ApiError>,
    state: &Services,
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
                    StatusCode::OK,
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
    metadata: Metadata,
    stream: objectstore_service::PayloadStream,
    state: &Services,
    context: &ObjectContext,
) -> Result<Part, ApiError> {
    let bytes = collect_payload(stream, state, context).await?;

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
    headers.insert(
        HEADER_BATCH_OPERATION_KEY,
        headers::encode_header_value(key),
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
    // Capture the error explicitly, as it is not converted via `IntoResponse` here.
    error.capture();

    let mut headers = HeaderMap::new();
    insert_index_header(&mut headers, idx);
    insert_status_header(&mut headers, error.status());

    let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(error))
        .inspect_err(|err| objectstore_log::error!(!!err, "Failed to serialize ApiErrorResponse"))
        .unwrap_or_default();
    Part::new(Bytes::from(error_body), headers, None)
}
