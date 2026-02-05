use std::time::SystemTime;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::Response;
use axum::routing;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_types::Metadata;

use crate::auth::AuthAwareService;
use crate::batch::{HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND};
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchOperationStream, Operation};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::rate_limits::MeteredPayloadStream;
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
    pub result: ApiResult<objectstore_service::InsertResponse>,
}

#[derive(Debug)]
struct DeleteResponse {
    pub key: ObjectKey,
    pub result: ApiResult<objectstore_service::DeleteResponse>,
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
    mut requests: BatchOperationStream,
) -> Response {
    let responses: BoxStream<OperationResponse> = async_stream::stream! {
        while let Some(operation) = requests.0.next().await {
            let (operation, key, kind) = match operation {
                Ok(op) => {
                    let key = op.key().clone();
                    let kind = match &op {
                        Operation::Get(_) => "get",
                        Operation::Insert(_) => "insert",
                        Operation::Delete(_) => "delete",
                    };
                    (Ok(op), Some(key), Some(kind))
                }
                Err(e) => (Err(e), None, None),
            };

            if !state.rate_limiter.check(&context) {
                tracing::debug!("Batch operation rejected due to rate limits");
                yield OperationResponse::Error(ErrorResponse {
                    key,
                    kind,
                    error: BatchError::RateLimited.into(),
                });
                continue;
            }

            let result = match operation {
                Ok(operation) => match operation {
                    Operation::Get(get) => {
                        let key = get.key.clone();
                        let result = service
                            .get_object(&ObjectId::new(context.clone(), get.key))
                            .await;

                        let result = match result {
                            Ok(Some((metadata, stream))) => {
                                let metered_stream = MeteredPayloadStream::from(
                                    stream,
                                    state.rate_limiter.bytes_accumulator()
                                );
                                match metered_stream.try_collect::<BytesMut>().await {
                                    Ok(bytes) => Ok(Some((metadata, bytes.freeze()))),
                                    Err(e) => Err(ApiError::Service(e.into())),
                                }
                            }
                            Ok(None) => Ok(None),
                            Err(e) => Err(e),
                        };

                        OperationResponse::Get(GetResponse { key, result })
                    }
                    Operation::Insert(insert) => {
                        let key = insert.key.clone();
                        let mut metadata = insert.metadata;
                        metadata.time_created = Some(SystemTime::now());

                        let payload_len = insert.payload.len() as u64;
                        state.rate_limiter.bytes_accumulator()
                            .fetch_add(payload_len, std::sync::atomic::Ordering::Relaxed);

                        let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                        let result = service
                            .insert_object(context.clone(), Some(insert.key), &metadata, stream)
                            .await;
                        OperationResponse::Insert(InsertResponse { key, result })
                    }
                    Operation::Delete(delete) => {
                        let key = delete.key.clone();
                        let result = service
                            .delete_object(&ObjectId::new(context.clone(), delete.key))
                            .await;
                        OperationResponse::Delete(DeleteResponse { key, result })
                    }
                },
                Err(error) => OperationResponse::Error(ErrorResponse {
                    key,
                    kind,
                    error: error.into(),
                }),
            };
            yield result;
        }
    }
    .boxed();

    let r = rand::random::<u128>();
    responses.into_multipart_response(r)
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

fn insert_kind_header(headers: &mut HeaderMap, kind: &str) {
    headers.insert(
        HEADER_BATCH_OPERATION_KIND,
        kind.parse().expect("always a valid header value"),
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
                    let mut metadata_headers = match metadata.to_headers("", false) {
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
