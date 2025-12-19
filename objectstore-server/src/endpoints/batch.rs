use std::pin::Pin;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing;
use bytes::BytesMut;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{DeleteResult, GetResult, InsertResult};

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::Operation;
use crate::extractors::{BatchRequest, Xt};
use crate::multipart::{IntoBytesStream, Part};
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut request: BatchRequest,
) -> ApiResult<Response> {
    let r = rand::random::<u128>();
    let boundary = format!("os-boundary-{r:032x}");
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/mixed; boundary={boundary}")).unwrap(),
    );

    let parts: BoxStream<anyhow::Result<Part>> = async_stream::try_stream! {
            while let Some(operation) = request.operations.next().await {
                let res = match operation {
                    Ok(operation) => match operation {
                        Operation::Get(get) => {
                            let res = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;
                            res.into_part().await
                        }
                        Operation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let res = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            res.into_part().await
                        }
                        Operation::Delete(delete) => {
                            let res = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            res.into_part().await
                        }
                    },
                    Err(_) => todo!()
                };
                yield res;
            }
        }.boxed();

    Ok((
        headers,
        Body::from_stream(parts.into_bytes_stream(boundary)),
    )
        .into_response())
}

const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";
const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";

#[async_trait]
pub trait IntoPart {
    async fn into_part(mut self) -> Part;
}

#[async_trait]
impl IntoPart for GetResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(Some((metadata, payload))) => {
                let payload = payload
                    .try_fold(BytesMut::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk);
                        Ok(acc)
                    })
                    .await
                    .unwrap()
                    .freeze();

                let mut headers = metadata.to_headers("", false).unwrap();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("200"),
                );

                Part::new(headers, payload)
            }
            Ok(None) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("404"),
                );
                Part::headers_only(headers)
            }
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            }
        }
    }
}

#[async_trait]
impl IntoPart for InsertResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(id) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_KEY,
                    HeaderValue::from_str(id.key()).unwrap(),
                );
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("200"),
                );
                Part::headers_only(headers)
            }
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            }
        }
    }
}

#[async_trait]
impl IntoPart for DeleteResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(()) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("200"),
                );
                Part::headers_only(headers)
            }
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::PublicKeyDirectory;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use bytes::Bytes;
    use objectstore_service::StorageConfig;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// Tests the batch endpoint end-to-end with insert, get, and delete operations
    #[tokio::test]
    async fn test_batch_endpoint_basic() {
        // Set up temporary filesystem storage
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let storage_service = objectstore_service::StorageService::new(config.clone(), config)
            .await
            .unwrap();

        // Create application state
        let state = Arc::new(crate::state::Services {
            config: crate::config::Config::default(),
            service: storage_service,
            key_directory: PublicKeyDirectory {
                keys: BTreeMap::new(),
            },
        });

        // Build the router with state
        let app = router().with_state(state);

        // Create a batch request with insert, get, delete, and get non-existing key
        let insert_data = b"test data";
        let request_body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: testkey\r\n\
             x-sn-batch-operation-kind: insert\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             {data}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: testkey\r\n\
             x-sn-batch-operation-kind: get\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: testkey\r\n\
             x-sn-batch-operation-kind: delete\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: nonexistent\r\n\
             x-sn-batch-operation-kind: get\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
            data = String::from_utf8_lossy(insert_data),
        );

        let request = Request::builder()
            .uri("/objects:batch/testing/scope=value/")
            .method("POST")
            .header("Content-Type", "multipart/mixed; boundary=boundary")
            .body(Body::from(request_body))
            .unwrap();

        // Call the endpoint
        let response = app.oneshot(request).await.unwrap();

        // Verify response status
        let status = response.status();
        if status != StatusCode::OK {
            let body = response.into_body();
            let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            let error_msg = String::from_utf8_lossy(&body_bytes);
            panic!("Expected 200 OK, got {}: {}", status, error_msg);
        }

        // Get the content type and extract boundary
        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.starts_with("multipart/mixed"));

        // Swap content type from multipart/mixed to multipart/form-data for multer
        let content_type = content_type.replace("multipart/mixed", "multipart/form-data");
        let boundary = multer::parse_boundary(&content_type).unwrap();

        // Parse the multipart response using multer
        let body = response.into_body();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();

        // Create a stream for multer
        use futures::stream;
        let chunks: Vec<_> = body_bytes
            .chunks(64)
            .map(|chunk| Ok::<_, multer::Error>(Bytes::copy_from_slice(chunk)))
            .collect();
        let body_stream = stream::iter(chunks);
        let mut multipart = multer::Multipart::new(body_stream, boundary);

        // Collect all parts
        let mut parts = vec![];
        loop {
            match multipart.next_field().await {
                Ok(Some(field)) => {
                    let headers = field.headers().clone();
                    match field.bytes().await {
                        Ok(data) => parts.push((headers, data)),
                        Err(e) => panic!("Failed to read field bytes: {:?}", e),
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("Failed to get next field: {:?}", e),
            }
        }

        // Should have exactly 4 parts
        assert_eq!(parts.len(), 4);

        // First part: insert response
        let (insert_headers, insert_body) = &parts[0];
        let insert_status = insert_headers
            .get(HEADER_BATCH_OPERATION_STATUS)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(insert_status, "200");

        let insert_key = insert_headers
            .get(HEADER_BATCH_OPERATION_KEY)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(insert_key, "testkey");
        assert!(insert_body.is_empty()); // Insert response has no body

        // Second part: get response
        let (get_headers, get_body) = &parts[1];
        let get_status = get_headers
            .get(HEADER_BATCH_OPERATION_STATUS)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(get_status, "200");

        // Verify the retrieved data matches what we inserted
        assert_eq!(get_body.as_ref(), insert_data);

        // Third part: delete response
        let (delete_headers, delete_body) = &parts[2];
        let delete_status = delete_headers
            .get(HEADER_BATCH_OPERATION_STATUS)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(delete_status, "200");
        assert!(delete_body.is_empty()); // Delete response has no body

        // Fourth part: get non-existing key (should be 404)
        let (not_found_headers, not_found_body) = &parts[3];
        let not_found_status = not_found_headers
            .get(HEADER_BATCH_OPERATION_STATUS)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(not_found_status, "404");
        assert!(not_found_body.is_empty()); // Not found response has no body
    }
}
