use std::fmt::Debug;

use anyhow::Context;
use axum::{
    extract::{FromRequest, Request},
    http::StatusCode,
    response::IntoResponse,
};
use bytes::Buf;
use futures::stream;
use http::header::CONTENT_TYPE;
use multer::Multipart;
use objectstore_service::{InsertStream, id::ObjectKey};
use objectstore_types::Metadata;
use serde::Deserialize;

use crate::error::AnyhowResponse;

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "op")]
pub enum Operation {
    Get { key: ObjectKey },
    Insert { key: Option<ObjectKey> },
    Delete { key: ObjectKey },
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Manifest {
    pub operations: Vec<Operation>,
}

pub struct BatchRequest {
    pub manifest: Manifest,
    pub inserts: InsertStream,
}

impl Debug for BatchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchRequest")
            .field("manifest", &self.manifest)
            .finish()
    }
}

impl<S> FromRequest<S> for BatchRequest
where
    S: Send + Sync,
{
    type Rejection = AnyhowResponse;

    async fn from_request(request: Request, _: &S) -> Result<Self, Self::Rejection> {
        let Some(content_type) = request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|ct| ct.to_str().ok())
        else {
            return Err((StatusCode::BAD_REQUEST, "expected valid Content-Type")
                .into_response()
                .into());
        };

        let Ok(mime) = content_type.parse::<mime::Mime>() else {
            return Err((StatusCode::BAD_REQUEST, "expected valid Content-Type")
                .into_response()
                .into());
        };
        if !(mime.type_() == mime::MULTIPART && mime.subtype() == "mixed") {
            return Err((
                StatusCode::BAD_REQUEST,
                "expected Content-Type: multipart/mixed",
            )
                .into_response()
                .into());
        }

        // XXX: `multer::parse_boundary` requires the content-type to be `multipart/form-data`
        let content_type = content_type.replace("multipart/mixed", "multipart/form-data");
        let boundary =
            multer::parse_boundary(content_type).context("failed to parse multipart boundary")?;
        let mut parts = Multipart::new(request.into_body().into_data_stream(), boundary);

        let manifest = parts
            .next_field()
            .await
            .context("failed to parse multipart part")?
            .ok_or(
                (
                    StatusCode::BAD_REQUEST,
                    "expected at least one multipart part",
                )
                    .into_response(),
            )?;
        let manifest = manifest
            .bytes()
            .await
            .context("failed to extract manifest")?;
        let manifest = serde_json::from_reader::<_, Manifest>(manifest.reader())
            .context("failed to parse manifest")?;

        let inserts = Box::pin(stream::unfold(parts, |mut parts| async move {
            match parts.next_field().await {
                Ok(Some(field)) => {
                    let metadata = match Metadata::from_headers(field.headers(), "") {
                        Ok(metadata) => metadata,
                        Err(err) => {
                            return Some((Err(err.into()), parts));
                        }
                    };
                    let bytes = match field.bytes().await {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            return Some((Err(err.into()), parts));
                        }
                    };
                    Some((Ok((metadata, bytes)), parts))
                }
                Ok(None) => None,
                Err(err) => Some((Err(err).context("failed to parse multipart part"), parts)),
            }
        }));

        Ok(Self { manifest, inserts })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header::CONTENT_TYPE};
    use futures::StreamExt;
    use objectstore_types::{ExpirationPolicy, HEADER_EXPIRATION};

    #[tokio::test]
    async fn test_valid_request_works() {
        let manifest = r#"{"operations":[{"op":"Insert"},{"op":"Get","key":"abc123"},{"op":"Insert","key":"xyz789"},{"op":"Delete","key":"def456"}]}"#;
        let insert1_data = b"first blob data";
        let insert2_data = b"second blob data";
        let expiration = ExpirationPolicy::TimeToLive(Duration::from_hours(1));
        let body = format!(
            "--boundary\r\n\
             Content-Type: application/json\r\n\
             \r\n\
             {manifest}\r\n\
             --boundary\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             {insert1}\r\n\
             --boundary\r\n\
             Content-Type: text/plain\r\n\
             {HEADER_EXPIRATION}: {expiration}\r\n\
             \r\n\
             {insert2}\r\n\
             --boundary--\r\n",
            insert1 = String::from_utf8_lossy(insert1_data),
            insert2 = String::from_utf8_lossy(insert2_data),
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/mixed; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchRequest::from_request(request, &()).await.unwrap();

        let expected_manifest = Manifest {
            operations: vec![
                Operation::Insert { key: None },
                Operation::Get {
                    key: "abc123".to_string(),
                },
                Operation::Insert {
                    key: Some("xyz789".to_string()),
                },
                Operation::Delete {
                    key: "def456".to_string(),
                },
            ],
        };
        assert_eq!(batch_request.manifest, expected_manifest);

        let inserts: Vec<_> = batch_request.inserts.collect().await;
        assert_eq!(inserts.len(), 2);

        let (metadata1, bytes1) = inserts[0].as_ref().unwrap();
        assert_eq!(metadata1.content_type, "application/octet-stream");
        assert_eq!(bytes1.as_ref(), insert1_data);

        let (metadata2, bytes2) = inserts[1].as_ref().unwrap();
        assert_eq!(metadata2.content_type, "text/plain");
        assert_eq!(metadata2.expiration_policy, expiration);
        assert_eq!(bytes2.as_ref(), insert2_data);
    }
}
