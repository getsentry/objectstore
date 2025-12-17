use std::{fmt::Debug, path::Path, pin::Pin};

use anyhow::Context;
use axum::{
    RequestExt, Router,
    body::{Body, Bytes},
    extract::{FromRequest, Request},
    http::{
        StatusCode,
        header::{HeaderValue, USER_AGENT},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Buf;
use futures::{Stream, stream};
use http::header::CONTENT_TYPE;
use multer::{Constraints, Multipart, SizeLimit};
use objectstore_service::{BACKEND_SIZE_THRESHOLD, id::ObjectKey};
use objectstore_types::Metadata;
use serde::Deserialize;

use crate::error::AnyhowResponse;

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
pub enum Operation {
    Get(ObjectKey),
    Insert(Option<ObjectKey>),
    Delete(ObjectKey),
}

#[derive(Deserialize, Debug)]
pub struct Manifest {
    pub operations: Vec<Operation>,
}

pub struct BatchRequest {
    pub manifest: Manifest,
    pub parts: Pin<Box<dyn Stream<Item = Result<(Metadata, Bytes), anyhow::Error>> + Send>>,
}

const MANIFEST_FIELD_NAME: &'static str = "manifest";

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

        let mut parts = Multipart::with_constraints(
            request.into_body().into_data_stream(),
            boundary,
            Constraints::new().size_limit(
                SizeLimit::new()
                    // 200 MiB: BigTable's maximum size for a single mutation
                    .whole_stream(200 * 1024 * 1024)
                    // A single operation serializes to (minimum) roughly 14 bytes, so this roughly
                    // means we accept a maximum of 10_000 operations per batch request
                    .for_field(MANIFEST_FIELD_NAME, 14 * 10_000)
                    // Each payload needs to be within the maximum size supported by BigTable
                    .per_field(BACKEND_SIZE_THRESHOLD as u64),
            ),
        );

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

        let parts = Box::pin(stream::unfold(parts, |mut m| async move {
            match m.next_field().await {
                Ok(Some(field)) => {
                    let metadata = match Metadata::from_headers(field.headers(), "") {
                        Ok(metadata) => metadata,
                        Err(err) => {
                            return Some((Err(err.into()), m));
                        }
                    };
                    let bytes = match field.bytes().await {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            return Some((Err(err.into()), m));
                        }
                    };
                    Some((Ok((metadata, bytes)), m))
                }
                Ok(None) => None,
                Err(err) => Some((Err(err).context("failed to parse multipart part"), m)),
            }
        }));

        Ok(Self { manifest, parts })
    }
}
