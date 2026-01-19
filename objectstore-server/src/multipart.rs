//! Utilities for working with Multipart streaming responses.

use axum::body::Body;
use axum::response::IntoResponse as _;
use axum::response::Response;
use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::StatusCode;
use http::header::CONTENT_TYPE;

use crate::endpoints::common::ApiError;

/// A Multipart part.
#[derive(Debug)]
pub struct Part {
    headers: HeaderMap,
    body: Bytes,
}

impl Part {
    /// Creates a new Multipart part with headers and body.
    pub fn new(headers: HeaderMap, body: Bytes) -> Self {
        Part { headers, body }
    }

    /// Creates a new Multipart part with headers only.
    pub fn headers_only(headers: HeaderMap) -> Self {
        Part {
            headers,
            body: Bytes::new(),
        }
    }

    /// Inserts a header into this part's headers.
    pub fn insert_header(
        &mut self,
        name: impl http::header::IntoHeaderName,
        value: http::HeaderValue,
    ) {
        self.headers.insert(name, value);
    }
}

pub trait IntoMultipartResponse {
    fn into_response(self, boundary: String) -> Response;
}

impl<S, T> IntoMultipartResponse for S
where
    S: Stream<Item = T> + Send + 'static,
    T: Into<Part> + Send,
{
    fn into_response(self, boundary: String) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary=\"{}\"", &boundary)
                .parse()
                .expect("should be a valid header value"),
        );

        let boundary = {
            let mut bytes = BytesMut::with_capacity(boundary.len() + 4);
            bytes.put(&b"--"[..]);
            bytes.put(boundary.as_bytes());
            bytes.put(&b"\r\n"[..]);
            bytes.freeze()
        };
        let body: BoxStream<Result<bytes::Bytes, std::convert::Infallible>> =
            async_stream::try_stream! {
                let items = self;
                futures::pin_mut!(items);
                while let Some(item) = items.next().await {
                    yield boundary.clone();
                    let part = item.into();
                    yield serialize_headers(part.headers);
                    yield serialize_body(part.body);
                }

                // Yield closing boundary: --boundary-- (without \r\n in between)
                // The boundary already has --boundary\r\n, so we need to strip the \r\n
                // and add --\r\n instead
                let boundary_str = std::str::from_utf8(&boundary).unwrap();
                let boundary_without_crlf = boundary_str.trim_end_matches("\r\n");
                let mut closing = BytesMut::with_capacity(boundary_without_crlf.len() + 4);
                closing.put(boundary_without_crlf.as_bytes());
                closing.put(&b"--\r\n"[..]);
                yield closing.freeze();
            }
            .boxed();

        (headers, Body::from_stream(body)).into_response()
    }
}

fn serialize_headers(headers: HeaderMap) -> Bytes {
    let mut res = BytesMut::with_capacity(10 + 15 * headers.len());
    for (name, value) in &headers {
        res.put(name.as_str().as_bytes());
        res.put(&b": "[..]);
        res.put(value.as_bytes());
        res.put(&b"\r\n"[..]);
    }
    res.put(&b"\r\n"[..]);
    res.freeze()
}

fn serialize_body(body: Bytes) -> Bytes {
    let mut res = BytesMut::with_capacity(body.len() + 2);
    res.put(body);
    res.put(&b"\r\n"[..]);
    res.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::response::IntoResponse as _;
    use axum_extra::response::multiple::{MultipartForm, Part as AxumPart};
    use http::header::CONTENT_DISPOSITION;

    #[tokio::test]
    async fn test_multipart_with_parts() {
        // Create the same parts using axum_extra first to get its boundary
        let axum_parts = vec![
            AxumPart::raw_part(
                "metadata",
                "application/json",
                r#"{"key":"value"}"#.as_bytes().to_vec(),
                None,
            )
            .expect("valid MIME type"),
            AxumPart::raw_part(
                "file",
                "application/octet-stream",
                vec![0x00, 0x01, 0x02, 0xff, 0xfe],
                Some("data.bin"),
            )
            .expect("valid MIME type"),
        ];

        let axum_response = MultipartForm::with_parts(axum_parts).into_response();

        // Extract boundary from axum's Content-Type header
        let content_type = axum_response
            .headers()
            .get(CONTENT_TYPE)
            .expect("should have content-type")
            .to_str()
            .expect("should be valid string");
        eprintln!("Content-Type: {}", content_type);
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap_or_else(|| panic!("should have boundary, got: {}", content_type))
            .trim_matches('"');

        // Create parts using our implementation with the same boundary
        let our_parts = vec![
            {
                let mut headers = HeaderMap::new();
                headers.insert(
                    CONTENT_DISPOSITION,
                    "form-data; name=\"metadata\"".parse().unwrap(),
                );
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
                Part::new(headers, Bytes::from(r#"{"key":"value"}"#))
            },
            {
                let mut headers = HeaderMap::new();
                headers.insert(
                    CONTENT_DISPOSITION,
                    "form-data; name=\"file\"; filename=\"data.bin\""
                        .parse()
                        .unwrap(),
                );
                headers.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                Part::new(headers, Bytes::from(vec![0x00, 0x01, 0x02, 0xff, 0xfe]))
            },
        ];

        let our_response = futures::stream::iter(our_parts).into_response(boundary.to_string());

        // Collect bodies
        let our_body = to_bytes(our_response.into_body(), usize::MAX)
            .await
            .expect("should collect body");
        let axum_body = to_bytes(axum_response.into_body(), usize::MAX)
            .await
            .expect("should collect body");

        // Compare
        assert_eq!(our_body, axum_body);
    }

    #[tokio::test]
    async fn test_multipart_with_zero_parts() {
        // Create empty parts using axum_extra first to get its boundary
        let axum_parts: Vec<AxumPart> = vec![];

        let axum_response = MultipartForm::with_parts(axum_parts).into_response();

        // Extract boundary from axum's Content-Type header
        let content_type = axum_response
            .headers()
            .get(CONTENT_TYPE)
            .expect("should have content-type")
            .to_str()
            .expect("should be valid string");
        eprintln!("Content-Type: {}", content_type);
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap_or_else(|| panic!("should have boundary, got: {}", content_type))
            .trim_matches('"');

        // Create empty parts using our implementation with the same boundary
        let our_parts: Vec<Part> = vec![];

        let our_response = futures::stream::iter(our_parts).into_response(boundary.to_string());

        // Collect bodies
        let our_body = to_bytes(our_response.into_body(), usize::MAX)
            .await
            .expect("should collect body");
        let axum_body = to_bytes(axum_response.into_body(), usize::MAX)
            .await
            .expect("should collect body");

        // Compare
        assert_eq!(our_body, axum_body);
    }
}
