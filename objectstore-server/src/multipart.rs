//! Utilities for Multipart streaming responses.

use std::pin::Pin;

use axum::body::Body;
use axum::response::IntoResponse as _;
use axum::response::Response;
use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::header::CONTENT_TYPE;

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

    /// Adds a header to this part.
    pub fn add_header(
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
    T: IntoPart + Send,
{
    fn into_response(self, boundary: String) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary=\"{}\"", &boundary)
                .parse()
                .unwrap(),
        );

        let body_stream: BoxStream<Result<bytes::Bytes, std::convert::Infallible>> =
            async_stream::try_stream! {
                let mut boundary_bytes = BytesMut::with_capacity(boundary.len() + 4);
                boundary_bytes.put(&b"--"[..]);
                boundary_bytes.put(boundary.as_bytes());
                boundary_bytes.put(&b"\r\n"[..]);
                let boundary = boundary_bytes.freeze();

                let items = self;
                futures::pin_mut!(items);
                while let Some(item) = items.next().await {
                    let part = item.into_part();

                    yield boundary.clone();
                    yield serialize_headers(part.headers);

                    let mut body_with_newline = BytesMut::with_capacity(part.body.len() + 2);
                    body_with_newline.put(part.body);
                    body_with_newline.put(&b"\r\n"[..]);
                    yield body_with_newline.freeze();
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

        (headers, Body::from_stream(body_stream)).into_response()
    }
}

fn serialize_headers(headers: HeaderMap) -> Bytes {
    let mut b = BytesMut::with_capacity(30 + 30 * headers.len());
    for (name, value) in &headers {
        b.put(name.as_str().as_bytes());
        b.put(&b": "[..]);
        b.put(value.as_bytes());
        b.put(&b"\r\n"[..]);
    }
    b.put(&b"\r\n"[..]);
    b.freeze()
}

pub trait IntoPart {
    fn into_part(self) -> Part;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_multipart_serialization() {
        // Create a stream of parts
        let parts = futures::stream::iter(vec![
            Ok::<_, std::convert::Infallible>(Part::new(
                {
                    let mut headers = HeaderMap::new();
                    headers.insert("Content-Type", "text/plain".parse().unwrap());
                    headers.insert(
                        "Content-Disposition",
                        "form-data; name=\"field1\"".parse().unwrap(),
                    );
                    headers
                },
                Bytes::from("Hello World"),
            )),
            Ok(Part::new(
                {
                    let mut headers = HeaderMap::new();
                    headers.insert("Content-Type", "application/json".parse().unwrap());
                    headers
                },
                Bytes::from(r#"{"key":"value"}"#),
            )),
        ]);

        // Convert to bytes stream
        let boundary = "test-boundary".to_string();
        let stream = parts.into_bytes_stream(boundary.clone());
        futures::pin_mut!(stream);

        // Collect all bytes
        let mut result = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            result.put(chunk.unwrap());
        }

        let result_str = String::from_utf8(result.to_vec()).unwrap();

        // Verify the structure
        let expected = concat!(
            "--test-boundary\r\n",
            "content-type: text/plain\r\n",
            "content-disposition: form-data; name=\"field1\"\r\n",
            "\r\n",
            "Hello World\r\n",
            "--test-boundary\r\n",
            "content-type: application/json\r\n",
            "\r\n",
            r#"{"key":"value"}"#,
            "\r\n",
            "--test-boundary--\r\n",
        );

        assert_eq!(result_str, expected);
    }

    #[tokio::test]
    async fn test_multipart_with_empty_body() {
        let parts = futures::stream::iter(vec![Ok::<_, std::convert::Infallible>(
            Part::headers_only({
                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", "text/plain".parse().unwrap());
                headers
            }),
        )]);

        let boundary = "boundary123".to_string();
        let stream = parts.into_bytes_stream(boundary);
        futures::pin_mut!(stream);

        let mut result = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            result.put(chunk.unwrap());
        }

        let result_str = String::from_utf8(result.to_vec()).unwrap();

        let expected = concat!(
            "--boundary123\r\n",
            "content-type: text/plain\r\n",
            "\r\n",
            "\r\n",
            "--boundary123--\r\n",
        );

        assert_eq!(result_str, expected);
    }

    #[tokio::test]
    async fn test_multipart_error_propagation() {
        // Create a stream that will error
        let parts = futures::stream::iter(vec![
            Ok(Part::new(HeaderMap::new(), Bytes::from("first"))),
            Err("test error"),
            Ok(Part::new(HeaderMap::new(), Bytes::from("third"))),
        ]);

        let stream = parts.into_bytes_stream("boundary".to_string());
        futures::pin_mut!(stream);

        // Collect until we hit the error
        let mut chunks = Vec::new();
        let mut count = 0;
        while let Some(chunk) = stream.next().await {
            count += 1;
            match chunk {
                Ok(bytes) => {
                    println!("{:?}", bytes);
                    chunks.push(bytes)
                }
                Err(e) => {
                    assert_eq!(e, "test error");
                }
            }
        }

        assert_eq!(count, 4);
    }
}
