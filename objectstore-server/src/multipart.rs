//! Types and utilities to support Multipart streaming responses.
//!
//! Compared to `axum_extra`'s `MultipartForm`, this implementation supports attaching arbitrary headers to
//! each part, as well as the possibility to convert a `Stream` of those parts to a streaming `Response`.

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
    /// Creates a new Multipart part with the specified name, content type, body, and optional filename.
    ///
    /// Additional headers can be provided, but if they contain `Content-Type`, it will be ignored
    /// in favor of the provided `content_type` parameter.
    pub fn new(
        name: impl AsRef<str>,
        content_type: impl AsRef<str>,
        body: Bytes,
        filename: Option<impl AsRef<str>>,
        additional_headers: HeaderMap,
    ) -> Self {
        use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};

        let mut headers = HeaderMap::new();

        // Build Content-Disposition header
        let mut disposition = format!("form-data; name=\"{}\"", name.as_ref());
        if let Some(fname) = filename {
            disposition.push_str(&format!("; filename=\"{}\"", fname.as_ref()));
        }
        headers.insert(
            CONTENT_DISPOSITION,
            disposition.parse().expect("valid header value"),
        );

        // Set Content-Type
        headers.insert(
            CONTENT_TYPE,
            content_type.as_ref().parse().expect("valid content type"),
        );

        // Merge additional headers, skipping Content-Type
        for (name, value) in additional_headers {
            if let Some(name) = name
                && name != CONTENT_TYPE
            {
                headers.insert(name, value);
            }
        }

        Part { headers, body }
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
            format!("multipart/form-data; boundary={}", &boundary)
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

                // Yield closing boundary: --boundary-- (without trailing \r\n)
                // The boundary already has --boundary\r\n, so we need to strip the \r\n
                // and add -- instead
                let boundary_str = std::str::from_utf8(&boundary).unwrap();
                let boundary_without_crlf = boundary_str.trim_end_matches("\r\n");
                let mut closing = BytesMut::with_capacity(boundary_without_crlf.len() + 2);
                closing.put(boundary_without_crlf.as_bytes());
                closing.put(&b"--"[..]);
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
    use axum_extra::response::multiple::{MultipartForm, Part as AxumPart};

    /// Validates that our `Multipart` streaming response matches what `axum` would construct when using its
    /// built-in `MultipartForm`.
    /// The test is performed by parsing both requests with `axum`'s `Multipart` extractor, to
    /// circumvent meaningless differences such as header casing.
    #[tokio::test]
    async fn test_multipart_response() {
        use axum::body::{Body, to_bytes};
        use axum::extract::{FromRequest, Multipart};
        use axum::http::Request;

        let parts = vec![
            AxumPart::raw_part(
                "metadata",
                "application/json",
                r#"{"key":"value"}"#.as_bytes().to_vec(),
                None,
            )
            .unwrap(),
            AxumPart::raw_part(
                "file",
                "application/octet-stream",
                vec![0x00, 0x01, 0x02, 0xff, 0xfe],
                Some("data.bin"),
            )
            .unwrap(),
        ];
        let expected_response = MultipartForm::with_parts(parts).into_response();

        let content_type = expected_response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let boundary = content_type.split("boundary=").nth(1).unwrap();

        let parts = vec![
            Part::new(
                "metadata",
                "application/json",
                Bytes::from(r#"{"key":"value"}"#),
                None::<String>,
                HeaderMap::new(),
            ),
            Part::new(
                "file",
                "application/octet-stream",
                Bytes::from(vec![0x00, 0x01, 0x02, 0xff, 0xfe]),
                Some("data.bin"),
                HeaderMap::new(),
            ),
        ];
        let response = futures::stream::iter(parts).into_response(boundary.to_string());

        assert_eq!(response.headers(), expected_response.headers());

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let expected_body = to_bytes(expected_response.into_body(), usize::MAX)
            .await
            .unwrap();

        let request = Request::builder()
            .header(CONTENT_TYPE, &content_type)
            .body(Body::from(body))
            .unwrap();
        let expected_request = Request::builder()
            .header(CONTENT_TYPE, &content_type)
            .body(Body::from(expected_body))
            .unwrap();

        let mut multipart = Multipart::from_request(request, &()).await.unwrap();
        let mut expected_multipart = Multipart::from_request(expected_request, &())
            .await
            .unwrap();

        loop {
            let field = multipart.next_field().await.unwrap();
            let expected_field = expected_multipart.next_field().await.unwrap();

            match (field, expected_field) {
                (None, None) => break,
                (Some(our), Some(expected)) => {
                    assert_eq!(our.name(), expected.name());
                    assert_eq!(our.file_name(), expected.file_name());
                    assert_eq!(our.content_type(), expected.content_type());
                    let our_bytes = our.bytes().await.unwrap();
                    let expected_bytes = expected.bytes().await.unwrap();
                    assert_eq!(our_bytes, expected_bytes);
                }
                _ => panic!("expected both values to have the same number of parts"),
            }
        }
    }
}
