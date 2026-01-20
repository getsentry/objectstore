//! Types and utilities to support Multipart streaming responses.
//!
//! Compared to `axum_extra::response::MultipartForm`, this implementation supports attaching arbitrary headers to
//! each part, as well as the possibility to convert a `Stream` of those parts to a streaming `Response`.

use axum::body::Body;
use axum::response::IntoResponse as _;
use axum::response::Response;
use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};

/// A part in a Multipart response.
#[derive(Debug)]
pub struct Part {
    headers: HeaderMap,
    body: Bytes,
}

impl Part {
    /// Creates a new Multipart part with the given name, content type, body, optional filename,
    /// and headers.
    pub fn new(
        name: &str,
        content_type: &str,
        body: Bytes,
        filename: Option<&str>,
        mut headers: HeaderMap,
    ) -> Self {
        let mut disposition = format!("form-data; name=\"{}\"", name);
        if let Some(filename) = filename {
            disposition.push_str(&format!("; filename=\"{}\"", filename));
        }
        headers.insert(
            CONTENT_DISPOSITION,
            disposition.parse().expect("valid header value"),
        );
        headers.insert(
            CONTENT_TYPE,
            content_type.parse().expect("valid content type"),
        );

        Part { headers, body }
    }
}

pub trait IntoMultipartResponse {
    fn into_response(self, boundary: u128) -> Response;
}

impl<S, T> IntoMultipartResponse for S
where
    S: Stream<Item = T> + Send + 'static,
    T: Into<Part> + Send,
{
    fn into_response(self, boundary: u128) -> Response {
        let boundary_str = format!("os-boundary-{:032x}", boundary);
        let boundary = {
            let mut bytes = BytesMut::with_capacity(boundary_str.len() + 4);
            bytes.put(&b"--"[..]);
            bytes.put(boundary_str.as_bytes());
            bytes.put(&b"\r\n"[..]);
            bytes.freeze()
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary=\"{}\"", &boundary_str)
                .parse()
                .expect("valid header value, as it only contains hex digits"),
        );

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

                let mut closing = BytesMut::with_capacity(boundary.len());
                closing.put(boundary.slice(..boundary.len() - 2)); // don't take trailing \r\n
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
    use axum::body::{Body, to_bytes};
    use axum::extract::{FromRequest, Multipart};
    use axum::http::Request;

    /// Validates that our `Multipart` streaming response produces a valid `multipart/form-data` response
    /// that's parsed as expected by `axum::extract::Multipart`.
    #[tokio::test]
    async fn test_multipart_response() {
        let mut extra_headers = HeaderMap::new();
        extra_headers.insert("X-Custom-Header", "custom-value".parse().unwrap());
        extra_headers.insert("X-File-Id", "12345".parse().unwrap());
        let parts = vec![
            Part::new(
                "metadata",
                "application/json",
                Bytes::from(r#"{"key":"value"}"#),
                None,
                HeaderMap::new(),
            ),
            Part::new(
                "file",
                "application/octet-stream",
                Bytes::from(vec![0x00, 0x01, 0x02, 0xff, 0xfe]),
                Some("data.bin"),
                extra_headers,
            ),
        ];
        let boundary: u128 = 0xdeadbeef;
        let response = futures::stream::iter(parts).into_response(boundary);

        let boundary = format!("os-boundary-{:032x}", boundary);
        let content_type_str = format!("multipart/form-data; boundary=\"{}\"", boundary);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            &content_type_str
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let request = Request::builder()
            .header(CONTENT_TYPE, &content_type_str)
            .body(Body::from(body))
            .unwrap();
        let mut multipart = Multipart::from_request(request, &()).await.unwrap();

        let field = multipart.next_field().await.unwrap().unwrap();
        assert_eq!(field.name(), Some("metadata"));
        assert_eq!(field.file_name(), None);
        assert_eq!(field.content_type(), Some("application/json"));
        assert_eq!(field.headers().len(), 2);
        assert_eq!(field.bytes().await.unwrap(), r#"{"key":"value"}"#);

        let field = multipart.next_field().await.unwrap().unwrap();
        assert_eq!(field.name(), Some("file"));
        assert_eq!(field.file_name(), Some("data.bin"));
        assert_eq!(field.content_type(), Some("application/octet-stream"));
        assert_eq!(field.headers().len(), 4);
        assert_eq!(
            field.headers().get("X-Custom-Header").unwrap(),
            "custom-value"
        );
        assert_eq!(field.headers().get("X-File-Id").unwrap(), "12345");
        assert!(field.headers().get("content-disposition").is_some());
        assert!(field.headers().get("content-type").is_some());
        assert_eq!(
            field.bytes().await.unwrap(),
            vec![0x00, 0x01, 0x02, 0xff, 0xfe]
        );

        assert!(multipart.next_field().await.unwrap().is_none());
    }
}
