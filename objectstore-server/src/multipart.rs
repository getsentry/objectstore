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
use http::HeaderValue;
use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};

/// Name used by [`Part::new`], the part layout of the current batch response format.
const LEGACY_PART_NAME: &str = "part";

/// Error returned when a part name or filename cannot be serialized into a `Content-Disposition`.
///
/// Names and filenames are emitted verbatim inside double quotes, so they must be ASCII and must
/// not contain `"`, `\`, CR, LF or other control characters.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid multipart {field} {value:?}: must be non-empty printable ASCII without quotes or backslashes"
)]
pub struct InvalidPartName {
    field: &'static str,
    value: String,
}

/// A part in a Multipart response.
#[derive(Debug)]
pub struct Part {
    headers: HeaderMap,
    body: Bytes,
}

impl Part {
    /// Creates a part named `part` with the given body, extra headers and content type.
    ///
    /// This is the layout of the current batch response format. It serializes as
    /// `Content-Disposition: form-data; name="part"`. See [`Part::named`] for custom names.
    pub fn new(body: Bytes, headers: HeaderMap, content_type: Option<HeaderValue>) -> Self {
        // Insert after extending so callers cannot override the disposition or content type.
        let mut part = Self { headers, body };
        part.headers.insert(
            CONTENT_DISPOSITION,
            content_disposition(LEGACY_PART_NAME, None),
        );
        if let Some(content_type) = content_type {
            part.headers.insert(CONTENT_TYPE, content_type);
        }
        part
    }

    /// Creates a part carrying only `Content-Disposition` and an optional `Content-Type`.
    ///
    /// The disposition serializes exactly as `form-data; name="<name>"`, or
    /// `form-data; name="<name>"; filename="<filename>"` when a filename is given.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPartName`] if `name` or `filename` contains `"`, `\`, CR, LF, other
    /// control characters, or non-ASCII bytes.
    pub fn named(
        name: &str,
        filename: Option<&str>,
        body: Bytes,
        content_type: Option<HeaderValue>,
    ) -> Result<Self, InvalidPartName> {
        validate("name", name)?;
        if let Some(filename) = filename {
            validate("filename", filename)?;
        }
        let disposition = content_disposition(name, filename);
        Ok(Self::from_disposition(disposition, body, content_type))
    }

    fn from_disposition(
        disposition: HeaderValue,
        body: Bytes,
        content_type: Option<HeaderValue>,
    ) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_DISPOSITION, disposition);
        if let Some(content_type) = content_type {
            headers.insert(CONTENT_TYPE, content_type);
        }
        Self { headers, body }
    }
}

fn validate(field: &'static str, value: &str) -> Result<(), InvalidPartName> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|b| (b.is_ascii_graphic() || b == b' ') && b != b'"' && b != b'\\')
    {
        Ok(())
    } else {
        Err(InvalidPartName {
            field,
            value: value.to_owned(),
        })
    }
}

/// Builds `form-data; name="<name>"[; filename="<filename>"]`. Inputs must be validated.
fn content_disposition(name: &str, filename: Option<&str>) -> HeaderValue {
    let value = match filename {
        Some(filename) => format!("form-data; name=\"{name}\"; filename=\"{filename}\""),
        None => format!("form-data; name=\"{name}\""),
    };
    // INVARIANT: callers pass validated printable ASCII (or the static legacy name), which is
    // always a valid header value.
    HeaderValue::from_str(&value).expect("validated ASCII is a valid header value")
}

/// Converts a stream of [`Part`]s into a streaming `multipart/form-data` HTTP response.
///
/// Implemented for any `Stream<Item = T>` where `T: Into<Part>`. The boundary is
/// formatted as `os-boundary-<hex>` using the provided `u128` value.
pub trait IntoMultipartResponse {
    /// Consumes `self` and returns a streaming `multipart/form-data` response.
    ///
    /// The `boundary` parameter is formatted as `os-boundary-<hex>` and embedded in the
    /// `Content-Type` header. Each item yielded by the stream becomes one part, separated
    /// by that boundary. A closing boundary is appended after the last item.
    fn into_multipart_response(self, boundary: u128) -> Response;
}

impl<S, T> IntoMultipartResponse for S
where
    S: Stream<Item = T> + Send + 'static,
    T: Into<Part> + Send,
{
    fn into_multipart_response(self, boundary: u128) -> Response {
        let boundary_str = format!("os-boundary-{boundary:032x}");
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
            format!("multipart/form-data; boundary=\"{}\"", boundary_str)
                .parse()
                .expect("valid header value, as we just defined it as \"os-boundary-X\" where X are hex digits"),
        );

        let body: BoxStream<Result<Bytes, std::convert::Infallible>> = async_stream::try_stream! {
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
    // https://github.com/hyperium/hyper/blob/0f0b6ed3ac55ac1682afd2104cb8d0385149249a/src/proto/h1/role.rs#L399
    let mut res = BytesMut::with_capacity(30 + 30 * headers.len());
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

    async fn parse_fields(parts: Vec<Part>) -> Vec<(HeaderMap, Bytes)> {
        let boundary: u128 = 0xfeed;
        let response = futures::stream::iter(parts).into_multipart_response(boundary);
        let content_type = response.headers()[CONTENT_TYPE].clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let request = Request::builder()
            .header(CONTENT_TYPE, content_type)
            .body(Body::from(body))
            .unwrap();
        let mut multipart = Multipart::from_request(request, &()).await.unwrap();
        let mut fields = Vec::new();
        while let Some(field) = multipart.next_field().await.unwrap() {
            let headers = field.headers().clone();
            fields.push((headers, field.bytes().await.unwrap()));
        }
        fields
    }

    #[tokio::test]
    async fn test_named_parts() {
        let parts = vec![
            Part::named("0", None, Bytes::from("{}"), None).unwrap(),
            Part::named(
                "1",
                None,
                Bytes::from("{}"),
                Some(HeaderValue::from_static("application/json")),
            )
            .unwrap(),
            Part::named(
                "1.payload",
                Some("blob.bin"),
                Bytes::from_static(&[0, 1]),
                None,
            )
            .unwrap(),
            Part::named(
                "2.payload",
                Some("a b.txt"),
                Bytes::from("hi"),
                Some(HeaderValue::from_static("text/plain")),
            )
            .unwrap(),
        ];
        let fields = parse_fields(parts).await;

        let disposition = |i: usize| {
            fields[i].0[CONTENT_DISPOSITION]
                .to_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(disposition(0), r#"form-data; name="0""#);
        assert_eq!(disposition(1), r#"form-data; name="1""#);
        assert_eq!(
            disposition(2),
            r#"form-data; name="1.payload"; filename="blob.bin""#
        );
        assert_eq!(
            disposition(3),
            r#"form-data; name="2.payload"; filename="a b.txt""#
        );

        // Only Content-Disposition, plus Content-Type when given.
        assert_eq!(fields[0].0.len(), 1);
        assert_eq!(fields[1].0.len(), 2);
        assert_eq!(fields[1].0[CONTENT_TYPE], "application/json");
        assert_eq!(fields[2].0.len(), 1);
        assert_eq!(fields[3].0[CONTENT_TYPE], "text/plain");
        assert_eq!(fields[2].1, vec![0, 1]);
    }

    #[test]
    fn test_legacy_part_name_is_quoted() {
        let part = Part::new(Bytes::new(), HeaderMap::new(), None);
        assert_eq!(
            part.headers[CONTENT_DISPOSITION],
            r#"form-data; name="part""#
        );
    }

    #[test]
    fn test_named_rejects_unsafe_values() {
        for bad in ["", "a\"b", "a\\b", "a\r\nb", "a\nb", "ä", "a\0b", "a\tb"] {
            assert!(
                Part::named(bad, None, Bytes::new(), None).is_err(),
                "{bad:?}"
            );
            assert!(
                Part::named("ok", Some(bad), Bytes::new(), None).is_err(),
                "{bad:?}"
            );
        }
    }

    /// Validates that our `Multipart` streaming response produces a valid `multipart/form-data` response
    /// that's parsed as expected by `axum::extract::Multipart`.
    #[tokio::test]
    async fn test_multipart_response() {
        let mut extra_headers = HeaderMap::new();
        extra_headers.insert("X-Custom-Header", "custom-value".parse().unwrap());
        extra_headers.insert("X-File-Id", "12345".parse().unwrap());
        let parts = vec![
            Part::new(
                Bytes::from(r#"{"key":"value"}"#),
                HeaderMap::new(),
                Some(HeaderValue::from_static("application/json")),
            ),
            Part::new(
                Bytes::from(vec![0x00, 0x01, 0x02, 0xff, 0xfe]),
                extra_headers,
                Some(HeaderValue::from_static("application/octet-stream")),
            ),
        ];
        let boundary: u128 = 0xdeadbeef;
        let response = futures::stream::iter(parts).into_multipart_response(boundary);

        let boundary = format!("os-boundary-{boundary:032x}");
        let content_type_str = format!("multipart/form-data; boundary=\"{boundary}\"");
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
        assert_eq!(field.name(), Some("part"));
        assert_eq!(field.file_name(), None);
        assert_eq!(field.content_type(), Some("application/json"));
        assert_eq!(field.headers().len(), 2);
        assert_eq!(field.bytes().await.unwrap(), r#"{"key":"value"}"#);

        let field = multipart.next_field().await.unwrap().unwrap();
        assert_eq!(field.name(), Some("part"));
        assert_eq!(field.file_name(), None);
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
