//! Utilities for multipart streaming responses.
//! This has been adapted from https://github.com/scottlamb/multipart-stream-rs.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use http::HeaderMap;
use pin_project::pin_project;

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
}

pub trait IntoBytesStream<E> {
    fn into_bytes_stream(self, boundary: String) -> impl Stream<Item = Result<Bytes, E>>;
}

impl<S, E> IntoBytesStream<E> for S
where
    S: Stream<Item = Result<Part, E>> + Send,
{
    fn into_bytes_stream(self, boundary: String) -> impl Stream<Item = Result<Bytes, E>> {
        let mut b = BytesMut::with_capacity(boundary.len() + 4);
        b.put(&b"--"[..]);
        b.put(boundary.as_bytes());
        b.put(&b"\r\n"[..]);
        PartsSerializer {
            parts: self,
            boundary: b.freeze(),
            state: State::Waiting,
        }
    }
}

#[pin_project]
struct PartsSerializer<S, E>
where
    S: Stream<Item = Result<Part, E>>,
{
    #[pin]
    parts: S,
    boundary: Bytes,
    state: State,
}

enum State {
    Waiting,
    SendHeaders(Part),
    SendBody(Bytes),
    SendClosingBoundary,
    Done,
}

impl<S, E> Stream for PartsSerializer<S, E>
where
    S: Stream<Item = Result<Part, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match std::mem::replace(this.state, State::Waiting) {
            State::Waiting => match this.parts.as_mut().poll_next(ctx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    *this.state = State::SendClosingBoundary;
                    ctx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(p))) => {
                    *this.state = State::SendHeaders(p);
                    Poll::Ready(Some(Ok(this.boundary.clone())))
                }
            },
            State::SendHeaders(part) => {
                *this.state = State::SendBody(part.body);
                let headers = serialize_headers(part.headers);
                Poll::Ready(Some(Ok(headers)))
            }
            State::SendBody(body) => {
                // Add \r\n after the body
                let mut body_with_newline = BytesMut::with_capacity(body.len() + 2);
                body_with_newline.put(body);
                body_with_newline.put(&b"\r\n"[..]);
                Poll::Ready(Some(Ok(body_with_newline.freeze())))
            }
            State::SendClosingBoundary => {
                *this.state = State::Done;
                // Create closing boundary: --boundary-- (without \r\n in between)
                // The boundary already has --boundary\r\n, so we need to strip the \r\n
                // and add --\r\n instead
                let boundary_str = std::str::from_utf8(this.boundary).unwrap();
                let boundary_without_crlf = boundary_str.trim_end_matches("\r\n");
                let mut closing = BytesMut::with_capacity(boundary_without_crlf.len() + 4);
                closing.put(boundary_without_crlf.as_bytes());
                closing.put(&b"--\r\n"[..]);
                Poll::Ready(Some(Ok(closing.freeze())))
            }
            State::Done => Poll::Ready(None),
        }
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
