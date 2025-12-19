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
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(p))) => {
                    *this.state = State::SendHeaders(p);
                    return Poll::Ready(Some(Ok(this.boundary.clone())));
                }
            },
            State::SendHeaders(part) => {
                *this.state = State::SendBody(part.body);
                let headers = serialize_headers(part.headers);
                return Poll::Ready(Some(Ok(headers)));
            }
            State::SendBody(body) => {
                return Poll::Ready(Some(Ok(body)));
            }
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
