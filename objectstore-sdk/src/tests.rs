use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::pin::pin;
use std::sync::Mutex;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing};
use futures_core::Stream;
use reqwest::StatusCode;

use super::*;

#[tokio::test]
async fn stores_uncompressed() {
    let server = TestServer::new();
    let client = StorageService::new(&server.url("/"), "TEST", "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let stored_id = client
        .put("foo")
        .compression(Compression::Uncompressible)
        .buffer(body)
        .send()
        .await
        .unwrap();
    assert_eq!(stored_id, "foo");

    let (stream, compression) = client.get("foo", &[]).await.unwrap();
    let stream = stream.unwrap();

    let received = collect(stream).await.unwrap();

    assert_eq!(compression, None);
    assert_eq!(received, b"oh hai!");
}

#[tokio::test]
async fn uses_zstd_by_default() {
    let server = TestServer::new();
    let client = StorageService::new(&server.url("/"), "TEST", "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let stored_id = client.put("foo").buffer(body).send().await.unwrap();
    assert_eq!(stored_id, "foo");

    // when the user indicates that it can deal with zstd, it gets zstd
    let (stream, compression) = client.get("foo", &[Compression::Zstd]).await.unwrap();
    let stream = stream.unwrap();
    let received_compressed = collect(stream).await.unwrap();
    let decompressed = zstd::bulk::decompress(&received_compressed, usize::MAX).unwrap();

    assert_eq!(compression, Some(Compression::Zstd));
    assert_eq!(&decompressed, b"oh hai!");

    // otherwise, the client does the decompression
    let (stream, compression) = client.get("foo", &[]).await.unwrap();
    let stream = stream.unwrap();
    let received = collect(stream).await.unwrap();

    assert_eq!(compression, None);
    assert_eq!(received, b"oh hai!");
}

#[tokio::test]
async fn stores_compressed_zstd() {
    let server = TestServer::new();
    let client = StorageService::new(&server.url("/"), "TEST", "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let compressed = zstd::bulk::compress(body.as_bytes(), 0).unwrap();
    let stored_id = client
        .put("foo")
        .compression(Compression::Zstd)
        .buffer(compressed.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(stored_id, "foo");

    // when the user indicates that it can deal with zstd, it gets zstd
    let (stream, compression) = client.get("foo", &[Compression::Zstd]).await.unwrap();
    let stream = stream.unwrap();
    let received_compressed = collect(stream).await.unwrap();

    assert_eq!(compression, Some(Compression::Zstd));
    assert_eq!(received_compressed, compressed);

    // otherwise, the client does the decompression
    let (stream, compression) = client.get("foo", &[]).await.unwrap();
    let stream = stream.unwrap();
    let received = collect(stream).await.unwrap();

    assert_eq!(compression, None);
    assert_eq!(received, b"oh hai!");
}

async fn collect<E>(s: impl Stream<Item = Result<Bytes, E>>) -> anyhow::Result<Vec<u8>>
where
    anyhow::Error: From<E>,
{
    let mut output = vec![];
    let mut s = pin!(s);
    while let Some(chunk) = s.next().await {
        output.extend_from_slice(&chunk?);
    }
    Ok(output)
}

#[derive(Debug)]
pub struct TestServer {
    handle: tokio::task::JoinHandle<()>,
    socket: SocketAddr,
}

impl TestServer {
    /// Creates a new Server with a special testing-focused router,
    /// as described in the main [`Server`] docs.
    pub fn new() -> Self {
        type TestState = Arc<Mutex<HashMap<String, (Option<String>, Bytes)>>>;
        let state: TestState = Default::default();

        async fn put(
            State(state): State<TestState>,
            Path(key): Path<String>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            let content_encoding = headers
                .get(header::CONTENT_ENCODING)
                .and_then(|h| h.to_str().ok().map(ToString::to_string));

            let mut state = state.lock().unwrap();
            state.insert(key.clone(), (content_encoding, body));

            format!(r#"{{"key":{key:?}}}"#).into_response()
        }

        async fn get(State(state): State<TestState>, Path(key): Path<String>) -> Response {
            let state = state.lock().unwrap();
            let Some((content_encoding, body)) = state.get(&key) else {
                return StatusCode::NOT_FOUND.into_response();
            };

            let mut headers = HeaderMap::new();
            if let Some(content_encoding) = content_encoding {
                headers.append(header::CONTENT_ENCODING, content_encoding.parse().unwrap());
            }
            (headers, body.clone()).into_response()
        }

        async fn delete(State(state): State<TestState>, Path(key): Path<String>) {
            let mut state = state.lock().unwrap();
            state.remove(&key);
        }

        let router = Router::new()
            .route("/{*key}", routing::put(put).get(get).delete(delete))
            .with_state(state);
        Self::with_router(router)
    }

    /// Creates a new Server with the given [`Router`].
    pub fn with_router(router: Router) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            axum::serve(listener, router).await.unwrap();
        });

        Self { handle, socket }
    }

    /// Returns a full URL pointing to the given path.
    ///
    /// This URL uses `localhost` as hostname.
    pub fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("http://localhost:{}/{}", self.socket.port(), path)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Default for TestServer {
    fn default() -> Self {
        Self::new()
    }
}
