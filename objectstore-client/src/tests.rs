use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing};
use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use reqwest::{StatusCode, header};
use uuid::Uuid;

use crate::get::GetResult;

use super::*;

#[tokio::test]
async fn stores_uncompressed() {
    let server = TestServer::new();
    let client = ClientBuilder::new(&server.url("/"), "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let stored_id = client.put(body).compression(None).send().await.unwrap().key;

    let GetResult { metadata, stream } = client.get(&stored_id).send().await.unwrap().unwrap();
    let received: BytesMut = stream.try_collect().await.unwrap();

    assert_eq!(metadata.compression, None);
    assert_eq!(received.as_ref(), b"oh hai!");
}

#[tokio::test]
async fn uses_zstd_by_default() {
    let server = TestServer::new();
    let client = ClientBuilder::new(&server.url("/"), "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let stored_id = client.put(body).send().await.unwrap().key;

    // when the user indicates that it can deal with zstd, it gets zstd
    let GetResult { metadata, stream } = client
        .get(&stored_id)
        .decompress(false)
        .send()
        .await
        .unwrap()
        .unwrap();
    let received_compressed: BytesMut = stream.try_collect().await.unwrap();
    let decompressed = zstd::bulk::decompress(&received_compressed, 1024).unwrap();

    assert_eq!(metadata.compression, Some(Compression::Zstd));
    assert_eq!(&decompressed, b"oh hai!");

    // otherwise, the client does the decompression
    let GetResult { metadata, stream } = client.get(&stored_id).send().await.unwrap().unwrap();
    let received: BytesMut = stream.try_collect().await.unwrap();

    assert_eq!(metadata.compression, None);
    assert_eq!(received.as_ref(), b"oh hai!");
}

#[tokio::test]
async fn deletes_stores_stuff() {
    let server = TestServer::new();
    let client = ClientBuilder::new(&server.url("/"), "test")
        .unwrap()
        .for_organization(12345);

    let body = "oh hai!";
    let stored_id = client.put(body).send().await.unwrap().key;

    client.delete(&stored_id).await.unwrap();

    let response = client.get(&stored_id).send().await.unwrap();
    assert!(response.is_none());
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

        async fn put(State(state): State<TestState>, headers: HeaderMap, body: Bytes) -> Response {
            let content_encoding = headers
                .get(header::CONTENT_ENCODING)
                .and_then(|h| h.to_str().ok().map(ToString::to_string));
            let key = Uuid::now_v7().to_string();

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
            .route("/", routing::put(put))
            .route("/{*key}", routing::get(get).delete(delete))
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
