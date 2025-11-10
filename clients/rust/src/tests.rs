use std::net::{SocketAddr, TcpListener};

use bytes::BytesMut;
use futures_util::TryStreamExt;
use objectstore_server::config::{Config, Storage};
use objectstore_server::http::App;
use objectstore_server::state::State;
use tempfile::TempDir;

use crate::get::GetResponse;

use super::*;

#[tokio::test]
async fn stores_uncompressed() {
    let server = TestServer::new().await;

    let client = ClientBuilder::new(server.url("/")).build().unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let body = "oh hai!";

    let stored_id = session
        .put(body)
        .compression(None)
        .send()
        .await
        .unwrap()
        .key;

    let GetResponse { metadata, stream } = session.get(&stored_id).send().await.unwrap().unwrap();
    let received: BytesMut = stream.try_collect().await.unwrap();

    assert_eq!(metadata.compression, None);
    assert_eq!(received.as_ref(), b"oh hai!");
}

#[tokio::test]
async fn uses_zstd_by_default() {
    let server = TestServer::new().await;

    let client = ClientBuilder::new(server.url("/")).build().unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let body = "oh hai!";
    let stored_id = session.put(body).send().await.unwrap().key;

    // when the user indicates that it can deal with zstd, it gets zstd
    let GetResponse { metadata, stream } = session
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
    let GetResponse { metadata, stream } = session.get(&stored_id).send().await.unwrap().unwrap();
    let received: BytesMut = stream.try_collect().await.unwrap();

    assert_eq!(metadata.compression, None);
    assert_eq!(received.as_ref(), b"oh hai!");
}

#[tokio::test]
async fn deletes_stores_stuff() {
    let server = TestServer::new().await;

    let client = ClientBuilder::new(server.url("/")).build().unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let body = "oh hai!";
    let stored_id = session.put(body).send().await.unwrap().key;

    session.delete(&stored_id).send().await.unwrap();

    let response = session.get(&stored_id).send().await.unwrap();
    assert!(response.is_none());
}

#[derive(Debug)]
pub struct TestServer {
    handle: tokio::task::JoinHandle<()>,
    socket: SocketAddr,
    _tempdir: TempDir,
}

impl TestServer {
    pub async fn new() -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            long_term_storage: Storage::FileSystem {
                path: tempdir.path().into(),
            },
            high_volume_storage: Storage::FileSystem {
                path: tempdir.path().into(),
            },
            ..Default::default()
        };

        let state = State::new(config).await.unwrap();
        let app = App::new(state);

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            app.serve(listener).await.unwrap();
        });

        Self {
            handle,
            socket,
            _tempdir: tempdir,
        }
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
