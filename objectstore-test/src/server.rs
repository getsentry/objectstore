//! Exposes an in-process test server for use in integration tests.
//!
//! ```
//! use objectstore_test::server::TestServer;
//!
//! #[tokio::main]
//! async fn main() {
//!    let server = TestServer::new().await;
//!    let url = server.url("/health");
//!    // use the URL in tests...
//! }
//! ```

use std::net::{SocketAddr, TcpListener};

use objectstore_server::config::{Config, Storage};
use objectstore_server::http::App;
use objectstore_server::state::State;
use tempfile::TempDir;

/// An in-process test server for use in integration tests.
///
/// This server runs the full objectstore service using a temporary directory for storage, which is
/// deleted when the server is dropped. It listens on a random available port on localhost.
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
