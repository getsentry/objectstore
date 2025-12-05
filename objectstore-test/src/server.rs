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

use std::io::Write;
use std::net::{SocketAddr, TcpListener};

use objectstore_server::config::Config;
use objectstore_server::http::App;
use objectstore_server::state::Services;
use tempfile::TempDir;

use crate::token::*;

/// An in-process test server for use in integration tests.
///
/// This server runs the full objectstore service using a temporary directory for storage, which is
/// deleted when the server is dropped. It listens on a random available port on localhost.
#[derive(Debug)]
pub struct TestServer {
    handle: tokio::task::JoinHandle<()>,
    socket: SocketAddr,
    _long_term_tempdir: TempDir,
    _high_volume_tempdir: TempDir,
}

impl TestServer {
    pub async fn new() -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        let long_term_tempdir = tempfile::tempdir().unwrap();
        let high_volume_tempdir = tempfile::tempdir().unwrap();

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        let long_term_dir_str = long_term_tempdir.path().display();
        let high_volume_dir_str = high_volume_tempdir.path().display();
        config_file
            .write_all(
                format!(
                    r#"long_term_storage:
    type: filesystem
    path: {}

high_volume_storage:
    type: filesystem
    path: {}

auth:
    enforce: true
    keys:
        {}:
            key_versions:
                - '{}'
            max_permissions:
                - "object.read"
                - "object.write"
                - "object.delete""#,
                    long_term_dir_str,
                    high_volume_dir_str,
                    TEST_EDDSA_KID,
                    TEST_EDDSA_PUBKEY.as_str(),
                )
                .as_bytes(),
            )
            .unwrap();
        let config = Config::load(Some(config_file.path())).unwrap();

        let state = Services::spawn(config).await.unwrap();
        let app = App::new(state);

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            app.serve(listener).await.unwrap();
        });

        Self {
            handle,
            socket,
            _long_term_tempdir: long_term_tempdir,
            _high_volume_tempdir: high_volume_tempdir,
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
