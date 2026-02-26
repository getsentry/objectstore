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

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::Request;
use objectstore_server::config::{AuthZVerificationKey, Config, Storage};
use objectstore_server::state::Services;
use objectstore_server::web::App;
use objectstore_types::auth::Permission;
use std::sync::Arc;
use tempfile::TempDir;

// Re-export `config` module so that e2e/integration tests can fully customize the server.
pub use objectstore_server::config;

/// Key ID (`kid`) for the EdDSA key pair used by the test server.
pub const TEST_EDDSA_KID: &str = "test_kid";

/// Filesystem path to the test Ed25519 private key PEM file.
pub static TEST_EDDSA_PRIVKEY_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    [env!("CARGO_MANIFEST_DIR"), "config", "ed25519.private.pem"]
        .iter()
        .collect::<PathBuf>()
});

/// PEM-encoded Ed25519 private key used to sign JWTs in tests.
pub static TEST_EDDSA_PRIVKEY: LazyLock<String> =
    LazyLock::new(|| std::fs::read_to_string(&*TEST_EDDSA_PRIVKEY_PATH).unwrap());

/// Filesystem path to the test Ed25519 public key PEM file.
pub static TEST_EDDSA_PUBKEY_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    [env!("CARGO_MANIFEST_DIR"), "config", "ed25519.public.pem"]
        .iter()
        .collect::<PathBuf>()
});

/// PEM-encoded Ed25519 public key registered with the test server for JWT verification.
pub static TEST_EDDSA_PUBKEY: LazyLock<String> =
    LazyLock::new(|| std::fs::read_to_string(&*TEST_EDDSA_PUBKEY_PATH).unwrap());

/// An in-process test server for use in integration tests.
///
/// This server runs the full objectstore service using a temporary directory for storage, which is
/// deleted when the server is dropped. It listens on a random available port on localhost.
#[derive(Debug)]
pub struct TestServer {
    handle: tokio::task::JoinHandle<()>,
    socket: SocketAddr,
    request_count: Arc<AtomicUsize>,
    _long_term_tempdir: TempDir,
    _high_volume_tempdir: TempDir,
}

impl TestServer {
    /// Spawns a new test server with the given configuration.
    ///
    /// Unless overridden to a different kind of backend, the long-term and high-volume storage
    /// backends will use temporary directories.
    pub async fn with_config(mut config: Config) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        config.logging.level = "trace".parse().unwrap();
        crate::tracing::init();

        let long_term_tempdir = tempfile::tempdir().unwrap();
        if let Storage::FileSystem { ref mut path } = config.long_term_storage {
            *path = long_term_tempdir.path().into();
        }
        let high_volume_tempdir = tempfile::tempdir().unwrap();
        if let Storage::FileSystem { ref mut path } = config.high_volume_storage {
            *path = high_volume_tempdir.path().into();
        }

        config.auth.keys = BTreeMap::from([(
            TEST_EDDSA_KID.into(),
            AuthZVerificationKey {
                max_permissions: Permission::rwd(),
                key_files: vec![TEST_EDDSA_PUBKEY_PATH.clone()],
            },
        )]);

        let state = Services::spawn(config).await.unwrap();
        let app = App::new(state);

        let request_count = Arc::new(AtomicUsize::new(0));
        let counter = request_count.clone();
        let router = app.into_router().layer(axum::middleware::from_fn(
            move |req: Request, next: axum::middleware::Next| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                    next.run(req).await
                }
            },
        ));

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let service = router.into_make_service_with_connect_info::<SocketAddr>();
            axum::serve(listener, service).await.unwrap();
        });

        Self {
            handle,
            socket,
            request_count,
            _long_term_tempdir: long_term_tempdir,
            _high_volume_tempdir: high_volume_tempdir,
        }
    }

    /// Spawns a new test server with default configuration.
    pub async fn new() -> Self {
        Self::with_config(Config::default()).await
    }

    /// Returns the total number of HTTP requests received by the server.
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
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
