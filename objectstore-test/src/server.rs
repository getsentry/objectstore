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

use objectstore_server::config::{
    AuthZVerificationKey, Config, MultipartUploadStorageConfig, StorageConfig,
};
use objectstore_server::state::Services;
use objectstore_server::web::App;
use objectstore_types::auth::Permission;
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
    _tempdirs: Vec<TempDir>,
}

impl TestServer {
    /// Spawns a new test server with the given configuration.
    ///
    /// Unless overridden to a different kind of backend, any filesystem storage backends will use
    /// temporary directories.
    pub async fn with_config(mut config: Config) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        config.logging.level = "trace".parse().unwrap();
        crate::tracing::init();

        let mut tempdirs = Vec::new();
        replace_fs_paths(&mut config.storage, &mut tempdirs);

        config.auth.keys = BTreeMap::from([(
            TEST_EDDSA_KID.into(),
            AuthZVerificationKey {
                max_permissions: Permission::rwd(),
                key_files: vec![TEST_EDDSA_PUBKEY_PATH.clone()],
            },
        )]);

        let state = Services::spawn(config).await.unwrap();
        let app = App::new(state);

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            app.serve(listener).await.unwrap();
        });

        Self {
            handle,
            socket,
            _tempdirs: tempdirs,
        }
    }

    /// Spawns a new test server with default configuration.
    pub async fn new() -> Self {
        Self::with_config(Config::default()).await
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

/// Recursively replaces filesystem backend paths with temporary directories.
///
/// For `FileSystem` variants, a new temp dir is created and its path is substituted.
/// For `Tiered` variants, both sub-backends are processed recursively.
/// Other backend types are left unchanged.
fn replace_fs_paths(config: &mut StorageConfig, tempdirs: &mut Vec<TempDir>) {
    match config {
        StorageConfig::FileSystem(c) => {
            let dir = tempfile::tempdir().unwrap();
            c.path = dir.path().into();
            tempdirs.push(dir);
        }
        StorageConfig::Tiered(c) => {
            if let MultipartUploadStorageConfig::FileSystem(fs) = &mut c.long_term {
                let dir = tempfile::tempdir().unwrap();
                fs.path = dir.path().into();
                tempdirs.push(dir);
            }
        }
        StorageConfig::S3Compatible(_) | StorageConfig::Gcs(_) | StorageConfig::BigTable(_) => {}
    }
}
