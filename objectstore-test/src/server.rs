//! Exposes an in-process test server for use in integration tests.
//!
//! The server always stores objects in [`TieredStorage`]. [`TestServer::tiered`] returns a
//! [`TestServerBuilder`] to replace either tier or to customize the rest of the configuration.
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
use std::net::{IpAddr, SocketAddr, TcpListener};

use objectstore_server::config::{AuthZVerificationKey, Config};
use objectstore_server::state::Services;
use objectstore_server::web::App;
use objectstore_service::backend::changelog::NoopChangeLog;
use objectstore_service::backend::common::{Backend, HighVolumeBackend};
use objectstore_service::backend::in_memory::InMemoryBackend;
use objectstore_service::backend::local_fs::{FileSystemConfig, LocalFsBackend};
use objectstore_service::backend::tiered::TieredStorage;
use objectstore_service::change_stream::ChangeStreamFactory;
use objectstore_types::auth::Permission;
use tempfile::TempDir;

// Re-export `config` module so that e2e/integration tests can fully customize the server.
pub use objectstore_server::config;

/// Key ID (`kid`) for the EdDSA key pair used by the test server.
pub const TEST_EDDSA_KID: &str = "test_kid";

/// Filesystem path to the test Ed25519 private key PEM file.
pub const TEST_EDDSA_PRIVKEY_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/config/ed25519.private.pem");

/// PEM-encoded Ed25519 private key used to sign JWTs in tests.
pub const TEST_EDDSA_PRIVKEY: &str = include_str!("../config/ed25519.private.pem");

/// Filesystem path to the test Ed25519 public key PEM file.
pub const TEST_EDDSA_PUBKEY_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/config/ed25519.public.pem");

/// PEM-encoded Ed25519 public key registered with the test server for JWT verification.
pub const TEST_EDDSA_PUBKEY: &str = include_str!("../config/ed25519.public.pem");

/// An in-process test server for use in integration tests.
///
/// This server runs the full objectstore service on [`TieredStorage`] and listens on a random
/// available port on localhost. See [`TestServerBuilder`] for the default backends.
#[derive(Debug)]
pub struct TestServer {
    handle: tokio::task::JoinHandle<()>,
    socket: SocketAddr,
    _tempdir: Option<TempDir>,
}

impl TestServer {
    /// Returns a builder for a test server on [`TieredStorage`].
    pub fn tiered() -> TestServerBuilder {
        TestServerBuilder::default()
    }

    /// Spawns a new test server with the default backends and configuration.
    ///
    /// Equivalent to `TestServer::tiered().build().await`.
    pub async fn new() -> Self {
        Self::tiered().build().await
    }

    /// Returns the IP address on which the server is listening.
    pub fn ip(&self) -> IpAddr {
        self.socket.ip()
    }

    /// Returns the port on which the server is listening.
    pub fn port(&self) -> u16 {
        self.socket.port()
    }

    /// Returns a full URL pointing to the given path.
    ///
    /// This URL uses `localhost` as hostname.
    pub fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("http://localhost:{}/{}", self.port(), path)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Builder for a [`TestServer`] on [`TieredStorage`], returned by [`TestServer::tiered`].
///
/// By default, the high-volume tier is an [`InMemoryBackend`] and the long-term tier is a
/// [`LocalFsBackend`] in a temporary directory, which is deleted when the server is dropped.
///
/// ```
/// use objectstore_test::server::{TestServer, config};
///
/// #[tokio::main]
/// async fn main() {
///     let server = TestServer::tiered()
///         .config(config::Config {
///             auth: config::AuthZ {
///                 enforce: false,
///                 ..Default::default()
///             },
///             ..Default::default()
///         })
///         .build()
///         .await;
///     let url = server.url("/health");
/// }
/// ```
#[derive(Debug, Default)]
pub struct TestServerBuilder {
    high_volume: Option<Box<dyn HighVolumeBackend>>,
    long_term: Option<Box<dyn Backend>>,
    config: Config,
}

impl TestServerBuilder {
    /// Replaces the default [`InMemoryBackend`] in the high-volume tier.
    pub fn high_volume(mut self, backend: Box<dyn HighVolumeBackend>) -> Self {
        self.high_volume = Some(backend);
        self
    }

    /// Replaces the default [`LocalFsBackend`] in the long-term tier.
    pub fn long_term(mut self, backend: Box<dyn Backend>) -> Self {
        self.long_term = Some(backend);
        self
    }

    /// Sets the server configuration, except for storage.
    ///
    /// The `storage` section of `config` is ignored, since the backends are set on this builder.
    /// The log level and auth keys are always overridden for tests.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Spawns the test server.
    pub async fn build(self) -> TestServer {
        let Self {
            high_volume,
            long_term,
            mut config,
        } = self;

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let socket = listener.local_addr().unwrap();

        config.logging.level = "trace".parse().unwrap();
        crate::tracing::init();

        config.auth.keys = BTreeMap::from([(
            TEST_EDDSA_KID.into(),
            AuthZVerificationKey {
                max_permissions: Permission::rwd(),
                key_files: vec![TEST_EDDSA_PUBKEY_PATH.into()],
            },
        )]);

        let high_volume =
            high_volume.unwrap_or_else(|| Box::new(InMemoryBackend::new("in-memory")));
        let mut tempdir = None;
        let long_term = long_term.unwrap_or_else(|| {
            let dir = tempfile::tempdir().unwrap();
            let config = FileSystemConfig {
                path: dir.path().into(),
                cogs: None,
            };
            tempdir = Some(dir);
            Box::new(LocalFsBackend::new(config, &ChangeStreamFactory::default()))
        });
        let backend = TieredStorage::new(high_volume, long_term, Box::new(NoopChangeLog));

        let state = Services::spawn_with_backend(config, Box::new(backend))
            .await
            .unwrap();
        let app = App::new(state);

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            app.serve(listener).await.unwrap();
        });

        TestServer {
            handle,
            socket,
            _tempdir: tempdir,
        }
    }
}
