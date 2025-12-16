use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use objectstore_types::{Compression, ExpirationPolicy, scope};
use url::Url;

use crate::auth::TokenGenerator;

const USER_AGENT: &str = concat!("objectstore-client/", env!("CARGO_PKG_VERSION"));

#[derive(Debug)]
struct ClientBuilderInner {
    service_url: Url,
    propagate_traces: bool,
    reqwest_builder: reqwest::ClientBuilder,
    token_generator: Option<TokenGenerator>,
}

impl ClientBuilderInner {
    /// Applies defaults that cannot be overridden by the caller.
    fn apply_defaults(mut self) -> Self {
        self.reqwest_builder = self
            .reqwest_builder
            // hickory-dns: Controlled by the `reqwest/hickory-dns` feature flag
            // we are dealing with de/compression ourselves:
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd();
        self
    }
}

/// Builder to create a [`Client`].
#[must_use = "call .build() on this ClientBuilder to create a Client"]
#[derive(Debug)]
pub struct ClientBuilder(crate::Result<ClientBuilderInner>);

impl ClientBuilder {
    /// Creates a new [`ClientBuilder`], configured with the given `service_url`.
    ///
    /// To perform CRUD operations, one has to create a [`Client`], and then scope it to a [`Usecase`]
    /// and Scope in order to create a [`Session`].
    pub fn new(service_url: impl reqwest::IntoUrl) -> Self {
        let service_url = match service_url.into_url() {
            Ok(url) => url,
            Err(err) => return Self(Err(err.into())),
        };
        if service_url.cannot_be_a_base() {
            return ClientBuilder(Err(crate::Error::InvalidUrl {
                message: "service_url cannot be a base".to_owned(),
            }));
        }

        let reqwest_builder = reqwest::Client::builder()
            // The read timeout "applies to each read operation", so should work fine for larger
            // transfers that are split into multiple chunks.
            // We define both as 500ms which is still very conservative, given that we are in the same network,
            // and expect our backends to respond in <100ms.
            // This can be overridden by the caller.
            .connect_timeout(Duration::from_millis(500))
            .read_timeout(Duration::from_millis(500))
            .user_agent(USER_AGENT);

        Self(Ok(ClientBuilderInner {
            service_url,
            propagate_traces: false,
            reqwest_builder,
            token_generator: None,
        }))
    }

    /// Changes whether the `sentry-trace` header will be sent to Objectstore
    /// to take advantage of Sentry's distributed tracing.
    ///
    /// By default, tracing headers will not be propagated.
    pub fn propagate_traces(mut self, propagate_traces: bool) -> Self {
        if let Ok(ref mut inner) = self.0 {
            inner.propagate_traces = propagate_traces;
        }
        self
    }

    /// Sets both the connect and the read timeout for the [`reqwest::Client`].
    /// For more fine-grained configuration, use [`Self::configure_reqwest`].
    ///
    /// By default, a connect and read timeout of 500ms is set.
    pub fn timeout(self, timeout: Duration) -> Self {
        let Ok(mut inner) = self.0 else { return self };
        inner.reqwest_builder = inner
            .reqwest_builder
            .connect_timeout(timeout)
            .read_timeout(timeout);
        Self(Ok(inner))
    }

    /// Calls the closure with the underlying [`reqwest::ClientBuilder`].
    ///
    /// By default, the ClientBuilder is configured to create a reqwest Client with a connect and read timeout of 500ms and a user agent identifying this library.
    pub fn configure_reqwest<F>(self, closure: F) -> Self
    where
        F: FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
    {
        let Ok(mut inner) = self.0 else { return self };
        inner.reqwest_builder = closure(inner.reqwest_builder);
        Self(Ok(inner))
    }

    /// Sets a [`TokenGenerator`] that will be used to sign authorization tokens before
    /// sending requests to Objectstore.
    pub fn token_generator(self, token_generator: TokenGenerator) -> Self {
        let Ok(mut inner) = self.0 else { return self };
        inner.token_generator = Some(token_generator);
        Self(Ok(inner))
    }

    /// Returns a [`Client`] that uses this [`ClientBuilder`] configuration.
    ///
    /// # Errors
    ///
    /// This method fails if:
    /// - the given `service_url` is invalid or cannot be used as a base URL
    /// - the [`reqwest::Client`] fails to build. Refer to [`reqwest::ClientBuilder::build`] for
    ///   more information on when this can happen.
    pub fn build(self) -> crate::Result<Client> {
        let inner = self.0?.apply_defaults();

        Ok(Client {
            inner: Arc::new(ClientInner {
                reqwest: inner.reqwest_builder.build()?,
                service_url: inner.service_url,
                propagate_traces: inner.propagate_traces,
                token_generator: inner.token_generator,
            }),
        })
    }
}

/// An identifier for a workload in Objectstore, along with defaults to use for all
/// operations within that Usecase.
///
/// Usecases need to be statically defined in Objectstore's configuration server-side.
/// Objectstore can make decisions based on the Usecase. For example, choosing the most
/// suitable storage backend.
#[derive(Debug, Clone)]
pub struct Usecase {
    name: Arc<str>,
    compression: Compression,
    expiration_policy: ExpirationPolicy,
}

impl Usecase {
    /// Creates a new Usecase.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            compression: Compression::Zstd,
            expiration_policy: Default::default(),
        }
    }

    /// Returns the name of this usecase.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the compression algorithm to use for operations within this usecase.
    #[inline]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Sets the compression algorithm to use for operations within this usecase.
    ///
    /// It's still possible to override this default on each operation's builder.
    ///
    /// By default, [`Compression::Zstd`] is used.
    pub fn with_compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    /// Returns the expiration policy to use by default for operations within this usecase.
    #[inline]
    pub fn expiration_policy(&self) -> ExpirationPolicy {
        self.expiration_policy
    }

    /// Sets the expiration policy to use for operations within this usecase.
    ///
    /// It's still possible to override this default on each operation's builder.
    ///
    /// By default, [`ExpirationPolicy::Manual`] is used, meaning that objects won't automatically
    /// expire.
    pub fn with_expiration_policy(self, expiration_policy: ExpirationPolicy) -> Self {
        Self {
            expiration_policy,
            ..self
        }
    }

    /// Creates a new custom [`Scope`].
    ///
    /// Add parts to it using [`Scope::push`].
    ///
    /// Generally, [`Usecase::for_organization`] and [`Usecase::for_project`] should fit most usecases,
    /// so prefer using those methods rather than creating your own custom [`Scope`].
    pub fn scope(&self) -> Scope {
        Scope::new(self.clone())
    }

    /// Creates a new [`Scope`] tied to the given organization.
    pub fn for_organization(&self, organization: u64) -> Scope {
        Scope::for_organization(self.clone(), organization)
    }

    /// Creates a new [`Scope`] tied to the given organization and project.
    pub fn for_project(&self, organization: u64, project: u64) -> Scope {
        Scope::for_project(self.clone(), organization, project)
    }
}

#[derive(Debug)]
pub(crate) struct ScopeInner {
    usecase: Usecase,
    scopes: scope::Scopes,
}

impl ScopeInner {
    #[inline]
    pub(crate) fn usecase(&self) -> &Usecase {
        &self.usecase
    }

    #[inline]
    pub(crate) fn scopes(&self) -> &scope::Scopes {
        &self.scopes
    }
}

/// A [`Scope`] is a sequence of key-value pairs that defines a (possibly nested) namespace within a
/// [`Usecase`].
///
/// To construct a [`Scope`], use [`Usecase::for_organization`], [`Usecase::for_project`], or
/// [`Usecase::scope`] for custom scopes.
#[derive(Debug)]
pub struct Scope(crate::Result<ScopeInner>);

impl Scope {
    /// Creates a new root-level Scope for the given usecase.
    ///
    /// Using a custom Scope is discouraged, prefer using [`Usecase::for_organization`] or [`Usecase::for_project`] instead.
    pub fn new(usecase: Usecase) -> Self {
        Self(Ok(ScopeInner {
            usecase,
            scopes: scope::Scopes::empty(),
        }))
    }

    fn for_organization(usecase: Usecase, organization: u64) -> Self {
        Self::new(usecase).push("org", organization)
    }

    fn for_project(usecase: Usecase, organization: u64, project: u64) -> Self {
        Self::for_organization(usecase, organization).push("project", project)
    }

    /// Extends this Scope by creating a new sub-scope nested within it.
    pub fn push<V>(self, key: &str, value: V) -> Self
    where
        V: std::fmt::Display,
    {
        let result = self.0.and_then(|mut inner| {
            inner.scopes.push(key, value)?;
            Ok(inner)
        });

        Self(result)
    }

    /// Creates a session for this scope using the given client.
    ///
    /// # Errors
    ///
    /// Returns an error if the scope is invalid (e.g. it contains invalid characters).
    pub fn session(self, client: &Client) -> crate::Result<Session> {
        client.session(self)
    }
}

#[derive(Debug)]
pub(crate) struct ClientInner {
    reqwest: reqwest::Client,
    service_url: Url,
    propagate_traces: bool,
    token_generator: Option<TokenGenerator>,
}

/// A client for Objectstore. Use [`Client::builder`] to configure and construct a Client.
///
/// To perform CRUD operations, one has to create a Client, and then scope it to a [`Usecase`]
/// and Scope in order to create a [`Session`].
///
/// If your Objectstore instance enforces authorization checks, you must provide a
/// [`TokenGenerator`] on creation.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use objectstore_client::{Client, SecretKey, TokenGenerator, Usecase};
/// use objectstore_types::Permission;
///
/// # async fn example() -> objectstore_client::Result<()> {
/// let token_generator = TokenGenerator::new(SecretKey {
///         secret_key: "<safely inject secret key>".into(),
///         kid: "my-service".into(),
///     })?
///     .expiry_seconds(30)
///     .permissions(&[Permission::ObjectRead]);
///
/// let client = Client::builder("http://localhost:8888/")
///     .timeout(Duration::from_secs(1))
///     .propagate_traces(true)
///     .token_generator(token_generator)
///     .build()?;
///
/// let session = Usecase::new("my_app")
///     .for_project(12345, 1337)
///     .session(&client)?;
///
/// let response = session.put("hello world").send().await?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Creates a new [`Client`], configured with the given `service_url` and default
    /// configuration.
    ///
    /// Use [`Client::builder`] for more fine-grained configuration.
    ///
    /// # Errors
    ///
    /// This method fails if [`ClientBuilder::build`] fails.
    pub fn new(service_url: impl reqwest::IntoUrl) -> crate::Result<Client> {
        ClientBuilder::new(service_url).build()
    }

    /// Convenience function to create a [`ClientBuilder`].
    pub fn builder(service_url: impl reqwest::IntoUrl) -> ClientBuilder {
        ClientBuilder::new(service_url)
    }

    /// Creates a session for the given scope using this client.
    ///
    /// # Errors
    ///
    /// Returns an error if the scope is invalid (e.g. it contains invalid characters).
    pub fn session(&self, scope: Scope) -> crate::Result<Session> {
        scope.0.map(|inner| Session {
            scope: inner.into(),
            client: self.inner.clone(),
        })
    }
}

/// Represents a session with Objectstore, tied to a specific Usecase and Scope within it.
///
/// Create a Session using [`Client::session`] or [`Scope::session`].
#[derive(Debug, Clone)]
pub struct Session {
    pub(crate) scope: Arc<ScopeInner>,
    pub(crate) client: Arc<ClientInner>,
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Session {
    /// Generates a GET url to the object with the given `key`.
    ///
    /// This can then be used by downstream services to fetch the given object.
    /// NOTE however that the service does not strictly follow HTTP semantics,
    /// in particular in relation to `Accept-Encoding`.
    pub fn object_url(&self, object_key: &str) -> Url {
        let mut url = self.client.service_url.clone();

        // `path_segments_mut` can only error if the url is cannot-be-a-base,
        // and we check that in `ClientBuilder::new`, therefore this will never panic.
        let mut segments = url.path_segments_mut().unwrap();
        segments
            .push("v1")
            .push("objects")
            .push(&self.scope.usecase.name)
            .push(&self.scope.scopes.as_api_path().to_string())
            .extend(object_key.split("/"));
        drop(segments);

        url
    }

    pub(crate) fn request(
        &self,
        method: reqwest::Method,
        object_key: &str,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let url = self.object_url(object_key);

        let mut builder = self.client.reqwest.request(method, url);

        if let Some(token_generator) = &self.client.token_generator {
            let token = token_generator.sign_for_scope(&self.scope)?;
            builder = builder.bearer_auth(token);
        }

        if self.client.propagate_traces {
            let trace_headers =
                sentry_core::configure_scope(|scope| Some(scope.iter_trace_propagation_headers()));
            for (header_name, value) in trace_headers.into_iter().flatten() {
                builder = builder.header(header_name, value);
            }
        }

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_url() {
        let client = Client::new("http://127.0.0.1:8888/").unwrap();
        let usecase = Usecase::new("testing");
        let scope = usecase
            .for_project(12345, 1337)
            .push("app_slug", "email_app");
        let session = client.session(scope).unwrap();

        assert_eq!(
            session.object_url("foo/bar").to_string(),
            "http://127.0.0.1:8888/v1/objects/testing/org=12345;project=1337;app_slug=email_app/foo/bar"
        )
    }

    #[test]
    fn test_object_url_with_base_path() {
        let client = Client::new("http://127.0.0.1:8888/api/prefix").unwrap();
        let usecase = Usecase::new("testing");
        let scope = usecase.for_project(12345, 1337);
        let session = client.session(scope).unwrap();

        assert_eq!(
            session.object_url("foo/bar").to_string(),
            "http://127.0.0.1:8888/api/prefix/v1/objects/testing/org=12345;project=1337/foo/bar"
        )
    }
}
