use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use objectstore_types::ExpirationPolicy;
use url::Url;

pub use objectstore_types::{Compression, PARAM_SCOPE, PARAM_USECASE};

const USER_AGENT: &str = concat!("objectstore-client/", env!("CARGO_PKG_VERSION"));

#[derive(Debug)]
struct ClientBuilderInner {
    service_url: Url,
    propagate_traces: bool,
    reqwest_builder: reqwest::ClientBuilder,
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
        }))
    }

    /// Changes whether the `sentry-trace` header will be sent to Objectstore
    /// to take advantage of Sentry's distributed tracing.
    pub fn propagate_traces(mut self, propagate_traces: bool) -> Self {
        if let Ok(ref mut inner) = self.0 {
            inner.propagate_traces = propagate_traces;
        }
        self
    }

    /// Sets both the connect and the read timeout for the [`reqwest::Client`].
    /// For more fine-grained configuration, use [`Self::configure_reqwest`].
    pub fn timeout(self, timeout: Duration) -> Self {
        let Ok(mut inner) = self.0 else { return self };
        inner.reqwest_builder = inner
            .reqwest_builder
            .connect_timeout(timeout)
            .read_timeout(timeout);
        Self(Ok(inner))
    }

    /// Calls the closure with the underlying [`reqwest::ClientBuilder`].
    pub fn configure_reqwest<F>(self, closure: F) -> Self
    where
        F: FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
    {
        let Ok(mut inner) = self.0 else { return self };
        inner.reqwest_builder = closure(inner.reqwest_builder);
        Self(Ok(inner))
    }

    /// Returns a [`Client`] that uses this [`ClientBuilder`] configuration.
    ///
    /// # Errors
    ///
    /// This method fails if:
    /// - the given `service_url` is invalid
    /// - the [`reqwest::Client`] fails to build. Refer to [`reqwest::ClientBuilder::build`] for
    ///   more information on when this can happen.
    pub fn build(self) -> crate::Result<Client> {
        let inner = self.0?.apply_defaults();
        Ok(Client {
            inner: Arc::new(ClientInner {
                reqwest: inner.reqwest_builder.build()?,
                service_url: inner.service_url,
                propagate_traces: inner.propagate_traces,
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
    expiration: ExpirationPolicy,
}

impl Usecase {
    /// Creates a new Usecase.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            compression: Compression::Zstd,
            expiration: Default::default(),
        }
    }

    /// Returns the name of this usecase.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the compression algorithm to use by default for operations within this usecase.
    #[inline]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Sets the compression algorithm to use by default for operations within this usecase.
    pub fn with_compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    /// Returns the expiration policy to use by default for operations within this usecase.
    #[inline]
    pub fn expiration(&self) -> ExpirationPolicy {
        self.expiration
    }

    /// Sets the expiration policy to use by default for operations within this usecase.
    pub fn with_expiration(self, expiration: ExpirationPolicy) -> Self {
        Self { expiration, ..self }
    }

    /// Creates a new custom [`Scope`].
    ///
    /// Add parts to it using [`Scope::push`].
    /// Generally, [`Scope::for_organization`] and [`Scope::for_project`] should fit most usecases,
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
    scope: String,
}

impl ScopeInner {
    #[inline]
    pub(crate) fn usecase(&self) -> &Usecase {
        &self.usecase
    }
}

impl std::fmt::Display for ScopeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.scope)
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
    /// Using a custom Scope is discouraged, prefer using [`Usecase::for_organization`] or [`Usecase::for_project`] instead.
    pub fn new(usecase: Usecase) -> Self {
        Self(Ok(ScopeInner {
            usecase,
            scope: String::new(),
        }))
    }

    fn for_organization(usecase: Usecase, organization: u64) -> Self {
        let scope = format!("org.{}", organization);
        Self(Ok(ScopeInner { usecase, scope }))
    }

    fn for_project(usecase: Usecase, organization: u64, project: u64) -> Self {
        let scope = format!("org.{}/project.{}", organization, project);
        Self(Ok(ScopeInner { usecase, scope }))
    }

    /// Extends this Scope by creating a new sub-scope nested within it.
    pub fn push<V>(self, key: &str, value: &V) -> Self
    where
        V: std::fmt::Display,
    {
        let result = self.0.and_then(|mut inner| {
            Self::validate_key(key)?;

            let value = value.to_string();
            Self::validate_value(&value)?;

            if !inner.scope.is_empty() {
                inner.scope.push('/');
            }
            inner.scope.push_str(key);
            inner.scope.push('.');
            inner.scope.push_str(&value);

            Ok(inner)
        });

        Self(result)
    }

    /// Characters allowed in a Scope's key and value.
    /// These are the URL safe characters, except for `.` which we use as separator between
    /// key and value of Scope components.
    const ALLOWED_CHARS: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-()$!+'";

    /// Validates that a scope key contains only allowed characters and is not empty.
    fn validate_key(key: &str) -> crate::Result<()> {
        if key.is_empty() {
            return Err(crate::Error::InvalidScope {
                message: "Scope key cannot be empty".to_string(),
            });
        }
        if key.bytes().all(|b| Self::ALLOWED_CHARS.contains(&b)) {
            Ok(())
        } else {
            Err(crate::Error::InvalidScope {
                message: format!("Invalid scope key '{key}'."),
            })
        }
    }

    /// Validates that a scope value contains only allowed characters and is not empty.
    fn validate_value(value: &str) -> crate::Result<()> {
        if value.is_empty() {
            return Err(crate::Error::InvalidScope {
                message: "Scope value cannot be empty".to_string(),
            });
        }
        if value.bytes().all(|b| Self::ALLOWED_CHARS.contains(&b)) {
            Ok(())
        } else {
            Err(crate::Error::InvalidScope {
                message: format!("Invalid scope value '{value}'."),
            })
        }
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
}

/// A client for Objectstore. Use [`Client::builder`] to configure and construct a Client.
///
/// To perform CRUD operations, one has to create a Client, and then scope it to a [`Usecase`]
/// and Scope in order to create a [`Session`].
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use objectstore_client::{Client, Usecase};
///
/// # async fn example() -> objectstore_client::Result<()> {
/// let client = Client::builder("http://localhost:8888/")
///     .timeout(Duration::from_secs(1))
///     .propagate_traces(true)
///     .build()?;
/// let usecase = Usecase::new("my_app");
///
/// let session = client.session(usecase.for_project(12345, 1337))?;
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
/// Create a Session using [`Client::session`] or [`Scope::session`].
#[derive(Debug, Clone)]
pub struct Session {
    pub(crate) scope: Arc<ScopeInner>,
    pub(crate) client: Arc<ClientInner>,
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Session {
    pub(crate) fn request(
        &self,
        method: reqwest::Method,
        resource_id: &str,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let mut url = self.client.service_url.clone();
        url.path_segments_mut()
            .map_err(|_| crate::Error::InvalidUrl {
                message: format!("The URL {} cannot be a base", self.client.service_url),
            })?
            .extend(&["v1", resource_id]);

        let mut builder = self.client.reqwest.request(method, url).query(&[
            (PARAM_SCOPE, self.scope.scope.as_str()),
            (PARAM_USECASE, self.scope.usecase.name.as_ref()),
        ]);

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
