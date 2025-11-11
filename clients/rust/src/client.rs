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
#[must_use]
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
        let Ok(inner) = self.0 else { return self };
        Self(Ok(ClientBuilderInner {
            service_url: inner.service_url,
            propagate_traces: inner.propagate_traces,
            reqwest_builder: inner
                .reqwest_builder
                .connect_timeout(timeout)
                .read_timeout(timeout),
        }))
    }

    /// Calls the closure with the underlying [`reqwest::ClientBuilder`].
    pub fn configure_reqwest<F>(self, closure: F) -> Self
    where
        F: FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
    {
        let Ok(inner) = self.0 else { return self };
        Self(Ok(ClientBuilderInner {
            service_url: inner.service_url,
            propagate_traces: inner.propagate_traces,
            reqwest_builder: closure(inner.reqwest_builder),
        }))
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
        self.0
            .map(|inner| inner.apply_defaults())
            .and_then(|inner| {
                Ok(Client {
                    inner: Arc::new(ClientInner {
                        reqwest: inner.reqwest_builder.build()?,
                        service_url: inner.service_url,
                        propagate_traces: inner.propagate_traces,
                    }),
                })
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

    /// TODO: document
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// TODO: document
    #[inline]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// TODO: document
    pub fn with_compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    /// TODO: document
    #[inline]
    pub fn expiration(&self) -> ExpirationPolicy {
        self.expiration
    }

    /// TODO: document
    pub fn with_expiration(self, expiration: ExpirationPolicy) -> Self {
        Self { expiration, ..self }
    }

    /// TODO: document
    pub fn scope(&self) -> Scope {
        Scope::new(self.clone())
    }

    /// TODO: document
    pub fn for_organization(&self, organization: u64) -> Scope {
        Scope::for_organization(self.clone(), organization)
    }

    /// TODO: document
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

/// TODO: document
#[derive(Debug)]
pub struct Scope(crate::Result<ScopeInner>);

impl Scope {
    /// TODO: document
    pub fn new(usecase: Usecase) -> Self {
        Self(Ok(ScopeInner {
            usecase,
            scope: String::new(),
        }))
    }

    fn for_organization(usecase: Usecase, organization: u64) -> Self {
        let mut buf = itoa::Buffer::new();
        let organization = buf.format(organization);
        let mut scope = String::with_capacity(4 + organization.len());
        scope.push_str("org.");
        scope.push_str(organization);
        Self(Ok(ScopeInner { usecase, scope }))
    }

    fn for_project(usecase: Usecase, organization: u64, project: u64) -> Self {
        let mut buf = itoa::Buffer::new();
        let organization = buf.format(organization);
        let mut buf = itoa::Buffer::new();
        let project = buf.format(project);
        let mut scope = String::with_capacity(4 + organization.len() + 9 + project.len());
        scope.push_str("org.");
        scope.push_str(organization);
        scope.push_str("/project.");
        scope.push_str(project);
        Self(Ok(ScopeInner { usecase, scope }))
    }

    /// TODO: document
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

    /// TODO: document
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

impl ClientInner {
    /// TODO: document
    #[inline]
    pub(crate) fn service_url(&self) -> &Url {
        &self.service_url
    }
}

/// A client for Objectstore. Use [`Client::builder`] to get configure and construct this.
///
/// To perform CRUD operations, one has to create a [`Client`], and then scope it to a [`Usecase`]
/// and Scope in order to create a [`Session`].
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Convenience function to create a [`ClientBuilder`].
    pub fn builder(service_url: impl reqwest::IntoUrl) -> ClientBuilder {
        ClientBuilder::new(service_url)
    }

    /// TODO: document
    pub fn session(&self, scope: Scope) -> crate::Result<Session> {
        scope.0.map(|inner| Session {
            scope: inner.into(),
            client: self.inner.clone(),
        })
    }
}

/// TODO: document
#[derive(Debug)]
pub struct Session {
    pub(crate) scope: Arc<ScopeInner>,
    pub(crate) client: Arc<ClientInner>,
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Session {
    pub(crate) fn request<U: reqwest::IntoUrl>(
        &self,
        method: reqwest::Method,
        uri: U,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let mut builder = self.client.reqwest.request(method, uri).query(&[
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
