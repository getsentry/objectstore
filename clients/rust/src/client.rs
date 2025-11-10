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

/// Builder to obtain a [`Client`].
#[derive(Debug)]
pub struct ClientBuilder(crate::Result<ClientBuilderInner>);

impl ClientBuilder {
    /// Creates a new [`ClientBuilder`], configured with the given `service_url`.
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

    /// This changes whether the `sentry-trace` header will be sent to Objectstore
    /// to take advantage of Sentry's distributed tracing.
    pub fn propagate_traces(mut self, propagate_traces: bool) -> Self {
        if let Ok(ref mut inner) = self.0 {
            inner.propagate_traces = propagate_traces;
        }
        self
    }

    /// Sets both the connect and the read timeout for the reqwest Client.
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

    /// TODO: document
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

    /// TODO: document
    pub fn build(self) -> crate::Result<Client> {
        self.0
            .map(|inner| inner.apply_defaults())
            .and_then(|inner| {
                Ok(Client {
                    reqwest: inner.reqwest_builder.build()?,
                    service_url: inner.service_url,
                    propagate_traces: inner.propagate_traces,
                })
            })
    }
}

/// TODO: document
#[derive(Debug, Clone)]
pub struct Usecase {
    name: Arc<str>,
    compression: Compression,
    expiration: ExpirationPolicy,
}

impl Usecase {
    /// TODO: document
    pub fn new(name: impl Into<Arc<str>>) -> Self {
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
struct ScopeInner {
    usecase: Usecase,
    scope: String,
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
    pub fn push<V>(mut self, key: &str, value: &V) -> Self
    where
        V: std::fmt::Display,
    {
        if let Ok(ref mut inner) = self.0 {
            // TODO: validate and turn into error on failure
            if !inner.scope.is_empty() {
                inner.scope.push('/');
            }
            inner.scope.push_str(key);
            inner.scope.push('.');
            inner.scope.push_str(value.to_string().as_str());
        }
        self
    }

    /// TODO: document
    pub fn session(self, client: &Client) -> crate::Result<Session> {
        client.session(self)
    }
}

/// A client for Objectstore.
#[derive(Debug)]
pub struct Client {
    reqwest: reqwest::Client,
    service_url: Url,
    propagate_traces: bool,
}

impl Client {
    /// TODO: document
    pub fn session(&self, scope: Scope) -> crate::Result<Session> {
        scope.0.map(|inner| Session {
            service_url: Arc::new(self.service_url.clone()), // TODO: revisit
            usecase: inner.usecase.clone(),
            scope: inner.scope,
            propagate_traces: self.propagate_traces,
            reqwest: self.reqwest.clone(),
        })
    }
}

/// TODO: document
#[derive(Debug)]
pub struct Session {
    // TODO: add getters instead of pub(crate)
    // these will probably be gone though
    pub(crate) service_url: Arc<Url>,
    pub(crate) usecase: Usecase,

    // TODO: change to something like this
    //client: Arc<Client>,
    //inner: Arc<SessionInner>,
    scope: String,
    propagate_traces: bool,
    reqwest: reqwest::Client,
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Session {
    pub(crate) fn request<U: reqwest::IntoUrl>(
        &self,
        method: reqwest::Method,
        uri: U,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let mut builder = self.reqwest.request(method, uri).query(&[
            (PARAM_SCOPE, self.scope.as_str()),
            (PARAM_USECASE, self.usecase.name.as_ref()),
        ]);

        if self.propagate_traces {
            let trace_headers =
                sentry_core::configure_scope(|scope| Some(scope.iter_trace_propagation_headers()));
            for (header_name, value) in trace_headers.into_iter().flatten() {
                builder = builder.header(header_name, value);
            }
        }

        Ok(builder)
    }
}
