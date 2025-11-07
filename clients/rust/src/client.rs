use std::fmt::Display;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use objectstore_types::ExpirationPolicy;
use reqwest::header::HeaderName;
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
pub enum ClientBuilder {
    Ok(ClientBuilderInner),
    Err(anyhow::Error),
}

impl ClientBuilder {
    /// Creates a new [`ClientBuilder`], configured with the given `service_url`.
    /// To perform CRUD operations, one has to create a [`Client`], and then scope it to a [`Usecase`]
    /// and Scope in order to create a [`Session`].
    pub fn new(service_url: &str) -> Self {
        let service_url = {
            let res = service_url.parse();
            if let Err(err) = res {
                return Self::Err(anyhow::Error::from(err));
            }
            res.unwrap()
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

        Self::Ok(ClientBuilderInner {
            service_url,
            propagate_traces: false,
            reqwest_builder,
        })
    }

    /// This changes whether the `sentry-trace` header will be sent to Objectstore
    /// to take advantage of Sentry's distributed tracing.
    pub fn with_distributed_tracing(mut self, propagate_traces: bool) -> Self {
        if let Self::Ok(ref mut inner) = self {
            inner.propagate_traces = propagate_traces;
        }
        self
    }

    pub fn with_reqwest_builder(mut self, builder: reqwest::ClientBuilder) -> Self {
        if let Self::Ok(ref mut inner) = self {
            inner.reqwest_builder = builder;
        }
        self
    }

    pub fn build(self) -> anyhow::Result<Client> {
        match self {
            Self::Ok(mut inner) => {
                inner = inner.apply_defaults();
                Ok(Client {
                    reqwest: inner.reqwest_builder.build()?,
                    service_url: Arc::new(inner.service_url),
                    propagate_traces: inner.propagate_traces,
                })
            }
            ClientBuilder::Err(err) => Err(err),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Usecase {
    name: Arc<str>,
    pub(crate) compression: Compression,
    pub(crate) expiration: ExpirationPolicy,
}

impl Usecase {
    pub fn new(name: &str) -> Self {
        Self {
            name: Arc::from(name),
            compression: Compression::Zstd,
            expiration: Default::default(),
        }
    }

    pub fn name(&self) -> Arc<str> {
        self.name.clone()
    }

    pub fn with_compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn with_expiration(self, expiration: ExpirationPolicy) -> Self {
        Self { expiration, ..self }
    }

    pub fn scope(&self) -> Scope {
        Scope::new(self.clone())
    }

    pub fn for_organization(&self, organization: u64) -> Scope {
        Scope::for_organization(self.clone(), organization)
    }

    pub fn for_project(&self, organization: u64, project: u64) -> Scope {
        Scope::for_project(self.clone(), organization, project)
    }
}

#[derive(Debug)]
struct ScopeInner {
    usecase: Usecase,
    scope: String,
}

impl Display for ScopeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.scope)
    }
}

#[derive(Debug)]
pub enum Scope {
    Ok(ScopeInner),
    Err(anyhow::Error),
}

impl Scope {
    pub fn new(usecase: Usecase) -> Self {
        Self::Ok(ScopeInner {
            usecase,
            scope: String::new(),
        })
    }

    fn for_organization(usecase: Usecase, organization: u64) -> Self {
        let mut buf = itoa::Buffer::new();
        let organization = buf.format(organization);
        let mut scope = String::with_capacity(4 + organization.len());
        scope.push_str("org.");
        scope.push_str(organization);
        Self::Ok(ScopeInner { usecase, scope })
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
        Self::Ok(ScopeInner { usecase, scope })
    }

    pub fn push<V>(mut self, key: &str, value: &V) -> Self
    where
        V: Display,
    {
        if let Self::Ok(ref mut inner) = self {
            if inner.scope.len() > 0 {
                inner.scope.push('/');
            }
            inner.scope.push_str(key);
            inner.scope.push('.');
            inner.scope.push_str(value.to_string().as_str());
        }
        self
    }
}

/// A client for Objectstore.
#[derive(Debug)]
pub struct Client {
    reqwest: reqwest::Client,
    service_url: Arc<Url>,
    propagate_traces: bool,
}

impl Client {
    pub fn session(&self, scope: Scope) -> anyhow::Result<Session> {
        match scope {
            Scope::Ok(inner) => Ok(Session {
                service_url: Arc::clone(&self.service_url),
                usecase: inner.usecase.clone(),
                scope: inner.scope,
                propagate_traces: self.propagate_traces,
                reqwest: self.reqwest.clone(),
            }),
            Scope::Err(error) => Err(error),
        }
    }
}

#[derive(Debug)]
pub struct Session {
    pub(crate) service_url: Arc<Url>,
    pub(crate) usecase: Usecase,
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
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let mut builder = self.reqwest.request(method, uri).query(&[
            (PARAM_SCOPE, self.scope.as_str()),
            (PARAM_USECASE, self.usecase.name.as_ref()),
        ]);

        if self.propagate_traces {
            let trace_headers =
                sentry_core::configure_scope(|scope| Some(scope.iter_trace_propagation_headers()));
            for (header_name, value) in trace_headers.into_iter().flatten() {
                builder = builder.header(HeaderName::try_from(header_name)?, value);
            }
        }

        Ok(builder)
    }

    /// Deletes the object with the given `id`.
    pub async fn delete(&self, id: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}v1/{id}", self.service_url);

        let _response = self
            .request(reqwest::Method::DELETE, delete_url)?
            .send()
            .await?;

        Ok(())
    }
}
