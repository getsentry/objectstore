use std::io;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use reqwest::header::HeaderName;

pub use objectstore_types::{Compression, PARAM_SCOPE, PARAM_USECASE};

/// Service for storing and retrieving objects.
///
/// The Service contains the base configuration to connect to a service.
/// It has to be further initialized with credentials using the
/// [`for_organization`](Self::for_organization) and
/// [`for_project`](Self::for_project) functions.
#[derive(Debug)]
pub struct ClientBuilder {
    service_url: Arc<str>,
    client: reqwest::Client,

    usecase: Arc<str>,
    default_compression: Compression,
    propagate_traces: bool,
}

impl ClientBuilder {
    /// Creates a new [`ClientBuilder`].
    ///
    /// This service instance is configured to target the given `service_url`.
    /// It is also scoped for the given `usecase`.
    ///
    /// In order to get or put objects, one has to create a [`Client`] using the
    /// [`for_organization`](Self::for_organization) function.
    pub fn new(service_url: &str, usecase: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            // we are dealing with de/compression ourselves:
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()?;

        Ok(Self {
            service_url: service_url.trim_end_matches('/').into(),
            client,

            usecase: usecase.into(),
            default_compression: Compression::Zstd,
            propagate_traces: false,
        })
    }

    /// This changes the default compression used for uploads.
    pub fn default_compression(mut self, compression: Compression) -> Self {
        self.default_compression = compression;
        self
    }

    /// This changes whether the `sentry-trace` header will be sent to Objectstore
    /// to take advantage of Sentry's distributed tracing.
    pub fn with_distributed_tracing(mut self, propagate_traces: bool) -> Self {
        self.propagate_traces = propagate_traces;
        self
    }

    fn make_client(&self, scope: String) -> Client {
        Client {
            service_url: self.service_url.clone(),
            http: self.client.clone(),

            usecase: self.usecase.clone(),
            scope,
            default_compression: self.default_compression,
            propagate_traces: self.propagate_traces,
        }
    }

    /// Create a new [`Client`] and sets its `scope` based on the provided organization.
    pub fn for_organization(&self, organization_id: u64) -> Client {
        let scope = format!("org.{organization_id}");
        self.make_client(scope)
    }

    /// Create a new [`Client`] and sets its `scope` based on the provided organization
    /// and project.
    pub fn for_project(&self, organization_id: u64, project_id: u64) -> Client {
        let scope = format!("org.{organization_id}/proj.{project_id}");
        self.make_client(scope)
    }
}

/// A scoped objectstore client that can access objects in a specific use case and scope.
#[derive(Debug)]
pub struct Client {
    pub(crate) http: reqwest::Client,
    pub(crate) service_url: Arc<str>,

    pub(crate) usecase: Arc<str>,

    /// The scope that this client operates within.
    ///
    /// Scopes are expected to be serialized ordered lists of key/value pairs. Each
    /// pair is serialized with a `.` character between the key and value, and with
    /// a `/` character between each pair. For example:
    /// - `org.123/proj.456`
    /// - `state.washington/city.seattle`
    ///
    /// It is recommended that both keys and values be restricted to alphanumeric
    /// characters.
    pub(crate) scope: String,
    pub(crate) default_compression: Compression,

    propagate_traces: bool,
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Client {
    pub(crate) fn request<U: reqwest::IntoUrl>(
        &self,
        method: reqwest::Method,
        uri: U,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let mut builder = self.http.request(method, uri).query(&[
            (PARAM_SCOPE, self.scope.as_ref()),
            (PARAM_USECASE, self.usecase.as_ref()),
        ]);

        if self.propagate_traces {
            let trace_headers =
                sentry::configure_scope(|scope| Some(scope.iter_trace_propagation_headers()));
            for (header_name, value) in trace_headers.into_iter().flatten() {
                builder = builder.header(HeaderName::try_from(header_name)?, value);
            }
        }

        Ok(builder)
    }

    /// Deletes the object with the given `id`.
    pub async fn delete(&self, id: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}/v1/{id}", self.service_url);

        let _response = self
            .request(reqwest::Method::DELETE, delete_url)?
            .send()
            .await?;

        Ok(())
    }
}
