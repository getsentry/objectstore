use std::sync::Arc;
use std::{fmt, io};

use bytes::Bytes;
use futures_util::stream::BoxStream;
use jsonwebtoken::{EncodingKey, Header};
use objectstore_types::Scope;
use reqwest::header;
use serde::Serialize;

pub use objectstore_types::Compression;

/// Service for storing and retrieving objects.
///
/// The Service contains the base configuration to connect to a service.
/// It has to be further initialized with credentials using the
/// [`for_organization`](Self::for_organization) and
/// [`for_project`](Self::for_project) functions.
pub struct ClientBuilder {
    service_url: Arc<str>,
    client: reqwest::Client,
    jwt_key: EncodingKey,

    usecase: Arc<str>,
    default_compression: Compression,
}
impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("service_url", &self.service_url)
            .field("client", &self.client)
            .field("jwt_key", &format_args!("[JWT Key]"))
            .field("usecase", &self.usecase)
            .field("default_compression", &self.default_compression)
            .finish()
    }
}

impl ClientBuilder {
    /// Creates a new [`ClientBuilder`].
    ///
    /// This service instance is configured to target the given `service_url`, using the `jwt_secret`
    /// for authentication.
    /// It is also scoped for the given `usecase`.
    ///
    /// In order to get or put objects, one has to create a [`Client`] using the
    /// [`for_organization`](Self::for_organization) function.
    pub fn new(service_url: &str, jwt_secret: &str, usecase: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            // we are dealing with de/compression ourselves:
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()?;
        let jwt_key = EncodingKey::from_secret(jwt_secret.as_bytes());

        Ok(Self {
            service_url: service_url.trim_end_matches('/').into(),
            client,

            jwt_key,
            usecase: usecase.into(),
            default_compression: Compression::Zstd,
        })
    }

    /// This changes the default compression used for uploads.
    pub fn default_compression(mut self, compression: Compression) -> Self {
        self.default_compression = compression;
        self
    }

    fn make_client(&self, scope: Scope) -> Client {
        Client {
            service_url: self.service_url.clone(),
            http: self.client.clone(),
            jwt_key: self.jwt_key.clone(),

            usecase: self.usecase.clone(),
            scope,
            default_compression: self.default_compression,
        }
    }

    /// Create a new [`Client`] scoped to the given organization.
    pub fn for_organization(&self, organization_id: u64) -> Client {
        self.make_client(Scope {
            organization: organization_id,
            project: None,
        })
    }

    /// Create a new [`Client`] scoped to the given organization/project.
    pub fn for_project(&self, organization_id: u64, project_id: u64) -> Client {
        self.make_client(Scope {
            organization: organization_id,
            project: Some(project_id),
        })
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
struct Claims<'a> {
    pub(crate) exp: u64,
    pub(crate) usecase: &'a str,
    pub(crate) scope: &'a Scope,
    pub(crate) permissions: &'a [&'a str],
}

/// A scoped objectstore client that can access objects in a specific use case and scope.
pub struct Client {
    pub(crate) http: reqwest::Client,
    pub(crate) service_url: Arc<str>,
    jwt_key: EncodingKey,

    usecase: Arc<str>,
    scope: Scope,
    pub(crate) default_compression: Compression,
}
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("http", &self.http)
            .field("service_url", &self.service_url)
            .field("jwt_key", &format_args!("[JWT Key]"))
            .field("usecase", &self.usecase)
            .field("scope", &self.scope)
            .field("default_compression", &self.default_compression)
            .finish()
    }
}

/// The type of [`Stream`](futures_util::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, io::Result<Bytes>>;

impl Client {
    pub(crate) fn make_authorization(&self, permission: &str) -> anyhow::Result<String> {
        let claims = Claims {
            exp: jsonwebtoken::get_current_timestamp(),
            usecase: &self.usecase,
            scope: &self.scope,
            permissions: &[permission],
        };
        let header = Header::default();

        let token = jsonwebtoken::encode(&header, &claims, &self.jwt_key)?;
        Ok(token)
    }

    /// Deletes the object with the given `id`.
    pub async fn delete(&self, id: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}/{id}", self.service_url);
        let authorization = self.make_authorization("write")?;

        let _response = self
            .http
            .delete(delete_url)
            .header(header::AUTHORIZATION, authorization)
            .send()
            .await?;

        Ok(())
    }
}
