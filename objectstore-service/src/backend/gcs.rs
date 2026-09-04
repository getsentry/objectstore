//! Google Cloud Storage backend for long-term storage of large objects.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use gcp_auth::TokenProvider;
use objectstore_types::headers;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};
use reqwest::header::HeaderName;
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode, Url, header, multipart};
use serde::{Deserialize, Serialize};

use crate::backend::common::{
    self, Backend, DeleteResponse, GetResponse, MetadataResponse, MultipartUploadBackend,
    PutResponse,
};
use crate::backend::extensions::{ReqwestResultExt, ResponseExt, SendTraced};
use crate::change_stream::{
    ChangeStream, ChangeStreamFactory, CostTrackerStreamConfig, flush_change_stream,
};
use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::gcp_auth::PrefetchingTokenProvider;
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::resumable::{BackendToken, UploadProgress};
use crate::stream::ClientStream;

/// Configuration for [`GcsBackend`].
///
/// Stores objects in [Google Cloud Storage]. Authentication uses Application Default Credentials
/// (ADC), which can be provided via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or
/// the GCE/GKE metadata service.
///
/// **Note**: The bucket must be pre-created with the following lifecycle policy:
/// - `daysSinceCustomTime`: 1 day
/// - `action`: delete
///
/// [Google Cloud Storage]: https://cloud.google.com/storage
///
/// # Example
///
/// ```yaml
/// storage:
///   type: gcs
///   bucket: objectstore-bucket
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GcsConfig {
    /// Optional custom GCS endpoint URL.
    ///
    /// Useful for testing with emulators. If `None`, uses the default GCS endpoint.
    ///
    /// # Default
    ///
    /// `None` (uses default GCS endpoint)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE=gcs`
    /// - `OS__STORAGE__ENDPOINT=http://localhost:9000` (optional)
    pub endpoint: Option<String>,

    /// GCS bucket name.
    ///
    /// The bucket must exist before starting the server.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__BUCKET=my-gcs-bucket`
    pub bucket: String,

    /// Reports what this backend stores, for per-usecase cost attribution.
    ///
    /// # Default
    ///
    /// `None`, which disables reporting for this backend.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__COGS__SHARED_RESOURCE_ID=gcs_objectstore`
    /// - `OS__STORAGE__COGS__SAMPLE_RATE=1.0` (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

/// Response header carrying the size GCS stored, in bytes.
///
/// Differs from `Content-Length`, which describes the transfer, once an object is
/// content-encoded.
const STORED_CONTENT_LENGTH: &str = "x-goog-stored-content-length";

/// Reads how many payload bytes GCS stored for an object, consuming the response.
///
/// Prefers the [`STORED_CONTENT_LENGTH`] response header. If it is missing or malformed, falls back
/// to the `size` of the [`GcsObject`] in the response body. Returns `None` if neither source yields
/// a size.
///
/// Either way the body is read to the end, so reqwest can return the connection to its pool.
async fn read_stored_content_length(response: reqwest::Response) -> Option<u64> {
    let header = response
        .headers()
        .get(STORED_CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());

    if let Some(size) = header {
        response.drain_body().await;
        return Some(size);
    }

    response.json::<GcsObject>().await.ok()?.size?.parse().ok()
}

/// Default endpoint used to access the GCS JSON API.
const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";
/// Permission scopes required for accessing GCS.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];
/// How many times to retry failed operations.
const REQUEST_RETRY_COUNT: usize = 2;

/// Prefix for our built-in metadata stored in GCS metadata field
const BUILTIN_META_PREFIX: &str = "x-sn-";
/// Prefix for user custom metadata stored in GCS metadata field
const CUSTOM_META_PREFIX: &str = "x-snme-";

/// GCS object resource.
///
/// This is the representation of the object resource in GCS JSON API without its payload. Where no
/// dedicated fields are available, we encode both built-in and custom metadata in the `metadata`
/// field.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsObject {
    /// Content-Type of the object data. If an object is stored without a Content-Type, it is served
    /// as application/octet-stream.
    pub content_type: Cow<'static, str>,

    /// Content encoding, used to store [`Metadata::compression`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,

    /// Custom time stamp used for time-based expiration.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    pub custom_time: Option<SystemTime>,

    /// The `Content-Length` of the data in bytes. GCS returns this as a string.
    ///
    /// GCS sets this in metadata responses. We can use it to know the size of an object
    /// without having to stream it.
    pub size: Option<String>,

    /// Timestamp of when this object was created.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    pub time_created: Option<SystemTime>,

    /// User-provided metadata, including our built-in metadata.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<GcsMetaKey, String>,

    /// Version of the object's contents.
    #[serde(skip_serializing)]
    pub generation: String,

    /// Version of the object's metadata.
    #[serde(skip_serializing)]
    pub metageneration: String,
}

impl GcsObject {
    /// Bytes this object's custom metadata occupies, keys included.
    ///
    /// GCS stores metadata alongside the payload, so it counts toward an object's size.
    fn metadata_size(&self) -> u64 {
        self.metadata
            .iter()
            .filter(|(key, _)| !matches!(key, GcsMetaKey::EmulatorIgnored))
            .map(|(key, value)| key.to_string().len() as u64 + value.len() as u64)
            .sum()
    }

    /// Converts our Metadata type to GCS JSON object metadata.
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let mut gcs_object = GcsObject {
            content_type: metadata.content_type.clone(),
            size: metadata.size.map(|size| size.to_string()),
            content_encoding: None,
            custom_time: None,
            time_created: metadata.time_created,
            metadata: BTreeMap::new(),
            generation: String::new(),
            metageneration: String::new(),
        };

        // For time-based expiration, set the `customTime` field. The bucket must have a
        // `daysSinceCustomTime` lifecycle rule configured to delete objects with this field set.
        // This rule automatically skips objects without `customTime` set.
        gcs_object.custom_time = metadata.time_expires;

        if let Some(compression) = metadata.compression {
            gcs_object.content_encoding = Some(compression.to_string());
        }

        if metadata.expiration_policy != ExpirationPolicy::default() {
            gcs_object.metadata.insert(
                GcsMetaKey::Expiration,
                metadata.expiration_policy.to_string(),
            );
        }

        // Free-form strings are stored escaped, even though this JSON representation could carry
        // them verbatim. See `insert_gcs_meta_header` for why, and why both writers must agree.
        if let Some(origin) = &metadata.origin {
            gcs_object.metadata.insert(
                GcsMetaKey::Origin,
                headers::encode_header_str(origin).into(),
            );
        }

        if let Some(filename) = &metadata.filename {
            gcs_object.metadata.insert(
                GcsMetaKey::Filename,
                headers::encode_header_str(filename).into(),
            );
        }

        for (key, value) in &metadata.custom {
            gcs_object.metadata.insert(
                GcsMetaKey::Custom(key.clone()),
                headers::encode_header_str(value).into(),
            );
        }

        gcs_object
    }

    /// Converts GCS JSON object metadata to our Metadata type.
    pub fn into_metadata(mut self) -> Result<Metadata> {
        // Remove ignored metadata keys that are set by the GCS emulator.
        self.metadata.remove(&GcsMetaKey::EmulatorIgnored);

        let expiration_policy = self
            .metadata
            .remove(&GcsMetaKey::Expiration)
            .map(|s| s.parse())
            .transpose()
            .context(ErrorKind::CorruptData, "decoding GCS expiration policy")?
            .unwrap_or_default();

        let origin = self
            .metadata
            .remove(&GcsMetaKey::Origin)
            .map(|value| decode_gcs_meta_value(&value, "decoding GCS origin metadata"))
            .transpose()?;
        let filename = self
            .metadata
            .remove(&GcsMetaKey::Filename)
            .map(|value| decode_gcs_meta_value(&value, "decoding GCS filename metadata"))
            .transpose()?;

        let content_type = self.content_type;
        let compression = self
            .content_encoding
            .map(|s| s.parse())
            .transpose()
            .context(ErrorKind::CorruptData, "decoding GCS compression")?;
        let size = self
            .size
            .map(|size| size.parse())
            .transpose()
            .context(ErrorKind::CorruptData, "decoding GCS object size")?;
        let time_created = self.time_created;

        // At this point, all built-in metadata should have been removed from self.metadata.
        let mut custom = BTreeMap::new();
        for (key, value) in self.metadata {
            if let GcsMetaKey::Custom(custom_key) = key {
                custom.insert(
                    custom_key,
                    decode_gcs_meta_value(&value, "decoding GCS custom metadata")?,
                );
            } else {
                return Err(Error::new(
                    ErrorKind::CorruptData,
                    format!("unexpected GCS metadata key: {key}"),
                ));
            }
        }

        Ok(Metadata {
            content_type,
            expiration_policy,
            compression,
            origin,
            filename,
            size,
            custom,
            time_created,
            time_expires: self.custom_time,
        })
    }
}

/// Key for [`GcsObject::metadata`].
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum GcsMetaKey {
    /// Built-in metadata key for [`Metadata::expiration_policy`].
    Expiration,
    /// Built-in metadata key for [`Metadata::origin`].
    Origin,
    /// Built-in metadata key for [`Metadata::filename`].
    Filename,
    /// Ignored metadata set by the GCS emulator.
    EmulatorIgnored,
    /// User-defined custom metadata key.
    Custom(String),
}

impl std::str::FromStr for GcsMetaKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("x_emulator_") || s.starts_with("x_testbench_") {
            return Ok(GcsMetaKey::EmulatorIgnored);
        }

        Ok(match s.strip_prefix(BUILTIN_META_PREFIX) {
            Some("expiration") => GcsMetaKey::Expiration,
            Some("origin") => GcsMetaKey::Origin,
            Some("filename") => GcsMetaKey::Filename,
            Some(unknown) => anyhow::bail!("unknown builtin metadata key: {unknown}"),
            None => match s.strip_prefix(CUSTOM_META_PREFIX) {
                Some(key) => GcsMetaKey::Custom(key.to_string()),
                None => anyhow::bail!("invalid GCS metadata key format: {s}"),
            },
        })
    }
}

impl fmt::Display for GcsMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expiration => write!(f, "{BUILTIN_META_PREFIX}expiration"),
            Self::Origin => write!(f, "{BUILTIN_META_PREFIX}origin"),
            Self::Filename => write!(f, "{BUILTIN_META_PREFIX}filename"),
            Self::EmulatorIgnored => unreachable!("do not serialize emulator metadata"),
            Self::Custom(key) => write!(f, "{CUSTOM_META_PREFIX}{key}"),
        }
    }
}

impl<'de> Deserialize<'de> for GcsMetaKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Cow::<'de, str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for GcsMetaKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

/// Builds HTTP headers that encode `metadata` for a GCS XML API request.
fn metadata_to_gcs_headers(metadata: &Metadata) -> Result<header::HeaderMap> {
    let mut headers = header::HeaderMap::new();

    if let Some(custom_time) = metadata.time_expires {
        let formatted = humantime::format_rfc3339_seconds(custom_time);
        headers.insert(
            HeaderName::from_static("x-goog-custom-time"),
            formatted
                .to_string()
                .parse()
                .context(ErrorKind::Internal, "encoding GCS custom-time header")?,
        );
    }

    if let Some(compression) = metadata.compression {
        headers.insert(
            header::CONTENT_ENCODING,
            compression
                .to_string()
                .parse()
                .context(ErrorKind::Internal, "encoding GCS content-encoding header")?,
        );
    }

    if metadata.expiration_policy != ExpirationPolicy::default() {
        insert_gcs_meta_header(
            &mut headers,
            &GcsMetaKey::Expiration,
            &metadata.expiration_policy.to_string(),
        )?;
    }

    if let Some(origin) = &metadata.origin {
        insert_gcs_meta_header(&mut headers, &GcsMetaKey::Origin, origin)?;
    }

    if let Some(filename) = &metadata.filename {
        insert_gcs_meta_header(&mut headers, &GcsMetaKey::Filename, filename)?;
    }

    for (key, value) in &metadata.custom {
        insert_gcs_meta_header(&mut headers, &GcsMetaKey::Custom(key.clone()), value)?;
    }

    Ok(headers)
}

/// Decodes a stored GCS metadata value into its logical string.
fn decode_gcs_meta_value(value: &str, context: &'static str) -> Result<String> {
    headers::decode_header_str(value).context(ErrorKind::CorruptData, context)
}

/// Inserts a single `x-goog-meta-*` header, escaping the value for transport.
///
/// Google: "you should generally avoid non-ascii characters, because they are not permitted in
/// HTTP headers, which the XML API uses" ([docs]). Real GCS does preserve raw UTF-8 here, but that
/// is undocumented, and it still drops leading whitespace and turns invalid UTF-8 into `U+FFFD`.
///
/// [`GcsObject::from_metadata`] escapes the same values on the JSON path: reads always come back
/// through the JSON API and cannot tell which writer produced an object, so both must agree.
///
/// [docs]: https://docs.cloud.google.com/storage/docs/metadata
fn insert_gcs_meta_header(
    headers: &mut header::HeaderMap,
    key: &GcsMetaKey,
    value: &str,
) -> Result<()> {
    let header_name = format!("x-goog-meta-{key}");
    headers.insert(
        HeaderName::try_from(&header_name).context(
            ErrorKind::Internal,
            format!("encoding GCS metadata header {header_name}"),
        )?,
        headers::encode_header_value(value),
    );
    Ok(())
}

/// Special status code returned by GCS when a Resumable Upload is canceled successfully or when
/// making other requests to a session that was recently canceled.
const CLIENT_CLOSED_REQUEST_STATUS: u16 = 499;

/// Represents a resumable upload session in GCS.
#[derive(Debug)]
struct ResumableUpload {
    // URI to use for requests that act on this session, returned by GCS in the `Location` header
    // on session creation.
    session_uri: Url,
    // Total length of the object, declared at session creation time.
    total_length: u64,
}

impl ResumableUpload {
    fn new(session_uri: Url, total_length: u64) -> Self {
        Self {
            session_uri,
            total_length,
        }
    }

    fn into_token(self) -> BackendToken {
        format!("{}.{}", self.total_length, self.session_uri)
    }

    fn from_token(token: &BackendToken, endpoint: &Url) -> Result<Self> {
        let (total_length, session_uri) = token
            .split_once('.')
            .ok_or(ErrorKind::UnknownUploadSession)?;
        let total_length = total_length
            .parse()
            .map_err(|_| ErrorKind::UnknownUploadSession)?;
        let session_uri = Url::parse(session_uri).map_err(|_| ErrorKind::UnknownUploadSession)?;
        let session = Self::new(session_uri, total_length);
        if session.session_uri.origin() != endpoint.origin() {
            return Err(ErrorKind::UnknownUploadSession.into());
        }
        Ok(session)
    }
}

/// Returns `true` if the error is a transient backend failure worth retrying.
fn error_is_retryable(error: &Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::BackendRateLimited | ErrorKind::BackendTimeout | ErrorKind::BackendUnavailable
    )
}

/// GCS JSON API backend for long-term storage of large objects.
pub struct GcsBackend {
    client: reqwest::Client,
    endpoint: Url,
    bucket: String,
    token_provider: Option<PrefetchingTokenProvider>,

    change_stream: Arc<dyn ChangeStream>,
}

impl GcsBackend {
    /// Creates an authenticated GCS JSON API backend bound to the bucket in `config`.
    pub async fn new(config: GcsConfig, streams: &ChangeStreamFactory) -> anyhow::Result<Self> {
        let GcsConfig {
            endpoint,
            bucket,
            cogs,
        } = config;
        let change_stream = streams.build(cogs.as_ref());

        let token_provider = if endpoint.is_none() {
            Some(PrefetchingTokenProvider::gcp_auth(TOKEN_SCOPES).await?)
        } else {
            None
        };

        let endpoint_str = endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT);

        Ok(Self {
            client: common::reqwest_client(),
            endpoint: endpoint_str
                .parse()
                .map_err(|e| anyhow::Error::new(e).context("invalid GCS endpoint URL"))?,
            bucket,
            token_provider,
            change_stream,
        })
    }

    /// Formats the GCS object (metadata) URL for the given key.
    fn object_url(&self, id: &ObjectId) -> Result<Url> {
        let mut url = self.endpoint.clone();

        let path = id.as_storage_path().to_string();
        url.path_segments_mut()
            .map_err(|()| {
                Error::new(
                    ErrorKind::Internal,
                    format!("building GCS object URL from {}", self.endpoint),
                )
            })?
            .extend(&["storage", "v1", "b", &self.bucket, "o", &path]);

        Ok(url)
    }

    /// Formats the GCS upload URL for the given upload type.
    fn upload_url(&self, id: &ObjectId, upload_type: &str) -> Result<Url> {
        let mut url = self.endpoint.clone();

        url.path_segments_mut()
            .map_err(|()| {
                Error::new(
                    ErrorKind::Internal,
                    format!("building GCS object URL from {}", self.endpoint),
                )
            })?
            .extend(&["upload", "storage", "v1", "b", &self.bucket, "o"]);

        url.query_pairs_mut()
            .append_pair("uploadType", upload_type)
            .append_pair("name", &id.as_storage_path().to_string());

        Ok(url)
    }

    /// Formats a GCS XML API URL for the given object.
    ///
    /// Unlike [`object_url`](Self::object_url) (JSON API at
    /// `/storage/v1/b/{bucket}/o/{name}`), this produces
    /// `/{bucket}/{path_segments}` for the S3-compatible XML API used by
    /// multipart uploads.
    fn xml_object_url(&self, id: &ObjectId) -> Result<Url> {
        let mut url = self.endpoint.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                Error::new(
                    ErrorKind::Internal,
                    format!("building GCS object URL from {}", self.endpoint),
                )
            })?;
            segments.push(&self.bucket);
            for part in id.as_storage_path().to_string().split('/') {
                segments.push(part);
            }
        }
        Ok(url)
    }

    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            let token = provider.token(TOKEN_SCOPES).await.context(
                ErrorKind::BackendFailure,
                "getting GCS authentication token",
            )?;
            builder = builder.bearer_auth(token.as_str());
        }
        Ok(builder)
    }

    /// Retries a GCS request on transient errors.
    async fn with_retry<T, F>(&self, action: &'static str, f: impl Fn() -> F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send,
    {
        let mut retry_count = 0usize;
        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(ref e) if retry_count < REQUEST_RETRY_COUNT && error_is_retryable(e) => {
                    retry_count += 1;
                    objectstore_metrics::count!("gcs.retries", action = action);
                    objectstore_log::warn!(!!e, retry_count, action, "Retrying request");
                }
                Err(e) => {
                    objectstore_metrics::count!("gcs.failures", action = action);
                    return Err(e);
                }
            }
        }
    }

    /// Fetches the GCS object metadata (without the payload), bumps TTI if
    /// needed, and returns the parsed [`Metadata`].
    ///
    /// `id` is only used to attribute a TTI bump to the right record in the change stream; the
    /// request itself is addressed by `object_url`.
    #[tracing::instrument(level = "debug", fields(%object_url), skip(self))]
    async fn fetch_gcs_metadata(
        &self,
        id: &ObjectId,
        object_url: &Url,
    ) -> Result<Option<Metadata>> {
        let metadata_opt = self
            .with_retry("get_metadata", || async {
                let resp = self
                    .request(Method::GET, object_url.clone())
                    .await?
                    .send_traced()
                    .await
                    .reqwest_context("getting GCS object metadata")?;

                if resp.status() == StatusCode::NOT_FOUND {
                    resp.drain_body().await;
                    return Ok(None);
                }

                let metadata: GcsObject = resp
                    .check_error("getting GCS object metadata")
                    .await?
                    .json()
                    .await
                    .reqwest_context("getting GCS object metadata")?;

                Ok(Some(metadata))
            })
            .await?;

        let Some(gcs_metadata) = metadata_opt else {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        };

        let generation = gcs_metadata.generation.clone();
        let metageneration = gcs_metadata.metageneration.clone();
        let metadata = gcs_metadata.into_metadata()?;

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();

        // Filter already expired objects but leave them to garbage collection
        if metadata.expiration_policy.is_timeout()
            && metadata.time_expires.is_some_and(|ts| ts < access_time)
        {
            objectstore_log::debug!("Object found but past expiry");
            return Ok(None);
        }

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let Some(new_expire_at) = metadata.check_tti_bump(access_time) {
            let bumped = self
                .update_custom_time(
                    object_url.clone(),
                    new_expire_at,
                    &generation,
                    &metageneration,
                )
                .await?;

            // Only report a deadline that actually moved.
            if bumped {
                self.change_stream.update(id, Some(new_expire_at));
            }
        }

        Ok(Some(metadata))
    }

    /// Moves an object's `customTime`, which is what its lifecycle expiry is anchored to.
    ///
    /// Returns whether the update was actually applied.
    #[tracing::instrument(level = "debug", fields(%object_url), skip(self))]
    async fn update_custom_time(
        &self,
        object_url: Url,
        custom_time: SystemTime,
        generation: &str,
        metageneration: &str,
    ) -> Result<bool> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CustomTimeRequest {
            #[serde(with = "humantime_serde")]
            custom_time: SystemTime,
        }

        let mut object_url = object_url;
        object_url
            .query_pairs_mut()
            .append_pair("ifGenerationMatch", generation)
            .append_pair("ifMetagenerationMatch", metageneration);

        self.with_retry("update_custom_time", || async {
            let response = self
                .request(Method::PATCH, object_url.clone())
                .await?
                .json(&CustomTimeRequest { custom_time })
                .send_traced()
                .await
                .reqwest_context("updating GCS custom time")?;

            // Bumping TTI is opportunistic. A concurrent metadata writer won the CAS race, so
            // leave its update intact and let a future read evaluate the TTI again.
            if response.status() == StatusCode::PRECONDITION_FAILED {
                response.drain_body().await;
                return Ok(false);
            }

            response
                .check_error("updating GCS custom time")
                .await?
                .drain_body()
                .await;

            Ok(true)
        })
        .await
    }

    /// Reports an object write to the [`ChangeStream`].
    fn report_object_write(
        &self,
        id: &ObjectId,
        stored_size: Option<u64>,
        metadata_size: u64,
        expires_at: Option<SystemTime>,
    ) {
        match stored_size {
            Some(stored_size) => {
                self.change_stream
                    .write(id, stored_size + metadata_size, expires_at)
            }
            None => {
                objectstore_metrics::count!("change_stream.unreported", reason = "no_stored_size")
            }
        }
    }

    /// Queries GCS for the progress of an existing session, reporting completion to the
    /// [`ChangeStream`] if completion is detected.
    async fn query_upload_offset(
        &self,
        id: &ObjectId,
        session: &ResumableUpload,
    ) -> Result<UploadProgress> {
        self.with_retry("query_resumable_upload", || async {
            let response = self
                .request(Method::PUT, session.session_uri.as_str())
                .await?
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes */{}", session.total_length),
                )
                .send_traced()
                .await
                .reqwest_context("GCS: query resumable upload")?;
            let (progress, completed) =
                range_response_to_upload_progress(session, response).await?;
            if let Some(object) = completed {
                let stored_size = object.size.as_deref().and_then(|size| size.parse().ok());
                self.report_object_write(
                    id,
                    stored_size,
                    object.metadata_size(),
                    object.custom_time,
                );
            }
            Ok(progress)
        })
        .await
    }
}

impl fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsJsonApi")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

/// Converts GCS's inclusive `Range: bytes=0-N` acknowledgement into the next offset.
fn range_header_to_offset(value: &str, total_length: u64) -> Result<u64> {
    let end = value
        .strip_prefix("bytes=0-")
        .filter(|end| !end.is_empty() && end.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|end| end.parse::<u64>().ok())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::BackendFailure,
                "GCS: malformed resumable Range header",
            )
        })?;
    let offset = end.checked_add(1).ok_or_else(|| {
        Error::new(
            ErrorKind::BackendFailure,
            "GCS: resumable Range header overflows",
        )
    })?;
    if offset > total_length {
        return Err(Error::new(
            ErrorKind::BackendFailure,
            "GCS: incomplete resumable Range reaches declared upload length",
        ));
    }
    Ok(offset)
}

/// Interprets a Resumable Upload response, shared by chunk writes and status queries.
///
/// Returns the progress GCS reported, plus the completed object when this is the response that
/// finished the upload.
async fn range_response_to_upload_progress(
    session: &ResumableUpload,
    response: reqwest::Response,
) -> Result<(UploadProgress, Option<GcsObject>)> {
    let status = response.status();

    match status {
        StatusCode::NOT_FOUND => {
            response.drain_body().await;
            return Err(ErrorKind::UnknownUploadSession.into());
        }
        status if status == StatusCode::GONE || status.as_u16() == CLIENT_CLOSED_REQUEST_STATUS => {
            response.drain_body().await;
            return Err(ErrorKind::UploadSessionGone.into());
        }
        _ => {}
    }

    let response = response
        .check_error("GCS: unexpected resumable upload status")
        .await?;

    match status {
        StatusCode::OK | StatusCode::CREATED => {
            let body = response
                .bytes()
                .await
                .reqwest_context("GCS: read completed resumable upload response")?;
            let object = serde_json::from_slice::<GcsObject>(&body).context(
                ErrorKind::CorruptData,
                "GCS: parse completed resumable upload response",
            )?;
            Ok((UploadProgress::Complete, Some(object)))
        }
        StatusCode::PERMANENT_REDIRECT => {
            let offset = match response.headers().get(header::RANGE) {
                Some(range) => {
                    let range = range.to_str().map_err(|_| {
                        Error::new(ErrorKind::BackendFailure, "GCS: invalid Range header")
                    })?;
                    range_header_to_offset(range, session.total_length)?
                }
                // GCS omits the header entirely while it holds nothing.
                None => 0,
            };
            response.drain_body().await;
            Ok((UploadProgress::Incomplete { offset }, None))
        }
        _ => {
            response.drain_body().await;
            Err(Error::new(
                ErrorKind::BackendFailure,
                format!("GCS: unexpected resumable upload status {status}"),
            ))
        }
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        Ok(self)
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to GCS backend");
        let gcs_metadata = GcsObject::from_metadata(metadata);

        // NB: Ensure the order of these fields and that a content-type is attached to them. Both
        // are required by the GCS API.
        let metadata_json = serde_json::to_string(&gcs_metadata)
            .context(ErrorKind::Internal, "encoding GCS upload metadata")?;

        let multipart = multipart::Form::new()
            .part(
                "metadata",
                multipart::Part::text(metadata_json)
                    .mime_str("application/json")
                    .expect("application/json is a valid mime type"),
            )
            .part(
                "media",
                multipart::Part::stream(Body::wrap_stream(stream.boxed()))
                    .mime_str(&metadata.content_type)
                    .context(ErrorKind::InvalidMetadata, "encoding GCS content type")?,
            );

        // GCS requires a multipart/related request. Its body looks identical to
        // multipart/form-data, but the Content-Type header is different. Hence, we have to manually
        // set the header *after* writing the multipart form into the request.
        let content_type = format!("multipart/related; boundary={}", multipart.boundary());

        let response = self
            .request(Method::POST, self.upload_url(id, "multipart")?)
            .await?
            .multipart(multipart)
            .header(header::CONTENT_TYPE, content_type)
            .send_traced()
            .await
            .check_error("uploading a GCS object")
            .await?;

        let stored_size = read_stored_content_length(response).await;
        self.report_object_write(
            id,
            stored_size,
            gcs_metadata.metadata_size(),
            metadata.time_expires,
        );

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object(&self, id: &ObjectId, range: Option<ByteRange>) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from GCS backend");
        let object_url = self.object_url(id)?;

        let Some(metadata) = self.fetch_gcs_metadata(id, &object_url).await? else {
            return Ok(None);
        };

        let mut download_url = object_url;
        download_url.query_pairs_mut().append_pair("alt", "media");

        let payload_response = self
            .with_retry("get_payload", || async {
                let mut req = self.request(Method::GET, download_url.clone()).await?;
                if let Some(r) = range {
                    req = req.header(header::RANGE, r.to_header_value());
                }

                let resp = req
                    .send_traced()
                    .await
                    .reqwest_context("getting a GCS object payload")?;

                if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                    let raw = resp
                        .headers()
                        .get(header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok());
                    let total = raw.and_then(ContentRange::parse_unsatisfiable_total);
                    let err = match total {
                        Some(total) => ErrorKind::RangeNotSatisfiable { total }.into(),
                        None => {
                            Error::new(ErrorKind::BackendFailure, "invalid GCS 416 Content-Range")
                        }
                    };
                    resp.drain_body().await;
                    return Err(err);
                }

                resp.check_error("getting a GCS object payload").await
            })
            .await?;

        let content_range = if payload_response.status() == StatusCode::PARTIAL_CONTENT {
            Some(
                payload_response
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<ContentRange>().ok())
                    .ok_or_else(|| {
                        Error::new(ErrorKind::BackendFailure, "missing GCS 206 Content-Range")
                    })?,
            )
        } else {
            None
        };

        let stream = payload_response
            .bytes_stream()
            .map_err(io::Error::other)
            .boxed();

        Ok(Some((metadata, content_range, stream)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from GCS backend");
        let object_url = self.object_url(id)?;
        self.fetch_gcs_metadata(id, &object_url).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from GCS backend");
        let object_url = self.object_url(id)?;

        let deleted = self
            .with_retry("delete", || async {
                let resp = self
                    .request(Method::DELETE, object_url.clone())
                    .await?
                    .send_traced()
                    .await
                    .reqwest_context("deleting a GCS object")?;

                // Do not error for objects that do not exist
                if resp.status() == StatusCode::NOT_FOUND {
                    resp.drain_body().await;
                    return Ok(false);
                }

                resp.check_error("deleting a GCS object")
                    .await?
                    .drain_body()
                    .await;

                Ok(true)
            })
            .await?;

        if deleted {
            self.change_stream.delete(id);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        total_length: u64,
    ) -> Result<Option<BackendToken>> {
        objectstore_log::debug!("Creating resumable upload session on GCS backend");
        let url = self.upload_url(id, "resumable")?;
        let metadata_json = serde_json::to_vec(&GcsObject::from_metadata(metadata)).context(
            ErrorKind::Internal,
            "GCS: failed to serialize resumable upload metadata",
        )?;
        let content_type = metadata.content_type.clone();

        let location = self
            .with_retry("create_resumable_upload", || {
                let url = url.clone();
                let metadata_json = metadata_json.clone();
                let content_type = content_type.clone();
                async move {
                    let response = self
                        .request(Method::POST, url)
                        .await?
                        .header(header::CONTENT_TYPE, "application/json")
                        .header("x-upload-content-type", content_type.as_ref())
                        .header("x-upload-content-length", total_length)
                        .body(metadata_json)
                        .send_traced()
                        .await
                        .check_error("GCS: create resumable upload")
                        .await?;

                    if response.status() != StatusCode::OK {
                        let status = response.status();
                        response.drain_body().await;
                        return Err(Error::new(
                            ErrorKind::BackendFailure,
                            format!("GCS: unexpected resumable session creation status {status}"),
                        ));
                    }

                    let location = response
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::BackendFailure,
                                "GCS: resumable session response missing valid Location header",
                            )
                        })?;
                    response.drain_body().await;
                    Ok(location)
                }
            })
            .await?;

        let session_uri = Url::parse(&location).map_err(|_| {
            Error::new(
                ErrorKind::BackendFailure,
                "GCS: resumable session Location is not a valid URL",
            )
        })?;
        if session_uri.origin() != self.endpoint.origin() {
            return Err(Error::new(
                ErrorKind::BackendFailure,
                "GCS: resumable session Location has an unexpected origin",
            ));
        }

        let session = ResumableUpload::new(session_uri, total_length);
        Ok(Some(session.into_token()))
    }

    #[tracing::instrument(level = "debug", fields(?id, offset, content_length), skip_all)]
    async fn put_chunk(
        &self,
        id: &ObjectId,
        token: &BackendToken,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        objectstore_log::debug!("Uploading resumable chunk to GCS backend");
        let session = ResumableUpload::from_token(token, &self.endpoint)?;

        let end = offset
            .checked_add(content_length)
            .filter(|end| *end <= session.total_length)
            .ok_or(ErrorKind::ChunkExceedsUploadLength {
                offset,
                content_length,
                upload_length: session.total_length,
            })?;

        let content_range = match content_length {
            // Edge case: a user could create an upload with length 0 and send an empty request to
            // complete it. The correct format to finish it in that case is `*/0`.
            0 => format!("bytes */{}", session.total_length),
            _ => format!("bytes {offset}-{}/{}", end - 1, session.total_length),
        };

        let response = self
            .request(Method::PUT, session.session_uri.as_str())
            .await?
            .header(header::CONTENT_LENGTH, content_length)
            .header(header::CONTENT_RANGE, content_range)
            .body(Body::wrap_stream(stream))
            .send_traced()
            .await
            .reqwest_context("GCS: upload resumable chunk")?;

        let status = response.status();
        let progress = range_response_to_upload_progress(&session, response)
            .await
            .map(|(progress, completed)| {
                if let Some(object) = completed {
                    let stored_size = object.size.as_deref().and_then(|size| size.parse().ok());
                    self.report_object_write(
                        id,
                        stored_size,
                        object.metadata_size(),
                        object.custom_time,
                    );
                }
                progress
            });

        match progress {
            // GCS answers 400 when a chunk starts past the prefix it holds, and 416 when the
            // range is unsatisfiable. 400 is broad enough to cover other malformed requests, so
            // rather than trust the status we ask GCS where it stands: only an offset it reports
            // back turns this into a mismatch the client can act on.
            Err(error)
                if matches!(
                    status,
                    StatusCode::BAD_REQUEST | StatusCode::RANGE_NOT_SATISFIABLE
                ) =>
            {
                match self.query_upload_offset(id, &session).await {
                    Ok(UploadProgress::Incomplete { offset }) => {
                        Err(ErrorKind::UploadOffsetMismatch { offset }.into())
                    }
                    Ok(UploadProgress::Complete) => Ok(UploadProgress::Complete),
                    Err(_) => Err(error),
                }
            }
            result => result,
        }
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn upload_offset(&self, id: &ObjectId, token: &BackendToken) -> Result<UploadProgress> {
        objectstore_log::debug!("Querying resumable upload offset on GCS backend");
        let session = ResumableUpload::from_token(token, &self.endpoint)?;
        self.query_upload_offset(id, &session).await
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn cancel_upload(&self, id: &ObjectId, token: &BackendToken) -> Result<()> {
        objectstore_log::debug!("Cancelling resumable upload on GCS backend");
        let session = ResumableUpload::from_token(token, &self.endpoint)?;
        let session_uri = session.session_uri;
        self.with_retry("cancel_resumable_upload", || {
            let session_uri = session_uri.clone();
            async move {
                let response = self
                    .request(Method::DELETE, session_uri)
                    .await?
                    .send_traced()
                    .await
                    .reqwest_context("GCS: cancel resumable upload")?;
                match response.status() {
                    // Expected status code when canceling a recently created upload.
                    status if status.as_u16() == CLIENT_CLOSED_REQUEST_STATUS => {
                        response.drain_body().await;
                        Ok(())
                    }
                    // The upload was already canceled or never existed.
                    StatusCode::GONE | StatusCode::NOT_FOUND => {
                        response.drain_body().await;
                        Ok(())
                    }
                    _ => {
                        response
                            .check_error("GCS: cancel resumable upload")
                            .await?
                            .drain_body()
                            .await;
                        Err(Error::new(
                            ErrorKind::BackendFailure,
                            "GCS: unexpected resumable cancellation status",
                        ))
                    }
                }
            }
        })
        .await
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlInitiateMultipartUploadResponse {
    upload_id: String,
}

impl TryFrom<XmlInitiateMultipartUploadResponse> for InitiateMultipartResponse {
    type Error = Error;

    fn try_from(r: XmlInitiateMultipartUploadResponse) -> Result<Self> {
        Ok(UploadId::new(r.upload_id)?)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlListPartsResponse {
    #[serde(default)]
    is_truncated: bool,
    next_part_number_marker: Option<PartNumber>,
    #[serde(default, rename = "Part")]
    parts: Vec<XmlPart>,
}

impl From<XmlListPartsResponse> for ListPartsResponse {
    fn from(xml: XmlListPartsResponse) -> Self {
        Self {
            parts: xml.parts.into_iter().map(Into::into).collect(),
            is_truncated: xml.is_truncated,
            next_part_number_marker: xml.next_part_number_marker,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlPart {
    part_number: PartNumber,
    #[serde(rename = "ETag")]
    e_tag: String,
    #[serde(with = "humantime_serde")]
    last_modified: SystemTime,
    size: u64,
}

impl From<XmlPart> for crate::multipart::Part {
    fn from(p: XmlPart) -> Self {
        Self {
            part_number: p.part_number,
            etag: p.e_tag,
            last_modified: p.last_modified,
            size: p.size,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct XmlCompleteMultipartUpload {
    #[serde(rename = "Part")]
    parts: Vec<XmlCompletePart>,
}

impl From<Vec<CompletedPart>> for XmlCompleteMultipartUpload {
    fn from(parts: Vec<CompletedPart>) -> Self {
        Self {
            parts: parts.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct XmlCompletePart {
    part_number: PartNumber,
    #[serde(rename = "ETag")]
    e_tag: String,
}

impl From<CompletedPart> for XmlCompletePart {
    fn from(p: CompletedPart) -> Self {
        Self {
            part_number: p.part_number,
            e_tag: p.etag,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Error", rename_all = "PascalCase")]
struct XmlError {
    code: String,
    message: String,
}

impl From<XmlError> for crate::multipart::CompleteMultipartError {
    fn from(e: XmlError) -> Self {
        Self {
            code: e.code,
            message: e.message,
        }
    }
}

/// XXX: Any change that affects this implementation should be manually tested against real GCS.
/// That's because the fork of [storage-testbench](https://github.com/googleapis/storage-testbench)
/// that we test against has an incomplete implementation of the XML multipart API that likely doesn't match GCS's behavior in many cases.
#[async_trait::async_trait]
impl MultipartUploadBackend for GcsBackend {
    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        objectstore_log::debug!("Initiating multipart upload on GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.set_query(Some("uploads"));

        let mut headers = metadata_to_gcs_headers(metadata)?;
        headers.insert(
            header::CONTENT_TYPE,
            metadata
                .content_type
                .parse()
                .context(ErrorKind::InvalidMetadata, "encoding GCS content type")?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_static("0"),
        );

        self.with_retry("initiate_multipart", || {
            let url = url.clone();
            let headers = headers.clone();
            async move {
                let resp = self
                    .request(Method::POST, url)
                    .await?
                    .headers(headers)
                    .send_traced()
                    .await
                    .check_error("initiating a GCS multipart upload")
                    .await?;

                let body = resp
                    .bytes()
                    .await
                    .reqwest_context("reading GCS initiate-multipart response")?;

                let xml: XmlInitiateMultipartUploadResponse =
                    quick_xml::de::from_reader(body.as_ref()).context(
                        ErrorKind::CorruptData,
                        "decoding GCS initiate-multipart response",
                    )?;

                xml.try_into()
            }
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, content_md5, body))]
    async fn upload_part(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        part_number: PartNumber,
        content_length: u64,
        content_md5: Option<&str>,
        body: ClientStream,
    ) -> Result<UploadPartResponse> {
        objectstore_log::debug!("Uploading part to GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.query_pairs_mut()
            .append_pair("partNumber", &part_number.to_string())
            .append_pair("uploadId", upload_id);

        let mut builder = self
            .request(Method::PUT, url)
            .await?
            .header(header::CONTENT_LENGTH, content_length)
            .body(Body::wrap_stream(body));

        if let Some(md5) = content_md5 {
            builder = builder.header("content-md5", md5);
        }

        let resp = builder
            .send_traced()
            .await
            .check_error("uploading a GCS multipart part")
            .await?;

        let etag = resp
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::BackendFailure,
                    "GCS upload-part response missing ETag",
                )
            })?;

        resp.drain_body().await;

        Ok(etag)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_parts(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse> {
        objectstore_log::debug!("Listing parts on GCS backend");
        let mut url = self.xml_object_url(id)?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("uploadId", upload_id);
            if let Some(max) = max_parts {
                pairs.append_pair("max-parts", &max.to_string());
            }
            if let Some(marker) = part_number_marker {
                pairs.append_pair("part-number-marker", &marker.to_string());
            }
        }

        self.with_retry("list_parts", || {
            let url = url.clone();
            async move {
                let resp = self
                    .request(Method::GET, url)
                    .await?
                    .send_traced()
                    .await
                    .check_error("listing GCS multipart parts")
                    .await?;

                let body = resp
                    .bytes()
                    .await
                    .reqwest_context("reading GCS list-parts response")?;

                let xml: XmlListPartsResponse = quick_xml::de::from_reader(body.as_ref())
                    .context(ErrorKind::CorruptData, "decoding GCS list-parts response")?;

                Ok(xml.into())
            }
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn abort_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartResponse> {
        objectstore_log::debug!("Aborting multipart upload on GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.query_pairs_mut().append_pair("uploadId", upload_id);

        self.with_retry("abort_multipart", || {
            let url = url.clone();
            async move {
                let resp = self
                    .request(Method::DELETE, url)
                    .await?
                    .send_traced()
                    .await
                    .reqwest_context("aborting a GCS multipart upload")?;

                // XXX: real S3 would return 404 here if the upload has been recently completed and we
                // would have to handle it. It turns out GCS returns 204 instead, so we don't need to
                // handle that case.

                resp.check_error("aborting a GCS multipart upload")
                    .await?
                    .drain_body()
                    .await;

                Ok(())
            }
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, parts))]
    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartResponse> {
        objectstore_log::debug!("Completing multipart upload on GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.query_pairs_mut().append_pair("uploadId", upload_id);

        let body = XmlCompleteMultipartUpload::from(parts);
        let xml = quick_xml::se::to_string(&body).context(
            ErrorKind::Internal,
            "encoding GCS complete-multipart request",
        )?;

        self.with_retry("complete_multipart", || {
            let url = url.clone();
            let xml = xml.clone();
            async move {
                let resp = self
                    .request(Method::POST, url)
                    .await?
                    .header(header::CONTENT_TYPE, "application/xml")
                    .body(xml)
                    .send_traced()
                    .await
                    .check_error("completing a GCS multipart upload")
                    .await?;

                // XXX: real S3 would return 404 here if the upload has been recently completed and we
                // would have to handle it. It turns out GCS returns 200 instead, so we don't need to
                // handle that case.

                let body = resp
                    .bytes()
                    .await
                    .reqwest_context("reading GCS complete-multipart response")?;

                let error = quick_xml::de::from_reader::<_, XmlError>(body.as_ref())
                    .ok()
                    .map(Into::into);

                Ok(error)
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::time::Duration;

    use anyhow::Result;
    use objectstore_types::scope::{Scope, Scopes};
    use reqwest::header::{HeaderMap, HeaderValue};

    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::OpType;
    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::test_utils::DummyProducer;

    #[cfg(feature = "storage-cogs")]
    use crate::stream::ClientError;

    use super::*;
    use crate::id::ObjectContext;
    use crate::multipart::CompletedPart;
    use crate::stream;

    impl GcsBackend {
        async fn create_upload_session(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            total_length: u64,
        ) -> Result<BackendToken> {
            <Self as Backend>::create_upload_session(self, id, metadata, total_length)
                .await?
                .ok_or_else(|| Error::from(ErrorKind::Unsupported).into())
        }
    }

    const RESUMABLE_CHUNK_SIZE: usize = 256 * 1024;

    // NB: Not run any of these tests, you need to have a GCS emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    fn test_config() -> GcsConfig {
        GcsConfig {
            endpoint: Some("http://localhost:8087".into()),
            bucket: "test-bucket".into(),
            cogs: None,
        }
    }

    async fn create_test_backend() -> Result<GcsBackend> {
        GcsBackend::new(test_config(), &ChangeStreamFactory::default()).await
    }

    #[cfg(feature = "storage-cogs")]
    async fn create_test_backend_with_change_stream() -> Result<(GcsBackend, DummyProducer)> {
        let (streams, producer) = crate::change_stream::dummy_factory();
        let config = GcsConfig {
            cogs: Some(CostTrackerStreamConfig {
                shared_resource_id: "gcs_objectstore".into(),
                sample_rate: 1.0,
            }),
            ..test_config()
        };

        Ok((GcsBackend::new(config, &streams).await?, producer))
    }

    #[derive(Deserialize)]
    struct RetryTestResource {
        id: String,
    }

    /// Configures one storage-testbench failure and sends its ID on subsequent backend requests.
    async fn inject_retry_test(
        backend: &mut GcsBackend,
        method: &str,
        instruction: &str,
    ) -> Result<()> {
        let retry_test: RetryTestResource = reqwest::Client::new()
            .post(backend.endpoint.join("retry_test")?)
            .json(&serde_json::json!({
                "instructions": { method: [instruction] },
                "transport": "HTTP",
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut headers = HeaderMap::new();
        headers.insert("x-retry-test-id", HeaderValue::from_str(&retry_test.id)?);
        backend.client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(())
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    fn make_id_with_key(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".into(),
                scopes: Scopes::from_iter([
                    Scope::create("organization", "42").unwrap(),
                    Scope::create("project", "7").unwrap(),
                ]),
            },
            key.into(),
        )
    }

    #[test]
    fn resumable_backend_token_round_trips_and_validates_uri() -> Result<()> {
        let endpoint = Url::parse("https://example.invalid")?;
        let session_uri = "https://example.invalid/opaque/session?arbitrary=value";
        let token = ResumableUpload::new(Url::parse(session_uri)?, 123).into_token();
        let decoded = ResumableUpload::from_token(&token, &endpoint)?;
        assert_eq!(decoded.session_uri.as_str(), session_uri);
        assert_eq!(decoded.total_length, 123);

        let wrong_origin =
            ResumableUpload::new(Url::parse("https://other.invalid/session")?, 123).into_token();
        for malformed in [
            "missing-delimiter".to_owned(),
            "not-a-length.https://example.invalid/session".to_owned(),
            "123.not-a-url".to_owned(),
            wrong_origin,
        ] {
            assert!(matches!(
                ResumableUpload::from_token(&malformed, &endpoint),
                Err(error) if error.kind() == ErrorKind::UnknownUploadSession
            ));
        }
        Ok(())
    }

    #[test]
    fn resumable_range_reports_next_offset_and_rejects_malformed_values() -> Result<()> {
        assert_eq!(range_header_to_offset("bytes=0-0", 10)?, 1);
        assert_eq!(range_header_to_offset("bytes=0-262143", 300_000)?, 262_144);

        for malformed in [
            "",
            "bytes=1-2",
            "bytes=0-",
            "bytes=0-*",
            "bytes=0-9 ",
            "bytes=0-9,bytes=20-30",
        ] {
            assert!(
                range_header_to_offset(malformed, 100).is_err(),
                "accepted {malformed:?}"
            );
        }
        assert_eq!(range_header_to_offset("bytes=0-9", 10)?, 10);
        assert!(range_header_to_offset("bytes=0-18446744073709551615", u64::MAX).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_zero_length_upload_and_metadata() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-zero");
        let metadata = Metadata {
            content_type: "application/x-empty".into(),
            custom: BTreeMap::from([("upload".into(), "zero".into())]),
            ..Default::default()
        };

        let token = backend.create_upload_session(&id, &metadata, 0).await?;
        assert_eq!(
            backend.upload_offset(&id, &token).await?,
            UploadProgress::Complete
        );

        let (stored_metadata, _, payload) = backend.get_object(&id, None).await?.unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, b"");
        assert_eq!(stored_metadata.content_type, metadata.content_type);
        assert_eq!(stored_metadata.custom, metadata.custom);

        // An empty chunk is how a client uploads a zero-length object, so it has to finalize the
        // session rather than be rejected or send a `Content-Range` naming a byte that is absent.
        let chunked_id = make_id_with_key("resumable-zero-chunk");
        let token = backend
            .create_upload_session(&chunked_id, &metadata, 0)
            .await?;
        assert_eq!(
            backend
                .put_chunk(&chunked_id, &token, 0, 0, stream::single(Vec::new()))
                .await?,
            UploadProgress::Complete
        );
        let (_, _, payload) = backend.get_object(&chunked_id, None).await?.unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, b"");
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_empty_chunk_reports_offset_without_writing() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-empty-chunk");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 4)
            .await?;

        // An empty chunk cannot advance a session that still expects bytes. It reports the
        // authoritative offset instead of failing, and leaves what GCS holds untouched.
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, 0, stream::single(Vec::new()))
                .await?,
            UploadProgress::Incomplete { offset: 0 }
        );
        let after_write = backend
            .put_chunk(&id, &token, 0, 2, stream::single(b"ab".to_vec()))
            .await?;
        assert!(matches!(after_write, UploadProgress::Incomplete { .. }));

        // Whichever prefix GCS acknowledged, an empty chunk reports that same position rather
        // than moving it. GCS documents that a chunk "should be a multiple of 256 KiB ... unless
        // it's the last chunk", and that a client "should not assume that the server received all
        // bytes sent in any given request". The emulator acknowledges any length, so the position
        // itself is not asserted here.
        assert_eq!(
            backend
                .put_chunk(&id, &token, 2, 0, stream::single(Vec::new()))
                .await?,
            after_write
        );
        assert_eq!(backend.upload_offset(&id, &token).await?, after_write);
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_single_and_multi_chunk_uploads() -> Result<()> {
        let backend = create_test_backend().await?;

        let single_id = make_id_with_key("resumable-single");
        let single = b"single chunk".to_vec();
        let token = backend
            .create_upload_session(&single_id, &Metadata::default(), single.len() as u64)
            .await?;
        assert_eq!(
            backend
                .put_chunk(
                    &single_id,
                    &token,
                    0,
                    single.len() as u64,
                    stream::single(single.clone()),
                )
                .await?,
            UploadProgress::Complete
        );
        let (_, _, payload) = backend.get_object(&single_id, None).await?.unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, single);

        let multi_id = make_id_with_key("resumable-multi");
        let mut expected = vec![b'a'; RESUMABLE_CHUNK_SIZE];
        expected.extend_from_slice(b"final");
        let token = backend
            .create_upload_session(&multi_id, &Metadata::default(), expected.len() as u64)
            .await?;
        assert_eq!(
            backend.upload_offset(&multi_id, &token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );
        assert_eq!(
            backend
                .put_chunk(
                    &multi_id,
                    &token,
                    0,
                    RESUMABLE_CHUNK_SIZE as u64,
                    stream::single(expected[..RESUMABLE_CHUNK_SIZE].to_vec()),
                )
                .await?,
            UploadProgress::Incomplete {
                offset: RESUMABLE_CHUNK_SIZE as u64
            }
        );
        assert_eq!(
            backend.upload_offset(&multi_id, &token).await?,
            UploadProgress::Incomplete {
                offset: RESUMABLE_CHUNK_SIZE as u64
            }
        );
        assert_eq!(
            backend
                .put_chunk(
                    &multi_id,
                    &token,
                    RESUMABLE_CHUNK_SIZE as u64,
                    5,
                    stream::single(b"final".to_vec()),
                )
                .await?,
            UploadProgress::Complete
        );
        let (_, _, payload) = backend.get_object(&multi_id, None).await?.unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_unaligned_chunk_and_rewind_preserve_persisted_bytes() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-rewind");
        let total_length = RESUMABLE_CHUNK_SIZE + 3;
        let token = backend
            .create_upload_session(&id, &Metadata::default(), total_length as u64)
            .await?;

        let prefix = vec![b'a'; RESUMABLE_CHUNK_SIZE];
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, prefix.len() as u64, stream::single(prefix),)
                .await?,
            UploadProgress::Incomplete {
                offset: RESUMABLE_CHUNK_SIZE as u64
            }
        );

        // Resend three already-persisted positions with different bytes. GCS ignores that overlap
        // without comparing it, then appends the suffix from its authoritative offset.
        assert_eq!(
            backend
                .put_chunk(
                    &id,
                    &token,
                    (RESUMABLE_CHUNK_SIZE - 3) as u64,
                    6,
                    stream::single(b"BADxyz".to_vec()),
                )
                .await?,
            UploadProgress::Complete
        );

        let (_, _, payload) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(payload).await?;
        assert_eq!(&payload[RESUMABLE_CHUNK_SIZE - 3..], b"aaaxyz");
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_rejects_oversized_chunks() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-validation");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 10)
            .await?;

        let error = backend
            .put_chunk(&id, &token, 8, 3, stream::single(b"abc".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::ChunkExceedsUploadLength {
                offset: 8,
                content_length: 3,
                upload_length: 10
            }
        );

        let error = backend
            .put_chunk(&id, &token, u64::MAX, 2, stream::single(b"ab".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::ChunkExceedsUploadLength { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_cancel_is_idempotent_and_session_is_not_found() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-cancel");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 10)
            .await?;

        backend.cancel_upload(&id, &token).await?;
        backend.cancel_upload(&id, &token).await?;
        assert!(matches!(
            backend.upload_offset(&id, &token).await,
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_retries_only_replayable_operations() -> Result<()> {
        let mut backend = create_test_backend().await?;
        inject_retry_test(&mut backend, "storage.objects.insert", "return-503").await?;
        let id = make_id_with_key("resumable-retries");

        // Creation consumes the injected 503 and succeeds on the backend's retry.
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 10)
            .await?;

        inject_retry_test(&mut backend, "storage.objects.insert", "return-503").await?;
        assert_eq!(
            backend.upload_offset(&id, &token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );

        inject_retry_test(&mut backend, "storage.objects.delete", "return-503").await?;
        backend.cancel_upload(&id, &token).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_stream_failures_require_explicit_offset_recovery() -> Result<()> {
        // A failure before persistence is returned directly: put_chunk must not retry the body.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-before");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 4)
            .await?;
        inject_retry_test(&mut backend, "storage.objects.insert", "return-503").await?;
        assert!(matches!(
            backend
                .put_chunk(&id, &token, 0, 4, stream::single(b"data".to_vec()))
                .await,
            Err(error) if error.kind() == ErrorKind::BackendUnavailable
        ));
        assert_eq!(
            backend.upload_offset(&id, &token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );

        // The emulator persists the first KiB, then fails. Recovery observes that prefix and the
        // caller resumes exactly from the returned offset.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-partial");
        let data = vec![b'p'; 2048];
        let token = backend
            .create_upload_session(&id, &Metadata::default(), data.len() as u64)
            .await?;
        inject_retry_test(
            &mut backend,
            "storage.objects.insert",
            "return-503-after-1K",
        )
        .await?;
        assert!(matches!(
            backend
                .put_chunk(
                    &id,
                    &token,
                    0,
                    data.len() as u64,
                    stream::single(data.clone()),
                )
                .await,
            Err(error) if error.kind() == ErrorKind::BackendUnavailable
        ));
        assert_eq!(
            backend.upload_offset(&id, &token).await?,
            UploadProgress::Incomplete { offset: 1024 }
        );
        assert_eq!(
            backend
                .put_chunk(
                    &id,
                    &token,
                    1024,
                    1024,
                    stream::single(data[1024..].to_vec()),
                )
                .await?,
            UploadProgress::Complete
        );

        // GCS persists the final bytes, but storage-testbench truncates the successful JSON
        // response. The chunk is not retried; an explicit status query observes completion.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-final");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), 5)
            .await?;
        inject_retry_test(
            &mut backend,
            "storage.objects.insert",
            "return-broken-stream-final-chunk-after-0B",
        )
        .await?;
        assert!(matches!(
            backend
                .put_chunk(&id, &token, 0, 5, stream::single(b"final".to_vec()))
                .await,
            Err(error) if error.kind() == ErrorKind::CorruptData
        ));
        assert_eq!(
            backend.upload_offset(&id, &token).await?,
            UploadProgress::Complete
        );
        Ok(())
    }

    async fn get_generation_matches(
        backend: &GcsBackend,
        object_url: Url,
    ) -> Result<(String, String)> {
        Ok(backend
            .request(Method::GET, object_url)
            .await?
            .send_traced()
            .await
            .check_error("getting GCS object metadata")
            .await?
            .json::<GcsObject>()
            .await
            .context(
                ErrorKind::BackendFailure,
                "decoding GCS object metadata response",
            )
            .map(|object| (object.generation, object.metageneration))?)
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            origin: Some("203.0.113.42".into()),
            filename: Some("hello.txt".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            time_created: Some(SystemTime::now()),
            time_expires: None,
            size: None,
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.filename, metadata.filename);
        assert_eq!(meta.custom, metadata.custom);
        assert!(metadata.time_created.is_some());

        Ok(())
    }

    /// Metadata with a non-ASCII filename and custom metadata value.
    fn unicode_metadata() -> Metadata {
        Metadata {
            filename: Some("réport-📄.pdf".into()),
            custom: BTreeMap::from_iter([("release".into(), "vérsion-1.0-🚀".into())]),
            ..Default::default()
        }
    }

    /// Both GCS write paths must agree on how a logical string is stored, because every read goes
    /// through the JSON API: `put_object` writes metadata as JSON, `initiate_multipart` writes it
    /// as `x-goog-meta-*` request headers.
    #[tokio::test]
    async fn test_unicode_metadata_roundtrip_json_upload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = unicode_metadata();

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.filename, metadata.filename);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_unicode_metadata_roundtrip_multipart_upload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = unicode_metadata();

        multipart_put(&backend, &id, &metadata, "hello, world").await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.filename, metadata.filename);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[test]
    fn from_metadata_uses_provided_time_expires() {
        let expires = SystemTime::now() + Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(expires),
            ..Default::default()
        };

        let gcs_object = GcsObject::from_metadata(&metadata);
        assert_eq!(gcs_object.custom_time, Some(expires));

        let roundtripped = gcs_object.into_metadata().unwrap();
        assert_eq!(roundtripped.time_expires, Some(expires));
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend.delete_object(&id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("world"))
            .await?;

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        backend.delete_object(&id).await?;

        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(SystemTime::now()),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            time_expires: Some(SystemTime::now()),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_returns_metadata() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            origin: Some("203.0.113.42".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_metadata(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_bumps_tti() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let tti = Duration::from_hours(2 * 24);
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(SystemTime::now() + tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // Backdate custom_time so it falls inside the bump window.
        let object_url = backend.object_url(&id)?;
        let old_deadline = SystemTime::now() + Duration::from_mins(1);
        let (generation, metageneration) =
            get_generation_matches(&backend, object_url.clone()).await?;
        backend
            .update_custom_time(object_url, old_deadline, &generation, &metageneration)
            .await?;

        // First get_metadata sees the old timestamp and triggers a TTI bump.
        let pre_meta = backend.get_metadata(&id).await?.unwrap();
        let pre_expiry = pre_meta.time_expires.unwrap();

        // Second get_metadata sees the bumped timestamp.
        let post_meta = backend.get_metadata(&id).await?.unwrap();
        let post_expiry = post_meta.time_expires.unwrap();
        assert!(
            post_expiry > pre_expiry,
            "TTI bump should have extended the expiry: {pre_expiry:?} -> {post_expiry:?}"
        );

        // Verify the payload is still intact after the bump.
        let (_, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(&payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_does_not_bump_fresh_tti() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let tti = Duration::from_hours(2 * 24);
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(SystemTime::now() + tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // A freshly written object has time_expires ≈ now + 2d, which is well outside
        // the bump window (now + 2d - 1d = now + 1d). No bump should occur.
        let first = backend.get_metadata(&id).await?.unwrap();
        let first_expiry = first.time_expires.unwrap();

        let second = backend.get_metadata(&id).await?.unwrap();
        let second_expiry = second.time_expires.unwrap();

        assert_eq!(
            first_expiry, second_expiry,
            "Fresh TTI object should not have its expiry bumped"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_short_tti_bumps() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let tti = Duration::from_hours(2);
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(SystemTime::now() + tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // Backdate custom_time so it falls inside the bump window.
        let object_url = backend.object_url(&id)?;
        let old_deadline = SystemTime::now() + Duration::from_mins(1);
        let (generation, metageneration) =
            get_generation_matches(&backend, object_url.clone()).await?;
        backend
            .update_custom_time(object_url, old_deadline, &generation, &metageneration)
            .await?;

        // First get_metadata triggers the bump.
        let pre_meta = backend.get_metadata(&id).await?.unwrap();
        let pre_expiry = pre_meta.time_expires.unwrap();

        // Second get_metadata sees the bumped timestamp.
        let post_meta = backend.get_metadata(&id).await?.unwrap();
        let post_expiry = post_meta.time_expires.unwrap();
        assert!(
            post_expiry > pre_expiry,
            "Short TTI bump should have extended the expiry: {pre_expiry:?} -> {post_expiry:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compressed_payload_roundtrip() -> Result<()> {
        use objectstore_types::metadata::Compression;

        let backend = create_test_backend().await?;

        let plaintext = b"hello, world (but compressed with zstd)";
        let compressed = zstd::encode_all(&plaintext[..], 3)?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Zstd),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single(compressed.clone()))
            .await?;

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Zstd));
        assert_eq!(
            payload, compressed,
            "Payload should be returned still compressed, not auto-decompressed"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_single_part() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_mins(33)),
            origin: Some("203.0.113.42".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        let upload_id = backend.initiate_multipart(&id, &metadata).await?;

        let data = b"hello, multipart world!";
        let etag = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                data.len() as u64,
                None,
                stream::single(data.to_vec()),
            )
            .await?;

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
            )
            .await?;
        assert!(result.is_none(), "expected no error on complete");

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(payload, data);
        assert_eq!(meta.content_type, "text/plain".to_string());
        assert_eq!(
            meta.expiration_policy,
            ExpirationPolicy::TimeToLive(Duration::from_mins(33))
        );
        assert_eq!(meta.origin, Some("203.0.113.42".into()));
        assert_eq!(
            meta.custom,
            BTreeMap::from_iter([("hello".into(), "world".into())])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_multiple_parts() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await?;

        // Non-final parts must be >= 5 MiB.
        const MIN_PART: usize = 5 * 1024 * 1024;
        let part1 = vec![b'a'; MIN_PART];
        let part2 = vec![b'b'; MIN_PART];
        let part3 = b"cccc".to_vec();

        let etag1 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                part1.len() as u64,
                None,
                stream::single(part1.clone()),
            )
            .await?;
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(2).unwrap(),
                part2.len() as u64,
                None,
                stream::single(part2.clone()),
            )
            .await?;
        let etag3 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(3).unwrap(),
                part3.len() as u64,
                None,
                stream::single(part3.clone()),
            )
            .await?;

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![
                    CompletedPart {
                        part_number: NonZeroU32::new(1).unwrap(),
                        etag: etag1,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(2).unwrap(),
                        etag: etag2,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(3).unwrap(),
                        etag: etag3,
                    },
                ],
            )
            .await?;
        assert!(result.is_none(), "expected no error on complete");

        // Object exists after complete
        let (_meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        let mut expected = Vec::new();
        expected.extend_from_slice(&part1);
        expected.extend_from_slice(&part2);
        expected.extend_from_slice(&part3);
        assert_eq!(payload, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_out_of_order_upload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await?;

        // Non-final parts must be >= 5 MiB.
        const MIN_PART: usize = 5 * 1024 * 1024;
        let part1 = vec![b'a'; MIN_PART];
        let part2 = vec![b'b'; MIN_PART];
        let part3 = b"cccc".to_vec();

        // Upload parts out of order: 2, 3, 1.
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(2).unwrap(),
                part2.len() as u64,
                None,
                stream::single(part2.clone()),
            )
            .await?;
        let etag3 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(3).unwrap(),
                part3.len() as u64,
                None,
                stream::single(part3.clone()),
            )
            .await?;
        let etag1 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                part1.len() as u64,
                None,
                stream::single(part1.clone()),
            )
            .await?;

        // Complete with parts listed in order.
        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![
                    CompletedPart {
                        part_number: NonZeroU32::new(1).unwrap(),
                        etag: etag1,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(2).unwrap(),
                        etag: etag2,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(3).unwrap(),
                        etag: etag3,
                    },
                ],
            )
            .await?;
        assert!(result.is_none(), "expected no error on complete");

        // Verify reassembly order matches part numbers, not upload order.
        let (_meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        let mut expected = Vec::new();
        expected.extend_from_slice(&part1);
        expected.extend_from_slice(&part2);
        expected.extend_from_slice(&part3);
        assert_eq!(payload, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_list_parts() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await?;

        let etag1 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                3,
                None,
                stream::single(b"aaa".to_vec()),
            )
            .await?;
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(2).unwrap(),
                3,
                None,
                stream::single(b"bbb".to_vec()),
            )
            .await?;

        // List all parts.
        let list = backend.list_parts(&id, &upload_id, None, None).await?;
        assert_eq!(list.parts.len(), 2);
        assert_eq!(list.parts[0].part_number.get(), 1);
        assert_eq!(list.parts[0].etag, etag1);
        assert_eq!(list.parts[0].size, 3);
        assert_eq!(list.parts[1].part_number.get(), 2);
        assert_eq!(list.parts[1].etag, etag2);
        assert_eq!(list.parts[1].size, 3);

        // List with max_parts=1 to test pagination.
        let page1 = backend.list_parts(&id, &upload_id, Some(1), None).await?;
        assert_eq!(page1.parts.len(), 1);
        assert_eq!(page1.parts[0].part_number.get(), 1);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        let page2 = backend
            .list_parts(&id, &upload_id, Some(1), page1.next_part_number_marker)
            .await?;
        assert_eq!(page2.parts.len(), 1);
        assert_eq!(page2.parts[0].part_number.get(), 2);

        // Clean up.
        backend.abort_multipart(&id, &upload_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_abort() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await?;

        backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                5,
                None,
                stream::single(b"hello".to_vec()),
            )
            .await?;

        backend.abort_multipart(&id, &upload_id).await?;

        // Object should not exist after abort.
        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none(), "object should not exist after abort");

        // A second abort should still succeed (idempotent 404 handling).
        backend.abort_multipart(&id, &upload_id).await?;

        Ok(())
    }

    async fn multipart_put(
        backend: &GcsBackend,
        id: &ObjectId,
        metadata: &Metadata,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<()> {
        let payload: bytes::Bytes = payload.into();
        let upload_id = backend.initiate_multipart(id, metadata).await?;
        let etag = backend
            .upload_part(
                id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                payload.len() as u64,
                None,
                stream::single(payload),
            )
            .await?;
        let error = backend
            .complete_multipart(
                id,
                &upload_id,
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
            )
            .await?;
        assert!(
            error.is_none(),
            "complete_multipart returned error: {error:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_ttl_immediate() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(SystemTime::now()),
            ..Default::default()
        };

        multipart_put(&backend, &id, &metadata, "hello, world").await?;

        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_tti_immediate() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            time_expires: Some(SystemTime::now()),
            ..Default::default()
        };

        multipart_put(&backend, &id, &metadata, "hello, world").await?;

        let result = backend.get_object(&id, None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_compressed_payload_roundtrip() -> Result<()> {
        use objectstore_types::metadata::Compression;

        let backend = create_test_backend().await?;

        let plaintext = b"hello, world (but compressed with zstd)";
        let compressed = zstd::encode_all(&plaintext[..], 3)?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Zstd),
            ..Default::default()
        };

        multipart_put(&backend, &id, &metadata, compressed.clone()).await?;

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Zstd));
        assert_eq!(
            payload, compressed,
            "Payload should be returned still compressed, not auto-decompressed"
        );

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_the_size_gcs_stored() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let payload = vec![b'x'; 4096];
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            time_expires: Some(SystemTime::now() + Duration::from_secs(3600)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<ClientError>(payload.clone()),
            )
            .await?;

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].shared_resource_id, "gcs_objectstore");
        assert_eq!(records[0].app_feature, "testing");
        assert_eq!(
            records[0].size,
            Some(payload.len() as u64 + GcsObject::from_metadata(&metadata).metadata_size())
        );
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn resumable_completion_reports_to_change_stream() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let payload = b"resumable payload".to_vec();
        let metadata = Metadata {
            time_expires: Some(SystemTime::now() + Duration::from_secs(3600)),
            ..Default::default()
        };
        let token = backend
            .create_upload_session(&id, &metadata, payload.len() as u64)
            .await?;

        assert_eq!(
            backend
                .put_chunk(
                    &id,
                    &token,
                    0,
                    payload.len() as u64,
                    stream::single::<ClientError>(payload.clone()),
                )
                .await?,
            UploadProgress::Complete
        );

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(
            records[0].size,
            Some(payload.len() as u64 + GcsObject::from_metadata(&metadata).metadata_size())
        );
        assert!(records[0].expiration_time.is_some());
        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_size_includes_metadata_keys_and_values() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let payload = b"tiny".to_vec();

        let bare = Metadata::default();
        backend
            .put_object(
                &make_id(),
                &bare,
                stream::single::<ClientError>(payload.clone()),
            )
            .await?;

        let annotated = Metadata {
            custom: BTreeMap::from_iter([("a-fairly-long-metadata-key".into(), "value".into())]),
            ..Default::default()
        };
        backend
            .put_object(
                &make_id(),
                &annotated,
                stream::single::<ClientError>(payload.clone()),
            )
            .await?;

        let records = producer.records();
        assert_eq!(records.len(), 2);

        let bare_size = records[0].size.unwrap();
        let annotated_size = records[1].size.unwrap();
        assert_eq!(
            bare_size,
            payload.len() as u64,
            "default metadata contributes no custom keys"
        );
        assert!(
            annotated_size > bare_size,
            "same payload, more metadata: {annotated_size} should exceed {bare_size}"
        );

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_nothing_when_the_object_was_already_gone() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;

        backend.delete_object(&make_id()).await?;

        assert!(producer.records().is_empty());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_deletes() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();

        backend
            .put_object(
                &id,
                &Metadata::default(),
                stream::single::<ClientError>(b"hi".to_vec()),
            )
            .await?;
        producer.clear();

        backend.delete_object(&id).await?;

        let records = producer.records();
        assert_eq!(records.len(), 1, "a retried delete must report only once");
        assert_eq!(records[0].op_type, OpType::Delete);
        assert_eq!(records[0].size, None);

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_tti_bump_as_an_update() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_expires: Some(SystemTime::now() + Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<ClientError>(b"hi".to_vec()),
            )
            .await?;
        producer.clear();

        backend.get_metadata(&id).await?;

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Update);
        assert_eq!(records[0].size, None);
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_nothing_when_tti_is_not_bumped() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_expires: Some(SystemTime::now() + Duration::from_secs(3600)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<ClientError>(b"hi".to_vec()),
            )
            .await?;
        producer.clear();

        backend.get_metadata(&id).await?;

        assert!(producer.records().is_empty());

        Ok(())
    }
}
