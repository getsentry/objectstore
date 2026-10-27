//! Google Cloud Storage backend for long-term storage of large objects.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use gcp_auth::TokenProvider;
use objectstore_types::headers;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};
use objectstore_types::time::{Rfc3339Timestamp, Timestamp};
use reqwest::header::HeaderMap;
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode, Url, header, multipart};
use serde::{Deserialize, Serialize};

use crate::backend::common::{
    self, Backend, DeleteResponse, ExpiryUpdate, GetResponse, MetadataResponse, PutResponse,
    SetExpiryResponse,
};
use crate::backend::extensions::{ReqwestResultExt, ResponseExt, SendTraced};
use crate::change_stream::{
    ChangeStream, ChangeStreamFactory, CostTrackerStreamConfig, flush_change_stream,
};
use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::gcp_auth::PrefetchingTokenProvider;
use crate::id::ObjectId;
use crate::resumable::{BackendToken, Session, UploadProgress};
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

    /// Maximum time TCP data may remain unacknowledged or blocked before failing the request.
    ///
    /// This detects stalled transports. It does not limit the total upload duration or the time
    /// waiting for a response after the upload is acknowledged. Applied on Linux and ignored on
    /// other platforms. `0s` uses the operating system's default.
    ///
    /// # Default
    ///
    /// `30s`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TCP_USER_TIMEOUT=30s`
    #[serde(default = "common::default_tcp_user_timeout", with = "humantime_serde")]
    pub tcp_user_timeout: Duration,

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_time: Option<Rfc3339Timestamp>,

    /// The `Content-Length` of the data in bytes. GCS returns this as a string.
    ///
    /// GCS sets this in metadata responses. We can use it to know the size of an object
    /// without having to stream it.
    pub size: Option<String>,

    /// Timestamp of when this object was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_created: Option<Rfc3339Timestamp>,

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

    /// Returns `true` if the object is expired at the given access time.
    pub fn is_expired(&self, access_time: Timestamp) -> bool {
        match self.custom_time {
            Some(expires_at) => access_time > expires_at.into_inner(),
            None => false,
        }
    }

    /// Returns the generation and metageneration of this object.
    pub fn generations(&self) -> GcsGenerations<'_> {
        (&self.generation, &self.metageneration)
    }

    /// Converts our Metadata type to GCS JSON object metadata.
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let mut gcs_object = GcsObject {
            content_type: metadata.content_type.clone(),
            size: metadata.size.map(|size| size.to_string()),
            content_encoding: None,
            custom_time: None,
            time_created: metadata.time_created.map(Timestamp::as_rfc3339),
            metadata: BTreeMap::new(),
            generation: String::new(),
            metageneration: String::new(),
        };

        // For time-based expiration, set the `customTime` field. The bucket must have a
        // `daysSinceCustomTime` lifecycle rule configured to delete objects with this field set.
        // This rule automatically skips objects without `customTime` set.
        gcs_object.custom_time = metadata.time_expires.map(Timestamp::as_rfc3339);

        if let Some(compression) = metadata.compression {
            gcs_object.content_encoding = Some(compression.to_string());
        }

        if metadata.expiration_policy != ExpirationPolicy::default() {
            gcs_object.metadata.insert(
                GcsMetaKey::Expiration,
                metadata.expiration_policy.to_string(),
            );
        }

        // Preserve the stored escaping convention for free-form metadata strings.
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
        let time_created = self.time_created.map(Rfc3339Timestamp::into_inner);

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
            time_expires: self.custom_time.map(Rfc3339Timestamp::into_inner),
        })
    }
}

/// The object and metadata versions of a GCS object, used for conditional updates.
type GcsGenerations<'a> = (&'a str, &'a str);

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

/// Decodes a stored GCS metadata value into its logical string.
fn decode_gcs_meta_value(value: &str, context: &'static str) -> Result<String> {
    headers::decode_header_str(value).context(ErrorKind::CorruptData, context)
}

/// Special status code returned by GCS when a Resumable Upload is canceled successfully or when
/// making other requests to a session that was recently canceled.
const CLIENT_CLOSED_REQUEST_STATUS: u16 = 499;

enum GcsUploadProgress {
    Incomplete(u64),
    Complete(GcsObject),
}

impl From<GcsUploadProgress> for UploadProgress {
    fn from(value: GcsUploadProgress) -> Self {
        match value {
            GcsUploadProgress::Incomplete(offset) => UploadProgress::Incomplete { offset },
            GcsUploadProgress::Complete(_) => UploadProgress::Complete,
        }
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
            tcp_user_timeout,
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
            client: common::reqwest_client(tcp_user_timeout),
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

    /// Counts a terminal request-operation error without retrying the future.
    async fn with_failure_metrics<T>(
        &self,
        action: &'static str,
        future: impl Future<Output = Result<T>> + Send,
    ) -> Result<T> {
        future.await.inspect_err(|_| {
            objectstore_metrics::count!("gcs.failures", action = action);
        })
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

    /// Fetches GCS object metadata without modifying the object.
    #[tracing::instrument(level = "debug", fields(%object_url), skip(self))]
    async fn get_gcs_metadata(
        &self,
        object_url: &Url,
        access_time: Timestamp,
    ) -> Result<Option<GcsObject>> {
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

        // Filter already expired objects but leave them to garbage collection
        if gcs_metadata.is_expired(access_time) {
            objectstore_log::debug!("Object found but past expiry");
            return Ok(None);
        }

        Ok(Some(gcs_metadata))
    }

    /// Moves an object's `customTime` and optionally updates its metadata in the same PATCH.
    ///
    /// Lifecycle expiry is anchored to `customTime`; TTL updates accompany that deadline.
    ///
    /// Returns the expiry update outcome, distinguishing absence from a conflict.
    #[tracing::instrument(level = "debug", fields(%object_url), skip(self))]
    async fn update_custom_time(
        &self,
        object_url: Url,
        custom_time: Timestamp,
        generations: GcsGenerations<'_>,
        metadata: Option<BTreeMap<GcsMetaKey, String>>,
    ) -> Result<SetExpiryResponse> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CustomTimeRequest {
            custom_time: Rfc3339Timestamp,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: Option<BTreeMap<GcsMetaKey, String>>,
        }

        let mut object_url = object_url;
        object_url
            .query_pairs_mut()
            .append_pair("ifGenerationMatch", generations.0)
            .append_pair("ifMetagenerationMatch", generations.1);

        let request = CustomTimeRequest {
            custom_time: custom_time.as_rfc3339(),
            metadata,
        };

        self.with_retry("update_custom_time", || async {
            let response = self
                .request(Method::PATCH, object_url.clone())
                .await?
                .json(&request)
                .send_traced()
                .await
                .reqwest_context("updating GCS custom time")?;

            let outcome = match response.status() {
                StatusCode::NOT_FOUND => Some(SetExpiryResponse::NotFound),
                StatusCode::PRECONDITION_FAILED => Some(SetExpiryResponse::Rejected),
                _ => None,
            };
            if let Some(outcome) = outcome {
                response.drain_body().await;
                return Ok(outcome);
            }

            response
                .check_error("updating GCS custom time")
                .await?
                .drain_body()
                .await;

            Ok(SetExpiryResponse::Satisfied(custom_time))
        })
        .await
    }

    /// Reports a committed object write to the [`ChangeStream`].
    async fn report_object_write(
        &self,
        id: &ObjectId,
        stored_size: Option<u64>,
        metadata_size: u64,
        expires_at: Option<Timestamp>,
    ) {
        match stored_size {
            Some(stored_size) => {
                self.change_stream
                    .commit_write(id, stored_size + metadata_size, expires_at)
                    .await
            }
            None => {
                objectstore_metrics::count!("change_stream.unreported", reason = "no_stored_size")
            }
        }
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
fn range_header_to_offset(value: &str, upload_length: NonZeroU64) -> Result<u64> {
    let end = value.strip_prefix("bytes=0-").ok_or_else(|| {
        Error::new(
            ErrorKind::Internal,
            "malformed GCS Range header for resumable progress",
        )
    })?;
    let end = end.parse::<u64>().context(
        ErrorKind::Internal,
        "invalid GCS Range header for resumable progress",
    )?;

    let offset = end
        .checked_add(1)
        .ok_or_else(|| Error::new(ErrorKind::Internal, "GCS offset overflows u64"))?;
    if offset > upload_length.get() {
        return Err(Error::new(
            ErrorKind::Internal,
            "GCS offset exceeds upload length",
        ));
    }
    Ok(offset)
}

/// Interprets a Resumable Upload response, shared by chunk writes and status queries.
///
/// Returns the progress GCS reported, plus the completed object when this is the response that
/// finished the upload.
async fn range_response_to_upload_progress(
    upload_length: NonZeroU64,
    response: reqwest::Response,
) -> Result<GcsUploadProgress> {
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
        .check_error("processing a GCS resumable upload response")
        .await?;

    match status {
        StatusCode::OK | StatusCode::CREATED => {
            let body = response
                .bytes()
                .await
                .reqwest_context("reading a completed GCS resumable upload response")?;
            let object = serde_json::from_slice::<GcsObject>(&body).context(
                ErrorKind::CorruptData,
                "parsing a completed GCS resumable upload response",
            )?;
            Ok(GcsUploadProgress::Complete(object))
        }
        // GCS calls it "308 Resume Incomplete"
        StatusCode::PERMANENT_REDIRECT => {
            let offset = match response.headers().get(header::RANGE) {
                Some(range) => {
                    let range = range.to_str().map_err(|_| {
                        Error::new(
                            ErrorKind::BackendFailure,
                            "invalid GCS resumable upload Range header",
                        )
                    })?;
                    range_header_to_offset(range, upload_length)?
                }
                // GCS omits this header while it holds nothing
                None => 0,
            };
            response.drain_body().await;
            Ok(GcsUploadProgress::Incomplete(offset))
        }
        _ => {
            response.drain_body().await;
            Err(Error::new(
                ErrorKind::BackendFailure,
                format!("unexpected GCS resumable upload status {status}"),
            ))
        }
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    fn upload_granularity(&self) -> u64 {
        256 * 1024
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to GCS backend");
        self.change_stream
            .begin_write(id, metadata.time_expires)
            .await?;
        let gcs_metadata = GcsObject::from_metadata(metadata);

        // NB: Ensure the order of these fields and that a content-type is attached to them. Both
        // are required by the GCS API.
        let metadata_json = serde_json::to_string(&gcs_metadata)
            .context(ErrorKind::Internal, "encoding GCS upload metadata")?;

        let content_type = metadata
            .content_type
            .parse()
            .context(ErrorKind::InvalidMetadata, "encoding GCS content type")?;

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
                    // Ensure the content type goes into headers unchanged including case
                    // sensitivity, as required by GCS. This means we cannot use `mime_str`.
                    .headers(HeaderMap::from_iter([(header::CONTENT_TYPE, content_type)])),
            );

        // GCS requires a multipart/related request. Its body looks identical to
        // multipart/form-data, but the Content-Type header is different. Hence, we have to manually
        // set the header *after* writing the multipart form into the request.
        let content_type = format!("multipart/related; boundary={}", multipart.boundary());

        let url = self.upload_url(id, "multipart")?;
        let response = self
            .with_failure_metrics("put_object", async {
                self.request(Method::POST, url)
                    .await?
                    .multipart(multipart)
                    .header(header::CONTENT_TYPE, content_type)
                    .send_traced()
                    .await
                    .check_error("uploading a GCS object")
                    .await
            })
            .await?;

        let stored_size = read_stored_content_length(response).await;
        self.report_object_write(
            id,
            stored_size,
            gcs_metadata.metadata_size(),
            metadata.time_expires,
        )
        .await;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from GCS backend");
        let object_url = self.object_url(id)?;
        let mut generation_retry_count = 0usize;

        loop {
            let Some(gcs_metadata) = self.get_gcs_metadata(&object_url, access_time).await? else {
                return Ok(None);
            };

            let mut download_url = object_url.clone();
            download_url
                .query_pairs_mut()
                .append_pair("alt", "media")
                .append_pair("ifGenerationMatch", &gcs_metadata.generation);

            let payload_response = self
                .with_retry("get_payload", || async {
                    // GCS transparently decompresses objects stored with `Content-Encoding: gzip`
                    // unless the request accepts gzip (decompressive transcoding). We always return
                    // payloads exactly as stored, so opt out of it. This also keeps range reads
                    // applying to the stored bytes. See:
                    // https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding
                    let mut req = self
                        .request(Method::GET, download_url.clone())
                        .await?
                        .header(header::ACCEPT_ENCODING, "gzip");
                    if let Some(r) = range {
                        req = req.header(header::RANGE, r.to_header_value());
                    }

                    let resp = req
                        .send_traced()
                        .await
                        .reqwest_context("getting a GCS object payload")?;

                    if resp.status() == StatusCode::PRECONDITION_FAILED {
                        resp.drain_body().await;
                        return Ok(None);
                    }

                    if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                        let raw = resp
                            .headers()
                            .get(header::CONTENT_RANGE)
                            .and_then(|v| v.to_str().ok());
                        let total = raw.and_then(ContentRange::parse_unsatisfiable_total);
                        let err = match total {
                            Some(total) => ErrorKind::RangeNotSatisfiable { total }.into(),
                            None => Error::new(
                                ErrorKind::BackendFailure,
                                "invalid GCS 416 Content-Range",
                            ),
                        };
                        resp.drain_body().await;
                        return Err(err);
                    }

                    resp.check_error("getting a GCS object payload")
                        .await
                        .map(Some)
                })
                .await?;

            let Some(payload_response) = payload_response else {
                if generation_retry_count >= REQUEST_RETRY_COUNT {
                    objectstore_metrics::count!("gcs.failures", action = "get_object");
                    return Err(Error::new(
                        ErrorKind::BackendFailure,
                        "GCS object kept changing while being read",
                    ));
                }

                generation_retry_count += 1;
                objectstore_metrics::count!("gcs.retries", action = "get_object");
                continue;
            };

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

            let metadata = gcs_metadata.into_metadata()?;
            return Ok(Some((metadata, content_range, stream)));
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from GCS backend");
        let object_url = self.object_url(id)?;
        match self.get_gcs_metadata(&object_url, access_time).await? {
            Some(gcs_metadata) => Ok(Some(gcs_metadata.into_metadata()?)),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let object_url = self.object_url(id)?;
        let Some(object) = self.get_gcs_metadata(&object_url, access_time).await? else {
            return Ok(SetExpiryResponse::NotFound);
        };
        let Some(current_expiry) = object.custom_time else {
            return Ok(SetExpiryResponse::Rejected);
        };
        let time_created = object.time_created.map(Rfc3339Timestamp::into_inner);
        let Some(expire_at) = target.resolve(time_created, access_time)? else {
            return Ok(SetExpiryResponse::Rejected);
        };
        if current_expiry.into_inner() >= expire_at {
            return Ok(SetExpiryResponse::Satisfied(expire_at)); // already satisfied
        }

        let policy = object
            .metadata
            .get(&GcsMetaKey::Expiration)
            .map(|value| value.parse())
            .transpose()
            .context(ErrorKind::CorruptData, "decoding GCS expiration policy")?
            .unwrap_or_default();
        let updated_policy = common::extended_expiration_policy(
            policy,
            time_created,
            current_expiry.into_inner(),
            expire_at,
        )?;
        let metadata = if updated_policy != policy {
            Some(BTreeMap::from([(
                GcsMetaKey::Expiration,
                updated_policy.to_string(),
            )]))
        } else {
            None
        };

        self.change_stream.begin_update(id, Some(expire_at)).await?;
        let outcome = self
            .update_custom_time(object_url, expire_at, object.generations(), metadata)
            .await?;
        if matches!(outcome, SetExpiryResponse::Satisfied(_)) {
            self.change_stream.commit_update(id, Some(expire_at)).await;
        }

        Ok(outcome)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
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
            self.change_stream.commit_delete(id).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        upload_length: NonZeroU64,
    ) -> Result<Option<BackendToken>> {
        objectstore_log::debug!("Creating resumable upload session on GCS backend");
        self.change_stream
            .begin_write(id, metadata.time_expires)
            .await?;
        let url = self.upload_url(id, "resumable")?;
        let metadata_json = serde_json::to_vec(&GcsObject::from_metadata(metadata)).context(
            ErrorKind::Internal,
            "serializing GCS resumable upload metadata",
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
                        .header("x-upload-content-length", upload_length.get())
                        .body(metadata_json)
                        .send_traced()
                        .await
                        .check_error("creating a GCS resumable upload")
                        .await?;

                    if response.status() != StatusCode::OK {
                        let status = response.status();
                        response.drain_body().await;
                        return Err(Error::new(
                            ErrorKind::BackendFailure,
                            format!("unexpected GCS resumable upload creation status {status}"),
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
                                "missing valid Location header in GCS resumable upload creation response",
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
                "invalid Location URL in GCS resumable upload creation response",
            )
        })?;
        Ok(Some(session_uri.into()))
    }

    #[tracing::instrument(level = "debug", fields(?session, offset, content_length), skip_all)]
    async fn put_chunk(
        &self,
        session: &Session,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        objectstore_log::debug!("Uploading resumable chunk to GCS backend");
        let session_uri =
            Url::parse(&session.backend_token).map_err(|_| ErrorKind::UnknownUploadSession)?;

        let end = offset
            .checked_add(content_length)
            .filter(|end| *end <= session.upload_length.get())
            .ok_or(ErrorKind::ChunkExceedsUploadLength {
                offset,
                content_length,
                upload_length: session.upload_length.get(),
            })?;
        let granularity = self.upload_granularity();
        if content_length > 0 && content_length < granularity && end != session.upload_length.get()
        {
            return Err(ErrorKind::ChunkTooSmall {
                chunk_length: content_length,
                upload_granularity: granularity,
            }
            .into());
        }

        let content_range = match content_length {
            // An empty chunk is equivalent to an offset query.
            0 => format!("bytes */{}", session.upload_length),
            _ => format!("bytes {offset}-{}/{}", end - 1, session.upload_length),
        };

        let progress = self
            .with_failure_metrics("put_chunk", async {
                let response = self
                    .request(Method::PUT, session_uri.as_str())
                    .await?
                    .header(header::CONTENT_LENGTH, content_length)
                    .header(header::CONTENT_RANGE, content_range)
                    .body(Body::wrap_stream(stream))
                    .send_traced()
                    .await
                    .reqwest_context("uploading a GCS resumable chunk")?;

                range_response_to_upload_progress(session.upload_length, response).await
            })
            .await?;
        if let GcsUploadProgress::Complete(ref object) = progress {
            let stored_size = object.size.as_deref().and_then(|size| size.parse().ok());
            let expires_at = object.custom_time.map(Rfc3339Timestamp::into_inner);
            self.report_object_write(
                &session.object_id,
                stored_size,
                object.metadata_size(),
                expires_at,
            )
            .await;
        }
        Ok(progress.into())
    }

    #[tracing::instrument(level = "debug", fields(?session), skip_all)]
    async fn upload_offset(&self, session: &Session) -> Result<UploadProgress> {
        objectstore_log::debug!("Querying resumable upload offset on GCS backend");
        let session_uri =
            Url::parse(&session.backend_token).map_err(|_| ErrorKind::UnknownUploadSession)?;

        self.with_retry("query_resumable_upload", || async {
            let response = self
                .request(Method::PUT, session_uri.as_str())
                .await?
                .header(header::CONTENT_LENGTH, 0)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes */{}", session.upload_length),
                )
                .send_traced()
                .await
                .reqwest_context("querying a GCS resumable upload")?;

            let progress =
                range_response_to_upload_progress(session.upload_length, response).await?;
            // The final `put_chunk` may have persisted the object but failed while
            // reading its response, so completion observed here must be reported too.
            if let GcsUploadProgress::Complete(ref object) = progress {
                let stored_size = object.size.as_deref().and_then(|size| size.parse().ok());
                let expires_at = object.custom_time.map(Rfc3339Timestamp::into_inner);
                self.report_object_write(
                    &session.object_id,
                    stored_size,
                    object.metadata_size(),
                    expires_at,
                )
                .await;
            }
            Ok(progress.into())
        })
        .await
    }

    #[tracing::instrument(level = "debug", fields(?session), skip_all)]
    async fn cancel_upload(&self, session: &Session) -> Result<()> {
        objectstore_log::debug!("Cancelling resumable upload on GCS backend");
        let session_uri =
            Url::parse(&session.backend_token).map_err(|_| ErrorKind::UnknownUploadSession)?;
        self.with_retry("cancel_resumable_upload", || {
            let session_uri = session_uri.clone();
            async move {
                let response = self
                    .request(Method::DELETE, session_uri)
                    .await?
                    .header(header::CONTENT_LENGTH, 0)
                    .send_traced()
                    .await
                    .reqwest_context("canceling a GCS resumable upload")?;
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
                            .check_error("canceling a GCS resumable upload")
                            .await?
                            .drain_body()
                            .await;
                        Err(Error::new(
                            ErrorKind::BackendFailure,
                            "unexpected GCS resumable upload cancellation status",
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::num::NonZeroU64;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use anyhow::Result;
    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::{OpType, test_utils::DummyProducer};
    use objectstore_types::scope::{Scope, Scopes};
    use reqwest::header::{HeaderMap, HeaderValue};

    use super::*;
    use crate::backend::common::ExpiryTarget;
    use crate::id::ObjectContext;
    use crate::stream;
    #[cfg(feature = "storage-cogs")]
    use crate::stream::ClientError;

    impl GcsBackend {
        async fn create_upload_session(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            upload_length: NonZeroU64,
        ) -> Result<Session> {
            let backend_token =
                <Self as Backend>::create_upload_session(self, id, metadata, upload_length)
                    .await?
                    .ok_or_else(|| Error::from(ErrorKind::Unsupported))?;
            Ok(Session {
                object_id: id.clone(),
                upload_length,
                backend_token,
            })
        }
    }

    fn read_http_request(connection: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            connection.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
        }
        String::from_utf8(bytes).unwrap()
    }

    fn write_http_response(
        connection: &mut TcpStream,
        status: &str,
        content_type: &str,
        body: &str,
    ) {
        write!(
            connection,
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    }

    fn start_generation_race_server()
    -> (String, mpsc::Receiver<Vec<String>>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let responses = [
                (
                    "200 OK",
                    "application/json",
                    r#"{"contentType":"text/old","generation":"1","metageneration":"1"}"#,
                ),
                ("412 Precondition Failed", "text/plain", "stale generation"),
                (
                    "200 OK",
                    "application/json",
                    r#"{"contentType":"text/new","generation":"2","metageneration":"1"}"#,
                ),
                ("200 OK", "text/plain", "new"),
            ];
            let mut requests = Vec::new();

            for (status, content_type, body) in responses {
                let (mut connection, _) = listener.accept().unwrap();
                requests.push(read_http_request(&mut connection));
                write_http_response(&mut connection, status, content_type, body);
            }

            request_tx.send(requests).unwrap();
        });
        (endpoint, request_rx, server)
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
            tcp_user_timeout: Duration::from_secs(30),
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
            usecase: "testing".parse().unwrap(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    fn make_id_with_key(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".parse().unwrap(),
                scopes: Scopes::from_iter([
                    Scope::create("organization", "42").unwrap(),
                    Scope::create("project", "7").unwrap(),
                ]),
            },
            key.into(),
        )
    }

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    #[tokio::test]
    async fn get_object_retries_from_metadata_after_generation_conflict() -> Result<()> {
        let (endpoint, request_rx, server) = start_generation_race_server();
        let backend = GcsBackend::new(
            GcsConfig {
                endpoint: Some(endpoint),
                bucket: "bucket".into(),
                tcp_user_timeout: Duration::from_secs(30),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        )
        .await?;

        let (metadata, _, stream) = backend
            .get_object(&make_id(), Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(metadata.content_type, "text/new");
        assert_eq!(stream::read_to_vec(stream).await?, b"new");

        let requests = request_rx.recv().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(!requests[0].contains("alt=media"));
        assert!(requests[1].contains("alt=media&ifGenerationMatch=1"));
        assert!(!requests[2].contains("alt=media"));
        assert!(requests[3].contains("alt=media&ifGenerationMatch=2"));
        // Payload downloads opt out of GCS decompressive transcoding.
        for request in [&requests[1], &requests[3]] {
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("\r\naccept-encoding: gzip\r\n"),
                "missing accept-encoding: {request}"
            );
        }
        server.join().unwrap();

        Ok(())
    }

    #[test]
    fn resumable_range_reports_next_offset_and_rejects_malformed_values() -> Result<()> {
        assert_eq!(range_header_to_offset("bytes=0-0", nonzero(10))?, 1);
        assert_eq!(
            range_header_to_offset("bytes=0-262143", nonzero(300_000))?,
            262_144
        );

        for malformed in [
            "",
            "bytes=1-2",
            "bytes=0-",
            "bytes=0-*",
            "bytes=0-9 ",
            "bytes=0-9,bytes=20-30",
        ] {
            assert!(
                range_header_to_offset(malformed, nonzero(100)).is_err(),
                "accepted {malformed:?}"
            );
        }
        assert_eq!(range_header_to_offset("bytes=0-9", nonzero(10))?, 10);
        assert!(range_header_to_offset("bytes=0-18446744073709551615", nonzero(u64::MAX)).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_empty_chunk_reports_offset_without_writing() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-empty-chunk");
        let upload_length = 2 * (RESUMABLE_CHUNK_SIZE + 2);
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(upload_length as u64))
            .await?;

        // An empty chunk cannot advance a session that still expects bytes. It reports the
        // authoritative offset instead of failing, and leaves what GCS holds untouched.
        assert_eq!(
            backend
                .put_chunk(&token, 0, 0, stream::single(Vec::new()))
                .await?,
            UploadProgress::Incomplete { offset: 0 }
        );
        // The backend rejects undersized non-final chunks, so use an unaligned chunk larger
        // than one granularity unit to exercise the emulator's alignment behavior.
        let chunk_length = RESUMABLE_CHUNK_SIZE + 2;
        let after_write = backend
            .put_chunk(
                &token,
                0,
                chunk_length as u64,
                stream::single(vec![b'a'; chunk_length]),
            )
            .await?;
        assert!(matches!(after_write, UploadProgress::Incomplete { .. }));

        // Whichever prefix GCS acknowledged, an empty chunk reports that same position rather
        // than moving it. GCS documents that a chunk "should be a multiple of 256 KiB ... unless
        // it's the last chunk", and that a client "should not assume that the server received all
        // bytes sent in any given request". The emulator acknowledges the unaligned tail, so the
        // position itself is not asserted here.
        assert_eq!(
            backend
                .put_chunk(&token, chunk_length as u64, 0, stream::single(Vec::new()))
                .await?,
            after_write
        );
        assert_eq!(backend.upload_offset(&token).await?, after_write);
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_single_and_multi_chunk_uploads() -> Result<()> {
        let backend = create_test_backend().await?;

        let single_id = make_id_with_key("resumable-single");
        let single = b"single chunk".to_vec();
        let token = backend
            .create_upload_session(
                &single_id,
                &Metadata::default(),
                nonzero(single.len() as u64),
            )
            .await?;
        assert_eq!(
            backend
                .put_chunk(
                    &token,
                    0,
                    single.len() as u64,
                    stream::single(single.clone()),
                )
                .await?,
            UploadProgress::Complete
        );
        let (_, _, payload) = backend
            .get_object(&single_id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, single);

        let multi_id = make_id_with_key("resumable-multi");
        let mut expected = vec![b'a'; RESUMABLE_CHUNK_SIZE];
        expected.extend_from_slice(b"final");
        let token = backend
            .create_upload_session(
                &multi_id,
                &Metadata::default(),
                nonzero(expected.len() as u64),
            )
            .await?;
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );
        let error = backend
            .put_chunk(&token, 0, 1, stream::single(b"a".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::ChunkTooSmall {
                chunk_length: 1,
                upload_granularity: RESUMABLE_CHUNK_SIZE as u64,
            }
        );
        assert_eq!(
            backend
                .put_chunk(
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
            backend.upload_offset(&token).await?,
            UploadProgress::Incomplete {
                offset: RESUMABLE_CHUNK_SIZE as u64
            }
        );
        assert_eq!(
            backend
                .put_chunk(
                    &token,
                    RESUMABLE_CHUNK_SIZE as u64,
                    5,
                    stream::single(b"final".to_vec()),
                )
                .await?,
            UploadProgress::Complete
        );
        let (_, _, payload) = backend
            .get_object(&multi_id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_unaligned_chunk_and_rewind_preserve_persisted_bytes() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-rewind");
        let upload_length = RESUMABLE_CHUNK_SIZE + 3;
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(upload_length as u64))
            .await?;

        let prefix = vec![b'a'; RESUMABLE_CHUNK_SIZE];
        assert_eq!(
            backend
                .put_chunk(&token, 0, prefix.len() as u64, stream::single(prefix))
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
                    &token,
                    (RESUMABLE_CHUNK_SIZE - 3) as u64,
                    6,
                    stream::single(b"BADxyz".to_vec()),
                )
                .await?,
            UploadProgress::Complete
        );

        let (_, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let payload = stream::read_to_vec(payload).await?;
        assert_eq!(&payload[RESUMABLE_CHUNK_SIZE - 3..], b"aaaxyz");
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_rejects_oversized_chunks() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-validation");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(10))
            .await?;

        let error = backend
            .put_chunk(&token, 8, 3, stream::single(b"abc".to_vec()))
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
            .put_chunk(&token, u64::MAX, 2, stream::single(b"ab".to_vec()))
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
            .create_upload_session(&id, &Metadata::default(), nonzero(10))
            .await?;

        backend.cancel_upload(&token).await?;
        backend.cancel_upload(&token).await?;
        assert!(matches!(
            backend.upload_offset(&token).await,
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
            .create_upload_session(&id, &Metadata::default(), nonzero(10))
            .await?;

        inject_retry_test(&mut backend, "storage.objects.insert", "return-503").await?;
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );

        inject_retry_test(&mut backend, "storage.objects.delete", "return-503").await?;
        backend.cancel_upload(&token).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_resumable_stream_failures_require_explicit_offset_recovery() -> Result<()> {
        // A failure before persistence is returned directly: put_chunk must not retry the body.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-before");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(4))
            .await?;
        inject_retry_test(&mut backend, "storage.objects.insert", "return-503").await?;
        let metrics = objectstore_metrics::with_capturing_test_client_async(async {
            assert!(matches!(
                backend
                    .put_chunk(&token, 0, 4, stream::single(b"data".to_vec()))
                    .await,
                Err(error) if error.kind() == ErrorKind::BackendUnavailable
            ));
        })
        .await;
        assert_eq!(
            metrics
                .iter()
                .filter(|metric| metric.starts_with("gcs.failures:"))
                .collect::<Vec<_>>(),
            ["gcs.failures:+1|c|#action:put_chunk"]
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.starts_with("gcs.retries:"))
        );
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Incomplete { offset: 0 }
        );

        // The emulator persists the first KiB, then fails. Recovery observes that prefix and the
        // caller resumes exactly from the returned offset.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-partial");
        let data = vec![b'p'; 2048];
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(data.len() as u64))
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
                    &token,
                    0,
                    data.len() as u64,
                    stream::single(data.clone()),
                )
                .await,
            Err(error) if error.kind() == ErrorKind::BackendUnavailable
        ));
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Incomplete { offset: 1024 }
        );
        assert_eq!(
            backend
                .put_chunk(&token, 1024, 1024, stream::single(data[1024..].to_vec()))
                .await?,
            UploadProgress::Complete
        );

        // GCS persists the final bytes, but storage-testbench truncates the successful JSON
        // response. The chunk is not retried; an explicit status query observes completion.
        let mut backend = create_test_backend().await?;
        let id = make_id_with_key("resumable-failure-final");
        let token = backend
            .create_upload_session(&id, &Metadata::default(), nonzero(5))
            .await?;
        inject_retry_test(
            &mut backend,
            "storage.objects.insert",
            "return-broken-stream-final-chunk-after-0B",
        )
        .await?;
        assert!(matches!(
            backend
                .put_chunk(&token, 0, 5, stream::single(b"final".to_vec()))
                .await,
            Err(error) if error.kind() == ErrorKind::CorruptData
        ));
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Complete
        );
        Ok(())
    }

    async fn get_gcs_generations(
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
            time_created: Some(Timestamp::now()),
            time_expires: None,
            size: None,
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let (meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();

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

    #[tokio::test]
    async fn test_unicode_metadata_roundtrip_json_upload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = unicode_metadata();

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
        assert_eq!(meta.filename, metadata.filename);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[test]
    fn from_metadata_uses_provided_time_expires() {
        let created = Timestamp::now();
        let expires = created + Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_created: Some(created),
            time_expires: Some(expires),
            ..Default::default()
        };

        let gcs_object = GcsObject::from_metadata(&metadata);
        let custom_time = gcs_object.custom_time.map(Rfc3339Timestamp::into_inner);
        assert_eq!(custom_time, Some(expires));

        let roundtripped = gcs_object.into_metadata().unwrap();
        assert_eq!(roundtripped.time_created, Some(created));
        assert_eq!(roundtripped.time_expires, Some(expires));
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_object(&id, Timestamp::now(), None).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend.delete_object(&id, Timestamp::now()).await?;

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
            .put_object(&id, &metadata, stream::single("hello"), Timestamp::now())
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("world"), Timestamp::now())
            .await?;

        let (meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();

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
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        backend.delete_object(&id, Timestamp::now()).await?;

        let result = backend.get_object(&id, Timestamp::now(), None).await?;
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
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let result = backend.get_object(&id, Timestamp::now(), None).await?;
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
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let result = backend.get_object(&id, Timestamp::now(), None).await?;
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
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_metadata(&id, Timestamp::now()).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_set_expiry() -> Result<()> {
        for is_ttl in [false, true] {
            let backend = create_test_backend().await?;

            let id = make_id();
            let tti = Duration::from_hours(2 * 24);
            let metadata = Metadata {
                content_type: "text/plain".into(),
                custom: [("preserved".into(), "yes".into())].into(),
                expiration_policy: if is_ttl {
                    ExpirationPolicy::TimeToLive(tti)
                } else {
                    ExpirationPolicy::TimeToIdle(tti)
                },
                time_expires: Some(Timestamp::now() + tti),
                ..Default::default()
            };

            backend
                .put_object(
                    &id,
                    &metadata,
                    stream::single("hello, world"),
                    Timestamp::now(),
                )
                .await?;

            // Backdate custom_time while keeping the object live.
            let object_url = backend.object_url(&id)?;
            let old_deadline = Timestamp::now() + Duration::from_mins(1);
            let generations = get_gcs_generations(&backend, object_url.clone()).await?;
            backend
                .update_custom_time(
                    object_url,
                    old_deadline,
                    (&generations.0, &generations.1),
                    None,
                )
                .await?;

            // Backend reads return the stored deadline without modifying it.
            let pre_meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
            let pre_expiry = pre_meta.time_expires.unwrap();
            let created = pre_meta.time_created.unwrap();
            assert_eq!(
                backend
                    .get_metadata(&id, Timestamp::now())
                    .await?
                    .unwrap()
                    .time_expires,
                Some(pre_expiry)
            );

            let requested = created + tti + tti;
            for target in [
                ExpiryTarget::At(requested),
                ExpiryTarget::FromCreation(tti + tti),
            ] {
                assert_eq!(
                    backend
                        .set_expiry(&id, target.into(), Timestamp::now())
                        .await?,
                    SetExpiryResponse::Satisfied(requested)
                );
            }
            assert_eq!(
                backend
                    .get_metadata(&id, Timestamp::now())
                    .await?
                    .unwrap()
                    .time_expires,
                Some(requested)
            );

            let (updated, _, stream) = backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .unwrap();
            assert_eq!(
                updated.expiration_policy,
                if is_ttl {
                    ExpirationPolicy::TimeToLive(tti + tti)
                } else {
                    ExpirationPolicy::TimeToIdle(tti)
                }
            );
            assert_eq!(updated.custom, metadata.custom);
            let payload = stream::read_to_vec(stream).await?;
            assert_eq!(&payload, b"hello, world");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_expiry_conflict() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_expires: Some(Timestamp::now() + Duration::from_mins(10)),
            ..Default::default()
        };
        backend
            .put_object(&id, &metadata, stream::single("payload"), Timestamp::now())
            .await?;
        let object_url = backend.object_url(&id)?;
        let generations = get_gcs_generations(&backend, object_url.clone()).await?;

        let deadline = Timestamp::now() + Duration::from_hours(1);
        assert_eq!(
            backend
                .update_custom_time(
                    object_url.clone(),
                    deadline,
                    (&generations.0, &generations.1),
                    None,
                )
                .await?,
            SetExpiryResponse::Satisfied(deadline)
        );
        assert_eq!(
            backend
                .update_custom_time(
                    object_url.clone(),
                    Timestamp::now() + Duration::from_hours(2),
                    (&generations.0, &generations.1),
                    None,
                )
                .await?,
            SetExpiryResponse::Rejected
        );

        backend.delete_object(&id, Timestamp::now()).await?;
        assert_eq!(
            backend
                .update_custom_time(
                    object_url,
                    Timestamp::now() + Duration::from_hours(2),
                    (&generations.0, &generations.1),
                    None,
                )
                .await?,
            SetExpiryResponse::NotFound
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
            .put_object(
                &id,
                &metadata,
                stream::single(compressed.clone()),
                Timestamp::now(),
            )
            .await?;

        let (meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Zstd));
        assert_eq!(
            payload, compressed,
            "Payload should be returned still compressed, not auto-decompressed"
        );

        Ok(())
    }

    fn gzip_encode(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    /// GCS decompresses `Content-Encoding: gzip` objects on download unless the request accepts
    /// gzip. Full and range reads must return the stored compressed bytes.
    #[tokio::test]
    async fn test_gzip_payload_full_and_range_reads() -> Result<()> {
        use objectstore_types::metadata::Compression;

        let backend = create_test_backend().await?;

        let plaintext = b"hello, world (but compressed with gzip)";
        let compressed = gzip_encode(plaintext);

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Gzip),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single(compressed.clone()),
                Timestamp::now(),
            )
            .await?;

        let (meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Gzip));
        assert_eq!(
            payload, compressed,
            "Payload should be returned still compressed, not transcoded"
        );

        let (meta, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Bounded(2, 9)))
            .await?
            .unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Gzip));
        assert_eq!(
            content_range,
            Some(ContentRange {
                start: 2,
                end: 9,
                total: compressed.len() as u64,
            })
        );
        assert_eq!(payload, &compressed[2..=9]);

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
            time_expires: Some(Timestamp::now() + Duration::from_secs(3600)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<ClientError>(payload.clone()),
                Timestamp::now(),
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
            time_expires: Some(Timestamp::now() + Duration::from_secs(3600)),
            ..Default::default()
        };
        let token = backend
            .create_upload_session(&id, &metadata, nonzero(payload.len() as u64))
            .await?;

        assert_eq!(
            backend
                .put_chunk(
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
    async fn resumable_completion_after_corrupt_response_reports_to_change_stream() -> Result<()> {
        let (mut backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id_with_key("resumable-change-stream-after-corrupt-response");
        let payload = b"final".to_vec();
        let metadata = Metadata::default();
        let token = backend
            .create_upload_session(&id, &metadata, nonzero(payload.len() as u64))
            .await?;
        inject_retry_test(
            &mut backend,
            "storage.objects.insert",
            "return-broken-stream-final-chunk-after-0B",
        )
        .await?;

        assert!(matches!(
            backend
                .put_chunk(
                    &token,
                    0,
                    payload.len() as u64,
                    stream::single::<ClientError>(payload.clone()),
                )
                .await,
            Err(error) if error.kind() == ErrorKind::CorruptData
        ));
        assert_eq!(
            backend.upload_offset(&token).await?,
            UploadProgress::Complete
        );

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(
            records[0].size,
            Some(payload.len() as u64 + GcsObject::from_metadata(&metadata).metadata_size())
        );
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
                Timestamp::now(),
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
                Timestamp::now(),
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

        backend.delete_object(&make_id(), Timestamp::now()).await?;

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
                Timestamp::now(),
            )
            .await?;
        producer.clear();

        backend.delete_object(&id, Timestamp::now()).await?;

        let records = producer.records();
        assert_eq!(records.len(), 1, "a retried delete must report only once");
        assert_eq!(records[0].op_type, OpType::Delete);
        assert_eq!(records[0].size, None);

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_expiry_extension_as_an_update() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_expires: Some(Timestamp::now() + Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<ClientError>(b"hi".to_vec()),
                Timestamp::now(),
            )
            .await?;
        producer.clear();

        backend.get_metadata(&id, Timestamp::now()).await?;
        assert!(producer.records().is_empty());

        backend
            .set_expiry(
                &id,
                ExpiryTarget::At(Timestamp::now() + Duration::from_secs(3600)).into(),
                Timestamp::now(),
            )
            .await?;

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Update);
        assert_eq!(records[0].size, None);
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }
}
