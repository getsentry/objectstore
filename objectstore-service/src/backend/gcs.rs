//! Google Cloud Storage backend for long-term storage of large objects.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::{Duration, SystemTime};
use std::{fmt, io};

use anyhow::Context;
use futures_util::{StreamExt, TryStreamExt};
use gcp_auth::TokenProvider;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::header::HeaderName;
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode, Url, header, multipart};
use serde::{Deserialize, Serialize};

use crate::backend::common::{
    self, Backend, DeleteResponse, GetResponse, MetadataResponse, MultipartUploadBackend,
    PutResponse,
};
use crate::error::{Error, Result};
use crate::gcp_auth::PrefetchingTokenProvider;
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::stream::{self, ClientStream};

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
}

/// Default endpoint used to access the GCS JSON API.
const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";
/// Permission scopes required for accessing GCS.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day
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
}

impl GcsObject {
    /// Converts our Metadata type to GCS JSON object metadata.
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let mut gcs_object = GcsObject {
            content_type: metadata.content_type.clone(),
            size: metadata.size.map(|size| size.to_string()),
            content_encoding: None,
            custom_time: None,
            time_created: metadata.time_created,
            metadata: BTreeMap::new(),
        };

        // For time-based expiration, set the `customTime` field. The bucket must have a
        // `daysSinceCustomTime` lifecycle rule configured to delete objects with this field set.
        // This rule automatically skips objects without `customTime` set.
        if let Some(expires_in) = metadata.expiration_policy.expires_in() {
            gcs_object.custom_time = Some(SystemTime::now() + expires_in);
        }

        if let Some(compression) = metadata.compression {
            gcs_object.content_encoding = Some(compression.to_string());
        }

        if metadata.expiration_policy != ExpirationPolicy::default() {
            gcs_object.metadata.insert(
                GcsMetaKey::Expiration,
                metadata.expiration_policy.to_string(),
            );
        }

        if let Some(origin) = &metadata.origin {
            gcs_object
                .metadata
                .insert(GcsMetaKey::Origin, origin.clone());
        }

        for (key, value) in &metadata.custom {
            gcs_object
                .metadata
                .insert(GcsMetaKey::Custom(key.clone()), value.clone());
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
            .transpose()?
            .unwrap_or_default();

        let origin = self.metadata.remove(&GcsMetaKey::Origin);

        let content_type = self.content_type;
        let compression = self.content_encoding.map(|s| s.parse()).transpose()?;
        let size = self
            .size
            .map(|size| size.parse())
            .transpose()
            .map_err(|e| Error::Generic {
                context: "GCS: failed to parse size from object metadata".to_string(),
                cause: Some(Box::new(e)),
            })?;
        let time_created = self.time_created;

        // At this point, all built-in metadata should have been removed from self.metadata.
        let mut custom = BTreeMap::new();
        for (key, value) in self.metadata {
            if let GcsMetaKey::Custom(custom_key) = key {
                custom.insert(custom_key, value);
            } else {
                return Err(Error::Generic {
                    context: format!(
                        "GCS: unexpected built-in metadata key in object metadata: {}",
                        key
                    ),
                    cause: None,
                });
            }
        }

        Ok(Metadata {
            content_type,
            expiration_policy,
            compression,
            origin,
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
    /// Ignored metadata set by the GCS emulator.
    EmulatorIgnored,
    /// User-defined custom metadata key.
    Custom(String),
}

impl std::str::FromStr for GcsMetaKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "x_emulator_upload" | "x_testbench_upload") {
            return Ok(GcsMetaKey::EmulatorIgnored);
        }

        Ok(match s.strip_prefix(BUILTIN_META_PREFIX) {
            Some("expiration") => GcsMetaKey::Expiration,
            Some("origin") => GcsMetaKey::Origin,
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
            Self::EmulatorIgnored => unreachable!("do not serialize emulator metadata"),
            Self::Custom(key) => write!(f, "{CUSTOM_META_PREFIX}{key}"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for GcsMetaKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Cow::<'de, str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for GcsMetaKey {
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

    if let Some(expires_in) = metadata.expiration_policy.expires_in() {
        let custom_time = SystemTime::now() + expires_in;
        let formatted = humantime::format_rfc3339_seconds(custom_time);
        headers.insert(
            HeaderName::from_static("x-goog-custom-time"),
            formatted.to_string().parse().map_err(|e| Error::Generic {
                context: "GCS: invalid custom-time header value".into(),
                cause: Some(Box::new(e)),
            })?,
        );
    }

    if let Some(compression) = metadata.compression {
        headers.insert(
            header::CONTENT_ENCODING,
            compression
                .to_string()
                .parse()
                .map_err(|e| Error::Generic {
                    context: "GCS: invalid content-encoding header value".into(),
                    cause: Some(Box::new(e)),
                })?,
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

    for (key, value) in &metadata.custom {
        insert_gcs_meta_header(&mut headers, &GcsMetaKey::Custom(key.clone()), value)?;
    }

    Ok(headers)
}

fn insert_gcs_meta_header(
    headers: &mut header::HeaderMap,
    key: &GcsMetaKey,
    value: &str,
) -> Result<()> {
    let header_name = format!("x-goog-meta-{key}");
    headers.insert(
        HeaderName::try_from(&header_name).map_err(|e| Error::Generic {
            context: format!("GCS: invalid header name: {header_name}"),
            cause: Some(Box::new(e)),
        })?,
        value.parse().map_err(|e| Error::Generic {
            context: format!("GCS: invalid header value for {header_name}"),
            cause: Some(Box::new(e)),
        })?,
    );
    Ok(())
}

/// Returns `true` if the error is a transient reqwest failure worth retrying.
fn is_retryable(error: &Error) -> bool {
    let Error::Reqwest { cause, .. } = error else {
        return false;
    };
    if cause.is_timeout() || cause.is_connect() || cause.is_request() {
        return true;
    }
    let Some(status) = cause.status() else {
        return false;
    };
    // https://docs.cloud.google.com/storage/docs/json_api/v1/status-codes
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

/// GCS JSON API backend for long-term storage of large objects.
pub struct GcsBackend {
    client: reqwest::Client,
    endpoint: Url,
    bucket: String,
    token_provider: Option<PrefetchingTokenProvider>,
}

impl GcsBackend {
    /// Creates an authenticated GCS JSON API backend bound to the bucket in `config`.
    pub async fn new(config: GcsConfig) -> anyhow::Result<Self> {
        let GcsConfig { endpoint, bucket } = config;

        let token_provider = if endpoint.is_none() {
            Some(PrefetchingTokenProvider::gcp_auth(TOKEN_SCOPES).await?)
        } else {
            None
        };

        let endpoint_str = endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT);

        Ok(Self {
            client: common::reqwest_client(),
            endpoint: endpoint_str.parse().context("invalid GCS endpoint URL")?,
            bucket,
            token_provider,
        })
    }

    /// Formats the GCS object (metadata) URL for the given key.
    fn object_url(&self, id: &ObjectId) -> Result<Url> {
        let mut url = self.endpoint.clone();

        let path = id.as_storage_path().to_string();
        url.path_segments_mut()
            .map_err(|()| Error::Generic {
                context: format!(
                    "GCS: invalid endpoint URL, {} cannot be a base",
                    self.endpoint
                ),
                cause: None,
            })?
            .extend(&["storage", "v1", "b", &self.bucket, "o", &path]);

        Ok(url)
    }

    /// Formats the GCS upload URL for the given upload type.
    fn upload_url(&self, id: &ObjectId, upload_type: &str) -> Result<Url> {
        let mut url = self.endpoint.clone();

        url.path_segments_mut()
            .map_err(|()| Error::Generic {
                context: format!(
                    "GCS: invalid endpoint URL, {} cannot be a base",
                    self.endpoint
                ),
                cause: None,
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
            let mut segments = url.path_segments_mut().map_err(|()| Error::Generic {
                context: format!(
                    "GCS: invalid endpoint URL, {} cannot be a base",
                    self.endpoint
                ),
                cause: None,
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
            let token = provider.token(TOKEN_SCOPES).await?;
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
                Err(ref e) if retry_count < REQUEST_RETRY_COUNT && is_retryable(e) => {
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
    async fn fetch_gcs_metadata(&self, object_url: &Url) -> Result<Option<Metadata>> {
        let metadata_opt = self
            .with_retry("get_metadata", || async {
                let resp = self
                    .request(Method::GET, object_url.clone())
                    .await?
                    .send()
                    .await
                    .map_err(|e| Error::reqwest("GCS: get metadata request", e))?;

                if resp.status() == StatusCode::NOT_FOUND {
                    return Ok(None);
                }

                let metadata: GcsObject = resp
                    .error_for_status()
                    .map_err(|e| Error::reqwest("GCS: get metadata status", e))?
                    .json()
                    .await
                    .map_err(|e| Error::reqwest("GCS: get metadata parse", e))?;

                Ok(Some(metadata))
            })
            .await?;

        let Some(gcs_metadata) = metadata_opt else {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        };

        let expire_at = gcs_metadata.custom_time;
        let metadata = gcs_metadata.into_metadata()?;

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();

        // Filter already expired objects but leave them to garbage collection
        if metadata.expiration_policy.is_timeout() && expire_at.is_some_and(|ts| ts < access_time) {
            objectstore_log::debug!("Object found but past expiry");
            return Ok(None);
        }

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy {
            let new_expire_at = access_time + tti;
            if expire_at.is_some_and(|ts| ts < new_expire_at - TTI_DEBOUNCE) {
                self.update_custom_time(object_url.clone(), new_expire_at)
                    .await?;
            }
        }

        Ok(Some(metadata))
    }

    async fn update_custom_time(&self, object_url: Url, custom_time: SystemTime) -> Result<()> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CustomTimeRequest {
            #[serde(with = "humantime_serde")]
            custom_time: SystemTime,
        }

        self.with_retry("update_custom_time", || async {
            self.request(Method::PATCH, object_url.clone())
                .await?
                .json(&CustomTimeRequest { custom_time })
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .map_err(|e| Error::reqwest("GCS: update custom time", e))?;
            Ok(())
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

#[async_trait::async_trait]
impl Backend for GcsBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
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
        let metadata_json = serde_json::to_string(&gcs_metadata).map_err(|cause| Error::Serde {
            context: "failed to serialize metadata for GCS upload".to_string(),
            cause,
        })?;

        let multipart = multipart::Form::new()
            .part(
                "metadata",
                multipart::Part::text(metadata_json)
                    .mime_str("application/json")
                    .expect("application/json is a valid mime type"),
            )
            .part(
                "media",
                multipart::Part::stream(Body::wrap_stream(stream))
                    .mime_str(&metadata.content_type)
                    .map_err(|e| Error::Generic {
                        context: format!("invalid mime type: {}", &metadata.content_type),
                        cause: Some(Box::new(e)),
                    })?,
            );

        // GCS requires a multipart/related request. Its body looks identical to
        // multipart/form-data, but the Content-Type header is different. Hence, we have to manually
        // set the header *after* writing the multipart form into the request.
        let content_type = format!("multipart/related; boundary={}", multipart.boundary());

        self.request(Method::POST, self.upload_url(id, "multipart")?)
            .await?
            .multipart(multipart)
            .header(header::CONTENT_TYPE, content_type)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::Client(ce),
                _ => Error::reqwest("GCS: upload object", e),
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from GCS backend");
        let object_url = self.object_url(id)?;

        let Some(metadata) = self.fetch_gcs_metadata(&object_url).await? else {
            return Ok(None);
        };

        let mut download_url = object_url;
        download_url.query_pairs_mut().append_pair("alt", "media");

        let payload_response = self
            .with_retry("get_payload", || async {
                self.request(Method::GET, download_url.clone())
                    .await?
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                    .map_err(|e| Error::reqwest("GCS: get payload", e))
            })
            .await?;

        let stream = payload_response
            .bytes_stream()
            .map_err(io::Error::other)
            .boxed();

        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from GCS backend");
        let object_url = self.object_url(id)?;
        self.fetch_gcs_metadata(&object_url).await
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from GCS backend");
        let object_url = self.object_url(id)?;

        self.with_retry("delete", || async {
            let resp = self
                .request(Method::DELETE, object_url.clone())
                .await?
                .send()
                .await
                .map_err(|e| Error::reqwest("GCS: delete object", e))?;

            // Do not error for objects that do not exist
            if resp.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }

            resp.error_for_status()
                .map_err(|e| Error::reqwest("GCS: delete object", e))?;

            Ok(())
        })
        .await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlInitiateMultipartUploadResponse {
    upload_id: String,
}

impl From<XmlInitiateMultipartUploadResponse> for InitiateMultipartResponse {
    fn from(r: XmlInitiateMultipartUploadResponse) -> Self {
        r.upload_id
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlListPartsResponse {
    #[serde(default)]
    is_truncated: bool,
    next_part_number_marker: Option<u32>,
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
    part_number: u32,
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

#[async_trait::async_trait]
impl MultipartUploadBackend for GcsBackend {
    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        objectstore_log::debug!("Initiating multipart upload on GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.set_query(Some("uploads"));

        let mut builder = self
            .request(Method::POST, url)
            .await?
            .header(header::CONTENT_TYPE, metadata.content_type.as_ref())
            .header(header::CONTENT_LENGTH, "0");

        let meta_headers = metadata_to_gcs_headers(metadata)?;
        for (name, value) in &meta_headers {
            builder = builder.header(name, value);
        }

        let resp = builder
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::reqwest("GCS: initiate multipart upload", e))?;

        let body = resp
            .bytes()
            .await
            .map_err(|e| Error::reqwest("GCS: read initiate multipart body", e))?;

        let xml: XmlInitiateMultipartUploadResponse = quick_xml::de::from_reader(body.as_ref())
            .map_err(|e| Error::Generic {
                context: "GCS: failed to parse initiate multipart response".to_owned(),
                cause: Some(Box::new(e)),
            })?;

        Ok(xml.into())
    }

    #[tracing::instrument(level = "trace", fields(?id, upload_id, part_number), skip_all)]
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
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::reqwest("GCS: upload part", e))?;

        let etag = resp
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
            .ok_or_else(|| Error::generic("GCS: upload part response missing ETag header"))?;

        Ok(etag)
    }

    #[tracing::instrument(level = "trace", fields(?id, upload_id), skip_all)]
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

        let resp = self
            .request(Method::GET, url)
            .await?
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::reqwest("GCS: list parts", e))?;

        let body = resp
            .bytes()
            .await
            .map_err(|e| Error::reqwest("GCS: read list parts body", e))?;

        let xml: XmlListPartsResponse =
            quick_xml::de::from_reader(body.as_ref()).map_err(|e| Error::Generic {
                context: "GCS: failed to parse list parts response".to_owned(),
                cause: Some(Box::new(e)),
            })?;

        Ok(xml.into())
    }

    #[tracing::instrument(level = "trace", fields(?id, upload_id), skip_all)]
    async fn abort_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartResponse> {
        objectstore_log::debug!("Aborting multipart upload on GCS backend");
        let mut url = self.xml_object_url(id)?;
        url.query_pairs_mut().append_pair("uploadId", upload_id);

        self.request(Method::DELETE, url)
            .await?
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::reqwest("GCS: abort multipart upload", e))?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id, upload_id), skip_all)]
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
        let xml = quick_xml::se::to_string(&body).map_err(|e| Error::Generic {
            context: "GCS: failed to serialize complete multipart request".into(),
            cause: Some(Box::new(e)),
        })?;

        let resp = self
            .request(Method::POST, url)
            .await?
            .header(header::CONTENT_TYPE, "application/xml")
            .body(xml)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::reqwest("GCS: complete multipart upload", e))?;

        let body = resp
            .bytes()
            .await
            .map_err(|e| Error::reqwest("GCS: read complete multipart body", e))?;

        let error = quick_xml::de::from_reader::<_, XmlError>(body.as_ref())
            .ok()
            .map(Into::into);

        Ok(error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;
    use crate::multipart::CompletedPart;
    use crate::stream;

    // NB: Not run any of these tests, you need to have a GCS emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<GcsBackend> {
        GcsBackend::new(GcsConfig {
            endpoint: Some("http://localhost:8087".into()),
            bucket: "test-bucket".into(),
        })
        .await
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
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
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            time_created: Some(SystemTime::now()),
            time_expires: None,
            size: None,
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);
        assert!(metadata.time_created.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_object(&id).await?;
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

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

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

        let result = backend.get_object(&id).await?;
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
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
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
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
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
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // Manually set custom_time to just inside the bump window.
        // The bump condition is: expire_at < now + tti - TTI_DEBOUNCE.
        let object_url = backend.object_url(&id)?;
        let old_deadline = SystemTime::now() + tti - TTI_DEBOUNCE - Duration::from_secs(60);
        backend.update_custom_time(object_url, old_deadline).await?;

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
        let (_, stream) = backend.get_object(&id).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(&payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_does_not_bump_fresh_tti() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
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

        let (meta, stream) = backend.get_object(&id).await?.unwrap();
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
                1,
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
                    part_number: 1,
                    etag,
                }],
            )
            .await?;
        assert!(result.is_none(), "expected no error on complete");

        let (meta, stream) = backend.get_object(&id).await?.unwrap();
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
                1,
                part1.len() as u64,
                None,
                stream::single(part1.clone()),
            )
            .await?;
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                2,
                part2.len() as u64,
                None,
                stream::single(part2.clone()),
            )
            .await?;
        let etag3 = backend
            .upload_part(
                &id,
                &upload_id,
                3,
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
                        part_number: 1,
                        etag: etag1,
                    },
                    CompletedPart {
                        part_number: 2,
                        etag: etag2,
                    },
                    CompletedPart {
                        part_number: 3,
                        etag: etag3,
                    },
                ],
            )
            .await?;
        assert!(result.is_none(), "expected no error on complete");

        // Object exists after complete
        let (_meta, stream) = backend.get_object(&id).await?.unwrap();
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
            .upload_part(&id, &upload_id, 1, 3, None, stream::single(b"aaa".to_vec()))
            .await?;
        let etag2 = backend
            .upload_part(&id, &upload_id, 2, 3, None, stream::single(b"bbb".to_vec()))
            .await?;

        // List all parts.
        let list = backend.list_parts(&id, &upload_id, None, None).await?;
        assert_eq!(list.parts.len(), 2);
        assert_eq!(list.parts[0].part_number, 1);
        assert_eq!(list.parts[0].etag, etag1);
        assert_eq!(list.parts[0].size, 3);
        assert_eq!(list.parts[1].part_number, 2);
        assert_eq!(list.parts[1].etag, etag2);
        assert_eq!(list.parts[1].size, 3);

        // List with max_parts=1 to test pagination.
        let page1 = backend.list_parts(&id, &upload_id, Some(1), None).await?;
        assert_eq!(page1.parts.len(), 1);
        assert_eq!(page1.parts[0].part_number, 1);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        let page2 = backend
            .list_parts(&id, &upload_id, Some(1), page1.next_part_number_marker)
            .await?;
        assert_eq!(page2.parts.len(), 1);
        assert_eq!(page2.parts[0].part_number, 2);

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
                1,
                5,
                None,
                stream::single(b"hello".to_vec()),
            )
            .await?;

        backend.abort_multipart(&id, &upload_id).await?;

        // Object should not exist after abort.
        let result = backend.get_object(&id).await?;
        assert!(result.is_none(), "object should not exist after abort");

        Ok(())
    }
}
