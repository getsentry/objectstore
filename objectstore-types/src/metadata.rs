//! Per-object metadata types and HTTP header serialization.
//!
//! This module defines [`Metadata`], the per-object metadata structure that
//! travels through the entire system: clients set it via HTTP headers, the
//! server parses and validates it, the service passes it to backends, and
//! backends persist it alongside the stored object.
//!
//! The module also defines further types used in metadata.
//!
//! # Serialization
//!
//! Metadata has two serialization formats:
//!
//! - **HTTP headers** — used by the public API. [`Metadata::from_headers`] and
//!   [`Metadata::to_headers`] handle this conversion for public fields only.
//! - **JSON** — used internally by backends for storage. JSON serialization
//!   includes additional internal fields that are skipped in the header
//!   representation.
//!
//! # HTTP header prefixes
//!
//! Headers use three prefix conventions:
//!
//! - Standard HTTP headers where applicable (`Content-Type`, `Content-Encoding`)
//! - `x-sn-*` for objectstore-specific fields (e.g. `x-sn-expiration`)
//! - `x-snme-` for custom user metadata (e.g. `x-snme-build_id`)
//!
//! Backends that store metadata as object metadata (like GCS) layer their own
//! prefix on top, so `x-sn-expiration` becomes `x-goog-meta-x-sn-expiration`.
//! The [`Metadata::from_headers`] and [`Metadata::to_headers`] methods accept
//! a `prefix` parameter for this purpose.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use http::header::{self, HeaderMap, HeaderName};
use humantime::{format_duration, format_rfc3339_micros, parse_duration, parse_rfc3339};
use serde::{Deserialize, Serialize};

/// The custom HTTP header that contains the serialized [`ExpirationPolicy`].
pub const HEADER_EXPIRATION: &str = "x-sn-expiration";
/// The custom HTTP header that contains the object creation time.
pub const HEADER_TIME_CREATED: &str = "x-sn-time-created";
/// The custom HTTP header that contains the object expiration time.
pub const HEADER_TIME_EXPIRES: &str = "x-sn-time-expires";
/// The custom HTTP header that contains the origin of the object.
pub const HEADER_ORIGIN: &str = "x-sn-origin";
/// The custom HTTP header that contains the filename of the object.
pub const HEADER_FILENAME: &str = "x-sn-filename";
/// The prefix for custom HTTP headers containing custom per-object metadata.
pub const HEADER_META_PREFIX: &str = "x-snme-";

/// The default content type for objects without a known content type.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Upper bound on the TTI debounce window.
///
/// The debounce window for TTI bumps is `min(tti / 4, MAX_TTI_DEBOUNCE)`. For
/// TTI values above 4 days the debounce stays at 24 hours (the historical
/// constant); shorter TTI values get a proportionally smaller window so that
/// bumps are not silently suppressed.
const MAX_TTI_DEBOUNCE: Duration = Duration::from_hours(24);

/// Errors that can happen dealing with metadata
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any problems dealing with http headers, essentially converting to/from [`str`].
    #[error("error dealing with http headers")]
    Header(#[from] Option<http::Error>),
    /// The value for the expiration policy is invalid.
    #[error("invalid expiration policy value")]
    Expiration(#[from] Option<humantime::DurationError>),
    /// The compression algorithm is invalid.
    #[error("invalid compression value")]
    Compression,
    /// The content type is invalid.
    #[error("invalid content type")]
    ContentType(#[from] mediatype::MediaTypeError),
    /// The creation time is invalid.
    #[error("invalid creation time")]
    CreationTime(#[from] humantime::TimestampError),
    /// An internal consistency invariant on the metadata was violated.
    #[error("invariant violation: {0}")]
    Invariant(&'static str),
}
impl From<header::InvalidHeaderValue> for Error {
    fn from(err: header::InvalidHeaderValue) -> Self {
        Self::Header(Some(err.into()))
    }
}
impl From<header::InvalidHeaderName> for Error {
    fn from(err: header::InvalidHeaderName) -> Self {
        Self::Header(Some(err.into()))
    }
}
impl From<header::ToStrError> for Error {
    fn from(_err: header::ToStrError) -> Self {
        // the error happens when converting a header value back to a `str`
        Self::Header(None)
    }
}

/// The per-object expiration policy.
///
/// Controls automatic object cleanup. The policy is set by the client at upload
/// time via the [`x-sn-expiration`](HEADER_EXPIRATION) header and persisted with
/// the object.
///
/// | Variant      | Wire format | Behavior                                     |
/// |--------------|-------------|----------------------------------------------|
/// | `Manual`     | `manual`    | No automatic expiration (default)            |
/// | `TimeToLive` | `ttl:30s`   | Expires after a fixed duration from creation |
/// | `TimeToIdle` | `tti:1h`    | Expires after a duration of no access        |
///
/// Durations use [humantime](https://docs.rs/humantime) format (e.g. `30s`,
/// `5m`, `1h`, `7d`).
///
/// **Important:** `Manual` is the default and must remain so — persisted objects
/// without an explicit policy are deserialized as `Manual`.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExpirationPolicy {
    /// Manual expiration, meaning no automatic cleanup.
    // IMPORTANT: Do not change the default, we rely on this for persisted objects.
    #[default]
    Manual,
    /// Time to live, with expiration after the specified duration.
    TimeToLive(Duration),
    /// Time to idle, with expiration once the object has not been accessed within the specified duration.
    TimeToIdle(Duration),
}
impl ExpirationPolicy {
    /// Returns the duration after which the object expires.
    pub fn expires_in(&self) -> Option<Duration> {
        match self {
            ExpirationPolicy::Manual => None,
            ExpirationPolicy::TimeToLive(duration) => Some(*duration),
            ExpirationPolicy::TimeToIdle(duration) => Some(*duration),
        }
    }

    /// Returns `true` if this policy indicates time-based expiry.
    pub fn is_timeout(&self) -> bool {
        match self {
            ExpirationPolicy::TimeToLive(_) => true,
            ExpirationPolicy::TimeToIdle(_) => true,
            ExpirationPolicy::Manual => false,
        }
    }

    /// Returns `true` if this policy is `Manual`.
    pub fn is_manual(&self) -> bool {
        *self == ExpirationPolicy::Manual
    }

    /// Checks whether a TTI deadline needs bumping given the current expiry and access time.
    ///
    /// Returns `Some(new_expire_at)` when the current deadline is stale enough
    /// to justify a write, `None` otherwise. The debounce window scales with the
    /// TTI duration so short-TTI objects get bumped more frequently.
    pub fn check_tti_bump(
        &self,
        time_expires: Option<SystemTime>,
        access_time: SystemTime,
    ) -> Option<SystemTime> {
        let ExpirationPolicy::TimeToIdle(tti) = *self else {
            return None;
        };

        let new_expire_at = access_time + tti;
        let debounce = (tti / 4).min(MAX_TTI_DEBOUNCE);
        match time_expires {
            Some(ts) if ts < new_expire_at - debounce => Some(new_expire_at),
            _ => None,
        }
    }
}
impl fmt::Display for ExpirationPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpirationPolicy::TimeToLive(duration) => {
                write!(f, "ttl:{}", format_duration(*duration))
            }
            ExpirationPolicy::TimeToIdle(duration) => {
                write!(f, "tti:{}", format_duration(*duration))
            }
            ExpirationPolicy::Manual => f.write_str("manual"),
        }
    }
}
impl FromStr for ExpirationPolicy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "manual" {
            return Ok(ExpirationPolicy::Manual);
        }
        if let Some(duration) = s.strip_prefix("ttl:") {
            return Ok(ExpirationPolicy::TimeToLive(parse_duration(duration)?));
        }
        if let Some(duration) = s.strip_prefix("tti:") {
            return Ok(ExpirationPolicy::TimeToIdle(parse_duration(duration)?));
        }
        Err(Error::Expiration(None))
    }
}

/// The compression algorithm applied to an object's payload.
///
/// Transmitted via the standard `Content-Encoding` HTTP header. Currently only
/// Zstandard (`zstd`) is supported.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    /// Compressed using `zstd`.
    Zstd,
    // /// Compressed using `gzip`.
    // Gzip,
    // /// Compressed using `lz4`.
    // Lz4,
}

impl Compression {
    /// Returns a string representation of the compression algorithm.
    pub fn as_str(&self) -> &str {
        match self {
            Compression::Zstd => "zstd",
            // Compression::Gzip => "gzip",
            // Compression::Lz4 => "lz4",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "zstd" => Ok(Compression::Zstd),
            // "gzip" => Compression::Gzip,
            // "lz4" => Compression::Lz4,
            _ => Err(Error::Compression),
        }
    }
}

/// Per-object metadata.
///
/// Includes first-class fields (expiration, compression, timestamps, etc.) and
/// arbitrary user-provided key-value metadata. See the [module-level
/// documentation](self) for the HTTP header mapping conventions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Metadata {
    /// The expiration policy of the object (header: `x-sn-expiration`).
    ///
    /// Skipped during serialization when set to [`ExpirationPolicy::Manual`].
    #[serde(skip_serializing_if = "ExpirationPolicy::is_manual")]
    pub expiration_policy: ExpirationPolicy,

    /// The creation/last replacement time of the object (header: `x-sn-time-created`).
    ///
    /// Set by the server every time an object is put, i.e. when objects are first
    /// created and when existing objects are overwritten.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_created: Option<SystemTime>,

    /// The resolved expiration timestamp (header: `x-sn-time-expires`).
    ///
    /// Derived from the [`expiration_policy`](Self::expiration_policy). When using
    /// a time-to-idle policy, this reflects the expiration timestamp present
    /// *prior to* the current access to the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_expires: Option<SystemTime>,

    /// IANA media type of the object (header: `Content-Type`).
    ///
    /// Defaults to [`DEFAULT_CONTENT_TYPE`] (`application/octet-stream`).
    pub content_type: Cow<'static, str>,

    /// The compression algorithm used for this object (header: `Content-Encoding`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    /// The origin of the object (header: `x-sn-origin`).
    ///
    /// Typically the IP address of the original source. This is an optional but
    /// encouraged field that tracks where the payload was originally obtained
    /// from (e.g. the IP of a Sentry SDK or CLI).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,

    /// An optional filename associated with this object (header: `x-sn-filename`).
    ///
    /// When present, the server includes a `Content-Disposition: attachment; filename="<filename>"`
    /// header in GET responses, prompting browsers and download tools to save the file
    /// under this name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,

    /// Size of the data in bytes, if known.
    ///
    /// Not transmitted via HTTP headers; set by backends when the object is
    /// stored or retrieved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    /// Arbitrary user-provided key-value metadata (header prefix: `x-snme-`).
    ///
    /// Each entry is transmitted as `x-snme-{key}: {value}`.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, String>,
}

impl Metadata {
    /// Parses the metadata headers accepted from writing endpoints.
    ///
    /// Unlike [`from_headers`](Self::from_headers), this skips parsing read-only attributes so
    /// clients cannot set them via headers.
    ///
    /// This materializes the following attributes:
    /// - [`time_created`](Self::time_created)
    /// - [`time_expires`](Self::time_expires)
    ///
    /// A prefix can also be provided which is stripped from custom non-standard headers.
    pub fn from_insert_headers(headers: &HeaderMap, prefix: &str) -> Result<Self, Error> {
        let mut metadata = Self::parse_headers(headers, prefix, true)?;

        let now = SystemTime::now();
        metadata.time_created = Some(now);
        metadata.time_expires = metadata.expiration_policy.expires_in().map(|ttl| now + ttl);

        Ok(metadata)
    }

    /// Validates internal consistency of the metadata.
    ///
    /// A time-based [`expiration_policy`](Self::expiration_policy) must carry a resolved
    /// [`time_expires`](Self::time_expires); backends rely on this to persist a concrete
    /// expiration.
    pub fn validate(&self) -> Result<(), Error> {
        if self.expiration_policy.is_timeout() && self.time_expires.is_none() {
            return Err(Error::Invariant(
                "expiration policy requires a resolved expiration time",
            ));
        }
        Ok(())
    }

    /// Checks whether this object's TTI deadline needs bumping.
    ///
    /// See [`ExpirationPolicy::check_tti_bump`] for details.
    pub fn check_tti_bump(&self, access_time: SystemTime) -> Option<SystemTime> {
        self.expiration_policy
            .check_tti_bump(self.time_expires, access_time)
    }

    /// Extracts public API metadata from the given [`HeaderMap`].
    ///
    /// A prefix can be also be provided which is being stripped from custom non-standard headers.
    pub fn from_headers(headers: &HeaderMap, prefix: &str) -> Result<Self, Error> {
        Self::parse_headers(headers, prefix, false)
    }

    /// Parses metadata from the given [`HeaderMap`].
    ///
    /// When `skip_read_only` is set, read-only attributes are not parsed off the headers, so a
    /// malformed client-supplied value cannot fail the parse. A prefix can also be provided which
    /// is stripped from custom non-standard headers.
    fn parse_headers(
        headers: &HeaderMap,
        prefix: &str,
        skip_read_only: bool,
    ) -> Result<Self, Error> {
        let mut metadata = Metadata::default();

        for (name, value) in headers {
            match *name {
                // standard HTTP headers
                header::CONTENT_TYPE => {
                    let content_type = value.to_str()?;
                    validate_content_type(content_type)?;
                    metadata.content_type = content_type.to_owned().into();
                }
                header::CONTENT_ENCODING => {
                    let compression = value.to_str()?;
                    metadata.compression = Some(Compression::from_str(compression)?);
                }
                _ => {
                    let Some(name) = name.as_str().strip_prefix(prefix) else {
                        continue;
                    };

                    match name {
                        // Objectstore first-class metadata
                        HEADER_EXPIRATION => {
                            let expiration_policy = value.to_str()?;
                            metadata.expiration_policy =
                                ExpirationPolicy::from_str(expiration_policy)?;
                        }
                        HEADER_TIME_CREATED if !skip_read_only => {
                            let timestamp = value.to_str()?;
                            let time = parse_rfc3339(timestamp)?;
                            metadata.time_created = Some(time);
                        }
                        HEADER_TIME_EXPIRES if !skip_read_only => {
                            let timestamp = value.to_str()?;
                            let time = parse_rfc3339(timestamp)?;
                            metadata.time_expires = Some(time);
                        }
                        HEADER_ORIGIN => {
                            metadata.origin = Some(value.to_str()?.to_owned());
                        }
                        HEADER_FILENAME => {
                            metadata.filename = Some(value.to_str()?.to_owned());
                        }
                        _ => {
                            // customer-provided metadata
                            if let Some(name) = name.strip_prefix(HEADER_META_PREFIX) {
                                let value = value.to_str()?;
                                metadata.custom.insert(name.into(), value.into());
                            }
                        }
                    }
                }
            }
        }

        Ok(metadata)
    }

    /// Turns the metadata into a [`HeaderMap`] for the public API.
    ///
    /// It will prefix any non-standard headers with the given `prefix`. GCS-specific headers are
    /// not emitted; backends handle those separately.
    pub fn to_headers(&self, prefix: &str) -> Result<HeaderMap, Error> {
        let Self {
            content_type,
            compression,
            origin,
            filename,
            expiration_policy,
            time_created,
            time_expires,
            size: _,
            custom,
        } = self;

        let mut headers = HeaderMap::new();

        // standard headers
        headers.append(header::CONTENT_TYPE, content_type.parse()?);
        if let Some(compression) = compression {
            headers.append(header::CONTENT_ENCODING, compression.as_str().parse()?);
        }

        // Objectstore first-class metadata
        if *expiration_policy != ExpirationPolicy::Manual {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_EXPIRATION}"))?;
            headers.append(name, expiration_policy.to_string().parse()?);
        }
        if let Some(time) = time_created {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_TIME_CREATED}"))?;
            let timestamp = format_rfc3339_micros(*time);
            headers.append(name, timestamp.to_string().parse()?);
        }
        if let Some(time) = time_expires {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_TIME_EXPIRES}"))?;
            let timestamp = format_rfc3339_micros(*time);
            headers.append(name, timestamp.to_string().parse()?);
        }
        if let Some(origin) = origin {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_ORIGIN}"))?;
            headers.append(name, origin.parse()?);
        }
        if let Some(filename) = filename {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_FILENAME}"))?;
            headers.append(name, filename.parse()?);
        }

        // customer-provided metadata
        for (key, value) in custom {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_META_PREFIX}{key}"))?;
            headers.append(name, value.parse()?);
        }

        Ok(headers)
    }
}

/// Validates that `content_type` is a valid [IANA Media
/// Type](https://www.iana.org/assignments/media-types/media-types.xhtml).
fn validate_content_type(content_type: &str) -> Result<(), Error> {
    mediatype::MediaType::parse(content_type)?;
    Ok(())
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            expiration_policy: ExpirationPolicy::Manual,
            time_created: None,
            time_expires: None,
            content_type: DEFAULT_CONTENT_TYPE.into(),
            compression: None,
            origin: None,
            filename: None,
            size: None,
            custom: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_headers_with_origin() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        headers.insert(HEADER_ORIGIN, "203.0.113.42".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(metadata.origin.as_deref(), Some("203.0.113.42"));
        assert_eq!(metadata.content_type, "text/plain");
    }

    #[test]
    fn from_headers_without_origin() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert!(metadata.origin.is_none());
    }

    #[test]
    fn to_headers_with_origin() {
        let metadata = Metadata {
            origin: Some("203.0.113.42".into()),
            ..Default::default()
        };

        let headers = metadata.to_headers("").unwrap();
        assert_eq!(headers.get(HEADER_ORIGIN).unwrap(), "203.0.113.42");
    }

    #[test]
    fn to_headers_without_origin() {
        let metadata = Metadata::default();
        let headers = metadata.to_headers("").unwrap();
        assert!(headers.get(HEADER_ORIGIN).is_none());
    }

    #[test]
    fn origin_header_roundtrip() {
        let metadata = Metadata {
            origin: Some("203.0.113.42".into()),
            ..Default::default()
        };

        let headers = metadata.to_headers("").unwrap();
        let roundtripped = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(roundtripped.origin, metadata.origin);
    }

    #[test]
    fn from_headers_with_filename() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_FILENAME, "report.pdf".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(metadata.filename.as_deref(), Some("report.pdf"));
    }

    #[test]
    fn from_headers_without_filename() {
        let headers = HeaderMap::new();
        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert!(metadata.filename.is_none());
    }

    #[test]
    fn to_headers_with_filename() {
        let metadata = Metadata {
            filename: Some("report.pdf".into()),
            ..Default::default()
        };

        let headers = metadata.to_headers("").unwrap();
        assert_eq!(headers.get(HEADER_FILENAME).unwrap(), "report.pdf");
    }

    #[test]
    fn to_headers_without_filename() {
        let metadata = Metadata::default();
        let headers = metadata.to_headers("").unwrap();
        assert!(headers.get(HEADER_FILENAME).is_none());
    }

    #[test]
    fn filename_header_roundtrip() {
        let metadata = Metadata {
            filename: Some("report.pdf".into()),
            ..Default::default()
        };

        let headers = metadata.to_headers("").unwrap();
        let roundtripped = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(roundtripped.filename, metadata.filename);
    }

    #[test]
    fn from_headers_content_type_and_encoding() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("content-encoding", "zstd".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(metadata.content_type, "application/json");
        assert_eq!(metadata.compression, Some(Compression::Zstd));
    }

    #[test]
    fn from_headers_expiration_policy() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_EXPIRATION, "ttl:30s".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(
            metadata.expiration_policy,
            ExpirationPolicy::TimeToLive(Duration::from_secs(30))
        );
    }

    #[test]
    fn from_headers_timestamps() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HEADER_TIME_CREATED,
            "2024-01-15T12:00:00.000000Z".parse().unwrap(),
        );
        headers.insert(
            HEADER_TIME_EXPIRES,
            "2024-01-16T12:00:00.000000Z".parse().unwrap(),
        );

        let metadata = Metadata::from_headers(&headers, "").unwrap();
        assert!(metadata.time_created.is_some());
        assert!(metadata.time_expires.is_some());
    }

    #[test]
    fn from_insert_headers_ignores_read_only_fields() {
        // Read-only and output attributes must never be taken from an untrusted
        // client request, even if the client supplies the headers.
        let forged_created = "2024-01-15T12:00:00.000000Z";
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        headers.insert(HEADER_TIME_CREATED, forged_created.parse().unwrap());
        headers.insert(
            HEADER_TIME_EXPIRES,
            "2024-01-16T12:00:00.000000Z".parse().unwrap(),
        );

        let metadata = Metadata::from_insert_headers(&headers, "").unwrap();
        // `time_created` is stamped by the server, not the client's forged value.
        let created = metadata.time_created.unwrap();
        assert_ne!(created, parse_rfc3339(forged_created).unwrap());
        assert!(metadata.time_expires.is_none());
        assert!(metadata.size.is_none());
        // Client-settable fields are still parsed.
        assert_eq!(metadata.content_type, "text/plain");
    }

    #[test]
    fn from_insert_headers_ignores_malformed_read_only_fields() {
        // A malformed read-only header must not fail the write: it is skipped, not parsed.
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_TIME_CREATED, "not-a-timestamp".parse().unwrap());
        headers.insert(HEADER_TIME_EXPIRES, "not-a-timestamp".parse().unwrap());

        let metadata = Metadata::from_insert_headers(&headers, "").unwrap();
        assert!(metadata.time_created.is_some());
        assert!(metadata.time_expires.is_none());
    }

    #[test]
    fn from_insert_headers_resolves_time_expires_for_ttl() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_EXPIRATION, "ttl:30s".parse().unwrap());

        let metadata = Metadata::from_insert_headers(&headers, "").unwrap();
        let created = metadata.time_created.unwrap();
        let expires = metadata.time_expires.unwrap();
        // Both timestamps derive from the same `now`, so the expiry is exact.
        assert_eq!(expires, created + Duration::from_secs(30));
    }

    #[test]
    fn from_insert_headers_resolves_time_expires_for_tti() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_EXPIRATION, "tti:1h".parse().unwrap());

        let metadata = Metadata::from_insert_headers(&headers, "").unwrap();
        let created = metadata.time_created.unwrap();
        let expires = metadata.time_expires.unwrap();
        assert_eq!(expires, created + Duration::from_hours(1));
    }

    #[test]
    fn from_insert_headers_manual_leaves_time_expires_none() {
        let headers = HeaderMap::new();
        let metadata = Metadata::from_insert_headers(&headers, "").unwrap();
        assert_eq!(metadata.expiration_policy, ExpirationPolicy::Manual);
        assert!(metadata.time_expires.is_none());
    }

    #[test]
    fn validate_accepts_resolved_timeout() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(30)),
            time_expires: Some(SystemTime::now() + Duration::from_secs(30)),
            ..Default::default()
        };
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn validate_accepts_manual_without_expiry() {
        let metadata = Metadata::default();
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn validate_rejects_timeout_without_expiry() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_expires: None,
            ..Default::default()
        };
        assert!(matches!(metadata.validate(), Err(Error::Invariant(_))));
    }

    #[test]
    fn from_headers_custom_metadata_with_prefix() {
        let mut headers = HeaderMap::new();
        // Simulate a backend that prefixes headers, e.g. "x-goog-meta-"
        let prefix = "x-goog-meta-";
        let expiration_header: HeaderName = format!("{prefix}{HEADER_EXPIRATION}").parse().unwrap();
        headers.insert(expiration_header, "tti:1h".parse().unwrap());

        let custom_header: HeaderName = format!("{prefix}{HEADER_META_PREFIX}my-key")
            .parse()
            .unwrap();
        headers.insert(custom_header, "my-value".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, prefix).unwrap();
        assert_eq!(
            metadata.expiration_policy,
            ExpirationPolicy::TimeToIdle(Duration::from_hours(1))
        );
        assert_eq!(metadata.custom.get("my-key").unwrap(), "my-value");
    }

    #[test]
    fn from_headers_invalid_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "not a valid media type!".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::ContentType(_)));
    }

    #[test]
    fn from_headers_invalid_compression() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "brotli".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::Compression));
    }

    #[test]
    fn from_headers_invalid_expiration() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_EXPIRATION, "garbage".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::Expiration(_)));
    }

    #[test]
    fn from_headers_invalid_timestamp() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_TIME_CREATED, "not-a-timestamp".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::CreationTime(_)));
    }

    #[test]
    fn to_headers_all_fields() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_mins(1)),
            time_created: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            time_expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_060)),
            content_type: "text/html".into(),
            compression: Some(Compression::Zstd),
            origin: Some("10.0.0.1".into()),
            filename: Some("report.pdf".into()),
            size: None,
            custom: BTreeMap::from([("foo".into(), "bar".into())]),
        };

        let headers = metadata.to_headers("pfx-").unwrap();
        let map: BTreeMap<_, _> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.to_str().unwrap()))
            .collect();

        insta::assert_debug_snapshot!(map, @r#"
        {
            "content-encoding": "zstd",
            "content-type": "text/html",
            "pfx-x-sn-expiration": "ttl:1m",
            "pfx-x-sn-filename": "report.pdf",
            "pfx-x-sn-origin": "10.0.0.1",
            "pfx-x-sn-time-created": "2023-11-14T22:13:20.000000Z",
            "pfx-x-sn-time-expires": "2023-11-14T22:14:20.000000Z",
            "pfx-x-snme-foo": "bar",
        }
        "#);
    }

    #[test]
    fn full_roundtrip_all_fields() {
        let prefix = "x-test-";
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(2)),
            time_created: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            time_expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_007_200)),
            content_type: "image/png".into(),
            compression: Some(Compression::Zstd),
            origin: Some("192.168.1.1".into()),
            filename: Some("image.png".into()),
            size: None,
            custom: BTreeMap::from([
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ]),
        };

        let headers = metadata.to_headers(prefix).unwrap();
        let roundtripped = Metadata::from_headers(&headers, prefix).unwrap();

        assert_eq!(roundtripped.expiration_policy, metadata.expiration_policy);
        assert_eq!(roundtripped.content_type, metadata.content_type);
        assert_eq!(roundtripped.compression, metadata.compression);
        assert_eq!(roundtripped.origin, metadata.origin);
        assert_eq!(roundtripped.filename, metadata.filename);
        assert_eq!(roundtripped.time_created, metadata.time_created);
        assert_eq!(roundtripped.time_expires, metadata.time_expires);
        assert_eq!(roundtripped.custom, metadata.custom);
    }

    #[test]
    fn from_headers_empty() {
        let headers = HeaderMap::new();
        let metadata = Metadata::from_headers(&headers, "x-goog-meta-").unwrap();
        assert_eq!(metadata, Metadata::default());
    }

    #[test]
    fn from_headers_invalid_time_expires() {
        let mut headers = HeaderMap::new();
        let name: HeaderName = format!("x-goog-meta-{HEADER_TIME_EXPIRES}")
            .parse()
            .unwrap();
        headers.insert(name, "not-a-timestamp".parse().unwrap());

        // NOTE: This produces InvalidCreationTime even for time_expires because
        // both fields share the same humantime::TimestampError #[from] conversion.
        assert!(Metadata::from_headers(&headers, "x-goog-meta-").is_err());
    }

    #[test]
    fn serde_roundtrip_default() {
        let metadata = Metadata::default();
        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metadata);
    }

    #[test]
    fn serde_roundtrip_all_fields() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_created: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            time_expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_003_600)),
            content_type: "application/json".into(),
            compression: Some(Compression::Zstd),
            origin: Some("10.0.0.1".into()),
            filename: Some("data.json".into()),
            size: Some(1024),
            custom: BTreeMap::from([("key".into(), "value".into())]),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metadata);
    }

    #[test]
    fn size_not_included_in_headers() {
        let metadata = Metadata {
            size: Some(42),
            ..Default::default()
        };

        let headers = metadata.to_headers("x-goog-meta-").unwrap();
        let has_size_header = headers.keys().any(|k| k.as_str().contains("size"));
        assert!(!has_size_header);
    }

    #[test]
    fn default_metadata() {
        let metadata = Metadata::default();
        assert_eq!(metadata.content_type, DEFAULT_CONTENT_TYPE);
        assert_eq!(metadata.expiration_policy, ExpirationPolicy::Manual);
        assert!(metadata.compression.is_none());
        assert!(metadata.origin.is_none());
        assert!(metadata.filename.is_none());
        assert!(metadata.time_created.is_none());
        assert!(metadata.time_expires.is_none());
        assert!(metadata.size.is_none());
        assert!(metadata.custom.is_empty());
    }

    #[test]
    fn expiration_display_roundtrip() {
        let cases = [
            ExpirationPolicy::Manual,
            ExpirationPolicy::TimeToLive(Duration::from_secs(30)),
            ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
        ];

        for policy in cases {
            let displayed = policy.to_string();
            let parsed: ExpirationPolicy = displayed.parse().unwrap();
            assert_eq!(parsed, policy);
        }
    }

    #[test]
    fn expiration_parse_invalid() {
        assert!(ExpirationPolicy::from_str("garbage").is_err());
        assert!(ExpirationPolicy::from_str("ttl:").is_err());
        assert!(ExpirationPolicy::from_str("").is_err());
    }

    #[test]
    fn expiration_policy_helpers() {
        assert_eq!(ExpirationPolicy::Manual.expires_in(), None);
        assert!(ExpirationPolicy::Manual.is_manual());
        assert!(!ExpirationPolicy::Manual.is_timeout());

        let ttl = ExpirationPolicy::TimeToLive(Duration::from_mins(1));
        assert_eq!(ttl.expires_in(), Some(Duration::from_mins(1)));
        assert!(ttl.is_timeout());
        assert!(!ttl.is_manual());

        let tti = ExpirationPolicy::TimeToIdle(Duration::from_mins(2));
        assert_eq!(tti.expires_in(), Some(Duration::from_mins(2)));
        assert!(tti.is_timeout());
        assert!(!tti.is_manual());
    }

    #[test]
    fn compression_display_roundtrip() {
        let displayed = Compression::Zstd.to_string();
        assert_eq!(displayed, "zstd");
        let parsed: Compression = displayed.parse().unwrap();
        assert_eq!(parsed, Compression::Zstd);
    }

    #[test]
    fn compression_parse_invalid() {
        assert!(Compression::from_str("gzip").is_err());
        assert!(Compression::from_str("").is_err());
    }

    #[test]
    fn check_tti_bump_returns_none_for_manual() {
        let metadata = Metadata::default();
        assert!(metadata.check_tti_bump(SystemTime::now()).is_none());
    }

    #[test]
    fn check_tti_bump_returns_none_for_ttl() {
        let now = SystemTime::now();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(now + Duration::from_hours(1)),
            ..Default::default()
        };
        assert!(metadata.check_tti_bump(now).is_none());
    }

    #[test]
    fn check_tti_bump_returns_none_when_fresh() {
        let now = SystemTime::now();
        let tti = Duration::from_hours(2 * 24);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(now + tti),
            ..Default::default()
        };
        assert!(metadata.check_tti_bump(now).is_none());
    }

    #[test]
    fn check_tti_bump_returns_new_deadline_when_stale() {
        let now = SystemTime::now();
        let tti = Duration::from_hours(2 * 24);
        let debounce = tti / 4;
        let stale_deadline = now + tti - debounce - Duration::from_mins(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(stale_deadline),
            ..Default::default()
        };
        let new_deadline = metadata.check_tti_bump(now).unwrap();
        assert_eq!(new_deadline, now + tti);
    }

    #[test]
    fn check_tti_bump_short_tti_triggers_bump() {
        let now = SystemTime::now();
        let tti = Duration::from_hours(2);
        let debounce = tti / 4;
        let stale_deadline = now + tti - debounce - Duration::from_mins(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(stale_deadline),
            ..Default::default()
        };
        let new_deadline = metadata.check_tti_bump(now).unwrap();
        assert_eq!(new_deadline, now + tti);
    }

    #[test]
    fn check_tti_bump_debounce_caps_at_24h() {
        let now = SystemTime::now();
        let tti = Duration::from_hours(30 * 24);
        let capped_debounce = Duration::from_hours(24);
        let stale_deadline = now + tti - capped_debounce - Duration::from_mins(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            time_expires: Some(stale_deadline),
            ..Default::default()
        };
        assert!(metadata.check_tti_bump(now).is_some());

        let fresh_deadline = now + tti - capped_debounce + Duration::from_mins(1);
        let metadata = Metadata {
            time_expires: Some(fresh_deadline),
            ..metadata
        };
        assert!(metadata.check_tti_bump(now).is_none());
    }

    #[test]
    fn check_tti_bump_returns_none_when_time_expires_missing() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_expires: None,
            ..Default::default()
        };
        assert!(metadata.check_tti_bump(SystemTime::now()).is_none());
    }
}
