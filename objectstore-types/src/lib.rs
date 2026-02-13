//! This is a collection of types shared among various objectstore crates.
//!
//! It primarily includes metadata-related structures being used by both the client and server/service
//! components.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod scope;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use http::header::{self, HeaderMap, HeaderName};
use humantime::{
    format_duration, format_rfc3339_micros, format_rfc3339_seconds, parse_duration, parse_rfc3339,
};
use serde::{Deserialize, Serialize};

/// The custom HTTP header that contains the serialized [`ExpirationPolicy`].
pub const HEADER_EXPIRATION: &str = "x-sn-expiration";
/// The custom HTTP header that contains the serialized redirect tombstone.
pub const HEADER_REDIRECT_TOMBSTONE: &str = "x-sn-redirect-tombstone";
/// The custom HTTP header that contains the object creation time.
pub const HEADER_TIME_CREATED: &str = "x-sn-time-created";
/// The custom HTTP header that contains the object expiration time.
pub const HEADER_TIME_EXPIRES: &str = "x-sn-time-expires";
/// The custom HTTP header that contains the origin of the object.
pub const HEADER_ORIGIN: &str = "x-sn-origin";
/// The prefix for custom HTTP headers containing custom per-object metadata.
pub const HEADER_META_PREFIX: &str = "x-snme-";

/// The default content type for objects without a known content type.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Errors that can happen dealing with metadata
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any problems dealing with http headers, essentially converting to/from [`str`].
    #[error("error dealing with http headers")]
    Header(#[from] Option<http::Error>),
    /// The value for the expiration policy is invalid.
    #[error("invalid expiration policy value")]
    InvalidExpiration(#[from] Option<humantime::DurationError>),
    /// The compression algorithm is invalid.
    #[error("invalid compression value")]
    InvalidCompression,
    /// The content type is invalid.
    #[error("invalid content type")]
    InvalidContentType(#[from] mediatype::MediaTypeError),
    /// The creation time is invalid.
    #[error("invalid creation time")]
    InvalidCreationTime(#[from] humantime::TimestampError),
}
impl From<http::header::InvalidHeaderValue> for Error {
    fn from(err: http::header::InvalidHeaderValue) -> Self {
        Self::Header(Some(err.into()))
    }
}
impl From<http::header::InvalidHeaderName> for Error {
    fn from(err: http::header::InvalidHeaderName) -> Self {
        Self::Header(Some(err.into()))
    }
}
impl From<http::header::ToStrError> for Error {
    fn from(_err: http::header::ToStrError) -> Self {
        // the error happens when converting a header value back to a `str`
        Self::Header(None)
    }
}

/// The per-object expiration policy
///
/// We support automatic time-to-live and time-to-idle policies.
/// Setting this to `Manual` means that the object has no automatic policy, and will not be
/// garbage-collected automatically. It essentially lives forever until manually deleted.
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
        Err(Error::InvalidExpiration(None))
    }
}

/// The compression algorithm of an object to upload.
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
            _ => Err(Error::InvalidCompression),
        }
    }
}

/// Permissions that control whether different operations are authorized.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Permission {
    /// The permission required to read objects from objectstore.
    #[serde(rename = "object.read")]
    ObjectRead,

    /// The permission required to write/overwrite objects in objectstore.
    #[serde(rename = "object.write")]
    ObjectWrite,

    /// The permission required to delete objects from objectstore.
    #[serde(rename = "object.delete")]
    ObjectDelete,
}

impl Permission {
    /// Convenience function for creating a set with read, write, and delete permissions.
    pub fn rwd() -> HashSet<Permission> {
        HashSet::from([
            Permission::ObjectRead,
            Permission::ObjectWrite,
            Permission::ObjectDelete,
        ])
    }
}

/// Per-object Metadata.
///
/// This includes special metadata like the expiration policy and compression used,
/// as well as arbitrary user-provided metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Metadata {
    /// The object/metadata denotes a "redirect key".
    ///
    /// This means that this particular object is just a tombstone, and the real thing
    /// is rather found on the other backend.
    /// In practice this means that the tombstone is stored on the "HighVolume" backend,
    /// to avoid unnecessarily slow "not found" requests on the "LongTerm" backend.
    ///
    /// **Important:** This field must remain the first field in the struct.
    /// The BigTable backend uses a regex predicate on the serialized JSON that
    /// assumes `is_redirect_tombstone` appears at the start of the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_redirect_tombstone: Option<bool>,

    /// The expiration policy of the object.
    #[serde(skip_serializing_if = "ExpirationPolicy::is_manual")]
    pub expiration_policy: ExpirationPolicy,

    /// The creation/last replacement time of the object, if known.
    ///
    /// This is set by the server every time an object is put, i.e. when objects are first created
    /// and when existing objects are overwritten.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_created: Option<SystemTime>,

    /// The expiration time of the object, if any, in accordance with its expiration policy.
    ///
    /// When using a Time To Idle expiration policy, this value will reflect the expiration
    /// timestamp present prior to the current access to the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_expires: Option<SystemTime>,

    /// The content type of the object, if known.
    pub content_type: Cow<'static, str>,

    /// The compression algorithm used for this object, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    /// The origin of the object, typically the IP address of the original source.
    ///
    /// This is an optional but encouraged field that tracks where the payload was
    /// originally obtained from (e.g., the IP of a Sentry SDK or CLI).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,

    /// Size of the data in bytes, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    /// Some arbitrary user-provided metadata.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, String>,
}

impl Metadata {
    /// Extracts metadata from the given [`HeaderMap`].
    ///
    /// A prefix can be also be provided which is being stripped from custom non-standard headers.
    pub fn from_headers(headers: &HeaderMap, prefix: &str) -> Result<Self, Error> {
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
                        HEADER_REDIRECT_TOMBSTONE => {
                            if value.to_str()? == "true" {
                                metadata.is_redirect_tombstone = Some(true);
                            }
                        }
                        HEADER_TIME_CREATED => {
                            let timestamp = value.to_str()?;
                            let time = parse_rfc3339(timestamp)?;
                            metadata.time_created = Some(time);
                        }
                        HEADER_TIME_EXPIRES => {
                            let timestamp = value.to_str()?;
                            let time = parse_rfc3339(timestamp)?;
                            metadata.time_expires = Some(time);
                        }
                        HEADER_ORIGIN => {
                            metadata.origin = Some(value.to_str()?.to_owned());
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

    /// Turns the metadata into a [`HeaderMap`].
    ///
    /// It will prefix any non-standard headers with the given `prefix`.
    /// If the `with_expiration` parameter is set, it will additionally resolve the expiration policy
    /// into a specific RFC3339 datetime, and set that as the `Custom-Time` header.
    pub fn to_headers(&self, prefix: &str, with_expiration: bool) -> Result<HeaderMap, Error> {
        let Self {
            is_redirect_tombstone,
            content_type,
            compression,
            origin,
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
        if matches!(is_redirect_tombstone, Some(true)) {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_REDIRECT_TOMBSTONE}"))?;
            headers.append(name, "true".parse()?);
        }
        if *expiration_policy != ExpirationPolicy::Manual {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_EXPIRATION}"))?;
            headers.append(name, expiration_policy.to_string().parse()?);
            if with_expiration {
                let expires_in = expiration_policy.expires_in().unwrap_or_default();
                let expires_at = format_rfc3339_seconds(SystemTime::now() + expires_in);
                headers.append("x-goog-custom-time", expires_at.to_string().parse()?);
            }
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

        // customer-provided metadata
        for (key, value) in custom {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_META_PREFIX}{key}"))?;
            headers.append(name, value.parse()?);
        }

        Ok(headers)
    }

    /// Returns `true` if this metadata represents a redirect tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.is_redirect_tombstone == Some(true)
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
            is_redirect_tombstone: None,
            expiration_policy: ExpirationPolicy::Manual,
            time_created: None,
            time_expires: None,
            content_type: DEFAULT_CONTENT_TYPE.into(),
            compression: None,
            origin: None,
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

        let headers = metadata.to_headers("", false).unwrap();
        assert_eq!(headers.get(HEADER_ORIGIN).unwrap(), "203.0.113.42");
    }

    #[test]
    fn to_headers_without_origin() {
        let metadata = Metadata::default();
        let headers = metadata.to_headers("", false).unwrap();
        assert!(headers.get(HEADER_ORIGIN).is_none());
    }

    #[test]
    fn origin_header_roundtrip() {
        let metadata = Metadata {
            origin: Some("203.0.113.42".into()),
            ..Default::default()
        };

        let headers = metadata.to_headers("", false).unwrap();
        let roundtripped = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(roundtripped.origin, metadata.origin);
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
    fn from_headers_custom_metadata_with_prefix() {
        let mut headers = HeaderMap::new();
        // Simulate a backend that prefixes headers, e.g. "x-goog-meta-"
        let prefix = "x-goog-meta-";
        let expiration_header: HeaderName =
            format!("{prefix}{HEADER_EXPIRATION}").parse().unwrap();
        headers.insert(expiration_header, "tti:1h".parse().unwrap());

        let custom_header: HeaderName =
            format!("{prefix}{HEADER_META_PREFIX}my-key").parse().unwrap();
        headers.insert(custom_header, "my-value".parse().unwrap());

        let metadata = Metadata::from_headers(&headers, prefix).unwrap();
        assert_eq!(
            metadata.expiration_policy,
            ExpirationPolicy::TimeToIdle(Duration::from_secs(3600))
        );
        assert_eq!(metadata.custom.get("my-key").unwrap(), "my-value");
    }

    #[test]
    fn from_headers_invalid_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "not a valid media type!".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::InvalidContentType(_)));
    }

    #[test]
    fn from_headers_invalid_compression() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "brotli".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::InvalidCompression));
    }

    #[test]
    fn from_headers_invalid_expiration() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_EXPIRATION, "garbage".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::InvalidExpiration(_)));
    }

    #[test]
    fn from_headers_invalid_timestamp() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_TIME_CREATED, "not-a-timestamp".parse().unwrap());

        let err = Metadata::from_headers(&headers, "").unwrap_err();
        assert!(matches!(err, Error::InvalidCreationTime(_)));
    }

    #[test]
    fn to_headers_all_fields() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(60)),
            content_type: "text/html".into(),
            compression: Some(Compression::Zstd),
            origin: Some("10.0.0.1".into()),
            time_created: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            time_expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_060)),
            custom: BTreeMap::from([("foo".into(), "bar".into())]),
            ..Default::default()
        };

        let headers = metadata.to_headers("pfx-", false).unwrap();
        assert_eq!(
            headers.get("content-type").unwrap(),
            "text/html"
        );
        assert_eq!(
            headers.get("content-encoding").unwrap(),
            "zstd"
        );
        assert_eq!(
            headers
                .get(format!("pfx-{HEADER_EXPIRATION}"))
                .unwrap()
                .to_str()
                .unwrap(),
            "ttl:1m"
        );
        assert!(headers
            .get(format!("pfx-{HEADER_TIME_CREATED}"))
            .is_some());
        assert!(headers
            .get(format!("pfx-{HEADER_TIME_EXPIRES}"))
            .is_some());
        assert_eq!(
            headers
                .get(format!("pfx-{HEADER_ORIGIN}"))
                .unwrap()
                .to_str()
                .unwrap(),
            "10.0.0.1"
        );
        assert_eq!(
            headers
                .get(format!("pfx-{HEADER_META_PREFIX}foo"))
                .unwrap()
                .to_str()
                .unwrap(),
            "bar"
        );
    }

    #[test]
    fn to_headers_redirect_tombstone() {
        let metadata = Metadata {
            is_redirect_tombstone: Some(true),
            ..Default::default()
        };

        let headers = metadata.to_headers("", false).unwrap();
        assert_eq!(
            headers.get(HEADER_REDIRECT_TOMBSTONE).unwrap(),
            "true"
        );
    }

    #[test]
    fn to_headers_with_expiration_sets_custom_time() {
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            ..Default::default()
        };

        let headers = metadata.to_headers("", true).unwrap();
        assert!(headers.get("x-goog-custom-time").is_some());

        // Without the flag, no custom-time header
        let headers_no_exp = metadata.to_headers("", false).unwrap();
        assert!(headers_no_exp.get("x-goog-custom-time").is_none());
    }

    #[test]
    fn full_roundtrip_all_fields() {
        let prefix = "x-test-";
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(7200)),
            content_type: "image/png".into(),
            compression: Some(Compression::Zstd),
            origin: Some("192.168.1.1".into()),
            time_created: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            time_expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_007_200)),
            custom: BTreeMap::from([
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ]),
            ..Default::default()
        };

        let headers = metadata.to_headers(prefix, false).unwrap();
        let roundtripped = Metadata::from_headers(&headers, prefix).unwrap();

        assert_eq!(roundtripped.expiration_policy, metadata.expiration_policy);
        assert_eq!(roundtripped.content_type, metadata.content_type);
        assert_eq!(roundtripped.compression, metadata.compression);
        assert_eq!(roundtripped.origin, metadata.origin);
        assert_eq!(roundtripped.time_created, metadata.time_created);
        assert_eq!(roundtripped.time_expires, metadata.time_expires);
        assert_eq!(roundtripped.custom, metadata.custom);
    }

    #[test]
    fn default_metadata() {
        let metadata = Metadata::default();
        assert_eq!(metadata.content_type, DEFAULT_CONTENT_TYPE);
        assert_eq!(metadata.expiration_policy, ExpirationPolicy::Manual);
        assert!(metadata.compression.is_none());
        assert!(metadata.origin.is_none());
        assert!(metadata.time_created.is_none());
        assert!(metadata.time_expires.is_none());
        assert!(metadata.is_redirect_tombstone.is_none());
        assert!(metadata.size.is_none());
        assert!(metadata.custom.is_empty());
    }
}
