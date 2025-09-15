//! This is a collection of types shared among various objectstore crates.
//!
//! It primarily includes metadata-related structures being used by both the client and server/service
//! components.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use http::header::{self, HeaderMap, HeaderName};
use humantime::{format_duration, format_rfc3339_seconds, parse_duration};
use serde::{Deserialize, Serialize};

/// The custom HTTP header that contains the serialized [`ExpirationPolicy`].
pub const HEADER_EXPIRATION: &str = "x-sn-expiration";
/// The prefix for custom HTTP headers containing custom per-object metadata.
pub const HEADER_META_PREFIX: &str = "x-snme-";

/// The storage scope for each object
///
/// Each object is stored within a scope. The scope is used for access control, as well as the ability
/// to quickly run queries on all the objects associated with a scope.
/// The scope could also be used as a sharding/routing key in the future.
///
/// The organization / project scope defined here is hierarchical in the sense that
/// analytical aggregations on an organzation level take into account all the project-level objects.
/// However, accessing an object requires supplying both of these original values in order to retrieve it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct Scope {
    /// The organization ID
    pub organization: u64,

    /// The project ID, if we have a project scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<u64>,
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
    type Err = anyhow::Error;

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
        anyhow::bail!("invalid expiration policy")
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
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "zstd" => Compression::Zstd,
            // "gzip" => Compression::Gzip,
            // "lz4" => Compression::Lz4,
            _ => anyhow::bail!("unknown compression algorithm"),
        })
    }
}

/// Per-object Metadata.
///
/// This includes special metadata like the expiration policy and compression used,
/// as well as arbitrary user-provided metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metadata {
    /// The expiration policy of the object.
    // #[serde(skip_serializing_if = "ExpirationPolicy::is_manual")]
    pub expiration_policy: ExpirationPolicy,

    /// The compression algorithm used for this object, if any.
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    /// Some arbitrary user-provided metadata.
    pub custom: BTreeMap<String, String>,
}

impl Metadata {
    /// Extracts metadata from the given [`HeaderMap`].
    ///
    /// A prefix can be also be provided which is being stripped from custom non-standard headers.
    pub fn from_headers(headers: &HeaderMap, prefix: &str) -> anyhow::Result<Self> {
        let mut metadata = Metadata::default();

        for (name, value) in headers {
            if name == header::CONTENT_ENCODING {
                let compression = value.to_str()?;
                metadata.compression = Some(Compression::from_str(compression)?);
            } else if let Some(name) = name.as_str().strip_prefix(prefix) {
                if name == HEADER_EXPIRATION {
                    let expiration_policy = value.to_str()?;
                    metadata.expiration_policy = ExpirationPolicy::from_str(expiration_policy)?;
                } else if let Some(name) = name.strip_prefix(HEADER_META_PREFIX) {
                    let value = value.to_str()?;
                    metadata.custom.insert(name.into(), value.into());
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
    pub fn to_headers(&self, prefix: &str, with_expiration: bool) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        if let Some(compression) = self.compression {
            headers.append(header::CONTENT_ENCODING, compression.as_str().parse()?);
        }
        if self.expiration_policy != ExpirationPolicy::Manual {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_EXPIRATION}"))?;
            headers.append(name, self.expiration_policy.to_string().parse()?);
            if with_expiration {
                let expires_in = self.expiration_policy.expires_in().unwrap_or_default();
                let expires_at = format_rfc3339_seconds(SystemTime::now() + expires_in);
                headers.append("x-goog-custom-time", expires_at.to_string().parse()?);
            }
        }

        for (key, value) in &self.custom {
            let name = HeaderName::try_from(format!("{prefix}{HEADER_META_PREFIX}{key}"))?;
            headers.append(name, value.parse()?);
        }

        Ok(headers)
    }
}
