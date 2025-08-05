use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use humantime::{format_duration, parse_duration};
use serde::{Deserialize, Serialize};

/// The custom HTTP header that contains the serialized [`ExpirationPolicy`]
pub const HEADER_EXPIRATION: &str = "X-Sn-Expiration";

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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExpirationPolicy {
    /// Manual expiration, meaning no automatic cleanup.
    #[default]
    Manual,
    /// Time to live, with expiration after the specified duration.
    TimeToLive(Duration),
    /// Time to idle, with expiration once the object has not been accessed within the specified duration.
    TimeToIdle(Duration),
}
impl ExpirationPolicy {
    /// Returns the duration after which the object expires.
    pub fn expires_in(&self) -> Duration {
        match self {
            ExpirationPolicy::Manual => Duration::ZERO,
            ExpirationPolicy::TimeToLive(duration) => *duration,
            ExpirationPolicy::TimeToIdle(duration) => *duration,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Compressed using `zstd`.
    Zstd,
    // /// Compressed using `gzip`.
    // Gzip,
    // /// Compressed using `lz4`.
    // Lz4,
}
impl Compression {
    pub fn as_str(&self) -> &str {
        match self {
            Compression::Zstd => "zstd",
            // Compression::Gzip => "gzip",
            // Compression::Lz4 => "lz4",
        }
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
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    // #[serde(skip_serializing_if = "ExpirationPolicy::is_manual")]
    pub expiration_policy: ExpirationPolicy,

    // #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,

    /// Some arbitrary user-provided metadata.
    pub custom: BTreeMap<String, String>,
}
