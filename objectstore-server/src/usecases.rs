//! Configuration and validation for use case properties.
//!
//! Use cases are user-defined strings that namespace objects (e.g. `attachments`,
//! `debug-files`). This module provides a central place to configure properties
//! of use cases, such as which expiration policies are permitted and any duration
//! caps.
//!
//! Unconfigured use cases receive the default configuration: all expiration
//! policies are allowed with no duration caps.
//!
//! # YAML Configuration
//!
//! ```yaml
//! usecases:
//!   attachments:
//!     expiration:
//!       manual:
//!         allowed: false
//!       tti:
//!         allowed: false
//!       max: "90d"
//!   debug-files:
//!     expiration:
//!       max: "90d"
//! ```

use std::collections::HashMap;
use std::time::Duration;

use objectstore_types::duration::format_duration;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Configuration for all use case properties.
///
/// Maps use case names to their configuration. Use cases not present in the map
/// receive the default configuration (all expiration policies allowed, no caps).
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
pub struct UseCases(pub HashMap<String, UseCaseConfig>);

impl UseCases {
    /// Validates metadata against the configuration for the given use case.
    ///
    /// Returns an error if any metadata field violates the use case's policy.
    /// Use cases not present in the configuration are always valid.
    pub fn validate(&self, usecase: &str, metadata: &Metadata) -> Result<(), UseCaseError> {
        if let Some(config) = self.0.get(usecase) {
            config.validate(usecase, metadata)?
        }
        Ok(())
    }

    /// Returns the maximum allowed expiration duration for the given use case, if configured.
    ///
    /// Applies to both TTL and TTI expiration policies. `None` means no limit.
    pub fn get_max_expiry(&self, usecase: &str) -> Option<Duration> {
        self.0.get(usecase).and_then(|config| config.expiration.max)
    }
}

/// Configuration for a single use case.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct UseCaseConfig {
    /// Expiration policy constraints for this use case.
    pub expiration: ExpirationConfig,
}

impl UseCaseConfig {
    fn validate(&self, usecase: &str, metadata: &Metadata) -> Result<(), UseCaseError> {
        let policy = metadata.expiration_policy;
        let allowed = match policy {
            ExpirationPolicy::Manual => self.expiration.manual.allowed,
            ExpirationPolicy::TimeToLive(_) => self.expiration.ttl.allowed,
            ExpirationPolicy::TimeToIdle(_) => self.expiration.tti.allowed,
        };
        if !allowed {
            return Err(UseCaseError::PolicyNotAllowed {
                usecase: usecase.to_owned(),
                policy,
            });
        }

        if let Some(max) = self.expiration.max
            && let Some(duration) = policy.expires_in()
            && duration > max
        {
            return Err(UseCaseError::DurationExceeded {
                usecase: usecase.to_owned(),
                duration: format_duration(duration).to_string(),
                max: format_duration(max).to_string(),
            });
        }
        Ok(())
    }
}

/// Controls which expiration policies are allowed and their duration constraints.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ExpirationConfig {
    /// Configuration for the [`ExpirationPolicy::Manual`] policy.
    pub manual: PolicyConfig,
    /// Configuration for the [`ExpirationPolicy::TimeToLive`] policy.
    pub ttl: PolicyConfig,
    /// Configuration for the [`ExpirationPolicy::TimeToIdle`] policy.
    pub tti: PolicyConfig,
    /// Maximum allowed duration for TTL and TTI policies. `None` means no limit.
    ///
    /// When extending the lifetime of an existing object, this limit applies
    /// from the request time regardless of the object's creation time.
    #[serde(default, with = "humantime_serde")]
    pub max: Option<Duration>,
}

/// Configuration for an expiration policy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct PolicyConfig {
    /// Whether this expiration policy is allowed. Defaults to `true`.
    pub allowed: bool,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self { allowed: true }
    }
}

/// Errors produced when metadata violates a use case's configuration.
#[derive(Debug, Error)]
pub enum UseCaseError {
    /// The expiration policy kind is not permitted for this use case.
    #[error("expiration policy '{policy}' is not allowed for use case '{usecase}'")]
    PolicyNotAllowed {
        /// The use case name.
        usecase: String,
        /// The disallowed policy, in wire format.
        policy: ExpirationPolicy,
    },

    /// The expiration duration exceeds the maximum for this use case.
    #[error("expiration duration {duration} exceeds maximum of {max} for use case '{usecase}'")]
    DurationExceeded {
        /// The use case name.
        usecase: String,
        /// The requested duration, in wire format.
        duration: String,
        /// The configured maximum, in wire format.
        max: String,
    },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use objectstore_types::metadata::{ExpirationPolicy, Metadata};

    use super::*;

    fn make_metadata(policy: ExpirationPolicy) -> Metadata {
        Metadata {
            expiration_policy: policy,
            ..Metadata::default()
        }
    }

    fn usecases_from(config: UseCaseConfig) -> UseCases {
        let mut map = HashMap::new();
        map.insert("test".to_owned(), config);
        UseCases(map)
    }

    // --- unconfigured use case ---

    #[test]
    fn unconfigured_usecase_allows_manual() {
        let usecases = UseCases::default();
        let metadata = make_metadata(ExpirationPolicy::Manual);
        usecases.validate("anything", &metadata).unwrap();
    }

    #[test]
    fn unconfigured_usecase_allows_ttl() {
        let usecases = UseCases::default();
        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(1)));
        usecases.validate("anything", &metadata).unwrap();
    }

    #[test]
    fn unconfigured_usecase_allows_tti() {
        let usecases = UseCases::default();
        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_hours(1)));
        usecases.validate("anything", &metadata).unwrap();
    }

    // --- manual policy ---

    #[test]
    fn manual_disallowed_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                manual: PolicyConfig { allowed: false },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::Manual);
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::PolicyNotAllowed { .. }));
    }

    #[test]
    fn manual_allowed_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                manual: PolicyConfig { allowed: true },
                max: Some(Duration::ZERO),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::Manual);
        usecases.validate("test", &metadata).unwrap();
    }

    // --- ttl policy ---

    #[test]
    fn ttl_disallowed_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                ttl: PolicyConfig { allowed: false },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(1)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::PolicyNotAllowed { .. }));
    }

    #[test]
    fn ttl_within_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                max: Some(Duration::from_hours(2)),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(1)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn ttl_at_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                max: Some(Duration::from_hours(1)),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(1)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn ttl_exceeds_max_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                max: Some(Duration::from_hours(1)),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(2)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::DurationExceeded { .. }));
    }

    // --- tti policy ---

    #[test]
    fn tti_disallowed_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: PolicyConfig { allowed: false },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_hours(1)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::PolicyNotAllowed { .. }));
    }

    #[test]
    fn tti_within_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                max: Some(Duration::from_hours(2)),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_hours(1)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn tti_exceeds_max_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                max: Some(Duration::from_hours(1)),
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_hours(2)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::DurationExceeded { .. }));
    }

    // --- partial config ---

    #[test]
    fn other_policies_unaffected_when_only_tti_restricted() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: PolicyConfig { allowed: false },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::Manual);
        usecases.validate("test", &metadata).unwrap();

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_hours(1)));
        usecases.validate("test", &metadata).unwrap();
    }
}
