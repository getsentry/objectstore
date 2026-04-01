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
//!       ttl:
//!         max: "90d"
//!       tti:
//!         allowed: false
//!   debug-files:
//!     expiration:
//!       tti:
//!         max: "90d"
//! ```

use std::collections::HashMap;
use std::time::Duration;

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
        match metadata.expiration_policy {
            ExpirationPolicy::Manual => {
                if !self.expiration.manual.allowed {
                    return Err(UseCaseError::PolicyNotAllowed {
                        usecase: usecase.to_owned(),
                        policy: metadata.expiration_policy,
                    });
                }
            }
            ExpirationPolicy::TimeToLive(duration) => {
                if !self.expiration.ttl.allowed {
                    return Err(UseCaseError::PolicyNotAllowed {
                        usecase: usecase.to_owned(),
                        policy: metadata.expiration_policy,
                    });
                }
                if let Some(max) = self.expiration.ttl.max
                    && duration > max
                {
                    return Err(UseCaseError::DurationExceeded {
                        usecase: usecase.to_owned(),
                        duration: humantime::format_duration(duration).to_string(),
                        max: humantime::format_duration(max).to_string(),
                    });
                }
            }
            ExpirationPolicy::TimeToIdle(duration) => {
                if !self.expiration.tti.allowed {
                    return Err(UseCaseError::PolicyNotAllowed {
                        usecase: usecase.to_owned(),
                        policy: metadata.expiration_policy,
                    });
                }
                if let Some(max) = self.expiration.tti.max
                    && duration > max
                {
                    return Err(UseCaseError::DurationExceeded {
                        usecase: usecase.to_owned(),
                        duration: humantime::format_duration(duration).to_string(),
                        max: humantime::format_duration(max).to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Controls which expiration policies are allowed and their duration constraints.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ExpirationConfig {
    /// Configuration for the [`ExpirationPolicy::Manual`] policy.
    pub manual: ManualPolicyConfig,
    /// Configuration for the [`ExpirationPolicy::TimeToLive`] policy.
    pub ttl: DurationPolicyConfig,
    /// Configuration for the [`ExpirationPolicy::TimeToIdle`] policy.
    pub tti: DurationPolicyConfig,
}

/// Configuration for the [`ExpirationPolicy::Manual`] policy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ManualPolicyConfig {
    /// Whether the manual expiration policy is allowed. Defaults to `true`.
    pub allowed: bool,
}

impl Default for ManualPolicyConfig {
    fn default() -> Self {
        Self { allowed: true }
    }
}

/// Configuration for a duration-based expiration policy (TTL or TTI).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct DurationPolicyConfig {
    /// Whether this expiration policy is allowed. Defaults to `true`.
    pub allowed: bool,
    /// Maximum allowed duration. `None` means no limit.
    #[serde(default, with = "humantime_serde")]
    pub max: Option<Duration>,
}

impl Default for DurationPolicyConfig {
    fn default() -> Self {
        Self {
            allowed: true,
            max: None,
        }
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
        /// The requested duration, in humantime format.
        duration: String,
        /// The configured maximum, in humantime format.
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
        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
        usecases.validate("anything", &metadata).unwrap();
    }

    #[test]
    fn unconfigured_usecase_allows_tti() {
        let usecases = UseCases::default();
        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)));
        usecases.validate("anything", &metadata).unwrap();
    }

    // --- manual policy ---

    #[test]
    fn manual_disallowed_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                manual: ManualPolicyConfig { allowed: false },
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
                manual: ManualPolicyConfig { allowed: true },
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
                ttl: DurationPolicyConfig {
                    allowed: false,
                    max: None,
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::PolicyNotAllowed { .. }));
    }

    #[test]
    fn ttl_within_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                ttl: DurationPolicyConfig {
                    allowed: true,
                    max: Some(Duration::from_secs(7200)),
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn ttl_at_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                ttl: DurationPolicyConfig {
                    allowed: true,
                    max: Some(Duration::from_secs(3600)),
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn ttl_exceeds_max_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                ttl: DurationPolicyConfig {
                    allowed: true,
                    max: Some(Duration::from_secs(3600)),
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(7200)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::DurationExceeded { .. }));
    }

    // --- tti policy ---

    #[test]
    fn tti_disallowed_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: DurationPolicyConfig {
                    allowed: false,
                    max: None,
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::PolicyNotAllowed { .. }));
    }

    #[test]
    fn tti_within_max_passes() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: DurationPolicyConfig {
                    allowed: true,
                    max: Some(Duration::from_secs(7200)),
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)));
        usecases.validate("test", &metadata).unwrap();
    }

    #[test]
    fn tti_exceeds_max_rejects() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: DurationPolicyConfig {
                    allowed: true,
                    max: Some(Duration::from_secs(3600)),
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::TimeToIdle(Duration::from_secs(7200)));
        let err = usecases.validate("test", &metadata).unwrap_err();
        assert!(matches!(err, UseCaseError::DurationExceeded { .. }));
    }

    // --- partial config ---

    #[test]
    fn other_policies_unaffected_when_only_tti_restricted() {
        let usecases = usecases_from(UseCaseConfig {
            expiration: ExpirationConfig {
                tti: DurationPolicyConfig {
                    allowed: false,
                    max: None,
                },
                ..ExpirationConfig::default()
            },
        });

        let metadata = make_metadata(ExpirationPolicy::Manual);
        usecases.validate("test", &metadata).unwrap();

        let metadata = make_metadata(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
        usecases.validate("test", &metadata).unwrap();
    }
}
