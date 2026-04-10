//! Runtime options for Objectstore, backed by [`sentry-options`].
//!
//! See the [`Options`] struct for details and usage instructions.
//!
//! [`sentry-options`]: https://crates.io/crates/sentry-options

use std::collections::BTreeMap;

use objectstore_typed_options::SentryOptions;
use serde::{Deserialize, Serialize};

pub use objectstore_typed_options::Error;

/// Runtime options for Objectstore, loaded from sentry-options.
///
/// Obtain a snapshot of the current options via [`Options::get`]. Before calling `get`,
/// the global instance must be initialized with [`Options::init`].
#[derive(Debug, SentryOptions)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[sentry_options(namespace = "objectstore", path = "../../sentry-options")]
pub struct Options {
    /// Active killswitches that may disable access to specific object contexts.
    #[cfg_attr(test, schemars(default))]
    killswitches: Vec<Killswitch>,
}

impl Options {
    /// Returns the list of active killswitches.
    pub fn killswitches(&self) -> &[Killswitch] {
        &self.killswitches
    }
}

/// A killswitch that may disable access to certain object contexts.
///
/// Note that at least one of the fields should be set, or else the killswitch will match all
/// contexts and discard all requests.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Killswitch {
    /// Optional usecase to match.
    ///
    /// If `None`, matches any usecase.
    #[serde(default)]
    pub usecase: Option<String>,

    /// Scopes to match.
    ///
    /// If empty, matches any scopes. Additional scopes in the context are ignored, so a killswitch
    /// matches if all of the specified scopes are present in the request with matching values.
    #[serde(default)]
    pub scopes: BTreeMap<String, String>,

    /// Optional service glob pattern to match.
    ///
    /// If `None`, matches any service (or absence of service header).
    /// If specified, the request must have a matching `x-downstream-service` header.
    #[serde(default)]
    pub service: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn schema_is_valid() {
        let _ = Options::get();
    }

    #[test]
    fn schema_matches_golden_file() {
        let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../sentry-options/schemas/objectstore/schema.json");
        objectstore_typed_options::schema::assert_schema_matches_golden_file::<Options>(
            &schema_path,
        );
    }
}
