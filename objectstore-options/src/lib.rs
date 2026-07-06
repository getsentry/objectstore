//! Runtime options for Objectstore, backed by [`sentry-options`].
//!
//! [`sentry-options`]: https://crates.io/crates/sentry-options

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use sentry_options::Options as Inner;
use serde::{Deserialize, Serialize};

const NAMESPACE: &str = "objectstore";
const SCHEMA: &str = include_str!("../../sentry-options/schemas/objectstore/schema.json");

/// Global instance of the raw sentry options.
static INNER: OnceLock<Inner> = OnceLock::new();

/// Global instance of the options, initialized by [`init`] and accessed via [`Options::get`].
static OPTIONS: OnceLock<ArcSwap<Options>> = OnceLock::new();

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Options(#[from] sentry_options::OptionsError),
    #[error("failed to deserialize option value")]
    Deserialize(#[from] serde_json::Error),
}

/// Runtime options for Objectstore, loaded from sentry-options.
///
/// Obtain a snapshot of the current options via [`Options::get`]. Before calling `get`,
/// the global instance must be initialized with [`init`].
#[derive(Debug)]
pub struct Options {
    killswitches: Vec<Killswitch>,
}

impl Options {
    /// Returns a snapshot of the current options.
    ///
    /// The returned [`Arc`] holds the most recently loaded values. Callers may hold it across
    /// await points without blocking updates — a new snapshot is swapped in atomically by the
    /// background refresh task without invalidating existing references.
    ///
    /// # Panics
    ///
    /// Panics if [`init`] has not been called.
    #[cfg(not(feature = "testing"))]
    pub fn get() -> Arc<Options> {
        OPTIONS.get().expect("options not initialized").load_full()
    }

    /// Returns a snapshot of the current options, deserializing fresh from schema defaults.
    ///
    /// In test builds this bypasses the global instance and reads directly from the schema, so
    /// [`init`] does not need to be called. Use [`override_options`] to test non-default values.
    #[cfg(feature = "testing")]
    pub fn get() -> Arc<Options> {
        let inner = Inner::builder()
            .with_schemas(&[(NAMESPACE, SCHEMA)])
            .build()
            .expect("options schema should be valid");

        Arc::new(Self::deserialize(&inner).expect("failed to deserialize options"))
    }

    fn deserialize(options: &sentry_options::Options) -> Result<Self, Error> {
        Ok(Self {
            killswitches: Deserialize::deserialize(options.get(NAMESPACE, "killswitches")?)?,
        })
    }

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
    /// If specified, the request must have a matching `x-downstream-service` header. The header
    /// value is normalized before matching: any trailing Kubernetes ReplicaSet hash and pod
    /// suffix are stripped, so patterns should match the base service name (e.g. `relay*`, not
    /// `relay-7d8f9c5b6d-*`).
    #[serde(default)]
    pub service: Option<String>,
}

/// Initializes the global options instance and spawns a background refresh task.
///
/// The standard fallback chain is used:
///
/// 1. `SENTRY_OPTIONS_DIR` environment variable
/// 2. `/etc/sentry-options` (if it exists)
/// 3. `sentry-options/` relative to the current working directory
/// 4. Schema defaults (if no values file is present)
///
/// Returns an `Err(AlreadyInitialized)` if called more than once.
pub fn init() -> Result<(), Error> {
    if OPTIONS.get().is_some() {
        return Err(sentry_options::OptionsError::AlreadyInitialized.into());
    }

    // Load an initial snapshot and fail loudly if it can't be loaded. This ensures the
    // application will not silently run with defaults or fail later when options are accessed.
    let inner = Inner::builder()
        .with_schemas(&[(NAMESPACE, SCHEMA)])
        .with_callback(refresh)
        .build()?;

    OPTIONS
        .set(ArcSwap::from_pointee(Options::deserialize(&inner)?))
        .map_err(|_| sentry_options::OptionsError::AlreadyInitialized)?;

    INNER
        .set(inner)
        .map_err(|_| sentry_options::OptionsError::AlreadyInitialized.into())
}

/// Periodically reloads options from disk and atomically swaps in the new snapshot.
fn refresh(namespace: &str, _delay: f64) {
    if namespace != NAMESPACE {
        return;
    }

    let (Some(snapshot), Some(inner)) = (OPTIONS.get(), INNER.get()) else {
        return;
    };

    match Options::deserialize(inner) {
        Ok(new_snapshot) => snapshot.store(Arc::new(new_snapshot)),
        Err(ref err) => {
            objectstore_log::error!(!!err, "Failed to refresh objectstore options")
        }
    }
}

/// Overrides the global options for testing purposes.
///
/// This function is only available in test builds and allows temporarily overriding
/// specific options. The overrides are applied for the duration of the returned
/// `OverrideGuard`.
#[cfg(feature = "testing")]
pub fn override_options(
    overrides: &[(&str, serde_json::Value)],
) -> sentry_options::testing::OverrideGuard {
    let overrides = overrides
        .iter()
        .map(|(key, value)| (NAMESPACE, *key, value.clone()))
        .collect::<Vec<_>>();

    sentry_options::testing::override_options(&overrides).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_valid() {
        let _ = Options::get();
    }
}
