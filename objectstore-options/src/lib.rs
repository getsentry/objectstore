//! Runtime options for Objectstore, backed by [`sentry-options`].
//!
//! [`sentry-options`]: https://crates.io/crates/sentry-options

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use serde::Deserialize;

const NAMESPACE: &str = "objectstore";
const SCHEMA: &str = include_str!("../../sentry-options/schemas/objectstore/schema.json");
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

static OPTIONS: OnceLock<RwLock<Options>> = OnceLock::new();

#[cfg(test)]
pub use sentry_options::{Value, testing::OverrideGuard};

#[cfg(test)]
thread_local! {
    static TEST_INNER: sentry_options::Options =
        sentry_options::Options::from_schemas(&[(NAMESPACE, SCHEMA)])
            .expect("objectstore schema should be valid");
}

/// TODO: Doc comment.
#[cfg(not(test))]
pub type Ref = std::sync::RwLockReadGuard<'static, Options>;

/// TODO: Doc comment.
#[cfg(test)]
pub type Ref = Options;

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Options(#[from] sentry_options::OptionsError),
    #[error("failed to deserialize option value")]
    Deserialize(#[from] serde_json::Error),
}

/// TODO: Doc comment. Also refer to `init`.
pub struct Options {
    killswitches: Vec<Killswitch>,
}

impl Options {
    /// TODO: Doc comment
    #[cfg(not(test))]
    pub fn get() -> Ref {
        let options = OPTIONS.get().expect("options not initialized");
        options.read().unwrap_or_else(|p| p.into_inner())
    }

    /// TODO: Doc comment
    #[cfg(test)]
    pub fn get() -> Ref {
        TEST_INNER.with(|inner| Self::deserialize(inner).expect("failed to deserialize options"))
    }

    fn deserialize(options: &sentry_options::Options) -> Result<Self, Error> {
        Ok(Self {
            killswitches: Deserialize::deserialize(options.get(NAMESPACE, "killswitches")?)?,
        })
    }

    /// TODO: Doc comment
    pub fn killswitches(&self) -> &[Killswitch] {
        &self.killswitches
    }
}

/// Initializes the global options instance and spawns a background refresh task.
///
/// If `base_dir` is provided, values are loaded from `{base_dir}/values/`. Otherwise, the
/// standard fallback chain is used:
///
/// 1. `SENTRY_OPTIONS_DIR` environment variable
/// 2. `/etc/sentry-options` (if it exists)
/// 3. `sentry-options/` relative to the current working directory
/// 4. Schema defaults (if no values file is present)
///
/// Idempotent: if already initialized, returns `Ok(())` without re-loading.
///
/// Must be called from within a Tokio runtime.
pub fn init(base_dir: Option<&Path>) -> Result<(), Error> {
    if OPTIONS.get().is_some() {
        return Ok(());
    }

    let schemas = &[(NAMESPACE, SCHEMA)];
    let inner = match base_dir {
        Some(dir) => sentry_options::Options::from_directory_and_schemas(dir, schemas)?,
        None => sentry_options::Options::from_schemas(schemas)?,
    };

    // Load an initial snapshot and fail loudly if it can't be loaded. This ensures the application
    // will not silently run with defaults or fail later when options get accessed.
    let _ = OPTIONS.set(RwLock::new(Options::deserialize(&inner)?));
    tokio::spawn(refresh(inner));

    Ok(())
}

/// TODO: Doc comment
async fn refresh(inner: sentry_options::Options) {
    let Some(snapshot) = OPTIONS.get() else {
        return;
    };

    let mut interval = tokio::time::interval(REFRESH_INTERVAL);
    interval.tick().await; // consume the immediate first tick

    loop {
        interval.tick().await;

        match Options::deserialize(&inner) {
            Ok(new_snapshot) => *snapshot.write().unwrap_or_else(|p| p.into_inner()) = new_snapshot,
            Err(ref err) => {
                objectstore_log::error!(!!err, "Failed to refresh objectstore options")
            }
        }
    }
}

/// Overrides the global options for testing purposes.
///
/// This function is only available in test builds and allows temporarily overriding
/// specific options. The overrides are applied for the duration of the returned
/// `OverrideGuard`.
#[cfg(test)]
pub fn override_options(overrides: &[(&str, sentry_options::Value)]) -> OverrideGuard {
    let overrides = overrides
        .iter()
        .map(|(key, value)| (NAMESPACE, *key, value.clone()))
        .collect::<Vec<_>>();

    sentry_options::testing::override_options(&overrides).unwrap()
}

/// A killswitch loaded from sentry-options.
///
/// This is the plain data representation used for deserialization. See
/// [`objectstore_server::killswitches::Killswitch`] for the full type with matching logic.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct Killswitch {
    #[serde(default)]
    pub usecase: Option<String>,
    #[serde(default)]
    pub scopes: BTreeMap<String, String>,
    #[serde(default)]
    pub service: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_valid() {
        let _ = Options::get();
    }
}
