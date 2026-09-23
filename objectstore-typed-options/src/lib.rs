//! Typed, live-reloading runtime options backed by sentry-options.
//!
//! Annotate a plain Rust struct with `#[derive(SentryOptions)]` and it gains a global
//! singleton, schema validation at startup, atomic live-reloading on option changes,
//! and a test-friendly override mechanism — without any hand-written boilerplate.
//!
//! # Usage
//!
//! Annotate a named struct with `#[derive(SentryOptions)]` and the two required container
//! attributes:
//!
//! - `namespace` — the sentry-options namespace string
//! - `path` — relative path (from the source file) to the `sentry-options/` directory;
//!   the schema is resolved as `{path}/schemas/{namespace}/schema.json`
//!
//! The struct must implement [`serde::Deserialize`] (typically via `#[derive(Deserialize)]`).
//! Each field corresponds to a schema property of the same name within the namespace. To
//! look up a key that differs from the field name, use `#[serde(rename = "...")]`.
//!
//! # Generated API
//!
//! - `T::get() -> Arc<T>` — Returns an atomic snapshot of the current values. Panics if
//!   `init` was not called (non-test builds).
//! - `T::init() -> Result<(), Error>` — Loads initial values and sets the global singleton.
//!   Returns an `Err(AlreadyInitialized)` if called more than once.
//! - `T::refresh()` — Force-reloads values from disk and atomically swaps in the new snapshot.
//! - `T::override_with(…)` — *(testing feature only)* Temporarily overrides option values;
//!   reverts when the returned guard is dropped.
//!
//! # Testing
//!
//! Compile with the `testing` feature to enable a test-friendly variant of `get()` that
//! deserializes fresh from schema defaults on every call, bypassing the global singleton.
//! This means [`init`](SentryOptions) does not need to be called in tests.
//!
//! A minimal schema validity test — which every options struct should have — looks like:
//!
//! ```rust,no_run
//! # use objectstore_typed_options::SentryOptions;
//! # use serde::Deserialize;
//! # #[derive(Debug, Deserialize, SentryOptions)]
//! # #[sentry_options(namespace = "objectstore", path = "../../sentry-options")]
//! # struct Options {}
//! #[cfg(test)]
//! mod tests {
//!     use super::*;
//!
//!     #[test]
//!     fn schema_is_valid() {
//!         let _ = Options::get();
//!     }
//! }
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use objectstore_typed_options::SentryOptions;
//! use serde::Deserialize;
//!
//! // `path` points to the sentry-options directory; the schema is resolved as
//! // `{path}/schemas/{namespace}/schema.json` and embedded at compile time.
//! #[derive(Debug, Deserialize, SentryOptions)]
//! #[sentry_options(
//!     namespace = "objectstore",
//!     path = "../../sentry-options"
//! )]
//! pub struct Options {
//!     #[serde(rename = "retries")]
//!     max_retries: u32,
//!     allowed_orgs: Vec<u32>,
//! }
//!
//! impl Options {
//!     pub fn max_retries(&self) -> u32 {
//!         self.max_retries
//!     }
//!
//!     pub fn allowed_orgs(&self) -> &[u32] {
//!         &self.allowed_orgs
//!     }
//! }
//!
//! // At startup:
//! Options::init().expect("failed to load options");
//! std::thread::spawn(|| loop {
//!     std::thread::sleep(std::time::Duration::from_secs(4));
//!     let _ = Options::refresh();
//! });
//!
//! // At call sites:
//! println!("max_retries = {}", Options::get().max_retries());
//! ```

// Re-exported for use by generated code from `#[derive(SentryOptions)]`. Not public API.
#[doc(hidden)]
pub use {arc_swap, sentry_options, serde, serde_json};

#[cfg(feature = "derive")]
pub use objectstore_typed_options_derive::SentryOptions;

/// Errors returned by sentry-options operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Options(#[from] sentry_options::OptionsError),
    #[error("failed to deserialize option value")]
    Deserialize(#[from] serde_json::Error),
    #[error("invalid options schema: {0}")]
    InvalidSchema(String),
}

/// Trait implemented by option structs that are backed by sentry-options.
///
/// Typically derived via `#[derive(SentryOptions)]` rather than implemented manually.
pub trait SentryOptions: serde::de::DeserializeOwned + Send + Sync + 'static {
    /// The sentry-options namespace for this type.
    const NAMESPACE: &str;

    /// The raw JSON schema string (embedded at compile time via `include_str!`).
    const SCHEMA: &str;

    /// Deserializes an instance from the loaded sentry-options values.
    ///
    /// The default implementation parses [`SCHEMA`](Self::SCHEMA) to discover property
    /// keys, fetches each value via [`Options::get`](sentry_options::Options::get), and
    /// deserializes the resulting map into `Self` using serde. Field-level renames should
    /// use `#[serde(rename = "...")]`.
    fn deserialize(options: &sentry_options::Options) -> Result<Self, Error> {
        let schema: serde_json::Value =
            serde_json::from_str(Self::SCHEMA).map_err(|e| Error::InvalidSchema(e.to_string()))?;

        let properties = schema
            .get("properties")
            .and_then(|p| p.as_object())
            .ok_or_else(|| Error::InvalidSchema("missing \"properties\" object".into()))?;

        let mut map = serde_json::Map::with_capacity(properties.len());
        for key in properties.keys() {
            let value = options.get(Self::NAMESPACE, key)?;
            map.insert(key.clone(), value);
        }

        let result = serde_json::from_value(serde_json::Value::Object(map))?;
        Ok(result)
    }
}
