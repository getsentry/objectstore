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
//! Each struct field must implement [`serde::Deserialize`] and corresponds to a key of the
//! same name within the namespace.
//!
//! # Generated API
//!
//! - `T::get() -> Arc<T>` — Returns an atomic snapshot of the current values. Panics if
//!   `init` was not called (non-test builds).
//! - `T::init() -> Result<(), Error>` — Loads initial values, sets the global singleton, and
//!   registers a propagation callback that live-reloads on changes. Returns an
//!   `Err(AlreadyInitialized)` if called more than once.
//! - `T::override_with(…)` — *(testing feature only)* Temporarily overrides option values;
//!   reverts when the returned guard is dropped.
//!
//! When sentry-options detects changed values it invokes the registered callback, which
//! reloads and atomically swaps in the new snapshot without blocking readers.
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
//! # #[derive(Debug, SentryOptions)]
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
//!
//! // `path` points to the sentry-options directory; the schema is resolved as
//! // `{path}/schemas/{namespace}/schema.json` and embedded at compile time.
//! #[derive(Debug, SentryOptions)]
//! #[sentry_options(
//!     namespace = "objectstore",
//!     path = "../../sentry-options"
//! )]
//! pub struct Options {
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
//!
//! // At call sites:
//! println!("max_retries = {}", Options::get().max_retries());
//! ```

use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;

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
}

/// Trait implemented by option structs that are backed by sentry-options.
///
/// Typically derived via `#[derive(SentryOptions)]` rather than implemented manually.
pub trait SentryOptions: Sized + Send + Sync + 'static {
    /// The sentry-options namespace for this type.
    const NAMESPACE: &str;

    /// The raw JSON schema string (embedded at compile time via `include_str!`).
    const SCHEMA: &str;

    /// Deserializes an instance from the loaded sentry-options values.
    fn deserialize(options: &sentry_options::Options) -> Result<Self, Error>;
}

/// Reloads options and atomically swaps in the new snapshot.
///
/// Registered as the sentry-options propagation callback by the generated
/// [`init`](SentryOptions) implementation and invoked whenever values change. Not intended for
/// direct use.
pub fn refresh<T: SentryOptions>(
    options: &'static OnceLock<ArcSwap<T>>,
    inner: &'static OnceLock<sentry_options::Options>,
    namespace: &str,
    _delay: f64,
) {
    if namespace != T::NAMESPACE {
        return;
    }

    let (Some(snapshot), Some(inner)) = (options.get(), inner.get()) else {
        return;
    };

    match T::deserialize(inner) {
        Ok(new_snapshot) => snapshot.store(Arc::new(new_snapshot)),
        Err(ref err) => {
            objectstore_log::error!(!!err, "Failed to refresh objectstore options")
        }
    }
}
