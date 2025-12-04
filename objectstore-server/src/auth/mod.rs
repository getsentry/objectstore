//! Authorization logic for objectstore.
#![warn(missing_docs)]

mod util;

mod auth_context;
pub use auth_context::*;

mod auth_aware_service;
pub use auth_aware_service::*;
