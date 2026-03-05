#![doc = include_str!("../docs/architecture.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

#[allow(missing_docs, private_interfaces)]
pub mod backend;
mod concurrency;
pub mod error;
mod gcp_auth;
pub mod id;
pub mod service;
pub mod stream;
pub mod streaming;
mod tiered;

pub use service::{StorageConfig, StorageService};
pub use stream::PayloadStream;
