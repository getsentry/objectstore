#![doc = include_str!("../docs/architecture.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod backend;
mod concurrency;
pub mod error;
mod gcp_auth;
pub mod id;
pub mod multipart;
pub mod service;
pub mod stream;
pub mod streaming;

pub use concurrency::ConcurrencyError;
pub use service::StorageService;
pub use stream::{ClientStream, PayloadStream};
