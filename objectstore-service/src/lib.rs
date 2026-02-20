#![doc = include_str!("../docs/architecture.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
mod concurrency;
pub mod error;
pub mod id;
pub mod service;
pub mod stream;
mod tiered;

pub use service::{StorageConfig, StorageService};
pub use stream::PayloadStream;
