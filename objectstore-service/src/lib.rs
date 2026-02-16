#![doc = include_str!("../docs/architecture.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
pub mod error;
pub mod id;
pub mod service;

use futures_util::stream::BoxStream;

pub use service::{StorageConfig, StorageService};

/// Type alias for data streams used in service APIs.
pub type PayloadStream = BoxStream<'static, std::io::Result<bytes::Bytes>>;
