#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod auth;
mod client;
mod delete;
mod error;
mod get;
mod head;
mod key;
mod many;
#[cfg(feature = "multipart")]
mod multipart;
mod put;
#[cfg(feature = "resumable-upload-api")]
mod resumable;
pub mod utils;

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};

pub use auth::*;
pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use head::*;
pub use key::*;
pub use many::*;
#[cfg(feature = "multipart")]
pub use multipart::*;
pub use put::*;
#[cfg(feature = "resumable-upload-api")]
pub use resumable::*;
