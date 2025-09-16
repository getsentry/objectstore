//! The Storage Client
//!
//! The Client is used to interface with the objectstore backend.
//! It handles responsibilities like transparent compression, and making sure that
//! uploads and downloads are done as efficiently as possible.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod client;
mod get;
mod patch;
mod put;

pub use objectstore_types::{Compression, ExpirationPolicy};

pub use client::*;
pub use get::*;
pub use patch::*;
pub use put::*;

#[cfg(test)]
mod tests;
