#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod client;
mod delete;
mod error;
mod get;
mod put;

pub use objectstore_types::{Compression, ExpirationPolicy};

pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use put::*;

#[cfg(test)]
mod tests;
