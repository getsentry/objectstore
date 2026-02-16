#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod auth;
mod client;
mod delete;
mod error;
mod get;
mod many;
mod put;
pub mod utils;

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};

/// A key that uniquely identifies an object within its usecase and scopes.
pub type ObjectKey = String;

pub use auth::*;
pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use many::*;
pub use put::*;
