#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod auth;
mod client;
mod delete;
mod error;
mod get;
mod put;
pub mod utils;

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};

pub use auth::*;
pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use put::*;
