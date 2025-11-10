//! # Objectstore Client
//!
//! The client is used to interface with the objectstore backend. It handles responsibilities like
//! transparent compression, and making sure that uploads and downloads are done as efficiently as
//! possible.
//!
//! ## Usage
//!
//! ```no_run
//! use objectstore_client::ClientBuilder;
//!
//! #[tokio::main]
//! # async fn main() -> reqwest::Result<()> {
//! let client = ClientBuilder::new("http://localhost:8888/", "my-usecase")?
//!     .for_organization(42);
//!
//! let id = client.put("hello world").send().await?;
//! let object = client.get(&id.key).send().await?.expect("object to exist");
//! assert_eq!(object.payload().await?, "hello world");
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod client;
mod error;
mod get;
mod put;

pub use objectstore_types::{Compression, ExpirationPolicy};

pub use client::*;
pub use error::*;
pub use get::*;
pub use put::*;

#[cfg(test)]
mod tests;
