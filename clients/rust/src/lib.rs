//! # Objectstore Client
//!
//! The client is used to interface with the objectstore backend. It handles responsibilities like
//! transparent compression, and making sure that uploads and downloads are done as efficiently as
//! possible.
//!
//! ## Usage
//!
//! ```no_run
//! use objectstore_client::{ClientBuilder, Usecase};
//!
//! #[tokio::main]
//! # async fn main() -> objectstore_client::Result<()> {
//! let client = ClientBuilder::new("http://localhost:8888/").build().unwrap();
//! let usecase = Usecase::new("usecase");
//! let session = client.session(usecase.for_project(12345, 1337)).unwrap();
//!
//! let response = session.put("hello world").send().await?;
//! let object = session.get(&response.key).send().await?.expect("object to exist");
//! assert_eq!(object.payload().await?, "hello world");
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod client;
mod delete;
mod error;
mod get;
mod put;
pub mod utils;

pub use objectstore_types::{Compression, ExpirationPolicy};

pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use put::*;

#[cfg(test)]
mod tests;
