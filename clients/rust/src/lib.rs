//! # Objectstore Client
//!
//! The client is used to interface with the objectstore backend. It handles responsibilities like
//! transparent compression, and making sure that uploads and downloads are done as efficiently as
//! possible.
//!
//! ## Usage
//!
//! ```no_run
//! use std::time::Duration;
//! use std::sync::LazyLock;
//! use objectstore_client::{Usecase, Client, Compression};
//!
//! static OBJECTSTORE_CLIENT: LazyLock<Client> = LazyLock::new(|| {
//!     Client::builder("http://localhost:8888/")
//!         // Optionally, propagate tracing headers to use distributed tracing in Sentry
//!         .propagate_traces(true)
//!         // Customize the reqwest::ClientBuilder
//!         .configure_reqwest(|builder| {
//!             builder.pool_idle_timeout(Duration::from_secs(90))
//!                    .pool_max_idle_per_host(10)
//!         })
//!         .build()
//!         .expect("Failed to build Objectstore client")
//! });
//!
//! static ATTACHMENTS: LazyLock<Usecase> = LazyLock::new(|| {
//!     Usecase::new("attachments")
//! });
//!
//! #[tokio::main]
//! # async fn main() -> objectstore_client::Result<()> {
//!     let session = OBJECTSTORE_CLIENT.session(ATTACHMENTS.for_project(42, 1337))?;
//!     session.put("Hello, world!").send().await?;
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

pub use objectstore_types::{Compression, ExpirationPolicy};

pub use client::*;
pub use delete::*;
pub use error::*;
pub use get::*;
pub use put::*;

#[cfg(test)]
mod tests;
