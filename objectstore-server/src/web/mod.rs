//! Module implementing the Objectstore API webserver.
//!
//! The main server application is implemented in the [`App`] struct, which sets up routing,
//! middleware, and the HTTP server. It is a tower service that can be run using any compatible
//! server framework.
//!
//! To listen to incoming connections, use the [`server()`] function, which opens a TCP listener and
//! serves the application.
//!
//! # Testing
//!
//! For end-to-end tests of the server, see the `objectstore-test` crate, which provides utilities
//! to start a test server and interact with it using the client SDK.

mod app;
mod middleware;
mod server;

pub use app::App;
pub use server::server;
