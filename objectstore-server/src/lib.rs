//! The storage server component.
//!
//! This builds on top of the [`objectstore_service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.

pub mod config;
pub mod endpoints;
pub mod error;
pub mod http;
pub mod observability;
pub mod state;
