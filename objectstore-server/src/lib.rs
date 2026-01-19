//! The storage server component.
//!
//! This builds on top of the [`objectstore_service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
#![warn(missing_debug_implementations)]

pub mod auth;
pub mod cli;
pub mod config;
pub mod endpoints;
pub mod extractors;
pub mod healthcheck;
pub mod killswitches;
pub mod multipart;
pub mod observability;
pub mod rate_limits;
pub mod state;
pub mod web;
