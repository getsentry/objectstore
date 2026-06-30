//! # Shared Types
//!
//! This crate defines the types shared between the objectstore server, service,
//! and client libraries. Refer to each module's documentation for details.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod auth;
pub mod metadata;
pub mod multipart;
pub mod operation;
pub mod range;
pub mod scope;
