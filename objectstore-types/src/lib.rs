//! This is a collection of types shared among various objectstore crates.
//!
//! It primarily includes metadata-related structures being used by both the client and server/service
//! components.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod auth;
pub mod metadata;
pub mod scope;
