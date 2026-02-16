#![doc = include_str!("../docs/architecture.md")]
#![warn(missing_debug_implementations)]

pub mod auth;
pub mod batch;
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
