//! Binary entry point for the objectstore server.
//!
//! This is a thin wrapper that configures the global allocator and delegates to the CLI defined in
//! [`objectstore_server::cli`]. See the [`objectstore_server`] crate for architecture
//! documentation, configuration reference, and endpoint details.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use objectstore_server::{cli, observability};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    match cli::execute() {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            observability::ensure_log_error(&error);
            std::process::exit(1);
        }
    }
}
