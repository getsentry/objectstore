//! The storage server component.
//!
//! This builds on top of the [`objectstore_service`], and exposes the underlying storage layer as
//! an `HTTP` layer which can serve files directly to *external clients* and our SDK.
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
