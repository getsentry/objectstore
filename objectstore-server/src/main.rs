//! Binary entry point for the objectstore server.
//!
//! This is a thin wrapper that configures the global allocator and delegates to the CLI defined in
//! [`objectstore_server::cli`]. See the [`objectstore_server`] crate for architecture
//! documentation, configuration reference, and endpoint details.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use objectstore_server::cli;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Prof enabled at compile time; sampling starts dormant and is activated on demand via the
// /debug/pprof/enable endpoint. lg_prof_sample:19 means one sample per 2^19 = 512 KiB allocated
// on average (jemalloc's default). The env var MALLOC_CONF still overrides this if set.
#[cfg(all(target_os = "linux", feature = "profiling"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
static malloc_conf: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19\0";

fn main() {
    match cli::execute() {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            objectstore_log::ensure_log_error(&error);
            std::process::exit(1);
        }
    }
}
