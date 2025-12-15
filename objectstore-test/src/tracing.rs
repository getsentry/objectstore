use tracing_subscriber::EnvFilter;

const CRATE_NAMES: &[&str] = &["objectstore", "objectstore_service", "objectstore_types"];

/// Initialize the logger for testing.
///
/// This logs to the stdout registered by the Rust test runner, and only captures logs from the
/// calling crate.
///
/// # Example
///
/// ```
/// objectstore_test::tracing::init();
/// ```
pub fn init() {
    let mut env_filter = EnvFilter::new("ERROR");

    // Add all internal modules with maximum log-level.
    for name in CRATE_NAMES {
        env_filter = env_filter.add_directive(format!("{name}=TRACE").parse().unwrap());
    }

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_test_writer()
        .compact()
        .try_init()
        .ok();
}
