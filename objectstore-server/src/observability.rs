//! Initialization of Sentry error reporting.
//!
//! Call [`init_sentry`] during server startup before creating the Tokio runtime so it can
//! instrument async tasks from the start. Tracing subscriber initialization (including the
//! Sentry tracing layer) is handled by [`objectstore_log::init`].

use secrecy::ExposeSecret;

use crate::config::Config;

/// The full release name including the objectstore version and SHA.
const RELEASE: &str = std::env!("OBJECTSTORE_RELEASE");

/// Initializes the Sentry error-reporting client, if a DSN is configured.
///
/// Returns `None` when `config.sentry.dsn` is not set. The returned
/// [`sentry::ClientInitGuard`] must be kept alive for the duration of the process;
/// dropping it flushes the event queue and shuts down the Sentry client.
pub fn init_sentry(config: &Config) -> Option<sentry::ClientInitGuard> {
    let config = &config.sentry;
    let dsn = config.dsn.as_ref()?;

    let dsn = match dsn.expose_secret().parse() {
        Ok(dsn) => Some(dsn),
        Err(error) => {
            // Sentry is initialized before the tracing subscriber, so a `warn!` here would be
            // dropped. Write to stderr instead to make the misconfiguration visible.
            eprintln!("WARN: invalid Sentry DSN, error reporting is disabled: {error}");
            None
        }
    };

    let guard = sentry::init(sentry::ClientOptions {
        dsn,
        release: Some(RELEASE.into()),
        environment: config.environment.clone(),
        server_name: config.server_name.clone(),
        sample_rate: config.sample_rate,
        traces_sampler: {
            let traces_sample_rate = config.traces_sample_rate;
            let inherit_sampling_decision = config.inherit_sampling_decision;
            Some(std::sync::Arc::new(move |ctx| {
                if let Some(sampled) = ctx.sampled()
                    && inherit_sampling_decision
                {
                    f32::from(sampled)
                } else {
                    traces_sample_rate
                }
            }))
        },
        enable_logs: true,
        debug: config.debug,
        ..Default::default()
    });

    sentry::configure_scope(|scope| {
        for (k, v) in &config.tags {
            scope.set_tag(k, v);
        }
    });

    Some(guard)
}
