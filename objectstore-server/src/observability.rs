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
    let dsn = config.dsn.as_ref()?.expose_secret().as_str();

    let traces_sample_rate = config.traces_sample_rate;
    let inherit_sampling_decision = config.inherit_sampling_decision;
    let mut options = sentry::ClientOptions::new()
        .release(RELEASE)
        .sample_rate(config.sample_rate)
        .traces_sampler(move |ctx| {
            if let Some(sampled) = ctx.sampled()
                && inherit_sampling_decision
            {
                f32::from(sampled)
            } else {
                traces_sample_rate
            }
        })
        .attach_stacktrace(config.attach_stacktrace)
        .debug(config.debug)
        .transport_channel_capacity(config.transport_channel_capacity);

    match dsn.parse::<sentry::types::Dsn>() {
        Ok(_) => options = options.dsn(dsn),
        Err(error) => {
            // Sentry is initialized before the tracing subscriber, so a `warn!` here would be
            // dropped. Write to stderr instead to make the misconfiguration visible.
            eprintln!("WARN: invalid Sentry DSN, error reporting is disabled: {error}");
        }
    }
    if let Some(environment) = &config.environment {
        options = options.environment(environment.clone());
    }
    if let Some(server_name) = &config.server_name {
        options = options.server_name(server_name.clone());
    }

    let guard = sentry::init(options);

    sentry::configure_scope(|scope| {
        for (k, v) in &config.tags {
            scope.set_tag(k, v);
        }
    });

    Some(guard)
}
