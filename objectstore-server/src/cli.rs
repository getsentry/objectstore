use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use argh::FromArgs;

use crate::config::Config;
use crate::{healthcheck, observability, web};

/// Objectstore API webserver.
#[derive(Debug, FromArgs)]
struct Args {
    /// path to the YAML configuration file
    #[argh(option, short = 'c')]
    pub config: Option<PathBuf>,

    #[argh(subcommand)]
    pub command: Command,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
enum Command {
    Run(RunCommand),
    Healthcheck(HealthcheckCommand),
    Version(VersionCommand),
    Sleep(SleepCommand),
}

/// run the objectstore web server
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "run")]
struct RunCommand {}

/// perform a healthcheck against the running objectstore web server
///
/// This command checks if the objectstore server is available on the configured host and port. This
/// is used for Docker healthchecks.
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "healthcheck")]
struct HealthcheckCommand {}

/// print the objectstore server version
#[derive(Default, Debug, FromArgs)]
#[argh(subcommand, name = "version")]
struct VersionCommand {}

/// sleep for the specified number of seconds
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "sleep")]
struct SleepCommand {
    #[argh(positional)]
    seconds: u64,
}

/// Bootstrap the runtime and execute the CLI command.
pub fn execute() -> Result<()> {
    let args: Args = argh::from_env();

    // Special switch to just print the version and exit.
    if let Command::Version(_) = args.command {
        println!("{}", env!("OBJECTSTORE_RELEASE"));
        return Ok(());
    }

    if let Command::Sleep(SleepCommand { seconds }) = args.command {
        thread::sleep(Duration::from_secs(seconds));
        return Ok(());
    }

    let config = Config::load(args.config.as_deref())?;

    // Ensure a rustls crypto provider is installed, required on distroless.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    // Sentry should be initialized before creating the async runtime.
    let _sentry_guard = observability::init_sentry(&config);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("main-rt")
        .enable_all()
        .worker_threads(config.runtime.worker_threads)
        .build()?;
    let _runtime_guard = runtime.enter();

    observability::init_tracing(&config);
    tracing::debug!(?config);

    let metrics_guard = observability::init_metrics(&config)?;

    let result = runtime.block_on(async move {
        match args.command {
            Command::Run(RunCommand {}) => web::server(config).await,
            Command::Healthcheck(HealthcheckCommand {}) => healthcheck::healthcheck(config).await,
            Command::Version(VersionCommand {}) | Command::Sleep(SleepCommand { .. }) => {
                unreachable!()
            }
        }
    });

    // Flush metrics unconditionally before shutdown, even on error.
    runtime.block_on(async {
        if let Some(metrics_guard) = metrics_guard {
            metrics_guard.flush(None).await.ok();
        }
    });

    result
}
