//! Benchmark tool for writing objects directly to the Bigtable backend.
//!
//! Bypasses the HTTP layer and writes directly via the `BigTableBackend`,
//! printing live latency percentiles every second. Intended for use with the
//! Bigtable emulator (`devservices up --mode=full`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use argh::FromArgs;
use bytesize::ByteSize;
use futures_util::StreamExt;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sketches_ddsketch::DDSketch;
use tokio::sync::Semaphore;
use yansi::Paint;

use objectstore_service::backend::bigtable::BigTableBackend;
use objectstore_service::backend::common::Backend;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::scope::{Scope, Scopes};

/// Benchmark tool for the Bigtable storage backend.
#[derive(Debug, FromArgs)]
struct Args {
    /// number of concurrent put requests (default: 10)
    #[argh(option, short = 'c', default = "10")]
    concurrency: usize,

    /// object size, e.g. "50KB" or "4096" (default: 50KB)
    #[argh(option, short = 's', default = "ByteSize::kb(50)")]
    size: ByteSize,

    /// bigtable gRPC connection pool size (default: 1)
    #[argh(option, short = 'p', default = "1")]
    pool: usize,

    /// bigtable emulator address; omit to connect to real GCP Bigtable
    #[argh(option, short = 'a')]
    addr: Option<String>,

    /// GCP project ID (default: testing)
    #[argh(option, default = "String::from(\"testing\")")]
    project: String,

    /// bigtable instance name (default: objectstore)
    #[argh(option, default = "String::from(\"objectstore\")")]
    instance: String,

    /// bigtable table name (default: objectstore)
    #[argh(option, default = "String::from(\"objectstore\")")]
    table: String,
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok(); // INVARIANT: no other code installs a provider before main()

    let args: Args = argh::from_env();
    let object_size = args.size.as_u64() as usize;

    match &args.addr {
        Some(addr) => eprintln!(
            "connecting to Bigtable emulator ({addr}), pool size {}",
            args.pool
        ),
        None => eprintln!(
            "connecting to GCP Bigtable ({}/{}), pool size {}",
            args.project, args.instance, args.pool
        ),
    }

    let backend = BigTableBackend::new(
        args.addr.as_deref(),
        &args.project,
        &args.instance,
        &args.table,
        Some(args.pool),
    )
    .await
    .context("failed to connect to Bigtable")?;

    let backend = Arc::new(backend);

    eprintln!(
        "starting benchmark: concurrency={}, object_size={}",
        args.concurrency, args.size,
    );

    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let sketch = Arc::new(Mutex::new(DDSketch::default()));
    let failures = Arc::new(AtomicUsize::new(0));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let concurrency = args.concurrency;
    let pool_size = args.pool;
    let bench_start = Instant::now();

    // Stats printer task.
    let stats_sketch = Arc::clone(&sketch);
    let stats_failures = Arc::clone(&failures);
    let stats_shutdown = shutdown.clone();
    let stats_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let start = Instant::now();
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                () = stats_shutdown.cancelled() => break,
            }
            let elapsed = start.elapsed();
            let guard = stats_sketch.lock().unwrap(); // INVARIANT: no panic inside lock
            let ops = guard.count();
            if ops == 0 {
                continue;
            }
            let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
            let ops_per_sec_per_conn = ops_per_sec / pool_size as f64;
            let bytes_per_sec = ops_per_sec * object_size as f64;
            let avg = Duration::from_secs_f64(guard.sum().unwrap() / ops as f64);
            let p50 = Duration::from_secs_f64(guard.quantile(0.5).unwrap().unwrap());
            let p95 = Duration::from_secs_f64(guard.quantile(0.95).unwrap().unwrap());
            let p99 = Duration::from_secs_f64(guard.quantile(0.99).unwrap().unwrap());
            let max = Duration::from_secs_f64(guard.max().unwrap());
            drop(guard);
            let failed = stats_failures.load(Ordering::Relaxed);

            eprint!(
                "\x1b[2K[{} ops | {:.0} ops/s | {:.2} ops/s/conn | {} | {}]",
                ops.bold(),
                ops_per_sec.bold(),
                ops_per_sec_per_conn.bold(),
                format_throughput(bytes_per_sec).bold(),
                format_elapsed(elapsed).bold(),
            );
            eprint!(
                "\n\x1b[2K  avg: {}   p50: {}   p95: {}   p99: {}   max: {}",
                format_ms(avg).bold(),
                format_ms(p50),
                format_ms(p95),
                format_ms(p99),
                format_ms(max),
            );
            if failed > 0 {
                eprint!("\n\x1b[2K  failed: {}\r\x1b[2A", failed.red().bold());
            } else {
                eprint!("\n\x1b[2K\r\x1b[2A");
            }
        }
    });

    // Worker loop — spawns tasks up to the semaphore limit until ctrl+c.
    let worker_shutdown = shutdown.clone();
    let worker_sketch = Arc::clone(&sketch);
    let worker_failures = Arc::clone(&failures);
    let worker_handle = tokio::spawn(async move {
        let context = ObjectContext {
            usecase: "bench".into(),
            scopes: Scopes::from_iter([
                Scope::create("org", "bench").unwrap(), // INVARIANT: valid scope name
            ]),
        };
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            ..Metadata::default()
        };

        loop {
            if worker_shutdown.is_cancelled() {
                break;
            }

            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break, // semaphore closed
            };

            let backend = Arc::clone(&backend);
            let sketch = Arc::clone(&worker_sketch);
            let failures = Arc::clone(&worker_failures);
            let context = context.clone();
            let metadata = metadata.clone();

            tokio::spawn(async move {
                let _permit = permit;

                let mut rng = SmallRng::from_os_rng();
                let mut buf = vec![0u8; object_size];
                rng.fill(&mut buf[..]);

                let id = ObjectId::random(context);
                let stream = futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(buf))
                })
                .boxed();

                let start = Instant::now();
                let result = backend.put_object(&id, &metadata, stream).await;
                let elapsed = start.elapsed();

                if result.is_err() {
                    failures.fetch_add(1, Ordering::Relaxed);
                } else {
                    sketch.lock().unwrap().add(elapsed.as_secs_f64()); // INVARIANT: no panic inside lock
                }
            });
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for ctrl+c")?;

    shutdown.cancel();

    let _ = stats_handle.await;
    let _ = worker_handle.await;

    // Clear the three-line live stats display before printing the final summary.
    eprint!("\r\x1b[2K\n\x1b[2K\n\x1b[2K\x1b[2A\r");

    // Print final summary.
    let guard = sketch.lock().unwrap(); // INVARIANT: no panic inside lock
    let ops = guard.count();
    if ops > 0 {
        let elapsed = bench_start.elapsed();
        let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
        let ops_per_sec_per_conn = ops_per_sec / pool_size as f64;
        let bytes_per_sec = ops_per_sec * object_size as f64;
        let avg = Duration::from_secs_f64(guard.sum().unwrap() / ops as f64);
        let p50 = Duration::from_secs_f64(guard.quantile(0.5).unwrap().unwrap());
        let p95 = Duration::from_secs_f64(guard.quantile(0.95).unwrap().unwrap());
        let p99 = Duration::from_secs_f64(guard.quantile(0.99).unwrap().unwrap());
        let max = Duration::from_secs_f64(guard.max().unwrap());
        eprintln!(
            "\nfinal: {} ops | {:.0} ops/s | {:.2} ops/s/conn | {} | {}\n  avg: {}   p50: {}   p95: {}   p99: {}   max: {}",
            ops.bold(),
            ops_per_sec.bold(),
            ops_per_sec_per_conn.bold(),
            format_throughput(bytes_per_sec).bold(),
            format_elapsed(elapsed).bold(),
            format_ms(avg).bold(),
            format_ms(p50),
            format_ms(p95),
            format_ms(p99),
            format_ms(max),
        );
    }

    Ok(())
}

/// Formats a [`Duration`] as milliseconds with two decimal places.
fn format_ms(d: Duration) -> String {
    format!("{:.2}ms", d.as_secs_f64() * 1000.0)
}

/// Formats a [`Duration`] as `M:SS`.
fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    format!("{}:{:02}", secs / 60, secs % 60)
}

/// Formats a byte rate using SI decimal prefixes (kB/s, MB/s, GB/s).
fn format_throughput(bytes_per_sec: f64) -> String {
    if bytes_per_sec >= 1e9 {
        format!("{:.2} GB/s", bytes_per_sec / 1e9)
    } else if bytes_per_sec >= 1e6 {
        format!("{:.2} MB/s", bytes_per_sec / 1e6)
    } else if bytes_per_sec >= 1e3 {
        format!("{:.2} kB/s", bytes_per_sec / 1e3)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}
