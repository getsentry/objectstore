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
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use sketches_ddsketch::DDSketch;
use tokio::sync::Semaphore;
use yansi::Paint;

use objectstore_service::backend::bigtable::BigTableBackend;
use objectstore_service::backend::bigtable::BigTableConfig;
use objectstore_service::backend::common::Backend;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::stream;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::scope::{Scope, Scopes};

/// Benchmark tool for the Bigtable storage backend.
#[derive(Debug, FromArgs)]
struct Args {
    /// number of concurrent put requests (default: 10)
    #[argh(option, short = 'c', default = "10")]
    concurrency: usize,

    /// median object size (p50), e.g. "50KB" (default: 2KiB)
    #[argh(option, default = "ByteSize::kib(2)")]
    p50: ByteSize,

    /// 99th-percentile object size (p99), e.g. "200KB" (default: 385KiB)
    #[argh(option, default = "ByteSize::kib(385)")]
    p99: ByteSize,

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

    let p50 = args.p50.as_u64() as f64;
    let p99 = args.p99.as_u64() as f64;
    let mu = p50.ln();
    let sigma = (p99.ln() - mu) / 2.3263;
    let size_distribution =
        LogNormal::new(mu, sigma).expect("invalid size distribution parameters"); // INVARIANT: p50 and p99 are positive

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

    let backend = BigTableBackend::new(BigTableConfig {
        endpoint: args.addr.clone(),
        project_id: args.project.clone(),
        instance_name: args.instance.clone(),
        table_name: args.table.clone(),
        connections: Some(args.pool),
    })
    .await
    .context("failed to connect to Bigtable")?;

    let backend = Arc::new(backend);

    eprintln!(
        "starting benchmark: concurrency={}, p50={}, p99={}",
        args.concurrency, args.p50, args.p99,
    );

    /// Maximum object size regardless of distribution output.
    const MAX_OBJECT_SIZE: u64 = 950 * 1024;

    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let sketch = Arc::new(Mutex::new(DDSketch::default()));
    let failures = Arc::new(AtomicUsize::new(0));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let pool_size = args.pool;
    let size_distribution = Arc::new(size_distribution);
    let bench_start = Instant::now();

    let total_bytes = Arc::new(AtomicUsize::new(0));

    // Stats printer task.
    let stats_sketch = Arc::clone(&sketch);
    let stats_failures = Arc::clone(&failures);
    let stats_bytes = Arc::clone(&total_bytes);
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
            let bytes_per_sec = stats_bytes.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64();
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
    let worker_bytes = Arc::clone(&total_bytes);
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
            let permit = tokio::select! {
                () = worker_shutdown.cancelled() => break,
                p = semaphore.clone().acquire_owned() => match p {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed
                },
            };

            let backend = Arc::clone(&backend);
            let sketch = Arc::clone(&worker_sketch);
            let failures = Arc::clone(&worker_failures);
            let bytes = Arc::clone(&worker_bytes);
            let size_dist = Arc::clone(&size_distribution);
            let context = context.clone();
            let metadata = metadata.clone();

            tokio::spawn(async move {
                let _permit = permit;

                let mut rng = SmallRng::from_os_rng();
                let object_size = (size_dist.sample(&mut rng) as u64).min(MAX_OBJECT_SIZE) as usize;
                let mut buf = vec![0u8; object_size];
                rng.fill(&mut buf[..]);

                let id = ObjectId::random(context);
                let stream = stream::single(buf);

                let start = Instant::now();
                let result = backend.put_object(&id, &metadata, stream).await;
                let elapsed = start.elapsed();

                if result.is_err() {
                    failures.fetch_add(1, Ordering::Relaxed);
                } else {
                    bytes.fetch_add(object_size, Ordering::Relaxed);
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
    let failed = failures.load(Ordering::Relaxed);
    if ops > 0 {
        let elapsed = bench_start.elapsed();
        let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
        let ops_per_sec_per_conn = ops_per_sec / pool_size as f64;
        let bytes_per_sec = total_bytes.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64();
        let avg = Duration::from_secs_f64(guard.sum().unwrap() / ops as f64);
        let p50 = Duration::from_secs_f64(guard.quantile(0.5).unwrap().unwrap());
        let p95 = Duration::from_secs_f64(guard.quantile(0.95).unwrap().unwrap());
        let p99 = Duration::from_secs_f64(guard.quantile(0.99).unwrap().unwrap());
        let max = Duration::from_secs_f64(guard.max().unwrap());
        eprint!(
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
        if failed > 0 {
            eprintln!("\n  failed: {}", failed.red().bold());
        } else {
            eprintln!();
        }
    } else if failed > 0 {
        eprintln!("\nfinal: {} ops | failed: {}", ops, failed.red().bold());
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
