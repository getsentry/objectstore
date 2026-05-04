//! Quick benchmark: single HEAD vs batch EXISTS latency.
//!
//! Usage:
//!   cargo run --bin exists_bench -- -c config.yaml
//!
//! Example config (single HEAD, one request per key):
//!   remote: http://localhost:18888
//!   count: 500
//!   batch_size: 0
//!
//! Example config (batch EXISTS, 50 keys per batch):
//!   remote: http://localhost:18888
//!   count: 500
//!   batch_size: 50
//!
//! batch_size=0 means single HEAD requests (one per key, sequential).
//! batch_size=N means batch EXISTS requests (N keys per batch, sequential batches).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use argh::FromArgs;
use futures::StreamExt;
use objectstore_client::{Client, ExpirationPolicy, OperationResult, Session, Usecase};
use serde::Deserialize;
use sketches_ddsketch::DDSketch;
use yansi::Paint;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Benchmark: single HEAD vs batch EXISTS
#[derive(Debug, FromArgs)]
struct Args {
    /// path to the yaml configuration file
    #[argh(option, short = 'c')]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    remote: String,
    count: usize,
    #[serde(default)]
    batch_size: usize,
    seed: Option<usize>,
    #[serde(default)]
    long_term_pct: u8,
    usecase: Option<String>,
}

fn make_session(remote: &str, usecase_name: &str) -> Session {
    let client = Client::builder(remote)
        .configure_reqwest(|r| r.no_hickory_dns())
        .build()
        .expect("failed to build client");
    let usecase = Usecase::new(usecase_name)
        .with_expiration_policy(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)));
    usecase
        .for_project(1, 1)
        .session(&client)
        .expect("failed to create session")
}

const SMALL_PAYLOAD_SIZE: usize = 1024; // 1 KiB → Bigtable
const LARGE_PAYLOAD_SIZE: usize = 1024 * 1024 + 1; // 1 MiB + 1 → GCS

async fn seed_objects(session: &Session, n: usize, long_term_pct: u8) -> Result<Vec<String>> {
    let lt_count = (n as u64 * long_term_pct as u64 / 100) as usize;
    let hv_count = n - lt_count;
    println!("Seeding {n} objects ({hv_count} bigtable, {lt_count} gcs)...");

    let small_payload = vec![0xABu8; SMALL_PAYLOAD_SIZE];
    let large_payload = vec![0xCDu8; LARGE_PAYLOAD_SIZE];
    let mut keys = Vec::with_capacity(n);

    for i in 0..n {
        let is_large = i >= hv_count;
        let payload = if is_large {
            large_payload.clone()
        } else {
            small_payload.clone()
        };
        let key = format!("bench/{i}");
        session
            .put(payload)
            .key(&key)
            .compression(None)
            .send()
            .await
            .with_context(|| format!("failed to seed object {i}"))?;
        keys.push(key);

        if (i + 1) % 100 == 0 {
            println!("  seeded {}/{n}", i + 1);
        }
    }
    println!("Seeding complete.");
    Ok(keys)
}

async fn bench_single_head(session: &Session, keys: &[String], count: usize) -> DDSketch {
    println!("\nRunning {count} sequential HEAD requests...\n");
    let mut sketch = DDSketch::default();

    for i in 0..count {
        let key = &keys[i % keys.len()];
        let start = Instant::now();
        let exists = session
            .head(key)
            .send()
            .await
            .expect("HEAD request failed");
        let elapsed = start.elapsed();
        assert!(exists, "expected object to exist: {key}");
        sketch.add(elapsed.as_secs_f64());
    }

    sketch
}

async fn bench_batch_exists(
    session: &Session,
    keys: &[String],
    count: usize,
    batch_size: usize,
) -> (DDSketch, DDSketch) {
    let num_batches = (count + batch_size - 1) / batch_size;
    println!(
        "\nRunning {count} EXISTS checks in {num_batches} sequential batches of {batch_size}...\n"
    );
    let mut batch_sketch = DDSketch::default();
    let mut per_op_sketch = DDSketch::default();

    let mut done = 0;
    for batch_idx in 0..num_batches {
        let remaining = count - done;
        let this_batch = remaining.min(batch_size);

        let mut many = session.many();
        for j in 0..this_batch {
            let key_idx = (batch_idx * batch_size + j) % keys.len();
            many = many.push(session.exists(&keys[key_idx]));
        }

        let start = Instant::now();
        let mut results = many.send().await;
        while let Some(result) = results.next().await {
            match result {
                OperationResult::Exists(_, Ok(exists)) => {
                    assert!(exists, "expected object to exist");
                }
                OperationResult::Exists(key, Err(e)) => {
                    panic!("exists check failed for {key}: {e}");
                }
                other => {
                    panic!("unexpected result: {other:?}");
                }
            }
        }
        let batch_elapsed = start.elapsed();
        batch_sketch.add(batch_elapsed.as_secs_f64());

        let per_op = batch_elapsed.as_secs_f64() / this_batch as f64;
        for _ in 0..this_batch {
            per_op_sketch.add(per_op);
        }

        done += this_batch;
    }

    (batch_sketch, per_op_sketch)
}

fn print_sketch(label: &str, sketch: &DDSketch) {
    if sketch.count() == 0 {
        return;
    }
    let avg = Duration::from_secs_f64(sketch.sum().unwrap() / sketch.count() as f64);
    let p50 = Duration::from_secs_f64(sketch.quantile(0.5).unwrap().unwrap());
    let p90 = Duration::from_secs_f64(sketch.quantile(0.9).unwrap().unwrap());
    let p99 = Duration::from_secs_f64(sketch.quantile(0.99).unwrap().unwrap());
    let total = Duration::from_secs_f64(sketch.sum().unwrap());
    println!(
        "{label}{} ops | avg: {avg:.2?} | p50: {p50:.2?} | p90: {p90:.2?} | p99: {p99:.2?} | total: {total:.2?}",
        sketch.count().bold()
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Args = argh::from_env();

    let config_file =
        std::fs::File::open(&args.config).context("failed to open config file")?;
    let config: Config =
        serde_yaml::from_reader(config_file).context("failed to parse config YAML")?;

    let seed_count = config.seed.unwrap_or(config.count);
    let usecase_name = config.usecase.as_deref().unwrap_or("exists-bench");
    let session = make_session(&config.remote, usecase_name);
    let keys = seed_objects(&session, seed_count, config.long_term_pct).await?;

    if config.batch_size == 0 {
        let sketch = bench_single_head(&session, &keys, config.count).await;
        println!("{}", "== Results: Single HEAD ==".bold().green());
        print_sketch("  ", &sketch);
    } else {
        let (batch_sketch, per_op_sketch) =
            bench_batch_exists(&session, &keys, config.count, config.batch_size).await;
        println!(
            "{}",
            format!(
                "== Results: Batch EXISTS (batch_size={}) ==",
                config.batch_size
            )
            .bold()
            .green()
        );
        print_sketch("  per-batch: ", &batch_sketch);
        print_sketch("  per-op (amortized): ", &per_op_sketch);
    }

    Ok(())
}
