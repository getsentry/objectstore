//! Run workloads concurrently against a remote storage service and print metrics.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Error, Result};

use bytesize::ByteSize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use objectstore_client::{ExpirationPolicy, Usecase};
use sketches_ddsketch::DDSketch;
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;
use yansi::Paint;

use crate::http::HttpRemote;
use crate::workload::{Action, Workload, WorkloadMode};

/// Stresstest runner that can execute multiple workloads concurrently against a remote.
///
/// To execute the stresstest, use [`run`](Self::run).
#[derive(Debug)]
pub struct Stresstest {
    remote: HttpRemote,
    workloads: Vec<Workload>,
    duration: Duration,
    ttl: Option<Duration>,
    cleanup: bool,
}

impl Stresstest {
    /// Default runtime for the stresstest.
    pub const DEFAULT_DURATION: Duration = Duration::from_secs(60);

    /// Default TTL for all objects created during the stresstest.
    pub const DEFAULT_TTL: Duration = Duration::from_hours(1);

    /// Creates a new `Stresstest` instance with the given remote.
    ///
    /// It is required to add at least one workload using [`Self::workload`] and configure the
    /// duration before running the stresstest.
    pub fn new(remote: HttpRemote) -> Self {
        Self {
            remote,
            workloads: Vec::new(),
            duration: Self::DEFAULT_DURATION,
            ttl: Some(Self::DEFAULT_TTL),
            cleanup: false,
        }
    }

    /// Sets the duration of the stresstest.
    ///
    /// Defaults to 60 seconds.
    pub fn duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Sets the TTL for all objects created during the stresstest.
    ///
    /// Defaults to 1 hour. Use `None` to disable TTL.
    pub fn ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }

    /// Adds a workload to the stresstest.
    ///
    /// Without any workloads, the stresstest will do nothing.
    pub fn workload(mut self, workload: Workload) -> Self {
        self.workloads.push(workload);
        self
    }

    /// Sets whether to cleanup all objects after the test.
    ///
    /// By default, objects will remain in the backend and be deleted after their TTL expires.
    /// The TTL is hard-coded to 1 hour.
    pub fn cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Runs the given workloads concurrently against the remote.
    ///
    /// The function runs all workloads concurrently, then prints metrics and finally deletes all
    /// objects from the remote.
    pub async fn run(self) -> Result<()> {
        let Self {
            remote,
            workloads,
            duration,
            ttl,
            cleanup,
        } = self;

        if workloads.is_empty() {
            println!("no workloads specified, nothing to do");
            return Ok(());
        }

        if duration.is_zero() {
            println!("duration is zero, nothing to do");
            return Ok(());
        }

        let remote = Arc::new(remote);

        let bar = ProgressBar::new_spinner()
            .with_style(ProgressStyle::with_template("{spinner} {msg} {elapsed}")?)
            .with_message("Running stresstest:");
        bar.enable_steady_tick(Duration::from_millis(100));

        // run the workloads concurrently
        let tasks: Vec<_> = workloads
            .into_iter()
            .map(|workload| {
                let remote = Arc::clone(&remote);
                match workload.mode {
                    WorkloadMode::Batch => {
                        tokio::spawn(run_batch_workload(remote, workload, duration, ttl))
                    }
                    _ => tokio::spawn(run_workload(remote, workload, duration, ttl)),
                }
            })
            .collect();

        let finished_tasks = futures::future::join_all(tasks).await;
        bar.finish_and_clear();

        let mut total_metrics = WorkloadMetrics::default();
        let mut max_elapsed = Duration::ZERO;
        let workloads = finished_tasks.into_iter().map(|task| {
            let (workload, metrics, elapsed) = task.unwrap();
            max_elapsed = max_elapsed.max(elapsed);

            println!();
            println!(
                "{} {} (mode: {:?}, concurrency: {})",
                "## Workload".bold(),
                workload.name.bold().blue(),
                workload.mode,
                workload.concurrency.bold()
            );
            print_metrics(&metrics, elapsed);

            total_metrics.file_sizes.merge(&metrics.file_sizes).unwrap();
            total_metrics.bytes_written += metrics.bytes_written;
            total_metrics.bytes_read += metrics.bytes_read;
            total_metrics
                .write_timing
                .merge(&metrics.write_timing)
                .unwrap();
            total_metrics
                .batch_timing
                .merge(&metrics.batch_timing)
                .unwrap();
            total_metrics
                .read_timing
                .merge(&metrics.read_timing)
                .unwrap();
            total_metrics
                .delete_timing
                .merge(&metrics.delete_timing)
                .unwrap();
            total_metrics.write_failures += metrics.write_failures;
            total_metrics.read_failures += metrics.read_failures;
            total_metrics.many_requests += metrics.many_requests;

            workload
        });

        let workloads: Vec<_> = workloads.collect();
        println!();
        println!("{}", "## TOTALS".bold());
        print_metrics(&total_metrics, max_elapsed);
        println!();

        if !cleanup {
            return Ok(());
        }

        let max_concurrency = workloads.iter().map(|w| w.concurrency).max().unwrap();
        let files_to_cleanup = workloads.iter().flat_map(|w| w.external_files());
        let cleanup_count = workloads.iter().flat_map(|w| w.external_files()).count();

        let bar = ProgressBar::new(cleanup_count as u64)
            .with_message("Deleting remaining files...")
            .with_style(ProgressStyle::with_template(
                "{msg}\n{wide_bar} {pos}/{len}",
            )?);
        bar.enable_steady_tick(Duration::from_millis(100));

        let start = Instant::now();
        let cleanup_timing = Arc::new(Mutex::new(DDSketch::default()));
        futures::stream::iter(files_to_cleanup)
            .for_each_concurrent(max_concurrency, |(usecase, organization_id, object_key)| {
                let remote = remote.clone();
                let cleanup_timing = cleanup_timing.clone();
                let bar = &bar;
                async move {
                    let start = Instant::now();
                    remote
                        .delete(usecase, *organization_id, object_key)
                        .await
                        .unwrap();
                    cleanup_timing
                        .lock()
                        .unwrap()
                        .add(start.elapsed().as_secs_f64());

                    bar.inc(1);
                }
            })
            .await;

        bar.finish_and_clear();

        let cleanup_duration = start.elapsed();
        let cleanup_timing = cleanup_timing.lock().unwrap();

        println!(
            "{} ({} files, concurrency: {})",
            "## CLEANUP".bold(),
            cleanup_timing.count().blue(),
            max_concurrency.bold()
        );
        if cleanup_timing.count() > 0 {
            print_ops(&cleanup_timing, cleanup_duration);
            println!();
            print_percentiles("", &cleanup_timing, Duration::from_secs_f64);
        }

        Ok(())
    }
}

async fn run_workload(
    remote: Arc<HttpRemote>,
    workload: Workload,
    duration: Duration,
    ttl: Option<Duration>,
) -> (Workload, WorkloadMetrics, Duration) {
    // In throughput mode, allow for a high concurrency value.
    let concurrency = match workload.mode {
        WorkloadMode::Weighted | WorkloadMode::Batch => workload.concurrency,
        WorkloadMode::Throughput => 100,
    };

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + duration;

    let workload = Arc::new(Mutex::new(workload));
    let metrics = Arc::new(Mutex::new(WorkloadMetrics::default()));

    // See <https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html#examples>
    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);

    loop {
        if deadline.elapsed() > Duration::ZERO {
            break;
        }
        tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                let workload = Arc::clone(&workload);
                let remote = Arc::clone(&remote);
                let metrics = Arc::clone(&metrics);

                let action = loop {
                    if let Some(action) = workload.lock().unwrap().next_action() {
                        break action;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                };


                let task = async move {
                    let start = Instant::now();
                    match action {
                        Action::Write(internal_id, payload) => {
                            let file_size = payload.len;
                            let organization_id = workload.lock().unwrap().next_organization_id();

                            let mut usecase = Usecase::new(&workload.lock().unwrap().name);
                            if let Some(ttl) = ttl {
                                usecase = usecase.with_expiration_policy(ExpirationPolicy::TimeToLive(ttl));
                            }

                            match remote.write(&usecase, organization_id, payload).await {
                                Ok(object_key) => {
                                    let external_id = (usecase, organization_id, object_key);
                                    workload.lock().unwrap().push_file(internal_id, external_id);
                                    let mut metrics = metrics.lock().unwrap();
                                    metrics.write_timing.add(start.elapsed().as_secs_f64());
                                    metrics.file_sizes.add(file_size as f64);
                                    metrics.bytes_written += file_size;
                                }
                                Err(err) => {
                                    print_error("writing object", &err);
                                    let mut metrics = metrics.lock().unwrap();
                                    metrics.write_failures += 1;
                                }
                            }
                        }
                        Action::Read(internal_id, external_id, payload) => {
                            let file_size = payload.len;
                            let (usecase, organization_id, object_key) = &external_id;
                            match remote.read(usecase, *organization_id, object_key, payload).await {
                                Ok(_) => {
                                    workload.lock().unwrap().push_file(internal_id, external_id);
                                    let mut metrics = metrics.lock().unwrap();
                                    metrics.read_timing.add(start.elapsed().as_secs_f64());
                                    metrics.bytes_read += file_size;
                                }
                                Err(err) => {
                                    print_error("reading object", &err);
                                    let mut metrics = metrics.lock().unwrap();
                                    metrics.read_failures += 1;
                                }
                            }
                        }
                        Action::Delete(external_id) => {
                            let (usecase, organization_id, object_key) = &external_id;
                            if let Err(err) = remote.delete(usecase, *organization_id, object_key).await {
                                print_error("deleting object", &err);
                            }
                            let mut metrics = metrics.lock().unwrap();
                            metrics.delete_timing.add(start.elapsed().as_secs_f64());
                        }
                    }
                    drop(permit);
                };
                tokio::spawn(task);
            }
            _ = &mut sleep => {
                break;
            }
        }
    }

    // by acquiring *all* the semaphores, we essentially wait for all outstanding tasks to finish
    let _permits = semaphore.acquire_many(concurrency as u32).await;

    let metrics: WorkloadMetrics = {
        let mut metrics = metrics.lock().unwrap();
        std::mem::take(&mut metrics)
    };

    let workload = Arc::try_unwrap(workload)
        .map_err(|_| ())
        .unwrap()
        .into_inner()
        .unwrap();

    let elapsed = start.elapsed();

    (workload, metrics, elapsed)
}

async fn run_batch_workload(
    remote: Arc<HttpRemote>,
    workload: Workload,
    duration: Duration,
    ttl: Option<Duration>,
) -> (Workload, WorkloadMetrics, Duration) {
    let concurrency = workload.concurrency;
    let batch_size = workload.batch_write_count();

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + duration;

    let workload = Arc::new(Mutex::new(workload));
    let metrics = Arc::new(Mutex::new(WorkloadMetrics::default()));

    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);

    loop {
        if deadline.elapsed() > Duration::ZERO {
            break;
        }
        tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                let workload = Arc::clone(&workload);
                let remote = Arc::clone(&remote);
                let metrics = Arc::clone(&metrics);

                let (payloads, usecase, org_id) = {
                    let mut wl = workload.lock().unwrap();
                    let payloads = wl.next_write_payloads(batch_size);
                    let org_id = wl.next_organization_id();
                    let mut usecase = Usecase::new(&wl.name);
                    if let Some(ttl) = ttl {
                        usecase = usecase.with_expiration_policy(ExpirationPolicy::TimeToLive(ttl));
                    }
                    (payloads, usecase, org_id)
                };

                let task = async move {
                    let session = remote.session(&usecase, org_id);
                    let mut many = session.many();

                    let mut payload_info = HashMap::with_capacity(payloads.len());

                    for (internal_id, payload) in payloads {
                        let key = internal_id.to_string();
                        payload_info.insert(key.clone(), (internal_id, payload.len));
                        let stream = ReaderStream::new(payload).boxed();
                        many = many.push(
                            session
                                .put_stream(stream)
                                .compression(None)
                                .key(key),
                        );
                    }

                    metrics.lock().unwrap().many_requests += 1;

                    let batch_start = Instant::now();
                    let mut results = many.send();

                    while let Some(result) = results.next().await {
                        match result {
                            objectstore_client::OperationResult::Put(key, Ok(_)) => {
                                if let Some((internal_id, file_size)) =
                                    payload_info.remove(&key)
                                {
                                    let external_id = (usecase.clone(), org_id, key);
                                    workload.lock().unwrap().push_file(internal_id, external_id);

                                    let mut m = metrics.lock().unwrap();
                                    m.file_sizes.add(file_size as f64);
                                    m.bytes_written += file_size;
                                } else {
                                    eprintln!("batch put returned unknown key: {key}");
                                }
                            }
                            objectstore_client::OperationResult::Put(_, Err(err)) => {
                                print_error("batch write", &err.into());
                                metrics.lock().unwrap().write_failures += 1;
                            }
                            objectstore_client::OperationResult::Error(err) => {
                                print_error("batch request", &err.into());
                                metrics.lock().unwrap().write_failures += 1;
                            }
                            other => {
                                eprintln!("unexpected batch result: {other:?}");
                            }
                        }
                    }

                    metrics
                        .lock()
                        .unwrap()
                        .batch_timing
                        .add(batch_start.elapsed().as_secs_f64());
                    drop(permit);
                };
                tokio::spawn(task);
            }
            _ = &mut sleep => {
                break;
            }
        }
    }

    let _permits = semaphore.acquire_many(concurrency as u32).await;

    let metrics: WorkloadMetrics = {
        let mut metrics = metrics.lock().unwrap();
        std::mem::take(&mut metrics)
    };

    let workload = Arc::try_unwrap(workload)
        .map_err(|_| ())
        .unwrap()
        .into_inner()
        .unwrap();

    let elapsed = start.elapsed();

    (workload, metrics, elapsed)
}

fn print_error(message: &str, error: &Error) {
    eprintln!("{} {}", "ERROR:".bold().red(), message.bold());
    for source in error.chain() {
        eprintln!("  {}: {source}", "caused by".italic());
    }
}

fn print_metrics(metrics: &WorkloadMetrics, duration: Duration) {
    let sketch = &metrics.file_sizes;
    if sketch.count() > 0 {
        print!("{} ({} ops", "WRITE:".bold().green(), sketch.count().bold());
        if metrics.many_requests > 0 {
            print!(", {} many requests", metrics.many_requests.bold());
        }
        if metrics.write_failures > 0 {
            print!(
                ", {}",
                format!("{} FAILURES", metrics.write_failures).bold().red()
            )
        }
        println!(")");
        let avg = ByteSize::b((sketch.sum().unwrap() / sketch.count() as f64) as u64);
        let p50 = ByteSize::b(sketch.quantile(0.5).unwrap().unwrap() as u64);
        let p90 = ByteSize::b(sketch.quantile(0.9).unwrap().unwrap() as u64);
        let p99 = ByteSize::b(sketch.quantile(0.99).unwrap().unwrap() as u64);
        println!(
            "  size avg: {}; p50: {p50:.2}; p90: {p90:.2}; p99: {p99:.2}",
            avg.bold()
        );

        // Always show individual ops/s (file_sizes tracks every individual write)
        print_ops(sketch, duration);
        if metrics.many_requests > 0 {
            let many_ps = metrics.many_requests as f64 / duration.as_secs_f64();
            print!(", {:.2} \"many\" operations/s", many_ps.bold());
        }
        print_throughput(metrics.bytes_written, duration);

        // Show timing percentiles, keeping batch and individual write timings separate
        let has_write_timing = metrics.write_timing.count() > 0;
        let has_batch_timing = metrics.batch_timing.count() > 0;
        match (has_write_timing, has_batch_timing) {
            (true, true) => {
                print_percentiles("write ", &metrics.write_timing, Duration::from_secs_f64);
                print_percentiles("batch ", &metrics.batch_timing, Duration::from_secs_f64);
            }
            (true, false) => {
                print_percentiles("", &metrics.write_timing, Duration::from_secs_f64);
            }
            (false, true) => {
                print_percentiles("", &metrics.batch_timing, Duration::from_secs_f64);
            }
            (false, false) => {}
        }
    } else if metrics.write_failures > 0 {
        println!(
            "{}",
            format!("{} WRITE FAILURES", metrics.write_failures)
                .bold()
                .red()
        );
    }
    if metrics.read_timing.count() > 0 {
        print!(
            "{} ({} ops",
            "READ:".bold().green(),
            metrics.read_timing.count().bold()
        );
        if metrics.read_failures > 0 {
            print!(
                ", {}",
                format!("{} FAILURES", metrics.read_failures).bold().red()
            )
        }
        println!(")");
        print_ops(&metrics.read_timing, duration);
        print_throughput(metrics.bytes_read, duration);
        print_percentiles("", &metrics.read_timing, Duration::from_secs_f64);
    } else if metrics.read_failures > 0 {
        println!(
            "{}",
            format!("{} READ FAILURES", metrics.read_failures)
                .bold()
                .red()
        );
    }
    if metrics.delete_timing.count() > 0 {
        println!(
            "{} ({} ops)",
            "DELETE:".bold().green(),
            metrics.delete_timing.count().bold()
        );
        print_ops(&metrics.delete_timing, duration);
        println!();
        print_percentiles("", &metrics.delete_timing, Duration::from_secs_f64);
    }
}

fn print_percentiles<T: fmt::Debug>(label: &str, sketch: &DDSketch, map: impl Fn(f64) -> T) {
    let ops = sketch.count();
    let avg = map(sketch.sum().unwrap() / ops as f64);
    let p50 = map(sketch.quantile(0.5).unwrap().unwrap());
    let p90 = map(sketch.quantile(0.9).unwrap().unwrap());
    let p99 = map(sketch.quantile(0.99).unwrap().unwrap());
    println!(
        "  {label}avg: {:.2?}; p50: {p50:.2?}; p90: {p90:.2?}; p99: {p99:.2?}",
        avg.bold()
    );
}

fn print_ops(sketch: &DDSketch, duration: Duration) {
    let ops = sketch.count();
    let ops_ps = ops as f64 / duration.as_secs_f64();
    print!("  {:.2} operations/s", ops_ps.bold());
}

fn print_throughput(total: u64, duration: Duration) {
    let throughput = (total as f64 / duration.as_secs_f64()) as u64;
    println!(", {:.2}/s", ByteSize::b(throughput).bold());
}

#[derive(Default)]
struct WorkloadMetrics {
    file_sizes: DDSketch,

    bytes_written: u64,
    bytes_read: u64,

    write_timing: DDSketch,
    batch_timing: DDSketch,
    read_timing: DDSketch,
    delete_timing: DDSketch,

    write_failures: u64,
    read_failures: u64,

    many_requests: u64,
}
