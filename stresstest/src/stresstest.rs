use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use bytesize::ByteSize;
use futures::StreamExt;
use sketches_ddsketch::DDSketch;
use tokio::sync::Semaphore;
use yansi::Paint;

use crate::http::HttpRemote;
use crate::workload::{Action, Workload};

pub async fn perform_stresstest(
    remote: HttpRemote,
    workloads: Vec<Workload>,
    duration: Duration,
) -> Result<()> {
    let remote = Arc::new(remote);
    // run the workloads concurrently
    let tasks: Vec<_> = workloads
        .into_iter()
        .map(|workload| {
            let remote = Arc::clone(&remote);
            tokio::spawn(run_workload(remote, workload, duration))
        })
        .collect();

    let finished_tasks = futures::future::join_all(tasks).await;

    let workloads = finished_tasks.into_iter().map(|task| {
        let (workload, metrics) = task.unwrap();

        println!();
        println!(
            "{} {} (concurrency: {})",
            "## Workload".bold(),
            workload.name.bold().blue(),
            workload.concurrency.bold()
        );
        println!("{}", "WRITE:".bold().green());
        let sketch = metrics.file_sizes;
        let avg = ByteSize::b((sketch.sum().unwrap() / sketch.count() as f64) as u64);
        let p50 = ByteSize::b(sketch.quantile(0.5).unwrap().unwrap() as u64);
        let p90 = ByteSize::b(sketch.quantile(0.9).unwrap().unwrap() as u64);
        let p99 = ByteSize::b(sketch.quantile(0.99).unwrap().unwrap() as u64);
        println!(
            "  size avg: {}; p50: {p50:.2}; p90: {p90:.2}; p99: {p99:.2}",
            avg.bold()
        );
        print_ops(&metrics.write_timing, duration);
        print_throughput(metrics.bytes_written, duration);
        print_percentiles(&metrics.write_timing, Duration::from_secs_f64);
        println!("{}", "READ:".bold().green());
        print_ops(&metrics.read_timing, duration);
        print_throughput(metrics.bytes_read, duration);
        print_percentiles(&metrics.read_timing, Duration::from_secs_f64);
        if metrics.delete_timing.count() > 0 {
            println!("{}", "DELETE:".bold().green());
            print_ops(&metrics.delete_timing, duration);
            println!();
            print_percentiles(&metrics.delete_timing, Duration::from_secs_f64);
        }

        workload
    });

    let workloads: Vec<_> = workloads.collect();
    let max_concurrency = workloads.iter().map(|w| w.concurrency).max().unwrap();
    let files_to_cleanup = workloads.into_iter().flat_map(|mut w| w.external_files());

    let start = Instant::now();
    let cleanup_timing = Arc::new(Mutex::new(DDSketch::default()));
    futures::stream::iter(files_to_cleanup)
        .for_each_concurrent(max_concurrency, |external_id| {
            let remote = remote.clone();
            let cleanup_timing = cleanup_timing.clone();
            async move {
                let start = Instant::now();
                remote.delete(external_id).await;
                cleanup_timing
                    .lock()
                    .unwrap()
                    .add(start.elapsed().as_secs_f64());
            }
        })
        .await;
    let cleanup_duration = start.elapsed();
    let cleanup_timing = cleanup_timing.lock().unwrap();

    println!();
    println!(
        "{} ({} files, concurrency: {})",
        "## CLEANUP".bold(),
        cleanup_timing.count().blue(),
        max_concurrency.bold()
    );
    print_ops(&cleanup_timing, cleanup_duration);
    println!();
    print_percentiles(&cleanup_timing, Duration::from_secs_f64);

    Ok(())
}

async fn run_workload(
    remote: Arc<HttpRemote>,
    workload: Workload,
    duration: Duration,
) -> (Workload, WorkloadMetrics) {
    let concurrency = workload.concurrency;
    let semaphore = Arc::new(Semaphore::new(concurrency));
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

                let task = async move {
                    let start = Instant::now();
                    let action = workload.lock().unwrap().next_action();
                    match action {
                        Action::Write(internal_id, payload) => {
                            let file_size = payload.len;
                            let external_id = remote.write(payload).await;
                            workload.lock().unwrap().push_file(internal_id, external_id);
                            let mut metrics = metrics.lock().unwrap();
                            metrics.write_timing.add(start.elapsed().as_secs_f64());
                            metrics.file_sizes.add(file_size as f64);
                            metrics.bytes_written += file_size;
                        }
                        Action::Read(internal_id, external_id, payload) => {
                            let file_size = payload.len;
                            remote.read(&external_id, payload).await;
                            workload.lock().unwrap().push_file(internal_id, external_id);
                            let mut metrics = metrics.lock().unwrap();
                            metrics.read_timing.add(start.elapsed().as_secs_f64());
                            metrics.bytes_read += file_size;
                        }
                        Action::Delete(external_id) => {
                            remote.delete(external_id).await;
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

    let metrics: WorkloadMetrics = {
        let mut metrics = metrics.lock().unwrap();
        std::mem::take(&mut metrics)
    };

    // by acquiring *all* the semaphores, we essentially wait for all outstanding tasks to finish
    let _permits = semaphore.acquire_many(concurrency as u32).await;

    let workload = Arc::try_unwrap(workload)
        .map_err(|_| ())
        .unwrap()
        .into_inner()
        .unwrap();

    (workload, metrics)
}

fn print_percentiles<T: fmt::Debug>(sketch: &DDSketch, map: impl Fn(f64) -> T) {
    let ops = sketch.count();
    let avg = map(sketch.sum().unwrap() / ops as f64);
    let p50 = map(sketch.quantile(0.5).unwrap().unwrap());
    let p90 = map(sketch.quantile(0.9).unwrap().unwrap());
    let p99 = map(sketch.quantile(0.99).unwrap().unwrap());
    println!(
        "  avg: {:.2?}; p50: {p50:.2?}; p90: {p90:.2?}; p99: {p99:.2?}",
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
    read_timing: DDSketch,
    delete_timing: DDSketch,
}
