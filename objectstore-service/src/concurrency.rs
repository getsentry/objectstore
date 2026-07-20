//! Concurrency limiter for backend operations.
//!
//! [`ConcurrencyLimiter`] caps the number of in-flight backend operations
//! using a tokio semaphore. Each acquired [`ConcurrencyPermit`] notifies
//! waiters on drop, allowing [`ConcurrencyLimiter::wait_all`] to resolve once
//! all permits have been returned.
//!
//! [`spawn_metered`] spawns an arbitrary future as an isolated task with panic
//! recovery and `service.task.*` metric emission.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use sentry::{Hub, SentryFutureExt, TransactionContext};
use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{Error, Result};

/// Interval for the periodic metrics emitter.
const EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Snapshot of concurrency limiter state.
///
/// Passed to the callback registered via
/// [`ConcurrencyLimiter::run_emitter`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// Number of execution permits currently held.
    pub in_use: u32,
    /// Number of callers waiting in the queue for a permit.
    pub queued: u32,
    /// Number of bulk operations currently in flight.
    pub bulk_in_use: u32,
}

/// Limits concurrent backend operations and tracks in-flight count.
///
/// Permits are acquired with [`acquire`](Self::acquire) or
/// [`acquire_bulk`](Self::acquire_bulk) and automatically returned when
/// the [`ConcurrencyPermit`] is dropped.
///
/// Bulk operations use a separate budget semaphore that limits how many
/// execution slots they may occupy. This is intended as a safe operating
/// point — below this level there should be little-to-no performance
/// degradation, leaving room for more tasks to be admitted via the queue
/// before rejection is necessary.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimiter {
    tasks: Arc<Semaphore>,
    queue: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
    tasks_total: u32,
    queue_total: u32,
    bulk_total: u32,
    timeout: Duration,
    released: Arc<Notify>,
}

impl ConcurrencyLimiter {
    /// Creates a new limiter with the given maximum number of permits.
    ///
    /// By default the queue depth is zero, preserving the original try-or-reject behavior. Use
    /// [`with_queue`](Self::with_queue) to enable bounded waiting.
    ///
    /// The bulk budget defaults to 100% of `max` (no restriction); use
    /// [`with_bulk_percent`](Self::with_bulk_percent) to set a safe operating point for bulk
    /// traffic.
    pub fn new(max: u32) -> Self {
        Self {
            tasks: Arc::new(Semaphore::new(max as usize)),
            queue: Arc::new(Semaphore::new(max as usize)),
            bulk: Arc::new(Semaphore::new(max as usize)),
            tasks_total: max,
            queue_total: max,
            bulk_total: max,
            timeout: Duration::ZERO,
            released: Arc::new(Notify::new()),
        }
    }

    /// Enables bounded waiting when all execution permits are held.
    ///
    /// Up to `size` additional callers may park in [`acquire`](Self::acquire)
    /// waiting for a permit, each for at most `timeout`. Callers beyond
    /// that are rejected immediately.
    pub fn with_queue(mut self, size: u32, timeout: Duration) -> Self {
        self.queue_total = self.tasks_total.saturating_add(size);
        self.queue = Arc::new(Semaphore::new((self.queue_total) as usize));
        self.timeout = timeout;
        self
    }

    /// Sets the bulk concurrency budget as a percentage of `max`.
    ///
    /// `percent` is clamped to `0..=100`. At `100` (the default), bulk
    /// operations can use all execution slots. Lower values set a safe
    /// operating point below which there is little-to-no performance
    /// degradation, allowing more tasks to queue before rejection is
    /// necessary — e.g. `60` means bulk operations can hold at most 60%
    /// of permits.
    pub fn with_bulk(mut self, percent: u32) -> Self {
        let clamped = percent.min(100);
        self.bulk_total = self.tasks_total * clamped / 100;
        self.bulk = Arc::new(Semaphore::new(self.bulk_total as usize));
        self
    }

    /// Acquires a single concurrency permit, waiting if necessary.
    ///
    /// If all `max + queue` slots are occupied, returns
    /// [`Error::AtCapacity`] immediately. Otherwise, waits up to the
    /// configured queue timeout for an execution permit to become
    /// available. Returns [`Error::AtCapacity`] on timeout.
    pub async fn acquire(&self) -> Result<ConcurrencyPermit> {
        if self.tasks_total == 0 {
            return Err(Error::AtCapacity);
        }

        let queue_permit = self
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;

        let acquire = self.tasks.clone().acquire_owned();
        let task_permit = tokio::time::timeout(self.timeout, acquire)
            .await
            .map_err(|_| Error::AtCapacity)?
            .map_err(|_| Error::AtCapacity)?;

        Ok(ConcurrencyPermit {
            task_permit: Some(task_permit),
            bulk_permit: None,
            queue_permit: Some(queue_permit),
            released: Arc::clone(&self.released),
        })
    }

    /// Tries to acquire a single permit without waiting.
    ///
    /// Returns [`Error::AtCapacity`] when no permits are available.
    pub fn try_acquire(&self) -> Result<ConcurrencyPermit> {
        let queue_permit = self
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;

        let task_permit = self
            .tasks
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;

        Ok(ConcurrencyPermit {
            task_permit: Some(task_permit),
            bulk_permit: None,
            queue_permit: Some(queue_permit),
            released: Arc::clone(&self.released),
        })
    }

    /// Acquires a single permit for a bulk operation, waiting if necessary.
    ///
    /// Bulk operations are bounded by the bulk budget — a safe operating
    /// point below which there is little-to-no performance degradation.
    /// Both the bulk semaphore and the inner execution semaphore are
    /// acquired under a single [`queue_timeout`](Self::with_queue)
    /// deadline.
    ///
    /// Returns [`Error::AtCapacity`] on timeout or when `max` is zero.
    pub async fn acquire_bulk(&self) -> Result<ConcurrencyPermit> {
        if self.tasks_total == 0 {
            return Err(Error::AtCapacity);
        }

        let bulk_sem = self.bulk.clone();
        let tasks_sem = self.tasks.clone();

        let acquire = async move {
            let bulk_permit = bulk_sem.acquire_owned().await?;
            let task_permit = tasks_sem.acquire_owned().await?;
            Ok((task_permit, bulk_permit))
        };

        let (task_permit, bulk_permit) = tokio::time::timeout(self.timeout, acquire)
            .await
            .map_err(|_| Error::AtCapacity)?
            .map_err(|_: AcquireError| Error::AtCapacity)?;

        Ok(ConcurrencyPermit {
            task_permit: Some(task_permit),
            bulk_permit: Some(bulk_permit),
            queue_permit: None,
            released: Arc::clone(&self.released),
        })
    }

    /// Returns the number of permits currently available.
    pub fn available_permits(&self) -> u32 {
        u32::try_from(self.tasks.available_permits()).unwrap_or(self.tasks_total)
    }

    /// Returns the number of permits currently held.
    pub fn used_permits(&self) -> u32 {
        self.tasks_total - self.available_permits()
    }

    /// Returns the total number of permits.
    pub fn total_permits(&self) -> u32 {
        self.tasks_total
    }

    /// Returns the number of callers currently waiting in the queue.
    pub fn queued_permits(&self) -> u32 {
        let available = u32::try_from(self.queue.available_permits()).unwrap_or(self.queue_total);
        (self.queue_total - available).saturating_sub(self.used_permits())
    }

    /// Returns the configured queue capacity.
    pub fn total_queue(&self) -> u32 {
        self.queue_total - self.tasks_total
    }

    /// Returns the number of bulk permits currently held.
    pub fn used_bulk_permits(&self) -> u32 {
        let available = u32::try_from(self.bulk.available_permits()).unwrap_or(self.bulk_total);
        self.bulk_total - available
    }

    /// Returns the bulk concurrency budget.
    pub fn total_bulk(&self) -> u32 {
        self.bulk_total
    }

    /// Waits until all permits have been returned.
    #[allow(dead_code)]
    pub async fn wait_all(&self) {
        loop {
            let notified = self.released.notified();
            if self.used_permits() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Returns a snapshot of the current counts.
    pub fn stats(&self) -> Stats {
        Stats {
            in_use: self.used_permits(),
            queued: self.queued_permits(),
            bulk_in_use: self.used_bulk_permits(),
        }
    }

    /// Periodically calls `emit` with the current in-use and queued counts.
    ///
    /// This future runs forever and is intended to be spawned as a background
    /// task alongside the service.
    pub async fn run_emitter<F, Fut>(&self, mut emit: F)
    where
        F: FnMut(Stats) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ticker = tokio::time::interval(EMITTER_INTERVAL);
        loop {
            ticker.tick().await;
            emit(self.stats()).await;
        }
    }
}

/// RAII guard for a concurrency permit.
///
/// Dropping this permit releases it back to the [`ConcurrencyLimiter`] and
/// notifies any task waiting in [`ConcurrencyLimiter::wait_all`].
///
/// Fields are ordered so that the inner (execution) permit drops first,
/// then the bulk and queue permits — a waiter that claims a freed slot
/// can immediately see the freed execution slot.
pub struct ConcurrencyPermit {
    task_permit: Option<OwnedSemaphorePermit>,
    bulk_permit: Option<OwnedSemaphorePermit>,
    queue_permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl std::fmt::Debug for ConcurrencyPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyPermit").finish_non_exhaustive()
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        drop(self.task_permit.take());
        drop(self.bulk_permit.take());
        drop(self.queue_permit.take());
        self.released.notify_waiters();
    }
}

/// Spawns a future on a dedicated task with panic isolation and timing metrics.
///
/// The `guard` is moved into the spawned task and dropped after the future
/// completes, ensuring any resource it represents (e.g. a concurrency permit)
/// outlives the operation.
///
/// Emits `service.task.start` (counter) before spawning and
/// `service.task.duration` (distribution) when the task completes, both tagged
/// with the given `operation` name. The duration tag includes an `outcome` of
/// `"success"` or `"error"`.
pub async fn spawn_metered<T, G, F>(operation: &'static str, guard: G, f: F) -> Result<T>
where
    T: Send + 'static,
    G: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    objectstore_metrics::count!("service.task.start", operation = operation);

    let hub = Hub::current();
    let span = hub.configure_scope(|scope| scope.get_span());

    let new_hub = Hub::new_from_top(hub);
    let transaction = new_hub.start_transaction(TransactionContext::continue_from_span(
        operation,
        "tokio.task",
        span,
    ));

    let scope_guard = new_hub.push_scope();
    new_hub.configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(
        async move {
            let start = tokio::time::Instant::now();
            let result = std::panic::AssertUnwindSafe(f)
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| Err(Error::panic(payload)));

            if let Err(ref e) = result {
                let error = e as &dyn std::error::Error;
                objectstore_log::event_dyn!(e.level(), error, operation, "Task failed");
            }

            objectstore_metrics::record!(
                "service.task.duration" = start.elapsed(),
                operation = operation,
                outcome = if result.is_ok() { "success" } else { "error" },
            );

            let _ = tx.send(result);
            drop(guard);
            transaction.finish();
            drop(scope_guard);
        }
        .bind_hub(new_hub),
    );

    rx.await.map_err(|_| {
        objectstore_log::error!(!!&Error::Dropped, operation, "Task failed");
        Error::Dropped
    })?
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::error::Error;

    #[test]
    fn available_permits_tracks_held() {
        let limiter = ConcurrencyLimiter::new(5);
        assert_eq!(limiter.available_permits(), 5);

        let p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.available_permits(), 4);

        let p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.available_permits(), 3);

        drop(p1);
        assert_eq!(limiter.available_permits(), 4);

        drop(p2);
        assert_eq!(limiter.available_permits(), 5);
    }

    #[test]
    fn total_permits_returns_configured_max() {
        let limiter = ConcurrencyLimiter::new(42);
        assert_eq!(limiter.total_permits(), 42);
    }

    #[test]
    fn acquire_and_release() {
        let limiter = ConcurrencyLimiter::new(2);
        assert_eq!(limiter.used_permits(), 0);

        let p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.used_permits(), 1);

        let p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.used_permits(), 2);

        drop(p1);
        assert_eq!(limiter.used_permits(), 1);

        drop(p2);
        assert_eq!(limiter.used_permits(), 0);
    }

    #[test]
    fn at_capacity_rejects() {
        let limiter = ConcurrencyLimiter::new(1);
        let _permit = limiter.try_acquire().unwrap();

        let result = limiter.try_acquire();
        assert!(matches!(result, Err(Error::AtCapacity)));
    }

    #[test]
    fn permit_recovery_after_drop() {
        let limiter = ConcurrencyLimiter::new(1);

        let permit = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_err());

        drop(permit);
        assert!(limiter.try_acquire().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_calls_callback() {
        let limiter = ConcurrencyLimiter::new(5);
        let _permit = limiter.try_acquire().unwrap();

        let emitted_in_use = Arc::new(AtomicU32::new(0));
        let emitted_queued = Arc::new(AtomicU32::new(0));
        let in_use_clone = Arc::clone(&emitted_in_use);
        let queued_clone = Arc::clone(&emitted_queued);

        let emitter = limiter.run_emitter(move |stats| {
            let in_use_ref = Arc::clone(&in_use_clone);
            let queued_ref = Arc::clone(&queued_clone);
            async move {
                in_use_ref.store(stats.in_use, Ordering::Relaxed);
                queued_ref.store(stats.queued, Ordering::Relaxed);
            }
        });

        tokio::select! {
            _ = emitter => unreachable!("emitter runs forever"),
            _ = tokio::time::sleep(EMITTER_INTERVAL) => {}
        }

        assert_eq!(emitted_in_use.load(Ordering::Relaxed), 1);
        assert_eq!(emitted_queued.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn wait_all_resolves_when_permits_returned() {
        let limiter = ConcurrencyLimiter::new(2);
        let p1 = limiter.try_acquire().unwrap();
        let p2 = limiter.try_acquire().unwrap();

        let mut wait = Box::pin(limiter.wait_all());

        // Dropping one permit is not enough.
        drop(p1);
        assert!(futures::poll!(&mut wait).is_pending());

        // Dropping the last permit should resolve it.
        drop(p2);
        assert!(futures::poll!(&mut wait).is_ready());
    }

    #[tokio::test]
    async fn wait_all_returns_immediately_when_empty() {
        let limiter = ConcurrencyLimiter::new(5);
        let wait = Box::pin(limiter.wait_all());
        assert!(futures::poll!(wait).is_ready());
    }

    // --- Queue tests ---

    #[test]
    fn queue_zero_rejects_immediately() {
        let limiter = ConcurrencyLimiter::new(2);
        assert_eq!(limiter.total_queue(), 0);

        let p1 = limiter.try_acquire().unwrap();
        let p2 = limiter.try_acquire().unwrap();
        assert!(matches!(limiter.try_acquire(), Err(Error::AtCapacity)));

        drop(p1);
        assert!(limiter.try_acquire().is_ok());
        drop(p2);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_succeeds_immediately_when_available() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(3, Duration::from_secs(5));

        let permit = limiter.acquire().await.unwrap();
        assert_eq!(limiter.used_permits(), 1);
        assert_eq!(limiter.queued_permits(), 0);
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_waits_and_succeeds_after_release() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2, Duration::from_secs(5));

        let held = limiter.acquire().await.unwrap();
        assert_eq!(limiter.used_permits(), 1);

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire().await });

        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 1);

        drop(held);

        let permit = waiter.await.unwrap().unwrap();
        assert_eq!(limiter.used_permits(), 1);
        assert_eq!(limiter.queued_permits(), 0);
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_times_out() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2, Duration::from_secs(1));

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire().await });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let result = waiter.await.unwrap();
        assert!(matches!(result, Err(Error::AtCapacity)));
        assert_eq!(limiter.queued_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_rejects_over_max_plus_queue() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(1, Duration::from_secs(5));

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let _waiter = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;

        assert_eq!(limiter.queued_permits(), 1);
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(Error::AtCapacity)));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_parked_acquire_releases_queue_slot() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(1, Duration::from_secs(60));

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 1);

        waiter.abort();
        let _ = waiter.await;
        tokio::task::yield_now().await;

        assert_eq!(limiter.queued_permits(), 0);

        let limiter3 = limiter.clone();
        let replacement = tokio::spawn(async move { limiter3.acquire().await });
        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 1);
        drop(replacement);
    }

    #[test]
    fn queued_permits_reflects_state() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(3, Duration::from_secs(5));

        assert_eq!(limiter.queued_permits(), 0);
        let _p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.queued_permits(), 0);
        let _p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.queued_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_rejects_immediately_when_max_is_zero() {
        let limiter = ConcurrencyLimiter::new(0).with_queue(5, Duration::from_secs(10));

        let start = tokio::time::Instant::now();
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(Error::AtCapacity)));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_queued_count() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2, Duration::from_secs(60));
        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let _waiter = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;

        let emitted_in_use = Arc::new(AtomicU32::new(0));
        let emitted_queued = Arc::new(AtomicU32::new(0));
        let in_use_clone = Arc::clone(&emitted_in_use);
        let queued_clone = Arc::clone(&emitted_queued);

        let emitter = limiter.run_emitter(move |stats| {
            let in_use_ref = Arc::clone(&in_use_clone);
            let queued_ref = Arc::clone(&queued_clone);
            async move {
                in_use_ref.store(stats.in_use, Ordering::Relaxed);
                queued_ref.store(stats.queued, Ordering::Relaxed);
            }
        });

        tokio::select! {
            _ = emitter => unreachable!("emitter runs forever"),
            _ = tokio::time::sleep(EMITTER_INTERVAL) => {}
        }

        assert_eq!(emitted_in_use.load(Ordering::Relaxed), 1);
        assert_eq!(emitted_queued.load(Ordering::Relaxed), 1);
    }

    // --- Bulk tests ---

    #[test]
    fn bulk_defaults_to_full_capacity() {
        let limiter = ConcurrencyLimiter::new(100);
        assert_eq!(limiter.total_bulk(), 100);
    }

    #[test]
    fn bulk_percent_computes_correctly() {
        let limiter = ConcurrencyLimiter::new(100).with_bulk(60);
        assert_eq!(limiter.total_bulk(), 60);

        let limiter = ConcurrencyLimiter::new(10).with_bulk(90);
        assert_eq!(limiter.total_bulk(), 9);

        let limiter = ConcurrencyLimiter::new(100).with_bulk(150);
        assert_eq!(limiter.total_bulk(), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_caps_at_budget() {
        let limiter = ConcurrencyLimiter::new(10)
            .with_queue(5, Duration::from_secs(5))
            .with_bulk(90);
        let bulk_budget = limiter.total_bulk();
        assert_eq!(bulk_budget, 9);

        let mut permits = Vec::new();
        for _ in 0..bulk_budget {
            let permit = limiter.acquire_bulk().await.unwrap();
            permits.push(permit);
        }

        assert_eq!(limiter.used_bulk_permits(), bulk_budget);
        assert_eq!(limiter.used_permits(), bulk_budget);

        // Normal request can still acquire the remaining permit.
        let normal = limiter.acquire().await.unwrap();
        assert_eq!(limiter.used_permits(), 10);
        drop(normal);
        drop(permits);
    }

    #[tokio::test(start_paused = true)]
    async fn normal_uses_all_permits_when_bulk_idle() {
        let limiter = ConcurrencyLimiter::new(10).with_queue(5, Duration::from_secs(5));

        let mut permits = Vec::new();
        for _ in 0..10 {
            let permit = limiter.acquire().await.unwrap();
            permits.push(permit);
        }

        assert_eq!(limiter.used_permits(), 10);
        assert_eq!(limiter.used_bulk_permits(), 0);
        drop(permits);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_waits_for_inner_permit() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2, Duration::from_secs(5));

        let held = limiter.acquire().await.unwrap();
        assert_eq!(limiter.used_permits(), 1);

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire_bulk().await });
        tokio::task::yield_now().await;

        drop(held);

        let permit = waiter.await.unwrap().unwrap();
        assert_eq!(limiter.used_permits(), 1);
        assert_eq!(limiter.used_bulk_permits(), 1);
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_timeout_spans_both_waits() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(0, Duration::from_secs(1));

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire_bulk().await });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let result = waiter.await.unwrap();
        assert!(matches!(result, Err(Error::AtCapacity)));
        assert_eq!(limiter.used_bulk_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_cancellation_leaks_nothing() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(2, Duration::from_secs(60));

        let _held1 = limiter.acquire().await.unwrap();
        let _held2 = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire_bulk().await });
        tokio::task::yield_now().await;

        waiter.abort();
        let _ = waiter.await;
        tokio::task::yield_now().await;

        assert_eq!(limiter.used_bulk_permits(), 0);
        assert_eq!(limiter.used_permits(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_rejects_immediately_when_max_is_zero() {
        let limiter = ConcurrencyLimiter::new(0).with_queue(5, Duration::from_secs(10));

        let start = tokio::time::Instant::now();
        let result = limiter.acquire_bulk().await;
        assert!(matches!(result, Err(Error::AtCapacity)));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
