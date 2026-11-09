//! Concurrency limiter for backend operations.
//!
//! [`ConcurrencyLimiter`] caps the number of in-flight backend operations
//! using shared pool and admission primitives. Each acquired
//! [`ConcurrencyPermit`] notifies
//! waiters on drop, allowing [`ConcurrencyLimiter::wait_all`] to resolve once
//! all permits have been returned.
//!
//! [`run_metered`] runs an isolated task and waits for its result, while
//! [`spawn_metered`] starts the same task without waiting. Both provide panic
//! recovery and `service.task.*` metric emission.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt, stream};
use sentry::{Hub, SentryFutureExt, TransactionContext};
use std::task::Poll;
use tokio::sync::{Notify, OwnedSemaphorePermit};

pub(crate) mod limiter;
use limiter::{Limiter, PermitPool, Priority, QueueLimit, Scheduling, WaitConfig};

use crate::error::{Error, ErrorKind, Panic, Result};

/// Interval for the periodic metrics emitter.
#[cfg(test)]
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
    /// Bulk permits reserved, including operations waiting for execution capacity.
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
    normal: Limiter,
    bulk: Limiter,
}

impl ConcurrencyLimiter {
    /// Creates a new limiter with the given maximum number of permits.
    ///
    /// By default the queue depth is zero, preserving the original try-or-reject behavior. Use
    /// [`with_queue`](Self::with_queue) to enable bounded waiting.
    ///
    /// The bulk budget defaults to 100% of `max` (no restriction); use
    /// [`with_bulk`](Self::with_bulk) to set a safe operating point for bulk traffic.
    pub fn new(max: u32) -> Self {
        let pool = PermitPool::new(max, Scheduling::Fifo);
        let wait = WaitConfig {
            queue: QueueLimit::Bounded(0),
            timeout: Duration::from_secs(1),
        };
        Self {
            normal: Limiter::new(pool.clone(), Priority::Normal, wait),
            bulk: Limiter::new(
                pool,
                Priority::Normal,
                WaitConfig {
                    queue: QueueLimit::Unbounded,
                    ..wait
                },
            )
            .with_budget(max),
        }
    }

    /// Enables bounded waiting when all execution permits are held.
    ///
    /// Up to `size` additional callers may park in [`acquire`](Self::acquire)
    /// waiting for a permit. Callers beyond that are rejected immediately.
    pub fn with_queue(mut self, size: u32) -> Self {
        self.normal = Limiter::new(
            self.normal.pool().clone(),
            Priority::Normal,
            WaitConfig {
                queue: QueueLimit::Bounded(size),
                timeout: self.normal.timeout(),
            },
        );
        self
    }

    /// Sets the maximum time a caller may wait for a permit.
    ///
    /// Applies to both [`acquire`](Self::acquire) (when parked in the
    /// queue) and [`acquire_bulk`](Self::acquire_bulk) (waiting for the
    /// bulk and execution semaphores). Defaults to 1 second.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.normal = self.normal.with_timeout(timeout);
        self.bulk = self.bulk.with_timeout(timeout);
        self
    }

    /// Sets the bulk concurrency budget as a percentage of `max`.
    ///
    /// `percent` is clamped to `1..=100`. At `100` (the default), bulk
    /// operations can use all execution slots. Lower values set a safe
    /// operating point below which there is little-to-no performance
    /// degradation, allowing more tasks to queue before rejection is
    /// necessary — e.g. `60` means bulk operations can hold at most 60%
    /// of permits.
    pub fn with_bulk(mut self, percent: u32) -> Self {
        let clamped = percent.min(100);
        let capacity = (self.total_permits() * clamped).div_ceil(100).max(1);
        self.bulk = self.bulk.with_budget(capacity);
        self
    }

    /// Acquires a single concurrency permit, waiting if necessary.
    ///
    /// If a permit is free, returns immediately without touching the
    /// queue. Otherwise, acquires a queue ticket (bounded by the queue
    /// depth) and waits up to the configured timeout. Returns
    /// [`ErrorKind::AtCapacity`] if the queue is full or on timeout.
    pub async fn acquire(&self) -> Result<ConcurrencyPermit> {
        self.normal.acquire().await
    }

    /// Tries to acquire a single permit without waiting.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when no permits are available.
    pub fn try_acquire(&self) -> Result<ConcurrencyPermit> {
        self.normal.try_acquire()
    }

    /// Acquires a single permit for a bulk operation, waiting if necessary.
    ///
    /// Bulk operations are bounded by the bulk budget — a safe operating
    /// point below which there is little-to-no performance degradation.
    /// Both the bulk semaphore and the inner execution semaphore are
    /// acquired under a single timeout deadline configured via
    /// [`with_timeout`](Self::with_timeout).
    ///
    /// Returns [`ErrorKind::AtCapacity`] on timeout or when `max` is zero.
    pub async fn acquire_bulk(&self) -> Result<ConcurrencyPermit> {
        self.bulk.acquire().await
    }

    /// Returns the number of permits currently available.
    pub fn available_permits(&self) -> u32 {
        self.normal.pool().available()
    }

    /// Returns the number of permits currently held.
    pub fn used_permits(&self) -> u32 {
        self.normal.pool().in_use()
    }

    /// Returns the total number of permits.
    pub fn total_permits(&self) -> u32 {
        self.normal.pool().capacity()
    }

    /// Returns the number of callers currently waiting in the queue.
    pub fn queued_permits(&self) -> u32 {
        self.normal.waiting() as u32
    }

    /// Returns the configured queue capacity.
    pub fn total_queue(&self) -> u32 {
        self.normal.queue_capacity()
    }

    /// Returns the number of bulk permits currently held.
    pub fn used_bulk_permits(&self) -> u32 {
        self.bulk.budget_in_use()
    }

    /// Returns the bulk concurrency budget.
    pub fn total_bulk(&self) -> u32 {
        self.bulk.budget_capacity()
    }

    /// Waits until all permits have been returned.
    #[allow(dead_code)]
    pub async fn wait_all(&self) {
        self.normal.pool().wait_all().await
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
    pub async fn run_emitter<F, Fut>(&self, emit: F)
    where
        F: FnMut(Stats) -> Fut,
        Fut: Future<Output = ()>,
    {
        limiter::run_emitter(|| self.stats(), emit).await;
    }
}

/// RAII guard for a concurrency permit.
///
/// Dropping this permit releases it back to the [`ConcurrencyLimiter`] and
/// notifies any task waiting in [`ConcurrencyLimiter::wait_all`].
///
/// The execution permit drops before the bulk permit so that a waiter
/// blocked on the task semaphore sees the freed slot immediately.
pub struct ConcurrencyPermit {
    task_permit: Option<OwnedSemaphorePermit>,
    bulk_permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl ConcurrencyPermit {
    /// Holds this permit until the stream ends, returns its first error, or is dropped.
    ///
    /// The wrapped stream is dropped before capacity is released. After EOF or
    /// the first error, subsequent polls return EOF without polling the source.
    pub fn guard_stream(
        self,
        stream: crate::stream::PayloadStream,
    ) -> crate::stream::PayloadStream {
        // Tuple fields drop in declaration order.
        let mut guarded = Some((stream, self));
        stream::poll_fn(move |cx| {
            let Some((stream, _)) = guarded.as_mut() else {
                return Poll::Ready(None);
            };
            let result = stream.as_mut().poll_next(cx);
            if matches!(result, Poll::Ready(None | Some(Err(_)))) {
                guarded.take();
            }
            result
        })
        .boxed()
    }
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
        self.released.notify_waiters();
    }
}

/// Runs a future on a dedicated task with panic isolation and timing metrics.
///
/// The `guard` is moved into the spawned task and dropped after the future
/// completes, ensuring any resource it represents (e.g. a concurrency permit)
/// outlives the operation.
///
/// Emits `service.task.start` (counter) before spawning and
/// `service.task.duration` (distribution) when the task completes, both tagged
/// with the given `operation` name. The duration tag includes an `outcome` of
/// `"success"` or `"error"`.
pub async fn run_metered<T, G, F>(operation: &'static str, guard: G, f: F) -> Result<T>
where
    T: Send + 'static,
    G: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    let receiver = spawn_metered_inner(operation, guard, f);
    receiver.await.map_err(|_| {
        let error = Error::new(ErrorKind::Internal, "service task dropped");
        objectstore_log::error!(!!&error, operation, "Task failed");
        error
    })?
}

/// Spawns a metered future without waiting for its result.
///
/// The task has the same panic isolation, metrics, and guard lifetime as
/// [`run_metered`], but its result is discarded.
pub fn spawn_metered<T, G, F>(operation: &'static str, guard: G, f: F)
where
    T: Send + 'static,
    G: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    drop(spawn_metered_inner(operation, guard, f));
}

fn spawn_metered_inner<T, G, F>(
    operation: &'static str,
    guard: G,
    f: F,
) -> tokio::sync::oneshot::Receiver<Result<T>>
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
                .unwrap_or_else(|payload| Err(Panic::new(payload).into()));

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

    rx
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

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
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
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

    #[tokio::test(start_paused = true)]
    async fn queue_zero_rejects_immediately() {
        let limiter = ConcurrencyLimiter::new(2);
        assert_eq!(limiter.total_queue(), 0);

        let p1 = limiter.try_acquire().unwrap();
        let p2 = limiter.try_acquire().unwrap();
        assert!(
            limiter
                .try_acquire()
                .is_err_and(|error| error.kind() == ErrorKind::AtCapacity)
        );

        drop(p1);
        assert!(limiter.try_acquire().is_ok());
        drop(p2);

        // Bulk holding all permits also rejects instantly (no timeout wait).
        let mut bulk_permits = Vec::new();
        for _ in 0..2 {
            let permit = limiter.acquire_bulk().await.unwrap();
            bulk_permits.push(permit);
        }

        let start = tokio::time::Instant::now();
        let result = limiter.acquire().await;
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(start.elapsed(), Duration::ZERO);
        drop(bulk_permits);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_succeeds_immediately_when_available() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(3);

        let permit = limiter.acquire().await.unwrap();
        assert_eq!(limiter.used_permits(), 1);
        assert_eq!(limiter.queued_permits(), 0);
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_waits_and_succeeds_after_release() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2);

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
        let limiter = ConcurrencyLimiter::new(1).with_queue(2);

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire().await });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let result = waiter.await.unwrap();
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(limiter.queued_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_rejects_over_max_plus_queue() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(1);

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let _waiter = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;

        assert_eq!(limiter.queued_permits(), 1);
        let result = limiter.acquire().await;
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_parked_acquire_releases_queue_slot() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(1);

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

    #[tokio::test(start_paused = true)]
    async fn queued_permits_reflects_state() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(3);

        assert_eq!(limiter.queued_permits(), 0);
        let _p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.queued_permits(), 0);
        let _p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.queued_permits(), 0);
        drop(_p1);
        drop(_p2);

        // Exact count when bulk holds permits.
        let _bulk = limiter.acquire_bulk().await.unwrap();
        let _bulk2 = limiter.acquire_bulk().await.unwrap();
        assert_eq!(limiter.queued_permits(), 0);

        let limiter2 = limiter.clone();
        let _waiter = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_rejects_immediately_when_max_is_zero() {
        let limiter = ConcurrencyLimiter::new(0).with_queue(5);

        let start = tokio::time::Instant::now();
        let result = limiter.acquire().await;
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_queued_count() {
        let limiter = ConcurrencyLimiter::new(1).with_queue(2);
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

        // Allow at least one bulk permit even when the percentage is zero.
        let limiter = ConcurrencyLimiter::new(1).with_bulk(60);
        assert_eq!(limiter.total_bulk(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_caps_at_budget() {
        let limiter = ConcurrencyLimiter::new(10).with_queue(5).with_bulk(90);
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
        let limiter = ConcurrencyLimiter::new(10).with_queue(5);

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
        let limiter = ConcurrencyLimiter::new(1).with_queue(2);

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
        let limiter = ConcurrencyLimiter::new(1);

        let _held = limiter.acquire().await.unwrap();

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire_bulk().await });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let result = waiter.await.unwrap();
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(limiter.used_bulk_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_cancellation_leaks_nothing() {
        let limiter = ConcurrencyLimiter::new(2).with_queue(2);

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
        let limiter = ConcurrencyLimiter::new(0).with_queue(5);

        let start = tokio::time::Instant::now();
        let result = limiter.acquire_bulk().await;
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_with_zero_percent_allows_one() {
        let limiter = ConcurrencyLimiter::new(10).with_bulk(0);

        assert_eq!(limiter.total_bulk(), 1);

        let permit = limiter.acquire_bulk().await.unwrap();
        assert_eq!(limiter.used_bulk_permits(), 1);

        let limiter2 = limiter.clone();
        let waiter = tokio::spawn(async move { limiter2.acquire_bulk().await });
        tokio::time::sleep(Duration::from_secs(2)).await;

        let result = waiter.await.unwrap();
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn queue_bounded_under_bulk_load() {
        let limiter = ConcurrencyLimiter::new(3).with_queue(2);

        let mut bulk_permits = Vec::new();
        for _ in 0..3 {
            let permit = limiter.acquire_bulk().await.unwrap();
            bulk_permits.push(permit);
        }

        let limiter2 = limiter.clone();
        let _w1 = tokio::spawn(async move { limiter2.acquire().await });
        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 1);

        let limiter3 = limiter.clone();
        let _w2 = tokio::spawn(async move { limiter3.acquire().await });
        tokio::task::yield_now().await;
        assert_eq!(limiter.queued_permits(), 2);

        // Third waiter exceeds queue depth — rejected instantly.
        let start = tokio::time::Instant::now();
        let result = limiter.acquire().await;
        assert!(result.is_err_and(|error| error.kind() == ErrorKind::AtCapacity));
        assert_eq!(start.elapsed(), Duration::ZERO);

        drop(bulk_permits);
    }
}
