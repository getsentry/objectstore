//! Simple and priority-based FIFO concurrency limiters.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt, stream};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{ErrorKind, Result};
use crate::stream::PayloadStream;

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Owned permit, released on drop.
#[derive(Debug)]
pub struct Permit {
    _permit: OwnedSemaphorePermit,
}

impl Permit {
    /// Wraps `stream` so that it holds this permit until EOF, the first error, or stream drop.
    pub fn guard_stream(self, stream: PayloadStream) -> PayloadStream {
        // Tuple field order ensures the stream drops before its permit.
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

/// Snapshot of permits in use and queued acquisitions for a simple limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LimiterStats {
    /// Permits currently in use.
    pub in_use: u32,
    /// Acquisitions waiting for a permit.
    pub queued: u32,
}

/// Shared interval loop for the metric emitters.
pub(crate) async fn run_emitter<S, F, Fut>(mut snapshot: impl FnMut() -> S, mut emit: F)
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut ticker = tokio::time::interval(METRICS_EMITTER_INTERVAL);
    loop {
        ticker.tick().await;
        emit(snapshot()).await;
    }
}

/// Limits concurrent operations with FIFO queuing and a configurable wait timeout.
#[derive(Clone, Debug)]
pub struct Limiter {
    semaphore: Arc<Semaphore>,
    queue: Option<Arc<Semaphore>>,
    queued: Arc<AtomicU32>,
    capacity: u32,
    timeout: Duration,
}

impl Limiter {
    /// Creates a FIFO limiter with the given capacity, wait timeout, and queue limit.
    pub fn new(capacity: NonZeroU32, timeout: Duration, max_queue_size: Option<u32>) -> Self {
        let capacity = capacity.get();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            queue: max_queue_size.map(|size| Arc::new(Semaphore::new(size as usize))),
            queued: Arc::new(AtomicU32::new(0)),
            capacity,
            timeout,
        }
    }

    /// Sets the wait timeout for this limiter handle.
    ///
    /// Existing clones keep their timeout and share execution capacity with this handle.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets a separate queue limit for this limiter handle, sharing execution capacity.
    ///
    /// Existing clones keep their queue. `None` allows an unbounded queue.
    pub fn with_queue(mut self, max_queue_size: Option<u32>) -> Self {
        self.queue = max_queue_size.map(|size| Arc::new(Semaphore::new(size as usize)));
        self.queued = Arc::new(AtomicU32::new(0));
        self
    }

    /// Acquires capacity immediately, or returns [`ErrorKind::AtCapacity`].
    pub fn try_acquire(&self) -> Result<Permit> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorKind::AtCapacity)?;
        Ok(Permit { _permit: permit })
    }

    /// Acquires capacity within the configured timeout, preserving FIFO order.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when the queue is full or the wait times out.
    /// Dropping the future releases its queue slot and any assigned permit.
    pub async fn acquire(&self) -> Result<Permit> {
        if let Ok(permit) = self.try_acquire() {
            return Ok(permit);
        }
        if self.timeout.is_zero() {
            return Err(ErrorKind::AtCapacity.into());
        }
        let _queued = self
            .queue
            .as_ref()
            .map(|queue| queue.try_acquire())
            .transpose()
            .map_err(|_| ErrorKind::AtCapacity)?;
        self.queued.fetch_add(1, Ordering::Relaxed);
        let _waiting = QueuedGuard(&self.queued);
        let permit = tokio::time::timeout(self.timeout, self.semaphore.clone().acquire_owned())
            .await
            .map_err(|_| ErrorKind::AtCapacity)?
            .map_err(|_| ErrorKind::AtCapacity)?;
        Ok(Permit { _permit: permit })
    }

    /// Returns the current number of permits in use and acquisitions in this handle's queue.
    pub fn stats(&self) -> LimiterStats {
        LimiterStats {
            in_use: self.capacity - self.semaphore.available_permits() as u32,
            queued: self.queued.load(Ordering::Relaxed),
        }
    }

    /// Passes this limiter's stats to `emit` immediately and once per second thereafter.
    pub async fn run_emitter<F, Fut>(&self, emit: F)
    where
        F: FnMut(LimiterStats) -> Fut,
        Fut: Future<Output = ()>,
    {
        run_emitter(|| self.stats(), emit).await;
    }
}

/// Removes an acquisition from the queue count on completion, timeout, or cancellation.
struct QueuedGuard<'a>(&'a AtomicU32);

impl Drop for QueuedGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Priority for an acquisition from [`PriorityLimiter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    /// Normal priority.
    Normal,
    /// High priority, blocks normal-priority waiters from entering the semaphore queue.
    High,
}

/// Approximate snapshot of permits and outstanding acquisitions by priority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PriorityLimiterStats {
    /// Permits currently in use.
    pub in_use: u32,
    /// Outstanding normal-priority acquisitions.
    pub waiting_normal: usize,
    /// Outstanding high-priority acquisitions.
    pub waiting_high: usize,
}

/// Limits concurrent operations with high-priority admission and FIFO within each priority.
///
/// Each priority has its own wait timeout and optional queue limit; execution capacity is shared.
/// Sustained high-priority traffic can starve normal-priority waiters.
#[derive(Clone, Debug)]
pub struct PriorityLimiter {
    normal_timeout: Duration,
    high_timeout: Duration,
    state: Arc<PriorityState>,
}

// Both priorities share `semaphore`.
//
// High-priority waiters increment `waiting_high`, and acquire `semaphore` directly.
//
// Normal-priority waiters lock `normal_lane`, so only one attempts acquisition at a time.
// That waiter first tries an immediate acquisition if `waiting_high == 0`.
// Otherwise, it waits on `high_drained` until `waiting_high == 0`, then acquires `semaphore`.
// If high-priority work arrives after this check, this one normal waiter can acquire first.
// Other normal waiters wait for `normal_lane` in the meantime.
#[derive(Debug)]
struct PriorityState {
    capacity: u32,
    semaphore: Arc<Semaphore>,
    normal_queue: Option<Semaphore>,
    high_queue: Option<Semaphore>,
    normal_lane: Mutex<()>,
    waiting_normal: AtomicUsize,
    waiting_high: AtomicUsize,
    high_drained: Notify,
}

impl PriorityState {
    fn waiting(&self, priority: Priority) -> &AtomicUsize {
        match priority {
            Priority::Normal => &self.waiting_normal,
            Priority::High => &self.waiting_high,
        }
    }

    async fn wait_until_high_drained(&self) {
        loop {
            // Create the notification future before checking the counter, so a notification
            // between the check and `.await` is not lost.
            let drained = self.high_drained.notified();
            if self.waiting_high.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
    }
}

impl PriorityLimiter {
    /// Creates a priority limiter with shared capacity and separate timeouts and queue limits.
    pub fn new(
        capacity: NonZeroU32,
        normal_timeout: Duration,
        high_timeout: Duration,
        max_normal_queue_size: Option<u32>,
        max_high_queue_size: Option<u32>,
    ) -> Self {
        Self {
            normal_timeout,
            high_timeout,
            state: Arc::new(PriorityState {
                capacity: capacity.get(),
                semaphore: Arc::new(Semaphore::new(capacity.get() as usize)),
                normal_queue: max_normal_queue_size.map(|size| Semaphore::new(size as usize)),
                high_queue: max_high_queue_size.map(|size| Semaphore::new(size as usize)),
                normal_lane: Mutex::new(()),
                waiting_normal: AtomicUsize::new(0),
                waiting_high: AtomicUsize::new(0),
                high_drained: Notify::new(),
            }),
        }
    }

    /// Acquires capacity within the timeout configured for the given priority.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when this priority's queue is full or the wait times out.
    /// Dropping the future releases its queue slot and any assigned permit.
    pub async fn acquire(&self, priority: Priority) -> Result<Permit> {
        let state = &self.state;
        let (timeout, queue) = match priority {
            Priority::Normal => (self.normal_timeout, &state.normal_queue),
            Priority::High => (self.high_timeout, &state.high_queue),
        };
        state.waiting(priority).fetch_add(1, Ordering::AcqRel);
        let _waiting = WaitingGuard { state, priority };

        let acquire = async {
            if priority == Priority::High {
                return state.semaphore.clone().acquire_owned().await;
            }

            let _lane = match state.normal_lane.try_lock() {
                Ok(lane) => lane,
                Err(_) => state.normal_lane.lock().await,
            };
            if state.waiting_high.load(Ordering::Acquire) == 0
                && let Ok(permit) = state.semaphore.clone().try_acquire_owned()
            {
                return Ok(permit);
            }

            state.wait_until_high_drained().await;
            state.semaphore.clone().acquire_owned().await
        };
        tokio::pin!(acquire);

        // Immediate acquisition does not consume queue capacity. Keep the same future if it
        // needs to wait, preserving its position in the lane or semaphore queue.
        let permit = match acquire.as_mut().now_or_never() {
            Some(permit) => permit,
            None => {
                if timeout.is_zero() {
                    return Err(ErrorKind::AtCapacity.into());
                }
                let _queued = queue
                    .as_ref()
                    .map(|queue| queue.try_acquire())
                    .transpose()
                    .map_err(|_| ErrorKind::AtCapacity)?;
                tokio::time::timeout(timeout, acquire)
                    .await
                    .map_err(|_| ErrorKind::AtCapacity)?
            }
        };
        let permit = permit.map_err(|_| ErrorKind::AtCapacity)?;
        Ok(Permit { _permit: permit })
    }

    /// Returns an approximate snapshot of permits and outstanding acquisitions.
    pub fn stats(&self) -> PriorityLimiterStats {
        let state = &self.state;
        PriorityLimiterStats {
            in_use: state.capacity - state.semaphore.available_permits() as u32,
            waiting_normal: state.waiting_normal.load(Ordering::Relaxed),
            waiting_high: state.waiting_high.load(Ordering::Relaxed),
        }
    }

    /// Passes this limiter's stats to `emit` immediately and once per second thereafter.
    pub async fn run_emitter<F, Fut>(&self, emit: F)
    where
        F: FnMut(PriorityLimiterStats) -> Fut,
        Fut: Future<Output = ()>,
    {
        run_emitter(|| self.stats(), emit).await;
    }
}

/// Tracks an acquisition until completion, timeout, or cancellation.
struct WaitingGuard<'a> {
    state: &'a PriorityState,
    priority: Priority,
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        let previous = self
            .state
            .waiting(self.priority)
            .fetch_sub(1, Ordering::AcqRel);
        if self.priority == Priority::High && previous == 1 {
            self.state.high_drained.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::{Stream, poll};
    use std::io;
    use std::sync::atomic::{AtomicBool, Ordering};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_current_limiter_usage() {
        let priority = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, TIMEOUT, None, None);
        let simple = Limiter::new(NonZeroU32::MIN, Duration::ZERO, None);
        let priority_permit = priority.acquire(Priority::Normal).await.unwrap();
        let simple_permit = simple.acquire().await.unwrap();
        let (priority_sender, mut priority_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (simple_sender, mut simple_receiver) = tokio::sync::mpsc::unbounded_channel();
        let priority_emitter = priority.run_emitter(|stats| {
            priority_sender.send(stats).unwrap();
            std::future::ready(())
        });
        let simple_emitter = simple.run_emitter(|stats| {
            simple_sender.send(stats).unwrap();
            std::future::ready(())
        });
        tokio::pin!(priority_emitter, simple_emitter);
        assert!(poll!(&mut priority_emitter).is_pending());
        assert!(poll!(&mut simple_emitter).is_pending());
        assert_eq!(priority_receiver.try_recv().unwrap().in_use, 1);
        assert_eq!(simple_receiver.try_recv().unwrap().in_use, 1);

        drop(priority_permit);
        drop(simple_permit);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut priority_emitter).is_pending());
        assert!(poll!(&mut simple_emitter).is_pending());
        assert_eq!(
            priority_receiver.try_recv().unwrap(),
            PriorityLimiterStats::default()
        );
        assert_eq!(simple_receiver.try_recv().unwrap(), LimiterStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn stream_retains_then_releases_at_terminal_item() {
        for error in [false, true] {
            let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO, None);
            let mut step = 0;
            let source = stream::poll_fn(move |_| {
                step += 1;
                match step {
                    1 => Poll::Ready(Some(Ok(Bytes::from_static(b"data")))),
                    2 => Poll::Pending,
                    3 if error => Poll::Ready(Some(Err(io::Error::other("original")))),
                    3 => Poll::Ready(None),
                    _ => panic!("polled after terminal item"),
                }
            })
            .boxed();
            let mut guarded = limiter.acquire().await.unwrap().guard_stream(source);
            assert_eq!(
                guarded.next().await.unwrap().unwrap(),
                Bytes::from_static(b"data")
            );
            assert_eq!(limiter.stats().in_use, 1);
            assert!(poll!(guarded.next()).is_pending());
            assert_eq!(limiter.stats().in_use, 1);
            let terminal = guarded.next().await;
            if error {
                assert_eq!(terminal.unwrap().unwrap_err().to_string(), "original");
            } else {
                assert!(terminal.is_none());
            }
            assert_eq!(limiter.stats().in_use, 0);
            assert!(guarded.next().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stream_drop_releases_after_stream() {
        struct PendingStream {
            dropped: Arc<AtomicBool>,
            limiter: Limiter,
        }
        impl Stream for PendingStream {
            type Item = io::Result<Bytes>;
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Poll::Pending
            }
        }
        impl Drop for PendingStream {
            fn drop(&mut self) {
                assert_eq!(self.limiter.stats().in_use, 1);
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO, None);
        let dropped = Arc::new(AtomicBool::new(false));
        let source = PendingStream {
            dropped: dropped.clone(),
            limiter: limiter.clone(),
        }
        .boxed();
        let mut guarded = limiter.acquire().await.unwrap().guard_stream(source);
        assert!(poll!(guarded.next()).is_pending());
        drop(guarded);
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(limiter.stats().in_use, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn simple_limiter_queue_limits_and_cleanup() {
        let immediate = Limiter::new(NonZeroU32::MIN, Duration::ZERO, None);
        let held = immediate.acquire().await.unwrap();
        assert_eq!(
            immediate.acquire().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        drop(held);

        for max_queue_size in [None, Some(0), Some(1)] {
            let limiter = Limiter::new(NonZeroU32::MIN, TIMEOUT, max_queue_size);
            let held = limiter.acquire().await.unwrap();
            assert_eq!(limiter.stats().queued, 0);
            let before = tokio::time::Instant::now();
            let mut waiting = Box::pin(limiter.acquire());
            if max_queue_size == Some(0) {
                assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);
                assert_eq!(tokio::time::Instant::now(), before);
                drop(held);
                continue;
            }
            assert!(poll!(&mut waiting).is_pending());
            assert_eq!(limiter.stats().queued, 1);
            if max_queue_size == Some(1) {
                assert_eq!(
                    limiter.acquire().await.unwrap_err().kind(),
                    ErrorKind::AtCapacity
                );
                assert_eq!(tokio::time::Instant::now(), before);
            }
            // Cancellation must free the queue slot for another waiter.
            drop(waiting);
            assert_eq!(limiter.stats().queued, 0);
            let mut timed_out = Box::pin(limiter.acquire());
            assert!(poll!(&mut timed_out).is_pending());
            tokio::time::advance(TIMEOUT).await;
            assert_eq!(timed_out.await.unwrap_err().kind(), ErrorKind::AtCapacity);
            assert_eq!(limiter.stats().queued, 0);
            // A timeout must also free the queue slot.
            let mut next = Box::pin(limiter.acquire());
            assert!(poll!(&mut next).is_pending());
            drop(held);
            drop(next.await.unwrap());
            assert_eq!(limiter.stats().in_use, 0);
            assert_eq!(limiter.stats().queued, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queue_handles_share_capacity_with_separate_limits_and_counts() {
        let bounded = Limiter::new(NonZeroU32::MIN, TIMEOUT, Some(1));
        let unbounded = bounded.clone().with_queue(None);
        let immediate = bounded.clone().with_timeout(Duration::ZERO);
        let held = bounded.try_acquire().unwrap();
        let mut bounded_wait = Box::pin(bounded.acquire());
        let mut unbounded_wait = Box::pin(unbounded.acquire());
        assert!(poll!(&mut bounded_wait).is_pending());
        assert!(poll!(&mut unbounded_wait).is_pending());
        assert_eq!(bounded.stats().queued, 1);
        assert_eq!(unbounded.stats().queued, 1);
        assert_eq!(
            bounded.acquire().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(
            unbounded.try_acquire().unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        let before = tokio::time::Instant::now();
        assert_eq!(
            immediate.acquire().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(tokio::time::Instant::now(), before);

        drop(held);
        // A new immediate acquisition must not bypass either queued handle.
        assert!(unbounded.try_acquire().is_err());
        let held = bounded_wait.await.unwrap();
        assert_eq!(bounded.stats().queued, 0);
        assert!(poll!(&mut unbounded_wait).is_pending());
        drop(held);
        let held = unbounded_wait.await.unwrap();
        assert_eq!(bounded.stats().in_use, 1);
        assert_eq!(unbounded.stats().queued, 0);
        drop(held);
        assert_eq!(bounded.stats(), LimiterStats::default());
    }

    fn priority_limiter(capacity: u32) -> PriorityLimiter {
        PriorityLimiter::new(
            NonZeroU32::new(capacity).unwrap(),
            TIMEOUT,
            TIMEOUT,
            None,
            None,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn priority_fifo_and_handoff_accounting() {
        let limiter = priority_limiter(1);
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut normal1 = Box::pin(limiter.acquire(Priority::Normal));
        let mut normal2 = Box::pin(limiter.acquire(Priority::Normal));
        let mut high1 = Box::pin(limiter.acquire(Priority::High));
        let mut high2 = Box::pin(limiter.acquire(Priority::High));
        assert!(poll!(&mut normal1).is_pending());
        assert!(poll!(&mut normal2).is_pending());
        assert!(poll!(&mut high1).is_pending());
        assert!(poll!(&mut high2).is_pending());
        drop(held);
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use: 1,
                waiting_normal: 2,
                waiting_high: 2
            }
        );
        let clone = limiter.clone();
        let mut newcomer = Box::pin(clone.acquire(Priority::Normal));
        assert!(poll!(&mut newcomer).is_pending());
        // One normal waiter entered the semaphore queue before high-priority work arrived.
        drop(normal1.await.unwrap());
        // Other normal waiters must wait for all high-priority acquisitions to finish.
        assert!(poll!(&mut normal2).is_pending());
        assert!(poll!(&mut high2).is_pending());
        drop(high1.await.unwrap());
        assert!(poll!(&mut normal2).is_pending());
        drop(high2.await.unwrap());
        // A free permit must not let a new normal waiter bypass the lane's older waiters.
        let mut latecomer = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut latecomer).is_pending());
        drop(normal2.await.unwrap());
        drop(newcomer.await.unwrap());
        drop(latecomer.await.unwrap());
        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_leaves_queue() {
        for priority in [Priority::Normal, Priority::High] {
            let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, TIMEOUT, Some(1), Some(1));
            let held = limiter.acquire(Priority::Normal).await.unwrap();
            let mut cancelled = Box::pin(limiter.acquire(priority));
            assert!(poll!(&mut cancelled).is_pending());
            assert_eq!(
                limiter.acquire(priority).await.unwrap_err().kind(),
                ErrorKind::AtCapacity
            );
            let other_priority = match priority {
                Priority::Normal => Priority::High,
                Priority::High => Priority::Normal,
            };
            // Filling one queue must not prevent admission to the other queue.
            let mut other = Box::pin(limiter.acquire(other_priority));
            assert!(poll!(&mut other).is_pending());
            drop(cancelled);
            let mut next = Box::pin(limiter.acquire(priority));
            assert!(poll!(&mut next).is_pending());
            drop(other);
            drop(held);
            drop(next.await.unwrap());
            assert_eq!(limiter.stats(), PriorityLimiterStats::default());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_after_grant_passes_slot_on() {
        let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, TIMEOUT, Some(2), Some(1));
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut cancelled = Box::pin(limiter.acquire(Priority::Normal));
        let mut next = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut cancelled).is_pending());
        assert!(poll!(&mut next).is_pending());

        // The semaphore assigns a slot to `cancelled`, which is dropped before receiving it.
        drop(held);
        drop(cancelled);
        drop(next.await.unwrap());
        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
        // Both normal queue slots must be available after cancellation and completion.
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut first = Box::pin(limiter.acquire(Priority::Normal));
        let mut second = Box::pin(limiter.acquire(Priority::Normal));
        let mut high = Box::pin(limiter.acquire(Priority::High));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(poll!(&mut high).is_pending());
        drop((first, second, high, held));
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_remove_waiters() {
        for timeout in [Duration::ZERO, TIMEOUT] {
            for priority in [Priority::Normal, Priority::High] {
                let queue_limit = if timeout.is_zero() { None } else { Some(0) };
                let (normal_timeout, high_timeout, normal_queue, high_queue) = match priority {
                    Priority::Normal => (timeout, TIMEOUT, queue_limit, Some(1)),
                    Priority::High => (TIMEOUT, timeout, Some(1), queue_limit),
                };
                let immediate = PriorityLimiter::new(
                    NonZeroU32::MIN,
                    normal_timeout,
                    high_timeout,
                    normal_queue,
                    high_queue,
                );
                let held = immediate.acquire(priority).await.unwrap();
                let before = tokio::time::Instant::now();
                assert_eq!(
                    immediate.acquire(priority).await.unwrap_err().kind(),
                    ErrorKind::AtCapacity
                );
                assert_eq!(tokio::time::Instant::now(), before);
                let other_priority = match priority {
                    Priority::Normal => Priority::High,
                    Priority::High => Priority::Normal,
                };
                let mut other = Box::pin(immediate.acquire(other_priority));
                assert!(poll!(&mut other).is_pending());
                drop(other);
                drop(held);
            }
        }

        for priority in [Priority::Normal, Priority::High] {
            let (normal_timeout, high_timeout, other_priority) = match priority {
                Priority::Normal => (TIMEOUT / 2, TIMEOUT, Priority::High),
                Priority::High => (TIMEOUT, TIMEOUT / 2, Priority::Normal),
            };
            let limiter = PriorityLimiter::new(
                NonZeroU32::MIN,
                normal_timeout,
                high_timeout,
                Some(1),
                Some(1),
            );
            let held = limiter.acquire(Priority::Normal).await.unwrap();
            let mut waiting = Box::pin(limiter.acquire(priority));
            let mut other = Box::pin(limiter.acquire(other_priority));
            assert!(poll!(&mut waiting).is_pending());
            assert!(poll!(&mut other).is_pending());
            tokio::time::advance(TIMEOUT / 2).await;
            assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);
            // The other priority's longer timeout has not elapsed.
            assert!(poll!(&mut other).is_pending());
            assert_eq!(limiter.stats().in_use, 1);
            drop(other);
            let mut retry = Box::pin(limiter.acquire(priority));
            assert!(poll!(&mut retry).is_pending());
            drop(held);
            drop(retry.await.unwrap());
            assert_eq!(limiter.stats(), PriorityLimiterStats::default());
        }
    }

    /// Races releases against timeouts and cancellations on several threads, including
    /// cancellation after the semaphore assigns a permit. Not guaranteed to hit every race on each
    /// run, but checks that no slot is ever lost or returned twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_releases_timeouts_and_cancellations_keep_accounting() {
        const CAPACITY: u32 = 4;
        let limiter = PriorityLimiter::new(
            NonZeroU32::new(CAPACITY).unwrap(),
            Duration::from_millis(2),
            Duration::from_millis(2),
            Some(CAPACITY),
            Some(CAPACITY),
        );

        let tasks: Vec<_> = (0..200)
            .map(|task| {
                let limiter = limiter.clone();
                tokio::spawn(async move {
                    for round in 0..20 {
                        let priority = if (task + round) % 2 == 0 {
                            Priority::Normal
                        } else {
                            Priority::High
                        };
                        let acquire = limiter.acquire(priority);
                        if (task + round) % 3 == 0 {
                            // Cancel from outside, possibly right as a slot is handed over.
                            let _ = tokio::time::timeout(Duration::from_millis(1), acquire).await;
                        } else if let Ok(permit) = acquire.await {
                            // Hold long enough for waiters to queue up and time out.
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            drop(permit);
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
        let mut permits = Vec::new();
        for _ in 0..CAPACITY {
            permits.push(limiter.acquire(Priority::Normal).await.unwrap());
        }
    }
}
