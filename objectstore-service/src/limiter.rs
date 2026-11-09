//! Simple and priority-based FIFO concurrency limiters.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt, stream};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{ErrorKind, Result};
use crate::stream::PayloadStream;

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Passes a usage snapshot to `emit` immediately and once per second thereafter.
///
/// The caller supplies the snapshot and can spawn this future as a background task.
pub async fn run_emitter<S, F, Fut>(mut snapshot: impl FnMut() -> S, mut emit: F)
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

/// Owned permit, released on drop or by a guarded stream.
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

/// Snapshot of permits in use by a simple limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LimiterStats {
    /// Permits held by callers or assigned to acquisitions that have not resumed yet.
    pub in_use: u32,
}

/// Limits concurrent operations with FIFO queuing and a configurable wait timeout.
#[derive(Clone, Debug)]
pub struct Limiter {
    semaphore: Arc<Semaphore>,
    queue: Option<Arc<Semaphore>>,
    capacity: u32,
    timeout: Duration,
}

impl Limiter {
    /// Creates a FIFO limiter with the given capacity, wait timeout, and queue limit.
    ///
    /// `None` allows unlimited waiting. `Some(n)` allows at most `n` queued acquisitions.
    /// `Some(0)` disables waiting. Immediate acquisitions do not consume queue capacity.
    pub fn new(capacity: NonZeroU32, timeout: Duration, max_queue_size: Option<u32>) -> Self {
        let capacity = capacity.get();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            queue: max_queue_size.map(|size| Arc::new(Semaphore::new(size as usize))),
            capacity,
            timeout,
        }
    }

    /// Acquires capacity within the configured timeout, preserving FIFO order.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when the queue is full or the wait times out.
    /// A zero timeout rejects immediately when no permit is available.
    /// Dropping the future releases its queue slot and any assigned permit.
    pub async fn acquire(&self) -> Result<Permit> {
        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            return Ok(Permit { _permit: permit });
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
        let permit = tokio::time::timeout(self.timeout, self.semaphore.clone().acquire_owned())
            .await
            .map_err(|_| ErrorKind::AtCapacity)?
            .map_err(|_| ErrorKind::AtCapacity)?;
        Ok(Permit { _permit: permit })
    }

    /// Returns the current number of permits in use.
    pub fn stats(&self) -> LimiterStats {
        LimiterStats {
            in_use: self.capacity - self.semaphore.available_permits() as u32,
        }
    }
}

/// Priority for an acquisition from [`PriorityLimiter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    /// Waits until no high-priority acquisitions are pending.
    Normal,
    /// Takes precedence over pending normal-priority acquisitions.
    High,
}

/// Approximate snapshot of permits and outstanding acquisitions by priority.
///
/// Fields are read independently. The waiting counts include unfinished acquisition calls,
/// including permits assigned to tasks that have not resumed yet. Immediate acquisitions and
/// rejected attempts are also counted briefly, so these values can exceed the queue limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PriorityLimiterStats {
    /// Permits held by callers or assigned to acquisitions that have not resumed yet.
    pub in_use: u32,
    /// Outstanding normal-priority acquisitions, including those waiting for the lane lock.
    pub waiting_normal: usize,
    /// Outstanding high-priority acquisitions.
    pub waiting_high: usize,
}

/// Limits concurrent operations with high-priority admission and FIFO within each priority.
///
/// Each priority has its own optional queue limit; execution capacity is shared.
/// Sustained high-priority traffic can starve normal-priority waiters.
#[derive(Clone, Debug)]
pub struct PriorityLimiter {
    timeout: Duration,
    state: Arc<PriorityState>,
}

#[derive(Debug)]
struct PriorityState {
    capacity: u32,
    semaphore: Arc<Semaphore>,
    normal_queue: Option<Arc<Semaphore>>,
    high_queue: Option<Arc<Semaphore>>,
    normal_lane: Mutex<()>,
    waiting_normal: AtomicUsize,
    waiting_high: AtomicUsize,
    high_drained: Notify,
}

impl PriorityLimiter {
    /// Creates a priority limiter with shared capacity and separate queue limits.
    ///
    /// `None` allows unlimited waiting. `Some(n)` allows at most `n` queued acquisitions.
    /// `Some(0)` disables waiting. Immediate acquisitions do not consume queue capacity.
    /// Each queue limit applies only to acquisitions with that priority.
    pub fn new(
        capacity: NonZeroU32,
        timeout: Duration,
        max_normal_queue_size: Option<u32>,
        max_high_queue_size: Option<u32>,
    ) -> Self {
        Self {
            timeout,
            state: Arc::new(PriorityState {
                capacity: capacity.get(),
                semaphore: Arc::new(Semaphore::new(capacity.get() as usize)),
                normal_queue: max_normal_queue_size
                    .map(|size| Arc::new(Semaphore::new(size as usize))),
                high_queue: max_high_queue_size.map(|size| Arc::new(Semaphore::new(size as usize))),
                normal_lane: Mutex::new(()),
                waiting_normal: AtomicUsize::new(0),
                waiting_high: AtomicUsize::new(0),
                high_drained: Notify::new(),
            }),
        }
    }

    /// Acquires capacity with the given priority within the configured timeout.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when this priority's queue is full or the wait times out.
    /// A zero timeout rejects immediately if acquisition cannot finish without waiting.
    /// Dropping the future releases its queue slot and any assigned permit.
    /// The timeout includes the normal-priority lane lock.
    pub async fn acquire(&self, priority: Priority) -> Result<Permit> {
        let state = &self.state;
        state.waiting(priority).fetch_add(1, Ordering::AcqRel);
        let _waiting = WaitingGuard { state, priority };
        let acquire = async {
            if priority == Priority::High {
                return state.semaphore.clone().acquire_owned().await;
            }

            // Only one normal-priority waiter can enter the semaphore queue at a time.
            let _lane = state.normal_lane.lock().await;
            loop {
                state.wait_until_high_drained().await;
                let permit = state.semaphore.clone().acquire_owned().await?;
                if state.waiting_high.load(Ordering::Acquire) == 0 {
                    return Ok(permit);
                }
                // High-priority work registered while this task was queued. Return the permit
                // and wait for it to drain, including tasks that have not joined the queue yet.
                drop(permit);
            }
        };

        tokio::pin!(acquire);
        // Immediate acquisition does not consume queue capacity. Keep the same future if it
        // needs to wait, preserving its position in the lane or semaphore queue.
        let permit = match acquire.as_mut().now_or_never() {
            Some(permit) => permit,
            None => {
                if self.timeout.is_zero() {
                    return Err(ErrorKind::AtCapacity.into());
                }
                let queue = match priority {
                    Priority::Normal => &state.normal_queue,
                    Priority::High => &state.high_queue,
                };
                let _queued = queue
                    .as_ref()
                    .map(|queue| queue.try_acquire())
                    .transpose()
                    .map_err(|_| ErrorKind::AtCapacity)?;
                // One deadline covers the lane lock, priority drain, and semaphore attempts.
                tokio::time::timeout(self.timeout, acquire)
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
            // notify_waiters reaches this future from creation, before its first poll.
            let drained = self.high_drained.notified();
            if self.waiting_high.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
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
        let priority = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, None, None);
        let simple = Limiter::new(NonZeroU32::MIN, Duration::ZERO, None);
        let priority_permit = priority.acquire(Priority::Normal).await.unwrap();
        let simple_permit = simple.acquire().await.unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let emitter = run_emitter(
            || (priority.stats(), simple.stats()),
            |stats| {
                sender.send(stats).unwrap();
                std::future::ready(())
            },
        );
        tokio::pin!(emitter);
        assert!(poll!(&mut emitter).is_pending());
        let busy = receiver.try_recv().unwrap();
        assert_eq!(busy.0.in_use, 1);
        assert_eq!(busy.1.in_use, 1);

        drop(priority_permit);
        drop(simple_permit);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut emitter).is_pending());
        let idle = receiver.try_recv().unwrap();
        assert_eq!(
            idle,
            (PriorityLimiterStats::default(), LimiterStats::default())
        );
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
            let before = tokio::time::Instant::now();
            let mut waiting = Box::pin(limiter.acquire());
            if max_queue_size == Some(0) {
                assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);
                assert_eq!(tokio::time::Instant::now(), before);
                drop(held);
                continue;
            }
            assert!(poll!(&mut waiting).is_pending());
            if max_queue_size == Some(1) {
                assert_eq!(
                    limiter.acquire().await.unwrap_err().kind(),
                    ErrorKind::AtCapacity
                );
                assert_eq!(tokio::time::Instant::now(), before);
            }
            // Cancellation must free the queue slot for another waiter.
            drop(waiting);
            let mut timed_out = Box::pin(limiter.acquire());
            assert!(poll!(&mut timed_out).is_pending());
            tokio::time::advance(TIMEOUT).await;
            assert_eq!(timed_out.await.unwrap_err().kind(), ErrorKind::AtCapacity);
            // A timeout must also free the queue slot.
            let mut next = Box::pin(limiter.acquire());
            assert!(poll!(&mut next).is_pending());
            drop(held);
            drop(next.await.unwrap());
            assert_eq!(limiter.stats().in_use, 0);
        }
    }

    fn priority_limiter(capacity: u32) -> PriorityLimiter {
        PriorityLimiter::new(NonZeroU32::new(capacity).unwrap(), TIMEOUT, None, None)
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
        // The normal-priority waiter must return its assigned permit to high-priority work.
        assert!(poll!(&mut normal1).is_pending());
        assert!(poll!(&mut high2).is_pending());
        drop(high1.await.unwrap());
        drop(high2.await.unwrap());
        assert!(poll!(&mut normal2).is_pending());
        drop(normal1.await.unwrap());
        drop(normal2.await.unwrap());
        drop(newcomer.await.unwrap());
        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_leaves_queue() {
        for priority in [Priority::Normal, Priority::High] {
            let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, Some(1), Some(1));
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
        let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, Some(2), Some(1));
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
                let (normal_queue, high_queue) = match priority {
                    Priority::Normal => (Some(0), Some(1)),
                    Priority::High => (Some(1), Some(0)),
                };
                let immediate =
                    PriorityLimiter::new(NonZeroU32::MIN, timeout, normal_queue, high_queue);
                let held = immediate.acquire(priority).await.unwrap();
                let before = tokio::time::Instant::now();
                assert_eq!(
                    immediate.acquire(priority).await.unwrap_err().kind(),
                    ErrorKind::AtCapacity
                );
                assert_eq!(tokio::time::Instant::now(), before);
                if !timeout.is_zero() {
                    let other_priority = match priority {
                        Priority::Normal => Priority::High,
                        Priority::High => Priority::Normal,
                    };
                    let mut other = Box::pin(immediate.acquire(other_priority));
                    assert!(poll!(&mut other).is_pending());
                }
                drop(held);
            }
        }

        for priority in [Priority::Normal, Priority::High] {
            let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, Some(1), Some(1));
            let held = limiter.acquire(Priority::Normal).await.unwrap();
            let mut waiting = Box::pin(limiter.acquire(priority));
            assert!(poll!(&mut waiting).is_pending());
            tokio::time::advance(TIMEOUT).await;
            assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);
            assert_eq!(
                limiter.stats(),
                PriorityLimiterStats {
                    in_use: 1,
                    ..Default::default()
                }
            );
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
