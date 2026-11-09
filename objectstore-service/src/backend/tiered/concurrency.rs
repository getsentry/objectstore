//! Concurrency limiters with FIFO queuing and configurable wait timeouts.
//!
//! [`Limiter`] admits requests in FIFO order. [`PriorityLimiter`] gives pending
//! [`Priority::High`] acquisitions priority over [`Priority::Normal`] acquisitions,
//! with FIFO order within each priority. A zero timeout accepts or rejects immediately.

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Priority {
    Normal,
    High,
}

/// Approximate snapshot of permits and outstanding acquisitions by priority.
///
/// Fields are read independently. Waiting counts include acquisitions with assigned
/// permits that have not resumed yet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PriorityLimiterStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
    /// Outstanding normal-priority acquisitions, including those waiting for the lane lock.
    pub(super) waiting_normal: usize,
    /// Outstanding high-priority acquisitions.
    pub(super) waiting_high: usize,
}

/// Snapshot of permits in use by a FIFO limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LimiterStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
}

/// Passes a usage snapshot to `emit` immediately and once per second thereafter.
///
/// The caller supplies the snapshot and can spawn this future as a background task.
pub(super) async fn run_emitter<S, F, Fut>(mut snapshot: impl FnMut() -> S, mut emit: F)
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

/// Limits concurrent operations with high-priority admission and FIFO within each priority.
///
/// The queue is unbounded. The caller must limit incoming requests.
/// Sustained high-priority traffic can starve normal-priority waiters.
#[derive(Clone, Debug)]
pub(super) struct PriorityLimiter {
    timeout: Duration,
    state: Arc<PriorityState>,
}

#[derive(Debug)]
struct PriorityState {
    capacity: u32,
    semaphore: Arc<Semaphore>,
    normal_lane: Mutex<()>,
    waiting_normal: AtomicUsize,
    waiting_high: AtomicUsize,
    high_drained: Notify,
}

impl PriorityLimiter {
    /// Creates a priority limiter with the given capacity and wait timeout.
    pub(super) fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        Self {
            timeout,
            state: Arc::new(PriorityState {
                capacity: capacity.get(),
                semaphore: Arc::new(Semaphore::new(capacity.get() as usize)),
                normal_lane: Mutex::new(()),
                waiting_normal: AtomicUsize::new(0),
                waiting_high: AtomicUsize::new(0),
                high_drained: Notify::new(),
            }),
        }
    }

    /// Acquires capacity with the given priority within the configured timeout.
    ///
    /// Returns [`ErrorKind::AtCapacity`] on timeout. Dropping the future removes its waiter
    /// and returns any assigned permit. The timeout includes the normal-priority lane lock.
    pub(super) async fn acquire(&self, priority: Priority) -> Result<Permit> {
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

        // A zero timeout polls once, including the lane lock, without waiting or using a timer.
        let permit = if self.timeout.is_zero() {
            acquire.now_or_never().ok_or(ErrorKind::AtCapacity)?
        } else {
            // One deadline covers the lane lock, priority drain, and all semaphore attempts.
            tokio::time::timeout(self.timeout, acquire)
                .await
                .map_err(|_| ErrorKind::AtCapacity)?
        };
        let permit = permit.map_err(|_| ErrorKind::AtCapacity)?;
        Ok(Permit { _permit: permit })
    }

    /// Returns an approximate snapshot of permits and outstanding acquisitions.
    pub(super) fn stats(&self) -> PriorityLimiterStats {
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

/// Limits concurrent operations with FIFO queuing and a configurable wait timeout.
#[derive(Clone, Debug)]
pub(super) struct Limiter {
    semaphore: Arc<Semaphore>,
    capacity: u32,
    timeout: Duration,
}

impl Limiter {
    /// Creates a FIFO limiter with the given capacity and wait timeout.
    pub(super) fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        let capacity = capacity.get();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
            timeout,
        }
    }

    /// Acquires capacity within the configured timeout, preserving FIFO order.
    ///
    /// Returns [`ErrorKind::AtCapacity`] on timeout. A zero timeout rejects immediately when full.
    pub(super) async fn acquire(&self) -> Result<Permit> {
        let permit = if self.timeout.is_zero() {
            self.semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| ErrorKind::AtCapacity)?
        } else {
            tokio::time::timeout(self.timeout, self.semaphore.clone().acquire_owned())
                .await
                .map_err(|_| ErrorKind::AtCapacity)?
                .map_err(|_| ErrorKind::AtCapacity)?
        };
        Ok(Permit { _permit: permit })
    }

    /// Returns the current number of permits in use.
    pub(super) fn stats(&self) -> LimiterStats {
        LimiterStats {
            in_use: self.capacity - self.semaphore.available_permits() as u32,
        }
    }
}

/// Owned permit, released on drop or by a guarded stream.
#[derive(Debug)]
pub(super) struct Permit {
    _permit: OwnedSemaphorePermit,
}

impl Permit {
    /// Wraps `stream` so that it holds this permit until EOF, the first error, or stream drop.
    pub(super) fn guard_stream(self, stream: PayloadStream) -> PayloadStream {
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
        let priority = priority_limiter(1);
        let simple = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
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

    fn priority_limiter(capacity: u32) -> PriorityLimiter {
        PriorityLimiter::new(NonZeroU32::new(capacity).unwrap(), TIMEOUT)
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
        let limiter = priority_limiter(1);
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut cancelled = Box::pin(limiter.acquire(Priority::Normal));
        let mut next = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut cancelled).is_pending());
        assert!(poll!(&mut next).is_pending());

        drop(cancelled);
        assert_eq!(limiter.stats().waiting_normal, 1);
        drop(held);
        drop(next.await.unwrap());
        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_after_grant_passes_slot_on() {
        let limiter = priority_limiter(1);
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
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_remove_waiters() {
        let immediate = PriorityLimiter::new(NonZeroU32::MIN, Duration::ZERO);
        let held = immediate.acquire(Priority::Normal).await.unwrap();
        assert_eq!(
            immediate
                .acquire(Priority::Normal)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::AtCapacity
        );
        drop(held);

        let limiter = priority_limiter(1);
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut waiting = Box::pin(limiter.acquire(Priority::Normal));
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
        drop(held);
        assert_eq!(limiter.stats(), PriorityLimiterStats::default());
    }

    /// Races releases against timeouts and cancellations on several threads, including
    /// cancellation after the semaphore assigns a permit. Not guaranteed to hit every race on each
    /// run, but checks that no slot is ever lost or returned twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_releases_timeouts_and_cancellations_keep_accounting() {
        const CAPACITY: u32 = 4;
        let limiter =
            PriorityLimiter::new(NonZeroU32::new(CAPACITY).unwrap(), Duration::from_millis(2));

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

    #[tokio::test]
    async fn simple_limiter_rejects_until_release() {
        let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
        let permit = limiter.acquire().await.unwrap();
        assert_eq!(
            limiter.clone().acquire().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(limiter.stats().in_use, 1);
        drop(permit);
        assert_eq!(limiter.stats().in_use, 0);
        drop(limiter.acquire().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn stream_retains_then_releases_at_terminal_item() {
        for error in [false, true] {
            let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
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
        let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
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
}
