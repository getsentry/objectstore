//! Simple and priority-based FIFO concurrency limiters.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::mem::DropGuard;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::error::{ErrorKind, Result};
use crate::stream::PayloadStream;

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Owned permit, released on drop.
#[derive(Debug)]
pub struct Permit {
    _inner: PermitInner,
}

#[derive(Debug)]
enum PermitInner {
    Semaphore { _permit: OwnedSemaphorePermit },
    Priority { _permit: PriorityPermit },
}

impl Permit {
    fn semaphore(permit: OwnedSemaphorePermit) -> Self {
        Self {
            _inner: PermitInner::Semaphore { _permit: permit },
        }
    }

    fn priority(state: Arc<Mutex<PriorityState>>) -> Self {
        Self {
            _inner: PermitInner::Priority {
                _permit: PriorityPermit { state },
            },
        }
    }

    /// Wraps `stream` so that it holds this permit until EOF, the first error, or stream drop.
    ///
    /// Returns an unboxed stream; call `.boxed()` if a [`PayloadStream`] is needed.
    pub fn guard_stream(self, stream: PayloadStream) -> impl Stream<Item = io::Result<Bytes>> {
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
    }
}

/// Snapshot of permits in use by a simple limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LimiterStats {
    /// Permits currently in use.
    pub in_use: u32,
    /// Most permits in use at once since the previous call to [`Limiter::stats`]. Unlike
    /// `in_use`, this captures bursts between two calls.
    pub in_use_peak: u32,
}

/// Most permits in use at once since the last [`take`](Self::take).
#[derive(Debug, Default)]
pub(crate) struct Peak(AtomicU32);

impl Peak {
    /// Raises the peak to `in_use` if it is higher.
    pub(crate) fn record(&self, in_use: u32) {
        self.0.fetch_max(in_use, Ordering::Relaxed);
    }

    /// Returns the peak and restarts it at `in_use`.
    ///
    /// Restarting at the current number instead of zero keeps permits visible that stay held while
    /// nothing new is acquired.
    pub(crate) fn take(&self, in_use: u32) -> u32 {
        self.0.swap(in_use, Ordering::Relaxed).max(in_use)
    }
}

/// Shared interval loop for the metric emitters.
async fn run_emitter<S, F, Fut>(mut snapshot: impl FnMut() -> S, mut emit: F) -> !
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
    in_use_peak: Arc<Peak>,
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
            in_use_peak: Arc::default(),
            capacity,
            timeout,
        }
    }

    /// Acquires capacity within the configured timeout, preserving FIFO order.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when the queue is full or the wait times out.
    /// Dropping the future releases its queue slot and any assigned permit.
    pub async fn acquire(&self) -> Result<Permit> {
        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            self.in_use_peak.record(self.in_use());
            return Ok(Permit::semaphore(permit));
        }
        // No permit was free, so the limiter is full. Recording that here makes every wait and
        // rejection visible, whatever is released right after.
        self.in_use_peak.record(self.capacity);
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
        Ok(Permit::semaphore(permit))
    }

    /// Returns the current and peak number of permits in use.
    ///
    /// Each call restarts the peak. [`run_emitter`](Self::run_emitter) calls this, so another
    /// caller takes away from the peak that the emitter reports.
    pub fn stats(&self) -> LimiterStats {
        let in_use = self.in_use();
        LimiterStats {
            in_use,
            in_use_peak: self.in_use_peak.take(in_use),
        }
    }

    fn in_use(&self) -> u32 {
        self.capacity - self.semaphore.available_permits() as u32
    }

    /// Passes this limiter's stats to `emit` immediately and once per second thereafter.
    pub async fn run_emitter<F, Fut>(&self, emit: F) -> !
    where
        F: FnMut(LimiterStats) -> Fut,
        Fut: Future<Output = ()>,
    {
        run_emitter(|| self.stats(), emit).await
    }
}

/// Priority for an acquisition from [`PriorityLimiter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    /// Normal priority.
    Normal,
    /// High priority, always served before queued normal-priority acquisitions.
    High,
}

/// Snapshot of permits in use and queued acquisitions by priority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PriorityLimiterStats {
    /// Permits currently in use.
    pub in_use: u32,
    /// Most permits in use at once since the previous call to [`PriorityLimiter::stats`].
    /// Unlike `in_use`, this captures bursts between two calls.
    pub in_use_peak: u32,
    /// Normal-priority acquisitions waiting in the queue.
    pub waiting_normal: usize,
    /// High-priority acquisitions waiting in the queue.
    pub waiting_high: usize,
}

/// Limits concurrent operations with high-priority admission and FIFO within each priority.
///
/// Each priority has its own wait timeout and optional queue limit; execution capacity is shared.
/// A released permit goes directly to the oldest high-priority waiter, or else to the oldest
/// normal-priority waiter, so sustained high-priority traffic can starve normal-priority waiters.
#[derive(Clone, Debug)]
pub struct PriorityLimiter {
    normal_timeout: Duration,
    high_timeout: Option<Duration>,
    max_normal_queue_size: Option<u32>,
    max_high_queue_size: Option<u32>,
    capacity: u32,
    state: Arc<Mutex<PriorityState>>,
}

impl PriorityLimiter {
    /// Creates a priority limiter with shared capacity and separate timeouts and queue limits.
    /// Pass `None` as the high-priority timeout to wait without a deadline.
    pub fn new(
        capacity: NonZeroU32,
        normal_timeout: Duration,
        high_timeout: impl Into<Option<Duration>>,
        max_normal_queue_size: Option<u32>,
        max_high_queue_size: Option<u32>,
    ) -> Self {
        Self {
            normal_timeout,
            high_timeout: high_timeout.into(),
            max_normal_queue_size,
            max_high_queue_size,
            capacity: capacity.get(),
            state: Arc::new(Mutex::new(PriorityState {
                free: capacity.get(),
                in_use_peak: Peak::default(),
                next_id: 0,
                normal_queue: WaiterQueue::new(),
                high_queue: WaiterQueue::new(),
            })),
        }
    }

    /// Acquires capacity within the timeout configured for the given priority.
    ///
    /// Returns [`ErrorKind::AtCapacity`] when this priority's queue is full or the wait times out.
    /// Dropping the future releases its queue slot and any assigned permit.
    pub async fn acquire(&self, priority: Priority) -> Result<Permit> {
        let (timeout, max_queue_size) = match priority {
            Priority::Normal => (Some(self.normal_timeout), self.max_normal_queue_size),
            Priority::High => (self.high_timeout, self.max_high_queue_size),
        };

        let (_entry, receiver) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            // A free permit implies that nobody is queued, so taking it overtakes no one.
            if state.free > 0 {
                state.free -= 1;
                state.in_use_peak.record(self.capacity - state.free);
                return Ok(Permit::priority(self.state.clone()));
            }
            if timeout == Some(Duration::ZERO) {
                return Err(ErrorKind::AtCapacity.into());
            }
            let id = state.next_id;
            let queue = state.queue(priority);
            if max_queue_size.is_some_and(|max| queue.len() >= max as usize) {
                return Err(ErrorKind::AtCapacity.into());
            }
            let (sender, receiver) = oneshot::channel();
            queue.insert(id, sender);
            state.next_id += 1;

            // Leaves the queue on timeout or cancellation. A no-op once a permit was handed over.
            let entry = DropGuard::new(id, move |id| {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.queue(priority).remove(&id);
            });
            (entry, receiver)
        };

        // A permit sent after a timeout or cancellation is dropped with the receiver, which passes
        // its slot on to the next waiter.
        let received = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, receiver)
                .await
                .map_err(|_| ErrorKind::AtCapacity)?,
            None => receiver.await,
        };
        received.map_err(|_| ErrorKind::AtCapacity.into())
    }

    /// Returns a snapshot of permits in use and queued acquisitions.
    ///
    /// Each call restarts the peak. [`run_emitter`](Self::run_emitter) calls this, so another
    /// caller takes away from the peak that the emitter reports.
    pub fn stats(&self) -> PriorityLimiterStats {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let in_use = self.capacity - state.free;
        PriorityLimiterStats {
            in_use,
            in_use_peak: state.in_use_peak.take(in_use),
            waiting_normal: state.normal_queue.len(),
            waiting_high: state.high_queue.len(),
        }
    }

    /// Passes this limiter's stats to `emit` immediately and once per second thereafter.
    pub async fn run_emitter<F, Fut>(&self, emit: F) -> !
    where
        F: FnMut(PriorityLimiterStats) -> Fut,
        Fut: Future<Output = ()>,
    {
        run_emitter(|| self.stats(), emit).await
    }
}

/// Waiters keyed by arrival order, each holding the channel its permit is delivered on.
type WaiterQueue = BTreeMap<u64, oneshot::Sender<Permit>>;

// A permit is either free or owned by exactly one `Permit`. On release, `PriorityPermit` hands
// its slot directly to the next waiter instead of returning it to `free`, so a waiter never has
// to race newcomers for it and every waiter can be admitted without waiting for another one.
#[derive(Debug)]
struct PriorityState {
    free: u32,
    /// Most permits in use at once since the emitter last took it. Handing a slot to a waiter
    /// keeps it in use, so only taking a free permit can raise this.
    in_use_peak: Peak,
    next_id: u64,
    normal_queue: WaiterQueue,
    high_queue: WaiterQueue,
}

impl PriorityState {
    fn queue(&mut self, priority: Priority) -> &mut WaiterQueue {
        match priority {
            Priority::Normal => &mut self.normal_queue,
            Priority::High => &mut self.high_queue,
        }
    }

    fn pop_next_waiter(&mut self) -> Option<oneshot::Sender<Permit>> {
        let (_, sender) = self
            .high_queue
            .pop_first()
            .or_else(|| self.normal_queue.pop_first())?;
        Some(sender)
    }
}

/// One slot of a [`PriorityLimiter`], handed to the next waiter or freed on drop.
#[derive(Debug)]
struct PriorityPermit {
    state: Arc<Mutex<PriorityState>>,
}

impl Drop for PriorityPermit {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(sender) = state.pop_next_waiter() else {
            state.free += 1;
            return;
        };
        // Unlock before sending: if the waiter is gone, `send` returns the permit, and dropping
        // it re-enters this function.
        drop(state);
        let _ = sender.send(Permit::priority(self.state.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, poll};
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
        // Both permits were still in use when the previous emission restarted the peak.
        assert_eq!(
            priority_receiver.try_recv().unwrap(),
            PriorityLimiterStats {
                in_use_peak: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            simple_receiver.try_recv().unwrap(),
            LimiterStats {
                in_use: 0,
                in_use_peak: 1
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_peak_usage_between_emissions() {
        const CAPACITY: u32 = 3;
        let capacity = NonZeroU32::new(CAPACITY).unwrap();
        let priority = PriorityLimiter::new(capacity, TIMEOUT, TIMEOUT, None, None);
        let simple = Limiter::new(capacity, Duration::ZERO, None);
        let (priority_sender, mut priority_peaks) = tokio::sync::mpsc::unbounded_channel();
        let (simple_sender, mut simple_peaks) = tokio::sync::mpsc::unbounded_channel();
        let priority_emitter = priority.run_emitter(|stats| {
            priority_sender
                .send((stats.in_use, stats.in_use_peak))
                .unwrap();
            std::future::ready(())
        });
        let simple_emitter = simple.run_emitter(|stats| {
            simple_sender
                .send((stats.in_use, stats.in_use_peak))
                .unwrap();
            std::future::ready(())
        });
        tokio::pin!(priority_emitter, simple_emitter);
        let mut emit = async |advance: Duration| {
            tokio::time::advance(advance).await;
            assert!(poll!(&mut priority_emitter).is_pending());
            assert!(poll!(&mut simple_emitter).is_pending());
            let priority = priority_peaks.try_recv().unwrap();
            assert_eq!(priority, simple_peaks.try_recv().unwrap());
            priority
        };
        assert_eq!(emit(Duration::ZERO).await, (0, 0));

        // A burst that is over before the next emission is still reported.
        let held = (
            priority.acquire(Priority::Normal).await.unwrap(),
            simple.acquire().await.unwrap(),
        );
        let burst = (
            priority.acquire(Priority::Normal).await.unwrap(),
            simple.acquire().await.unwrap(),
        );
        drop(burst);
        assert_eq!(emit(METRICS_EMITTER_INTERVAL).await, (1, 2));

        // Permits held throughout count even though nothing was acquired.
        assert_eq!(emit(METRICS_EMITTER_INTERVAL).await, (1, 1));

        // Permits released since the previous emission were in use at its start.
        drop(held);
        assert_eq!(emit(METRICS_EMITTER_INTERVAL).await, (0, 1));
        assert_eq!(emit(METRICS_EMITTER_INTERVAL).await, (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn rejection_marks_the_limiter_as_full() {
        let limiter = Limiter::new(NonZeroU32::new(2).unwrap(), Duration::ZERO, None);
        let held = (
            limiter.acquire().await.unwrap(),
            limiter.acquire().await.unwrap(),
        );
        // Lower the peak so that only the failed acquisition can raise it back to capacity.
        assert_eq!(limiter.in_use_peak.take(1), 2);
        limiter.acquire().await.unwrap_err();
        drop(held);
        assert_eq!(limiter.stats().in_use_peak, 2);
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
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use: 1,
                in_use_peak: 1,
                waiting_normal: 2,
                waiting_high: 2
            }
        );
        // The slot goes straight to the oldest high-priority waiter.
        drop(held);
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use: 1,
                in_use_peak: 1,
                waiting_normal: 2,
                waiting_high: 1
            }
        );
        // A newcomer cannot take the slot assigned to a waiter that has not woken up yet.
        let clone = limiter.clone();
        let mut newcomer = Box::pin(clone.acquire(Priority::Normal));
        assert!(poll!(&mut newcomer).is_pending());
        // Normal waiters must wait for all high-priority acquisitions to finish.
        assert!(poll!(&mut normal1).is_pending());
        assert!(poll!(&mut high2).is_pending());
        drop(high1.await.unwrap());
        assert!(poll!(&mut normal1).is_pending());
        drop(high2.await.unwrap());
        // Normal waiters are then served in arrival order.
        assert!(poll!(&mut normal2).is_pending());
        assert!(poll!(&mut newcomer).is_pending());
        drop(normal1.await.unwrap());
        assert!(poll!(&mut newcomer).is_pending());
        drop(normal2.await.unwrap());
        drop(newcomer.await.unwrap());
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use_peak: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn released_slots_admit_all_waiters_at_once() {
        let limiter = priority_limiter(3);
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(limiter.acquire(Priority::Normal).await.unwrap());
        }
        let mut waiters: Vec<_> = (0..3)
            .map(|_| Box::pin(limiter.acquire(Priority::Normal)))
            .collect();
        for waiter in &mut waiters {
            assert!(poll!(waiter).is_pending());
        }
        drop(held);
        // Every waiter owns a slot before any of them is polled again.
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use: 3,
                in_use_peak: 3,
                waiting_normal: 0,
                waiting_high: 0
            }
        );
        for waiter in &mut waiters {
            assert!(poll!(waiter).is_ready());
        }
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use_peak: 3,
                ..Default::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn high_priority_can_wait_without_a_deadline_or_queue_limit() {
        let limiter = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT, None, Some(1), None);
        let held = limiter.acquire(Priority::Normal).await.unwrap();
        let mut high1 = Box::pin(limiter.acquire(Priority::High));
        let mut high2 = Box::pin(limiter.acquire(Priority::High));
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut high1).is_pending());
        assert!(poll!(&mut high2).is_pending());
        assert!(poll!(&mut normal).is_pending());
        assert_eq!(
            limiter.acquire(Priority::Normal).await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );

        tokio::time::advance(TIMEOUT * 2).await;
        assert_eq!(normal.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert!(poll!(&mut high1).is_pending());
        assert!(poll!(&mut high2).is_pending());
        drop(held);
        drop(high1.await.unwrap());
        drop(high2.await.unwrap());
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
            assert_eq!(
                limiter.stats(),
                PriorityLimiterStats {
                    in_use_peak: 1,
                    ..Default::default()
                }
            );
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

        // The slot is handed to `cancelled`, which is dropped before receiving it.
        drop(held);
        drop(cancelled);
        drop(next.await.unwrap());
        assert_eq!(
            limiter.stats(),
            PriorityLimiterStats {
                in_use_peak: 1,
                ..Default::default()
            }
        );
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
            assert_eq!(
                limiter.stats(),
                PriorityLimiterStats {
                    in_use_peak: 1,
                    ..Default::default()
                }
            );
        }
    }

    /// Races releases against timeouts and cancellations on several threads, including
    /// cancellation after a permit is handed over. Not guaranteed to hit every race on each
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

        let stats = limiter.stats();
        assert!(stats.in_use_peak <= CAPACITY);
        assert_eq!(
            stats,
            PriorityLimiterStats {
                in_use_peak: stats.in_use_peak,
                ..Default::default()
            }
        );
        let mut permits = Vec::new();
        for _ in 0..CAPACITY {
            permits.push(limiter.acquire(Priority::Normal).await.unwrap());
        }
    }
}
