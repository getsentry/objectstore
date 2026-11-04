//! Primitives for concurrency limiting in the Tiered backend.
//!
//! High-volume operations are classified between `Start` (HV operations happening *before* any
//! LT operations in a Tiered request) and `Continue` (HV operations happening *after* some LT operations).
//! Both classes share the same limiter, but `Continue` operations always have priority over
//! `Start` ones, as we don't want to reject requests after performing the (usually) expensive LT calls.
//! Requests queue in FIFO order within each class and wait up to a configured timeout.
//!
//! The long-term limit uses a semaphore that accepts or rejects LT operations directly, without
//! queuing or waiting.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::error::{ErrorKind, Result};
use crate::stream::PayloadStream;

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HighVolumeClass {
    Start,
    Continue,
}

/// Usage snapshot for both tiers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Stats {
    /// High-volume snapshot.
    pub(super) high_volume: HighVolumeStats,
    /// Long-term snapshot.
    pub(super) long_term: LongTermStats,
}

/// Snapshot of high-volume permits and queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct HighVolumeStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
    /// `Start` acquisitions waiting in queue.
    pub(super) waiting_start: usize,
    /// `Continue` acquisitions waiting in queue.
    pub(super) waiting_continue: usize,
}

/// Snapshot of long-term permits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LongTermStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
}

/// A queue of waiters, keyed by arrival order.
type Queue = BTreeMap<u64, oneshot::Sender<HighVolumePermit>>;

#[derive(Debug)]
struct State {
    capacity: u32,
    free: u32,
    next_id: u64,
    start_queue: Queue,
    continue_queue: Queue,
}

impl State {
    fn queue(&mut self, class: HighVolumeClass) -> &mut Queue {
        match class {
            HighVolumeClass::Start => &mut self.start_queue,
            HighVolumeClass::Continue => &mut self.continue_queue,
        }
    }

    fn pop_next_waiter(&mut self) -> Option<oneshot::Sender<HighVolumePermit>> {
        let (_, sender) = self
            .continue_queue
            .pop_first()
            .or_else(|| self.start_queue.pop_first())?;
        Some(sender)
    }
}

/// Concurrency limiter for the Tiered backend.
#[derive(Clone, Debug)]
pub(super) struct ConcurrencyLimiter {
    high_volume: HighVolumeLimiter,
    long_term: LongTermLimiter,
}

impl ConcurrencyLimiter {
    /// Creates the limiter with the given capacities and high-volume wait timeout.
    pub(super) fn new(
        high_volume_capacity: NonZeroU32,
        long_term_capacity: NonZeroU32,
        high_volume_timeout: Duration,
    ) -> Self {
        Self {
            high_volume: HighVolumeLimiter::new(high_volume_capacity, high_volume_timeout),
            long_term: LongTermLimiter::new(long_term_capacity),
        }
    }

    /// Acquires high-volume capacity, waiting at most the configured timeout once queued.
    ///
    /// Returns [`ErrorKind::AtCapacity`] if a permit cannot be acquired within the timeout.
    /// Dropping this future returns an already assigned permit or frees up the corresponding slot
    /// in the queue.
    pub(super) async fn acquire_high_volume(
        &self,
        class: HighVolumeClass,
    ) -> Result<HighVolumePermit> {
        self.high_volume.acquire(class).await
    }

    /// Tries once to acquire long-term capacity, returning [`ErrorKind::AtCapacity`] if full.
    pub(super) fn try_acquire_long_term(&self) -> Result<LongTermPermit> {
        self.long_term.try_acquire()
    }

    fn stats(&self) -> Stats {
        Stats {
            high_volume: self.high_volume.stats(),
            long_term: self.long_term.stats(),
        }
    }

    /// Passes a usage snapshot to `emit` immediately and once per second thereafter.
    ///
    /// The caller can spawn this future as a background task to record metrics.
    pub(super) async fn run_emitter<F, Fut>(&self, mut emit: F)
    where
        F: FnMut(Stats) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ticker = tokio::time::interval(METRICS_EMITTER_INTERVAL);
        loop {
            ticker.tick().await;
            emit(self.stats()).await;
        }
    }
}

/// Shared high-volume capacity with CONTINUE priority and FIFO queuing within each class.
///
/// Note that queues here are unbounded. A separate component is expected to limit the
/// amount of requests that can queue up in the first place.
#[derive(Clone, Debug)]
struct HighVolumeLimiter {
    timeout: Duration,
    state: Arc<Mutex<State>>,
}

impl HighVolumeLimiter {
    fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        Self {
            timeout,
            state: Arc::new(Mutex::new(State {
                capacity: capacity.get(),
                free: capacity.get(),
                next_id: 0,
                start_queue: Queue::new(),
                continue_queue: Queue::new(),
            })),
        }
    }

    async fn acquire(&self, class: HighVolumeClass) -> Result<HighVolumePermit> {
        let shared = &self.state;
        let (_entry, receiver) = {
            let mut state = shared.lock().unwrap_or_else(PoisonError::into_inner);
            if state.free > 0 {
                state.free -= 1;
                return Ok(HighVolumePermit {
                    state: shared.clone(),
                });
            }
            if self.timeout.is_zero() {
                return Err(ErrorKind::AtCapacity.into());
            }
            let id = state.next_id;
            state.next_id = id.wrapping_add(1);
            let (sender, receiver) = oneshot::channel();
            state.queue(class).insert(id, sender);
            let entry = QueueEntryGuard {
                state: shared,
                class,
                id,
            };
            (entry, receiver)
        };
        // A permit sent after this times out is dropped with the receiver, which passes its slot on.
        match tokio::time::timeout(self.timeout, receiver).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(ErrorKind::AtCapacity.into()),
        }
    }

    fn stats(&self) -> HighVolumeStats {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        HighVolumeStats {
            in_use: state.capacity - state.free,
            waiting_start: state.start_queue.len(),
            waiting_continue: state.continue_queue.len(),
        }
    }
}

/// A waiter's place in line.
///
/// Borrows the state because it never outlives the pending `acquire` call that created it.
struct QueueEntryGuard<'a> {
    state: &'a Mutex<State>,
    class: HighVolumeClass,
    id: u64,
}

impl Drop for QueueEntryGuard<'_> {
    fn drop(&mut self) {
        // Leave the queue if we gave up.
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queue(self.class)
            .remove(&self.id);
    }
}

/// One high-volume permit, released on drop.
#[derive(Debug)]
pub(super) struct HighVolumePermit {
    state: Arc<Mutex<State>>,
}

impl Drop for HighVolumePermit {
    // Hands the slot directly to the next waiter if there is one, or return it to the
    // pool otherwise.
    //
    // We cannot simply always return the slot to the pool, as a request that comes in at the right
    // time might observer `free > 0` and overtake a waiter that hasn't woken up yet.
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(sender) = state.pop_next_waiter() else {
            state.free += 1;
            return;
        };
        // Unlock before sending: if the waiter is gone, `send` returns the permit, and dropping it
        // re-enters this function which locks it again.
        drop(state);
        let _ = sender.send(Self {
            state: self.state.clone(),
        });
    }
}

/// Long-term capacity acquired without waiting.
#[derive(Clone, Debug)]
struct LongTermLimiter {
    semaphore: Arc<Semaphore>,
    capacity: u32,
}

impl LongTermLimiter {
    fn new(capacity: NonZeroU32) -> Self {
        let capacity = capacity.get();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    fn try_acquire(&self) -> Result<LongTermPermit> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorKind::AtCapacity)?;
        Ok(LongTermPermit { _permit: permit })
    }

    fn stats(&self) -> LongTermStats {
        LongTermStats {
            in_use: self.capacity - self.semaphore.available_permits() as u32,
        }
    }
}

/// Owned long-term permit, released on drop or by a guarded stream.
#[derive(Debug)]
pub(super) struct LongTermPermit {
    _permit: OwnedSemaphorePermit,
}

impl LongTermPermit {
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
    async fn emitter_reports_current_usage_of_both_tiers() {
        let limiter = hv(1);
        let high = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let long = limiter.try_acquire_long_term().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let emitter = limiter.run_emitter(|stats| {
            sender.send(stats).unwrap();
            std::future::ready(())
        });
        tokio::pin!(emitter);
        assert!(poll!(&mut emitter).is_pending());
        let busy = receiver.try_recv().unwrap();
        assert_eq!(busy.high_volume.in_use, 1);
        assert_eq!(busy.long_term.in_use, 1);

        drop(high);
        drop(long);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut emitter).is_pending());
        let idle = receiver.try_recv().unwrap();
        assert_eq!(idle, Stats::default());
    }

    fn hv(capacity: u32) -> ConcurrencyLimiter {
        ConcurrencyLimiter::new(NonZeroU32::new(capacity).unwrap(), NonZeroU32::MIN, TIMEOUT)
    }

    #[tokio::test(start_paused = true)]
    async fn priority_fifo_and_handoff_accounting() {
        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut s1 = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut s2 = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut c1 = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Continue));
        let mut c2 = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Continue));
        assert!(poll!(&mut s1).is_pending());
        assert!(poll!(&mut s2).is_pending());
        assert!(poll!(&mut c1).is_pending());
        assert!(poll!(&mut c2).is_pending());
        drop(held);
        assert_eq!(
            limiter.stats().high_volume,
            HighVolumeStats {
                in_use: 1,
                waiting_start: 2,
                waiting_continue: 1
            }
        );
        let clone = limiter.clone();
        let mut newcomer = Box::pin(clone.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut newcomer).is_pending());
        assert!(poll!(&mut c2).is_pending());
        drop(c1.await.unwrap());
        drop(c2.await.unwrap());
        assert!(poll!(&mut s2).is_pending());
        drop(s1.await.unwrap());
        drop(s2.await.unwrap());
        drop(newcomer.await.unwrap());
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_leaves_queue() {
        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut cancelled = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut next = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut cancelled).is_pending());
        assert!(poll!(&mut next).is_pending());

        drop(cancelled);
        assert_eq!(limiter.stats().high_volume.waiting_start, 1);
        drop(held);
        drop(next.await.unwrap());
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_after_grant_passes_slot_on() {
        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut cancelled = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut next = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut cancelled).is_pending());
        assert!(poll!(&mut next).is_pending());

        // The slot is sent to `cancelled`, which is dropped before reading it.
        drop(held);
        drop(cancelled);
        drop(next.await.unwrap());
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_remove_waiters() {
        let immediate = ConcurrencyLimiter::new(NonZeroU32::MIN, NonZeroU32::MIN, Duration::ZERO);
        let held = immediate
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        assert_eq!(
            immediate
                .acquire_high_volume(HighVolumeClass::Start)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::AtCapacity
        );
        drop(held);

        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut waiting = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut waiting).is_pending());
        tokio::time::advance(TIMEOUT).await;
        assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(
            limiter.stats().high_volume,
            HighVolumeStats {
                in_use: 1,
                ..Default::default()
            }
        );
        drop(held);
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    /// Races releases against timeouts and cancellations on several threads, including a
    /// releaser picking a waiter that is already gone. Not guaranteed to hit every race on each
    /// run, but checks that no slot is ever lost or returned twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_releases_timeouts_and_cancellations_keep_accounting() {
        const CAPACITY: u32 = 4;
        let limiter = ConcurrencyLimiter::new(
            NonZeroU32::new(CAPACITY).unwrap(),
            NonZeroU32::MIN,
            Duration::from_millis(2),
        );

        let tasks: Vec<_> = (0..200)
            .map(|task| {
                let limiter = limiter.clone();
                tokio::spawn(async move {
                    for round in 0..20 {
                        let class = if (task + round) % 2 == 0 {
                            HighVolumeClass::Start
                        } else {
                            HighVolumeClass::Continue
                        };
                        let acquire = limiter.acquire_high_volume(class);
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

        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
        let mut permits = Vec::new();
        for _ in 0..CAPACITY {
            permits.push(
                limiter
                    .acquire_high_volume(HighVolumeClass::Start)
                    .await
                    .unwrap(),
            );
        }
    }

    #[test]
    fn long_term_rejects_until_release() {
        let limiter = hv(1);
        let permit = limiter.try_acquire_long_term().unwrap();
        assert_eq!(
            limiter.clone().try_acquire_long_term().unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(limiter.stats().long_term.in_use, 1);
        drop(permit);
        assert_eq!(limiter.stats().long_term.in_use, 0);
        drop(limiter.try_acquire_long_term().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn stream_retains_then_releases_at_terminal_item() {
        for error in [false, true] {
            let limiter = hv(1);
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
            let mut guarded = limiter
                .try_acquire_long_term()
                .unwrap()
                .guard_stream(source);
            assert_eq!(
                guarded.next().await.unwrap().unwrap(),
                Bytes::from_static(b"data")
            );
            assert_eq!(limiter.stats().long_term.in_use, 1);
            assert!(poll!(guarded.next()).is_pending());
            assert_eq!(limiter.stats().long_term.in_use, 1);
            let terminal = guarded.next().await;
            if error {
                assert_eq!(terminal.unwrap().unwrap_err().to_string(), "original");
            } else {
                assert!(terminal.is_none());
            }
            assert_eq!(limiter.stats().long_term.in_use, 0);
            assert!(guarded.next().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stream_drop_releases_after_stream() {
        struct PendingStream {
            dropped: Arc<AtomicBool>,
            limiter: ConcurrencyLimiter,
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
                assert_eq!(self.limiter.stats().long_term.in_use, 1);
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        let limiter = hv(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let source = PendingStream {
            dropped: dropped.clone(),
            limiter: limiter.clone(),
        }
        .boxed();
        let mut guarded = limiter
            .try_acquire_long_term()
            .unwrap()
            .guard_stream(source);
        assert!(poll!(guarded.next()).is_pending());
        drop(guarded);
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(limiter.stats().long_term.in_use, 0);
    }
}
