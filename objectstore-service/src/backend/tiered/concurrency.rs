//! Primitives for concurrency limiting in the Tiered backend.
//!
//! High-volume operations are classified between `Start` (HV operations happening *before* any
//! LT operations in a Tiered request) and `Continue` (HV operations happening *after* some LT operations).
//! Both classes share the same limiter, but `Continue` operations always have priority over
//! `Start` ones, as we don't want to reject requests after performing the (usually) expensive LT calls.
//! Requests queue in FIFO order within each class and wait up to a configured timeout.
//! Sustained CONTINUE traffic can starve START. A START waiter may need to resume and return
//! an assigned permit before CONTINUE can receive it. Other START waiters wait for the lane
//! holder to finish acquiring, even if capacity is available.
//! CONTINUE remains pending until its task receives the assigned permit, so START can also
//! wait for that task to resume while other permits are free.
//!
//! The long-term limit uses FIFO queuing without priority. Both tiers have an independent
//! wait timeout. A zero timeout accepts or rejects immediately.
//! Both use `Limiter<S>`: `Prioritized` supplies HV scheduling, and `Fifo` supplies LT scheduling.
//! Only the priority limiter accepts a priority argument and contains priority state.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt, stream};
use tokio::sync::{AcquireError, Mutex, Notify, OwnedSemaphorePermit, Semaphore};

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

/// Approximate snapshot of high-volume permits and outstanding acquisitions.
///
/// Fields are read independently. Waiting counts include acquisitions with assigned
/// permits that have not resumed yet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct HighVolumeStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
    /// Outstanding `Start` acquisitions, including those waiting for the lane lock.
    pub(super) waiting_start: usize,
    /// Outstanding `Continue` acquisitions.
    pub(super) waiting_continue: usize,
}

/// Snapshot of long-term permits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LongTermStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
}

/// Concurrency limiter for the Tiered backend.
#[derive(Clone, Debug)]
pub(super) struct ConcurrencyLimiter {
    high_volume: Arc<Limiter<Prioritized>>,
    long_term: Arc<Limiter<Fifo>>,
}

impl ConcurrencyLimiter {
    /// Creates independent tier limits with their own capacities and wait timeouts.
    pub(super) fn new(
        high_volume_capacity: NonZeroU32,
        long_term_capacity: NonZeroU32,
        high_volume_timeout: Duration,
        long_term_timeout: Duration,
    ) -> Self {
        Self {
            high_volume: Arc::new(Limiter::new(high_volume_capacity, high_volume_timeout)),
            long_term: Arc::new(Limiter::new(long_term_capacity, long_term_timeout)),
        }
    }

    /// Acquires high-volume capacity, waiting at most the configured timeout in total.
    ///
    /// Returns [`ErrorKind::AtCapacity`] if a permit cannot be acquired within the timeout.
    /// Dropping this future returns an already assigned permit or frees up the corresponding slot
    /// in the queue.
    pub(super) async fn acquire_high_volume(
        &self,
        class: HighVolumeClass,
    ) -> Result<HighVolumePermit> {
        let priority = match class {
            HighVolumeClass::Start => Priority::Normal,
            HighVolumeClass::Continue => Priority::High,
        };
        self.high_volume.acquire(priority).await
    }

    /// Acquires long-term capacity within its configured timeout.
    ///
    /// A zero timeout returns [`ErrorKind::AtCapacity`] immediately when full.
    pub(super) async fn acquire_long_term(&self) -> Result<LongTermPermit> {
        let permit = self.long_term.acquire().await?;
        Ok(LongTermPermit { _permit: permit })
    }

    fn stats(&self) -> Stats {
        let scheduling = &self.high_volume.scheduling;
        Stats {
            high_volume: HighVolumeStats {
                in_use: self.high_volume.in_use(),
                waiting_start: scheduling.waiting_normal.load(Ordering::Relaxed),
                waiting_continue: scheduling.waiting_high.load(Ordering::Relaxed),
            },
            long_term: LongTermStats {
                in_use: self.long_term.in_use(),
            },
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

/// Capacity and timeout shared by FIFO and priority scheduling.
///
/// Queues are unbounded. Service admission must limit incoming requests.
#[derive(Debug)]
struct Limiter<S> {
    capacity: u32,
    semaphore: Arc<Semaphore>,
    timeout: Duration,
    scheduling: S,
}

/// FIFO scheduling without priority state.
#[derive(Debug, Default)]
struct Fifo;

/// FIFO within each priority, with high-priority waiters admitted first.
#[derive(Debug, Default)]
struct Prioritized {
    normal_lane: Mutex<()>,
    waiting_normal: AtomicUsize,
    waiting_high: AtomicUsize,
    high_drained: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Priority {
    Normal,
    High,
}

impl<S> Limiter<S> {
    fn new(capacity: NonZeroU32, timeout: Duration) -> Self
    where
        S: Default,
    {
        Self {
            capacity: capacity.get(),
            semaphore: Arc::new(Semaphore::new(capacity.get() as usize)),
            timeout,
            scheduling: S::default(),
        }
    }

    fn in_use(&self) -> u32 {
        self.capacity - self.semaphore.available_permits() as u32
    }

    async fn wait_for_permit(
        &self,
        acquire: impl Future<Output = std::result::Result<OwnedSemaphorePermit, AcquireError>>,
    ) -> Result<OwnedSemaphorePermit> {
        // A zero timeout polls once without waiting or using a timer.
        let permit = if self.timeout.is_zero() {
            acquire.now_or_never().ok_or(ErrorKind::AtCapacity)?
        } else {
            // One deadline covers the complete acquisition, including priority scheduling.
            tokio::time::timeout(self.timeout, acquire)
                .await
                .map_err(|_| ErrorKind::AtCapacity)?
        };
        permit.map_err(|_| ErrorKind::AtCapacity.into())
    }
}

impl Limiter<Fifo> {
    async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        // FIFO has no scheduling gates, so a zero timeout can try the semaphore directly.
        if self.timeout.is_zero() {
            return self
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| ErrorKind::AtCapacity.into());
        }
        self.wait_for_permit(self.semaphore.clone().acquire_owned())
            .await
    }
}

impl Limiter<Prioritized> {
    async fn acquire(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        let scheduling = &self.scheduling;
        scheduling.waiting(priority).fetch_add(1, Ordering::AcqRel);
        let _waiting = WaitingGuard {
            scheduling,
            priority,
        };
        let acquire = async {
            if priority == Priority::High {
                return self.semaphore.clone().acquire_owned().await;
            }

            // Only one normal waiter can enter the semaphore queue at a time.
            let _lane = scheduling.normal_lane.lock().await;
            loop {
                scheduling.wait_until_high_drained().await;
                let permit = self.semaphore.clone().acquire_owned().await?;
                if scheduling.waiting_high.load(Ordering::Acquire) == 0 {
                    return Ok(permit);
                }
                // High-priority work registered while this task was queued. Return the permit
                // and wait for it to drain, including tasks that have not joined the queue yet.
                drop(permit);
            }
        };
        self.wait_for_permit(acquire).await
    }
}

impl Prioritized {
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
    scheduling: &'a Prioritized,
    priority: Priority,
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        let previous = self
            .scheduling
            .waiting(self.priority)
            .fetch_sub(1, Ordering::AcqRel);
        if self.priority == Priority::High && previous == 1 {
            self.scheduling.high_drained.notify_waiters();
        }
    }
}

/// One owned high-volume permit, released on drop.
pub(super) type HighVolumePermit = OwnedSemaphorePermit;

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
        let long = limiter.acquire_long_term().await.unwrap();
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
        ConcurrencyLimiter::new(
            NonZeroU32::new(capacity).unwrap(),
            NonZeroU32::MIN,
            TIMEOUT,
            Duration::ZERO,
        )
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
                waiting_continue: 2
            }
        );
        let clone = limiter.clone();
        let mut newcomer = Box::pin(clone.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut newcomer).is_pending());
        // START was first in the semaphore queue. It must return its assigned permit.
        assert!(poll!(&mut s1).is_pending());
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
        for class in [HighVolumeClass::Start, HighVolumeClass::Continue] {
            let limiter = hv(1);
            let held = limiter
                .acquire_high_volume(HighVolumeClass::Start)
                .await
                .unwrap();
            let mut cancelled = Box::pin(limiter.acquire_high_volume(class));
            let mut next = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
            assert!(poll!(&mut cancelled).is_pending());
            assert!(poll!(&mut next).is_pending());

            drop(cancelled);
            assert_eq!(limiter.stats().high_volume.waiting_start, 1);
            drop(held);
            drop(next.await.unwrap());
            assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_after_grant_passes_slot_on() {
        for class in [HighVolumeClass::Start, HighVolumeClass::Continue] {
            let limiter = hv(1);
            let held = limiter
                .acquire_high_volume(HighVolumeClass::Start)
                .await
                .unwrap();
            let mut cancelled = Box::pin(limiter.acquire_high_volume(class));
            let mut next = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
            assert!(poll!(&mut cancelled).is_pending());
            assert!(poll!(&mut next).is_pending());

            // The semaphore assigns a slot to `cancelled`, which is dropped before receiving it.
            drop(held);
            drop(cancelled);
            drop(next.await.unwrap());
            assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn start_returns_permit_for_continue_not_yet_in_semaphore_queue() {
        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut start = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut start).is_pending());

        // Pause CONTINUE between registering and joining the semaphore queue.
        let scheduling = &limiter.high_volume.scheduling;
        scheduling.waiting_high.fetch_add(1, Ordering::AcqRel);
        let pending = WaitingGuard {
            scheduling,
            priority: Priority::High,
        };
        drop(held);
        // START must return its permit and suspend rather than take it again in a loop.
        assert!(poll!(&mut start).is_pending());
        assert_eq!(limiter.high_volume.semaphore.available_permits(), 1);

        // Cancelling that CONTINUE registration must wake START.
        drop(pending);
        drop(start.await.unwrap());
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_covers_lane_and_priority_wait_together() {
        let limiter = hv(1);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut first = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut second = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut continuing = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Continue));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(poll!(&mut continuing).is_pending());

        tokio::time::advance(TIMEOUT / 2).await;
        drop(first);
        // The second START leaves the lane queue, then waits for CONTINUE to drain.
        assert!(poll!(&mut second).is_pending());
        tokio::time::advance(TIMEOUT / 2).await;
        assert_eq!(second.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(limiter.stats().high_volume.waiting_start, 0);
        drop(continuing);
        drop(held);
        assert_eq!(limiter.stats().high_volume, HighVolumeStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_remove_waiters() {
        let immediate = ConcurrencyLimiter::new(
            NonZeroU32::MIN,
            NonZeroU32::MIN,
            Duration::ZERO,
            Duration::ZERO,
        );
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

    /// Races releases against timeouts and cancellations on several threads, including
    /// cancellation after the semaphore assigns a permit. Not guaranteed to hit every race on each
    /// run, but checks that no slot is ever lost or returned twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_releases_timeouts_and_cancellations_keep_accounting() {
        const CAPACITY: u32 = 4;
        let limiter = ConcurrencyLimiter::new(
            NonZeroU32::new(CAPACITY).unwrap(),
            NonZeroU32::MIN,
            Duration::from_millis(2),
            Duration::ZERO,
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

    #[tokio::test(start_paused = true)]
    async fn long_term_rejects_until_release() {
        let limiter = hv(1);
        let permit = limiter.acquire_long_term().await.unwrap();
        let before = tokio::time::Instant::now();
        assert_eq!(
            limiter
                .clone()
                .acquire_long_term()
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(tokio::time::Instant::now(), before);
        assert_eq!(limiter.stats().long_term.in_use, 1);
        drop(permit);
        assert_eq!(limiter.stats().long_term.in_use, 0);
        drop(limiter.acquire_long_term().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn long_term_waits_in_fifo_order_with_its_own_timeout() {
        let timeout = Duration::from_millis(100);
        let limiter = ConcurrencyLimiter::new(NonZeroU32::MIN, NonZeroU32::MIN, TIMEOUT, timeout);
        let held = limiter.acquire_long_term().await.unwrap();
        let mut waiting = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut waiting).is_pending());
        tokio::time::advance(timeout).await;
        assert_eq!(waiting.await.unwrap_err().kind(), ErrorKind::AtCapacity);

        let mut first = Box::pin(limiter.acquire_long_term());
        let mut second = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        drop(held);
        assert!(poll!(&mut second).is_pending());
        drop(first.await.unwrap());
        drop(second.await.unwrap());
        assert_eq!(limiter.stats().long_term, LongTermStats::default());
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
                .acquire_long_term()
                .await
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
            .acquire_long_term()
            .await
            .unwrap()
            .guard_stream(source);
        assert!(poll!(guarded.next()).is_pending());
        drop(guarded);
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(limiter.stats().long_term.in_use, 0);
    }
}
