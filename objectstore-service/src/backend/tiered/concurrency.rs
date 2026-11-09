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
//!
//! The long-term limit uses FIFO scheduling without priority. Both tiers have an
//! independent wait timeout. A zero timeout accepts or rejects immediately.

use std::future::Future;
use std::num::NonZeroU32;
use std::task::Poll;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use tokio::sync::OwnedSemaphorePermit;

use crate::concurrency::limiter::{Limiter, Priority, Scheduling};
use crate::error::Result;
use crate::stream::PayloadStream;

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HighVolumeClass {
    Start,
    Continue,
}

/// Approximate usage snapshot for both tiers; fields are read independently.
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

/// Approximate snapshot of long-term permits and outstanding acquisitions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LongTermStats {
    /// Permits currently in use.
    pub(super) in_use: u32,
    /// Outstanding acquisitions, including assigned permits not yet received.
    pub(super) waiting: usize,
}

/// Concurrency limiter for the Tiered backend.
#[derive(Clone, Debug)]
pub(super) struct ConcurrencyLimiter {
    high_volume: HighVolumeLimiter,
    long_term: LongTermLimiter,
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
            high_volume: HighVolumeLimiter::new(high_volume_capacity, high_volume_timeout),
            long_term: LongTermLimiter::new(long_term_capacity, long_term_timeout),
        }
    }

    /// Acquires high-volume capacity, waiting at most the configured timeout in total.
    ///
    /// Returns [`crate::error::ErrorKind::AtCapacity`] if a permit cannot be acquired within the timeout.
    /// Dropping this future returns an already assigned permit or frees up the corresponding slot
    /// in the queue.
    pub(super) async fn acquire_high_volume(
        &self,
        class: HighVolumeClass,
    ) -> Result<HighVolumePermit> {
        self.high_volume.acquire(class).await
    }

    /// Acquires long-term capacity within its configured timeout.
    ///
    /// A zero timeout returns [`crate::error::ErrorKind::AtCapacity`] immediately when full.
    pub(super) async fn acquire_long_term(&self) -> Result<LongTermPermit> {
        self.long_term.acquire().await
    }

    /// Tries once to acquire long-term capacity, returning [`crate::error::ErrorKind::AtCapacity`] if full.
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

/// Shared high-volume capacity with CONTINUE priority and FIFO within each class.
///
/// Queues are unbounded. Service admission must limit incoming requests.
#[derive(Clone, Debug)]
struct HighVolumeLimiter {
    inner: Limiter,
}

impl HighVolumeLimiter {
    fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        Self {
            inner: Limiter::new(capacity, Scheduling::Priority, timeout),
        }
    }

    async fn acquire(&self, class: HighVolumeClass) -> Result<HighVolumePermit> {
        let priority = match class {
            HighVolumeClass::Start => Priority::Normal,
            HighVolumeClass::Continue => Priority::High,
        };
        self.inner.acquire(priority).await
    }

    fn stats(&self) -> HighVolumeStats {
        let stats = self.inner.stats();
        HighVolumeStats {
            in_use: stats.in_use,
            waiting_start: stats.waiting_normal,
            waiting_continue: stats.waiting_high,
        }
    }
}

/// One owned high-volume permit, released on drop.
pub(super) type HighVolumePermit = OwnedSemaphorePermit;

/// Long-term capacity with FIFO scheduling and a configurable wait timeout.
#[derive(Clone, Debug)]
struct LongTermLimiter {
    inner: Limiter,
}

impl LongTermLimiter {
    fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        Self {
            inner: Limiter::new(capacity, Scheduling::Fifo, timeout),
        }
    }

    async fn acquire(&self) -> Result<LongTermPermit> {
        let permit = self.inner.acquire(Priority::Normal).await?;
        Ok(LongTermPermit { _permit: permit })
    }

    fn try_acquire(&self) -> Result<LongTermPermit> {
        let permit = self.inner.try_acquire(Priority::Normal)?;
        Ok(LongTermPermit { _permit: permit })
    }

    fn stats(&self) -> LongTermStats {
        let stats = self.inner.stats();
        LongTermStats {
            in_use: stats.in_use,
            waiting: stats.waiting_normal,
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
    use crate::error::ErrorKind;
    use bytes::Bytes;
    use futures_util::{Stream, poll};
    use std::io;
    use std::sync::Arc;
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

        // The semaphore assigns a slot to `cancelled`, which is dropped before receiving it.
        drop(held);
        drop(cancelled);
        drop(next.await.unwrap());
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
    async fn long_term_has_its_own_timeout_and_fifo_queue() {
        let limiter = ConcurrencyLimiter::new(
            NonZeroU32::MIN,
            NonZeroU32::MIN,
            TIMEOUT,
            Duration::from_millis(100),
        );
        let held = limiter.acquire_long_term().await.unwrap();
        let clone = limiter.clone();
        let mut cancelled = Box::pin(clone.acquire_long_term());
        assert!(poll!(&mut cancelled).is_pending());
        assert_eq!(limiter.stats().long_term.waiting, 1);
        drop(cancelled);
        assert_eq!(limiter.stats().long_term.waiting, 0);

        let mut timeout = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut timeout).is_pending());
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(timeout.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(limiter.stats().long_term.waiting, 0);

        let mut first = Box::pin(limiter.acquire_long_term());
        let mut second = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        drop(held);
        assert!(poll!(&mut second).is_pending());
        drop(first.await.unwrap());
        drop(second.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn long_term_zero_timeout_rejects_immediately() {
        let limiter = hv(1);
        let held = limiter.acquire_long_term().await.unwrap();
        let before = tokio::time::Instant::now();
        assert_eq!(
            limiter.acquire_long_term().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(tokio::time::Instant::now(), before);
        assert_eq!(limiter.stats().long_term.waiting, 0);
        drop(held);
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
