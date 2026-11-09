//! Concurrency admission for the two storage tiers.
//!
//! START covers consecutive HV calls before any LT operation. CONTINUE covers
//! consecutive HV calls after an LT operation. Both share capacity, with strict
//! CONTINUE priority and FIFO order within each lane. Sustained CONTINUE traffic
//! can starve START. LT has its own FIFO pool. All queues are unbounded because
//! service admission already bounds incoming work.
//!
//! Each acquisition has its own timeout, covering all waiting within that
//! acquisition. A zero timeout allows only an immediate attempt. Release a tier's
//! permit before acquiring another tier; hold LT permits through streamed reads
//! with [`ConcurrencyPermit::guard_stream`].

use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use crate::concurrency::ConcurrencyPermit;
use crate::concurrency::limiter::{
    self, Limiter, PermitPool, Priority, QueueLimit, Scheduling, WaitConfig,
};
use crate::error::Result;

/// Capacity and maximum acquisition wait for one tier.
#[derive(Clone, Copy, Debug)]
pub(super) struct TierLimit {
    pub capacity: NonZeroU32,
    pub timeout: Duration,
}

/// Independent capacity and timeout settings for the two tiers.
#[derive(Clone, Copy, Debug)]
pub(super) struct TieredConcurrencyConfig {
    pub high_volume: TierLimit,
    pub long_term: TierLimit,
}

/// Whether HV work occurs before or after LT work in this request.
#[derive(Clone, Copy, Debug)]
pub(super) enum HighVolumeClass {
    Start,
    Continue,
}

/// Approximate usage snapshot; fields are read independently.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct TieredStats {
    pub high_volume: HighVolumeStats,
    pub long_term: LongTermStats,
}

/// Shared HV usage and outstanding acquisitions in each lane.
///
/// Waiting includes assigned permits whose acquisitions have not resumed yet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct HighVolumeStats {
    pub in_use: u32,
    pub waiting_start: usize,
    pub waiting_continue: usize,
}

/// LT usage and outstanding acquisitions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LongTermStats {
    pub in_use: u32,
    pub waiting: usize,
}

/// Two independent tier pools with separate START and CONTINUE admission handles.
///
/// Clones share capacity and waiting counts. A failed or cancelled acquisition
/// returns all reservations. Capacity failures and timeouts return
/// [`crate::error::ErrorKind::AtCapacity`]. No backend operation is started here.
#[derive(Clone, Debug)]
pub(super) struct TieredConcurrencyLimiter {
    start: Limiter,
    continuation: Limiter,
    long_term: Limiter,
}

impl TieredConcurrencyLimiter {
    /// Creates both pools; zero LT timeout preserves immediate rejection when full.
    pub fn new(config: TieredConcurrencyConfig) -> Self {
        let high_volume = PermitPool::new(config.high_volume.capacity.get(), Scheduling::Priority);
        let wait = WaitConfig {
            queue: QueueLimit::Unbounded,
            timeout: config.high_volume.timeout,
        };
        Self {
            start: Limiter::new(high_volume.clone(), Priority::Normal, wait),
            continuation: Limiter::new(high_volume, Priority::High, wait),
            long_term: Limiter::new(
                PermitPool::new(config.long_term.capacity.get(), Scheduling::Fifo),
                Priority::Normal,
                WaitConfig {
                    queue: QueueLimit::Unbounded,
                    timeout: config.long_term.timeout,
                },
            ),
        }
    }

    /// Acquires shared HV capacity in the selected priority lane.
    pub async fn acquire_high_volume(&self, class: HighVolumeClass) -> Result<ConcurrencyPermit> {
        match class {
            HighVolumeClass::Start => self.start.acquire().await,
            HighVolumeClass::Continue => self.continuation.acquire().await,
        }
    }

    /// Acquires LT capacity, waiting up to the configured LT timeout.
    pub async fn acquire_long_term(&self) -> Result<ConcurrencyPermit> {
        self.long_term.acquire().await
    }

    /// Attempts LT acquisition once, regardless of its configured timeout.
    pub fn try_acquire_long_term(&self) -> Result<ConcurrencyPermit> {
        self.long_term.try_acquire()
    }

    /// Reads shared HV usage once and each handle's waiting count separately.
    pub fn stats(&self) -> TieredStats {
        TieredStats {
            high_volume: HighVolumeStats {
                in_use: self.start.pool().in_use(),
                waiting_start: self.start.waiting(),
                waiting_continue: self.continuation.waiting(),
            },
            long_term: LongTermStats {
                in_use: self.long_term.pool().in_use(),
                waiting: self.long_term.waiting(),
            },
        }
    }

    /// Emits a usage snapshot immediately and once per second thereafter.
    pub async fn run_emitter<F, Fut>(&self, emit: F)
    where
        F: FnMut(TieredStats) -> Fut,
        Fut: Future<Output = ()>,
    {
        limiter::run_emitter(|| self.stats(), emit).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use bytes::Bytes;
    use futures_util::{StreamExt, poll, stream};

    fn limiter(lt_timeout: Duration) -> TieredConcurrencyLimiter {
        TieredConcurrencyLimiter::new(TieredConcurrencyConfig {
            high_volume: TierLimit {
                capacity: NonZeroU32::MIN,
                timeout: Duration::from_secs(1),
            },
            long_term: TierLimit {
                capacity: NonZeroU32::MIN,
                timeout: lt_timeout,
            },
        })
    }

    #[tokio::test(start_paused = true)]
    async fn tiers_are_independent_and_clones_share_capacity() {
        let limiter = limiter(Duration::ZERO);
        let high = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let long = limiter.try_acquire_long_term().unwrap();
        let clone = limiter.clone();
        assert!(clone.try_acquire_long_term().is_err());
        let mut start = Box::pin(clone.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut start).is_pending());
        assert_eq!(limiter.stats().high_volume.in_use, 1);
        assert_eq!(limiter.stats().high_volume.waiting_start, 1);
        assert_eq!(limiter.stats().long_term.in_use, 1);
        drop(high);
        drop(start.await.unwrap());
        // Returning HV capacity does not change LT usage.
        assert_eq!(limiter.stats().long_term.in_use, 1);
        drop(long);
        assert_eq!(limiter.stats(), TieredStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn continue_precedes_start_with_fifo_in_each_lane() {
        let limiter = limiter(Duration::ZERO);
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
        assert_eq!(limiter.stats().high_volume.waiting_continue, 2);
        drop(held);
        // The first START task must return the semaphore's assigned permit.
        assert!(poll!(&mut s1).is_pending());
        assert!(poll!(&mut c2).is_pending());
        drop(c1.await.unwrap());
        drop(c2.await.unwrap());
        assert!(poll!(&mut s2).is_pending());
        drop(s1.await.unwrap());
        drop(s2.await.unwrap());
        assert_eq!(limiter.stats(), TieredStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn long_term_zero_timeout_rejects_without_waiting() {
        let limiter = limiter(Duration::ZERO);
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
    async fn long_term_waits_and_cancellation_or_timeout_removes_waiters() {
        let limiter = limiter(Duration::from_millis(100));
        let held = limiter.acquire_long_term().await.unwrap();
        let mut cancelled = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut cancelled).is_pending());
        assert!(limiter.try_acquire_long_term().is_err());
        assert_eq!(limiter.stats().long_term.waiting, 1);
        drop(cancelled);
        assert_eq!(limiter.stats().long_term.waiting, 0);
        let mut timeout = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut timeout).is_pending());
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(timeout.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        let mut next = Box::pin(limiter.acquire_long_term());
        assert!(poll!(&mut next).is_pending());
        drop(held);
        drop(next.await.unwrap());
        assert_eq!(limiter.stats(), TieredStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_assigned_continue_unblocks_start() {
        let limiter = limiter(Duration::ZERO);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut continuation = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Continue));
        let mut start = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        assert!(poll!(&mut continuation).is_pending());
        assert!(poll!(&mut start).is_pending());
        drop(held);
        drop(continuation);
        drop(start.await.unwrap());
        assert_eq!(limiter.stats(), TieredStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn high_volume_timeout_returns_capacity() {
        let limiter = limiter(Duration::ZERO);
        let held = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let mut start = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Start));
        let mut continuation = Box::pin(limiter.acquire_high_volume(HighVolumeClass::Continue));
        assert!(poll!(&mut start).is_pending());
        assert!(poll!(&mut continuation).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(start.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(
            continuation.await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(limiter.stats().high_volume.waiting_continue, 0);
        drop(held);
        drop(
            limiter
                .acquire_high_volume(HighVolumeClass::Start)
                .await
                .unwrap(),
        );
        assert_eq!(limiter.stats(), TieredStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_both_tiers_immediately_and_periodically() {
        let limiter = limiter(Duration::ZERO);
        let high = limiter
            .acquire_high_volume(HighVolumeClass::Start)
            .await
            .unwrap();
        let long = limiter.try_acquire_long_term().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut emit = Box::pin(limiter.run_emitter(|stats| {
            tx.send(stats).unwrap();
            std::future::ready(())
        }));
        assert!(poll!(&mut emit).is_pending());
        assert_eq!(rx.try_recv().unwrap(), limiter.stats());
        assert!(rx.try_recv().is_err());
        drop((high, long));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut emit).is_pending());
        assert_eq!(rx.try_recv().unwrap(), TieredStats::default());
    }

    #[tokio::test]
    async fn streamed_read_releases_capacity_at_eof_error_or_drop() {
        let limiter = limiter(Duration::ZERO);
        for error in [false, true] {
            let source = stream::iter([
                Ok(Bytes::from_static(b"data")),
                if error {
                    Err(std::io::Error::other("read failed"))
                } else {
                    Ok(Bytes::new())
                },
            ])
            .boxed();
            let mut guarded = limiter
                .try_acquire_long_term()
                .unwrap()
                .guard_stream(source);
            assert_eq!(
                guarded.next().await.unwrap().unwrap(),
                Bytes::from_static(b"data")
            );
            assert!(limiter.try_acquire_long_term().is_err());
            if error {
                assert!(guarded.next().await.unwrap().is_err());
            } else {
                assert!(guarded.next().await.unwrap().unwrap().is_empty());
                assert!(limiter.try_acquire_long_term().is_err());
            }
            assert!(guarded.next().await.is_none());
            assert_eq!(limiter.stats().long_term.in_use, 0);
        }
        let guarded = limiter
            .try_acquire_long_term()
            .unwrap()
            .guard_stream(stream::pending().boxed());
        assert!(limiter.try_acquire_long_term().is_err());
        drop(guarded);
        assert_eq!(limiter.stats(), TieredStats::default());
    }
}
