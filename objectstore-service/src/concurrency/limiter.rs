//! Shared permit capacity with optional priority scheduling.
//!
//! FIFO mode uses Tokio's semaphore queue directly. Priority mode adds a mutex
//! that lets only one normal waiter enter that queue. High-priority waiters
//! register before acquiring; the normal waiter returns an assigned permit if
//! priority work is pending. Each lane is FIFO by its lock or semaphore queue
//! order. Continuous priority traffic can starve normal traffic.
//!
//! Cancellation returns assigned permits, releases the normal lane, and removes
//! pending counts. Queues are unbounded; callers must bound incoming work.
//! Each acquisition has one configurable deadline covering all waiting. A zero
//! timeout allows only an immediate attempt.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{ErrorKind, Result};

/// Scheduling policy for a shared limiter.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Scheduling {
    /// Serve semaphore waiters in FIFO order without a normal-lane lock.
    Fifo,
    /// Serve pending high-priority acquisitions before normal acquisitions.
    Priority,
}

/// Acquisition lane; FIFO mode treats both lanes equally.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Priority {
    Normal,
    High,
}

/// Approximate usage; fields are read independently.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Stats {
    pub in_use: u32,
    /// Includes assigned permits whose acquisitions have not resumed.
    pub waiting_normal: usize,
    /// Includes assigned permits whose acquisitions have not resumed.
    pub waiting_high: usize,
}

#[derive(Debug, Default)]
struct PriorityState {
    normal_lane: Mutex<()>,
    drained: Notify,
}

#[derive(Debug)]
struct Inner {
    permits: Arc<Semaphore>,
    capacity: u32,
    timeout: Duration,
    waiting_normal: AtomicUsize,
    waiting_high: AtomicUsize,
    priority: Option<PriorityState>,
}

/// Shared capacity, configured with FIFO or priority scheduling.
///
/// Clones share permits, pending counts, and scheduling state. Returned permits
/// own their semaphore reference and can outlive this limiter.
#[derive(Clone, Debug)]
pub(crate) struct Limiter {
    inner: Arc<Inner>,
}

impl Limiter {
    pub fn new(capacity: NonZeroU32, scheduling: Scheduling, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(capacity.get() as usize)),
                capacity: capacity.get(),
                timeout,
                waiting_normal: AtomicUsize::new(0),
                waiting_high: AtomicUsize::new(0),
                priority: matches!(scheduling, Scheduling::Priority).then(PriorityState::default),
            }),
        }
    }

    /// Acquires capacity within the configured timeout.
    ///
    /// One deadline covers the lane lock, priority drain, and semaphore wait.
    /// Zero timeout makes one immediate attempt. Timeout or capacity failures
    /// return [`ErrorKind::AtCapacity`].
    ///
    /// In priority mode, a successful normal acquisition takes effect at its
    /// final pending-count check. A concurrent priority registration after that
    /// check does not revoke the permit.
    pub async fn acquire(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        if self.inner.timeout.is_zero() {
            return self.try_acquire(priority);
        }
        tokio::time::timeout(self.inner.timeout, self.acquire_waiting(priority))
            .await
            .map_err(|_| ErrorKind::AtCapacity)?
    }

    async fn acquire_waiting(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        let inner = &self.inner;
        let count = match priority {
            Priority::Normal => &inner.waiting_normal,
            Priority::High => &inner.waiting_high,
        };
        let drained = match (&inner.priority, priority) {
            (Some(state), Priority::High) => Some(&state.drained),
            _ => None,
        };
        count.fetch_add(1, Ordering::AcqRel);
        let _waiting = Waiting { count, drained };

        let Some(state) = &inner.priority else {
            return self.acquire_permit().await;
        };
        if matches!(priority, Priority::High) {
            return self.acquire_permit().await;
        }

        let _lane = state.normal_lane.lock().await;
        loop {
            loop {
                // notify_waiters reaches Notified futures from their creation,
                // even before polling. Subscribe before checking the count.
                let drained = state.drained.notified();
                if inner.waiting_high.load(Ordering::Acquire) == 0 {
                    break;
                }
                drained.await;
            }
            let permit = self.acquire_permit().await?;
            if inner.waiting_high.load(Ordering::Acquire) == 0 {
                return Ok(permit);
            }
            // The priority waiter may not have entered the semaphore yet.
            // Wait for the count to drain before attempting another acquire.
            drop(permit);
        }
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ErrorKind::AtCapacity.into())
    }

    /// Attempts acquisition once, respecting the normal lane and priority count.
    pub fn try_acquire(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        let inner = &self.inner;
        let _lane = match (&inner.priority, priority) {
            (Some(state), Priority::Normal) => {
                let lane = state
                    .normal_lane
                    .try_lock()
                    .map_err(|_| ErrorKind::AtCapacity)?;
                if inner.waiting_high.load(Ordering::Acquire) != 0 {
                    return Err(ErrorKind::AtCapacity.into());
                }
                Some(lane)
            }
            _ => None,
        };
        let permit = inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorKind::AtCapacity)?;
        if inner.priority.is_some()
            && matches!(priority, Priority::Normal)
            && inner.waiting_high.load(Ordering::Acquire) != 0
        {
            drop(permit);
            return Err(ErrorKind::AtCapacity.into());
        }
        Ok(permit)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            in_use: self.inner.capacity - self.inner.permits.available_permits() as u32,
            waiting_normal: self.inner.waiting_normal.load(Ordering::Relaxed),
            waiting_high: self.inner.waiting_high.load(Ordering::Relaxed),
        }
    }
}

struct Waiting<'a> {
    count: &'a AtomicUsize,
    drained: Option<&'a Notify>,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.count.fetch_sub(1, Ordering::AcqRel) == 1
            && let Some(drained) = self.drained
        {
            drained.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::poll;

    fn limiter(capacity: u32, scheduling: Scheduling) -> Limiter {
        Limiter::new(
            NonZeroU32::new(capacity).unwrap(),
            scheduling,
            Duration::from_secs(1),
        )
    }

    #[tokio::test]
    async fn fifo_ignores_priority_and_assigns_all_available_permits() {
        let limiter = limiter(2, Scheduling::Fifo);
        assert!(limiter.inner.priority.is_none());
        let held = [
            limiter.try_acquire(Priority::Normal).unwrap(),
            limiter.try_acquire(Priority::High).unwrap(),
        ];
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        let clone = limiter.clone();
        let mut high = Box::pin(clone.acquire(Priority::High));
        assert!(poll!(&mut normal).is_pending());
        assert!(poll!(&mut high).is_pending());
        drop(held);
        // FIFO has no lane lock: the second task can complete before the first
        // task resumes, because both already have assigned permits.
        let second = high.await.unwrap();
        assert_eq!(limiter.stats().waiting_normal, 1);
        let first = normal.await.unwrap();
        drop((first, second));
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test]
    async fn fifo_does_not_let_high_priority_overtake_normal() {
        let limiter = limiter(1, Scheduling::Fifo);
        let held = limiter.try_acquire(Priority::Normal).unwrap();
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        let mut high = Box::pin(limiter.acquire(Priority::High));
        assert!(poll!(&mut normal).is_pending());
        assert!(poll!(&mut high).is_pending());
        drop(held);
        assert!(poll!(&mut high).is_pending());
        assert!(limiter.try_acquire(Priority::High).is_err());
        drop(normal.await.unwrap());
        drop(high.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test]
    async fn registered_priority_blocks_normal_before_entering_semaphore() {
        let limiter = limiter(1, Scheduling::Priority);
        // Simulate a priority task paused after registration but before it
        // enters the semaphore queue. Normal must leave the free permit alone.
        limiter.inner.waiting_high.fetch_add(1, Ordering::AcqRel);
        let waiting = Waiting {
            count: &limiter.inner.waiting_high,
            drained: Some(&limiter.inner.priority.as_ref().unwrap().drained),
        };
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut normal).is_pending());
        assert_eq!(limiter.stats().in_use, 0);
        assert!(limiter.try_acquire(Priority::Normal).is_err());
        let high = limiter.try_acquire(Priority::High).unwrap();
        drop(waiting);
        assert!(poll!(&mut normal).is_pending());
        drop(high);
        drop(normal.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test]
    async fn cancellation_after_assignment_clears_priority_and_wakes_normal() {
        let limiter = limiter(1, Scheduling::Priority);
        let held = limiter.try_acquire(Priority::Normal).unwrap();
        let mut high = Box::pin(limiter.acquire(Priority::High));
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut high).is_pending());
        assert!(poll!(&mut normal).is_pending());
        drop(held);
        drop(high);
        assert_eq!(limiter.stats().waiting_high, 0);
        drop(normal.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test]
    async fn normal_try_acquire_cannot_pass_the_lane_holder() {
        let limiter = limiter(2, Scheduling::Priority);
        let held = [
            limiter.try_acquire(Priority::Normal).unwrap(),
            limiter.try_acquire(Priority::Normal).unwrap(),
        ];
        let mut first = Box::pin(limiter.acquire(Priority::Normal));
        let mut second = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        drop(held);
        assert_eq!(limiter.inner.permits.available_permits(), 1);
        assert!(limiter.try_acquire(Priority::Normal).is_err());
        assert!(poll!(&mut second).is_pending());
        // Cancellation must release both the assigned permit and the lane.
        drop(first);
        drop(second.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }

    #[tokio::test]
    async fn drain_notification_before_first_poll_is_received() {
        let limiter = limiter(1, Scheduling::Priority);
        let drained = &limiter.inner.priority.as_ref().unwrap().drained;
        limiter.inner.waiting_high.fetch_add(1, Ordering::AcqRel);
        let waiting = Waiting {
            count: &limiter.inner.waiting_high,
            drained: Some(drained),
        };
        let notified = drained.notified();
        assert_eq!(limiter.inner.waiting_high.load(Ordering::Acquire), 1);
        drop(waiting);
        tokio::pin!(notified);
        assert!(poll!(notified).is_ready());
    }

    #[tokio::test]
    async fn owned_permit_outlives_limiter() {
        let limiter = limiter(1, Scheduling::Fifo);
        let permit = limiter.acquire(Priority::Normal).await.unwrap();
        drop(limiter);
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_accepts_or_rejects_without_waiting() {
        for scheduling in [Scheduling::Fifo, Scheduling::Priority] {
            for priority in [Priority::Normal, Priority::High] {
                let limiter = Limiter::new(NonZeroU32::MIN, scheduling, Duration::ZERO);
                let held = limiter.acquire(priority).await.unwrap();
                let before = tokio::time::Instant::now();
                assert_eq!(
                    limiter.acquire(priority).await.unwrap_err().kind(),
                    ErrorKind::AtCapacity
                );
                assert_eq!(tokio::time::Instant::now(), before);
                assert_eq!(limiter.stats().waiting_normal, 0);
                assert_eq!(limiter.stats().waiting_high, 0);
                drop(held);
                drop(limiter.acquire(priority).await.unwrap());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn one_deadline_covers_lane_priority_drain_and_semaphore() {
        let limiter = limiter(1, Scheduling::Priority);
        let held = limiter.try_acquire(Priority::Normal).unwrap();
        let mut first = Box::pin(limiter.acquire(Priority::Normal));
        let mut second = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        tokio::time::advance(Duration::from_millis(600)).await;

        let mut high = Box::pin(limiter.acquire(Priority::High));
        assert!(poll!(&mut high).is_pending());
        drop(first);
        // The second waiter now holds the lane, but waits for priority to drain.
        assert!(poll!(&mut second).is_pending());
        tokio::time::advance(Duration::from_millis(200)).await;
        drop(high);
        // Priority has drained; the same acquisition now waits on the semaphore.
        assert!(poll!(&mut second).is_pending());
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(second.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(
            limiter.stats(),
            Stats {
                in_use: 1,
                ..Default::default()
            }
        );
        drop(held);
        drop(limiter.try_acquire(Priority::Normal).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn priority_timeout_clears_count_and_wakes_normal() {
        let limiter = limiter(1, Scheduling::Priority);
        let held = limiter.try_acquire(Priority::Normal).unwrap();
        let mut high = Box::pin(limiter.acquire(Priority::High));
        assert!(poll!(&mut high).is_pending());
        tokio::time::advance(Duration::from_millis(600)).await;
        let mut normal = Box::pin(limiter.acquire(Priority::Normal));
        assert!(poll!(&mut normal).is_pending());
        tokio::time::advance(Duration::from_millis(400)).await;
        assert_eq!(high.await.unwrap_err().kind(), ErrorKind::AtCapacity);
        assert_eq!(limiter.stats().waiting_high, 0);
        drop(held);
        drop(normal.await.unwrap());
        assert_eq!(limiter.stats(), Stats::default());
    }
}
