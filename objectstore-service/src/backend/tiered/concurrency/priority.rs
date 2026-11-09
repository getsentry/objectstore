//! FIFO concurrency limiting with normal and high priorities.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::{Mutex, Notify, Semaphore};

use crate::error::{ErrorKind, Result};

use super::Permit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum Priority {
    Normal,
    High,
}

/// Approximate snapshot of permits and outstanding acquisitions by priority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in super::super) struct PriorityLimiterStats {
    /// Permits currently in use.
    pub(in super::super) in_use: u32,
    /// Outstanding normal-priority acquisitions, including those waiting for the lane lock.
    pub(in super::super) waiting_normal: usize,
    /// Outstanding high-priority acquisitions.
    pub(in super::super) waiting_high: usize,
}

/// Limits concurrent operations with high-priority admission and FIFO within each priority.
///
/// The queue is unbounded. The caller must limit incoming requests.
/// Sustained high-priority traffic can starve normal-priority waiters.
#[derive(Clone, Debug)]
pub(in super::super) struct PriorityLimiter {
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
    pub(in super::super) fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
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
    pub(in super::super) async fn acquire(&self, priority: Priority) -> Result<Permit> {
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
    pub(in super::super) fn stats(&self) -> PriorityLimiterStats {
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
    use futures_util::poll;

    const TIMEOUT: Duration = Duration::from_secs(1);

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
}
