//! Concurrency limiter for backend operations.
//!
//! [`ConcurrencyLimiter`] caps the number of in-flight backend operations
//! using a tokio semaphore. Each acquired [`ConcurrencyPermit`] notifies
//! waiters on drop, allowing [`ConcurrencyLimiter::wait_all`] to resolve once
//! all permits have been returned.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{Error, Result};

/// Interval for the periodic metrics emitter.
const EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Limits concurrent backend operations and tracks in-flight count.
///
/// Permits are acquired with [`try_acquire`](Self::try_acquire) and
/// automatically returned when the [`ConcurrencyPermit`] is dropped.
#[derive(Clone, Debug)]
pub(crate) struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    max: usize,
    released: Arc<Notify>,
}

impl ConcurrencyLimiter {
    /// Creates a new limiter with the given maximum number of permits.
    pub(crate) fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            max,
            released: Arc::new(Notify::new()),
        }
    }

    /// Tries to acquire a concurrency permit.
    ///
    /// Returns [`Error::AtCapacity`] when all permits are held.
    pub(crate) fn try_acquire(&self) -> Result<ConcurrencyPermit> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;

        Ok(ConcurrencyPermit {
            permit: Some(permit),
            released: Arc::clone(&self.released),
        })
    }

    /// Returns the number of permits currently held.
    pub(crate) fn used_permits(&self) -> usize {
        self.max - self.semaphore.available_permits()
    }

    /// Waits until all permits have been returned.
    #[allow(dead_code)]
    pub(crate) async fn wait_all(&self) {
        loop {
            let notified = self.released.notified();
            if self.used_permits() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Periodically calls `emit` with the current in-use count.
    ///
    /// This future runs forever and is intended to be spawned as a background
    /// task alongside the service.
    pub(crate) async fn run_emitter<F, Fut>(&self, mut emit: F)
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ticker = tokio::time::interval(EMITTER_INTERVAL);
        loop {
            ticker.tick().await;
            emit(self.used_permits()).await;
        }
    }
}

/// RAII guard for a concurrency permit.
///
/// Dropping this permit releases it back to the [`ConcurrencyLimiter`] and
/// notifies any task waiting in [`ConcurrencyLimiter::wait_all`].
pub(crate) struct ConcurrencyPermit {
    permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::Error;

    #[test]
    fn acquire_and_release() {
        let limiter = ConcurrencyLimiter::new(2);
        assert_eq!(limiter.used_permits(), 0);

        let p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.used_permits(), 1);

        let p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.used_permits(), 2);

        drop(p1);
        assert_eq!(limiter.used_permits(), 1);

        drop(p2);
        assert_eq!(limiter.used_permits(), 0);
    }

    #[test]
    fn at_capacity_rejects() {
        let limiter = ConcurrencyLimiter::new(1);
        let _permit = limiter.try_acquire().unwrap();

        let result = limiter.try_acquire();
        assert!(matches!(result, Err(Error::AtCapacity)));
    }

    #[test]
    fn permit_recovery_after_drop() {
        let limiter = ConcurrencyLimiter::new(1);

        let permit = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_err());

        drop(permit);
        assert!(limiter.try_acquire().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn emitter_calls_callback() {
        let limiter = ConcurrencyLimiter::new(5);
        let _permit = limiter.try_acquire().unwrap();

        let emitted = Arc::new(AtomicUsize::new(0));
        let emitted_clone = Arc::clone(&emitted);

        let emitter = limiter.run_emitter(move |count| {
            let emitted = Arc::clone(&emitted_clone);
            async move {
                emitted.store(count, Ordering::Relaxed);
            }
        });

        tokio::select! {
            _ = emitter => unreachable!("emitter runs forever"),
            _ = tokio::time::sleep(EMITTER_INTERVAL) => {}
        }

        assert_eq!(emitted.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn wait_all_resolves_when_permits_returned() {
        let limiter = ConcurrencyLimiter::new(2);
        let p1 = limiter.try_acquire().unwrap();
        let p2 = limiter.try_acquire().unwrap();

        let mut wait = Box::pin(limiter.wait_all());

        // Dropping one permit is not enough.
        drop(p1);
        assert!(futures::poll!(&mut wait).is_pending());

        // Dropping the last permit should resolve it.
        drop(p2);
        assert!(futures::poll!(&mut wait).is_ready());
    }

    #[tokio::test]
    async fn wait_all_returns_immediately_when_empty() {
        let limiter = ConcurrencyLimiter::new(5);
        let wait = Box::pin(limiter.wait_all());
        assert!(futures::poll!(wait).is_ready());
    }
}
