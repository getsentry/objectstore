//! Concurrency limiter for backend operations.
//!
//! [`ConcurrencyLimiter`] caps the number of in-flight backend operations
//! using a tokio semaphore. Each acquired [`ConcurrencyPermit`] notifies
//! waiters on drop, allowing [`ConcurrencyLimiter::wait_all`] to resolve once
//! all permits have been returned.
//!
//! [`spawn_metered`] spawns an arbitrary future as an isolated task with panic
//! recovery and `service.task.*` metric emission.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use sentry::{Hub, SentryFutureExt, TransactionContext};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::Level;

use crate::error::{Error, Result};

/// Interval for the periodic metrics emitter.
const EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Limits concurrent backend operations and tracks in-flight count.
///
/// Permits are acquired with [`try_acquire`](Self::try_acquire) and
/// automatically returned when the [`ConcurrencyPermit`] is dropped.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    max: usize,
    released: Arc<Notify>,
}

impl ConcurrencyLimiter {
    /// Creates a new limiter with the given maximum number of permits.
    pub fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            max,
            released: Arc::new(Notify::new()),
        }
    }

    /// Tries to acquire `count` permits at once as a single bulk reservation.
    ///
    /// Returns a [`ConcurrencyPermit`] that releases all `count` permits and
    /// notifies waiters on drop, just like single-permit acquisition.
    ///
    /// Returns [`Error::AtCapacity`] when fewer than `count` permits are available.
    pub fn try_acquire_many(&self, count: usize) -> Result<ConcurrencyPermit> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_many_owned(count as u32)
            .map_err(|_| Error::AtCapacity)?;
        Ok(ConcurrencyPermit {
            permit: Some(permit),
            released: Arc::clone(&self.released),
        })
    }

    /// Tries to acquire a single concurrency permit.
    ///
    /// Convenience shorthand for `try_acquire_many(1)`.
    ///
    /// Returns [`Error::AtCapacity`] when all permits are held.
    pub fn try_acquire(&self) -> Result<ConcurrencyPermit> {
        self.try_acquire_many(1)
    }

    /// Returns the number of permits currently available.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Returns the number of permits currently held.
    pub fn used_permits(&self) -> usize {
        self.max - self.semaphore.available_permits()
    }

    /// Returns the total number of permits.
    pub fn total_permits(&self) -> usize {
        self.max
    }

    /// Waits until all permits have been returned.
    #[allow(dead_code)]
    pub async fn wait_all(&self) {
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
    pub async fn run_emitter<F, Fut>(&self, mut emit: F)
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
pub struct ConcurrencyPermit {
    permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl std::fmt::Debug for ConcurrencyPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyPermit").finish_non_exhaustive()
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.released.notify_waiters();
    }
}

/// Spawns a future on a dedicated task with panic isolation and timing metrics.
///
/// The `guard` is moved into the spawned task and dropped after the future
/// completes, ensuring any resource it represents (e.g. a concurrency permit)
/// outlives the operation.
///
/// Emits `service.task.start` (counter) before spawning and
/// `service.task.duration` (distribution) when the task completes, both tagged
/// with the given `operation` name. The duration tag includes an `outcome` of
/// `"success"` or `"error"`.
pub async fn spawn_metered<T, G, F>(operation: &'static str, guard: G, f: F) -> Result<T>
where
    T: Send + 'static,
    G: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    objectstore_metrics::count!("service.task.start", operation = operation);

    let hub = Hub::current();
    let span = hub.configure_scope(|scope| scope.get_span());

    let new_hub = Hub::new_from_top(hub);
    let transaction = new_hub.start_transaction(TransactionContext::continue_from_span(
        operation,
        "tokio.task",
        span,
    ));

    let scope_guard = new_hub.push_scope();
    new_hub.configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(
        async move {
            let start = tokio::time::Instant::now();
            let result = std::panic::AssertUnwindSafe(f)
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| Err(Error::panic(payload)));

            if let Err(ref e) = result {
                let err_ref = e as &dyn std::error::Error;
                match e.level() {
                    Level::ERROR => tracing::error!(operation, error = err_ref, "Task failed"),
                    Level::WARN => tracing::warn!(operation, error = err_ref, "Task failed"),
                    Level::INFO => tracing::info!(operation, error = err_ref, "Task failed"),
                    Level::DEBUG => tracing::debug!(operation, error = err_ref, "Task failed"),
                    Level::TRACE => tracing::trace!(operation, error = err_ref, "Task failed"),
                }
            }

            objectstore_metrics::record!(
                "service.task.duration" = start.elapsed(),
                operation = operation,
                outcome = if result.is_ok() { "success" } else { "error" },
            );

            let _ = tx.send(result);
            drop(guard);
            transaction.finish();
            drop(scope_guard);
        }
        .bind_hub(new_hub),
    );
    rx.await.map_err(|_| Error::Dropped)?
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::Error;

    #[test]
    fn available_permits_tracks_held() {
        let limiter = ConcurrencyLimiter::new(5);
        assert_eq!(limiter.available_permits(), 5);

        let p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.available_permits(), 4);

        let p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.available_permits(), 3);

        drop(p1);
        assert_eq!(limiter.available_permits(), 4);

        drop(p2);
        assert_eq!(limiter.available_permits(), 5);
    }

    #[test]
    fn total_permits_returns_configured_max() {
        let limiter = ConcurrencyLimiter::new(42);
        assert_eq!(limiter.total_permits(), 42);
    }

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
