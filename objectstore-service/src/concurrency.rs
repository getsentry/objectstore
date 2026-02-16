//! Concurrency limiting for backends with resource constraints.

use std::sync::Arc;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::{ServiceError, ServiceResult};

/// Guard that holds a semaphore permit and releases it on drop.
///
/// This ensures the concurrency slot is freed even if the operation panics or returns early.
#[derive(Debug)]
pub struct ConcurrencyGuard<'a> {
    _permit: SemaphorePermit<'a>,
}

/// Helper for backends that need concurrency limiting.
///
/// This provides backpressure management by limiting the number of concurrent operations
/// to a backend. When the limit is reached, new requests will be rejected with a
/// `ServiceError::Backpressure` error.
#[derive(Debug, Clone)]
pub struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    max_concurrency: usize,
    backend_name: &'static str,
}

impl ConcurrencyLimiter {
    /// Creates a new concurrency limiter with the given maximum.
    ///
    /// # Arguments
    ///
    /// * `max_concurrency` - Maximum number of concurrent operations allowed
    /// * `backend_name` - Name of the backend for logging and metrics
    pub fn new(max_concurrency: usize, backend_name: &'static str) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            max_concurrency,
            backend_name,
        }
    }

    /// Try to acquire a concurrency slot. Returns a guard that releases on drop.
    ///
    /// If the maximum concurrency is reached, this returns a `ServiceError::Backpressure`
    /// error. The returned guard automatically releases the slot when dropped, ensuring
    /// proper cleanup even on early returns or panics.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// async fn put_object(&self, ...) -> ServiceResult<()> {
    ///     let _guard = self.limiter.try_acquire()?;
    ///     // ... your operation here ...
    ///     Ok(())
    /// }
    /// ```
    pub fn try_acquire(&self) -> ServiceResult<ConcurrencyGuard<'_>> {
        let permit = self.semaphore.try_acquire().map_err(|_| {
            tracing::debug!(
                backend = self.backend_name,
                max_concurrency = self.max_concurrency,
                "Backend at capacity, rejecting request"
            );
            merni::counter!("backend.backpressure.rejected": 1, "backend" => self.backend_name);

            ServiceError::Backpressure {
                context: format!("{} backend at capacity", self.backend_name),
                max_concurrency: self.max_concurrency,
            }
        })?;

        Ok(ConcurrencyGuard { _permit: permit })
    }

    /// Returns the number of available permits (for metrics/debugging).
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}
