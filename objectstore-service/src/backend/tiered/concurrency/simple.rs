//! FIFO concurrency limiting without priorities.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::error::{ErrorKind, Result};

use super::Permit;

/// Snapshot of permits in use by a simple limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in super::super) struct LimiterStats {
    /// Permits currently in use.
    pub(in super::super) in_use: u32,
}

/// Limits concurrent operations with FIFO queuing and a configurable wait timeout.
#[derive(Clone, Debug)]
pub(in super::super) struct Limiter {
    semaphore: Arc<Semaphore>,
    capacity: u32,
    timeout: Duration,
}

impl Limiter {
    /// Creates a FIFO limiter with the given capacity and wait timeout.
    pub(in super::super) fn new(capacity: NonZeroU32, timeout: Duration) -> Self {
        let capacity = capacity.get();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
            timeout,
        }
    }

    /// Acquires capacity within the configured timeout, preserving FIFO order.
    ///
    /// Returns [`ErrorKind::AtCapacity`] on timeout. A zero timeout rejects immediately when full.
    pub(in super::super) async fn acquire(&self) -> Result<Permit> {
        let permit = if self.timeout.is_zero() {
            self.semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| ErrorKind::AtCapacity)?
        } else {
            tokio::time::timeout(self.timeout, self.semaphore.clone().acquire_owned())
                .await
                .map_err(|_| ErrorKind::AtCapacity)?
                .map_err(|_| ErrorKind::AtCapacity)?
        };
        Ok(Permit { _permit: permit })
    }

    /// Returns the current number of permits in use.
    pub(in super::super) fn stats(&self) -> LimiterStats {
        LimiterStats {
            in_use: self.capacity - self.semaphore.available_permits() as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_limiter_rejects_until_release() {
        let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
        let permit = limiter.acquire().await.unwrap();
        assert_eq!(
            limiter.clone().acquire().await.unwrap_err().kind(),
            ErrorKind::AtCapacity
        );
        assert_eq!(limiter.stats().in_use, 1);
        drop(permit);
        assert_eq!(limiter.stats().in_use, 0);
        drop(limiter.acquire().await.unwrap());
    }
}
