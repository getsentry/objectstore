//! Simple and priority-based FIFO concurrency limiters.

use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use tokio::sync::OwnedSemaphorePermit;

use crate::stream::PayloadStream;

mod priority;
mod simple;

pub(super) use priority::{Priority, PriorityLimiter, PriorityLimiterStats};
pub(super) use simple::{Limiter, LimiterStats};

const METRICS_EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Passes a usage snapshot to `emit` immediately and once per second thereafter.
///
/// The caller supplies the snapshot and can spawn this future as a background task.
pub(super) async fn run_emitter<S, F, Fut>(mut snapshot: impl FnMut() -> S, mut emit: F)
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut ticker = tokio::time::interval(METRICS_EMITTER_INTERVAL);
    loop {
        ticker.tick().await;
        emit(snapshot()).await;
    }
}

/// Owned permit, released on drop or by a guarded stream.
#[derive(Debug)]
pub(super) struct Permit {
    _permit: OwnedSemaphorePermit,
}

impl Permit {
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
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test(start_paused = true)]
    async fn emitter_reports_current_limiter_usage() {
        let priority = PriorityLimiter::new(NonZeroU32::MIN, TIMEOUT);
        let simple = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
        let priority_permit = priority.acquire(Priority::Normal).await.unwrap();
        let simple_permit = simple.acquire().await.unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let emitter = run_emitter(
            || (priority.stats(), simple.stats()),
            |stats| {
                sender.send(stats).unwrap();
                std::future::ready(())
            },
        );
        tokio::pin!(emitter);
        assert!(poll!(&mut emitter).is_pending());
        let busy = receiver.try_recv().unwrap();
        assert_eq!(busy.0.in_use, 1);
        assert_eq!(busy.1.in_use, 1);

        drop(priority_permit);
        drop(simple_permit);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut emitter).is_pending());
        let idle = receiver.try_recv().unwrap();
        assert_eq!(
            idle,
            (PriorityLimiterStats::default(), LimiterStats::default())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_retains_then_releases_at_terminal_item() {
        for error in [false, true] {
            let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
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
            let mut guarded = limiter.acquire().await.unwrap().guard_stream(source);
            assert_eq!(
                guarded.next().await.unwrap().unwrap(),
                Bytes::from_static(b"data")
            );
            assert_eq!(limiter.stats().in_use, 1);
            assert!(poll!(guarded.next()).is_pending());
            assert_eq!(limiter.stats().in_use, 1);
            let terminal = guarded.next().await;
            if error {
                assert_eq!(terminal.unwrap().unwrap_err().to_string(), "original");
            } else {
                assert!(terminal.is_none());
            }
            assert_eq!(limiter.stats().in_use, 0);
            assert!(guarded.next().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stream_drop_releases_after_stream() {
        struct PendingStream {
            dropped: Arc<AtomicBool>,
            limiter: Limiter,
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
                assert_eq!(self.limiter.stats().in_use, 1);
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        let limiter = Limiter::new(NonZeroU32::MIN, Duration::ZERO);
        let dropped = Arc::new(AtomicBool::new(false));
        let source = PendingStream {
            dropped: dropped.clone(),
            limiter: limiter.clone(),
        }
        .boxed();
        let mut guarded = limiter.acquire().await.unwrap().guard_stream(source);
        assert!(poll!(guarded.next()).is_pending());
        drop(guarded);
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(limiter.stats().in_use, 0);
    }
}
