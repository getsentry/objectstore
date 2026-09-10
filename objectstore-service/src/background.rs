//! Scheduling and execution of background service jobs.
//!
//! This module currently manages expiry renewals triggered by reads of TTI
//! objects. Renewals are scheduled without blocking the read, deduplicated by
//! object ID, and placed in a bounded queue. A worker executes accepted jobs
//! using the service's bulk concurrency budget, while shutdown can wait for all
//! pending renewals to finish. See [`RenewalScheduler`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::{Notify, mpsc};

use crate::backend::common::Backend;
use crate::concurrency::ConcurrencyLimiter;
use crate::error::ErrorKind;
use crate::id::ObjectId;

const EMITTER_INTERVAL: Duration = Duration::from_secs(1);

/// Measures the number of items currently waiting in the queue.
#[derive(Debug, Default)]
struct QueueCounter {
    value: AtomicUsize,
}

impl QueueCounter {
    fn enter(self: &Arc<Self>) -> QueueCounterGuard {
        self.value.fetch_add(1, Ordering::Relaxed);
        QueueCounterGuard(Arc::clone(self))
    }

    fn get(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// Decrements the queue counter when a queued renewal is removed.
#[derive(Debug)]
struct QueueCounterGuard(Arc<QueueCounter>);

impl Drop for QueueCounterGuard {
    fn drop(&mut self) {
        self.0.value.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Removes an object from the pending set and notifies shutdown when dropped.
#[derive(Debug)]
struct PendingGuard {
    id: ObjectId,
    pending: Arc<papaya::HashSet<ObjectId>>,
    pending_notify: Arc<Notify>,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.pin().remove(&self.id);
        self.pending_notify.notify_waiters();
    }
}

/// A queued expiry-renewal request and its lifecycle guards.
#[derive(Debug)]
struct Renewal {
    id: ObjectId,
    expire_at: SystemTime,
    pending: PendingGuard,
    queued: QueueCounterGuard,
}

/// Shared state used by renewal scheduler clones.
#[derive(Debug)]
struct RenewalSchedulerInner {
    sender: mpsc::Sender<Renewal>,
    pending: Arc<papaya::HashSet<ObjectId>>,
    pending_notify: Arc<Notify>,
    queue_counter: Arc<QueueCounter>,
}

/// Consumes queued renewals and starts their backend operations.
///
/// Created by [`RenewalScheduler`] to process queued expiry-renewal requests.
#[derive(Debug)]
struct RenewalWorker {
    receiver: mpsc::Receiver<Renewal>,
    backend: Arc<dyn Backend>,
    concurrency: ConcurrencyLimiter,
}

impl RenewalWorker {
    async fn run(mut self) {
        while let Some(renewal) = self.receiver.recv().await {
            self.process(renewal).await;
        }
    }

    async fn process(&self, renewal: Renewal) {
        let Renewal {
            id,
            expire_at,
            pending,
            queued,
        } = renewal;

        drop(queued);

        if self.concurrency.total_permits() == 0 {
            objectstore_metrics::count!("service.expiry_renewal", outcome = "dropped");
            return;
        }

        let permit = loop {
            match self.concurrency.acquire_bulk().await {
                Ok(permit) => break permit,
                Err(error) if error.kind() == ErrorKind::AtCapacity => continue,
                Err(error) => {
                    objectstore_metrics::count!(
                        "service.expiry_renewal",
                        outcome = "dropped",
                        reason = "guard"
                    );
                    objectstore_log::error!(!!&error, "failed to acquire expiry renewal permit");
                    return;
                }
            }
        };

        let backend = Arc::clone(&self.backend);
        // This ID-only operation can run after a replacement write. Its
        // original read timestamp and requested deadline may therefore
        // extend a replacement TTL/TTI object. Process exit can also lose
        // this opportunistic renewal; a later read may retry it.
        crate::concurrency::spawn_metered("set_expiry", (pending, permit), async move {
            match backend.set_expiry(&id, expire_at).await {
                Ok(true) => {
                    objectstore_metrics::count!("service.expiry_renewal", outcome = "applied");
                    Ok(())
                }
                Ok(false) => {
                    objectstore_metrics::count!(
                        "service.expiry_renewal",
                        outcome = "skipped",
                        reason = "conflict"
                    );
                    Ok(())
                }
                Err(error) => {
                    objectstore_metrics::count!("service.expiry_renewal", outcome = "failed");
                    Err(error)
                }
            }
        });
    }
}

/// Schedules bounded, deduplicated expiry renewals in the background.
#[derive(Debug)]
pub struct RenewalScheduler {
    inner: Arc<RenewalSchedulerInner>,
    worker: Option<RenewalWorker>,
}

impl Clone for RenewalScheduler {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            worker: None,
        }
    }
}

impl RenewalScheduler {
    /// Creates a renewal scheduler for `backend`.
    ///
    /// `concurrency` supplies the bulk permits used to run renewals alongside
    /// foreground service operations. `capacity` bounds the number of renewals
    /// waiting in the background queue; values below one are clamped to one.
    pub fn new(
        backend: Arc<dyn Backend>,
        concurrency: ConcurrencyLimiter,
        capacity: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));

        let inner = Arc::new(RenewalSchedulerInner {
            sender,
            pending: Arc::new(papaya::HashSet::new()),
            pending_notify: Arc::new(Notify::new()),
            queue_counter: Arc::new(QueueCounter::default()),
        });
        let worker = Some(RenewalWorker {
            receiver,
            backend,
            concurrency,
        });

        Self { inner, worker }
    }

    /// Starts the renewal worker if this instance owns the queue receiver.
    ///
    /// Repeated calls and calls on scheduler clones have no effect.
    pub fn start(&mut self) {
        if let Some(worker) = self.worker.take() {
            tokio::spawn(worker.run());
        }
    }

    /// Schedules an expiry renewal without waiting for queue or execution capacity.
    ///
    /// [`Self::start`] must be called before scheduling renewals.
    pub fn schedule(&self, id: ObjectId, expire_at: SystemTime) {
        let pending = Arc::clone(&self.inner.pending);
        if !pending.pin().insert(id.clone()) {
            objectstore_metrics::count!(
                "service.expiry_renewal",
                outcome = "skipped",
                reason = "duplicate"
            );
            return;
        }

        let pending = PendingGuard {
            id: id.clone(),
            pending,
            pending_notify: Arc::clone(&self.inner.pending_notify),
        };

        let Ok(permit) = self.inner.sender.try_reserve() else {
            objectstore_metrics::count!(
                "service.expiry_renewal",
                outcome = "dropped",
                reason = "capacity"
            );
            return;
        };

        permit.send(Renewal {
            id,
            expire_at,
            pending,
            queued: self.inner.queue_counter.enter(),
        });
    }

    /// Replaces the concurrency limiter used when this instance starts the worker.
    ///
    /// Has no effect if this instance does not own the worker anymore.
    pub fn set_concurrency(&mut self, concurrency: ConcurrencyLimiter) {
        if let Some(worker) = &mut self.worker {
            worker.concurrency = concurrency;
        }
    }

    /// Replaces the queue capacity before the worker starts.
    ///
    /// Has no effect if this instance does not own the worker anymore.
    pub fn set_capacity(&mut self, capacity: usize) {
        let Some(worker) = self.worker.take() else {
            return;
        };

        let (sender, receiver) = mpsc::channel(capacity.max(1));
        self.inner = Arc::new(RenewalSchedulerInner {
            sender,
            pending: Arc::clone(&self.inner.pending),
            pending_notify: Arc::clone(&self.inner.pending_notify),
            queue_counter: Arc::clone(&self.inner.queue_counter),
        });
        self.worker = Some(RenewalWorker { receiver, ..worker });
    }

    /// Returns the number of renewals waiting in the queue.
    pub fn queued(&self) -> usize {
        self.inner.queue_counter.get()
    }

    /// Periodically calls `emit` with the number of queued renewals.
    ///
    /// This future runs forever and is intended to be spawned alongside the
    /// renewal worker.
    pub async fn run_emitter<F, Fut>(&self, mut emit: F)
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ticker = tokio::time::interval(EMITTER_INTERVAL);
        loop {
            ticker.tick().await;
            emit(self.queued()).await;
        }
    }

    /// Returns the number of deduplicated renewals queued or executing.
    #[cfg(test)]
    pub fn pending(&self) -> usize {
        self.inner.pending.len()
    }

    /// Waits until every accepted renewal has finished processing.
    ///
    /// No new renewals may be scheduled concurrently with this method.
    pub async fn join(&self) {
        loop {
            // Register before checking `pending` so the final removal cannot
            // notify between the check and the await.
            let notified = self.inner.pending_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.inner.pending.is_empty() {
                break;
            }

            notified.await;
        }
    }
}
