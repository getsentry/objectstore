//! Shared capacity and admission rules for concurrency limiters.
//!
//! Pools own capacity; handles share a pool but can have separate queue limits,
//! timeouts, and additional budgets. FIFO pools need no priority state. Priority
//! pools admit high-priority waiters before normal waiters, with FIFO within
//! each lane. Sustained priority traffic can starve normal traffic.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use super::ConcurrencyPermit;
use crate::error::{ErrorKind, Result};

/// Scheduling discipline of a shared execution pool.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Scheduling {
    Fifo,
    #[allow(dead_code, reason = "Used by tier admission when it is integrated")]
    Priority,
}

/// Admission lane within a priority pool; FIFO pools treat both equally.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Priority {
    Normal,
    #[allow(dead_code, reason = "Used by tier admission when it is integrated")]
    High,
}

/// Maximum number of suspended acquisitions for one handle.
#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueLimit {
    Bounded(u32),
    Unbounded,
}

/// Queue and timeout settings; zero timeout permits only an immediate attempt.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WaitConfig {
    pub queue: QueueLimit,
    pub timeout: Duration,
}

#[derive(Debug)]
struct PriorityState {
    lane: Mutex<()>,
    pending: AtomicUsize,
    drained: Notify,
}

/// Shared execution capacity and scheduling state.
#[derive(Clone, Debug)]
pub(crate) struct PermitPool {
    semaphore: Arc<Semaphore>,
    capacity: u32,
    priority: Option<Arc<PriorityState>>,
    released: Arc<Notify>,
}

impl PermitPool {
    pub fn new(capacity: u32, scheduling: Scheduling) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
            priority: matches!(scheduling, Scheduling::Priority).then(|| {
                Arc::new(PriorityState {
                    lane: Mutex::new(()),
                    pending: AtomicUsize::new(0),
                    drained: Notify::new(),
                })
            }),
            released: Arc::new(Notify::new()),
        }
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }
    pub fn available(&self) -> u32 {
        self.semaphore.available_permits() as u32
    }
    pub fn in_use(&self) -> u32 {
        self.capacity - self.available()
    }

    fn try_acquire(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        let _lane = match (&self.priority, priority) {
            (Some(state), Priority::Normal) => {
                let lane = state.lane.try_lock().map_err(|_| ErrorKind::AtCapacity)?;
                if state.pending.load(Ordering::Acquire) != 0 {
                    return Err(ErrorKind::AtCapacity.into());
                }
                Some(lane)
            }
            _ => None,
        };
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorKind::AtCapacity)?;
        if let (Some(state), Priority::Normal) = (&self.priority, priority)
            && state.pending.load(Ordering::Acquire) != 0
        {
            drop(permit);
            return Err(ErrorKind::AtCapacity.into());
        }
        Ok(permit)
    }

    async fn acquire(&self, priority: Priority) -> Result<OwnedSemaphorePermit> {
        let Some(state) = &self.priority else {
            return self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ErrorKind::AtCapacity.into());
        };
        if matches!(priority, Priority::High) {
            state.pending.fetch_add(1, Ordering::AcqRel);
            let _pending = PendingPriority(state);
            return self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ErrorKind::AtCapacity.into());
        }
        let _lane = state.lane.lock().await;
        loop {
            loop {
                // notify_waiters reaches futures created before the notification,
                // including futures that have not yet been polled.
                let drained = state.drained.notified();
                if state.pending.load(Ordering::Acquire) == 0 {
                    break;
                }
                drained.await;
            }
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ErrorKind::AtCapacity)?;
            if state.pending.load(Ordering::Acquire) == 0 {
                return Ok(permit);
            }
            drop(permit);
        }
    }

    /// Observes an empty pool; does not close admission or drain queued callers.
    pub async fn wait_all(&self) {
        loop {
            let notified = self.released.notified();
            if self.in_use() == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct PendingPriority<'a>(&'a PriorityState);
impl Drop for PendingPriority<'_> {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

/// Admission rules for one group of callers sharing a pool.
#[derive(Clone, Debug)]
pub(crate) struct Limiter {
    pool: PermitPool,
    priority: Priority,
    wait: WaitConfig,
    queue: Option<Arc<Semaphore>>,
    waiting: Arc<AtomicUsize>,
    budget: Option<PermitPool>,
}

impl Limiter {
    pub fn new(pool: PermitPool, priority: Priority, wait: WaitConfig) -> Self {
        Self {
            pool,
            priority,
            wait,
            queue: match wait.queue {
                QueueLimit::Bounded(size) => Some(Arc::new(Semaphore::new(size as usize))),
                QueueLimit::Unbounded => None,
            },
            waiting: Arc::new(AtomicUsize::new(0)),
            budget: None,
        }
    }

    /// Sets an additional capacity requirement before cloning this handle.
    pub fn with_budget(mut self, capacity: u32) -> Self {
        self.budget = Some(PermitPool::new(capacity, Scheduling::Fifo));
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.wait.timeout = timeout;
        self
    }

    pub fn timeout(&self) -> Duration {
        self.wait.timeout
    }
    pub fn pool(&self) -> &PermitPool {
        &self.pool
    }
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }
    pub fn budget_in_use(&self) -> u32 {
        self.budget.as_ref().map_or(0, PermitPool::in_use)
    }
    pub fn budget_capacity(&self) -> u32 {
        self.budget.as_ref().map_or(0, PermitPool::capacity)
    }
    pub fn queue_capacity(&self) -> u32 {
        match self.wait.queue {
            QueueLimit::Bounded(size) => size,
            QueueLimit::Unbounded => 0,
        }
    }

    pub fn try_acquire(&self) -> Result<ConcurrencyPermit> {
        let result = (|| {
            let budget = self
                .budget
                .as_ref()
                .map(|pool| pool.try_acquire(Priority::Normal))
                .transpose()?;
            let task = self.pool.try_acquire(self.priority)?;
            Ok(self.permit(task, budget))
        })();
        if result.is_err() {
            // A priority race can return a briefly reserved main-pool permit.
            self.pool.released.notify_waiters();
        }
        result
    }

    fn permit(
        &self,
        task: OwnedSemaphorePermit,
        budget: Option<OwnedSemaphorePermit>,
    ) -> ConcurrencyPermit {
        ConcurrencyPermit {
            task_permit: Some(task),
            bulk_permit: budget,
            released: self.pool.released.clone(),
        }
    }

    pub async fn acquire(&self) -> Result<ConcurrencyPermit> {
        if self.pool.capacity == 0 {
            return Err(ErrorKind::AtCapacity.into());
        }
        if let Ok(permit) = self.try_acquire() {
            return Ok(permit);
        }
        if self.wait.timeout.is_zero() {
            return Err(ErrorKind::AtCapacity.into());
        }
        let _ticket = self
            .queue
            .as_ref()
            .map(|queue| queue.clone().try_acquire_owned())
            .transpose()
            .map_err(|_| ErrorKind::AtCapacity)?;
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let _waiting = Waiting(&self.waiting);
        // Notify after the acquisition future has dropped its reservations on cancellation.
        let _released = Released(&self.pool.released);
        tokio::time::timeout(self.wait.timeout, async {
            let budget = match &self.budget {
                Some(pool) => Some(pool.acquire(Priority::Normal).await?),
                None => None,
            };
            let task = self.pool.acquire(self.priority).await?;
            Ok(self.permit(task, budget))
        })
        .await
        .map_err(|_| ErrorKind::AtCapacity)?
    }
}

struct Waiting<'a>(&'a AtomicUsize);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Released<'a>(&'a Notify);
impl Drop for Released<'_> {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

/// Emits a caller-defined snapshot immediately and once per second thereafter.
pub(crate) async fn run_emitter<S, Snapshot, Emit, Fut>(mut snapshot: Snapshot, mut emit: Emit)
where
    Snapshot: FnMut() -> S,
    Emit: FnMut(S) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        emit(snapshot()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::poll;

    fn handle(pool: &PermitPool, priority: Priority, queue: QueueLimit) -> Limiter {
        Limiter::new(
            pool.clone(),
            priority,
            WaitConfig {
                queue,
                timeout: Duration::from_secs(1),
            },
        )
    }

    #[tokio::test(start_paused = true)]
    async fn handles_share_capacity_but_have_separate_queues() {
        let pool = PermitPool::new(1, Scheduling::Fifo);
        let bounded = handle(&pool, Priority::Normal, QueueLimit::Bounded(1));
        let unbounded = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let held = bounded.acquire().await.unwrap();
        let mut first = Box::pin(bounded.acquire());
        let mut second = Box::pin(unbounded.acquire());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert_eq!(bounded.waiting(), 1);
        assert_eq!(unbounded.waiting(), 1);
        assert!(bounded.clone().acquire().await.is_err());
        drop(held);
        drop(first.await.unwrap());
        drop(second.await.unwrap());
        assert_eq!(pool.in_use(), 0);
        assert_eq!(bounded.waiting(), 0);
        assert_eq!(unbounded.waiting(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn priority_and_fifo_within_each_lane() {
        let pool = PermitPool::new(1, Scheduling::Priority);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let high = handle(&pool, Priority::High, QueueLimit::Unbounded);
        let held = normal.acquire().await.unwrap();
        let mut n1 = Box::pin(normal.acquire());
        let mut n2 = Box::pin(normal.acquire());
        let mut h1 = Box::pin(high.acquire());
        let mut h2 = Box::pin(high.acquire());
        assert!(poll!(&mut n1).is_pending());
        assert!(poll!(&mut n2).is_pending());
        assert!(poll!(&mut h1).is_pending());
        assert!(poll!(&mut h2).is_pending());
        drop(held);
        assert!(normal.try_acquire().is_err());
        assert!(poll!(&mut n1).is_pending());
        assert!(poll!(&mut h2).is_pending());
        drop(h1.await.unwrap());
        drop(h2.await.unwrap());
        assert!(poll!(&mut n2).is_pending());
        drop(n1.await.unwrap());
        drop(n2.await.unwrap());
        assert_eq!(pool.in_use(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_priority_after_assignment_wakes_normal() {
        let pool = PermitPool::new(1, Scheduling::Priority);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let high = handle(&pool, Priority::High, QueueLimit::Unbounded);
        let held = normal.acquire().await.unwrap();
        let mut priority = Box::pin(high.acquire());
        let mut waiter = Box::pin(normal.acquire());
        assert!(poll!(&mut priority).is_pending());
        assert!(poll!(&mut waiter).is_pending());
        drop(held);
        drop(priority);
        drop(waiter.await.unwrap());
        assert_eq!(pool.in_use(), 0);
        assert_eq!(high.waiting(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn priority_timeout_releases_lane_and_pending_count() {
        let pool = PermitPool::new(1, Scheduling::Priority);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let high = handle(&pool, Priority::High, QueueLimit::Unbounded);
        let held = normal.acquire().await.unwrap();
        let mut priority = Box::pin(high.acquire());
        assert!(poll!(&mut priority).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(priority.await.is_err());
        drop(held);
        drop(normal.try_acquire().unwrap());
        assert_eq!(pool.in_use(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_normal_after_assignment_releases_lane_and_capacity() {
        let pool = PermitPool::new(1, Scheduling::Priority);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let held = normal.acquire().await.unwrap();
        let mut first = Box::pin(normal.acquire());
        let mut second = Box::pin(normal.acquire());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        drop(held);
        let mut drained = Box::pin(pool.wait_all());
        assert!(poll!(&mut drained).is_pending());
        drop(first);
        drop(second);
        assert!(poll!(&mut drained).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn budget_is_shared_by_clones_and_timeout_cleans_reservations() {
        let pool = PermitPool::new(2, Scheduling::Fifo);
        let bulk = handle(&pool, Priority::Normal, QueueLimit::Unbounded).with_budget(1);
        let clone = bulk.clone();
        let held = bulk.acquire().await.unwrap();
        assert!(clone.try_acquire().is_err());
        let mut waiter = Box::pin(clone.acquire());
        assert!(poll!(&mut waiter).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(waiter.await.is_err());
        assert_eq!(bulk.budget_in_use(), 1);
        assert_eq!(bulk.waiting(), 0);
        drop(held);
        drop(clone.try_acquire().unwrap());
    }
    #[tokio::test(start_paused = true)]
    async fn one_deadline_covers_budget_and_main_pool() {
        let pool = PermitPool::new(1, Scheduling::Fifo);
        let bulk = handle(&pool, Priority::Normal, QueueLimit::Unbounded).with_budget(1);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let held = bulk.acquire().await.unwrap();
        let mut waiting = Box::pin(bulk.acquire());
        assert!(poll!(&mut waiting).is_pending());
        tokio::time::advance(Duration::from_millis(600)).await;
        drop(held);
        let held = normal.try_acquire().unwrap();
        assert!(poll!(&mut waiting).is_pending());
        assert_eq!(bulk.budget_in_use(), 1);
        tokio::time::advance(Duration::from_millis(400)).await;
        assert!(waiting.await.is_err());
        assert_eq!(bulk.budget_in_use(), 0);
        assert_eq!(pool.in_use(), 1);
        drop(held);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_priority_and_cancellation_return_all_capacity() {
        let pool = PermitPool::new(3, Scheduling::Priority);
        let normal = handle(&pool, Priority::Normal, QueueLimit::Unbounded);
        let high = handle(&pool, Priority::High, QueueLimit::Unbounded);
        let mut tasks = Vec::new();
        for i in 0..50 {
            let limiter = if i % 2 == 0 {
                normal.clone()
            } else {
                high.clone()
            };
            tasks.push(tokio::spawn(async move {
                for round in 0..20 {
                    let mut acquire = Box::pin(limiter.acquire());
                    if round % 3 == 0 {
                        // Cancel after polling, including races with permit assignment.
                        let _ = poll!(&mut acquire);
                    } else {
                        let permit = acquire.await.unwrap();
                        tokio::task::yield_now().await;
                        drop(permit);
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(pool.in_use(), 0);
        assert_eq!(normal.waiting(), 0);
        assert_eq!(high.waiting(), 0);
        let state = pool.priority.as_ref().unwrap();
        assert_eq!(state.pending.load(Ordering::Acquire), 0);
        drop(normal.try_acquire().unwrap());
    }
}
