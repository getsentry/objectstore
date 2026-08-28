//! The transport abstraction records are handed to.
//!
//! The abstraction is very thin and narrow, but it allows us to avoid needing to add a
//! "build `librdkafka` with `cmake`" step to tests or local development builds.
//!
//! [`Producer`] carries an associated error type, so it is not usable as `dyn Producer`.
//! See [`SharedProducer`] for handing one producer to several trackers.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Sends serialized inventory records somewhere durable.
pub trait Producer {
    /// What can go wrong when sending.
    type Error;

    /// Enqueues one record.
    ///
    /// `key` controls which partition receives the message.
    ///
    /// `Ok()` does not necessarily mean the message will be sent successfully. It just
    /// means the message has been enqueued.
    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error>;

    /// Blocks until enqueued records have been delivered, or `timeout` elapses.
    ///
    /// Prefer [`join`](Self::join) from async code.
    fn join_blocking(&self, timeout: Duration) -> Result<(), Self::Error>;

    /// Waits for enqueued records to be delivered, or until `timeout` elapses.
    ///
    /// Runs [`join_blocking`](Self::join_blocking) on a blocking thread. Returns a future
    /// rather than being `async fn` so it does not borrow `self`, which a `dyn` caller's
    /// `async_trait` boxing requires.
    fn join(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<Self>
    where
        Self: Sized + Clone + Send + Sync + 'static,
        Self::Error: Send + 'static,
    {
        let producer = self.clone();
        async move {
            // spawn_blocking panics outside a runtime, which would lose the records.
            if tokio::runtime::Handle::try_current().is_err() {
                return producer.join_blocking(timeout);
            }

            match tokio::task::spawn_blocking(move || producer.join_blocking(timeout)).await {
                Ok(result) => result,
                // Cancelled or panicked; nothing left to report to during shutdown.
                Err(_) => Ok(()),
            }
        }
    }

    /// Erases this producer's transport and error type, so one producer can serve
    /// several trackers.
    ///
    /// ```
    /// use objectstore_inventory_tracker::{InventoryTracker, NoopProducer, Producer};
    ///
    /// let producer = NoopProducer.shared();
    /// let tracker = InventoryTracker::new(producer.clone(), "my_gcs_bucket", 1.0);
    /// ```
    fn shared(self) -> SharedProducer
    where
        Self: Sized + Send + Sync + 'static,
        Self::Error: std::error::Error + Send + Sync + 'static,
    {
        Arc::new(BoxErrors(self))
    }
}

impl<P: Producer + ?Sized> Producer for Box<P> {
    type Error = P::Error;

    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error> {
        (**self).send(key, payload)
    }

    fn join_blocking(&self, timeout: Duration) -> Result<(), Self::Error> {
        (**self).join_blocking(timeout)
    }
}

impl<P: Producer + ?Sized> Producer for Arc<P> {
    type Error = P::Error;

    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error> {
        (**self).send(key, payload)
    }

    fn join_blocking(&self, timeout: Duration) -> Result<(), Self::Error> {
        (**self).join_blocking(timeout)
    }
}

/// The error a [`SharedProducer`] reports.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A [`Producer`] whose transport and error type have both been erased.
///
/// Built with [`Producer::shared`]. The `Arc` is what keeps [`Producer::join`] usable:
/// it needs a `Clone + 'static` value for the blocking thread.
pub type SharedProducer = Arc<dyn Producer<Error = BoxError> + Send + Sync>;

/// Adapts a [`Producer`] to report [`BoxError`], so it can become a [`SharedProducer`].
struct BoxErrors<P>(P);

impl<P: Producer> Producer for BoxErrors<P>
where
    P::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = BoxError;

    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error> {
        self.0.send(key, payload).map_err(Into::into)
    }

    fn join_blocking(&self, timeout: Duration) -> Result<(), Self::Error> {
        self.0.join_blocking(timeout).map_err(Into::into)
    }
}

/// A [`Producer`] that discards everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProducer;

impl Producer for NoopProducer {
    type Error = std::convert::Infallible;

    fn send(&self, _key: &[u8], _payload: Vec<u8>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn join_blocking(&self, _timeout: Duration) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::DummyProducer;

    #[tokio::test]
    async fn an_erased_producer_still_reaches_its_transport() {
        let dummy = DummyProducer::default();
        let producer = dummy.clone().shared();

        producer.send(b"key", b"payload".to_vec()).unwrap();
        producer.join(Duration::from_secs(1)).await.unwrap();

        assert_eq!(dummy.raw(), [(b"key".to_vec(), b"payload".to_vec())]);
    }

    #[test]
    fn joining_outside_a_runtime_still_drains() {
        let dummy = DummyProducer::default();
        let producer = dummy.clone().shared();

        producer.send(b"key", b"payload".to_vec()).unwrap();
        futures::executor::block_on(producer.join(Duration::from_secs(1))).unwrap();

        assert_eq!(dummy.raw(), [(b"key".to_vec(), b"payload".to_vec())]);
    }

    #[test]
    fn one_erased_producer_serves_many_trackers() {
        let dummy = DummyProducer::default();
        let producer = dummy.clone().shared();

        for resource in ["bigtable_objectstore", "gcs_objectstore"] {
            let tracker = crate::InventoryTracker::new(producer.clone(), resource, 1.0);
            tracker
                .delete(
                    "attachments/objects/abc",
                    "attachments",
                    std::time::SystemTime::now(),
                )
                .unwrap();
        }

        let resources: Vec<_> = dummy
            .records()
            .into_iter()
            .map(|record| record.shared_resource_id)
            .collect();
        assert_eq!(resources, ["bigtable_objectstore", "gcs_objectstore"]);
    }
}
