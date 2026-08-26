//! Helpers for asserting on emitted records without a broker.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{InventoryRecord, Producer};

/// A [`Producer`] that records everything it is given, for assertions in tests.
///
/// Cloning shares the same buffer, so a clone can be handed to an
/// [`InventoryTracker`](crate::InventoryTracker) while the original is used to read
/// back what was emitted.
#[derive(Clone, Debug, Default)]
pub struct DummyProducer {
    sent: Arc<Mutex<Vec<SentMessage>>>,
}

/// A message as handed to [`Producer::send`], as `(key, payload)`.
pub type SentMessage = (Vec<u8>, Vec<u8>);

impl DummyProducer {
    /// Returns the raw messages sent so far, in order.
    pub fn raw(&self) -> Vec<SentMessage> {
        self.sent.lock().unwrap().clone()
    }

    /// Returns the records sent so far, deserialized, in order.
    pub fn records(&self) -> Vec<InventoryRecord> {
        self.raw()
            .iter()
            .map(|(_, payload)| serde_json::from_slice(payload).unwrap())
            .collect()
    }

    /// Discards everything sent so far.
    pub fn clear(&self) {
        self.sent.lock().unwrap().clear();
    }
}

impl Producer for DummyProducer {
    type Error = std::convert::Infallible;

    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error> {
        self.sent.lock().unwrap().push((key.to_vec(), payload));
        Ok(())
    }

    // Nothing is ever queued, so there is nothing to wait for.
    fn join_blocking(&self, _timeout: Duration) -> Result<(), Self::Error> {
        Ok(())
    }
}
