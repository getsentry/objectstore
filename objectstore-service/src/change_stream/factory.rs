//! Constructs the [`ChangeStream`] implementation(s) for a backend based on the available
//! service-wide sink config and per-backend stream config.

use std::fmt;
use std::sync::Arc;

#[cfg(feature = "storage-cogs")]
use objectstore_inventory_tracker::SharedProducer;
#[cfg(feature = "storage-cogs")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "storage-cogs")]
use super::CostTrackerStream;
use super::{ChangeStream, CostTrackerStreamConfig, NoopStream};

/// Where every backend's change stream records are carried for cost tracking.
///
/// Service-wide: a transport owns connections and a send queue worth sharing.
#[cfg(feature = "storage-cogs")]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CostTrackerConfig {
    /// Reports onto a Kafka topic.
    Kafka(objectstore_inventory_tracker::kafka::KafkaConfig),
}

/// Builds the [`ChangeStream`] impl(s) a backend reports to.
///
/// Without a usable transport every backend gets a [`NoopStream`].
#[derive(Clone, Default)]
pub struct ChangeStreamFactory {
    #[cfg(feature = "storage-cogs")]
    producer: Option<SharedProducer>,
}

impl ChangeStreamFactory {
    /// Builds the transport described by `config`.
    ///
    /// Fails open: an unusable transport is logged, not fatal.
    #[cfg(feature = "storage-cogs")]
    pub fn new(config: &CostTrackerConfig) -> Self {
        let CostTrackerConfig::Kafka(kafka) = config;
        Self {
            producer: build_kafka_producer(kafka),
        }
    }

    /// Builds the stream `config` asks for, or a [`NoopStream`] if it cannot be built.
    #[cfg(feature = "storage-cogs")]
    pub fn build(&self, config: Option<&CostTrackerStreamConfig>) -> Arc<dyn ChangeStream> {
        match (config, self.producer.clone()) {
            (Some(config), Some(producer)) => Arc::new(CostTrackerStream::new(producer, config)),
            (None, None) => Arc::new(NoopStream),
            (c, p) => {
                objectstore_log::warn!(
                    stream_configured = c.is_some(),
                    producer_configured = p.is_some(),
                    "incomplete change stream configuration, returning NoopStream",
                );
                Arc::new(NoopStream)
            }
        }
    }

    /// Reporting is not compiled in, so every backend reports nothing.
    #[cfg(not(feature = "storage-cogs"))]
    pub fn build(&self, _config: Option<&CostTrackerStreamConfig>) -> Arc<dyn ChangeStream> {
        Arc::new(NoopStream)
    }
}

impl fmt::Debug for ChangeStreamFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("ChangeStreamFactory");
        #[cfg(feature = "storage-cogs")]
        f.field("producer", &self.producer.is_some());
        f.finish()
    }
}

/// Creates the shared Kafka producer, or logs why there will be no reporting.
#[cfg(feature = "storage-cogs")]
fn build_kafka_producer(
    config: &objectstore_inventory_tracker::kafka::KafkaConfig,
) -> Option<SharedProducer> {
    use objectstore_inventory_tracker::Producer as _;
    use objectstore_inventory_tracker::kafka::KafkaProducer;

    // Delivery is asynchronous, so an accepted record can still fail to arrive. Without
    // this those show up only as a shortfall in the downstream data.
    let on_delivery_failure = Box::new(|_: &_| {
        objectstore_metrics::count!("cost_tracker.undelivered" += 1);
    });

    match KafkaProducer::try_new(config.clone(), Some(on_delivery_failure)) {
        Ok(producer) => Some(producer.shared()),
        Err(error) => {
            objectstore_log::error!(
                !!&error,
                "failed to create the change stream kafka producer; \
                 backends with a change stream will report nothing"
            );
            None
        }
    }
}
