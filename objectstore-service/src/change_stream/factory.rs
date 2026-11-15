//! Constructs the [`ChangeStream`] implementation(s) for a backend based on the available
//! service-wide sink config and per-backend stream config.

use std::fmt;
use std::sync::Arc;

#[cfg(feature = "storage-cogs")]
use objectstore_inventory_tracker::SharedProducer;
#[cfg(all(test, feature = "storage-cogs"))]
use objectstore_inventory_tracker::test_utils;
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

/// Builds the [`ChangeStream`] impl a backend reports to.
///
/// A backend receives at most one of cost tracking and garbage collection. The service
/// rejects a configuration that enables both for the same backend. Without either, the
/// backend gets a [`NoopStream`].
#[derive(Clone, Default)]
pub struct ChangeStreamFactory {
    #[cfg(feature = "storage-cogs")]
    producer: Option<SharedProducer>,
    gc: Option<Arc<dyn ChangeStream>>,
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
            gc: None,
        }
    }

    /// Adds the stream that receives this backend's changes for garbage collection.
    ///
    /// The service calls this only for backends that need a garbage collector. A backend
    /// must not also have cost tracking configured; service startup rejects that.
    pub fn with_gc(mut self, gc: Arc<dyn ChangeStream>) -> Self {
        self.gc = Some(gc);
        self
    }

    /// Builds the stream `config` asks for, or a [`NoopStream`] if there is none.
    ///
    /// A garbage-collector stream takes precedence over cost tracking. The service rejects
    /// a configuration that enables both for the same backend, so the combination is only
    /// reachable when a backend is constructed directly.
    pub fn build(&self, config: Option<&CostTrackerStreamConfig>) -> Arc<dyn ChangeStream> {
        match (self.cost_tracker(config), self.gc.clone()) {
            (Some(_cost_tracker), Some(gc)) => {
                objectstore_log::error!(
                    "both cost tracking and garbage collection are configured for one backend; \
                     using the garbage collector"
                );
                gc
            }
            (Some(cost_tracker), None) => cost_tracker,
            (None, Some(gc)) => gc,
            (None, None) => Arc::new(NoopStream),
        }
    }

    /// Builds the cost-tracking stream, if both the service-wide transport and the
    /// per-backend configuration are present and usable.
    #[cfg(feature = "storage-cogs")]
    fn cost_tracker(
        &self,
        config: Option<&CostTrackerStreamConfig>,
    ) -> Option<Arc<dyn ChangeStream>> {
        match (config, self.producer.clone()) {
            (Some(config), Some(producer)) => {
                Some(Arc::new(CostTrackerStream::new(producer, config)))
            }
            (None, None) => None,
            (c, p) => {
                objectstore_log::warn!(
                    stream_configured = c.is_some(),
                    producer_configured = p.is_some(),
                    "incomplete cost-tracking change stream configuration, \
                     disabling cost tracking for this backend",
                );
                None
            }
        }
    }

    /// Cost tracking is not compiled in, so no backend reports through it.
    #[cfg(not(feature = "storage-cogs"))]
    fn cost_tracker(
        &self,
        _config: Option<&CostTrackerStreamConfig>,
    ) -> Option<Arc<dyn ChangeStream>> {
        None
    }
}

impl fmt::Debug for ChangeStreamFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("ChangeStreamFactory");
        #[cfg(feature = "storage-cogs")]
        f.field("producer", &self.producer.is_some());
        f.field("gc", &self.gc.is_some());
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

/// A [`ChangeStreamFactory`] that reports into the returned producer.
#[cfg(all(test, feature = "storage-cogs"))]
pub(crate) fn dummy_factory() -> (ChangeStreamFactory, test_utils::DummyProducer) {
    use objectstore_inventory_tracker::Producer as _;

    let producer = test_utils::DummyProducer::default();
    let factory = ChangeStreamFactory {
        producer: Some(producer.clone().shared()),
        gc: None,
    };

    (factory, producer)
}

#[cfg(all(test, feature = "storage-cogs"))]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A stream that identifies itself in [`Debug`] output.
    #[derive(Debug)]
    struct TestStream;

    #[async_trait::async_trait]
    impl ChangeStream for TestStream {
        async fn join(&self, _timeout: Duration) {}
    }

    fn config() -> CostTrackerStreamConfig {
        CostTrackerStreamConfig {
            shared_resource_id: "bigtable_objectstore".into(),
            sample_rate: 1.0,
        }
    }

    fn reports(stream: &Arc<dyn ChangeStream>) -> bool {
        !format!("{stream:?}").contains("NoopStream")
    }

    #[test]
    fn a_backend_without_a_change_stream_config_reports_nothing() {
        let (factory, _producer) = dummy_factory();

        assert!(!reports(&factory.build(None)));
    }

    #[test]
    fn a_configured_backend_without_a_transport_reports_nothing() {
        let factory = ChangeStreamFactory::default();

        assert!(!reports(&factory.build(Some(&config()))));
    }

    #[tokio::test]
    async fn a_configured_backend_reports_through_the_transport() {
        let (factory, producer) = dummy_factory();
        let stream = factory.build(Some(&config()));

        assert!(reports(&stream));

        stream
            .commit_delete(
                &crate::id::ObjectId::from_storage_path("attachments/objects/abc").unwrap(),
            )
            .await;

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].shared_resource_id, "bigtable_objectstore");
    }

    #[test]
    fn a_backend_with_only_a_garbage_collector_reports_through_it() {
        let factory = ChangeStreamFactory::default().with_gc(Arc::new(TestStream));

        assert!(format!("{:?}", factory.build(None)).contains("TestStream"));
    }

    #[test]
    fn a_broken_cost_tracker_does_not_disable_the_garbage_collector() {
        let factory = ChangeStreamFactory::default().with_gc(Arc::new(TestStream));

        assert!(format!("{:?}", factory.build(Some(&config()))).contains("TestStream"));
    }

    #[test]
    fn a_garbage_collector_takes_precedence_when_both_are_constructed_directly() {
        let (factory, _producer) = dummy_factory();
        let factory = factory.with_gc(Arc::new(TestStream));

        assert!(format!("{:?}", factory.build(Some(&config()))).contains("TestStream"));
    }

    #[test]
    fn an_unusable_transport_disables_reporting_instead_of_failing() {
        let factory = ChangeStreamFactory::new(&CostTrackerConfig::Kafka(
            objectstore_inventory_tracker::kafka::KafkaConfig {
                topic: "shared-resources-inventory".into(),
                bootstrap_servers: vec!["127.0.0.1:9092".into()],
                override_params: [("not.a.real.property".to_owned(), "1".to_owned())].into(),
            },
        ));

        assert!(!reports(&factory.build(Some(&config()))));
    }
}

#[cfg(all(test, not(feature = "storage-cogs")))]
mod tests_without_cogs {
    use std::time::Duration;

    use super::*;

    /// A stream that identifies itself in [`Debug`] output.
    #[derive(Debug)]
    struct TestStream;

    #[async_trait::async_trait]
    impl ChangeStream for TestStream {
        async fn join(&self, _timeout: Duration) {}
    }

    #[test]
    fn a_garbage_collector_is_used_without_the_cogs_feature() {
        let factory = ChangeStreamFactory::default().with_gc(Arc::new(TestStream));

        assert!(format!("{:?}", factory.build(None)).contains("TestStream"));
    }
}
