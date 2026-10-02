//! Constructs the [`ChangeStream`] implementation(s) for a backend based on the available
//! service-wide sink config and per-backend stream config.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "storage-cogs")]
use objectstore_inventory_tracker::SharedProducer;
#[cfg(all(test, feature = "storage-cogs"))]
use objectstore_inventory_tracker::test_utils;
use objectstore_types::time::Timestamp;
#[cfg(feature = "storage-cogs")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "storage-cogs")]
use crate::change_stream::CostTrackerStream;
use crate::change_stream::{ChangeStream, CostTrackerStreamConfig, NoopStream};
use crate::id::ObjectId;

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

#[derive(Default, Debug)]
struct MultiStream {
    streams: Vec<Box<dyn ChangeStream>>,
}

impl MultiStream {
    pub fn with_stream(mut self, stream: Box<dyn ChangeStream>) -> Self {
        self.streams.push(stream);
        self
    }
}

#[async_trait::async_trait]
impl ChangeStream for MultiStream {
    fn write(&self, id: &ObjectId, size: u64, expires_at: Option<Timestamp>) {
        for stream in self.streams.iter() {
            stream.write(id, size, expires_at);
        }
    }

    fn update(&self, id: &ObjectId, expires_at: Option<Timestamp>) {
        for stream in self.streams.iter() {
            stream.update(id, expires_at);
        }
    }

    fn delete(&self, id: &ObjectId) {
        for stream in self.streams.iter() {
            stream.delete(id);
        }
    }

    async fn join(&self, timeout: Duration) {
        let _ = futures_util::future::join_all(
            self.streams
                .iter()
                .map(|stream| stream.join(timeout))
                .collect::<Vec<_>>(),
        )
        .await;
    }
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

/// A [`ChangeStreamFactory`] that reports into the returned producer.
#[cfg(all(test, feature = "storage-cogs"))]
pub(crate) fn dummy_factory() -> (ChangeStreamFactory, test_utils::DummyProducer) {
    use objectstore_inventory_tracker::Producer as _;

    let producer = test_utils::DummyProducer::default();
    let factory = ChangeStreamFactory {
        producer: Some(producer.clone().shared()),
    };

    (factory, producer)
}

#[cfg(all(test, feature = "storage-cogs"))]
mod tests {
    use super::*;

    use std::sync::RwLock;

    type SharedMessages = Arc<RwLock<Vec<(&'static str, &'static str, String)>>>;

    #[derive(Default, Debug)]
    struct RecordingStream {
        name: &'static str,
        pub messages: SharedMessages,
    }

    impl RecordingStream {
        pub fn new(name: &'static str, messages: SharedMessages) -> Self {
            Self { name, messages }
        }
    }

    #[async_trait::async_trait]
    impl ChangeStream for RecordingStream {
        fn write(&self, id: &ObjectId, _size: u64, _expires_at: Option<Timestamp>) {
            self.messages.write().unwrap().push((
                self.name,
                "write",
                id.as_storage_path().to_string(),
            ));
        }

        fn update(&self, id: &ObjectId, _expires_at: Option<Timestamp>) {
            self.messages.write().unwrap().push((
                self.name,
                "update",
                id.as_storage_path().to_string(),
            ));
        }

        fn delete(&self, id: &ObjectId) {
            self.messages.write().unwrap().push((
                self.name,
                "delete",
                id.as_storage_path().to_string(),
            ));
        }

        async fn join(&self, _timeout: Duration) {
            self.messages
                .write()
                .unwrap()
                .push((self.name, "join", "".into()));
        }
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

    #[test]
    fn a_configured_backend_reports_through_the_transport() {
        let (factory, producer) = dummy_factory();
        let stream = factory.build(Some(&config()));

        assert!(reports(&stream));

        stream.delete(&ObjectId::from_storage_path("attachments/objects/abc").unwrap());

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].shared_resource_id, "bigtable_objectstore");
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

    #[tokio::test]
    async fn test_multi_stream() {
        // Create `MultiStream` with a pair of `RecordingStream`s that write messages to
        // the same log
        let messages = Arc::new(RwLock::new(vec![]));
        let s1 = Box::new(RecordingStream::new("s1", messages.clone()));
        let s2 = Box::new(RecordingStream::new("s2", messages.clone()));
        let ms = MultiStream::default().with_stream(s1).with_stream(s2);

        // Write one of each message type
        ms.write(
            &ObjectId::from_storage_path("foo/objects/bar").unwrap(),
            0,
            None,
        );
        ms.update(
            &ObjectId::from_storage_path("foo/objects/bar").unwrap(),
            None,
        );
        ms.delete(&ObjectId::from_storage_path("foo/objects/bar").unwrap());
        ms.join(Duration::from_secs(3)).await;

        // Ensure `s1` and `s2` each wrote one of each message
        assert_eq!(
            *messages.read().unwrap(),
            [
                ("s1", "write", "foo/objects/bar".into()),
                ("s2", "write", "foo/objects/bar".into()),
                ("s1", "update", "foo/objects/bar".into()),
                ("s2", "update", "foo/objects/bar".into()),
                ("s1", "delete", "foo/objects/bar".into()),
                ("s2", "delete", "foo/objects/bar".into()),
                ("s1", "join", "".into()),
                ("s2", "join", "".into())
            ]
        );
    }
}
