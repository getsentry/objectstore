//! A [`Producer`] backed by [`rdkafka`].

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{
    BaseRecord, DeliveryResult, Producer as RdKafkaProducer, ProducerContext, ThreadedProducer,
};

use crate::producer::Producer;

/// Connection settings for [`KafkaProducer`].
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Topic to produce to.
    pub topic: String,

    /// Broker addresses.
    pub bootstrap_servers: Vec<String>,

    /// Additional librdkafka properties, passed through verbatim.
    ///
    /// SASL credentials, compression, and buffering limits go here.
    pub override_params: HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            topic: "shared-resources-inventory".to_owned(),
            bootstrap_servers: Vec::new(),
            override_params: HashMap::new(),
        }
    }
}

/// Something that went wrong producing to Kafka.
#[derive(Debug, thiserror::Error)]
pub enum KafkaProducerError {
    /// The producer could not be created from the given configuration.
    #[error("failed to create kafka producer")]
    InvalidConfig(#[source] KafkaError),

    /// The record could not be enqueued.
    ///
    /// This may mean the local queue is full, which may be caused by the broker being
    /// unreachable or backed up.
    #[error("failed to enqueue inventory record")]
    SendFailed(#[source] KafkaError),

    /// The local queue was not emptied before the flush timeout elapsed.
    #[error("failed to flush inventory records")]
    FlushFailed(#[source] KafkaError),
}

/// Called for each record that fails to deliver.
///
/// Invoked from a librdkafka background thread, so it must not block.
pub type OnDeliveryFailure = Box<dyn Fn(&KafkaError) + Send + Sync>;

/// Reports delivery outcomes.
///
/// Delivery is asynchronous, so a successful [`Producer::send`] only means the record was
/// enqueued locally. If the broker later rejects messages for some reason, this type
/// provides a callback.
struct DeliveryReporter {
    on_failure: Option<OnDeliveryFailure>,
}

impl ClientContext for DeliveryReporter {}

impl ProducerContext for DeliveryReporter {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        if let Err((error, _)) = result {
            // `&dyn Error` rather than `%error` so the source chain is captured.
            tracing::warn!(
                error = error as &dyn std::error::Error,
                "failed to deliver inventory record"
            );
            if let Some(on_failure) = &self.on_failure {
                on_failure(error);
            }
        }
    }
}

/// Produces inventory records onto a Kafka topic.
///
/// Sends are non-blocking: records go onto librdkafka's internal queue and a background
/// thread delivers them. When that queue is full, [`Producer::send`] returns an error
/// rather than waiting.
pub struct KafkaProducer {
    topic: String,
    producer: ThreadedProducer<DeliveryReporter>,
}

impl std::fmt::Debug for KafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducer")
            .field("topic", &self.topic)
            .field("in_flight", &self.producer.in_flight_count())
            .finish_non_exhaustive()
    }
}

impl KafkaProducer {
    /// Creates a producer from `config`, optionally reporting delivery failures to
    /// `on_delivery_failure`.
    ///
    /// Failures are always logged. The callback is how a caller additionally counts them
    /// without this crate having to depend on a metrics backend.
    pub fn try_new(
        config: KafkaConfig,
        on_delivery_failure: Option<OnDeliveryFailure>,
    ) -> Result<Self, KafkaProducerError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", config.bootstrap_servers.join(","));

        // Applied after the broker list so that a caller can override it if they need to.
        for (key, value) in &config.override_params {
            client_config.set(key, value);
        }

        let producer = client_config
            .create_with_context(DeliveryReporter {
                on_failure: on_delivery_failure,
            })
            .map_err(KafkaProducerError::InvalidConfig)?;

        Ok(Self {
            topic: config.topic,
            producer,
        })
    }
}

impl Producer for KafkaProducer {
    type Error = KafkaProducerError;

    fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<(), Self::Error> {
        let record: BaseRecord<'_, [u8], [u8]> =
            BaseRecord::to(&self.topic).key(key).payload(&payload);

        self.producer
            .send(record)
            .map_err(|(error, _)| KafkaProducerError::SendFailed(error))
    }

    fn join_blocking(&self, timeout: Duration) -> Result<(), Self::Error> {
        self.producer
            .flush(timeout)
            .map_err(KafkaProducerError::FlushFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> KafkaConfig {
        KafkaConfig {
            topic: "shared-resources-inventory".into(),
            bootstrap_servers: vec!["127.0.0.1:9092".into()],
            override_params: HashMap::from([("compression.type".into(), "lz4".into())]),
        }
    }

    #[test]
    fn producer_targets_the_configured_topic() {
        let producer = KafkaProducer::try_new(config(), None).unwrap();
        assert_eq!(producer.topic, "shared-resources-inventory");
    }

    #[test]
    fn every_bootstrap_server_is_passed_through() {
        let mut config = config();
        config.bootstrap_servers = vec!["a:9092".into(), "b:9092".into(), "c:9092".into()];

        // librdkafka parses the broker list at creation, so building successfully is the
        // assertion that the joined value was well formed.
        assert!(KafkaProducer::try_new(config, None).is_ok());
    }

    #[test]
    fn bad_config_is_an_error() {
        let mut config = config();
        config
            .override_params
            .insert("not.a.real.property".into(), "1".into());

        assert!(matches!(
            KafkaProducer::try_new(config, None),
            Err(KafkaProducerError::InvalidConfig(_))
        ));
    }

    #[test]
    fn joining_an_empty_queue_returns_immediately() {
        let producer = KafkaProducer::try_new(config(), None).unwrap();

        let start = std::time::Instant::now();
        producer.join_blocking(Duration::from_secs(5)).unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn delivery_failures_reach_the_callback() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let failures = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&failures);

        let mut config = config();
        // Nothing is listening on this port, and a short timeout means librdkafka gives
        // up and reports the record as undeliverable while the test is still running.
        config.bootstrap_servers = vec!["127.0.0.1:1".into()];
        config
            .override_params
            .insert("message.timeout.ms".into(), "300".into());

        let producer = KafkaProducer::try_new(
            config,
            Some(Box::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
        )
        .unwrap();

        producer.send(b"key", b"payload".to_vec()).unwrap();
        // Draining drives the delivery callbacks; the record cannot succeed.
        let _ = producer.join_blocking(Duration::from_secs(5));

        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }
}
