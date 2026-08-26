//! The change stream implementation that reports through an [`InventoryTracker`].

use std::fmt;
use std::time::{Duration, SystemTime};

use objectstore_inventory_tracker::{BoxError, InventoryTracker, Producer};

use crate::change_stream::{
    ChangeStream, CostTrackerStreamConfig, SCOPE_ORGANIZATION, SCOPE_PROJECT, scope_id,
};
use crate::id::ObjectId;

/// Reports through an [`InventoryTracker`], which hashes each [`ObjectId`] both to
/// anonymize it and to decide whether it is sampled. See [`objectstore_inventory_tracker`]
/// for the record format.
///
/// Logs, counts, and swallows errors returned by the [`InventoryTracker`].
pub struct CostTrackerStream<P: Producer> {
    tracker: InventoryTracker<P>,
}

impl<P: Producer> CostTrackerStream<P> {
    /// Reports changes through `producer`, as described by `config`.
    pub fn new(producer: P, config: &CostTrackerStreamConfig) -> Self {
        Self {
            tracker: InventoryTracker::new(
                producer,
                &config.shared_resource_id,
                config.sample_rate,
            ),
        }
    }

    /// Counts and logs a failed report, then returns.
    ///
    /// Note: success here doesn't necessarily mean that a message was emitted. The
    /// tracker's sampling policy may have filtered an object out and chosen to emit
    /// nothing. Or, a message may have been enqueued only to fail later. Those are
    /// counted by the transport's delivery failure callback instead.
    fn swallow(&self, op: &'static str, result: Result<(), P::Error>)
    where
        P::Error: Into<BoxError>,
    {
        if let Err(error) = result {
            // Boxed because `SharedProducer` reports a `Box<dyn Error>`, which std does
            // not implement `Error` for.
            let error: BoxError = error.into();
            // Records are dropped rather than retried
            objectstore_metrics::count!(
                "cost_tracker.dropped" += 1,
                shared_resource_id = self.tracker.shared_resource_id().to_owned(),
                op = op,
            );
            objectstore_log::warn!(!!&*error, op, "failed to publish change stream record");
        }
    }
}

impl<P: Producer> fmt::Debug for CostTrackerStream<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CostTrackerStream")
            .field("shared_resource_id", &self.tracker.shared_resource_id())
            .field("sample_rate", &self.tracker.sample_rate())
            .finish()
    }
}

#[async_trait::async_trait]
impl<P> ChangeStream for CostTrackerStream<P>
where
    P: Producer + Clone + Send + Sync + 'static,
    P::Error: Into<BoxError> + Send + 'static,
{
    fn write(&self, id: &ObjectId, size: u64, expires_at: Option<SystemTime>) {
        let result = self.tracker.write(
            &id.as_storage_path().to_string(),
            id.usecase(),
            size,
            SystemTime::now(),
            expires_at,
            scope_id(id, SCOPE_ORGANIZATION),
            scope_id(id, SCOPE_PROJECT),
        );
        self.swallow("write", result);
    }

    fn update(&self, id: &ObjectId, expires_at: Option<SystemTime>) {
        let result = self.tracker.update(
            &id.as_storage_path().to_string(),
            id.usecase(),
            SystemTime::now(),
            expires_at,
            scope_id(id, SCOPE_ORGANIZATION),
            scope_id(id, SCOPE_PROJECT),
        );
        self.swallow("update", result);
    }

    fn delete(&self, id: &ObjectId) {
        let result = self.tracker.delete(
            &id.as_storage_path().to_string(),
            id.usecase(),
            SystemTime::now(),
        );
        self.swallow("delete", result);
    }

    async fn join(&self, timeout: Duration) {
        self.swallow("join", self.tracker.join(timeout).await);
    }
}

#[cfg(test)]
mod tests {
    use objectstore_inventory_tracker::OpType;
    use objectstore_inventory_tracker::test_utils::DummyProducer;

    use super::*;

    fn object_id(path: &str) -> ObjectId {
        ObjectId::from_storage_path(path).expect("valid storage path")
    }

    fn stream(sample_rate: f64) -> (DummyProducer, CostTrackerStream<DummyProducer>) {
        let producer = DummyProducer::default();
        let stream = CostTrackerStream::new(
            producer.clone(),
            &CostTrackerStreamConfig {
                shared_resource_id: "bigtable_objectstore".into(),
                sample_rate,
            },
        );
        (producer, stream)
    }

    #[test]
    fn usecase_and_scopes_are_extracted_from_the_id() {
        let (producer, stream) = stream(1.0);
        let id = object_id("attachments/org.17/project.42/objects/abc");

        stream.write(&id, 4096, None);

        let record = &producer.records()[0];
        assert_eq!(record.shared_resource_id, "bigtable_objectstore");
        assert_eq!(record.app_feature, "attachments");
        assert_eq!(record.organization_id, Some(17));
        assert_eq!(record.project_id, Some(42));
        assert_eq!(record.size, Some(4096));
        assert_eq!(record.op_type, OpType::Write);
    }

    #[test]
    fn the_storage_path_is_not_emitted() {
        let (producer, stream) = stream(1.0);
        let id = object_id("attachments/org.17/project.42/objects/abc");

        stream.write(&id, 4096, None);

        let record_id = &producer.records()[0].record_id;
        assert_ne!(record_id, &id.as_storage_path().to_string());
        assert!(!record_id.contains("attachments"));
    }

    #[test]
    fn missing_or_unparseable_scopes_are_reported_as_absent() {
        let (producer, stream) = stream(1.0);

        for path in [
            "attachments/objects/abc",
            "attachments/organization.17/objects/abc",
            "attachments/org.not-a-number/project.42/objects/abc",
        ] {
            stream.write(&object_id(path), 1, None);
        }

        let records = producer.records();
        assert_eq!(
            records.len(),
            3,
            "every object is reported regardless of scopes"
        );
        assert_eq!(records[0].organization_id, None, "no scopes at all");
        assert_eq!(
            records[1].organization_id, None,
            "wrong scope key is not org"
        );
        assert_eq!(
            records[2].organization_id, None,
            "unparseable org is absent"
        );
        assert_eq!(
            records[2].project_id,
            Some(42),
            "but project still resolves"
        );
        for record in &records {
            assert_eq!(record.app_feature, "attachments");
        }
    }

    #[test]
    fn every_operation_on_an_object_reports_the_same_record() {
        let (producer, stream) = stream(1.0);
        let id = object_id("attachments/org.1/project.2/objects/abc");

        stream.write(&id, 10, None);
        stream.update(&id, Some(SystemTime::now()));
        stream.delete(&id);

        let records = producer.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_id, records[1].record_id);
        assert_eq!(records[1].record_id, records[2].record_id);
    }

    #[test]
    fn distinct_revisions_are_distinct_records() {
        let (producer, stream) = stream(1.0);

        stream.write(
            &object_id("attachments/org.1/project.2/objects/abc/0199aaaa"),
            1,
            None,
        );
        stream.write(
            &object_id("attachments/org.1/project.2/objects/abc/0199bbbb"),
            1,
            None,
        );

        let records = producer.records();
        assert_ne!(records[0].record_id, records[1].record_id);
    }

    #[test]
    fn update_omits_size_and_delete_omits_everything_optional() {
        let (producer, stream) = stream(1.0);
        let id = object_id("attachments/org.1/project.2/objects/abc");

        stream.update(&id, Some(SystemTime::now()));
        stream.delete(&id);

        let records = producer.records();
        assert_eq!(records[0].op_type, OpType::Update);
        assert_eq!(records[0].size, None);
        assert!(records[0].expiration_time.is_some());
        assert_eq!(records[1].op_type, OpType::Delete);
        assert_eq!(records[1].size, None);
        assert_eq!(records[1].expiration_time, None);
    }

    #[test]
    fn a_listener_sampled_at_zero_reports_nothing() {
        let (producer, stream) = stream(0.0);

        for i in 0..100 {
            let id = object_id(&format!("attachments/org.1/project.2/objects/{i}"));
            stream.write(&id, 1, None);
            stream.update(&id, None);
            stream.delete(&id);
        }

        assert!(producer.records().is_empty());
    }
}
