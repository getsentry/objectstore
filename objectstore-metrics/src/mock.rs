//! Mock metrics recorder for tests.
//!
//! Provides [`with_capturing_test_client`], which installs a thread-local
//! recorder that captures all emitted metrics as DogStatsD-format strings.

use std::sync::{Arc, Mutex};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

/// Runs `f` with a thread-local mock recorder installed, then returns all
/// captured metrics as `"name:value|type|#key:value,key:value"` strings.
///
/// Only affects the calling thread — safe for use in parallel tests.
///
/// # Example
///
/// ```ignore
/// let captured = objectstore_metrics::with_capturing_test_client(|| {
///     objectstore_metrics::counter!("test.counter": 1, "tag" => "val");
/// });
/// assert!(captured.iter().any(|m| m.starts_with("test.counter:")));
/// ```
pub fn with_capturing_test_client(f: impl FnOnce()) -> Vec<String> {
    let recorder = MockRecorder::default();
    metrics::with_local_recorder(&recorder, f);
    recorder.consume()
}

/// A metrics recorder that formats and stores every operation as a string.
#[derive(Clone, Default)]
struct MockRecorder {
    inner: Arc<Mutex<Vec<String>>>,
}

impl MockRecorder {
    /// Drains and returns all captured metric strings.
    fn consume(self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }
}

impl Recorder for MockRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(MockFn::new(key.clone(), self.inner.clone())))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(MockFn::new(key.clone(), self.inner.clone())))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(MockFn::new(key.clone(), self.inner.clone())))
    }
}

/// Shared implementation for all metric types that formats and records operations.
struct MockFn {
    key: Key,
    inner: Arc<Mutex<Vec<String>>>,
}

impl MockFn {
    fn new(key: Key, inner: Arc<Mutex<Vec<String>>>) -> Self {
        Self { key, inner }
    }

    fn push(&self, value: &str, ty: &str) {
        let labels = self
            .key
            .labels()
            .map(|l| format!("{}:{}", l.key(), l.value()))
            .collect::<Vec<_>>()
            .join(",");

        let entry = if labels.is_empty() {
            format!("{}:{}|{}", self.key.name(), value, ty)
        } else {
            format!("{}:{}|{}|#{}", self.key.name(), value, ty, labels)
        };

        if let Ok(mut vec) = self.inner.lock() {
            vec.push(entry);
        }
    }
}

impl metrics::CounterFn for MockFn {
    fn increment(&self, value: u64) {
        self.push(&format!("+{value}"), "c");
    }

    fn absolute(&self, value: u64) {
        self.push(&format!("={value}"), "c");
    }
}

impl metrics::GaugeFn for MockFn {
    fn increment(&self, value: f64) {
        self.push(&format!("+{value}"), "g");
    }

    fn decrement(&self, value: f64) {
        self.push(&format!("-{value}"), "g");
    }

    fn set(&self, value: f64) {
        self.push(&format!("{value}"), "g");
    }
}

impl metrics::HistogramFn for MockFn {
    fn record(&self, value: f64) {
        self.push(&format!("{value}"), "d");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_counter() {
        let captured = with_capturing_test_client(|| {
            crate::counter!("test.counter": 1);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.counter:+1|c");
    }

    #[test]
    fn captures_counter_with_tags() {
        let captured = with_capturing_test_client(|| {
            crate::counter!("test.counter": 1, "env" => "prod", "region" => "us");
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.counter:+1|c|#env:prod,region:us");
    }

    #[test]
    fn captures_gauge() {
        let captured = with_capturing_test_client(|| {
            crate::gauge!("test.gauge": 42usize);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.gauge:42|g");
    }

    #[test]
    fn captures_gauge_bytes() {
        let captured = with_capturing_test_client(|| {
            crate::gauge!("test.gauge"@b: 1024u64);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.gauge:1024|g");
    }

    #[test]
    fn captures_distribution() {
        let captured = with_capturing_test_client(|| {
            crate::distribution!("test.dist": 2.78f64);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.dist:2.78|d");
    }

    #[test]
    fn captures_distribution_seconds() {
        let captured = with_capturing_test_client(|| {
            let dur = std::time::Duration::from_millis(1500);
            crate::distribution!("test.latency"@s: dur);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.latency:1.5|d");
    }

    #[test]
    fn captures_distribution_bytes() {
        let captured = with_capturing_test_client(|| {
            crate::distribution!("test.size"@b: 4096u64);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.size:4096|d");
    }

    #[test]
    fn captures_distribution_with_tags() {
        let captured = with_capturing_test_client(|| {
            let dur = std::time::Duration::from_secs(2);
            crate::distribution!(
                "test.latency"@s: dur,
                "route" => "/v1/test",
                "method" => "GET"
            );
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.latency:2|d|#route:/v1/test,method:GET");
    }

    #[test]
    fn integer_tag_values() {
        let captured = with_capturing_test_client(|| {
            crate::counter!("test.status": 1, "status" => 200u16);
        });
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "test.status:+1|c|#status:200");
    }
}
