//! Tests for the spans emitted with the `tracing` feature, as captured by Sentry.

#![cfg(feature = "tracing")]

mod common;

use std::time::Duration;

use objectstore_client::{Client, Compression, ExpiryExtension, Usecase};
use sentry::integrations::tracing::SpanLayer;
use sentry::protocol::{EnvelopeItem, Span, Value};
use tracing::Instrument as _;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;

use common::{test_server, test_session, test_token_generator};

/// Runs `future` in a root span and returns the Objectstore spans that Sentry captured.
fn capture_spans<F>(future: F) -> Vec<Span>
where
    F: Future<Output = ()>,
{
    let options = sentry::ClientOptions::new().traces_sample_rate(1.0);

    let envelopes = sentry::test::with_captured_envelopes_options(
        || {
            let subscriber = tracing_subscriber::registry()
                .with(SpanLayer::new().with_filter(LevelFilter::INFO));
            let _guard = tracing::subscriber::set_default(subscriber);

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(future.instrument(tracing::info_span!("test")));
        },
        options,
    );

    envelopes
        .iter()
        .flat_map(|envelope| envelope.items())
        .filter_map(|item| match item {
            EnvelopeItem::Transaction(transaction) => Some(transaction.spans.clone()),
            _ => None,
        })
        .flatten()
        .filter(|span| {
            span.op
                .as_deref()
                .is_some_and(|op| op.starts_with("objectstore."))
        })
        .collect()
}

fn find_span<'a>(spans: &'a [Span], op: &str) -> &'a Span {
    spans
        .iter()
        .find(|span| span.op.as_deref() == Some(op))
        .unwrap_or_else(|| panic!("no span with op {op} in {spans:#?}"))
}

fn data<'a>(span: &'a Span, key: &str) -> Option<&'a Value> {
    span.data.get(key)
}

#[test]
fn simple_operations() {
    let spans = capture_spans(async {
        let server = test_server().await;
        let client = Client::builder(server.url("/"))
            .token(test_token_generator())
            .build()
            .unwrap();
        let scope = Usecase::from_static("usecase")
            .for_project(12345, 1337)
            .push("app_slug", "email_app");
        let session = client.session(scope).unwrap();

        session.put("payload").key("my-key").send().await.unwrap();
        session.get("my-key").send().await.unwrap().unwrap();
        session.head("my-key").send().await.unwrap().unwrap();
        session
            .extend_expiry("my-key", ExpiryExtension::FromNow(Duration::from_secs(60)))
            .send()
            .await
            .unwrap();
        session.delete("my-key").send().await.unwrap();
    });

    let expected = [
        ("objectstore.put", vec![]),
        (
            "objectstore.get",
            vec![
                ("objectstore.found", Value::from(true)),
                ("objectstore.compression", Value::from("zstd")),
                ("objectstore.decompressed", Value::from(true)),
            ],
        ),
        (
            "objectstore.head",
            vec![("objectstore.found", Value::from(true))],
        ),
        ("objectstore.extend_expiry", vec![]),
        ("objectstore.delete", vec![]),
    ];

    for (op, expected_data) in expected {
        let span = find_span(&spans, op);
        assert_eq!(span.description.as_deref(), Some(&*format!("{op} usecase")));
        assert_eq!(
            data(span, "objectstore.usecase"),
            Some(&Value::from("usecase"))
        );
        assert_eq!(
            data(span, "objectstore.scopes"),
            Some(&Value::from("org=12345;project=1337;app_slug=email_app"))
        );
        assert_eq!(
            data(span, "objectstore.scopes.org"),
            Some(&Value::from(12345))
        );
        assert_eq!(
            data(span, "objectstore.scopes.project"),
            Some(&Value::from(1337))
        );
        assert_eq!(data(span, "objectstore.key"), Some(&Value::from("my-key")));
        for (key, value) in expected_data {
            assert_eq!(data(span, key), Some(&value), "{op}: {key}");
        }
    }
}

#[test]
fn get_missing_object() {
    let spans = capture_spans(async {
        let server = test_server().await;
        let session = test_session(&server);

        assert!(session.get("missing").send().await.unwrap().is_none());
        assert!(session.head("missing").send().await.unwrap().is_none());
    });

    for op in ["objectstore.get", "objectstore.head"] {
        let span = find_span(&spans, op);
        assert_eq!(data(span, "objectstore.found"), Some(&Value::from(false)));
    }
}

#[test]
fn put_records_compression_and_sizes() {
    let payload = "x".repeat(1024);

    let spans = capture_spans(async move {
        let server = test_server().await;
        let session = test_session(&server);

        session
            .put(payload)
            .resumable_threshold_bytes(None)
            .send()
            .await
            .unwrap();
    });

    let span = find_span(&spans, "objectstore.put");
    assert_eq!(
        data(span, "objectstore.compression"),
        Some(&Value::from("zstd"))
    );
    assert_eq!(
        data(span, "objectstore.uncompressed_size"),
        Some(&Value::from(1024))
    );
    let compressed_size = data(span, "objectstore.compressed_size")
        .and_then(Value::as_u64)
        .expect("compressed size to be recorded");
    assert!(compressed_size > 0 && compressed_size < 1024);
    // The server assigns the key, so it is only known after the response.
    assert!(data(span, "objectstore.key").is_some_and(Value::is_string));
}

#[test]
fn put_precompressed_records_compressed_size() {
    let payload = zstd::encode_all(&b"hello world"[..], 0).unwrap();
    let payload_len = payload.len() as u64;

    let spans = capture_spans(async move {
        let server = test_server().await;
        let session = test_session(&server);

        session
            .put(payload)
            .precompressed(Compression::Zstd)
            .send()
            .await
            .unwrap();
    });

    let span = find_span(&spans, "objectstore.put");
    assert_eq!(
        data(span, "objectstore.compression"),
        Some(&Value::from("zstd"))
    );
    assert_eq!(
        data(span, "objectstore.compressed_size"),
        Some(&Value::from(payload_len))
    );
    assert_eq!(data(span, "objectstore.uncompressed_size"), None);
}

#[test]
fn automatic_resumable_upload_nests_spans() {
    let spans = capture_spans(async {
        let server = test_server().await;
        let session = test_session(&server);

        session
            .put("payload")
            .key("my-key")
            .resumable_threshold_bytes(0)
            .send()
            .await
            .unwrap();
    });

    let put = find_span(&spans, "objectstore.put");
    for op in ["objectstore.resumable.create", "objectstore.resumable.put"] {
        let span = find_span(&spans, op);
        assert_eq!(span.parent_span_id, Some(put.span_id), "{op}");
        assert_eq!(data(span, "objectstore.key"), Some(&Value::from("my-key")));
    }

    let chunk = find_span(&spans, "objectstore.resumable.put");
    assert_eq!(data(chunk, "objectstore.offset"), Some(&Value::from(0)));
    assert!(data(chunk, "objectstore.size").is_some_and(Value::is_u64));
}

#[test]
fn batch_records_operation_count() {
    let spans = capture_spans(async {
        let server = test_server().await;
        let session = test_session(&server);

        session
            .many()
            .push(session.put("a").key("a"))
            .push(session.put("b").key("b"))
            .push(session.delete("c"))
            .send()
            .await
            .error_for_failures()
            .await
            .unwrap_or_else(|e| panic!("batch failures: {:?}", e.collect::<Vec<_>>()));
    });

    let span = find_span(&spans, "objectstore.batch");
    assert_eq!(
        span.description.as_deref(),
        Some("objectstore.batch usecase")
    );
    assert_eq!(data(span, "objectstore.operations"), Some(&Value::from(3)));
    // Batched parts must not leak per-object attributes into the batch span.
    assert_eq!(data(span, "objectstore.compressed_size"), None);
}
