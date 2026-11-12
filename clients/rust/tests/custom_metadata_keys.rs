//! Tests for client-side validation of custom metadata keys.
//!
//! These run against a mock server that counts incoming requests, so they can assert that an
//! invalid key never causes a request.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::http::HeaderMap;
use objectstore_client::{Client, Error, OperationResult, Session, Usecase};
use objectstore_types::metadata::Error as MetadataError;
use tokio::net::TcpListener;

/// Starts a mock server that answers every request with 500 and records its count and headers.
async fn mock_session() -> (
    Session,
    Arc<AtomicUsize>,
    Arc<std::sync::Mutex<Vec<HeaderMap>>>,
) {
    let requests = Arc::new(AtomicUsize::new(0));
    let seen_headers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let app = Router::new().fallback({
        let requests = requests.clone();
        let seen_headers = seen_headers.clone();
        move |headers: HeaderMap| async move {
            requests.fetch_add(1, Ordering::SeqCst);
            seen_headers.lock().unwrap().push(headers);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = Client::builder(url).build().unwrap();
    let session = client
        .session(Usecase::from_static("usecase").for_organization(1))
        .unwrap();
    (session, requests, seen_headers)
}

fn assert_invalid_key(error: &Error, expected_key: &str) {
    match error {
        Error::Metadata(MetadataError::CustomKey(key)) => assert_eq!(key, expected_key),
        other => panic!("expected invalid custom key error, got {other:?}"),
    }
}

#[tokio::test]
async fn put_with_invalid_key_fails_without_request() {
    let (session, requests, _) = mock_session().await;

    let error = session
        .put("payload")
        .append_metadata("fine", "ok")
        .append_metadata("has space", "bad")
        .send()
        .await
        .unwrap_err();

    assert_invalid_key(&error, "has space");
    assert!(error.to_string().contains("has space"));
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn put_with_empty_key_fails_without_request() {
    let (session, requests, _) = mock_session().await;

    let error = session
        .put("payload")
        .append_metadata("", "bad")
        .send()
        .await
        .unwrap_err();

    assert_invalid_key(&error, "");
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn put_lowercases_and_encodes_keys() {
    let (session, requests, seen_headers) = mock_session().await;

    // The mock answers 500, so the put fails after the request went out.
    let _ = session
        .put("payload")
        .append_metadata("Build_ID", "1")
        .append_metadata("a:b", "2")
        .append_metadata("100%", "3")
        .send()
        .await;

    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let headers = seen_headers.lock().unwrap();
    let header = |name: &str| headers[0].get(name).map(|v| v.to_str().unwrap().to_owned());
    assert_eq!(header("x-snme-build_id").as_deref(), Some("1"));
    assert_eq!(header("x-snme-a%3ab").as_deref(), Some("2"));
    assert_eq!(header("x-snme-100%25").as_deref(), Some("3"));
}

#[tokio::test]
async fn batch_invalid_key_fails_only_that_operation() {
    let (session, requests, _) = mock_session().await;

    let results: Vec<_> = {
        use futures_util::StreamExt as _;
        session
            .many()
            .push(session.put("a").key("bad").append_metadata("no way", "x"))
            .push(session.put("b").key("good").append_metadata("Fine", "x"))
            .send()
            .await
            .collect()
            .await
    };

    assert_eq!(results.len(), 2);
    let bad = results
        .iter()
        .find_map(|r| match r {
            OperationResult::Put(key, Err(error)) if key == "bad" => Some(error),
            _ => None,
        })
        .expect("invalid operation to fail");
    assert_invalid_key(bad, "no way");

    // The valid operation was still sent (the mock rejects it), and not as an invalid key error.
    let good = results
        .iter()
        .find_map(|r| match r {
            OperationResult::Put(key, Err(error)) if key == "good" => Some(error),
            _ => None,
        })
        .expect("valid operation to fail against the mock");
    assert!(!matches!(good, Error::Metadata(_)));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}
