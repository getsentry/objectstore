//! Integration tests for the resumable upload endpoints.
//!
//! No backend implements resumable uploads yet, so the reachable surface is session denial
//! and request validation. That is deliberate: a deployment must answer `409 Conflict` to
//! every session creation so clients fall back to a regular upload, and it must reject a
//! malformed request before it reaches a backend.
//!
//! The `501 Not Implemented` assertions are the proof that dispatch and header parsing work:
//! the only way to reach a declining backend method is through a well-formed request.

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
use objectstore_types::resumable::{HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET};
use reqwest::StatusCode;

async fn test_server() -> TestServer {
    TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

// --- Session creation ---

#[tokio::test]
async fn create_session_is_denied_with_client_key() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
        .header(HEADER_UPLOAD_LENGTH, "1048576")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test]
async fn create_session_is_denied_with_generated_key() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .post(server.url("/v1/objects/test/org=1/?upload_type=resumable"))
        .header(HEADER_UPLOAD_LENGTH, "1048576")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test]
async fn create_session_requires_upload_length() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn create_session_rejects_malformed_upload_length() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    for invalid in ["", "-1", "1.5", "lots"] {
        let response = client
            .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
            .header(HEADER_UPLOAD_LENGTH, invalid)
            .send()
            .await?;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "accepted {HEADER_UPLOAD_LENGTH}: {invalid:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn unknown_upload_type_is_rejected() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=multipart"))
        .header(HEADER_UPLOAD_LENGTH, "1048576")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// --- Chunks and offset queries ---

#[tokio::test]
async fn chunk_reaches_the_declining_backend() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
        .header(HEADER_UPLOAD_OFFSET, "0")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    Ok(())
}

#[tokio::test]
async fn offset_query_reaches_the_declining_backend() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
        .header(HEADER_UPLOAD_OFFSET, "*")
        .header(reqwest::header::CONTENT_LENGTH, "0")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    Ok(())
}

#[tokio::test]
async fn chunk_requires_upload_offset() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn chunk_rejects_malformed_upload_offset() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    for invalid in ["", "-1", "1.5", "**", "here"] {
        let response = client
            .put(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
            .header(HEADER_UPLOAD_OFFSET, invalid)
            .body("payload")
            .send()
            .await?;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "accepted {HEADER_UPLOAD_OFFSET}: {invalid:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn offset_query_rejects_a_payload() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
        .header(HEADER_UPLOAD_OFFSET, "*")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn session_token_with_path_traversal_is_rejected() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=../escape"))
        .header(HEADER_UPLOAD_OFFSET, "0")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// --- Termination ---

#[tokio::test]
async fn terminate_reaches_the_declining_backend() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .delete(server.url("/v1/objects/test/org=1/my-key?session=some-token"))
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    Ok(())
}

#[tokio::test]
async fn delete_rejects_upload_type() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .delete(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// --- Parameter combinations ---

#[tokio::test]
async fn upload_type_and_session_are_mutually_exclusive() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable&session=some-token"))
        .header(HEADER_UPLOAD_LENGTH, "1048576")
        .header(HEADER_UPLOAD_OFFSET, "0")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn session_on_the_collection_route_is_rejected() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .post(server.url("/v1/objects/test/org=1/?session=some-token"))
        .header(HEADER_UPLOAD_OFFSET, "0")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// --- Regular uploads are unaffected ---

#[tokio::test]
async fn regular_object_operations_still_work() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .put(server.url("/v1/objects/test/org=1/my-key"))
        .body("payload")
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .get(server.url("/v1/objects/test/org=1/my-key"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await?, "payload");

    let response = client
        .delete(server.url("/v1/objects/test/org=1/my-key"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    Ok(())
}

#[tokio::test]
async fn regular_upload_ignores_resumable_headers() -> Result<()> {
    let server = test_server().await;

    // Without a query parameter the request is a regular upload, and the protocol headers
    // carry no meaning. They must not accidentally engage the resumable path.
    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key"))
        .header(HEADER_UPLOAD_LENGTH, "7")
        .header(HEADER_UPLOAD_OFFSET, "0")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
