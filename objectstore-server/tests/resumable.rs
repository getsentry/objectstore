//! Integration tests for the resumable upload endpoints.
//!
//! The test server uses its default filesystem backend, which does not implement resumable
//! uploads. These tests cover request validation and ensure regular object requests remain
//! unaffected. Backend behavior is covered in the service and backend test suites.
//! TODO: Add end-to-end resumable upload coverage once the filesystem backend supports it.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config, ResumableTokenEncryptionConfig, Service};
use objectstore_test::server::TestServer;
use objectstore_types::resumable::{HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET};
use reqwest::StatusCode;
use tempfile::NamedTempFile;

/// Unpadded base64url for the opaque backend token `some-token`.
const SESSION: &str = "c29tZS10b2tlbg";

/// Protected `some-token`, bound to `test/org.1/objects/my-key` with the test key below.
const PROTECTED_SESSION: &str = "BHRlc3QAAAAAAAAAAAAAAAAa-tfGlOBqj4RY_wWwE2mUn5vMigfzxClxO7HnIItwA469Onvui0aVocPaU7rFKckt5cYr1ohbg6xApSYkWJYXtqNEDcZQxks39a8Pobn7tlSglUyTbMHmtQ";

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

async fn test_server_with_protected_session() -> Result<TestServer> {
    let mut key_file = NamedTempFile::new()?;
    key_file.write_all(&[7; 32])?;

    Ok(TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        service: Service {
            resumable_token_encryption: Some(ResumableTokenEncryptionConfig {
                active_key_id: "test".into(),
                key_files: BTreeMap::from([("test".into(), key_file.path().into())]),
            }),
            ..Default::default()
        },
        ..Default::default()
    })
    .await)
}

/// Sends a raw HTTP/1.1 `PUT`, preserving the caller's exact body framing headers.
async fn raw_put(server: &TestServer, path: &str, headers: &str, body: &str) -> Result<String> {
    let url = reqwest::Url::parse(&server.url(path))?;
    let ip = server.ip();
    let port = server.port();
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    let headers = headers.to_owned();
    let body = body.to_owned();

    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut stream = TcpStream::connect((ip, port))?;
        write!(
            stream,
            "PUT {target} HTTP/1.1\r\nHost: {ip}:{port}\r\n{headers}Connection: close\r\n\r\n{body}"
        )?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    })
    .await?
}

// --- Session creation ---

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
async fn create_session_rejects_a_payload() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
        .header(HEADER_UPLOAD_LENGTH, "7")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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

#[tokio::test]
async fn declined_session_creation_returns_not_implemented() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?upload_type=resumable"))
        .header(HEADER_UPLOAD_LENGTH, "1048576")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    Ok(())
}

// --- Chunks and offset queries ---

#[tokio::test]
async fn offset_query_does_not_require_content_length() -> Result<()> {
    let server = test_server_with_protected_session().await?;
    let response = raw_put(
        &server,
        &format!("/v1/objects/test/org=1/my-key?session={PROTECTED_SESSION}"),
        &format!("{HEADER_UPLOAD_OFFSET}: *\r\n"),
        "",
    )
    .await?;

    assert!(
        response.starts_with("HTTP/1.1 501 Not Implemented\r\n"),
        "unexpected response: {response}"
    );
    Ok(())
}

#[tokio::test]
async fn offset_query_rejects_chunked_body_without_content_length() -> Result<()> {
    let server = test_server().await;
    let response = raw_put(
        &server,
        &format!("/v1/objects/test/org=1/my-key?session={SESSION}"),
        &format!("{HEADER_UPLOAD_OFFSET}: *\r\nTransfer-Encoding: chunked\r\n"),
        "7\r\npayload\r\n0\r\n\r\n",
    )
    .await?;

    assert!(
        response.starts_with("HTTP/1.1 400 Bad Request\r\n"),
        "unexpected response: {response}"
    );
    Ok(())
}

#[tokio::test]
async fn chunk_requires_upload_offset() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url(&format!("/v1/objects/test/org=1/my-key?session={SESSION}")))
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
            .put(server.url(&format!("/v1/objects/test/org=1/my-key?session={SESSION}")))
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
        .put(server.url(&format!("/v1/objects/test/org=1/my-key?session={SESSION}")))
        .header(HEADER_UPLOAD_OFFSET, "*")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn session_token_requires_base64url() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?session=%25%25%25"))
        .header(HEADER_UPLOAD_OFFSET, "0")
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// --- Cancellation ---

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
async fn session_takes_precedence_over_upload_type() -> Result<()> {
    let server = test_server_with_protected_session().await?;

    let response = reqwest::Client::new()
        .put(server.url(&format!(
            "/v1/objects/test/org=1/my-key?upload_type=resumable&session={PROTECTED_SESSION}"
        )))
        .header(HEADER_UPLOAD_OFFSET, "*")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    Ok(())
}

#[tokio::test]
async fn session_on_the_collection_route_is_rejected() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .post(server.url(&format!("/v1/objects/test/org=1/?session={SESSION}")))
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

#[tokio::test]
async fn regular_upload_ignores_unrelated_query_parameters() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url("/v1/objects/test/org=1/my-key?unrelated=value"))
        .body("payload")
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
