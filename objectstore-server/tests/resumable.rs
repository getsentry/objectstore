//! Integration tests for the resumable upload endpoints.
//!
//! No backend implements resumable uploads yet. Until a supporting test backend exists, these
//! tests cover request validation and ensure regular object requests remain unaffected.

use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
use objectstore_types::resumable::{HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET};
use reqwest::StatusCode;

/// Unpadded base64url for the opaque backend token `some-token`.
const SESSION: &str = "c29tZS10b2tlbg";

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

/// Sends a raw HTTP/1.1 `PUT`, preserving the caller's exact body framing headers.
async fn raw_put(server: &TestServer, path: &str, headers: &str, body: &str) -> Result<String> {
    let url = reqwest::Url::parse(&server.url(path))?;
    let host = server.host();
    let port = server.port();
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    let headers = headers.to_owned();
    let body = body.to_owned();

    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut stream = TcpStream::connect((host, port))?;
        write!(
            stream,
            "PUT {target} HTTP/1.1\r\nHost: {host}:{port}\r\n{headers}Connection: close\r\n\r\n{body}"
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

// --- Chunks and offset queries ---

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
async fn upload_type_and_session_are_mutually_exclusive() -> Result<()> {
    let server = test_server().await;

    let response = reqwest::Client::new()
        .put(server.url(&format!(
            "/v1/objects/test/org=1/my-key?upload_type=resumable&session={SESSION}"
        )))
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
