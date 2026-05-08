//! Integration tests for the multipart upload endpoints.

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
use objectstore_types::multipart::{
    CompleteErrorResponse, CompleteSuccessResponse, InitiateResponse, ListPartsResponse,
    UploadPartResponse,
};

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

/// Sends a complete request and returns the trimmed response body.
///
/// The complete endpoint uses a streaming response with keepalive whitespace, so we
/// trim the response body before returning.
async fn send_complete(
    client: &reqwest::Client,
    url: &str,
    parts: &[(&str, &str)],
) -> Result<String> {
    let parts_json: Vec<_> = parts
        .iter()
        .map(|(pn, etag)| {
            serde_json::json!({"part_number": pn.parse::<u32>().unwrap(), "etag": etag})
        })
        .collect();
    let body = serde_json::json!({ "parts": parts_json });

    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let text = response.text().await?;
    Ok(text.trim().to_string())
}

/// Sends a complete request and asserts success. Returns the key from the response.
async fn complete_and_assert(
    client: &reqwest::Client,
    url: &str,
    parts: &[(&str, &str)],
) -> Result<String> {
    let body = send_complete(client, url, parts).await?;
    let parsed: CompleteSuccessResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("expected success response, got {body:?}: {e}"))?;
    Ok(parsed.key)
}

// --- Initiate ---

#[tokio::test]
async fn test_initiate_put_with_key() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/my-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body: InitiateResponse = response.json().await?;
    assert_eq!(body.key, "my-key");
    assert!(!body.upload_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_initiate_post_generates_key() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .post(server.url("/v1/objects:multipart/test/org=1/"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body: InitiateResponse = response.json().await?;
    assert!(!body.key.is_empty());
    assert!(!body.upload_id.is_empty());

    Ok(())
}

// --- Full flow: initiate → upload parts → list → complete → GET ---

#[tokio::test]
async fn test_multipart_full_flow() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/full-flow-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;
    assert_eq!(initiate.key, "full-flow-key");

    // 2. Upload part 1
    let part1_data = b"hello ";
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/full-flow-key?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", part1_data.len().to_string())
        .body(part1_data.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let part1: UploadPartResponse = response.json().await?;
    assert!(!part1.etag.is_empty());

    // 3. Upload part 2
    let part2_data = b"world!";
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/full-flow-key?upload_id={upload_id}&part_number=2"
        )))
        .header("content-length", part2_data.len().to_string())
        .body(part2_data.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let part2: UploadPartResponse = response.json().await?;
    assert!(!part2.etag.is_empty());

    // 4. List parts
    let response = client
        .get(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/full-flow-key?upload_id={upload_id}"
        )))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let list: ListPartsResponse = response.json().await?;
    assert_eq!(list.parts.len(), 2);
    assert!(list.parts.contains_key(&1));
    assert!(list.parts.contains_key(&2));
    assert_eq!(list.parts[&1].size, part1_data.len() as u64);
    assert_eq!(list.parts[&2].size, part2_data.len() as u64);
    assert!(!list.is_truncated);

    // 5. Complete
    let key = complete_and_assert(
        &client,
        &server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/full-flow-key?upload_id={upload_id}"
        )),
        &[("1", &part1.etag), ("2", &part2.etag)],
    )
    .await?;
    assert_eq!(key, "full-flow-key");

    // 6. Verify object is accessible via regular GET
    let response = client
        .get(server.url("/v1/objects/test/org=1/full-flow-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let payload = response.bytes().await?;
    assert_eq!(&payload[..], b"hello world!");

    Ok(())
}

// --- Initiate with POST and auto-generated key, then complete ---

#[tokio::test]
async fn test_multipart_post_initiate_full_flow() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate with POST (server-generated key)
    let response = client
        .post(server.url("/v1/objects:multipart/test/org=1/"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;
    let key = &initiate.key;
    assert!(!key.is_empty());

    // 2. Upload a single part
    let data = b"auto-keyed content";
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/{key}?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", data.len().to_string())
        .body(data.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let part: UploadPartResponse = response.json().await?;

    // 3. Complete
    complete_and_assert(
        &client,
        &server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/{key}?upload_id={upload_id}"
        )),
        &[("1", &part.etag)],
    )
    .await?;

    // 4. Verify
    let response = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let payload = response.bytes().await?;
    assert_eq!(&payload[..], b"auto-keyed content");

    Ok(())
}

// --- Abort ---

#[tokio::test]
async fn test_multipart_abort() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/abort-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    // 2. Upload a part
    let data = b"will be aborted";
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/abort-key?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", data.len().to_string())
        .body(data.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // 3. Abort
    let response = client
        .delete(server.url(&format!(
            "/v1/objects:multipart/test/org=1/abort-key?upload_id={upload_id}"
        )))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    // 4. Object should not exist
    let response = client
        .get(server.url("/v1/objects/test/org=1/abort-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

// --- List parts with pagination ---

#[tokio::test]
async fn test_list_parts_pagination() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/paginated-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    // 2. Upload 3 parts
    for i in 1..=3 {
        let data = format!("part-{i}");
        let response = client
            .put(server.url(&format!(
                "/v1/objects:multipart:parts/test/org=1/paginated-key?upload_id={upload_id}&part_number={i}"
            )))
            .header("content-length", data.len().to_string())
            .body(data)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    // 3. List with max_parts=2 (should be truncated)
    let response = client
        .get(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/paginated-key?upload_id={upload_id}&max_parts=2"
        )))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let page1: ListPartsResponse = response.json().await?;
    assert_eq!(page1.parts.len(), 2);
    assert!(page1.is_truncated);
    assert!(page1.next_part_number_marker.is_some());

    // 4. List next page
    let marker = page1.next_part_number_marker.unwrap();
    let response = client
        .get(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/paginated-key?upload_id={upload_id}&max_parts=2&part_number_marker={marker}"
        )))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let page2: ListPartsResponse = response.json().await?;
    assert_eq!(page2.parts.len(), 1);
    assert!(!page2.is_truncated);

    Ok(())
}

// --- Part overwrite ---

#[tokio::test]
async fn test_upload_part_overwrite() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/overwrite-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    // 2. Upload part 1 with initial data
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/overwrite-key?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", "5")
        .body("first")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let first_etag: UploadPartResponse = response.json().await?;

    // 3. Overwrite part 1 with new data
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/overwrite-key?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", "6")
        .body("second")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let second_etag: UploadPartResponse = response.json().await?;

    // The etags should differ since the data changed
    assert_ne!(first_etag.etag, second_etag.etag);

    // 4. List parts — should show only the latest version
    let response = client
        .get(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/overwrite-key?upload_id={upload_id}"
        )))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let list: ListPartsResponse = response.json().await?;
    assert_eq!(list.parts.len(), 1);
    assert_eq!(list.parts[&1].etag, second_etag.etag);
    assert_eq!(list.parts[&1].size, 6);

    // 5. Complete with the overwritten part
    complete_and_assert(
        &client,
        &server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/overwrite-key?upload_id={upload_id}"
        )),
        &[("1", &second_etag.etag)],
    )
    .await?;

    // 6. Verify the final object has the overwritten data
    let response = client
        .get(server.url("/v1/objects/test/org=1/overwrite-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let payload = response.bytes().await?;
    assert_eq!(&payload[..], b"second");

    Ok(())
}

// --- Missing Content-Length on upload_part ---

#[tokio::test]
async fn test_upload_part_missing_content_length() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::builder().no_proxy().build()?;

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/no-cl-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    // 2. Upload part without Content-Length — reqwest normally adds it automatically for
    //    fixed-size bodies, so we use a streaming body to avoid that.
    let stream =
        futures_util::stream::once(async { Ok::<_, std::io::Error>(bytes::Bytes::from("data")) });
    let body = reqwest::Body::wrap_stream(stream);
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/no-cl-key?upload_id={upload_id}&part_number=1"
        )))
        .body(body)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn test_upload_part_zero_part_number() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/zero-part-upload"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/zero-part-upload?upload_id={upload_id}&part_number=0"
        )))
        .header("content-length", "4")
        .body("data")
        .send()
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

// --- Missing upload_id query param ---

#[tokio::test]
async fn test_missing_upload_id_on_complete() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let complete_body = serde_json::json!({
        "parts": [{"part_number": 1, "etag": "fake"}]
    });
    let response = client
        .post(server.url("/v1/objects:multipart:complete/test/org=1/some-key"))
        .header("content-type", "application/json")
        .body(complete_body.to_string())
        .send()
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn test_complete_zero_part_number() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/zero-part-complete"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    let response = client
        .post(server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/zero-part-complete?upload_id={upload_id}"
        )))
        .header("content-type", "application/json")
        .body(r#"{"parts":[{"part_number":0,"etag":"fake"}]}"#)
        .send()
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}

#[tokio::test]
async fn test_missing_upload_id_on_abort() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .delete(server.url("/v1/objects:multipart/test/org=1/some-key"))
        .send()
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

// --- Complete with invalid etag ---

#[tokio::test]
async fn test_complete_invalid_etag() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/bad-etag-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;

    // 2. Upload a part
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/bad-etag-key?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", "4")
        .body("data")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // 3. Complete with a wrong etag
    let body = send_complete(
        &client,
        &server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/bad-etag-key?upload_id={upload_id}"
        )),
        &[("1", "wrong-etag")],
    )
    .await?;

    let err: CompleteErrorResponse = serde_json::from_str(&body)?;
    assert!(!err.error.code.is_empty());
    assert!(!err.error.message.is_empty());

    // 4. Object should not exist
    let response = client
        .get(server.url("/v1/objects/test/org=1/bad-etag-key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

// --- Structured key with slashes ---

#[tokio::test]
async fn test_multipart_structured_key() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // 1. Initiate with a key containing slashes
    let response = client
        .put(server.url("/v1/objects:multipart/test/org=1/path/to/object"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let initiate: InitiateResponse = response.json().await?;
    let upload_id = &initiate.upload_id;
    assert_eq!(initiate.key, "path/to/object");

    // 2. Upload a part
    let data = b"structured key data";
    let response = client
        .put(server.url(&format!(
            "/v1/objects:multipart:parts/test/org=1/path/to/object?upload_id={upload_id}&part_number=1"
        )))
        .header("content-length", data.len().to_string())
        .body(data.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let part: UploadPartResponse = response.json().await?;

    // 3. Complete
    complete_and_assert(
        &client,
        &server.url(&format!(
            "/v1/objects:multipart:complete/test/org=1/path/to/object?upload_id={upload_id}"
        )),
        &[("1", &part.etag)],
    )
    .await?;

    // 4. Verify via regular GET
    let response = client
        .get(server.url("/v1/objects/test/org=1/path/to/object"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let payload = response.bytes().await?;
    assert_eq!(&payload[..], b"structured key data");

    Ok(())
}
