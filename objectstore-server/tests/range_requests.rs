//! End-to-end tests for HTTP Range request support.

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;

async fn setup() -> (TestServer, String) {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = "Hello, Range Requests!"; // 22 bytes

    let resp = client
        .post(server.url("/v1/objects/test/org=1/"))
        .header("content-type", "text/plain")
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    let key = body["key"].as_str().unwrap().to_string();

    (server, key)
}

#[tokio::test]
async fn no_range_returns_200_with_accept_ranges() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("accept-ranges").unwrap().to_str()?,
        "bytes"
    );
    assert!(
        resp.headers().get("content-range").is_none(),
        "200 response must not include Content-Range"
    );

    let body = resp.text().await?;
    assert_eq!(body, "Hello, Range Requests!");
    Ok(())
}

#[tokio::test]
async fn range_prefix_returns_206() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=0-4")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("accept-ranges").unwrap().to_str()?,
        "bytes"
    );
    assert_eq!(
        resp.headers().get("content-range").unwrap().to_str()?,
        "bytes 0-4/22"
    );
    assert_eq!(resp.headers().get("content-length").unwrap().to_str()?, "5");

    let body = resp.text().await?;
    assert_eq!(body, "Hello");
    Ok(())
}

#[tokio::test]
async fn range_suffix_returns_206() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=-9")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap().to_str()?,
        "bytes 13-21/22"
    );

    let body = resp.text().await?;
    assert_eq!(body, "Requests!");
    Ok(())
}

#[tokio::test]
async fn range_from_offset_returns_206() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=7-")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap().to_str()?,
        "bytes 7-21/22"
    );
    assert_eq!(
        resp.headers().get("content-length").unwrap().to_str()?,
        "15"
    );

    let body = resp.text().await?;
    assert_eq!(body, "Range Requests!");
    Ok(())
}

#[tokio::test]
async fn unknown_range_unit_returns_400() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "items=0-10")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn invalid_bytes_range_returns_400() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=abc-def")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn multi_range_falls_back_to_full_body() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=0-4, 10-14")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await?;
    assert_eq!(body, "Hello, Range Requests!");
    Ok(())
}

#[tokio::test]
async fn unsatisfiable_range_returns_416() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=100-200")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        resp.headers().get("content-range").unwrap().to_str()?,
        "bytes */22"
    );
    assert_eq!(
        resp.headers().get("accept-ranges").unwrap().to_str()?,
        "bytes"
    );
    Ok(())
}

#[tokio::test]
async fn range_on_nonexistent_object_returns_404() -> Result<()> {
    let (server, _key) = setup().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .header("range", "bytes=0-10")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn full_range_returns_200() -> Result<()> {
    let (server, key) = setup().await;
    let client = reqwest::Client::new();

    // Request the full object as a range — should still get 200 since it's the full content.
    let resp = client
        .get(server.url(&format!("/v1/objects/test/org=1/{key}")))
        .header("range", "bytes=0-21")
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        resp.headers().get("content-range").is_none(),
        "full-object range should be 200 without Content-Range"
    );

    let body = resp.text().await?;
    assert_eq!(body, "Hello, Range Requests!");
    Ok(())
}
