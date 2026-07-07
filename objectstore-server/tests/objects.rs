use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;

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

#[tokio::test]
async fn filename_produces_content_disposition() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .put(server.url("/v1/objects/test/org=1/cd-key"))
        .header("x-sn-filename", "report.pdf")
        .body("data")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // GET includes both x-sn-filename and Content-Disposition
    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-key"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.headers().get("x-sn-filename").unwrap(), "report.pdf");
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="report.pdf""#,
    );

    // HEAD includes both x-sn-filename and Content-Disposition
    let resp = client
        .head(server.url("/v1/objects/test/org=1/cd-key"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(resp.headers().get("x-sn-filename").unwrap(), "report.pdf");
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="report.pdf""#,
    );

    Ok(())
}

#[tokio::test]
async fn filename_with_quotes_is_escaped() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    client
        .put(server.url("/v1/objects/test/org=1/cd-quotes"))
        .header("x-sn-filename", r#"has"quote.txt"#)
        .body("data")
        .send()
        .await?;

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-quotes"))
        .send()
        .await?;
    assert_eq!(
        resp.headers().get("x-sn-filename").unwrap(),
        r#"has"quote.txt"#,
    );
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="has\"quote.txt""#,
    );

    Ok(())
}

#[tokio::test]
async fn filename_with_slashes_is_sanitized() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    client
        .put(server.url("/v1/objects/test/org=1/cd-slashes"))
        .header("x-sn-filename", "path/to/file.txt")
        .body("data")
        .send()
        .await?;

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-slashes"))
        .send()
        .await?;
    // Raw filename preserved in metadata header
    assert_eq!(
        resp.headers().get("x-sn-filename").unwrap(),
        "path/to/file.txt",
    );
    // Content-Disposition has slashes sanitized to dashes
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="path-to-file.txt""#,
    );

    Ok(())
}

#[tokio::test]
async fn filename_dot_and_dotdot_are_sanitized() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    // Single dot
    client
        .put(server.url("/v1/objects/test/org=1/cd-dot"))
        .header("x-sn-filename", ".")
        .body("data")
        .send()
        .await?;

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-dot"))
        .send()
        .await?;
    assert_eq!(resp.headers().get("x-sn-filename").unwrap(), ".");
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="-""#,
    );

    // Double dot
    client
        .put(server.url("/v1/objects/test/org=1/cd-dotdot"))
        .header("x-sn-filename", "..")
        .body("data")
        .send()
        .await?;

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-dotdot"))
        .send()
        .await?;
    assert_eq!(resp.headers().get("x-sn-filename").unwrap(), "..");
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="--""#,
    );

    Ok(())
}
