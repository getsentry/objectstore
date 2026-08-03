use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
use objectstore_types::metadata::Metadata;

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
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
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
async fn filename_with_unicode_roundtrips() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let filename = "réport-📄.pdf";

    // Non-ASCII travels percent-encoded; this is the raw wire form our clients send.
    let resp = client
        .put(server.url("/v1/objects/test/org=1/cd-unicode"))
        .header("x-sn-filename", "r%C3%A9port-%F0%9F%93%84.pdf")
        .body("data")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-unicode"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Read the filename back through the metadata parser rather than off the raw
    // header, so the assertion holds regardless of how the value is encoded on the wire.
    let metadata = Metadata::from_headers(resp.headers(), "")?;
    assert_eq!(metadata.filename.as_deref(), Some(filename));

    // The wire value is escaped, so it survives as visible ASCII.
    assert_eq!(
        resp.headers().get("x-sn-filename").unwrap(),
        "r%C3%A9port-%F0%9F%93%84.pdf",
    );

    // Non-ASCII needs the RFC 8187 form, with the quoted-string as the ASCII fallback.
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        "attachment; filename=\"r_port-_.pdf\"; \
         filename*=UTF-8''r%C3%A9port-%F0%9F%93%84.pdf",
    );

    Ok(())
}

#[tokio::test]
async fn custom_metadata_with_unicode_roundtrips() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let release = "vérsion-1.0-🚀";

    // Non-ASCII travels percent-encoded, as does a literal percent sign.
    let resp = client
        .put(server.url("/v1/objects/test/org=1/meta-unicode"))
        .header("x-snme-release", "v%C3%A9rsion-1.0-%F0%9F%9A%80")
        .header("x-snme-note", "100%25 done")
        .body("data")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    for resp in [
        client
            .get(server.url("/v1/objects/test/org=1/meta-unicode"))
            .send()
            .await?,
        client
            .head(server.url("/v1/objects/test/org=1/meta-unicode"))
            .send()
            .await?,
    ] {
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let metadata = Metadata::from_headers(resp.headers(), "")?;
        assert_eq!(
            metadata.custom.get("release").map(String::as_str),
            Some(release)
        );
        assert_eq!(
            metadata.custom.get("note").map(String::as_str),
            Some("100% done"),
        );

        // Escaped on the wire, including the literal percent sign.
        assert_eq!(
            resp.headers().get("x-snme-release").unwrap(),
            "v%C3%A9rsion-1.0-%F0%9F%9A%80",
        );
        assert_eq!(resp.headers().get("x-snme-note").unwrap(), "100%25 done");
    }

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
