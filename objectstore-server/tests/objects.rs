use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;

#[tokio::test]
async fn filename_produces_content_disposition() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let client = reqwest::Client::new();

    let resp = client
        .put(server.url("/v1/objects/test/org=1/cd-key"))
        .header("x-sn-filename", "report.pdf")
        .body("data")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // GET includes Content-Disposition
    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-key"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="report.pdf""#,
    );

    // HEAD includes Content-Disposition
    let resp = client
        .head(server.url("/v1/objects/test/org=1/cd-key"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        r#"attachment; filename="report.pdf""#,
    );

    Ok(())
}
