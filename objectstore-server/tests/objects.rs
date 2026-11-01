use std::time::Duration;

use anyhow::Result;
use http::{Method, StatusCode};
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::Url;

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
async fn object_without_filename_is_served_as_attachment() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .put(server.url("/v1/objects/test/org=1/cd-no-filename"))
        .body("data")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(server.url("/v1/objects/test/org=1/cd-no-filename"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        "attachment"
    );

    let resp = client
        .head(server.url("/v1/objects/test/org=1/cd-no-filename"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-disposition").unwrap(),
        "attachment"
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
async fn patch_extends_expiry() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();
    let url = server.url("/v1/objects/test/org=1/extend-expiry");
    let response = client
        .put(&url)
        .header("x-sn-expiration", "ttl:1d")
        .body("payload")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let response = client.head(&url).send().await?;
    let before = Metadata::from_headers(response.headers(), "")?;
    let expected_expiry = before
        .time_created
        .unwrap()
        .checked_add(Duration::from_secs(30 * 24 * 60 * 60))
        .unwrap();

    let response = client
        .patch(&url)
        .json(&serde_json::json!({"extend_expiry": {"after": "30d", "from": "creation"}}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let response = client.head(&url).send().await?;
    let after = Metadata::from_headers(response.headers(), "")?;
    assert_eq!(
        after.expiration_policy,
        ExpirationPolicy::TimeToLive(Duration::from_secs(30 * 24 * 60 * 60))
    );
    assert_eq!(after.time_expires, Some(expected_expiry));

    Ok(())
}

#[tokio::test]
async fn patch_missing_object_returns_not_found() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();
    let url = server.url("/v1/objects/test/org=1/missing-expiry");
    let response = client
        .patch(&url)
        .json(&serde_json::json!({"extend_expiry": {"after": "1d", "from": "now"}}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(response.bytes().await?.is_empty());
    assert_eq!(
        client.get(&url).send().await?.status(),
        reqwest::StatusCode::NOT_FOUND
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

#[tokio::test]
async fn successful_downloads_apply_options_and_preserve_metadata() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();
    let path = "/v1/objects/test/org=1/options";
    client
        .put(server.url(path))
        .header("content-type", "text/html")
        .header("x-sn-filename", "report.html")
        .body("<html>example</html>")
        .send()
        .await?
        .error_for_status()?;

    for method in [Method::GET, Method::HEAD] {
        for disposition in [None, Some("attachment"), Some("inline")] {
            let mut url = Url::parse(&server.url(path))?;
            if let Some(disposition) = disposition {
                url.query_pairs_mut()
                    .append_pair("response-content-disposition", disposition);
            }
            let response = client.request(method.clone(), url).send().await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["content-type"], "text/html");
            assert_eq!(response.headers()["x-sn-filename"], "report.html");
            assert_eq!(response.headers()["content-length"], "20");
            assert_eq!(
                response.headers()["content-disposition"],
                format!(
                    "{}; filename=\"report.html\"",
                    disposition.unwrap_or("attachment")
                )
            );
            assert_eq!(response.headers()["x-content-type-options"], "nosniff");
            assert_eq!(response.headers()["referrer-policy"], "no-referrer");
            assert_eq!(
                response
                    .headers()
                    .get("content-security-policy")
                    .map(|v| v.to_str().unwrap()),
                (disposition == Some("inline")).then_some("sandbox")
            );
            let body = response.text().await?;
            assert_eq!(
                body,
                if method == Method::HEAD {
                    ""
                } else {
                    "<html>example</html>"
                }
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn ranged_and_error_responses() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();
    let path = "/v1/objects/test/org=1/range-options";
    client
        .put(server.url(path))
        .body("abcdef")
        .send()
        .await?
        .error_for_status()?;
    let mut url = Url::parse(&server.url(path))?;
    url.set_query(Some("response-content-disposition=inline"));
    let response = client
        .get(url.clone())
        .header("range", "bytes=1-3")
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-range"], "bytes 1-3/6");
    assert_eq!(response.headers()["content-length"], "3");
    assert_eq!(response.headers()["content-disposition"], "inline");
    assert_eq!(response.headers()["content-security-policy"], "sandbox");
    assert_eq!(response.headers()["referrer-policy"], "no-referrer");
    assert_eq!(response.text().await?, "bcd");

    for (target, range, expected) in [
        (url.clone(), "bytes=99-", StatusCode::RANGE_NOT_SATISFIABLE),
        (
            {
                let mut missing = url.clone();
                missing.set_path("/v1/objects/test/org=1/missing-options");
                missing
            },
            "bytes=0-",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = client.get(target).header("range", range).send().await?;
        assert_eq!(response.status(), expected);
        for name in [
            "content-disposition",
            "x-content-type-options",
            "content-security-policy",
            "referrer-policy",
        ] {
            assert!(
                !response.headers().contains_key(name),
                "unexpected {name} on {expected}"
            );
        }
    }
    url.set_path("/v1/objects/test/org=1/missing-options");
    let response = client.head(url).send().await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    for name in [
        "content-disposition",
        "x-content-type-options",
        "content-security-policy",
        "referrer-policy",
    ] {
        assert!(!response.headers().contains_key(name));
    }

    Ok(())
}

#[tokio::test]
async fn invalid_and_duplicate_options_are_rejected() -> Result<()> {
    let server = test_server().await;
    let client = reqwest::Client::new();
    for query in [
        "response-content-disposition=other",
        "response-content-disposition=inline%3Bfilename%3Dx",
        "response-content-disposition=inline&RESPONSE-CONTENT-DISPOSITION=attachment",
    ] {
        let mut url = Url::parse(&server.url("/v1/objects/test/org=1/invalid-options"))?;
        url.set_query(Some(query));
        assert_eq!(
            client.get(url).send().await?.status(),
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    Ok(())
}
