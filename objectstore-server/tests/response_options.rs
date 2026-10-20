//! Response overrides on successful downloads, with error behavior left unchanged.

use anyhow::Result;
use http::{Method, StatusCode};
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::TestServer;
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
            url.query_pairs_mut().append_pair(
                "response-cache-control",
                "private, max-age=3600, x=\"a&b=+%?#\"",
            );
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
            assert_eq!(
                response.headers()["cache-control"],
                "private, max-age=3600, x=\"a&b=+%?#\""
            );
            assert_eq!(response.headers()["x-content-type-options"], "nosniff");
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
    let response = client.get(server.url(path)).send().await?;
    assert!(!response.headers().contains_key("cache-control"));
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
    url.set_query(Some(
        "response-content-disposition=inline&response-cache-control=public%2C%20max-age%3D3600",
    ));
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
            "cache-control",
            "x-content-type-options",
            "content-security-policy",
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
        "cache-control",
        "x-content-type-options",
        "content-security-policy",
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
        "response-cache-control=public%0D%0AX-Evil%3Ayes",
        "response-cache-control=%00",
        "response-content-disposition=inline&RESPONSE-CONTENT-DISPOSITION=attachment",
        "response-cache-control=public&%72esponse-cache-control=private",
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
