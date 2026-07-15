//! End-to-end tests for the `os_auth` query parameter authentication path.
//!
//! These run against a server with auth enforcement enabled. A JWT can be
//! supplied either via the `x-os-auth` header or, base64url-encoded, via the
//! `os_auth` query parameter. The header takes precedence when both are present.

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::header;
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TestServer};
use serde::Serialize;

/// Object path used across the tests: usecase `test`, scope `org=1`, key `query-auth-key`.
const OBJECT_PATH: &str = "/v1/objects/test/org=1/query-auth-key";

async fn test_server() -> TestServer {
    TestServer::with_config(Config {
        auth: AuthZ {
            enforce: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

#[derive(Serialize)]
struct Res {
    #[serde(rename = "os:usecase")]
    usecase: &'static str,
    org: &'static str,
}

#[derive(Serialize)]
struct Claims {
    exp: u64,
    res: Res,
    permissions: Vec<&'static str>,
}

/// Builds a JWT for `test`/`org=1` with the given permissions.
fn jwt(permissions: Vec<&'static str>) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};

    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(TEST_EDDSA_KID.into());
    header.typ = Some("JWT".into());

    let claims = Claims {
        exp: get_current_timestamp() + 300,
        res: Res {
            usecase: "test",
            org: "1",
        },
        permissions,
    };

    let key = EncodingKey::from_ed_pem(TEST_EDDSA_PRIVKEY.as_bytes()).unwrap();
    encode(&header, &claims, &key).unwrap()
}

/// Seeds the object at [`OBJECT_PATH`] with the given body via an authorized `PUT`.
async fn seed_object(server: &TestServer, body: &'static str) -> Result<()> {
    let resp = reqwest::Client::new()
        .put(server.url(OBJECT_PATH))
        .header(
            header::AUTHORIZATION.as_str(),
            format!(
                "Bearer {}",
                jwt(vec!["object.read", "object.write", "object.delete"])
            ),
        )
        .body(body)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_auth_get_succeeds() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    let token = URL_SAFE_NO_PAD.encode(jwt(vec!["object.read"]));
    let url = format!("{}?os_auth={token}", server.url(OBJECT_PATH));
    let resp = reqwest::Client::new().get(url).send().await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "hello");
    Ok(())
}

#[tokio::test]
async fn query_auth_garbage_is_bad_request() -> Result<()> {
    let server = test_server().await;

    let url = format!("{}?os_auth=not%20base64%21%21", server.url(OBJECT_PATH));
    let resp = reqwest::Client::new().get(url).send().await?;

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn query_auth_tampered_token_is_unauthorized() -> Result<()> {
    let server = test_server().await;

    // Valid base64url, but the decoded JWT has a broken signature.
    let mut token = jwt(vec!["object.read"]);
    let last = token.pop().unwrap();
    token.push(if last == 'A' { 'B' } else { 'A' });
    let token = URL_SAFE_NO_PAD.encode(token);

    let url = format!("{}?os_auth={token}", server.url(OBJECT_PATH));
    let resp = reqwest::Client::new().get(url).send().await?;

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn header_takes_precedence_over_query() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    // Valid header token, garbage query token: the header must win, so the
    // request succeeds despite the unusable query value.
    let url = format!("{}?os_auth=not%20base64%21%21", server.url(OBJECT_PATH));
    let resp = reqwest::Client::new()
        .get(url)
        .header(
            header::AUTHORIZATION.as_str(),
            format!("Bearer {}", jwt(vec!["object.read"])),
        )
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "hello");
    Ok(())
}
