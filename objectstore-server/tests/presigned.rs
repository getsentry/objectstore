//! End-to-end tests for pre-signed URL authentication.
//!
//! These run against a server with auth enforcement enabled. The test server registers the
//! [`TEST_EDDSA_KID`] key, so we can seed objects with a JWT and then exercise the pre-signed
//! URL path with the matching private key.

use std::time::{Duration, SystemTime};

use anyhow::Result;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use http::{HeaderMap, HeaderName, HeaderValue, Method, header};
use objectstore_server::config::{AuthZ, Config};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TestServer};
use objectstore_types::presign::{
    CanonicalRequest, X_OS_EXPIRES, X_OS_KEY_ID, X_OS_SIG, X_OS_SIGNED_HEADERS, X_OS_TIMESTAMP,
};
use serde::Serialize;

/// Object path used across the tests: usecase `test`, scope `org=1`, key `presign-key`.
const OBJECT_PATH: &str = "/v1/objects/test/org=1/presign-key";
const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

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

fn signing_key() -> SigningKey {
    SigningKey::from_pkcs8_pem(TEST_EDDSA_PRIVKEY).unwrap()
}

#[derive(Serialize)]
struct SeedRes {
    #[serde(rename = "os:usecase")]
    usecase: &'static str,
    org: &'static str,
}

#[derive(Serialize)]
struct SeedClaims {
    exp: u64,
    res: SeedRes,
    permissions: Vec<&'static str>,
}

/// Builds a JWT granting full permissions on `test`/`org=1`, used to seed fixtures via `PUT`
/// (pre-signed writes are rejected, so we authorize the write with a token instead).
fn seed_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};

    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(TEST_EDDSA_KID.into());
    header.typ = Some("JWT".into());

    let claims = SeedClaims {
        exp: get_current_timestamp() + 300,
        res: SeedRes {
            usecase: "test",
            org: "1",
        },
        permissions: vec!["object.read", "object.write", "object.delete"],
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
            format!("Bearer {}", seed_jwt()),
        )
        .body(body)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

/// Signs a pre-signed request and returns the full query string (including `X-Os-Sig`), signed
/// with the given `key`. When `host` is set, the `host` header is included in the signature.
fn presign_query_with_key(
    key: &SigningKey,
    method: &Method,
    path: &str,
    timestamp: SystemTime,
    expires: Duration,
    host: Option<&str>,
) -> String {
    let ts = humantime::format_rfc3339(timestamp).to_string();
    let mut base = format!(
        "{X_OS_KEY_ID}={TEST_EDDSA_KID}&{X_OS_TIMESTAMP}={ts}&{X_OS_EXPIRES}={}",
        expires.as_secs()
    );

    let mut headers = HeaderMap::new();
    let mut signed_headers = Vec::new();
    if let Some(host) = host {
        base.push_str(&format!("&{X_OS_SIGNED_HEADERS}=host"));
        headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
        signed_headers.push(HeaderName::from_static("host"));
    }
    let signed_refs: Vec<&HeaderName> = signed_headers.iter().collect();

    let canonical =
        CanonicalRequest::new(method, path, Some(&base), &headers, &signed_refs).unwrap();
    let signature = canonical.sign(key);
    format!("{base}&{X_OS_SIG}={signature}")
}

/// Signs a valid pre-signed request with the test key.
fn presign_query(method: &Method, path: &str, expires: Duration) -> String {
    presign_query_with_key(
        &signing_key(),
        method,
        path,
        SystemTime::now(),
        expires,
        None,
    )
}

/// Builds a full request URL for `path` with the given query string.
fn presigned_url(server: &TestServer, path: &str, query: &str) -> String {
    format!("{}?{}", server.url(path), query)
}

#[tokio::test]
async fn presigned_get_succeeds() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    let query = presign_query(&Method::GET, OBJECT_PATH, ONE_HOUR);
    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "hello");
    Ok(())
}

#[tokio::test]
async fn presigned_head_succeeds() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    let query = presign_query(&Method::HEAD, OBJECT_PATH, ONE_HOUR);
    let resp = reqwest::Client::new()
        .head(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    Ok(())
}

#[tokio::test]
async fn presigned_delete_succeeds_then_object_is_gone() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    let delete_query = presign_query(&Method::DELETE, OBJECT_PATH, ONE_HOUR);
    let resp = reqwest::Client::new()
        .delete(presigned_url(&server, OBJECT_PATH, &delete_query))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // A subsequent pre-signed GET should now 404.
    let get_query = presign_query(&Method::GET, OBJECT_PATH, ONE_HOUR);
    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &get_query))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn presigned_get_with_signed_host_succeeds() -> Result<()> {
    let server = test_server().await;
    seed_object(&server, "hello").await?;

    // `server.url` renders `http://localhost:<port>/...`; the authority is what reqwest sends
    // as the `Host` header, so sign that exact value.
    let url = server.url(OBJECT_PATH);
    let host = url
        .strip_prefix("http://")
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();

    let query = presign_query_with_key(
        &signing_key(),
        &Method::GET,
        OBJECT_PATH,
        SystemTime::now(),
        ONE_HOUR,
        Some(&host),
    );
    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "hello");
    Ok(())
}

#[tokio::test]
async fn presigned_get_with_tampered_signature_is_unauthorized() -> Result<()> {
    let server = test_server().await;

    let query = presign_query(&Method::GET, OBJECT_PATH, ONE_HOUR);
    // Flip the last character of the signature.
    let mut tampered = query;
    let last = tampered.pop().unwrap();
    tampered.push(if last == 'A' { 'B' } else { 'A' });

    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &tampered))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn presigned_get_with_wrong_key_is_unauthorized() -> Result<()> {
    let server = test_server().await;

    // Sign with a key that is not the registered test key.
    let wrong_key = SigningKey::from_bytes(&[0x11; 32]);
    let query = presign_query_with_key(
        &wrong_key,
        &Method::GET,
        OBJECT_PATH,
        SystemTime::now(),
        ONE_HOUR,
        None,
    );

    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn presigned_get_expired_is_unauthorized() -> Result<()> {
    let server = test_server().await;

    // Signed two hours ago with a one-hour validity window: already expired.
    let timestamp = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
    let query = presign_query_with_key(
        &signing_key(),
        &Method::GET,
        OBJECT_PATH,
        timestamp,
        ONE_HOUR,
        None,
    );

    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn presigned_get_over_one_week_validity_is_bad_request() -> Result<()> {
    let server = test_server().await;

    // Eight days exceeds the one-week cap.
    let expires = Duration::from_secs(8 * 24 * 60 * 60);
    let query = presign_query(&Method::GET, OBJECT_PATH, expires);

    let resp = reqwest::Client::new()
        .get(presigned_url(&server, OBJECT_PATH, &query))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn presigned_put_is_rejected() -> Result<()> {
    let server = test_server().await;

    let query = presign_query(&Method::PUT, OBJECT_PATH, ONE_HOUR);
    let resp = reqwest::Client::new()
        .put(presigned_url(&server, OBJECT_PATH, &query))
        .body("data")
        .send()
        .await?;

    // Pre-signed URLs are only supported for GET/HEAD/DELETE.
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}
