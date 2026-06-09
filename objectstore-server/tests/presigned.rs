use std::collections::{BTreeMap, HashSet};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TestServer, config};
use objectstore_types::auth::Permission;
use secrecy::SecretBox;
use serde::{Deserialize, Serialize};

const PRESIGNED_KID: &str = "presigned-test";
const PRESIGNED_SECRET: &str = "presigned-secret-for-tests";

#[derive(Deserialize)]
struct PresignResponse {
    signature: String,
    expires_at: u64,
    operation: String,
}

#[derive(Serialize)]
struct JwtClaims {
    exp: u64,
    permissions: HashSet<Permission>,
    res: JwtRes,
}

#[derive(Serialize)]
struct JwtRes {
    #[serde(rename = "os:usecase")]
    usecase: String,
    #[serde(flatten)]
    scopes: BTreeMap<String, String>,
}

async fn presigned_server() -> TestServer {
    TestServer::with_config(config::Config {
        auth: config::AuthZ {
            enforce: true,
            presigned: config::PresignedAuth {
                signing_key_id: Some(PRESIGNED_KID.into()),
                keys: BTreeMap::from([(
                    PRESIGNED_KID.into(),
                    config::PresignedHmacKey {
                        secrets: vec![SecretBox::new(Box::new(config::ConfigSecret::from(
                            PRESIGNED_SECRET,
                        )))],
                        secret_files: vec![],
                    },
                )]),
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

fn sign_token(permissions: HashSet<Permission>, exp: u64) -> String {
    let encoding_key = EncodingKey::from_ed_pem(TEST_EDDSA_PRIVKEY.as_bytes()).unwrap();
    let claims = JwtClaims {
        exp,
        permissions,
        res: JwtRes {
            usecase: "usecase".into(),
            scopes: BTreeMap::from([("org".into(), "1".into())]),
        },
    };
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(TEST_EDDSA_KID.into());
    encode(&header, &claims, &encoding_key).unwrap()
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

async fn put_object(server: &TestServer, client: &reqwest::Client, token: &str, key: &str) {
    let response = client
        .put(server.url(&format!("/v1/objects/usecase/org=1/{key}")))
        .header("x-os-auth", bearer(token))
        .body("hello presigned")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

async fn mint_signature(
    server: &TestServer,
    client: &reqwest::Client,
    token: &str,
    key: &str,
    expires_at: u64,
) -> reqwest::Response {
    client
        .post(server.url(&format!(
            "/v1/objects:presign/usecase/org=1/{key}?operation=GET&expires_at={expires_at}"
        )))
        .header("x-os-auth", bearer(token))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn mint_and_use_presigned_get_range_and_head() {
    let server = presigned_server().await;
    let client = reqwest::Client::new();
    let token = sign_token(Permission::rwd(), get_current_timestamp() + 120);
    put_object(&server, &client, &token, "key").await;

    let expires_at = get_current_timestamp() + 60;
    let response = mint_signature(&server, &client, &token, "key", expires_at).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.text().await.unwrap();
    let presign: PresignResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(presign.expires_at, expires_at);
    assert_eq!(presign.operation, "GET");

    let presigned_url = server.url(&format!(
        "/v1/objects/usecase/org=1/key?X-Os-Signature={}",
        presign.signature
    ));

    let response = client.get(&presigned_url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "hello presigned");

    let response = client
        .get(&presigned_url)
        .header(reqwest::header::RANGE, "bytes=0-4")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.text().await.unwrap(), "hello");

    let response = client.head(&presigned_url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn mint_requires_read_and_valid_expiry() {
    let server = presigned_server().await;
    let client = reqwest::Client::new();
    let now = get_current_timestamp();

    let write_only = sign_token(HashSet::from([Permission::ObjectWrite]), now + 120);
    let response = mint_signature(&server, &client, &write_only, "key", now + 60).await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    let read_token = sign_token(HashSet::from([Permission::ObjectRead]), now + 60);
    let response = mint_signature(&server, &client, &read_token, "key", now + 120).await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    let response = mint_signature(&server, &client, &read_token, "key", now - 1).await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn presigned_signature_is_bound_to_exact_object_and_does_not_authorize_put() {
    let server = presigned_server().await;
    let client = reqwest::Client::new();
    let token = sign_token(Permission::rwd(), get_current_timestamp() + 120);
    put_object(&server, &client, &token, "key").await;
    put_object(&server, &client, &token, "other").await;

    let response = mint_signature(
        &server,
        &client,
        &token,
        "key",
        get_current_timestamp() + 60,
    )
    .await;
    let body = response.text().await.unwrap();
    let presign: PresignResponse = serde_json::from_str(&body).unwrap();

    let wrong_object_url = server.url(&format!(
        "/v1/objects/usecase/org=1/other?X-Os-Signature={}",
        presign.signature
    ));
    let response = client.get(wrong_object_url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    let response = client
        .put(server.url(&format!(
            "/v1/objects/usecase/org=1/key?X-Os-Signature={}",
            presign.signature
        )))
        .body("overwrite")
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}

#[tokio::test]
async fn header_auth_takes_precedence_over_bad_signature() {
    let server = presigned_server().await;
    let client = reqwest::Client::new();
    let token = sign_token(Permission::rwd(), get_current_timestamp() + 120);
    put_object(&server, &client, &token, "key").await;

    let response = client
        .get(server.url("/v1/objects/usecase/org=1/key?X-Os-Signature=not-a-jwt"))
        .header("x-os-auth", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "hello presigned");
}
