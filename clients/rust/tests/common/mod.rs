#![allow(dead_code)]

use std::sync::LazyLock;

use objectstore_client::{Client, SecretKey, Session, TokenGenerator, Usecase};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY_PATH, TestServer, config};
use secrecy::SecretBox;

pub const TEST_PRESIGNED_KID: &str = "presigned-test";
pub const TEST_PRESIGNED_SECRET: &str = "presigned-secret-for-tests";

pub static TEST_EDDSA_PRIVKEY: LazyLock<String> =
    LazyLock::new(|| std::fs::read_to_string(&*TEST_EDDSA_PRIVKEY_PATH).unwrap());

pub async fn test_server() -> TestServer {
    TestServer::with_config(config::Config {
        auth: config::AuthZ {
            enforce: true,
            presigned: config::PresignedAuth {
                active_key_id: Some(TEST_PRESIGNED_KID.into()),
                keys: [(
                    TEST_PRESIGNED_KID.into(),
                    config::PresignedHmacKey {
                        secrets: vec![SecretBox::new(Box::new(config::ConfigSecret::from(
                            TEST_PRESIGNED_SECRET,
                        )))],
                        secret_files: vec![],
                    },
                )]
                .into(),
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

pub fn test_token_generator() -> TokenGenerator {
    TokenGenerator::new(SecretKey {
        kid: TEST_EDDSA_KID.into(),
        secret_key: TEST_EDDSA_PRIVKEY.clone(),
    })
    .unwrap()
}

pub fn test_session(server: &TestServer) -> Session {
    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    client.session(usecase.for_organization(12345)).unwrap()
}
