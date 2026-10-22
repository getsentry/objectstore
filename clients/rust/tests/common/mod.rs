#![allow(dead_code)]

use objectstore_client::{Client, SecretKey, Session, Usecase};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TestServer, config};

pub async fn test_server() -> TestServer {
    TestServer::with_config(config::Config {
        auth: config::AuthZ {
            enforce: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

pub fn test_secret_key() -> SecretKey {
    SecretKey::new(TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY).unwrap()
}

pub fn test_session(server: &TestServer) -> Session {
    let client = Client::builder(server.url("/"))
        .token(test_secret_key())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    client.session(usecase.for_organization(12345)).unwrap()
}
