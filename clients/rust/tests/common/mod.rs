#![allow(dead_code)]

use objectstore_client::{Client, SecretKey, Session, TokenGenerator, Usecase};
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

pub fn test_token_generator() -> TokenGenerator {
    TokenGenerator::new(SecretKey {
        kid: TEST_EDDSA_KID.into(),
        secret_key: TEST_EDDSA_PRIVKEY.to_owned(),
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
