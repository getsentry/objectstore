#![allow(dead_code)]

use std::time::Duration;

use objectstore_client::{Client, SecretKey, Session, TokenGenerator, Usecase};
use objectstore_service::backend::bigtable::{BigTableBackend, BigTableConfig};
use objectstore_service::change_stream::ChangeStreamFactory;
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TestServer, config};

/// Spawns a test server whose high-volume tier is the Bigtable emulator from devservices.
pub async fn test_server() -> TestServer {
    let bigtable = BigTableConfig {
        endpoint: Some("localhost:8086".into()),
        project_id: "testing".into(),
        instance_name: "objectstore".into(),
        table_name: "objectstore".into(),
        connections: None,
        rpc_timeout: Duration::from_secs(2),
        cogs: None,
    };
    let high_volume = BigTableBackend::new(bigtable, &ChangeStreamFactory::default())
        .await
        .unwrap();

    TestServer::tiered()
        .high_volume(Box::new(high_volume))
        .config(config::Config {
            auth: config::AuthZ {
                enforce: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .build()
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
    let usecase = Usecase::from_static("usecase");
    client
        .session(usecase.for_organization(server.org_id()))
        .unwrap()
}
