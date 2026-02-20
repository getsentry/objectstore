use std::sync::LazyLock;

use objectstore_client::{Client, Error, SecretKey, TokenGenerator, Usecase};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY_PATH, TestServer, config};
use objectstore_types::metadata::Compression;
use reqwest::StatusCode;

pub static TEST_EDDSA_PRIVKEY: LazyLock<String> =
    LazyLock::new(|| std::fs::read_to_string(&*TEST_EDDSA_PRIVKEY_PATH).unwrap());

async fn test_server() -> TestServer {
    TestServer::with_config(config::Config {
        auth: config::AuthZ {
            enforce: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
}

fn test_token_generator() -> TokenGenerator {
    TokenGenerator::new(SecretKey {
        kid: TEST_EDDSA_KID.into(),
        secret_key: TEST_EDDSA_PRIVKEY.clone(),
    })
    .unwrap()
}

#[tokio::test]
async fn stores_uncompressed() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let body = "oh hai!";

    let stored_id = session
        .put(body)
        .compression(None)
        .send()
        .await
        .unwrap()
        .key;

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, None);
    assert!(response.metadata.time_created.is_some());

    let received = response.payload().await.unwrap();
    assert_eq!(received, "oh hai!");
}

#[tokio::test]
async fn uses_zstd_by_default() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let body = "oh hai!";
    let stored_id = session.put(body).send().await.unwrap().key;

    // when the user indicates that it can deal with zstd, it gets zstd
    let request = session.get(&stored_id).decompress(false);
    let response = request.send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, Some(Compression::Zstd));

    let received_compressed = response.payload().await.unwrap();
    let decompressed = zstd::bulk::decompress(&received_compressed, 1024).unwrap();
    assert_eq!(decompressed, b"oh hai!");

    // otherwise, the client does the decompression
    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, None);

    let received = response.payload().await.unwrap();
    assert_eq!(received, "oh hai!");
}

#[tokio::test]
async fn deletes_stores_stuff() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let body = "oh hai!";
    let stored_id = session.put(body).send().await.unwrap().key;

    session.delete(&stored_id).send().await.unwrap();

    let response = session.get(&stored_id).send().await.unwrap();
    assert!(response.is_none());
}

#[tokio::test]
async fn stores_under_given_key() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let body = "oh hai!";
    let request = session.put(body).key("test-key123!!");
    let stored_id = request.send().await.unwrap().key;

    // The client decodes the percent-encoded key from the server
    assert_eq!(stored_id, "test-key123!!");
}

#[tokio::test]
async fn stores_structured_keys() {
    let server = TestServer::new().await;

    let client = Client::builder(server.url("/")).build().unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let body = "oh hai!";
    let request = session.put(body).key("1/shard-0.json");
    let stored_id = request.send().await.unwrap().key;
    assert_eq!(stored_id, "1/shard-0.json");

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    let received = response.payload().await.unwrap();
    assert_eq!(received, body);
}

#[tokio::test]
async fn overwrites_existing_key() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let stored_id = session.put("initial body").send().await.unwrap().key;
    let request = session.put("new body").key(&stored_id);
    let overwritten_id = request.send().await.unwrap().key;

    assert_eq!(stored_id, overwritten_id);

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "new body");
}

#[tokio::test]
async fn not_found_with_wrong_scope() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");

    // First we have to place an object with one scope
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();
    let stored_id = session.put("initial body").send().await.unwrap().key;

    // Now we need to try to fetch the object with a different scope
    let session = client.session(usecase.for_project(12345, 9999)).unwrap();
    let response = session.get(&stored_id).send().await.unwrap();
    assert!(response.is_none());
}

#[tokio::test]
async fn stores_with_origin() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let stored_id = session
        .put("hello with origin")
        .compression(None)
        .origin("203.0.113.42")
        .send()
        .await
        .unwrap()
        .key;

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.origin.as_deref(), Some("203.0.113.42"));
}

#[tokio::test]
async fn stores_without_origin() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let stored_id = session
        .put("hello without origin")
        .compression(None)
        .send()
        .await
        .unwrap()
        .key;

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    assert!(response.metadata.origin.is_none());
}

#[tokio::test]
async fn fails_with_insufficient_auth_token_perms() {
    let server = test_server().await;

    let token_generator = test_token_generator().permissions(&[]);

    let client = Client::builder(server.url("/"))
        .token_generator(token_generator)
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let put_result = session.put("initial body").send().await;
    println!("{:?}", put_result);
    match put_result {
        Err(Error::Reqwest(err)) => assert_eq!(err.status().unwrap(), StatusCode::FORBIDDEN),
        _ => panic!("Expected error"),
    }
}
