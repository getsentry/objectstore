use std::sync::LazyLock;

use objectstore_client::{Client, Error, OperationResult, SecretKey, TokenGenerator, Usecase};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY_PATH, TestServer, config};
use objectstore_types::Compression;

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
    // TODO: When server errors cause appropriate status codes to be returned, ensure this is 403
    assert!(matches!(put_result, Err(Error::Reqwest(_))));
}

#[tokio::test]
async fn batch_mixed_operations() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    // First, store an object that we'll retrieve and delete in the batch
    let existing_key = session
        .put("existing object")
        .compression(None)
        .key("batch-test-existing")
        .send()
        .await
        .unwrap()
        .key;

    // Build a batch with: GET existing, INSERT new, DELETE existing
    let results: Vec<_> = session
        .many()
        .push(session.get(&existing_key))
        .push(session.put("new batch object").compression(None).key("batch-test-new"))
        .push(session.delete(&existing_key))
        .send()
        .await
        .unwrap()
        .collect();

    assert_eq!(results.len(), 3);

    // Verify results (order may not be guaranteed, so we check by type)
    let mut get_count = 0;
    let mut put_count = 0;
    let mut delete_count = 0;

    for result in &results {
        match result {
            OperationResult::Get(key, Ok(Some(response))) => {
                assert_eq!(key, "batch-test-existing");
                let payload = response.metadata.content_type.clone();
                assert!(!payload.is_empty());
                get_count += 1;
            }
            OperationResult::Get(_, Ok(None)) => {
                panic!("Expected to find the object");
            }
            OperationResult::Put(key, Ok(_)) => {
                assert_eq!(key, "batch-test-new");
                put_count += 1;
            }
            OperationResult::Delete(key, Ok(())) => {
                assert_eq!(key, "batch-test-existing");
                delete_count += 1;
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    assert_eq!(get_count, 1);
    assert_eq!(put_count, 1);
    assert_eq!(delete_count, 1);

    // Verify the new object was stored
    let response = session.get("batch-test-new").send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "new batch object");

    // Verify the old object was deleted
    let response = session.get(&existing_key).send().await.unwrap();
    assert!(response.is_none());
}

#[tokio::test]
async fn batch_insert_auto_generated_key() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    // Insert without providing a key - should auto-generate a UUID
    let results: Vec<_> = session
        .many()
        .push(session.put("auto-key object").compression(None))
        .send()
        .await
        .unwrap()
        .collect();

    assert_eq!(results.len(), 1);

    let generated_key = match &results[0] {
        OperationResult::Put(key, Ok(_)) => {
            // Key should be a valid UUID (36 characters with hyphens)
            assert_eq!(key.len(), 36);
            assert!(key.chars().filter(|c| *c == '-').count() == 4);
            key.clone()
        }
        other => panic!("Expected Put result, got: {:?}", other),
    };

    // Verify the object was stored under the generated key
    let response = session.get(&generated_key).send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "auto-key object");
}
