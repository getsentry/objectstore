use std::sync::LazyLock;

use objectstore_client::{Client, OperationResult, SecretKey, TokenGenerator, Usecase};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY_PATH, TestServer, config};

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
async fn batch_operations() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token_generator(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    // First batch: 4 PUT operations
    // key-2 uses default compression (zstd), others are uncompressed
    let results: Vec<_> = session
        .many()
        .push(session.put("first object").compression(None).key("key-1"))
        .push(session.put("second object").key("key-2"))
        .push(session.put("third object").compression(None).key("key-3"))
        .push(session.put("fourth object").compression(None).key("key-4"))
        .send()
        .await
        .unwrap()
        .into_iter()
        .collect();

    // Assert on first batch responses
    assert_eq!(results.len(), 4);
    let keys: Vec<String> = results
        .iter()
        .map(|r| match r {
            OperationResult::Put(key, Ok(_)) => key.clone(),
            other => panic!("Expected Put result, got: {:?}", other),
        })
        .collect();
    assert_eq!(keys, vec!["key-1", "key-2", "key-3", "key-4"]);

    // Second batch: GET key-1, GET key-2 (automatic decompression), DELETE key-3, PUT key-4 (override)
    let results: Vec<_> = session
        .many()
        .push(session.get("key-1"))
        .push(session.get("key-2"))
        .push(session.delete("key-3"))
        .push(
            session
                .put("overridden fourth object")
                .compression(None)
                .key("key-4"),
        )
        .send()
        .await
        .unwrap()
        .into_iter()
        .collect();

    // Assert on second batch responses
    assert_eq!(results.len(), 4);

    // Extract and verify each result
    let mut results_iter = results.into_iter();

    // Check first GET result
    match results_iter.next().unwrap() {
        OperationResult::Get(key, Ok(Some(response))) => {
            assert_eq!(key, "key-1");
            assert_eq!(response.metadata.compression, None);
            assert!(response.metadata.time_created.is_some());
            let payload = response.payload().await.unwrap();
            assert_eq!(payload, "first object");
        }
        other => panic!("Expected Get(key-1) with Some, got: {:?}", other),
    }

    // Check second GET result (automatic decompression)
    match results_iter.next().unwrap() {
        OperationResult::Get(key, Ok(Some(response))) => {
            assert_eq!(key, "key-2");
            // Automatic decompression means compression is None in metadata
            assert_eq!(response.metadata.compression, None);
            assert!(response.metadata.time_created.is_some());
            let payload = response.payload().await.unwrap();
            assert_eq!(payload, "second object");
        }
        other => panic!("Expected Get(key-2) with Some, got: {:?}", other),
    }

    // Check DELETE result
    match results_iter.next().unwrap() {
        OperationResult::Delete(key, Ok(())) => {
            assert_eq!(key, "key-3");
        }
        other => panic!("Expected Delete(key-3) success, got: {:?}", other),
    }

    // Check PUT (override) result
    match results_iter.next().unwrap() {
        OperationResult::Put(key, Ok(_)) => {
            assert_eq!(key, "key-4");
        }
        other => panic!("Expected Put(key-4) success, got: {:?}", other),
    }

    // Verify the overridden object has the new content
    let response = session.get("key-4").send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "overridden fourth object");

    // Verify the deleted object is gone
    let response = session.get("key-3").send().await.unwrap();
    assert!(response.is_none());
}
