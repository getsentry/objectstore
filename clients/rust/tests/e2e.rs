use std::collections::{BTreeMap, HashSet};
use std::io::Write as _;
use std::sync::LazyLock;

use futures_util::StreamExt as _;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use objectstore_client::{
    Client, Error, OperationResult, Permission, SecretKey, TokenGenerator, Usecase,
};
use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY_PATH, TestServer, config};
use objectstore_types::metadata::Compression;
use reqwest::StatusCode;
use serde::Serialize;

pub static TEST_EDDSA_PRIVKEY: LazyLock<String> =
    LazyLock::new(|| std::fs::read_to_string(&*TEST_EDDSA_PRIVKEY_PATH).unwrap());

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

/// Signs a static token for the given usecase and scopes.
fn sign_static_token(usecase: &str, scopes: &[(&str, &str)]) -> String {
    let encoding_key = EncodingKey::from_ed_pem(TEST_EDDSA_PRIVKEY.as_bytes()).unwrap();
    let claims = JwtClaims {
        exp: get_current_timestamp() + 60,
        permissions: HashSet::from([
            Permission::ObjectRead,
            Permission::ObjectWrite,
            Permission::ObjectDelete,
        ]),
        res: JwtRes {
            usecase: usecase.into(),
            scopes: scopes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        },
    };
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(TEST_EDDSA_KID.into());
    encode(&header, &claims, &encoding_key).unwrap()
}

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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(test_token_generator())
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
        .token(token_generator)
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

    let delete_result = session.delete("some-key").send().await;
    println!("{:?}", delete_result);
    match delete_result {
        Err(Error::Reqwest(err)) => assert_eq!(err.status().unwrap(), StatusCode::FORBIDDEN),
        _ => panic!("Expected error"),
    }
}

#[tokio::test]
async fn stores_with_static_token() {
    let server = test_server().await;

    let token = sign_static_token("usecase", &[("org", "12345")]);

    let client = Client::builder(server.url("/"))
        .token(token)
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let body = "hello with static token!";
    let stored_id = session
        .put(body)
        .compression(None)
        .send()
        .await
        .unwrap()
        .key;

    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    let received = response.payload().await.unwrap();
    assert_eq!(received, body);
}

#[tokio::test]
async fn batch_operations() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
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
        .collect()
        .await;

    assert_eq!(results.len(), 4);
    let mut keys: Vec<String> = results
        .iter()
        .map(|r| match r {
            OperationResult::Put(key, Ok(_)) => key.clone(),
            other => panic!("Expected Put result, got: {:?}", other),
        })
        .collect();
    keys.sort();
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
        .collect()
        .await;

    assert_eq!(results.len(), 4);

    // Results may come back in any order due to concurrent execution, so collect into a map by key
    let mut gets = BTreeMap::new();
    let mut deletes = HashSet::new();
    let mut puts = HashSet::new();
    for result in results {
        match result {
            OperationResult::Get(key, inner) => {
                gets.insert(key, inner);
            }
            OperationResult::Delete(key, Ok(())) => {
                deletes.insert(key);
            }
            OperationResult::Put(key, Ok(_)) => {
                puts.insert(key);
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    // GET key-1 (uncompressed)
    let get1 = gets
        .remove("key-1")
        .expect("missing get key-1")
        .unwrap()
        .unwrap();
    assert_eq!(get1.metadata.compression, None);
    assert!(get1.metadata.time_created.is_some());
    assert_eq!(get1.payload().await.unwrap().as_ref(), b"first object");

    // GET key-2 (automatic decompression)
    let get2 = gets
        .remove("key-2")
        .expect("missing get key-2")
        .unwrap()
        .unwrap();
    assert_eq!(get2.metadata.compression, None);
    assert!(get2.metadata.time_created.is_some());
    assert_eq!(get2.payload().await.unwrap().as_ref(), b"second object");

    // DELETE key-3
    assert!(deletes.contains("key-3"), "missing delete for key-3");

    // PUT key-4 (override)
    assert!(puts.contains("key-4"), "missing put for key-4");

    // Verify the overridden object has the new content
    let response = session.get("key-4").send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "overridden fourth object");

    // Verify the deleted object is gone
    let response = session.get("key-3").send().await.unwrap();
    assert!(response.is_none());
}

#[tokio::test]
async fn batch_insert_without_key() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    // Insert without specifying a key — the server should generate one
    let results: Vec<_> = session
        .many()
        .push(session.put("keyless object").compression(None))
        .send()
        .await
        .collect()
        .await;

    assert_eq!(results.len(), 1);

    let server_key = match &results[0] {
        OperationResult::Put(key, Ok(_)) => key.clone(),
        other => panic!("Expected Put result, got: {:?}", other),
    };

    // The server should have assigned a non-empty key
    assert!(
        !server_key.is_empty(),
        "server-assigned key must not be empty"
    );

    // Verify we can retrieve the object using the server-assigned key
    let response = session.get(&server_key).send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "keyless object");
}

#[tokio::test]
async fn batch_partial_failures() {
    let server = test_server().await;

    // Use a read-only token so writes/deletes fail with 403
    let read_only_token = test_token_generator().permissions(&[Permission::ObjectRead]);

    let client = Client::builder(server.url("/"))
        .token(read_only_token)
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let results: Vec<_> = session
        .many()
        .push(session.get("nonexistent-key-1"))
        .push(session.put("should fail").key("write-key"))
        .push(session.delete("delete-key"))
        .push(session.get("nonexistent-key-2"))
        .push(session.get("nonexistent-key-3"))
        .send()
        .await
        .collect()
        .await;

    assert_eq!(results.len(), 5);

    // Collect results into maps by key for order-independent assertions
    let mut gets = BTreeMap::new();
    let mut puts = BTreeMap::new();
    let mut deletes = BTreeMap::new();
    for result in results {
        match result {
            OperationResult::Get(key, inner) => {
                gets.insert(key, inner);
            }
            OperationResult::Put(key, inner) => {
                puts.insert(key, inner);
            }
            OperationResult::Delete(key, inner) => {
                deletes.insert(key, inner);
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    // GET before failures should succeed (read is permitted)
    let get_result = gets
        .remove("nonexistent-key-1")
        .expect("missing get result");
    assert!(matches!(get_result, Ok(None)));

    // PUT should fail with 403 (no write permission)
    let put_result = puts.remove("write-key").expect("missing put result");
    match put_result {
        Err(Error::OperationFailure { status, .. }) => assert_eq!(status, 403),
        other => panic!("Expected OperationFailure(403), got: {:?}", other),
    }

    // DELETE should fail with 403 (no delete permission)
    let delete_result = deletes.remove("delete-key").expect("missing delete result");
    match delete_result {
        Err(Error::OperationFailure { status, .. }) => assert_eq!(status, 403),
        other => panic!("Expected OperationFailure(403), got: {:?}", other),
    }

    // GETs after the failures should still succeed
    let get_result = gets
        .remove("nonexistent-key-2")
        .expect("missing get result for key-2");
    assert!(matches!(get_result, Ok(None)));

    let get_result = gets
        .remove("nonexistent-key-3")
        .expect("missing get result for key-3");
    assert!(matches!(get_result, Ok(None)));
}

#[tokio::test]
async fn batch_put_files() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let dir = tempfile::tempdir().unwrap();

    // Create a few small files (under 1 MB batch limit).
    let small_bodies: Vec<(&str, Vec<u8>)> = vec![
        ("small-1", b"hello from file 1".to_vec()),
        ("small-2", b"hello from file 2".to_vec()),
        ("small-3", b"hello from file 3".to_vec()),
    ];

    // Create a large file that exceeds the 1 MB batch body size limit.
    // This should be routed to a single PUT request instead of being batched.
    let large_body: Vec<u8> = vec![0xAB; 2 * 1024 * 1024]; // 2 MB

    let mut many = session.many();

    for (name, content) in &small_bodies {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        let file = tokio::fs::File::open(&path).await.unwrap();
        many = many.push(session.put_file(file).compression(None).key(*name));
    }

    let large_path = dir.path().join("large");
    std::fs::File::create(&large_path)
        .unwrap()
        .write_all(&large_body)
        .unwrap();
    let large_file = tokio::fs::File::open(&large_path).await.unwrap();
    many = many.push(session.put_file(large_file).compression(None).key("large"));

    let results: Vec<_> = many.send().await.collect().await;

    assert_eq!(results.len(), 4);

    let mut keys: Vec<String> = results
        .iter()
        .map(|r| match r {
            OperationResult::Put(key, Ok(_)) => key.clone(),
            other => panic!("Expected successful Put result, got: {:?}", other),
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["large", "small-1", "small-2", "small-3"]);

    // Verify all objects are retrievable with the correct content.
    for (name, expected) in &small_bodies {
        let response = session.get(name).send().await.unwrap().unwrap();
        let payload = response.payload().await.unwrap();
        assert_eq!(payload.as_ref(), expected.as_slice(), "mismatch for {name}");
    }

    let response = session.get("large").send().await.unwrap().unwrap();
    let payload = response.payload().await.unwrap();
    assert_eq!(payload.as_ref(), large_body.as_slice());
}

#[tokio::test]
async fn put_file_with_compression() {
    let server = test_server().await;

    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_organization(12345)).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compressed.txt");
    let body = "hello compressed file!";
    std::fs::write(&path, body).unwrap();

    // Default compression is zstd
    let file = tokio::fs::File::open(&path).await.unwrap();
    let stored_id = session.put_file(file).send().await.unwrap().key;

    // When requesting raw, the stored object should be zstd-compressed
    let response = session
        .get(&stored_id)
        .decompress(false)
        .send()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.metadata.compression, Some(Compression::Zstd));

    let compressed = response.payload().await.unwrap();
    let decompressed = zstd::bulk::decompress(&compressed, 1024).unwrap();
    assert_eq!(decompressed, body.as_bytes());

    // Default get decompresses transparently
    let response = session.get(&stored_id).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, None);

    let received = response.payload().await.unwrap();
    assert_eq!(received, body);
}
