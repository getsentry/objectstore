use objectstore_client::{Client, Error, Usecase};
use objectstore_test::server::TestServer;
use objectstore_test::token::TestTokenGenerator;
use objectstore_types::Compression;

#[tokio::test]
async fn stores_uncompressed() {
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
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
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
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
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .proj("1337")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
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
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .proj("1337")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
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
async fn overwrites_existing_key() {
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .proj("1337")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
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
async fn fails_with_wrong_auth_token_scope() {
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .proj("9999")
        .usecase("usecase")
        .perms("rwd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let put_result = session.put("initial body").send().await;
    // TODO: When server errors cause appropriate status codes to be returned, ensure this is 403
    assert!(matches!(put_result, Err(Error::Reqwest(_))));
}

#[tokio::test]
async fn fails_with_insufficient_auth_token_perms() {
    let server = TestServer::new().await;

    let token = TestTokenGenerator::new()
        .org("12345")
        .proj("1337")
        .usecase("usecase")
        .perms("rd")
        .sign();

    let client = Client::builder(server.url("/"))
        .auth_token(token)
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase");
    let session = client.session(usecase.for_project(12345, 1337)).unwrap();

    let put_result = session.put("initial body").send().await;
    // TODO: When server errors cause appropriate status codes to be returned, ensure this is 403
    assert!(matches!(put_result, Err(Error::Reqwest(_))));
}
