//! End-to-end tests for the multipart upload client API.

mod common;

use common::{test_server, test_session};
use futures_util::StreamExt as _;
use futures_util::stream;
use objectstore_client::{Client, CompletePart, Compression, Error, PartNumber, Usecase};

use crate::common::test_token_generator;

fn pn(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

#[tokio::test]
async fn test_full_upload_uncompressed() {
    let server = test_server().await;
    let client = Client::builder(server.url("/"))
        .token(test_token_generator())
        .build()
        .unwrap();
    let usecase = Usecase::new("usecase").with_compression(None);

    let session = client.session(usecase.for_organization(12345)).unwrap();

    let upload = session
        .initiate_multipart_upload()
        .key("multipart-test-key")
        .send()
        .await
        .unwrap();

    assert_eq!(upload.key(), "multipart-test-key");
    assert!(!upload.upload_id().is_empty());

    let parts_data: Vec<(&[u8], PartNumber)> = vec![(b"hello ", pn(1)), (b"world!", pn(2))];

    let results: Vec<_> = stream::iter(
        parts_data
            .into_iter()
            .map(|(data, part_number)| upload.put(data, part_number, None)),
    )
    .buffer_unordered(2)
    .collect()
    .await;

    let mut parts = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(part) => parts.push(part),
            Err(e) => errors.push(e),
        }
    }
    assert!(errors.is_empty(), "part uploads failed: {errors:?}");

    let key = upload.complete(parts).await.unwrap();

    assert_eq!(key, "multipart-test-key");

    let response = session
        .get(&key)
        .decompress(false)
        .send()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.metadata.compression, None);
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "hello world!");
}

#[tokio::test]
async fn test_full_upload_compressed() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .key("multipart-compressed-key")
        .compression(Compression::Zstd)
        .send()
        .await
        .unwrap();

    let part1_data = b"hello ";
    let part2_data = b"world!";

    let parts_data: Vec<(Vec<u8>, PartNumber)> = vec![
        (zstd::encode_all(&part1_data[..], 0).unwrap(), pn(1)),
        (zstd::encode_all(&part2_data[..], 0).unwrap(), pn(2)),
    ];

    let results: Vec<_> = stream::iter(
        parts_data
            .into_iter()
            .map(|(data, part_number)| upload.put(data, part_number, None)),
    )
    .buffer_unordered(2)
    .collect()
    .await;

    let mut parts = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(part) => parts.push(part),
            Err(e) => errors.push(e),
        }
    }
    assert!(errors.is_empty(), "part uploads failed: {errors:?}");

    let key = upload.complete(parts).await.unwrap();

    let response = session
        .get(&key)
        .decompress(false)
        .send()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.metadata.compression, Some(Compression::Zstd));

    let mut expected = zstd::encode_all(&part1_data[..], 0).unwrap();
    expected.extend(zstd::encode_all(&part2_data[..], 0).unwrap());
    assert_eq!(
        response.payload().await.unwrap().as_ref(),
        expected.as_slice()
    );

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, None);
    assert_eq!(response.payload().await.unwrap(), "hello world!");
}

#[tokio::test]
async fn test_server_generated_key() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .compression(None)
        .send()
        .await
        .unwrap();

    assert!(!upload.key().is_empty());

    let part = upload.put(b"data".as_slice(), pn(1), None).await.unwrap();

    let key = upload.complete([part]).await.unwrap();

    assert!(!key.is_empty());

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.payload().await.unwrap(), "data");
}

#[tokio::test]
async fn test_list_parts() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .key("list-parts-key")
        .compression(None)
        .send()
        .await
        .unwrap();

    upload
        .put(b"part-two".as_slice(), pn(2), None)
        .await
        .unwrap();
    upload
        .put(b"part-one".as_slice(), pn(1), None)
        .await
        .unwrap();

    let parts = upload.list_parts().await.unwrap();
    assert_eq!(parts.len(), 2);

    let p1 = parts
        .iter()
        .find(|p| p.part_number == pn(1))
        .expect("missing part 1");
    let p2 = parts
        .iter()
        .find(|p| p.part_number == pn(2))
        .expect("missing part 2");
    assert_eq!(p1.size, 8);
    assert_eq!(p2.size, 8);

    upload.abort().await.unwrap();
}

#[tokio::test]
async fn test_abort() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .key("abort-key")
        .send()
        .await
        .unwrap();

    upload
        .put(b"some data".as_slice(), pn(1), None)
        .await
        .unwrap();
    upload.abort().await.unwrap();
}

#[tokio::test]
async fn test_metadata_preserved() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .key("metadata-key")
        .compression(None)
        .content_type("text/plain")
        .origin("203.0.113.42")
        .append_metadata("my-key".to_string(), "my-value".to_string())
        .send()
        .await
        .unwrap();

    let part = upload
        .put(b"payload".as_slice(), pn(1), None)
        .await
        .unwrap();

    let key = upload.complete([part]).await.unwrap();

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.content_type, "text/plain");
    assert_eq!(response.metadata.origin.as_deref(), Some("203.0.113.42"));
    assert_eq!(
        response.metadata.custom.get("my-key").map(String::as_str),
        Some("my-value")
    );
}

#[tokio::test]
async fn test_complete_with_bad_etag() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .initiate_multipart_upload()
        .key("bad-etag-key")
        .compression(None)
        .send()
        .await
        .unwrap();

    upload
        .put(b"real data".as_slice(), pn(1), None)
        .await
        .unwrap();

    let result = upload
        .complete(vec![CompletePart {
            part_number: pn(1),
            etag: "bogus-etag".to_string(),
        }])
        .await;

    match result {
        Err(Error::MultipartComplete { code, message }) => {
            assert!(!code.is_empty(), "error code should not be empty");
            assert!(!message.is_empty(), "error message should not be empty");
        }
        other => panic!("expected MultipartComplete error, got: {other:?}"),
    }
}
