//! End-to-end tests for the multipart upload client API.

mod common;

use common::{test_server, test_session};
use objectstore_client::{Compression, Error, MultipartCompletePart};

#[tokio::test]
async fn full_upload_flow() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("multipart-test-key")
        .send()
        .await
        .unwrap();

    assert_eq!(upload.key(), "multipart-test-key");
    assert!(!upload.id().is_empty());

    let part1_data = b"hello ";
    let part2_data = b"world!";

    let etag1 = upload.put(part1_data.as_slice(), 1, None).await.unwrap();
    let etag2 = upload.put(part2_data.as_slice(), 2, None).await.unwrap();

    assert!(!etag1.is_empty());
    assert!(!etag2.is_empty());

    let key = upload
        .complete(vec![
            MultipartCompletePart {
                part_number: 1,
                etag: etag1,
            },
            MultipartCompletePart {
                part_number: 2,
                etag: etag2,
            },
        ])
        .await
        .unwrap();

    assert_eq!(key, "multipart-test-key");

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.compression, None);
    let payload = response.payload().await.unwrap();
    assert_eq!(payload, "hello world!");
}

#[tokio::test]
async fn compressed_upload_flow() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("multipart-compressed-key")
        .compression(Compression::Zstd)
        .send()
        .await
        .unwrap();

    let part1_data = b"hello ";
    let part2_data = b"world!";

    let etag1 = upload.put(part1_data.as_slice(), 1, None).await.unwrap();
    let etag2 = upload.put(part2_data.as_slice(), 2, None).await.unwrap();

    let key = upload
        .complete(vec![
            MultipartCompletePart {
                part_number: 1,
                etag: etag1,
            },
            MultipartCompletePart {
                part_number: 2,
                etag: etag2,
            },
        ])
        .await
        .unwrap();

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
async fn server_generated_key() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .compression(None)
        .send()
        .await
        .unwrap();

    assert!(!upload.key().is_empty());

    let etag = upload.put(b"data".as_slice(), 1, None).await.unwrap();

    let key = upload
        .complete(vec![MultipartCompletePart {
            part_number: 1,
            etag,
        }])
        .await
        .unwrap();

    assert!(!key.is_empty());

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.payload().await.unwrap(), "data");
}

#[tokio::test]
async fn list_parts() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("list-parts-key")
        .compression(None)
        .send()
        .await
        .unwrap();

    upload.put(b"part-one".as_slice(), 1, None).await.unwrap();
    upload.put(b"part-two".as_slice(), 2, None).await.unwrap();

    let parts = upload.list_parts(None, None).await.unwrap();
    assert_eq!(parts.parts.len(), 2);
    assert!(parts.parts.contains_key(&1));
    assert!(parts.parts.contains_key(&2));
    assert_eq!(parts.parts[&1].size, 8);
    assert_eq!(parts.parts[&2].size, 8);
    assert!(!parts.is_truncated);

    upload.abort().await.unwrap();
}

#[tokio::test]
async fn abort_upload() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("abort-key")
        .compression(None)
        .send()
        .await
        .unwrap();

    upload.put(b"some data".as_slice(), 1, None).await.unwrap();
    upload.abort().await.unwrap();
}

#[tokio::test]
async fn metadata_preserved() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("metadata-key")
        .compression(None)
        .content_type("text/plain")
        .origin("203.0.113.42")
        .append_metadata("my-key".to_string(), "my-value".to_string())
        .send()
        .await
        .unwrap();

    let etag = upload.put(b"payload".as_slice(), 1, None).await.unwrap();

    let key = upload
        .complete(vec![MultipartCompletePart {
            part_number: 1,
            etag,
        }])
        .await
        .unwrap();

    let response = session.get(&key).send().await.unwrap().unwrap();
    assert_eq!(response.metadata.content_type, "text/plain");
    assert_eq!(response.metadata.origin.as_deref(), Some("203.0.113.42"));
    assert_eq!(
        response.metadata.custom.get("my-key").map(String::as_str),
        Some("my-value")
    );
}

#[tokio::test]
async fn complete_with_bad_etag() {
    let server = test_server().await;
    let session = test_session(&server);

    let upload = session
        .create_multipart_upload()
        .key("bad-etag-key")
        .compression(None)
        .send()
        .await
        .unwrap();

    upload.put(b"real data".as_slice(), 1, None).await.unwrap();

    let result = upload
        .complete(vec![MultipartCompletePart {
            part_number: 1,
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
