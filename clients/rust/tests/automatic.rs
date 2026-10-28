//! Automatic upload tests do not require the `resumable-upload-api` feature.

use std::collections::VecDeque;
use std::io::{self, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, Response, Uri};
use futures_util::{StreamExt, stream};
use objectstore_client::{Client, Compression, Error, ExpirationPolicy, Session, Usecase};
use tokio::io::AsyncSeekExt;

fn decode(compression: Compression, encoded: &[u8]) -> Vec<u8> {
    match compression {
        Compression::Zstd => zstd::decode_all(encoded).unwrap(),
        Compression::Gzip => {
            let mut decoded = Vec::new();
            io::Read::read_to_end(&mut flate2::read::GzDecoder::new(encoded), &mut decoded)
                .unwrap();
            decoded
        }
    }
}

#[derive(Clone, Debug)]
struct Received {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

enum Reply {
    Create,
    Complete,
    Offset(u16, u64),
    EarlyOffset(u64),
    Status(u16),
    LostCompletion,
    Response(Response<Body>),
}

impl Reply {
    fn response(self) -> Response<Body> {
        let mut response = Response::builder();
        let body = match self {
            Self::Create => Body::from(r#"{"key":"key","session":"dG9rZW4","granularity":4}"#),
            Self::Complete => {
                response = response.status(201);
                Body::from(r#"{"key":"key"}"#)
            }
            Self::Offset(status, offset) => {
                response = response.status(status).header("upload-offset", offset);
                Body::empty()
            }
            Self::EarlyOffset(offset) => return Self::Offset(409, offset).response(),
            Self::Status(status) => {
                response = response.status(status);
                Body::from(r#"{"key":"key"}"#)
            }
            Self::LostCompletion => {
                response = response.status(201).header("content-length", "100");
                Body::from_stream(stream::once(async {
                    Err::<Bytes, _>(io::Error::other("connection lost after commit"))
                }))
            }
            Self::Response(response) => return response,
        };
        response.body(body).unwrap()
    }
}

#[derive(Default)]
struct Script {
    replies: VecDeque<Reply>,
    received: Vec<Received>,
}

struct Server {
    url: String,
    script: Arc<Mutex<Script>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn new(replies: impl IntoIterator<Item = Reply>) -> Self {
        async fn handle(
            State(script): State<Arc<Mutex<Script>>>,
            request: Request,
        ) -> Response<Body> {
            let (parts, body) = request.into_parts();
            // Model a server that responds before consuming the request body.
            {
                let mut script = script.lock().unwrap();
                if matches!(script.replies.front(), Some(Reply::EarlyOffset(_))) {
                    script.received.push(Received {
                        method: parts.method,
                        uri: parts.uri,
                        headers: parts.headers,
                        body: Bytes::new(),
                    });
                    // Drain in the background so HTTP/1 can deliver the early response
                    // without resetting the connection due to unread request data.
                    tokio::spawn(async move {
                        let _ = to_bytes(body, 16 * 1024 * 1024).await;
                    });
                    return script.replies.pop_front().unwrap().response();
                }
            }
            // A truncated request body is expected in the local-file-failure test.
            let Ok(body) = to_bytes(body, 16 * 1024 * 1024).await else {
                return Reply::Status(400).response();
            };
            let reply = {
                let mut script = script.lock().unwrap();
                script.received.push(Received {
                    method: parts.method,
                    uri: parts.uri,
                    headers: parts.headers,
                    body,
                });
                script.replies.pop_front().expect("unexpected HTTP request")
            };
            reply.response()
        }
        let script = Arc::new(Mutex::new(Script {
            replies: replies.into_iter().collect(),
            ..Default::default()
        }));
        let app = axum::Router::new()
            .fallback(handle)
            .with_state(script.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, script, task }
    }

    fn client(&self) -> objectstore_client::ClientBuilder {
        Client::builder(&self.url)
            .configure_reqwest(|builder| builder.no_proxy().connect_timeout(Duration::from_secs(5)))
    }

    fn session(&self, threshold: Option<u64>) -> Session {
        self.client()
            .build()
            .unwrap()
            .session(
                Usecase::from_static("testing")
                    .with_compression(None)
                    .with_resumable_threshold_bytes(threshold)
                    .for_organization(123),
            )
            .unwrap()
    }

    fn finish(&self) -> Vec<Received> {
        let script = self.script.lock().unwrap();
        assert!(
            script.replies.is_empty(),
            "{} unused replies",
            script.replies.len()
        );
        script.received.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn threshold_inheritance_overrides_and_empty_payloads() {
    assert_eq!(
        Usecase::from_static("testing").resumable_threshold_bytes(),
        Some(256 * 1024 * 1024)
    );
    for (threshold, override_threshold, data, resumable) in [
        (Some(4), None, "abc", false),
        (Some(4), None, "abcd", true),
        (Some(4), Some(None), "abcd", false),
        (None, None, "abcd", false),
        (None, Some(Some(4)), "abcd", true),
        (Some(0), None, "", true),
    ] {
        let replies = if resumable {
            vec![Reply::Create, Reply::Complete]
        } else {
            vec![Reply::Status(200)]
        };
        let server = Server::new(replies).await;
        let mut put = server.session(threshold).put(data);
        if let Some(threshold) = override_threshold {
            put = put.resumable_threshold_bytes(threshold);
        }
        assert_eq!(put.send().await.unwrap().key, "key");
        let requests = server.finish();
        assert_eq!(requests.last().unwrap().body.as_ref(), data.as_bytes());
        assert_eq!(requests[0].headers.contains_key("upload-length"), resumable);
    }
}

#[tokio::test]
async fn streams_and_readers_remain_direct() {
    let server = Server::new([Reply::Status(200), Reply::Status(200)]).await;
    let session = server.session(Some(0));
    session
        .put_stream(
            stream::iter([Ok(Bytes::from_static(b"a")), Ok(Bytes::from_static(b"bc"))]).boxed(),
        )
        .send()
        .await
        .unwrap();
    session
        .put_read(io::Cursor::new(b"def"))
        .send()
        .await
        .unwrap();
    let requests = server.finish();
    assert_eq!(requests[0].body, "abc");
    assert_eq!(requests[1].body, "def");
    assert!(
        requests
            .iter()
            .all(|r| !r.headers.contains_key("upload-length"))
    );
}

#[tokio::test]
async fn files_remain_direct_and_paths_open_lazily() {
    let server = Server::new([
        Reply::Status(200),
        Reply::Status(200),
        Reply::Create,
        Reply::Offset(204, 2),
        Reply::Complete,
    ])
    .await;
    let session = server.session(Some(0));
    let mut temporary = tempfile::NamedTempFile::new().unwrap();
    temporary.write_all(b"prefix-data").unwrap();
    for position in [7, 8] {
        let mut file = tokio::fs::File::open(temporary.path()).await.unwrap();
        file.seek(SeekFrom::Start(position)).await.unwrap();
        session.put_file(file).send().await.unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("created-after-builder");
    let put = session.put_path(&path);
    tokio::fs::write(path, b"path").await.unwrap();
    put.send().await.unwrap();
    let requests = server.finish();
    assert_eq!(requests[0].body, "data");
    assert_eq!(requests[1].body, "ata");
    assert!(
        requests[..2]
            .iter()
            .all(|r| !r.headers.contains_key("upload-length"))
    );
    assert_eq!(requests[2].headers["upload-length"], "4");
    assert_eq!(requests[3].body, "path");
    assert_eq!(requests[4].body, "th");
}

#[tokio::test]
async fn path_retry_after_response_before_body_is_consumed() {
    let server = Server::new([Reply::Create, Reply::EarlyOffset(4), Reply::Complete]).await;
    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&data).unwrap();
    let put = server.session(Some(0)).put_path(file.path());
    tokio::time::timeout(Duration::from_secs(10), put.send())
        .await
        .expect("retry must use an independent reader")
        .unwrap();
    let requests = server.finish();
    assert_eq!(requests[2].method, Method::PUT);
    assert_eq!(requests[2].body.len(), data.len() - 4);
    assert!(requests[2].body.as_ref() == &data[4..]);
}

#[tokio::test]
async fn compression_spooling_metadata_and_creation_fallback() {
    for compression in [Compression::Zstd, Compression::Gzip] {
        for creation in [Reply::Create, Reply::Status(501), Reply::Status(503)] {
            let fallback = !matches!(creation, Reply::Create);
            let server = Server::new([
                creation,
                if fallback {
                    Reply::Status(200)
                } else {
                    Reply::Complete
                },
            ])
            .await;
            let dir = tempfile::tempdir().unwrap();
            let client = server.client().spool_dir(dir.path()).build().unwrap();
            let session = client
                .session(
                    Usecase::from_static("testing")
                        .with_compression(compression)
                        .with_resumable_threshold_bytes(1024)
                        .for_organization(123),
                )
                .unwrap();
            session
                .put(vec![b'x'; 1024])
                .key("key")
                .content_type("text/plain")
                .filename("file.txt")
                .origin("127.0.0.1")
                .append_metadata("test", "value")
                .expiration_policy(ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
                .send()
                .await
                .unwrap();
            let requests = server.finish();
            let encoded = &requests[1].body;
            assert_eq!(decode(compression, encoded), vec![b'x'; 1024]);
            assert!(encoded.len() < 1024); // Eligibility was decided before compression.
            assert_eq!(
                requests[0].headers["upload-length"],
                encoded.len().to_string()
            );
            assert_eq!(
                requests[1].headers["content-length"],
                encoded.len().to_string()
            );
            assert_eq!(requests[0].method, Method::PUT);
            let metadata =
                objectstore_types::metadata::Metadata::from_headers(&requests[0].headers, "")
                    .unwrap();
            assert_eq!(metadata.compression, Some(compression));
            assert_eq!(metadata.content_type, "text/plain");
            assert_eq!(metadata.filename.as_deref(), Some("file.txt"));
            assert_eq!(metadata.origin.as_deref(), Some("127.0.0.1"));
            assert_eq!(metadata.custom["test"], "value");
            assert_eq!(
                metadata.expiration_policy,
                ExpirationPolicy::TimeToLive(Duration::from_secs(60))
            );
            if fallback {
                assert_eq!(
                    requests[1].headers["content-encoding"],
                    compression.as_str()
                );
            }
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }
}

#[tokio::test]
async fn missing_spool_directory_falls_back_and_precompressed_input_needs_no_spool() {
    let server = Server::new([Reply::Status(200), Reply::Create, Reply::Complete]).await;
    let dir = tempfile::tempdir().unwrap();
    let client = server
        .client()
        .spool_dir(dir.path().join("missing"))
        .build()
        .unwrap();
    let session = client
        .session(
            Usecase::from_static("testing")
                .with_resumable_threshold_bytes(0)
                .for_organization(123),
        )
        .unwrap();
    session.put("original").send().await.unwrap();
    let compressed = zstd::encode_all(&b"original"[..], 0).unwrap();
    session
        .put(compressed.clone())
        .precompressed(Compression::Zstd)
        .send()
        .await
        .unwrap();
    let requests = server.finish();
    assert!(!requests[0].headers.contains_key("upload-length"));
    assert_eq!(
        zstd::decode_all(requests[0].body.as_ref()).unwrap(),
        b"original"
    );
    assert_eq!(requests[2].body.as_ref(), compressed);
}

#[tokio::test]
async fn many_keeps_oversized_uploads_direct() {
    let server = Server::new([Reply::Status(200)]).await;
    let session = server.session(Some(0));
    let data = Bytes::from(vec![b'x'; 1024 * 1024 + 1]);
    session
        .many()
        .push(session.put(data.clone()).resumable_threshold_bytes(0))
        .send()
        .await
        .error_for_failures()
        .await
        .map_err(|errors| errors.collect::<Vec<_>>())
        .unwrap();
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].uri.query(), None);
    assert_eq!(requests[0].body, data);
}

#[tokio::test]
async fn partial_progress_and_conflicts_replay_only_the_suffix() {
    let server = Server::new([
        Reply::Create,
        Reply::Offset(204, 3),
        Reply::Offset(409, 5),
        Reply::Complete,
    ])
    .await;
    server
        .session(Some(0))
        .put("abcdefgh")
        .send()
        .await
        .unwrap();
    let requests = server.finish();
    for (request, offset, body) in [
        (&requests[1], "0", "abcdefgh"),
        (&requests[2], "3", "defgh"),
        (&requests[3], "5", "fgh"),
    ] {
        assert_eq!(request.headers["upload-offset"], offset);
        assert_eq!(request.body, body);
    }
}

#[tokio::test]
async fn recovery_probes_and_progress_resets_the_budget() {
    let server = Server::new([
        Reply::Create,
        Reply::Status(503),
        Reply::Status(502),
        Reply::Offset(204, 2),
        Reply::Status(429),
        Reply::Status(504),
        Reply::Offset(204, 4),
        Reply::Complete,
    ])
    .await;
    // Even with middleware retries enabled, a failed write must be followed by a probe.
    let client = server
        .client()
        .configure_reqwest(|b| {
            b.retry(
                reqwest::retry::for_host("127.0.0.1")
                    .no_budget()
                    .classify_fn(|r| {
                        if r.status() == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) {
                            r.retryable()
                        } else {
                            r.success()
                        }
                    }),
            )
        })
        .build()
        .unwrap();
    let session = client
        .session(
            Usecase::from_static("testing")
                .with_compression(None)
                .with_resumable_threshold_bytes(0)
                .for_organization(123),
        )
        .unwrap();
    session.put("abcdef").send().await.unwrap();
    let requests = server.finish();
    let offsets: Vec<_> = requests[1..]
        .iter()
        .map(|r| r.headers["upload-offset"].to_str().unwrap())
        .collect();
    assert_eq!(offsets, ["0", "*", "*", "2", "*", "*", "4"]);
    assert_eq!(requests[4].body, "cdef");
    assert_eq!(requests[7].body, "ef");
}

#[tokio::test]
async fn no_progress_and_probe_failures_share_the_retry_budget() {
    let server = Server::new([
        Reply::Create,
        Reply::Offset(204, 0),
        Reply::Status(503),
        Reply::Offset(204, 0),
        Reply::Offset(409, 0),
        Reply::Status(500),
    ])
    .await;
    let error = server
        .session(Some(0))
        .put("data")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(&error, Error::UploadFailed(_)));
    assert!(error.to_string().contains("no progress"));
    let requests = server.finish();
    assert_eq!(requests[2].headers["upload-offset"], "*");
    assert_eq!(requests[3].headers["upload-offset"], "*");
    assert_eq!(requests.last().unwrap().method, Method::DELETE);
}

#[tokio::test]
async fn permanent_errors_and_invalid_offsets_cancel_without_restarting() {
    for replies in [
        vec![Reply::Create, Reply::Status(403), Reply::Status(500)],
        vec![Reply::Create, Reply::Offset(204, 10), Reply::Status(204)],
        vec![
            Reply::Create,
            Reply::Offset(204, 2),
            Reply::Offset(204, 1),
            Reply::Status(204),
        ],
        vec![Reply::Create, Reply::Status(410), Reply::Status(204)],
    ] {
        let server = Server::new(replies).await;
        let error = server
            .session(Some(0))
            .put("data")
            .send()
            .await
            .unwrap_err();
        assert!(matches!(error, Error::UploadFailed(_)));
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(server.finish().last().unwrap().method, Method::DELETE);
    }
}

#[tokio::test]
async fn lost_completion_is_recovered_without_reuploading() {
    let server = Server::new([Reply::Create, Reply::LostCompletion, Reply::Complete]).await;
    server.session(Some(0)).put("data").send().await.unwrap();
    let requests = server.finish();
    assert_eq!(requests[2].headers["upload-offset"], "*");
    assert!(requests[2].body.is_empty());
}

#[tokio::test]
async fn local_file_errors_are_not_retried() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"data").unwrap();
    let path = file.path().to_owned();
    let (parts, body) = Reply::Create.response().into_parts();
    let body = Body::from_stream(stream::once(async move {
        // The client has measured the file, but cannot upload until it receives this body.
        tokio::fs::write(path, []).await?;
        to_bytes(body, 1024).await.map_err(io::Error::other)
    }));
    let server = Server::new([
        Reply::Response(Response::from_parts(parts, body)),
        Reply::Status(204),
    ])
    .await;
    let error = server
        .session(Some(0))
        .put_path(file.path())
        .send()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::UploadFailed(_)));
    assert!(error.to_string().contains("upload failed"));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].method, Method::DELETE);
}

#[tokio::test]
async fn compressed_path_replays_the_prepared_bytes() {
    for compression in [Compression::Zstd, Compression::Gzip] {
        let server = Server::new([Reply::Create, Reply::Offset(204, 4), Reply::Complete]).await;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"payload").unwrap();
        server
            .session(Some(0))
            .put_path(file.path())
            .compress(compression)
            .send()
            .await
            .unwrap();
        let requests = server.finish();
        let encoded = &requests[1].body;
        assert_eq!(requests[2].body, encoded.slice(4..));
        assert_eq!(decode(compression, encoded), b"payload");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn non_regular_paths_use_direct_uploads() {
    let server = Server::new([Reply::Status(200)]).await;
    server
        .session(Some(0))
        .put_path("/dev/null")
        .send()
        .await
        .unwrap();
    let requests = server.finish();
    assert!(!requests[0].headers.contains_key("upload-length"));
    assert!(requests[0].body.is_empty());
}

#[test]
fn upload_futures_are_send() {
    fn assert_send(_: impl Send) {}
    let session = Client::builder("http://localhost")
        .build()
        .unwrap()
        .session(Usecase::from_static("testing").for_organization(123))
        .unwrap();
    assert_send(session.put("data").send());
    assert_send(session.put_path("data").send());
    assert_send(session.many().push(session.put("data")).send());
}
