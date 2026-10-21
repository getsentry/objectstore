//! Preparation and recovery for automatic resumable uploads.

use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use super::{CompressionMode, PutBody, PutResponse, encode_body};
use crate::resumable::{ResumableUpload, UploadProgress};
use crate::{ClientStream, Error};

pub(super) enum PreparedBody {
    Direct(reqwest::Body),
    Replay(ReplayBody),
}

pub(super) enum ReplayBody {
    Buffer(bytes::Bytes),
    File {
        file: Option<ReplayFile>,
        start: u64,
        length: u64,
    },
}

/// One reader owns the file for an attempt and returns it after stopping.
pub(super) enum ReplayFile {
    Ready(File),
    Reading {
        stop: oneshot::Sender<()>,
        task: tokio::task::JoinHandle<File>,
    },
}

/// Marks local read failures even when reqwest wraps them in a transport error.
#[derive(Debug, thiserror::Error)]
#[error("could not read upload payload: {0}")]
struct PayloadReadError(#[source] io::Error);

#[derive(Debug, thiserror::Error)]
#[error("upload made no progress")]
struct NoUploadProgress;

pub(super) async fn prepare(
    mut body: PutBody,
    compression: Option<CompressionMode>,
    threshold: Option<u64>,
    spool_dir: Option<PathBuf>,
) -> crate::Result<PreparedBody> {
    if let Some(threshold) = threshold {
        if let PutBody::Path(path) = body {
            body = PutBody::File(tokio::fs::File::open(path).await?);
        }
        if let Some(size) = body.remaining_size().await.ok().flatten()
            && size >= threshold
        {
            let mut length = size;
            if let Some(CompressionMode::Compress(crate::Compression::Zstd)) = compression {
                let temporary = tokio::task::spawn_blocking(move || match spool_dir {
                    Some(dir) => tempfile::tempfile_in(dir),
                    None => tempfile::tempfile(),
                })
                .await
                .map_err(io::Error::other)?;
                let mut temporary = match temporary {
                    Ok(file) => tokio::fs::File::from_std(file),
                    // No input has been consumed, so direct streaming is still possible.
                    Err(_) => {
                        return Ok(PreparedBody::Direct(encode_body(body, compression).await?));
                    }
                };
                let reader: Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin> = match body {
                    PutBody::Buffer(bytes) => Box::new(io::Cursor::new(bytes)),
                    PutBody::File(file) => Box::new(file),
                    _ => unreachable!("only buffers and opened regular files have a known size"),
                };
                let mut encoder = super::ZstdEncoder::new(super::BufReader::new(reader));
                length = tokio::io::copy(&mut encoder, &mut temporary).await?;
                temporary.flush().await?;
                temporary.rewind().await?;
                body = PutBody::File(temporary);
            }
            let replay = match body {
                PutBody::Buffer(bytes) => ReplayBody::Buffer(bytes),
                PutBody::File(mut file) => {
                    let start = file.stream_position().await?;
                    ReplayBody::File {
                        file: Some(ReplayFile::Ready(file)),
                        start,
                        length,
                    }
                }
                _ => unreachable!("only buffers and opened regular files have a known size"),
            };
            return Ok(PreparedBody::Replay(replay));
        }
    }
    Ok(PreparedBody::Direct(encode_body(body, compression).await?))
}

impl ReplayBody {
    pub(super) fn len(&self) -> u64 {
        match self {
            Self::Buffer(bytes) => bytes.len() as u64,
            Self::File { length, .. } => *length,
        }
    }

    /// Creates a non-replayable HTTP stream, waiting for the previous file reader to finish.
    pub(super) async fn stream(&mut self, offset: u64) -> io::Result<ClientStream> {
        match self {
            Self::Buffer(bytes) => {
                let bytes = bytes.slice(offset as usize..);
                Ok(stream::once(async move { Ok(bytes) }).boxed())
            }
            Self::File {
                file,
                start,
                length,
            } => {
                let mut owned = match file.take().expect("file is present between attempts") {
                    ReplayFile::Ready(file) => file,
                    ReplayFile::Reading { stop, task } => {
                        // The HTTP layer may retain the old body after returning a response.
                        // Stop its reader explicitly, then wait for exclusive file ownership.
                        drop(stop);
                        task.await.map_err(io::Error::other)?
                    }
                };
                owned.seek(SeekFrom::Start(*start + offset)).await?;
                let (send, receive) = mpsc::channel(1);
                let (stop, stopped) = oneshot::channel();
                let length = *length - offset;
                let task = tokio::spawn(async move {
                    {
                        let read = read_file(&mut owned, length, send);
                        futures_util::pin_mut!(read, stopped);
                        let _ = futures_util::future::select(stopped, read).await;
                    }
                    // Canceling an async read can leave disk IO running. Wait for it
                    // before returning the file to the next attempt, without reopening it.
                    File::from_std(owned.into_std().await)
                });
                *file = Some(ReplayFile::Reading { stop, task });
                Ok(stream::unfold(receive, |mut receive| async {
                    receive.recv().await.map(|bytes| (bytes, receive))
                })
                .boxed())
            }
        }
    }
}

/// Reads only while the request accepts data, with at most one buffered chunk.
async fn read_file(
    file: &mut File,
    mut remaining: u64,
    send: mpsc::Sender<io::Result<bytes::Bytes>>,
) {
    while remaining > 0 {
        let Ok(permit) = send.reserve().await else {
            break;
        };
        let mut bytes = vec![0; remaining.min(64 * 1024) as usize];
        let result = file.read(&mut bytes).await.and_then(|count| {
            if count == 0 {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "upload file became shorter",
                ))
            } else {
                remaining -= count as u64;
                bytes.truncate(count);
                Ok(bytes::Bytes::from(bytes))
            }
        });
        let failed = result.is_err();
        permit.send(result.map_err(|error| io::Error::other(PayloadReadError(error))));
        if failed {
            break;
        }
    }
}

pub(super) async fn upload(
    mut body: ReplayBody,
    handle: ResumableUpload,
) -> crate::Result<PutResponse> {
    let result = upload_inner(&mut body, &handle).await;
    if result.is_err() {
        let _ = handle.cancel().send().await;
    }
    result.map_err(Error::UploadFailed)
}

async fn upload_inner(
    body: &mut ReplayBody,
    handle: &ResumableUpload,
) -> Result<PutResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut offset = 0;
    let mut retries = 0;
    let mut probe = false;
    loop {
        let result = if probe {
            handle.progress().send().await
        } else {
            handle
                .put_stream(offset, body.len() - offset, body.stream(offset).await?)
                .send()
                .await
        };
        let progress = match result {
            Err(Error::UploadOffsetMismatch { offset }) => {
                Ok(UploadProgress::Incomplete { offset })
            }
            other => other,
        };
        let failure: Box<dyn std::error::Error + Send + Sync> = match progress {
            Ok(UploadProgress::Complete) => {
                return Ok(PutResponse {
                    key: handle.key().to_owned(),
                });
            }
            Ok(UploadProgress::Incomplete { offset: next }) => {
                if next < offset || next > body.len() {
                    return Err(Error::MalformedResponse("invalid upload offset".into()).into());
                }
                if next == offset && !probe {
                    Box::new(NoUploadProgress)
                } else {
                    if next > offset {
                        retries = 0;
                    }
                    offset = next;
                    probe = false;
                    continue;
                }
            }
            Err(error) => {
                if !is_transient(&error) {
                    return Err(error.into());
                }
                error.into()
            }
        };
        if retries >= 2 {
            return Err(failure);
        }
        backoff(retries).await;
        retries += 1;
        probe = true;
    }
}

async fn backoff(retries: u32) {
    let delay = Duration::from_secs(2 << retries) + Duration::from_secs_f64(rand::random::<f64>());
    tokio::time::sleep(delay).await;
}

fn is_transient(error: &Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = source {
        if error.is::<PayloadReadError>()
            || error
                .downcast_ref::<io::Error>()
                .and_then(io::Error::get_ref)
                .is_some_and(|inner| inner.is::<PayloadReadError>())
        {
            return false;
        }
        source = error.source();
    }
    match error {
        Error::Reqwest(error) => match error.status() {
            Some(status) => matches!(status.as_u16(), 408 | 429 | 502 | 503 | 504),
            None => {
                error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
            }
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    #[test]
    fn retry_stops_previous_reader_and_waits_for_pending_read() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let data: Vec<u8> = (0..200_000).map(|n| (n % 251) as u8).collect();
            let mut file = tempfile::tempfile().unwrap();
            std::io::Write::write_all(&mut file, &data).unwrap();
            let mut body = ReplayBody::File {
                file: Some(ReplayFile::Ready(File::from_std(file))),
                start: 7,
                length: 150_000,
            };
            let mut old = body.stream(0).await.unwrap();

            // Occupy the sole blocking worker so the old read remains pending.
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, running) = oneshot::channel();
            let worker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            running.await.unwrap();
            tokio::task::yield_now().await;
            assert!(futures_util::poll!(old.next()).is_pending());
            // Retain the old HTTP body: retry must stop its reader explicitly.
            let retry = body.stream(100);
            tokio::pin!(retry);
            assert!(futures_util::poll!(&mut retry).is_pending());
            release.send(()).unwrap();
            worker.await.unwrap();
            let retry = retry.await.unwrap();
            let remaining: Vec<_> = retry.try_collect().await.unwrap();
            assert_eq!(remaining.concat(), data[107..150_007]);
            drop(old);
        });
    }
}
