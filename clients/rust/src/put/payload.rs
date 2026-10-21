//! Prepare replayable wire bytes and give each file request exclusive reader ownership.

use std::io::{self, SeekFrom};
use std::path::PathBuf;

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use super::{CompressionMode, PutBody};
use crate::ClientStream;

pub(super) enum PreparedBody {
    Direct(PutBody),
    Replay(ReplayBody),
}

impl PreparedBody {
    /// Supplies a direct request with either the original input or prepared wire bytes.
    pub(super) async fn with_body<T, F>(
        self,
        compression: Option<CompressionMode>,
        request: impl FnOnce(reqwest::Body, Option<u64>) -> F,
    ) -> crate::Result<T>
    where
        F: std::future::Future<Output = crate::Result<T>>,
    {
        match self {
            Self::Direct(body) => request(super::encode_body(body, compression).await?, None).await,
            Self::Replay(mut body) => {
                let length = body.len();
                body.with_stream(0, |stream| {
                    request(reqwest::Body::wrap_stream(stream), Some(length))
                })
                .await
            }
        }
    }
}

pub(super) enum ReplayBody {
    Buffer(Bytes),
    File { file: File, start: u64, length: u64 },
}

pub(super) async fn prepare(
    mut body: PutBody,
    compression: Option<CompressionMode>,
    threshold: Option<u64>,
    spool_dir: Option<PathBuf>,
) -> crate::Result<PreparedBody> {
    let Some(threshold) = threshold else {
        return Ok(PreparedBody::Direct(body));
    };
    if let PutBody::Path(path) = body {
        body = PutBody::File(File::open(path).await?);
    }
    let Some(mut length) = body.len().await else {
        return Ok(PreparedBody::Direct(body));
    };
    if length < threshold {
        return Ok(PreparedBody::Direct(body));
    }

    if let Some(CompressionMode::Compress(crate::Compression::Zstd)) = compression {
        let temporary = tokio::task::spawn_blocking(move || match spool_dir {
            Some(dir) => tempfile::tempfile_in(dir),
            None => tempfile::tempfile(),
        })
        .await
        .map_err(io::Error::other)?;
        let Ok(temporary) = temporary else {
            // Creation has not consumed input, so direct streaming is still possible.
            return Ok(PreparedBody::Direct(body));
        };
        let mut temporary = File::from_std(temporary);
        let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = match body {
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

    Ok(PreparedBody::Replay(match body {
        PutBody::Buffer(bytes) => ReplayBody::Buffer(bytes),
        PutBody::File(mut file) => ReplayBody::File {
            start: file.stream_position().await?,
            file,
            length,
        },
        _ => unreachable!("only buffers and opened regular files have a known size"),
    }))
}

impl ReplayBody {
    pub(super) fn len(&self) -> u64 {
        match self {
            Self::Buffer(bytes) => bytes.len() as u64,
            Self::File { length, .. } => *length,
        }
    }

    /// Runs a request with a suffix, then stops its reader before returning the result.
    ///
    /// The request body may outlive the response. Keeping reader ownership here makes local
    /// read errors explicit and prevents an old request from reading after a retry seeks.
    pub(super) async fn with_stream<T, F>(
        &mut self,
        offset: u64,
        request: impl FnOnce(ClientStream) -> F,
    ) -> crate::Result<T>
    where
        F: std::future::Future<Output = crate::Result<T>>,
    {
        match self {
            Self::Buffer(bytes) => {
                let bytes = bytes.slice(offset as usize..);
                // Streaming bodies prevent independent replays by HTTP middleware.
                request(stream::once(async move { Ok(bytes) }).boxed()).await
            }
            Self::File {
                file,
                start,
                length,
            } => {
                let (stream, reader) =
                    FileReader::start(file, *start + offset, *length - offset).await?;
                let response = request(stream).await;
                // Read failures are returned directly, independently of reqwest's error wrapping.
                reader.finish().await?;
                response
            }
        }
    }
}

/// Explicitly stoppable because HTTP may retain the request body after returning a response.
/// Dropping this handle signals cancellation; finishing also waits for pending disk IO.
struct FileReader {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl FileReader {
    async fn start(file: &File, offset: u64, length: u64) -> io::Result<(ClientStream, Self)> {
        // Duplicate the open handle, never reopen a path. Clones share the OS cursor, so
        // with_stream must finish this reader before starting another request.
        let mut file = file.try_clone().await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let (send, receive) = mpsc::channel(1);
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = {
                let read = forward_file(&mut file, length, send);
                futures_util::pin_mut!(read, stopped);
                match futures_util::future::select(stopped, read).await {
                    futures_util::future::Either::Left(_) => Ok(()),
                    futures_util::future::Either::Right((result, _)) => result,
                }
            };
            // An async read may leave blocking IO running after cancellation.
            drop(file.into_std().await);
            result
        });
        let stream = stream::unfold(receive, |mut receive| async {
            receive.recv().await.map(|bytes| (bytes, receive))
        })
        .boxed();
        Ok((stream, Self { stop, task }))
    }

    async fn finish(self) -> io::Result<()> {
        drop(self.stop);
        self.task.await.map_err(io::Error::other)?
    }
}

async fn forward_file(
    file: &mut File,
    length: u64,
    send: mpsc::Sender<io::Result<Bytes>>,
) -> io::Result<()> {
    let mut remaining = length;
    while remaining > 0 {
        let Ok(permit) = send.reserve().await else {
            break;
        };
        let mut bytes = vec![0; remaining.min(64 * 1024) as usize];
        if let Err(error) = file.read_exact(&mut bytes).await {
            // Fail the HTTP body too, but keep the original error for the caller.
            permit.send(Err(io::Error::other(error.to_string())));
            return Err(error);
        }
        remaining -= bytes.len() as u64;
        permit.send(Ok(Bytes::from(bytes)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    #[test]
    fn stopping_reader_waits_for_pending_read_even_if_http_keeps_body() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let data: Vec<u8> = (0..200_000).map(|n| (n % 251) as u8).collect();
            let mut file = tempfile::tempfile().unwrap();
            std::io::Write::write_all(&mut file, &data).unwrap();
            let file = File::from_std(file);
            let (mut old, reader) = FileReader::start(&file, 7, 150_000).await.unwrap();

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
            let finish = reader.finish();
            tokio::pin!(finish);
            assert!(futures_util::poll!(&mut finish).is_pending());
            release.send(()).unwrap();
            worker.await.unwrap();
            finish.await.unwrap();
            let (stream, reader) = FileReader::start(&file, 107, 149_900).await.unwrap();
            let remaining: Vec<_> = stream.try_collect().await.unwrap();
            reader.finish().await.unwrap();
            assert_eq!(remaining.concat(), data[107..150_007]);
            drop(old);
        });
    }
}
