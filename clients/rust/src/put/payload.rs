//! Prepare replayable buffers and stable paths.
//!
//! Each request opens an independent reader. Compression spools to a named temporary file.
//! The HTTP layer may retain readers after an upload returns or is dropped, delaying cleanup.

use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Poll, ready};

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt, ReadBuf};

use super::{CompressionMode, PutBody};
use crate::ClientStream;

pub(super) enum PreparedBody {
    OneShot(PutBody),
    Replayable(ReplayableBody),
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
            Self::OneShot(body) => {
                request(super::encode_body(body, compression).await?, None).await
            }
            Self::Replayable(body) => {
                request(
                    reqwest::Body::wrap_stream(body.stream(0).await?),
                    Some(body.len()),
                )
                .await
            }
        }
    }
}

pub(super) enum ReplayableBody {
    Buffer(Bytes),
    Path {
        path: PathBuf,
        length: u64,
    },
    Temporary {
        file: Arc<NamedTempFile>,
        length: u64,
    },
}

pub(super) async fn prepare(
    body: PutBody,
    compression: Option<CompressionMode>,
    threshold: Option<u64>,
    spool_dir: Option<PathBuf>,
) -> crate::Result<PreparedBody> {
    let Some(threshold) = threshold else {
        return Ok(PreparedBody::OneShot(body));
    };
    let length = match &body {
        PutBody::Buffer(bytes) => Some(bytes.len() as u64),
        PutBody::Path(path) => tokio::fs::metadata(path)
            .await
            .ok()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| metadata.len()),
        _ => None,
    };
    let Some(length) = length.filter(|length| *length >= threshold) else {
        return Ok(PreparedBody::OneShot(body));
    };

    if let Some(CompressionMode::Compress(compression)) = compression {
        let temporary = tokio::task::spawn_blocking(move || {
            let temporary = match spool_dir {
                Some(dir) => NamedTempFile::new_in(dir),
                None => NamedTempFile::new(),
            }?;
            let output = temporary.as_file().try_clone()?;
            Ok::<_, io::Error>((temporary, output))
        })
        .await
        .map_err(io::Error::other)?;
        let Ok((temporary, output)) = temporary else {
            return Ok(PreparedBody::OneShot(body));
        };
        let mut output = File::from_std(output);
        let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = match body {
            PutBody::Buffer(bytes) => Box::new(io::Cursor::new(bytes)),
            PutBody::Path(path) => Box::new(File::open(path).await?),
            _ => unreachable!("only buffers and paths are eligible"),
        };
        let reader = super::BufReader::new(reader);
        let length = match compression {
            crate::Compression::Zstd => {
                tokio::io::copy(&mut super::ZstdEncoder::new(reader), &mut output).await?
            }
            crate::Compression::Gzip => {
                tokio::io::copy(&mut super::GzipEncoder::new(reader), &mut output).await?
            }
        };
        output.flush().await?;
        return Ok(PreparedBody::Replayable(ReplayableBody::Temporary {
            file: Arc::new(temporary),
            length,
        }));
    }

    Ok(PreparedBody::Replayable(match body {
        PutBody::Buffer(bytes) => ReplayableBody::Buffer(bytes),
        PutBody::Path(path) => ReplayableBody::Path { path, length },
        _ => unreachable!("only buffers and paths are eligible"),
    }))
}

/// Distinguishes local read failures from transport failures wrapped by reqwest.
#[derive(Debug, thiserror::Error)]
#[error("could not read upload payload: {0}")]
pub(super) struct ReadError(#[source] io::Error);

impl ReplayableBody {
    pub(super) fn len(&self) -> u64 {
        match self {
            Self::Buffer(bytes) => bytes.len() as u64,
            Self::Path { length, .. } | Self::Temporary { length, .. } => *length,
        }
    }

    /// Opens an independent reader for the remaining suffix.
    pub(super) async fn stream(&self, offset: u64) -> io::Result<ClientStream> {
        let mut file = match self {
            Self::Buffer(bytes) => {
                let bytes = bytes.slice(offset as usize..);
                // Streaming bodies prevent independent replays by HTTP middleware.
                return Ok(stream::once(async move { Ok(bytes) }).boxed());
            }
            Self::Path { path, .. } => File::open(path).await?,
            Self::Temporary { file, .. } => {
                let file = Arc::clone(file);
                File::from_std(
                    tokio::task::spawn_blocking(move || file.reopen())
                        .await
                        .map_err(io::Error::other)??,
                )
            }
        };
        file.seek(SeekFrom::Start(offset)).await?;
        let mut remaining = self.len() - offset;
        let mut buffer = vec![0; 64 * 1024];
        Ok(stream::poll_fn(move |cx| {
            if remaining == 0 {
                return Poll::Ready(None);
            }
            let count = remaining.min(buffer.len() as u64) as usize;
            let mut read = ReadBuf::new(&mut buffer[..count]);
            let result = ready!(Pin::new(&mut file).poll_read(cx, &mut read)).and_then(|()| {
                if read.filled().is_empty() {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "upload file became shorter",
                    ))
                } else {
                    remaining -= read.filled().len() as u64;
                    Ok(Bytes::copy_from_slice(read.filled()))
                }
            });
            if result.is_err() {
                remaining = 0;
            }
            Poll::Ready(Some(
                result.map_err(|error| io::Error::other(ReadError(error))),
            ))
        })
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn attempts_have_independent_readers() {
        let data: Vec<u8> = (0..200_000).map(|n| (n % 251) as u8).collect();
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, &data).unwrap();
        for body in [
            ReplayableBody::Path {
                path: file.path().to_owned(),
                length: data.len() as u64,
            },
            ReplayableBody::Temporary {
                file: Arc::new(file),
                length: data.len() as u64,
            },
        ] {
            let mut first = body.stream(0).await.unwrap();
            let mut received = first.try_next().await.unwrap().unwrap().to_vec();
            let second = body.stream(100).await.unwrap();
            let suffix: Vec<_> = second.try_collect().await.unwrap();
            let tail: Vec<_> = first.try_collect().await.unwrap();
            received.extend(tail.concat());
            assert_eq!(received, data);
            assert_eq!(suffix.concat(), data[100..]);
        }
    }
}
