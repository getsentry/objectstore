//! Helpers to buffer resumable upload bodies.

use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use async_compression::tokio::bufread::{GzipEncoder, ZstdEncoder};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;

use crate::put::{CompressionMode, PutBody, encode_body};
use crate::{ClientStream, Compression};

const READ_BUFFER_SIZE: usize = 4 * 1024;

pub(crate) enum PreparedBody {
    OneShot(PutBody),
    Replayable(ReplayableBody),
}

impl PreparedBody {
    pub async fn into_reqwest_body(
        self,
        compression: Option<CompressionMode>,
    ) -> io::Result<reqwest::Body> {
        match self {
            Self::OneShot(body) => encode_body(body, compression).await,
            Self::Replayable(body) => Ok(reqwest::Body::wrap_stream(body.stream(0).await?)),
        }
    }
}

pub(crate) enum ReplayableBody {
    Buffer(Bytes),
    Path {
        path: PathBuf,
        length: u64,
    },
    Temporary {
        file: Arc<std::fs::File>,
        length: u64,
    },
}

impl ReplayableBody {
    pub fn len(&self) -> u64 {
        match self {
            Self::Buffer(bytes) => bytes.len() as u64,
            Self::Path { length, .. } | Self::Temporary { length, .. } => *length,
        }
    }

    /// Streams the remaining suffix with an independent read position.
    pub async fn stream(&self, offset: u64) -> io::Result<ClientStream> {
        let mut file = match self {
            Self::Buffer(bytes) => {
                let bytes = bytes.slice(offset as usize..);
                // Streaming bodies prevent independent replays by HTTP middleware.
                return Ok(stream::once(async move { Ok(bytes) }).boxed());
            }
            Self::Path { path, .. } => File::open(path).await?,
            Self::Temporary { file, .. } => {
                let file = Arc::clone(file);
                let length = self.len();
                // Positional reads let old requests finish without changing a retry's position.
                return Ok(stream::iter((offset..length).step_by(READ_BUFFER_SIZE))
                    .then(move |position| {
                        let file = Arc::clone(&file);
                        async move {
                            tokio::task::spawn_blocking(move || {
                                let mut buffer = BytesMut::zeroed(
                                    (length - position).min(READ_BUFFER_SIZE as u64) as usize,
                                );
                                read_exact_at(&file, &mut buffer, position)?;
                                Ok::<_, io::Error>(buffer.freeze())
                            })
                            .await
                            .map_err(io::Error::other)?
                        }
                    })
                    .map_err(|error| io::Error::other(ReadError(error)))
                    .boxed());
            }
        };
        if file.metadata().await?.len() < self.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upload file became shorter",
            ));
        }
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(ReaderStream::new(file.take(self.len() - offset))
            .map_err(|error| io::Error::other(ReadError(error)))
            .boxed())
    }
}

pub async fn prepare(
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
        let temporary = tokio::task::spawn_blocking(move || match spool_dir {
            Some(dir) => tempfile::tempfile_in(dir),
            None => tempfile::tempfile(),
        })
        .await
        .map_err(io::Error::other)?;
        let Ok(temporary) = temporary else {
            return Ok(PreparedBody::OneShot(body));
        };
        let mut output = File::from_std(temporary);
        let length = match body {
            PutBody::Buffer(bytes) => {
                compress_into(io::Cursor::new(bytes), compression, &mut output).await?
            }
            PutBody::Path(path) => {
                compress_into(
                    BufReader::new(File::open(path).await?),
                    compression,
                    &mut output,
                )
                .await?
            }
            // Unreachable: the eligibility check above only accepts buffers and paths.
            body => return Ok(PreparedBody::OneShot(body)),
        };
        output.flush().await?;
        return Ok(PreparedBody::Replayable(ReplayableBody::Temporary {
            file: Arc::new(output.into_std().await),
            length,
        }));
    }

    Ok(PreparedBody::Replayable(match body {
        PutBody::Buffer(bytes) => ReplayableBody::Buffer(bytes),
        PutBody::Path(path) => ReplayableBody::Path { path, length },
        // Unreachable: the eligibility check above only accepts buffers and paths.
        body => return Ok(PreparedBody::OneShot(body)),
    }))
}

fn read_exact_at(file: &std::fs::File, buffer: &mut [u8], position: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buffer, position)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut buffer = buffer;
        let mut position = position;
        while !buffer.is_empty() {
            match file.seek_read(buffer, position) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => {
                    position += count as u64;
                    buffer = &mut buffer[count..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, buffer, position);
        Err(io::ErrorKind::Unsupported.into())
    }
}

async fn compress_into(
    reader: impl AsyncBufRead + Unpin,
    compression: Compression,
    output: &mut File,
) -> io::Result<u64> {
    match compression {
        Compression::Zstd => tokio::io::copy(&mut ZstdEncoder::new(reader), output).await,
        Compression::Gzip => tokio::io::copy(&mut GzipEncoder::new(reader), output).await,
    }
}

/// Distinguishes local read failures from transport failures wrapped by reqwest.
#[derive(Debug, thiserror::Error)]
#[error("could not read upload payload: {0}")]
pub(crate) struct ReadError(#[source] io::Error);

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn attempts_have_independent_readers() {
        let data: Vec<u8> = (0..200_000).map(|n| (n % 251) as u8).collect();
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, &data).unwrap();
        let mut temporary = tempfile::tempfile().unwrap();
        std::io::Write::write_all(&mut temporary, &data).unwrap();
        for body in [
            ReplayableBody::Path {
                path: file.path().to_owned(),
                length: data.len() as u64,
            },
            ReplayableBody::Temporary {
                file: Arc::new(temporary),
                length: data.len() as u64,
            },
        ] {
            let mut first = body.stream(0).await.unwrap();
            let mut received = first.try_next().await.unwrap().unwrap().to_vec();
            let second = body.stream(100).await.unwrap();
            drop(body);
            let (suffix, tail): (Vec<_>, Vec<_>) =
                futures_util::try_join!(second.try_collect(), first.try_collect()).unwrap();
            received.extend(tail.concat());
            assert_eq!(received, data);
            assert_eq!(suffix.concat(), data[100..]);
        }
    }

    #[test]
    fn cancelled_read_does_not_interfere_with_retry() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let data: Vec<u8> = (0..20_000).map(|n| (n % 251) as u8).collect();
            let mut file = tempfile::tempfile().unwrap();
            std::io::Write::write_all(&mut file, &data).unwrap();
            let body = ReplayableBody::Temporary {
                file: Arc::new(file),
                length: data.len() as u64,
            };

            // Keep the first read pending while its request is dropped and a retry starts.
            let (release, blocked) = std::sync::mpsc::channel();
            let worker = tokio::task::spawn_blocking(move || blocked.recv().unwrap());
            let mut first = body.stream(0).await.unwrap();
            assert!(futures_util::poll!(first.next()).is_pending());
            drop(first);
            let mut retry = body.stream(107).await.unwrap();
            assert!(futures_util::poll!(retry.next()).is_pending());
            drop(body);
            release.send(()).unwrap();
            worker.await.unwrap();

            let suffix: Vec<_> = retry.try_collect().await.unwrap();
            assert!(suffix.iter().all(|chunk| chunk.len() <= READ_BUFFER_SIZE));
            assert_eq!(suffix.concat(), data[107..]);
        });
    }
}
