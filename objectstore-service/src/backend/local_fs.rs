//! Local filesystem backend for development and testing.

use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, MultipartUploadBackend, PutResponse,
};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, Part, PartNumber, UploadId, UploadPartResponse,
};
use crate::stream::{self, ClientStream};

/// Configuration for [`LocalFsBackend`].
///
/// Stores objects as files on the local filesystem. Suitable for development, testing,
/// and single-server deployments.
///
/// # Example
///
/// ```yaml
/// storage:
///   type: filesystem
///   path: /data
/// ```
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FileSystemConfig {
    /// Directory path for storing objects.
    ///
    /// The directory will be created if it doesn't exist. Relative paths are resolved from
    /// the server's working directory.
    ///
    /// # Default
    ///
    /// `"data"` (relative to the server's working directory)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE=filesystem`
    /// - `OS__STORAGE__PATH=/path/to/storage`
    pub path: PathBuf,
}

/// Local filesystem backend for development and testing.
#[derive(Debug)]
pub struct LocalFsBackend {
    path: PathBuf,
}

impl LocalFsBackend {
    /// Creates a new [`LocalFsBackend`] rooted at the directory in `config`.
    pub fn new(config: FileSystemConfig) -> Self {
        Self { path: config.path }
    }
}

#[async_trait::async_trait]
impl Backend for LocalFsBackend {
    fn name(&self) -> &'static str {
        "local-fs"
    }

    fn as_multipart_upload_backend(self: Arc<Self>) -> Result<Arc<dyn MultipartUploadBackend>> {
        Ok(self)
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        let path = self.path.join(id.as_storage_path().to_string());
        objectstore_log::debug!(path=%path.display(), "Writing to local_fs backend");
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;

        let mut reader = pin!(StreamReader::new(stream));
        let mut writer = BufWriter::new(file);

        let metadata_json = serde_json::to_string(metadata).map_err(|cause| Error::Serde {
            context: "failed to serialize metadata".to_string(),
            cause,
        })?;
        writer.write_all(metadata_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::Client(ce),
                None => e.into(),
            })?;

        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(())
    }

    // TODO: Return `Ok(None)` if object is found but past expiry
    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from local_fs backend");
        let path = self.path.join(id.as_storage_path().to_string());
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                objectstore_log::debug!("Object not found");
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(file);
        let mut metadata_line = String::new();
        reader.read_line(&mut metadata_line).await?;
        let file_len = reader.get_ref().metadata().await?.len();
        let mut metadata: Metadata =
            serde_json::from_str(metadata_line.trim_end()).map_err(|cause| Error::Serde {
                context: "failed to deserialize metadata".to_string(),
                cause,
            })?;
        let payload_size = file_len
            .checked_sub(metadata_line.len() as u64)
            .ok_or_else(|| Error::generic("local-fs file corrupted: shorter than header"))?;
        metadata.size = Some(payload_size as usize);

        let stream = ReaderStream::new(reader);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from local_fs backend");
        let path = self.path.join(id.as_storage_path().to_string());
        let result = tokio::fs::remove_file(path).await;
        if let Err(e) = &result
            && e.kind() == ErrorKind::NotFound
        {
            objectstore_log::debug!("Object not found");
        }
        Ok(result?)
    }
}

fn safe_join(base: &Path, segment: &str) -> Result<PathBuf> {
    for component in Path::new(segment).components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(Error::generic(format!("invalid path segment: {segment}")));
        }
    }
    Ok(base.join(segment))
}

impl LocalFsBackend {
    fn multipart_dir(&self, id: &ObjectId, upload_id: &UploadId) -> Result<PathBuf> {
        let base = self
            .path
            .join("__multipart__")
            .join(id.as_storage_path().to_string());
        safe_join(&base, upload_id)
    }
}

#[async_trait::async_trait]
impl MultipartUploadBackend for LocalFsBackend {
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        let upload_id = uuid::Uuid::now_v7().to_string();
        let dir = self.multipart_dir(id, &upload_id)?;
        tokio::fs::create_dir_all(&dir).await?;

        let meta_path = dir.join("metadata.json");
        let metadata_json = serde_json::to_string(metadata).map_err(|cause| Error::Serde {
            context: "failed to serialize multipart metadata".to_string(),
            cause,
        })?;
        tokio::fs::write(meta_path, metadata_json).await?;

        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        part_number: PartNumber,
        content_length: u64,
        _content_md5: Option<&str>,
        body: ClientStream,
    ) -> Result<UploadPartResponse> {
        let dir = self.multipart_dir(id, upload_id)?;
        if !tokio::fs::try_exists(&dir).await? {
            return Err(Error::generic("multipart upload not found"));
        }

        let etag = format!("\"etag-{part_number}-{content_length}\"");

        let header = serde_json::json!({
            "etag": etag,
            "uploaded_at": SystemTime::now(),
            "size": content_length,
        });
        let header_line = serde_json::to_string(&header).map_err(|cause| Error::Serde {
            context: "failed to serialize part header".to_string(),
            cause,
        })?;

        let part_path = dir.join(format!("{part_number}.part"));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(part_path)
            .await?;

        let mut reader = pin!(StreamReader::new(body));
        let mut writer = BufWriter::new(file);
        writer.write_all(header_line.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        let _bytes_copied = tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::Client(ce),
                None => e.into(),
            })?;

        // TODO: validate bytes_copied against content_length and return a BadRequest-style
        // error. Needs a service-layer error variant that maps to HTTP 400 without abusing
        // ClientError (which is meant for stream errors).

        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(etag)
    }

    async fn list_parts(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse> {
        let dir = self.multipart_dir(id, upload_id)?;
        if !tokio::fs::try_exists(&dir).await? {
            return Err(Error::generic("multipart upload not found"));
        }

        let mut entries = tokio::fs::read_dir(&dir).await?;
        let mut parts = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let Some(pn_str) = name_str.strip_suffix(".part") else {
                continue;
            };
            let Ok(pn) = pn_str.parse::<PartNumber>() else {
                continue;
            };

            if part_number_marker.is_some_and(|marker| pn <= marker) {
                continue;
            }

            let file = tokio::fs::File::open(entry.path()).await?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader.read_line(&mut header_line).await?;
            let header: serde_json::Value =
                serde_json::from_str(header_line.trim_end()).map_err(|cause| Error::Serde {
                    context: "failed to deserialize part header".to_string(),
                    cause,
                })?;

            parts.push(Part {
                part_number: pn,
                etag: header["etag"].as_str().unwrap_or("").to_string(),
                last_modified: serde_json::from_value(header["uploaded_at"].clone())
                    .unwrap_or(SystemTime::UNIX_EPOCH),
                size: header["size"].as_u64().unwrap_or(0),
            });
        }

        parts.sort_by_key(|p| p.part_number);

        let max = max_parts.unwrap_or(u32::MAX) as usize;
        let is_truncated = parts.len() > max;
        parts.truncate(max);

        let next_part_number_marker = if is_truncated {
            parts.last().map(|p| p.part_number)
        } else {
            None
        };

        Ok(ListPartsResponse {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    async fn abort_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartResponse> {
        let dir = self.multipart_dir(id, upload_id)?;
        if tokio::fs::try_exists(&dir).await? {
            tokio::fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }

    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartResponse> {
        let dir = self.multipart_dir(id, upload_id)?;
        if !tokio::fs::try_exists(&dir).await? {
            return Err(Error::generic("multipart upload not found"));
        }

        // Read metadata
        let meta_path = dir.join("metadata.json");
        let meta_bytes = tokio::fs::read(&meta_path).await?;
        let metadata: Metadata =
            serde_json::from_slice(&meta_bytes).map_err(|cause| Error::Serde {
                context: "failed to deserialize multipart metadata".to_string(),
                cause,
            })?;

        // TODO: validate that parts are in ascending part_number order and reject with
        // InvalidPartOrder if not (matches S3/GCS behavior). Needs a proper client error variant.

        // Validate all parts (headers only) before writing anything
        for completed in &parts {
            let part_path = dir.join(format!("{}.part", completed.part_number));
            if !tokio::fs::try_exists(&part_path).await? {
                return Ok(Some(crate::multipart::CompleteMultipartError {
                    code: "InvalidPart".into(),
                    message: format!("part number {} was not uploaded", completed.part_number),
                }));
            }

            let file = tokio::fs::File::open(&part_path).await?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader.read_line(&mut header_line).await?;
            let header: serde_json::Value =
                serde_json::from_str(header_line.trim_end()).map_err(|cause| Error::Serde {
                    context: "failed to deserialize part header".to_string(),
                    cause,
                })?;

            let stored_etag = header["etag"].as_str().unwrap_or("");
            if stored_etag != completed.etag {
                return Ok(Some(crate::multipart::CompleteMultipartError {
                    code: "InvalidPart".into(),
                    message: format!(
                        "etag mismatch for part {}: expected {}, got {}",
                        completed.part_number, stored_etag, completed.etag
                    ),
                }));
            }
        }

        // Stream parts directly to the final object file
        let path = self.path.join(id.as_storage_path().to_string());
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;
        let mut writer = BufWriter::new(file);

        let metadata_json = serde_json::to_string(&metadata).map_err(|cause| Error::Serde {
            context: "failed to serialize metadata".to_string(),
            cause,
        })?;
        writer.write_all(metadata_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        for completed in &parts {
            let part_path = dir.join(format!("{}.part", completed.part_number));
            let file = tokio::fs::File::open(&part_path).await?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader.read_line(&mut header_line).await?;
            tokio::io::copy(&mut reader, &mut writer).await?;
        }

        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        // Clean up multipart state
        tokio::fs::remove_dir_all(dir).await?;

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::{Duration, SystemTime};

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::{Compression, ExpirationPolicy};
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;
    use crate::stream;

    #[tokio::test]
    async fn stores_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(FileSystemConfig {
            path: tempdir.path().to_path_buf(),
        });

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_created: Some(SystemTime::now()),
            time_expires: None,
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            custom: [("foo".into(), "bar".into())].into(),
            size: None,
        };
        backend
            .put_object(&id, &metadata, stream::single("oh hai!"))
            .await
            .unwrap();

        let (read_metadata, stream) = backend.get_object(&id).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(
            read_metadata,
            Metadata {
                size: Some(file_contents.len()),
                ..metadata
            }
        );
        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn get_metadata_returns_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(FileSystemConfig {
            path: tempdir.path().to_path_buf(),
        });

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            custom: [("foo".into(), "bar".into())].into(),
            ..Default::default()
        };
        backend
            .put_object(&id, &metadata, stream::single("oh hai!"))
            .await
            .unwrap();

        let read_metadata = backend.get_metadata(&id).await.unwrap().unwrap();
        assert_eq!(
            read_metadata,
            Metadata {
                size: Some(7),
                ..metadata
            }
        );
    }

    #[tokio::test]
    async fn get_metadata_nonexistent() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(FileSystemConfig {
            path: tempdir.path().to_path_buf(),
        });

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let result = backend.get_metadata(&id).await.unwrap();
        assert!(result.is_none());
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    fn make_backend() -> (tempfile::TempDir, LocalFsBackend) {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(FileSystemConfig {
            path: tempdir.path().to_path_buf(),
        });
        (tempdir, backend)
    }

    #[tokio::test]
    async fn multipart_single_part() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            origin: Some("203.0.113.42".into()),
            custom: [("foo".into(), "bar".into())].into(),
            ..Default::default()
        };

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let data = b"hello, multipart world!";
        let etag = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                data.len() as u64,
                None,
                stream::single(data.to_vec()),
            )
            .await
            .unwrap();

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![crate::multipart::CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "expected no error on complete");

        let (meta, body) = backend.get_object(&id).await.unwrap().unwrap();
        let payload: BytesMut = body.try_collect().await.unwrap();
        assert_eq!(payload.as_ref(), data);
        assert_eq!(meta.content_type, "text/plain".to_string());
        assert_eq!(
            meta.expiration_policy,
            ExpirationPolicy::TimeToIdle(Duration::from_secs(3600))
        );
        assert_eq!(meta.origin, Some("203.0.113.42".into()));
        assert_eq!(meta.custom, [("foo".into(), "bar".into())].into());
    }

    #[tokio::test]
    async fn multipart_multiple_parts() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let part1 = b"aaaa".to_vec();
        let part2 = b"bbbb".to_vec();
        let part3 = b"cc".to_vec();

        let etag1 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                part1.len() as u64,
                None,
                stream::single(part1.clone()),
            )
            .await
            .unwrap();
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(2).unwrap(),
                part2.len() as u64,
                None,
                stream::single(part2.clone()),
            )
            .await
            .unwrap();
        let etag3 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(3).unwrap(),
                part3.len() as u64,
                None,
                stream::single(part3.clone()),
            )
            .await
            .unwrap();

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![
                    crate::multipart::CompletedPart {
                        part_number: NonZeroU32::new(1).unwrap(),
                        etag: etag1,
                    },
                    crate::multipart::CompletedPart {
                        part_number: NonZeroU32::new(2).unwrap(),
                        etag: etag2,
                    },
                    crate::multipart::CompletedPart {
                        part_number: NonZeroU32::new(3).unwrap(),
                        etag: etag3,
                    },
                ],
            )
            .await
            .unwrap();
        assert!(result.is_none());

        let (_, body) = backend.get_object(&id).await.unwrap().unwrap();
        let payload: BytesMut = body.try_collect().await.unwrap();
        assert_eq!(payload.as_ref(), b"aaaabbbbcc");
    }

    #[tokio::test]
    async fn multipart_list_parts() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let etag1 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                3,
                None,
                stream::single(b"aaa".to_vec()),
            )
            .await
            .unwrap();
        let etag2 = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(2).unwrap(),
                3,
                None,
                stream::single(b"bbb".to_vec()),
            )
            .await
            .unwrap();

        let list = backend
            .list_parts(&id, &upload_id, None, None)
            .await
            .unwrap();
        assert_eq!(list.parts.len(), 2);
        assert_eq!(list.parts[0].part_number.get(), 1);
        assert_eq!(list.parts[0].etag, etag1);
        assert_eq!(list.parts[0].size, 3);
        assert_eq!(list.parts[1].part_number.get(), 2);
        assert_eq!(list.parts[1].etag, etag2);
        assert_eq!(list.parts[1].size, 3);

        // Pagination
        let page1 = backend
            .list_parts(&id, &upload_id, Some(1), None)
            .await
            .unwrap();
        assert_eq!(page1.parts.len(), 1);
        assert_eq!(page1.parts[0].part_number.get(), 1);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        let page2 = backend
            .list_parts(&id, &upload_id, Some(1), page1.next_part_number_marker)
            .await
            .unwrap();
        assert_eq!(page2.parts.len(), 1);
        assert_eq!(page2.parts[0].part_number.get(), 2);

        backend.abort_multipart(&id, &upload_id).await.unwrap();
    }

    #[tokio::test]
    async fn multipart_abort() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                5,
                None,
                stream::single(b"hello".to_vec()),
            )
            .await
            .unwrap();

        backend.abort_multipart(&id, &upload_id).await.unwrap();

        let result = backend.get_object(&id).await.unwrap();
        assert!(result.is_none(), "object should not exist after abort");
    }

    #[tokio::test]
    async fn multipart_invalid_etag() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let etag = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                5,
                None,
                stream::single(b"hello".to_vec()),
            )
            .await
            .unwrap();

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![crate::multipart::CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag: "wrong-etag".into(),
                }],
            )
            .await
            .unwrap();
        assert!(result.is_some(), "expected error for bad etag");
        assert_eq!(result.unwrap().code, "InvalidPart");

        // Upload must survive a failed complete so the client can retry.
        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![crate::multipart::CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "retry with correct etag should succeed");
    }

    #[tokio::test]
    async fn multipart_missing_part() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let etag = backend
            .upload_part(
                &id,
                &upload_id,
                NonZeroU32::new(1).unwrap(),
                5,
                None,
                stream::single(b"hello".to_vec()),
            )
            .await
            .unwrap();

        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![crate::multipart::CompletedPart {
                    part_number: NonZeroU32::new(99).unwrap(),
                    etag: "whatever".into(),
                }],
            )
            .await
            .unwrap();
        assert!(result.is_some(), "expected error for missing part");
        assert_eq!(result.unwrap().code, "InvalidPart");

        // Upload must survive a failed complete so the client can retry.
        let result = backend
            .complete_multipart(
                &id,
                &upload_id,
                vec![crate::multipart::CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "retry with correct part should succeed");
    }
}
