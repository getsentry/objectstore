//! Local filesystem backend for development and testing.
//!
//! Complete object files are published by atomically renaming same-directory drafts, so readers
//! observe either the previous or next complete file. Unpublished drafts are removed automatically
//! when dropped.
//!
//! To avoid races on metadata, expiry, and upload updates, this backend uses locks placed under
//! `.locks/` to synchronize mutations across backend instances and cooperating processes. The first
//! two bytes of the BLAKE3 hash of an object's storage path select one of 65,536 permanent lock
//! slots under `.locks/<first byte>/<second byte>` (lowercase hexadecimal). An object and all of its
//! resumable uploads share the same lock.
//!
//! Shared filesystems are supported only when locks propagate across the cluster, pathname
//! visibility is coherent, and rename is atomic.

use std::fs::File;
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;
use objectstore_types::resumable::UploadProgress;
use objectstore_types::time::Timestamp;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio_util::io::{ReaderStream, StreamReader};
use uuid::Uuid;

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, MultipartUploadBackend, PutResponse,
};
use crate::change_stream::{
    ChangeStream, ChangeStreamFactory, CostTrackerStreamConfig, flush_change_stream,
};
use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, Part, PartNumber, UploadId, UploadPartResponse,
};
use crate::resumable::BackendToken;
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

    /// Reports what this backend stores, for per-usecase cost attribution.
    ///
    /// # Default
    ///
    /// `None`, which disables reporting for this backend.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__COGS__SHARED_RESOURCE_ID=filesystem_objectstore`
    /// - `OS__STORAGE__COGS__SAMPLE_RATE=1.0` (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

/// Local filesystem backend for development and testing.
#[derive(Debug)]
pub struct LocalFsBackend {
    path: PathBuf,
    locks: ObjectLocks,

    change_stream: Arc<dyn ChangeStream>,
}

impl LocalFsBackend {
    /// Creates a new [`LocalFsBackend`] rooted at the directory in `config`.
    pub fn new(config: FileSystemConfig, streams: &ChangeStreamFactory) -> Self {
        let FileSystemConfig { path, cogs } = config;
        let locks = ObjectLocks::new(&path);
        Self {
            path,
            locks,
            change_stream: streams.build(cogs.as_ref()),
        }
    }

    /// Returns the filesystem path for the given object ID.
    fn path(&self, id: &ObjectId) -> PathBuf {
        self.path.join(id.as_storage_path().to_string())
    }

    fn upload_path(&self, upload_id: Uuid) -> PathBuf {
        self.path.join("uploads").join(upload_id.to_string())
    }

    /// Ensures that an object file can be created at the given path.
    async fn create_dir_all(path: &Path) -> Result<()> {
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .context(
                ErrorKind::BackendFailure,
                "creating local-fs object directory",
            )
    }
}

#[async_trait::async_trait]
impl Backend for LocalFsBackend {
    fn name(&self) -> &'static str {
        "local-fs"
    }

    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        Ok(self)
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        let path = self.path(id);
        objectstore_log::debug!(path=%path.display(), "Writing to local_fs backend");
        Self::create_dir_all(&path).await?;

        let mut draft = Draft::create(&path, metadata).await?;
        let mut reader = pin!(StreamReader::new(stream));
        let payload_size = tokio::io::copy(&mut reader, draft.writer())
            .await
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::from(ce),
                None => Error::with_context(
                    ErrorKind::BackendFailure,
                    "writing local-fs object payload",
                    e,
                ),
            })?;

        let stored_size = draft.preamble_len() + payload_size;

        draft.prepare().await?;
        let _guard = self.locks.acquire(id).await?;
        draft.publish().await?;

        self.change_stream
            .write(id, stored_size, metadata.time_expires);

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from local_fs backend");
        let path = self.path(id);
        let Some(object) = ObjectFile::try_open(&path, access_time).await? else {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        };
        let ObjectFile {
            metadata,
            preamble_len,
            payload_size,
            mut reader,
        } = object;

        let (content_range, stream) = match range {
            Some(byte_range) => {
                let content_range =
                    byte_range
                        .resolve(payload_size)
                        .ok_or(ErrorKind::RangeNotSatisfiable {
                            total: payload_size,
                        })?;
                let payload_start = preamble_len + content_range.start;
                reader
                    .seek(std::io::SeekFrom::Start(payload_start))
                    .await
                    .context(ErrorKind::BackendFailure, "seeking local-fs object payload")?;
                let limited = reader.take(content_range.len());
                (Some(content_range), ReaderStream::new(limited).boxed())
            }
            None => (None, ReaderStream::new(reader).boxed()),
        };
        Ok(Some((metadata, content_range, stream)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_expiry(
        &self,
        id: &ObjectId,
        expire_at: Timestamp,
        access_time: Timestamp,
    ) -> Result<bool> {
        let _guard = self.locks.acquire(id).await?;

        let path = self.path(id);
        let Some(object) = ObjectFile::try_open(&path, access_time).await? else {
            return Ok(false);
        };
        let ObjectFile {
            mut metadata,
            mut reader,
            ..
        } = object;

        let Some(current_expiry) = metadata.time_expires else {
            return Ok(false);
        };
        if current_expiry >= expire_at {
            return Ok(true); // already satisfied
        }
        metadata.time_expires = Some(expire_at);

        let mut draft = Draft::create(&path, &metadata).await?;
        tokio::io::copy(&mut reader, draft.writer()).await.context(
            ErrorKind::BackendFailure,
            "copying local-fs object payload for expiry extension",
        )?;

        draft.prepare().await?;
        draft.publish().await?;

        self.change_stream.update(id, Some(expire_at));

        Ok(true)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
        let _guard = self.locks.acquire(id).await?;

        objectstore_log::debug!("Deleting from local_fs backend");
        let path = self.path(id);
        match tokio::fs::remove_file(path).await {
            Ok(()) => self.change_stream.delete(id),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                objectstore_log::debug!("Object not found");
            }
            result => {
                result.context(ErrorKind::BackendFailure, "deleting local-fs object")?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", fields(?id, total_length), skip_all)]
    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        total_length: NonZeroU64,
    ) -> Result<Option<BackendToken>> {
        let upload_id = uuid::Uuid::now_v7();
        let path = self.upload_path(upload_id);
        Self::create_dir_all(&path).await?;
        UploadFile::create(&path, metadata).await?;
        Ok(Some(format!("{total_length}.{upload_id}")))
    }

    #[tracing::instrument(level = "debug", fields(?id, offset, content_length), skip_all)]
    async fn put_chunk(
        &self,
        id: &ObjectId,
        token: &BackendToken,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        let session = UploadSession::from_token(token)?;
        offset
            .checked_add(content_length)
            .filter(|end| *end <= session.total_length.get())
            .ok_or(ErrorKind::ChunkExceedsUploadLength {
                offset,
                content_length,
                upload_length: session.total_length.get(),
            })?;
        let _guard = self.locks.acquire(id).await?;

        let upload_path = self.upload_path(session.upload_id);
        let mut upload = UploadFile::open(&upload_path).await?;
        if upload.offset() == session.total_length.get() {
            return Err(ErrorKind::UploadSessionGone.into());
        }
        if content_length != 0 && offset != upload.offset() {
            return Err(ErrorKind::UploadOffsetMismatch {
                offset: upload.offset(),
            }
            .into());
        }

        let persisted_offset = upload.append(stream, content_length).await?;

        if persisted_offset != session.total_length.get() {
            return Ok(UploadProgress::Incomplete {
                offset: persisted_offset,
            });
        }

        let object_path = self.path(id);
        Self::create_dir_all(&object_path).await?;
        let (stored_size, expires_at) = upload.publish(object_path).await?;
        self.change_stream.write(id, stored_size, expires_at);
        Ok(UploadProgress::Complete)
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn upload_offset(&self, id: &ObjectId, token: &BackendToken) -> Result<UploadProgress> {
        let session = UploadSession::from_token(token)?;
        let _guard = self.locks.acquire(id).await?;
        let upload = UploadFile::open(&self.upload_path(session.upload_id)).await?;
        if upload.offset() == session.total_length.get() {
            Err(ErrorKind::UploadSessionGone.into())
        } else {
            Ok(UploadProgress::Incomplete {
                offset: upload.offset(),
            })
        }
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn cancel_upload(&self, id: &ObjectId, token: &BackendToken) -> Result<()> {
        let session = UploadSession::from_token(token)?;
        let _guard = self.locks.acquire(id).await?;
        let path = self.upload_path(session.upload_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ErrorKind::UnknownUploadSession.into())
            }
            result => result.context(
                ErrorKind::BackendFailure,
                "canceling local-fs resumable upload",
            ),
        }
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

impl LocalFsBackend {
    fn multipart_dir(&self, id: &ObjectId, upload_id: &UploadId) -> PathBuf {
        self.path
            .join("__multipart__")
            .join(id.as_storage_path().to_string())
            .join(upload_id.as_str())
    }
}

#[async_trait::async_trait]
impl MultipartUploadBackend for LocalFsBackend {
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        let upload_id = UploadId::new(Uuid::now_v7().to_string())?;
        let dir = self.multipart_dir(id, &upload_id);
        tokio::fs::create_dir_all(&dir).await.context(
            ErrorKind::BackendFailure,
            "creating local-fs multipart upload",
        )?;

        let meta_path = dir.join("metadata.json");
        let metadata_json = serde_json::to_string(metadata)
            .context(ErrorKind::Internal, "encoding local-fs multipart metadata")?;
        tokio::fs::write(meta_path, metadata_json).await.context(
            ErrorKind::BackendFailure,
            "writing local-fs multipart metadata",
        )?;

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
        let dir = self.multipart_dir(id, upload_id);
        if !tokio::fs::try_exists(&dir).await.context(
            ErrorKind::BackendFailure,
            "checking local-fs multipart upload",
        )? {
            return Err(Error::new(
                ErrorKind::BackendFailure,
                "local-fs multipart upload not found",
            ));
        }

        let etag = format!("\"etag-{part_number}-{content_length}\"");

        let header = serde_json::json!({
            "etag": etag,
            "uploaded_at": SystemTime::now(),
            "size": content_length,
        });
        let header_line = serde_json::to_string(&header)
            .context(ErrorKind::Internal, "encoding local-fs part header")?;

        let part_path = dir.join(format!("{part_number}.part"));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(part_path)
            .await
            .context(
                ErrorKind::BackendFailure,
                "opening local-fs multipart part for writing",
            )?;

        let mut reader = pin!(StreamReader::new(body));
        let mut writer = BufWriter::new(file);
        writer
            .write_all(header_line.as_bytes())
            .await
            .context(ErrorKind::BackendFailure, "writing local-fs part header")?;
        writer
            .write_all(b"\n")
            .await
            .context(ErrorKind::BackendFailure, "writing local-fs part header")?;

        let _bytes_copied = tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::from(ce),
                None => Error::with_context(
                    ErrorKind::BackendFailure,
                    "writing local-fs multipart part payload",
                    e,
                ),
            })?;

        // TODO: validate bytes_copied against content_length and return a BadRequest-style
        // error. Needs a service-layer error variant that maps to HTTP 400 without abusing
        // ClientError (which is meant for stream errors).

        writer.flush().await.context(
            ErrorKind::BackendFailure,
            "flushing local-fs multipart part",
        )?;
        let file = writer.into_inner();
        file.sync_data()
            .await
            .context(ErrorKind::BackendFailure, "syncing local-fs multipart part")?;
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
        let dir = self.multipart_dir(id, upload_id);
        if !tokio::fs::try_exists(&dir).await.context(
            ErrorKind::BackendFailure,
            "checking local-fs multipart upload",
        )? {
            return Err(Error::new(
                ErrorKind::BackendFailure,
                "local-fs multipart upload not found",
            ));
        }

        let mut entries = tokio::fs::read_dir(&dir).await.context(
            ErrorKind::BackendFailure,
            "listing local-fs multipart parts",
        )?;
        let mut parts = Vec::new();

        while let Some(entry) = entries.next_entry().await.context(
            ErrorKind::BackendFailure,
            "listing local-fs multipart parts",
        )? {
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

            let file = tokio::fs::File::open(entry.path())
                .await
                .context(ErrorKind::BackendFailure, "opening local-fs multipart part")?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .await
                .context(ErrorKind::BackendFailure, "reading local-fs part header")?;
            let header: serde_json::Value = serde_json::from_str(header_line.trim_end())
                .context(ErrorKind::CorruptData, "decoding local-fs part header")?;

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
        let dir = self.multipart_dir(id, upload_id);
        if tokio::fs::try_exists(&dir).await.context(
            ErrorKind::BackendFailure,
            "checking local-fs multipart upload",
        )? {
            tokio::fs::remove_dir_all(dir).await.context(
                ErrorKind::BackendFailure,
                "removing local-fs multipart upload",
            )?;
        }
        Ok(())
    }

    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
        _access_time: Timestamp,
    ) -> Result<CompleteMultipartResponse> {
        let dir = self.multipart_dir(id, upload_id);
        if !tokio::fs::try_exists(&dir).await.context(
            ErrorKind::BackendFailure,
            "checking local-fs multipart upload",
        )? {
            return Err(Error::new(
                ErrorKind::BackendFailure,
                "local-fs multipart upload not found",
            ));
        }

        // Read metadata
        let meta_path = dir.join("metadata.json");
        let meta_bytes = tokio::fs::read(&meta_path).await.context(
            ErrorKind::BackendFailure,
            "reading local-fs multipart metadata",
        )?;
        let metadata: Metadata = serde_json::from_slice(&meta_bytes).context(
            ErrorKind::CorruptData,
            "decoding local-fs multipart metadata",
        )?;

        // TODO: validate that parts are in ascending part_number order and reject with
        // InvalidPartOrder if not (matches S3/GCS behavior). Needs a proper client error variant.

        // Validate all parts (headers only) before writing anything
        for completed in &parts {
            let part_path = dir.join(format!("{}.part", completed.part_number));
            if !tokio::fs::try_exists(&part_path).await.context(
                ErrorKind::BackendFailure,
                "checking local-fs multipart part",
            )? {
                return Ok(Some(crate::multipart::CompleteMultipartError {
                    code: "InvalidPart".into(),
                    message: format!("part number {} was not uploaded", completed.part_number),
                }));
            }

            let file = tokio::fs::File::open(&part_path)
                .await
                .context(ErrorKind::BackendFailure, "opening local-fs multipart part")?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .await
                .context(ErrorKind::BackendFailure, "reading local-fs part header")?;
            let header: serde_json::Value = serde_json::from_str(header_line.trim_end())
                .context(ErrorKind::CorruptData, "decoding local-fs part header")?;

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

        // Assemble the parts into a draft before publishing the object.
        let path = self.path(id);
        Self::create_dir_all(&path).await?;
        let mut draft = Draft::create(&path, &metadata).await?;

        let mut payload_size = 0;
        for completed in &parts {
            let part_path = dir.join(format!("{}.part", completed.part_number));
            let file = tokio::fs::File::open(&part_path)
                .await
                .context(ErrorKind::BackendFailure, "opening local-fs multipart part")?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .await
                .context(ErrorKind::BackendFailure, "reading local-fs part header")?;
            payload_size += tokio::io::copy(&mut reader, draft.writer()).await.context(
                ErrorKind::BackendFailure,
                "assembling local-fs object payload",
            )?;
        }

        let stored_size = draft.preamble_len() + payload_size;

        draft.prepare().await?;
        let guard = self.locks.acquire(id).await?;
        draft.publish().await?;
        drop(guard);

        self.change_stream
            .write(id, stored_size, metadata.time_expires);

        // Clean up multipart state
        tokio::fs::remove_dir_all(dir).await.context(
            ErrorKind::BackendFailure,
            "removing local-fs multipart upload",
        )?;

        Ok(None)
    }
}

// Must be lower than the tokio runtime `max_blocking_threads` setting.
const MAX_BLOCKING_LOCK_WAITERS: usize = 256;

#[derive(Debug)]
struct ObjectLocks {
    root: PathBuf,
    blocking_waiters: Arc<Semaphore>,
}

impl ObjectLocks {
    pub fn new(storage_root: &Path) -> Self {
        Self {
            root: storage_root.join(".locks"),
            blocking_waiters: Arc::new(Semaphore::new(MAX_BLOCKING_LOCK_WAITERS)),
        }
    }

    fn object_lock_path(&self, id: &ObjectId) -> PathBuf {
        let hash = blake3::hash(id.as_storage_path().to_string().as_bytes());
        let bytes = hash.as_bytes();
        self.root
            .join(format!("{:02x}", bytes[0]))
            .join(format!("{:02x}", bytes[1]))
    }

    /// Acquires an object's slot lock until the returned guard is dropped.
    async fn acquire(&self, id: &ObjectId) -> Result<File> {
        let path = self.object_lock_path(id);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .context(
                ErrorKind::BackendFailure,
                "creating local-fs object lock directory",
            )?;

        // Leave blocking-pool capacity available for the current lock holder's filesystem work.
        let permit = Arc::clone(&self.blocking_waiters)
            .acquire_owned()
            .await
            .expect("local-fs lock semaphore is never closed");

        tokio::task::spawn_blocking(move || -> io::Result<File> {
            let _permit = permit;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            file.lock()?;
            Ok(file)
        })
        .await
        .context(ErrorKind::Internal, "waiting for local-fs object lock")?
        .context(ErrorKind::BackendFailure, "acquiring local-fs object lock")
    }
}

#[derive(Debug)]
struct UploadSession {
    total_length: NonZeroU64,
    upload_id: Uuid,
}

impl UploadSession {
    fn from_token(token: &BackendToken) -> Result<Self> {
        let (length, upload_id) = token
            .split_once('.')
            .ok_or(ErrorKind::UnknownUploadSession)?;
        let total_length = length
            .parse::<NonZeroU64>()
            .map_err(|_| ErrorKind::UnknownUploadSession)?;
        let upload_id_str = upload_id;
        let upload_id =
            Uuid::parse_str(upload_id_str).map_err(|_| ErrorKind::UnknownUploadSession)?;
        if upload_id.get_version_num() != 7 || upload_id.to_string() != upload_id_str {
            return Err(ErrorKind::UnknownUploadSession.into());
        }
        Ok(Self {
            total_length,
            upload_id,
        })
    }
}

/// An open resumable upload containing a metadata preamble followed by payload bytes.
struct UploadFile {
    file: tokio::fs::File,
    path: PathBuf,
    stored_size: u64,
    expires_at: Option<Timestamp>,
    /// Number of payload bytes stored after the metadata preamble.
    payload_size: u64,
}

impl UploadFile {
    async fn create(path: &Path, metadata: &Metadata) -> Result<()> {
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        options.mode(0o666);

        let mut file = options.open(path).await.context(
            ErrorKind::BackendFailure,
            "creating local-fs resumable upload",
        )?;
        write_metadata_preamble(&mut file, metadata).await?;
        file.sync_data().await.context(
            ErrorKind::BackendFailure,
            "syncing local-fs resumable upload",
        )
    }

    async fn open(path: &Path) -> Result<Self> {
        let file = match OpenOptions::new().read(true).write(true).open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ErrorKind::UnknownUploadSession.into());
            }
            result => result.context(
                ErrorKind::BackendFailure,
                "opening local-fs resumable upload",
            )?,
        };
        let mut reader = BufReader::new(file);
        let (metadata, preamble_len) = read_metadata_preamble(&mut reader).await?;
        let file = reader.into_inner();
        let stored_size = file
            .metadata()
            .await
            .context(
                ErrorKind::BackendFailure,
                "reading local-fs resumable upload size",
            )?
            .len();
        let preamble_len = preamble_len as u64;
        let payload_size = stored_size.checked_sub(preamble_len).ok_or_else(|| {
            Error::new(
                ErrorKind::CorruptData,
                "reading truncated local-fs resumable upload",
            )
        })?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            stored_size,
            expires_at: metadata.time_expires,
            payload_size,
        })
    }

    fn offset(&self) -> u64 {
        self.payload_size
    }

    async fn append(&mut self, stream: ClientStream, content_length: u64) -> Result<u64> {
        self.file.seek(std::io::SeekFrom::End(0)).await.context(
            ErrorKind::BackendFailure,
            "seeking local-fs resumable upload",
        )?;

        let mut reader = pin!(StreamReader::new(stream));
        let mut contents = reader.as_mut().take(content_length);
        let copied = match tokio::io::copy(&mut contents, &mut self.file).await {
            Ok(copied) => copied,
            Err(error) => {
                let client_error = stream::unpack_client_error(&error);
                self.file.sync_data().await.context(
                    ErrorKind::BackendFailure,
                    "syncing partial local-fs resumable chunk",
                )?;
                return Err(match client_error {
                    Some(client_error) => Error::from(client_error),
                    None => Error::with_context(
                        ErrorKind::BackendFailure,
                        "writing local-fs resumable chunk",
                        error,
                    ),
                });
            }
        };

        let payload_size = self.payload_size.checked_add(copied).ok_or_else(|| {
            Error::new(
                ErrorKind::BackendFailure,
                "local-fs resumable upload size overflow",
            )
        })?;
        let stored_size = self.stored_size.checked_add(copied).ok_or_else(|| {
            Error::new(
                ErrorKind::BackendFailure,
                "local-fs resumable upload size overflow",
            )
        })?;
        self.file.sync_data().await.context(
            ErrorKind::BackendFailure,
            "syncing local-fs resumable chunk",
        )?;
        self.payload_size = payload_size;
        self.stored_size = stored_size;
        Ok(payload_size)
    }

    async fn publish(self, target: PathBuf) -> Result<(u64, Option<Timestamp>)> {
        let Self {
            file,
            path,
            stored_size,
            expires_at,
            ..
        } = self;
        drop(file);
        tokio::fs::rename(path, target).await.context(
            ErrorKind::BackendFailure,
            "publishing local-fs resumable upload",
        )?;
        Ok((stored_size, expires_at))
    }
}

struct ObjectFile {
    metadata: Metadata,
    preamble_len: u64,
    payload_size: u64,
    reader: BufReader<tokio::fs::File>,
}

async fn write_metadata_preamble<W>(writer: &mut W, metadata: &Metadata) -> Result<u64>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let metadata_json = serde_json::to_string(metadata)
        .context(ErrorKind::Internal, "encoding local-fs object metadata")?;
    writer.write_all(metadata_json.as_bytes()).await.context(
        ErrorKind::BackendFailure,
        "writing local-fs object metadata",
    )?;
    writer.write_all(b"\n").await.context(
        ErrorKind::BackendFailure,
        "writing local-fs object metadata",
    )?;

    Ok(metadata_json.len() as u64 + 1)
}

async fn read_metadata_preamble<R>(reader: &mut R) -> Result<(Metadata, usize)>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut metadata_line = String::new();
    let preamble_len = reader.read_line(&mut metadata_line).await.context(
        ErrorKind::BackendFailure,
        "reading local-fs object metadata",
    )?;
    let metadata = serde_json::from_str(metadata_line.trim_end())
        .context(ErrorKind::CorruptData, "decoding local-fs object metadata")?;
    Ok((metadata, preamble_len))
}

impl ObjectFile {
    /// Opens an object file, returning `None` when it does not exist.
    async fn try_open(path: &Path, access_time: Timestamp) -> Result<Option<Self>> {
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            result => result.context(ErrorKind::BackendFailure, "opening local-fs object")?,
        };

        let mut reader = BufReader::new(file);
        let (mut metadata, preamble_len) = read_metadata_preamble(&mut reader).await?;

        if metadata.is_expired(access_time) {
            objectstore_log::debug!("Object found but past expiry");
            return Ok(None);
        }

        let preamble_len = preamble_len as u64;
        let file_len = reader
            .get_ref()
            .metadata()
            .await
            .context(ErrorKind::BackendFailure, "reading local-fs object size")?
            .len();

        let payload_size = file_len.checked_sub(preamble_len).ok_or_else(|| {
            Error::new(ErrorKind::CorruptData, "reading truncated local-fs object")
        })?;

        metadata.size = Some(payload_size as usize);

        Ok(Some(Self {
            metadata,
            preamble_len,
            payload_size,
            reader,
        }))
    }
}

struct Draft {
    writer: BufWriter<tokio::fs::File>,
    path: tempfile::TempPath,
    target: PathBuf,
    /// Bytes the metadata preamble occupies, counted as it is written.
    preamble_len: u64,
}

impl Draft {
    async fn create(target: &Path, metadata: &Metadata) -> Result<Self> {
        let parent = target.parent().unwrap().to_path_buf();
        let tempfile = tokio::task::spawn_blocking(move || create_tempfile(&parent))
            .await
            .context(
                ErrorKind::Internal,
                "waiting for local-fs object draft creation",
            )?
            .context(ErrorKind::BackendFailure, "creating local-fs object draft")?;
        let (file, path) = tempfile.into_parts();

        let mut writer = BufWriter::new(tokio::fs::File::from_std(file));
        let preamble_len = write_metadata_preamble(&mut writer, metadata).await?;

        Ok(Self {
            writer,
            path,
            target: target.to_path_buf(),
            preamble_len,
        })
    }

    fn writer(&mut self) -> &mut BufWriter<tokio::fs::File> {
        &mut self.writer
    }

    /// Bytes the metadata preamble occupies, for sizing the stored object.
    fn preamble_len(&self) -> u64 {
        self.preamble_len
    }

    async fn prepare(&mut self) -> Result<()> {
        self.writer
            .flush()
            .await
            .context(ErrorKind::BackendFailure, "flushing local-fs object draft")?;
        self.writer
            .get_ref()
            .sync_data()
            .await
            .context(ErrorKind::BackendFailure, "syncing local-fs object draft")
    }

    async fn publish(self) -> Result<()> {
        let Self {
            writer,
            path,
            target,
            ..
        } = self;

        drop(writer);
        tokio::task::spawn_blocking(move || path.persist(target))
            .await
            .context(
                ErrorKind::Internal,
                "waiting to publish local-fs object draft",
            )?
            .context(
                ErrorKind::BackendFailure,
                "publishing local-fs object draft",
            )
    }
}

fn create_tempfile(parent: &Path) -> io::Result<tempfile::NamedTempFile> {
    let mut builder = tempfile::Builder::new();
    builder.suffix(".draft");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }

    builder.tempfile_in(parent)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use futures_util::{TryStreamExt, stream as futures_stream};
    use objectstore_types::metadata::{Compression, ExpirationPolicy};
    use objectstore_types::scope::{Scope, Scopes};
    use objectstore_types::time::Timestamp;

    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::OpType;
    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::test_utils::DummyProducer;

    use super::*;
    use crate::id::ObjectContext;
    use crate::stream;

    async fn upload_token(backend: &LocalFsBackend, id: &ObjectId, length: u64) -> BackendToken {
        backend
            .create_upload_session(id, &Metadata::default(), NonZeroU64::new(length).unwrap())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn resumable_upload() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/resumable".into(),
            ..Default::default()
        };
        let token = backend
            .create_upload_session(&id, &metadata, NonZeroU64::new(6).unwrap())
            .await
            .unwrap()
            .unwrap();
        let session = UploadSession::from_token(&token).unwrap();
        assert_eq!(session.total_length.get(), 6);
        assert_eq!(session.upload_id.get_version_num(), 7);
        let upload_path = backend.upload_path(session.upload_id);
        assert_eq!(
            upload_path.parent().unwrap().file_name().unwrap(),
            "uploads"
        );
        assert!(!backend.path(&id).exists());

        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap(),
            UploadProgress::Incomplete { offset: 0 }
        );
        assert_eq!(
            backend
                .put_chunk(&id, &token, 2, 0, stream::single(""))
                .await
                .unwrap(),
            UploadProgress::Incomplete { offset: 0 }
        );
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, 3, stream::single("abc"))
                .await
                .unwrap(),
            UploadProgress::Incomplete { offset: 3 }
        );
        assert!(!backend.path(&id).exists());
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap(),
            UploadProgress::Incomplete { offset: 3 }
        );
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, 0, stream::single(""))
                .await
                .unwrap(),
            UploadProgress::Incomplete { offset: 3 }
        );
        let error = backend
            .put_chunk(&id, &token, 0, 3, stream::single("abc"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UploadOffsetMismatch { offset: 3 });
        assert_eq!(
            backend
                .put_chunk(&id, &token, 3, 3, stream::single("def"))
                .await
                .unwrap(),
            UploadProgress::Complete
        );
        assert!(!upload_path.exists());
        let (stored_metadata, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_metadata.content_type, metadata.content_type);
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"abcdef");
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap_err().kind(),
            ErrorKind::UnknownUploadSession
        );
    }

    #[tokio::test]
    async fn failed_resumable_chunk_preserves_partial_progress() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let token = upload_token(&backend, &id, 4).await;
        backend
            .put_chunk(&id, &token, 0, 2, stream::single("ab"))
            .await
            .unwrap();
        let broken = futures_stream::iter([
            Ok(Bytes::from_static(b"c")),
            Err(stream::ClientError::new(io::Error::other(
                "client disconnected",
            ))),
        ])
        .boxed();
        let error = backend
            .put_chunk(&id, &token, 2, 2, broken)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ClientStream);
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap(),
            UploadProgress::Incomplete { offset: 3 }
        );
        assert_eq!(
            backend
                .put_chunk(&id, &token, 3, 1, stream::single("d"))
                .await
                .unwrap(),
            UploadProgress::Complete
        );
        let (_, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"abcd");
    }

    #[tokio::test]
    async fn concurrent_uploads_for_same_object_do_not_deadlock() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let first = upload_token(&backend, &id, 3).await;
        let second = upload_token(&backend, &id, 3).await;
        let writes = async {
            tokio::join!(
                backend.put_chunk(&id, &first, 0, 3, stream::single("one")),
                backend.put_chunk(&id, &second, 0, 3, stream::single("two")),
            )
        };
        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(2), writes)
            .await
            .unwrap();
        assert_eq!(first_result.unwrap(), UploadProgress::Complete);
        assert_eq!(second_result.unwrap(), UploadProgress::Complete);
        let (_, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        let payload = stream::read_to_vec(payload).await.unwrap();
        assert!(payload == b"one" || payload == b"two");
    }

    #[tokio::test]
    async fn stores_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(
            FileSystemConfig {
                path: tempdir.path().to_path_buf(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        );

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_created: Some(Timestamp::now()),
            time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            filename: Some("hello.txt".into()),
            custom: [("foo".into(), "bar".into())].into(),
            size: None,
        };
        backend
            .put_object(&id, &metadata, stream::single("oh hai!"), Timestamp::now())
            .await
            .unwrap();

        let (read_metadata, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(
            read_metadata,
            Metadata {
                size: Some(file_contents.len()),
                ..metadata
            }
        );
        assert_eq!(file_contents.as_ref(), b"oh hai!");

        let lock_path = backend.locks.object_lock_path(&id);
        assert!(lock_path.exists());
        backend.delete_object(&id, Timestamp::now()).await.unwrap();
        assert!(lock_path.exists());
    }

    #[tokio::test]
    async fn object_locks_coordinate_by_slot() {
        let tempdir = tempfile::tempdir().unwrap();
        let first_locks = ObjectLocks::new(tempdir.path());
        let second_locks = ObjectLocks::new(tempdir.path());
        let mut slots = HashMap::new();
        let (first_id, colliding_id) = (0..=65_536)
            .find_map(|key| {
                let id = ObjectId::from_parts("testing".into(), Scopes::empty(), key.to_string());
                let path = first_locks.object_lock_path(&id);
                slots.insert(path, id.clone()).map(|first| (first, id))
            })
            .expect("65,537 keys must collide in 65,536 slots");
        let lock_path = first_locks.object_lock_path(&first_id);
        let other_id = slots
            .values()
            .find(|id| first_locks.object_lock_path(id) != lock_path)
            .unwrap();
        assert_eq!(second_locks.object_lock_path(&colliding_id), lock_path);

        let first_guard = first_locks.acquire(&first_id).await.unwrap();
        let mut waiter =
            tokio::spawn(async move { second_locks.acquire(&colliding_id).await.unwrap() });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );

        let other_guard =
            tokio::time::timeout(Duration::from_secs(1), first_locks.acquire(other_id))
                .await
                .unwrap()
                .unwrap();
        drop(other_guard);
        drop(first_guard);
        let guard = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        drop(guard);
        assert!(lock_path.exists());
    }

    #[tokio::test]
    async fn missing_object_expiry_does_not_block_descendant() {
        let (_tempdir, backend) = make_backend();
        let id = ObjectId::from_parts("testing".into(), Scopes::empty(), "foo".into());
        assert!(
            !backend
                .set_expiry(
                    &id,
                    Timestamp::now() + Duration::from_hours(1),
                    Timestamp::now()
                )
                .await
                .unwrap()
        );
        let descendant = ObjectId::new(id.context.clone(), "foo/bar".into());
        backend
            .put_object(
                &descendant,
                &Metadata::default(),
                stream::single("payload"),
                Timestamp::now(),
            )
            .await
            .unwrap();
        let (_, _, payload) = backend
            .get_object(&descendant, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"payload");
    }

    #[tokio::test]
    async fn failed_put_preserves_published_object() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let original_metadata = Metadata {
            content_type: "text/original".into(),
            ..Default::default()
        };
        backend
            .put_object(
                &id,
                &original_metadata,
                stream::single("original"),
                Timestamp::now(),
            )
            .await
            .unwrap();

        let replacement_metadata = Metadata {
            content_type: "text/replacement".into(),
            ..Default::default()
        };
        let replacement = futures_stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(stream::ClientError::new(io::Error::other(
                "replacement stream failed",
            ))),
        ])
        .boxed();
        let error = backend
            .put_object(&id, &replacement_metadata, replacement, Timestamp::now())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ClientStream);

        let (metadata, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.content_type, original_metadata.content_type);
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"original");

        assert_eq!(draft_count(&backend, &id), 0);
    }

    #[tokio::test]
    async fn cancelled_put_removes_draft() {
        let (_tempdir, backend) = make_backend();
        let backend = Arc::new(backend);
        let id = make_id();
        backend
            .put_object(
                &id,
                &Metadata::default(),
                stream::single("original"),
                Timestamp::now(),
            )
            .await
            .unwrap();

        let (writing, writing_started) = tokio::sync::oneshot::channel();
        let replacement = futures_stream::once(async move {
            let _ = writing.send(());
            Ok(Bytes::from_static(b"partial"))
        })
        .chain(futures_stream::pending())
        .boxed();

        let task = tokio::spawn({
            let backend = Arc::clone(&backend);
            let id = id.clone();
            async move {
                backend
                    .put_object(&id, &Metadata::default(), replacement, Timestamp::now())
                    .await
            }
        });

        writing_started.await.unwrap();
        assert_eq!(draft_count(&backend, &id), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let (_, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"original");

        assert_eq!(draft_count(&backend, &id), 0);
    }

    #[tokio::test]
    async fn set_expiry() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let old_expiry = Timestamp::now() + Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            time_expires: Some(old_expiry),
            custom: [("preserved".into(), "yes".into())].into(),
            ..Default::default()
        };
        backend
            .put_object(&id, &metadata, stream::single("payload"), Timestamp::now())
            .await
            .unwrap();

        let requested = old_expiry + Duration::from_hours(1) + Duration::from_nanos(999);
        assert!(
            backend
                .set_expiry(&id, requested, Timestamp::now())
                .await
                .unwrap()
        );
        let (updated, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.expiration_policy, metadata.expiration_policy);
        assert_eq!(updated.custom, metadata.custom);
        assert_eq!(updated.time_expires, Some(requested));
        assert_eq!(stream::read_to_vec(payload).await.unwrap(), b"payload");

        assert_eq!(draft_count(&backend, &id), 0);
    }

    #[tokio::test]
    async fn expired_object() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        backend
            .put_object(&id, &metadata, stream::single("expired"), Timestamp::now())
            .await
            .unwrap();

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !backend
                .set_expiry(
                    &id,
                    Timestamp::now() + Duration::from_hours(1),
                    Timestamp::now()
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_metadata_returns_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(
            FileSystemConfig {
                path: tempdir.path().to_path_buf(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        );

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
            .put_object(&id, &metadata, stream::single("oh hai!"), Timestamp::now())
            .await
            .unwrap();

        let read_metadata = backend
            .get_metadata(&id, Timestamp::now())
            .await
            .unwrap()
            .unwrap();
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
        let backend = LocalFsBackend::new(
            FileSystemConfig {
                path: tempdir.path().to_path_buf(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        );

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let result = backend.get_metadata(&id, Timestamp::now()).await.unwrap();
        assert!(result.is_none());
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[cfg(feature = "storage-cogs")]
    fn make_backend_with_change_stream() -> (tempfile::TempDir, LocalFsBackend, DummyProducer) {
        let tempdir = tempfile::tempdir().unwrap();
        let (streams, producer) = crate::change_stream::dummy_factory();
        let backend = LocalFsBackend::new(
            FileSystemConfig {
                path: tempdir.path().to_path_buf(),
                cogs: Some(CostTrackerStreamConfig {
                    shared_resource_id: "filesystem_objectstore".into(),
                    sample_rate: 1.0,
                }),
            },
            &streams,
        );
        (tempdir, backend, producer)
    }

    fn make_backend() -> (tempfile::TempDir, LocalFsBackend) {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(
            FileSystemConfig {
                path: tempdir.path().to_path_buf(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        );
        (tempdir, backend)
    }

    fn draft_count(backend: &LocalFsBackend, id: &ObjectId) -> usize {
        let object_path = backend.path(id);
        std::fs::read_dir(object_path.parent().unwrap())
            .unwrap()
            .flat_map(|entry| entry.map(|entry| entry.path()))
            .filter(|path| path.to_string_lossy().ends_with(".draft"))
            .count()
    }

    #[tokio::test]
    async fn multipart_single_part() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
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
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
                Timestamp::now(),
            )
            .await
            .unwrap();
        assert!(result.is_none(), "expected no error on complete");

        let (meta, _, body) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
        let payload: BytesMut = body.try_collect().await.unwrap();
        assert_eq!(payload.as_ref(), data);
        assert_eq!(meta.content_type, "text/plain".to_string());
        assert_eq!(
            meta.expiration_policy,
            ExpirationPolicy::TimeToIdle(Duration::from_hours(1))
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
                    CompletedPart {
                        part_number: NonZeroU32::new(1).unwrap(),
                        etag: etag1,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(2).unwrap(),
                        etag: etag2,
                    },
                    CompletedPart {
                        part_number: NonZeroU32::new(3).unwrap(),
                        etag: etag3,
                    },
                ],
                Timestamp::now(),
            )
            .await
            .unwrap();
        assert!(result.is_none());

        let (_, _, body) = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap()
            .unwrap();
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
    async fn get_object_range_bounded() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let payload = b"Hello, range requests!";
        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();

        // Request bytes 7-11 → "range"
        let (_, content_range, body) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Bounded(7, 11)))
            .await
            .unwrap()
            .unwrap();
        let data: BytesMut = body.try_collect().await.unwrap();

        assert_eq!(data.as_ref(), b"range");
        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 11);
        assert_eq!(content_range.total, payload.len() as u64);
    }

    #[tokio::test]
    async fn get_object_range_from() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let payload = b"Hello, range requests!";
        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();

        // Request bytes 7- → "range requests!"
        let (_, content_range, body) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::From(7)))
            .await
            .unwrap()
            .unwrap();
        let data: BytesMut = body.try_collect().await.unwrap();

        assert_eq!(data.as_ref(), b"range requests!");
        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 21);
        assert_eq!(content_range.total, payload.len() as u64);
    }

    #[tokio::test]
    async fn get_object_range_last() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        let payload = b"Hello, range requests!";
        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();

        // Request last 9 bytes → "requests!"
        let (_, content_range, body) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Last(9)))
            .await
            .unwrap()
            .unwrap();
        let data: BytesMut = body.try_collect().await.unwrap();

        assert_eq!(data.as_ref(), b"requests!");
        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 13);
        assert_eq!(content_range.end, 21);
        assert_eq!(content_range.total, payload.len() as u64);
    }

    #[tokio::test]
    async fn get_object_range_unsatisfiable() {
        let (_tempdir, backend) = make_backend();
        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(
                &id,
                &metadata,
                stream::single(b"short".to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();

        match backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::From(100)))
            .await
        {
            Err(error) if matches!(error.kind(), ErrorKind::RangeNotSatisfiable { total: 5 }) => {}
            Err(other) => panic!("expected RangeNotSatisfiable, got: {other:?}"),
            Ok(_) => panic!("expected RangeNotSatisfiable, got Ok"),
        }
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

        let result = backend
            .get_object(&id, Timestamp::now(), None)
            .await
            .unwrap();
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
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag: "wrong-etag".into(),
                }],
                Timestamp::now(),
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
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
                Timestamp::now(),
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
                vec![CompletedPart {
                    part_number: NonZeroU32::new(99).unwrap(),
                    etag: "whatever".into(),
                }],
                Timestamp::now(),
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
                vec![CompletedPart {
                    part_number: NonZeroU32::new(1).unwrap(),
                    etag,
                }],
                Timestamp::now(),
            )
            .await
            .unwrap();
        assert!(result.is_none(), "retry with correct part should succeed");
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_size_written_to_disk() {
        let (tempdir, backend, producer) = make_backend_with_change_stream();
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
            ..Default::default()
        };
        let payload = b"oh hai!";

        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();

        let file = tokio::fs::read(tempdir.path().join(id.as_storage_path().to_string()))
            .await
            .unwrap();

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].shared_resource_id, "filesystem_objectstore");
        assert_eq!(records[0].app_feature, "testing");
        assert_eq!(
            records[0].size,
            Some(file.len() as u64),
            "the metadata header line counts towards the reported size"
        );
        assert!(records[0].expiration_time.is_some());
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_resumable_upload_size() {
        let (tempdir, backend, producer) = make_backend_with_change_stream();
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
            ..Default::default()
        };
        let payload = b"oh hai!";
        let token = backend
            .create_upload_session(
                &id,
                &metadata,
                NonZeroU64::new(payload.len() as u64).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(producer.records().is_empty());
        assert_eq!(
            backend
                .put_chunk(
                    &id,
                    &token,
                    0,
                    payload.len() as u64,
                    stream::single(payload.to_vec()),
                )
                .await
                .unwrap(),
            UploadProgress::Complete
        );

        let file = tokio::fs::read(tempdir.path().join(id.as_storage_path().to_string()))
            .await
            .unwrap();
        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].size, Some(file.len() as u64));
        assert!(records[0].expiration_time.is_some());
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_assembled_multipart_size() {
        let (tempdir, backend, producer) = make_backend_with_change_stream();
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let mut completed = Vec::new();
        for (number, payload) in [(1u32, b"aaaa".to_vec()), (2, b"bbbb".to_vec())] {
            let part_number = NonZeroU32::new(number).unwrap();
            let etag = backend
                .upload_part(
                    &id,
                    &upload_id,
                    part_number,
                    payload.len() as u64,
                    None,
                    stream::single(payload),
                )
                .await
                .unwrap();
            completed.push(CompletedPart { part_number, etag });
        }

        // Uploading parts is intermediate state, so nothing is reported until completion.
        assert!(producer.records().is_empty());

        assert!(
            backend
                .complete_multipart(&id, &upload_id, completed, Timestamp::now())
                .await
                .unwrap()
                .is_none()
        );

        let file = tokio::fs::read(tempdir.path().join(id.as_storage_path().to_string()))
            .await
            .unwrap();

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(
            records[0].size,
            Some(file.len() as u64),
            "the assembled object counts its metadata header and every part payload"
        );
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_deletes_on_success() {
        let (_tempdir, backend, producer) = make_backend_with_change_stream();
        let id = make_id();

        // Try to delete a non-existent object. Don't emit a message.
        backend
            .delete_object(&id, Timestamp::now())
            .await
            .expect("deleting a non-existent object returns Ok(())");
        assert!(producer.records().is_empty());

        backend
            .put_object(
                &id,
                &Metadata::default(),
                stream::single(b"hi".to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();
        producer.clear();

        backend.delete_object(&id, Timestamp::now()).await.unwrap();

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Delete);
    }
}
