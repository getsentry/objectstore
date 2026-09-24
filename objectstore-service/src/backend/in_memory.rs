//! In-memory backend for tests.
//!
//! This provides a [`Backend`](super::common::Backend) backed by a `HashMap`,
//! removing the need for filesystem tempdir management in unit tests. The
//! backend is [`Clone`] so tests can hold a handle for direct inspection while
//! the service owns a boxed copy.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use objectstore_types::range::ByteRange;
use objectstore_types::time::Timestamp;

use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use objectstore_types::metadata::Metadata;

use crate::backend::common::{
    self, DeleteResponse, ExpiryUpdate, GetResponse, HighVolumeBackend, MultipartUploadBackend,
    PutResponse, SetExpiryResponse, TieredGet, TieredMetadata, TieredUpdate, TieredWrite,
    Tombstone,
};
use crate::change_stream::{ChangeStream, NoopStream, flush_change_stream};
use crate::error::{Error, ErrorKind, Result};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, Part, PartNumber, UploadId, UploadPartResponse,
};
use crate::resumable::{BackendToken, UploadProgress};
use crate::stream::ClientStream;

/// An entry in the in-memory store.
#[derive(Clone, Debug)]
enum StoreEntry {
    Object(Metadata, Bytes),
    Tombstone(Tombstone),
}

impl StoreEntry {
    fn is_expired(&self, now: Timestamp) -> bool {
        match self {
            StoreEntry::Object(metadata, _) => metadata.is_expired(now),
            StoreEntry::Tombstone(tombstone) => tombstone.is_expired(now),
        }
    }

    /// Number of bytes an entry occupies (payload plus serialized metadata)
    pub(crate) fn stored_size(&self) -> usize {
        match self {
            StoreEntry::Object(metadata, payload) => json_len(metadata) + payload.len(),
            // A tombstone carries no payload: its bytes are the redirect target plus the
            // deadline stored alongside it.
            StoreEntry::Tombstone(tombstone) => {
                tombstone.target.as_storage_path().to_string().len()
                    + tombstone.time_expires.map_or(0, |d| json_len(&d))
            }
        }
    }
}

type Store = HashMap<ObjectId, StoreEntry>;

#[derive(Clone, Debug)]
struct MultipartUpload {
    metadata: Metadata,
    parts: BTreeMap<PartNumber, UploadedPart>,
}

#[derive(Clone, Debug)]
struct UploadedPart {
    etag: String,
    data: Bytes,
    uploaded_at: SystemTime,
}

#[derive(Debug)]
struct ResumableUpload {
    metadata: Metadata,
    total_length: NonZeroU64,
    data: BytesMut,
}

// None marks a removed session for operations already waiting on its mutex.
type ResumableSession = Arc<tokio::sync::Mutex<Option<ResumableUpload>>>;
type ResumableStore = HashMap<(ObjectId, BackendToken), ResumableSession>;

type MultipartStore = HashMap<(ObjectId, UploadId), MultipartUpload>;

/// In-memory [`Backend`](super::common::Backend) backed by a `HashMap`.
///
/// Removes the need for filesystem tempdir management in unit tests. The
/// backend is [`Clone`] so tests can hold a handle for direct inspection while
/// the service owns a boxed copy.
#[derive(Debug, Clone)]
pub struct InMemoryBackend {
    name: &'static str,
    store: Arc<Mutex<Store>>,
    multipart_store: Arc<Mutex<MultipartStore>>,
    resumable_store: Arc<Mutex<ResumableStore>>,
    change_stream: Arc<dyn ChangeStream>,
}

impl InMemoryBackend {
    /// Creates a new `InMemoryBackend` with the given diagnostic `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            store: Arc::new(Mutex::new(HashMap::new())),
            multipart_store: Arc::new(Mutex::new(HashMap::new())),
            resumable_store: Arc::new(Mutex::new(HashMap::new())),
            change_stream: Arc::new(NoopStream),
        }
    }

    fn upload_session(&self, id: &ObjectId, token: &BackendToken) -> Result<ResumableSession> {
        self.resumable_store
            .lock()
            .unwrap()
            .get(&(id.clone(), token.clone()))
            // Clone the session handle so that the `resumable_store` mutex is released immediately.
            .cloned()
            .ok_or_else(|| ErrorKind::UnknownUploadSession.into())
    }

    /// Publishes this backend's changes to `change_stream`.
    pub fn with_change_stream(mut self, change_stream: Arc<dyn ChangeStream>) -> Self {
        self.change_stream = change_stream;
        self
    }

    /// Returns the stored entry for `id`, for direct inspection in tests.
    pub fn get(&self, id: &ObjectId) -> Entry {
        match self.store.lock().unwrap().get(id).cloned() {
            None => Entry::NotFound,
            Some(StoreEntry::Tombstone(tombstone)) => Entry::Tombstone(tombstone),
            Some(StoreEntry::Object(metadata, bytes)) => Entry::Object(metadata, bytes),
        }
    }

    /// Returns `true` if the backend contains an entry for the given id.
    pub fn contains(&self, id: &ObjectId) -> bool {
        self.store.lock().unwrap().contains_key(id)
    }

    /// Returns `true` if the backend has no stored objects.
    pub fn is_empty(&self) -> bool {
        self.store.lock().unwrap().is_empty()
    }

    /// Removes an entry directly, bypassing the `Backend` trait.
    ///
    /// Useful for simulating partial failures (e.g. orphan tombstones).
    pub fn remove(&self, id: &ObjectId) {
        self.store.lock().unwrap().remove(id);
    }
}

#[async_trait::async_trait]
impl super::common::Backend for InMemoryBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        Ok(self)
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        let bytes: BytesMut = stream.try_collect().await?;
        let entry = StoreEntry::Object(metadata.clone(), bytes.freeze());
        let size = entry.stored_size();
        self.store.lock().unwrap().insert(id.clone(), entry);
        self.change_stream
            .write(id, size as u64, metadata.time_expires);
        Ok(())
    }

    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        match entry {
            None => Ok(None),
            Some(entry) if entry.is_expired(access_time) => Ok(None),
            Some(StoreEntry::Tombstone(_)) => Err(ErrorKind::UnexpectedTombstone.into()),
            Some(StoreEntry::Object(mut metadata, bytes)) => {
                let total = bytes.len() as u64;
                metadata.size = Some(bytes.len());
                let (content_range, payload) = match range {
                    Some(range) => {
                        let content_range = range
                            .resolve(total)
                            .ok_or(ErrorKind::RangeNotSatisfiable { total })?;
                        let sliced =
                            bytes.slice(content_range.start as usize..=content_range.end as usize);
                        (Some(content_range), sliced)
                    }
                    None => (None, bytes),
                };
                Ok(Some((
                    metadata,
                    content_range,
                    crate::stream::single(payload),
                )))
            }
        }
    }

    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let outcome = {
            let mut store = self.store.lock().unwrap();
            match store.get_mut(id) {
                None => ExpiryOutcome::NotFound,
                Some(entry) if entry.is_expired(access_time) => ExpiryOutcome::NotFound,
                Some(StoreEntry::Object(metadata, _)) => {
                    extend_object_expiry(metadata, target, access_time)?
                }
                _ => ExpiryOutcome::Rejected,
            }
        };

        if let ExpiryOutcome::Extended(expire_at) = outcome {
            self.change_stream.update(id, Some(expire_at));
        }

        Ok(outcome.response())
    }

    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
        if self.store.lock().unwrap().remove(id).is_some() {
            self.change_stream.delete(id);
        }
        Ok(())
    }

    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        total_length: NonZeroU64,
    ) -> Result<Option<BackendToken>> {
        let token = uuid::Uuid::now_v7().to_string();
        let upload = ResumableUpload {
            metadata: metadata.clone(),
            total_length,
            data: BytesMut::new(),
        };
        self.resumable_store.lock().unwrap().insert(
            (id.clone(), token.clone()),
            Arc::new(tokio::sync::Mutex::new(Some(upload))),
        );
        Ok(Some(token))
    }

    async fn put_chunk(
        &self,
        id: &ObjectId,
        token: &BackendToken,
        offset: u64,
        content_length: u64,
        mut stream: ClientStream,
    ) -> Result<UploadProgress> {
        let session = self.upload_session(id, token)?;
        let mut guard = session.lock().await;
        let upload = guard.as_mut().ok_or(ErrorKind::UnknownUploadSession)?;
        offset
            .checked_add(content_length)
            .filter(|end| *end <= upload.total_length.get())
            .ok_or(ErrorKind::ChunkExceedsUploadLength {
                offset,
                content_length,
                upload_length: upload.total_length.get(),
            })?;
        if content_length != 0 && offset != upload.data.len() as u64 {
            return Err(ErrorKind::UploadOffsetMismatch {
                offset: upload.data.len() as u64,
            }
            .into());
        }

        let mut remaining = content_length;
        while remaining > 0 {
            let Some(chunk) = stream.try_next().await? else {
                break;
            };
            let count = remaining.min(chunk.len() as u64) as usize;
            upload.data.extend_from_slice(&chunk[..count]);
            remaining -= count as u64;
        }

        let offset = upload.data.len() as u64;
        if offset != upload.total_length.get() {
            return Ok(UploadProgress::Incomplete { offset });
        }

        let upload = guard.take().unwrap();
        let metadata = upload.metadata;
        let expires_at = metadata.time_expires;
        let entry = StoreEntry::Object(metadata, upload.data.freeze());
        let size = entry.stored_size();
        self.store.lock().unwrap().insert(id.clone(), entry);

        self.change_stream.write(id, size as u64, expires_at);
        self.resumable_store
            .lock()
            .unwrap()
            .remove(&(id.clone(), token.clone()));

        Ok(UploadProgress::Complete)
    }

    async fn upload_offset(&self, id: &ObjectId, token: &BackendToken) -> Result<UploadProgress> {
        let session = self.upload_session(id, token)?;
        let guard = session.lock().await;
        let upload = guard.as_ref().ok_or(ErrorKind::UnknownUploadSession)?;
        Ok(UploadProgress::Incomplete {
            offset: upload.data.len() as u64,
        })
    }

    async fn cancel_upload(&self, id: &ObjectId, token: &BackendToken) -> Result<()> {
        let session = self.upload_session(id, token)?;
        let mut guard = session.lock().await;
        guard.take().ok_or(ErrorKind::UnknownUploadSession)?;
        self.resumable_store
            .lock()
            .unwrap()
            .remove(&(id.clone(), token.clone()));
        Ok(())
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

#[async_trait::async_trait]
impl HighVolumeBackend for InMemoryBackend {
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        let mut store = self.store.lock().unwrap();
        if let Some(StoreEntry::Tombstone(tombstone)) = store.get(id)
            && !tombstone.is_expired(access_time)
        {
            return Ok(Some(tombstone.clone()));
        }

        let mut metadata = metadata.clone();
        metadata.size = Some(payload.len());
        let expires_at = metadata.time_expires;
        let entry = StoreEntry::Object(metadata, payload);
        let size = entry.stored_size();
        store.insert(id.clone(), entry);
        self.change_stream.write(id, size as u64, expires_at);
        Ok(None)
    }

    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<TieredGet> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => TieredGet::NotFound,
            Some(entry) if entry.is_expired(access_time) => TieredGet::NotFound,
            Some(StoreEntry::Tombstone(tombstone)) => TieredGet::Tombstone(tombstone),
            Some(StoreEntry::Object(mut metadata, bytes)) => {
                let total = bytes.len() as u64;
                metadata.size = Some(bytes.len());
                let (content_range, payload) = match range {
                    Some(range) => {
                        let content_range = range
                            .resolve(total)
                            .ok_or(ErrorKind::RangeNotSatisfiable { total })?;
                        let sliced =
                            bytes.slice(content_range.start as usize..=content_range.end as usize);
                        (Some(content_range), sliced)
                    }
                    None => (None, bytes),
                };
                TieredGet::Object(metadata, content_range, crate::stream::single(payload))
            }
        })
    }

    async fn get_tiered_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<TieredMetadata> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => TieredMetadata::NotFound,
            Some(entry) if entry.is_expired(access_time) => TieredMetadata::NotFound,
            Some(StoreEntry::Tombstone(tombstone)) => TieredMetadata::Tombstone(tombstone),
            Some(StoreEntry::Object(metadata, _bytes)) => TieredMetadata::Object(metadata),
        })
    }

    async fn delete_non_tombstone(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        let mut store = self.store.lock().unwrap();
        if let Some(StoreEntry::Tombstone(tombstone)) = store.get(id).cloned()
            && !tombstone.is_expired(access_time)
        {
            return Ok(Some(tombstone));
        }

        if store.remove(id).is_some() {
            self.change_stream.delete(id);
        }
        Ok(None)
    }

    async fn compare_and_update(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        update: TieredUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let TieredUpdate::SetExpiry(expiry_target) = update;
        let outcome = {
            let mut store = self.store.lock().unwrap();
            match (store.get_mut(id), current) {
                (None, _) => ExpiryOutcome::NotFound,
                (Some(entry), _) if entry.is_expired(access_time) => ExpiryOutcome::NotFound,
                (Some(StoreEntry::Object(metadata, _)), None) => {
                    extend_object_expiry(metadata, expiry_target, access_time)?
                }
                (Some(StoreEntry::Tombstone(t)), Some(target)) if t.target == *target => {
                    extend_expiry(&mut t.time_expires, expiry_target, None, access_time)?
                }
                _ => ExpiryOutcome::Rejected,
            }
        };

        if let ExpiryOutcome::Extended(expire_at) = outcome {
            self.change_stream.update(id, Some(expire_at));
        }

        Ok(outcome.response())
    }

    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
        access_time: Timestamp,
    ) -> Result<bool> {
        let mut store = self.store.lock().unwrap();

        let actual = store.get(id);
        let matches_current = matches_redirect(actual, current, access_time);
        let matches_next = matches_redirect(actual, write.target(), access_time);

        if matches_current {
            match write {
                TieredWrite::Tombstone(tombstone) => {
                    let expires_at = tombstone.time_expires;
                    let entry = StoreEntry::Tombstone(tombstone);
                    let size = entry.stored_size();
                    store.insert(id.clone(), entry);
                    self.change_stream.write(id, size as u64, expires_at);
                }
                TieredWrite::Object(metadata, payload) => {
                    let expires_at = metadata.time_expires;
                    let entry = StoreEntry::Object(metadata, payload);
                    let size = entry.stored_size();
                    store.insert(id.clone(), entry);
                    self.change_stream.write(id, size as u64, expires_at);
                }
                TieredWrite::Delete => {
                    if store.remove(id).is_some() {
                        self.change_stream.delete(id);
                    }
                }
            }
        }

        Ok(matches_current || matches_next)
    }
}

#[async_trait::async_trait]
impl MultipartUploadBackend for InMemoryBackend {
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        let upload_id = UploadId::new(uuid::Uuid::now_v7().to_string())?;
        let upload = MultipartUpload {
            metadata: metadata.clone(),
            parts: BTreeMap::new(),
        };
        self.multipart_store
            .lock()
            .unwrap()
            .insert((id.clone(), upload_id.clone()), upload);
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        part_number: PartNumber,
        _content_length: u64,
        _content_md5: Option<&str>,
        body: ClientStream,
    ) -> Result<UploadPartResponse> {
        let data: BytesMut = body.try_collect().await?;
        let data = data.freeze();
        let etag = format!("\"etag-{part_number}-{}\"", data.len());

        let mut store = self.multipart_store.lock().unwrap();
        let upload = store
            .get_mut(&(id.clone(), upload_id.clone()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::BackendFailure,
                    "in-memory multipart upload not found",
                )
            })?;

        upload.parts.insert(
            part_number,
            UploadedPart {
                etag: etag.clone(),
                data,
                uploaded_at: SystemTime::now(),
            },
        );

        Ok(etag)
    }

    async fn list_parts(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse> {
        let store = self.multipart_store.lock().unwrap();
        let upload = store.get(&(id.clone(), upload_id.clone())).ok_or_else(|| {
            Error::new(
                ErrorKind::BackendFailure,
                "in-memory multipart upload not found",
            )
        })?;

        let iter = upload
            .parts
            .iter()
            .filter(|(pn, _)| part_number_marker.is_none_or(|marker| **pn > marker));

        let max = max_parts.unwrap_or(u32::MAX) as usize;
        let all: Vec<_> = iter.collect();
        let is_truncated = all.len() > max;
        let page: Vec<_> = all.into_iter().take(max).collect();

        let next_part_number_marker = if is_truncated {
            page.last().map(|(pn, _)| **pn)
        } else {
            None
        };

        let parts = page
            .into_iter()
            .map(|(pn, part)| Part {
                part_number: *pn,
                etag: part.etag.clone(),
                last_modified: part.uploaded_at,
                size: part.data.len() as u64,
            })
            .collect();

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
        self.multipart_store
            .lock()
            .unwrap()
            .remove(&(id.clone(), upload_id.clone()));
        Ok(())
    }

    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
        _access_time: Timestamp,
    ) -> Result<CompleteMultipartResponse> {
        let key = (id.clone(), upload_id.clone());

        // TODO: validate that parts are in ascending part_number order and reject with
        // InvalidPartOrder if not (matches S3/GCS behavior). Needs a proper client error variant.

        // Validate and assemble while holding the multipart lock, but don't
        // remove the upload yet — a failed validation must leave it intact so
        // the client can retry.
        let (metadata, payload) = {
            let store = self.multipart_store.lock().unwrap();
            let upload = store.get(&key).ok_or_else(|| {
                Error::new(
                    ErrorKind::BackendFailure,
                    "in-memory multipart upload not found",
                )
            })?;

            for completed in &parts {
                match upload.parts.get(&completed.part_number) {
                    None => {
                        return Ok(Some(crate::multipart::CompleteMultipartError {
                            code: "InvalidPart".into(),
                            message: format!(
                                "part number {} was not uploaded",
                                completed.part_number
                            ),
                        }));
                    }
                    Some(stored) if stored.etag != completed.etag => {
                        return Ok(Some(crate::multipart::CompleteMultipartError {
                            code: "InvalidPart".into(),
                            message: format!(
                                "etag mismatch for part {}: expected {}, got {}",
                                completed.part_number, stored.etag, completed.etag
                            ),
                        }));
                    }
                    _ => {}
                }
            }

            let mut payload = BytesMut::new();
            for completed in &parts {
                let stored = &upload.parts[&completed.part_number];
                payload.extend_from_slice(&stored.data);
            }

            let mut metadata = upload.metadata.clone();
            metadata.size = Some(payload.len());

            (metadata, payload.freeze())
        };

        let expires_at = metadata.time_expires;
        let entry = StoreEntry::Object(metadata, payload);
        let size = entry.stored_size();
        self.store.lock().unwrap().insert(id.clone(), entry);
        self.change_stream.write(id, size as u64, expires_at);

        self.multipart_store.lock().unwrap().remove(&key);

        Ok(None)
    }
}

/// Serialized length of `value`, or `0` if it cannot be serialized.
fn json_len<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_string(value).map_or(0, |json| json.len())
}

/// Returns `true` if `entry` matches the expected tombstone redirect state.
///
/// - `expected = None`: matches any non-tombstone (absent or inline object).
/// - `expected = Some(target)`: matches a tombstone whose redirect target equals `target`.
fn matches_redirect(
    entry: Option<&StoreEntry>,
    expected: Option<&ObjectId>,
    now: Timestamp,
) -> bool {
    match entry {
        None | Some(StoreEntry::Object(..)) => expected.is_none(),
        Some(StoreEntry::Tombstone(tombstone)) => match expected {
            None => tombstone.is_expired(now),
            Some(target) => tombstone.target == *target && !tombstone.is_expired(now),
        },
    }
}

/// What [`extend_expiry`] did to an entry's deadline.
///
/// Callers need to tell a deadline that actually moved apart from one that already
/// covered the request, because only the former is a change worth reporting to the
/// [`ChangeStream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpiryOutcome {
    /// The entry is absent or expired.
    NotFound,
    /// The entry was not eligible or the target could not be resolved.
    Rejected,
    /// The stored deadline already covered `expire_at`. Nothing was written.
    AlreadySatisfied(Timestamp),
    /// The stored deadline moved out to `expire_at`.
    Extended(Timestamp),
}

impl ExpiryOutcome {
    /// Converts the internal mutation outcome to the backend response.
    fn response(self) -> SetExpiryResponse {
        match self {
            Self::NotFound => SetExpiryResponse::NotFound,
            Self::Rejected => SetExpiryResponse::Rejected,
            Self::AlreadySatisfied(deadline) | Self::Extended(deadline) => {
                SetExpiryResponse::Satisfied(deadline)
            }
        }
    }
}

/// Extends an object's deadline and TTL together, leaving metadata intact on error.
fn extend_object_expiry(
    metadata: &mut Metadata,
    target: ExpiryUpdate,
    access_time: Timestamp,
) -> Result<ExpiryOutcome> {
    let Some(original_expires) = metadata.time_expires else {
        return Ok(ExpiryOutcome::Rejected); // entry without a deadline
    };

    // Only write `updated_expires` back if `extended_expiration_policy` also succeeds
    let mut updated_expires = Some(original_expires);
    let outcome = extend_expiry(
        &mut updated_expires,
        target,
        metadata.time_created,
        access_time,
    )?;

    if let ExpiryOutcome::Extended(expire_at) = outcome {
        let updated_policy = common::extended_expiration_policy(
            metadata.expiration_policy,
            metadata.time_created,
            original_expires,
            expire_at,
        )?;

        metadata.time_expires = updated_expires;
        metadata.expiration_policy = updated_policy;
    }

    Ok(outcome)
}

/// Resolves `target` and extends an active expiry time where valid.
///
/// Eligibility is checked before target resolution.
fn extend_expiry(
    field: &mut Option<Timestamp>,
    target: ExpiryUpdate,
    time_created: Option<Timestamp>,
    access_time: Timestamp,
) -> Result<ExpiryOutcome> {
    let Some(time_expires) = *field else {
        return Ok(ExpiryOutcome::Rejected); // entries without a deadline cannot be extended
    };

    if time_expires < access_time {
        return Ok(ExpiryOutcome::NotFound); // already expired
    }

    let Some(expire_at) = target.resolve(time_created, access_time)? else {
        return Ok(ExpiryOutcome::Rejected);
    };
    if time_expires >= expire_at {
        Ok(ExpiryOutcome::AlreadySatisfied(expire_at))
    } else {
        *field = Some(expire_at);
        Ok(ExpiryOutcome::Extended(expire_at))
    }
}

/// Type returned by [`InMemoryBackend::get`] for direct inspection of stored entries.
#[derive(Clone, Debug)]
pub enum Entry {
    /// No entry exists at this key.
    NotFound,
    /// A real object with its metadata and payload bytes.
    Object(Metadata, Bytes),
    /// A redirect tombstone indicating the real object lives in the long-term backend.
    Tombstone(Tombstone),
}

impl Entry {
    /// Returns `true` if the entry is [`Entry::NotFound`].
    pub fn is_not_found(&self) -> bool {
        matches!(self, Entry::NotFound)
    }

    /// Returns `true` if the entry is [`Entry::Object`].
    pub fn is_object(&self) -> bool {
        matches!(self, Entry::Object(_, _))
    }

    /// Returns `true` if the entry is [`Entry::Tombstone`].
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Entry::Tombstone(_))
    }

    /// Panics unless the entry is [`Entry::NotFound`].
    pub fn expect_not_found(&self) {
        match self {
            Entry::NotFound => (),
            _ => panic!("expected not found entry, got {self:?}"),
        }
    }

    /// Returns the metadata and payload bytes, panicking if the entry is not [`Entry::Object`].
    pub fn expect_object(&self) -> (Metadata, Bytes) {
        match self {
            Entry::Object(metadata, bytes) => (metadata.clone(), bytes.clone()),
            _ => panic!("expected object entry, got {self:?}"),
        }
    }

    /// Returns the tombstone, panicking if the entry is not [`Entry::Tombstone`].
    pub fn expect_tombstone(&self) -> Tombstone {
        match self {
            Entry::Tombstone(tombstone) => tombstone.clone(),
            _ => panic!("expected tombstone entry, got {self:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use std::num::NonZeroU32;
    use std::time::Duration;

    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::{OpType, test_utils::DummyProducer};
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::{Backend, ExpiryTarget};
    use crate::id::ObjectContext;
    use crate::stream;

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    async fn create_session(backend: &InMemoryBackend, id: &ObjectId, length: u64) -> BackendToken {
        backend
            .create_upload_session(id, &Metadata::default(), NonZeroU64::new(length).unwrap())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn resumable_upload() {
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let metadata = Metadata {
            custom: [("preserved".into(), "yes".into())].into(),
            ..Default::default()
        };

        // Upload an object and create a session for the same key.
        backend
            .put_object(
                &id,
                &Metadata::default(),
                stream::single("old"),
                Timestamp::now(),
            )
            .await
            .unwrap();
        let token = backend
            .create_upload_session(&id, &metadata, NonZeroU64::new(3).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap(),
            UploadProgress::Incomplete { offset: 0 }
        );

        // Upload a prefix. The session advances, and the old object remains visible.
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, 1, stream::single("a"))
                .await
                .unwrap(),
            UploadProgress::Incomplete { offset: 1 }
        );
        assert_eq!(backend.get(&id).expect_object().1, "old");
        assert_eq!(
            backend
                .put_chunk(&id, &token, 0, 0, stream::single(""))
                .await
                .unwrap(),
            UploadProgress::Incomplete { offset: 1 }
        );
        let error = backend
            .put_chunk(&id, &token, 0, 1, stream::single("a"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UploadOffsetMismatch { offset: 1 });

        // Upload the suffix. Publication replaces the object and deletes the session.
        assert_eq!(
            backend
                .put_chunk(&id, &token, 1, 2, stream::single("bc"))
                .await
                .unwrap(),
            UploadProgress::Complete
        );
        let (actual, bytes) = backend.get(&id).expect_object();
        assert_eq!(bytes, "abc");
        assert_eq!(actual.custom, metadata.custom);
        assert_eq!(actual.size, metadata.size);
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap_err().kind(),
            ErrorKind::UnknownUploadSession
        );
    }

    #[tokio::test]
    async fn resumable_cancel_and_invalid_sessions() {
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let token = create_session(&backend, &id, 3).await;

        // The backend rejects a chunk whose declared range exceeds the session length.
        let error = backend
            .put_chunk(&id, &token, 3, 1, stream::single("x"))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::ChunkExceedsUploadLength {
                offset: 3,
                content_length: 1,
                upload_length: 3
            }
        );

        // Cancellation discards partial progress and makes the token unknown to the backend.
        backend
            .put_chunk(&id, &token, 0, 1, stream::single("a"))
            .await
            .unwrap();
        backend.cancel_upload(&id, &token).await.unwrap();
        assert!(!backend.contains(&id));
        assert_eq!(
            backend.upload_offset(&id, &token).await.unwrap_err().kind(),
            ErrorKind::UnknownUploadSession
        );
    }

    #[tokio::test]
    async fn failed_resumable_chunk_preserves_partial_progress() {
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let token = create_session(&backend, &id, 4).await;

        // Persist a prefix, then disconnect after writing one byte of the next chunk.
        backend
            .put_chunk(&id, &token, 0, 2, stream::single("ab"))
            .await
            .unwrap();
        let body = futures_util::stream::iter([
            Ok(Bytes::from_static(b"c")),
            Err(stream::ClientError::new(std::io::Error::other(
                "interrupted",
            ))),
        ])
        .boxed();
        assert_eq!(
            backend
                .put_chunk(&id, &token, 2, 2, body)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::ClientStream
        );

        // Resume from the partial byte and verify the complete object.
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
        assert_eq!(backend.get(&id).expect_object().1, "abcd");
    }

    #[tokio::test]
    async fn resumable_serializes_session_operations() {
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let token = create_session(&backend, &id, 1).await;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let body = futures_util::stream::once(async { Ok(receiver.await.unwrap()) }).boxed();

        // An offset query waits while a chunk holds the session lock.
        let request = backend.put_chunk(&id, &token, 0, 1, body);
        tokio::pin!(request);
        assert!(futures_util::poll!(&mut request).is_pending());
        let query = backend.upload_offset(&id, &token);
        tokio::pin!(query);
        assert!(futures_util::poll!(&mut query).is_pending());

        // Completion wakes the query, which observes that the session is now missing.
        sender.send(Bytes::from_static(b"x")).unwrap();
        assert_eq!(request.await.unwrap(), UploadProgress::Complete);
        assert_eq!(
            query.await.unwrap_err().kind(),
            ErrorKind::UnknownUploadSession
        );
        assert_eq!(backend.get(&id).expect_object().1, "x");
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn resumable_emits_only_publication() {
        let (backend, producer) = backend_with_change_stream();
        let id = make_id();
        let token = create_session(&backend, &id, 2).await;

        // Partial session state is not reported as a stored object.
        backend
            .put_chunk(&id, &token, 0, 1, stream::single("a"))
            .await
            .unwrap();
        assert!(producer.records().is_empty());

        // Completion emits exactly one write for the published object.
        backend
            .put_chunk(&id, &token, 1, 1, stream::single("b"))
            .await
            .unwrap();
        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
    }

    #[tokio::test]
    async fn set_expiry() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        for policy in [
            ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
        ] {
            let original_expiry = access_time + Duration::from_hours(1);
            let requested = original_expiry + Duration::from_hours(1) + Duration::from_nanos(999);
            for target in [
                ExpiryTarget::At(requested),
                ExpiryTarget::FromCreation(Duration::from_hours(2) + Duration::from_nanos(999)),
            ] {
                let backend = InMemoryBackend::new("test");
                let id = make_id();
                let metadata = Metadata {
                    expiration_policy: policy,
                    time_created: Some(access_time),
                    time_expires: Some(original_expiry),
                    custom: [("preserved".into(), "yes".into())].into(),
                    ..Default::default()
                };
                backend
                    .put_object(&id, &metadata, stream::single("payload"), access_time)
                    .await
                    .unwrap();

                let restricted = ExpiryUpdate {
                    max: Some(Duration::ZERO),
                    target,
                };
                assert_eq!(
                    backend
                        .set_expiry(&id, restricted, access_time)
                        .await
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidMetadata,
                );
                let target = ExpiryUpdate {
                    max: Some(Duration::from_hours(100)),
                    target,
                };
                assert_eq!(
                    backend.set_expiry(&id, target, access_time).await.unwrap(),
                    SetExpiryResponse::Satisfied(requested)
                );
                assert_eq!(
                    backend
                        .set_expiry(&id, restricted, access_time)
                        .await
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidMetadata,
                );
                let (updated, payload) = backend.get(&id).expect_object();
                let expected_policy = match policy {
                    ExpirationPolicy::TimeToLive(_) => {
                        ExpirationPolicy::TimeToLive(Duration::from_hours(2))
                    }
                    other => other,
                };
                assert_eq!(updated.expiration_policy, expected_policy);
                assert_eq!(updated.time_created, metadata.time_created);
                assert_eq!(updated.custom, metadata.custom);
                assert_eq!(payload, Bytes::from_static(b"payload"));
                assert_eq!(updated.time_expires, Some(requested));

                assert_eq!(
                    backend
                        .set_expiry(&id, ExpiryTarget::At(original_expiry).into(), access_time)
                        .await
                        .unwrap(),
                    SetExpiryResponse::Satisfied(original_expiry)
                );
                assert_eq!(backend.get(&id).expect_object().0, updated);
            }
        }
    }

    #[test]
    fn extend_object_expiry_preserves_metadata_on_error() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let original_expiry = access_time + Duration::from_hours(1);
        let requested = original_expiry + Duration::from_hours(1);
        for (time_created, ttl) in [
            (
                Some(requested + Duration::from_secs(1)),
                Duration::from_hours(1),
            ),
            (None, Duration::MAX),
        ] {
            let original = Metadata {
                expiration_policy: ExpirationPolicy::TimeToLive(ttl),
                time_created,
                time_expires: Some(original_expiry),
                ..Default::default()
            };
            let mut metadata = original.clone();
            for _ in 0..2 {
                assert_eq!(
                    extend_object_expiry(
                        &mut metadata,
                        ExpiryTarget::At(requested).into(),
                        access_time,
                    )
                    .unwrap_err()
                    .kind(),
                    ErrorKind::CorruptData
                );
                assert_eq!(metadata, original);
            }
        }
    }

    #[tokio::test]
    async fn set_expiry_rejected() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let backend = InMemoryBackend::new("test");
        let absent = make_id();
        assert_eq!(
            backend
                .set_expiry(
                    &absent,
                    ExpiryTarget::At(access_time + Duration::from_hours(1)).into(),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::NotFound
        );

        let missing_creation = make_id();
        let original_expiry = access_time + Duration::from_hours(1);
        backend
            .put_object(
                &missing_creation,
                &Metadata {
                    expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
                    time_expires: Some(original_expiry),
                    ..Default::default()
                },
                stream::single("legacy"),
                access_time,
            )
            .await
            .unwrap();
        let absolute = original_expiry + Duration::from_hours(1);
        assert_eq!(
            backend
                .set_expiry(
                    &missing_creation,
                    ExpiryTarget::At(absolute).into(),
                    access_time
                )
                .await
                .unwrap(),
            SetExpiryResponse::Satisfied(absolute)
        );
        let updated = backend.get(&missing_creation).expect_object().0;
        assert_eq!(
            updated.expiration_policy,
            ExpirationPolicy::TimeToLive(Duration::from_hours(2))
        );
        assert_eq!(updated.time_created, None);
        assert_eq!(
            backend
                .set_expiry(
                    &missing_creation,
                    ExpiryTarget::FromCreation(Duration::ZERO).into(),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::Rejected
        );

        let manual = make_id();
        backend
            .put_object(
                &manual,
                &Metadata::default(),
                stream::single("manual"),
                access_time,
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .set_expiry(
                    &manual,
                    ExpiryTarget::At(access_time + Duration::from_hours(1)).into(),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::Rejected
        );

        let expired = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(access_time - Duration::from_secs(1)),
            ..Default::default()
        };
        backend
            .put_object(&expired, &metadata, stream::single("expired"), access_time)
            .await
            .unwrap();
        let deadline = metadata.time_expires.unwrap();
        for time in [deadline - Duration::from_secs(1), deadline] {
            assert!(
                backend
                    .get_object(&expired, time, None)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert!(
            backend
                .get_object(&expired, access_time, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend
                .set_expiry(
                    &expired,
                    ExpiryTarget::At(access_time + Duration::from_hours(1)).into(),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::NotFound
        );
    }

    #[tokio::test]
    async fn redirect_expiry() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let target = make_id();
        let other = make_id();
        let old_expiry = access_time + Duration::from_hours(1);
        backend
            .compare_and_write(
                &id,
                None,
                TieredWrite::Tombstone(Tombstone {
                    target: target.clone(),
                    time_expires: Some(old_expiry),
                }),
                access_time,
            )
            .await
            .unwrap();

        assert!(
            backend
                .get_object(&id, access_time, None)
                .await
                .is_err_and(|error| error.kind() == ErrorKind::UnexpectedTombstone)
        );
        assert!(
            backend
                .get_metadata(&id, access_time)
                .await
                .is_err_and(|error| error.kind() == ErrorKind::UnexpectedTombstone)
        );

        let new_expiry = old_expiry + Duration::from_hours(1);
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&other),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(new_expiry).into()),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::Rejected
        );
        assert_eq!(
            backend.get(&id).expect_tombstone().time_expires,
            Some(old_expiry)
        );
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(
                        ExpiryTarget::FromCreation(Duration::from_hours(2)).into()
                    ),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::Rejected
        );
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(new_expiry).into()),
                    access_time
                )
                .await
                .unwrap(),
            SetExpiryResponse::Satisfied(new_expiry)
        );
        assert_eq!(
            backend.get(&id).expect_tombstone().time_expires,
            Some(new_expiry)
        );
    }

    #[tokio::test]
    async fn multipart_single_part() {
        let backend = InMemoryBackend::new("test");
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
        let payload = stream::read_to_vec(body).await.unwrap();
        assert_eq!(payload, data);
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
        let backend = InMemoryBackend::new("test");
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
        let payload = stream::read_to_vec(body).await.unwrap();
        assert_eq!(payload, b"aaaabbbbcc");
    }

    #[tokio::test]
    async fn multipart_list_parts() {
        let backend = InMemoryBackend::new("test");
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
        let backend = InMemoryBackend::new("test");
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
        let backend = InMemoryBackend::new("test");
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
        let backend = InMemoryBackend::new("test");
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
    fn backend_with_change_stream() -> (InMemoryBackend, DummyProducer) {
        use crate::change_stream::CostTrackerStreamConfig;

        let (streams, producer) = crate::change_stream::dummy_factory();
        let change_stream = streams.build(Some(&CostTrackerStreamConfig {
            shared_resource_id: "in_memory_objectstore".into(),
            sample_rate: 1.0,
        }));
        (
            InMemoryBackend::new("test").with_change_stream(change_stream),
            producer,
        )
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_writes_and_deletes() {
        let (backend, producer) = backend_with_change_stream();
        let id = make_id();
        let metadata = Metadata::default();
        let payload = b"hello";

        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();
        backend.delete_object(&id, Timestamp::now()).await.unwrap();
        // The object is already gone, so this reports nothing.
        backend.delete_object(&id, Timestamp::now()).await.unwrap();

        let records = producer.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].app_feature, "testing");
        assert_eq!(
            records[0].size,
            Some((json_len(&metadata) + payload.len()) as u64),
            "the reported size covers metadata as well as the payload"
        );
        assert!(json_len(&metadata) > 0, "metadata must contribute bytes");
        assert_eq!(records[1].op_type, OpType::Delete);
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_expiry_extension_as_update() {
        let (backend, producer) = backend_with_change_stream();
        let id = make_id();
        let access_time = Timestamp::now();
        let expires = access_time + Duration::from_secs(3600);
        let metadata = Metadata {
            time_expires: Some(expires),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single(b"hello".to_vec()),
                access_time,
            )
            .await
            .unwrap();
        producer.clear();

        let extended = expires + Duration::from_secs(3600);
        assert_eq!(
            backend
                .set_expiry(&id, ExpiryTarget::At(extended).into(), access_time)
                .await
                .unwrap(),
            SetExpiryResponse::Satisfied(extended)
        );

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Update);

        // A deadline that already covers the request writes nothing, so it reports nothing.
        producer.clear();
        let requested = expires + Duration::from_secs(60);
        assert_eq!(
            backend
                .set_expiry(&id, ExpiryTarget::At(requested).into(), access_time)
                .await
                .unwrap(),
            SetExpiryResponse::Satisfied(requested)
        );
        assert!(producer.records().is_empty());
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_tombstone_expiry_extension() {
        let (backend, producer) = backend_with_change_stream();
        let id = make_id();
        let target = make_id();
        let access_time = Timestamp::now();
        let expires = access_time + Duration::from_secs(3600);

        backend
            .compare_and_write(
                &id,
                None,
                TieredWrite::Tombstone(Tombstone {
                    target: target.clone(),
                    time_expires: Some(expires),
                }),
                access_time,
            )
            .await
            .unwrap();
        producer.clear();

        let extended = expires + Duration::from_secs(3600);
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(extended).into()),
                    access_time,
                )
                .await
                .unwrap(),
            SetExpiryResponse::Satisfied(extended)
        );

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Update);
    }
}
