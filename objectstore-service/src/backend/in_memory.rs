//! In-memory backend for tests.
//!
//! This provides a [`Backend`](super::common::Backend) backed by a `HashMap`,
//! removing the need for filesystem tempdir management in unit tests. The
//! backend is [`Clone`] so tests can hold a handle for direct inspection while
//! the service owns a boxed copy.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use objectstore_types::metadata::Metadata;

use super::common::{
    DeleteResponse, GetResponse, HighVolumeBackend, MultipartUploadBackend, PutResponse, TieredGet,
    TieredMetadata, TieredWrite, Tombstone,
};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, Part, PartNumber, UploadId, UploadPartResponse,
};
use crate::stream::ClientStream;

/// An entry in the in-memory store.
#[derive(Clone, Debug)]
enum StoreEntry {
    Object(Metadata, Bytes),
    Tombstone(Tombstone),
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
}

impl InMemoryBackend {
    /// Creates a new `InMemoryBackend` with the given diagnostic `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            store: Arc::new(Mutex::new(HashMap::new())),
            multipart_store: Arc::new(Mutex::new(HashMap::new())),
        }
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

    fn as_multipart_upload_backend(&self) -> Option<&dyn MultipartUploadBackend> {
        Some(self)
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        let bytes: BytesMut = stream.try_collect().await?;
        self.store.lock().unwrap().insert(
            id.clone(),
            StoreEntry::Object(metadata.clone(), bytes.freeze()),
        );
        Ok(())
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        match entry {
            None => Ok(None),
            Some(StoreEntry::Tombstone(_)) => Err(Error::UnexpectedTombstone),
            Some(StoreEntry::Object(mut metadata, bytes)) => {
                metadata.size = Some(bytes.len());
                let stream = crate::stream::single(bytes);
                Ok(Some((metadata, stream)))
            }
        }
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        self.store.lock().unwrap().remove(id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl HighVolumeBackend for InMemoryBackend {
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<Option<Tombstone>> {
        let mut store = self.store.lock().unwrap();
        if let Some(StoreEntry::Tombstone(tombstone)) = store.get(id).cloned() {
            return Ok(Some(tombstone));
        }

        let mut metadata = metadata.clone();
        metadata.size = Some(payload.len());
        store.insert(id.clone(), StoreEntry::Object(metadata, payload));
        Ok(None)
    }

    async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => TieredGet::NotFound,
            Some(StoreEntry::Tombstone(tombstone)) => TieredGet::Tombstone(tombstone),
            Some(StoreEntry::Object(mut metadata, bytes)) => {
                metadata.size = Some(bytes.len());
                TieredGet::Object(metadata, crate::stream::single(bytes))
            }
        })
    }

    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => TieredMetadata::NotFound,
            Some(StoreEntry::Tombstone(tombstone)) => TieredMetadata::Tombstone(tombstone),
            Some(StoreEntry::Object(metadata, _bytes)) => TieredMetadata::Object(metadata),
        })
    }

    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
        let mut store = self.store.lock().unwrap();
        if let Some(StoreEntry::Tombstone(tombstone)) = store.get(id).cloned() {
            return Ok(Some(tombstone));
        }

        store.remove(id);
        Ok(None)
    }

    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
    ) -> Result<bool> {
        let mut store = self.store.lock().unwrap();

        let actual = store.get(id);
        let matches_current = matches_redirect(actual, current);
        let matches_next = matches_redirect(actual, write.target());

        if matches_current {
            match write {
                TieredWrite::Tombstone(tombstone) => {
                    store.insert(id.clone(), StoreEntry::Tombstone(tombstone));
                }
                TieredWrite::Object(metadata, payload) => {
                    store.insert(id.clone(), StoreEntry::Object(metadata, payload));
                }
                TieredWrite::Delete => {
                    store.remove(id);
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
        let upload_id = uuid::Uuid::now_v7().to_string();
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
            .ok_or_else(|| Error::generic("multipart upload not found"))?;

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
        let upload = store
            .get(&(id.clone(), upload_id.clone()))
            .ok_or_else(|| Error::generic("multipart upload not found"))?;

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
    ) -> Result<CompleteMultipartResponse> {
        let key = (id.clone(), upload_id.clone());

        // TODO: validate that parts are in ascending part_number order and reject with
        // InvalidPartOrder if not (matches S3/GCS behavior). Needs a proper client error variant.

        // Validate and assemble while holding the multipart lock, but don't
        // remove the upload yet — a failed validation must leave it intact so
        // the client can retry.
        let assembled = {
            let store = self.multipart_store.lock().unwrap();
            let upload = store
                .get(&key)
                .ok_or_else(|| Error::generic("multipart upload not found"))?;

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

        self.store
            .lock()
            .unwrap()
            .insert(id.clone(), StoreEntry::Object(assembled.0, assembled.1));

        self.multipart_store.lock().unwrap().remove(&key);

        Ok(None)
    }
}

/// Returns `true` if `entry` matches the expected tombstone redirect state.
///
/// - `expected = None`: matches any non-tombstone (absent or inline object).
/// - `expected = Some(target)`: matches a tombstone whose redirect target equals `target`.
fn matches_redirect(entry: Option<&StoreEntry>, expected: Option<&ObjectId>) -> bool {
    match expected {
        None => matches!(entry, Some(StoreEntry::Object { .. }) | None),
        Some(target) => matches!(entry, Some(StoreEntry::Tombstone(t)) if t.target == *target),
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
            _ => panic!("expected not found entry, got {:?}", self),
        }
    }

    /// Returns the metadata and payload bytes, panicking if the entry is not [`Entry::Object`].
    pub fn expect_object(&self) -> (Metadata, Bytes) {
        match self {
            Entry::Object(metadata, bytes) => (metadata.clone(), bytes.clone()),
            _ => panic!("expected object entry, got {:?}", self),
        }
    }

    /// Returns the tombstone, panicking if the entry is not [`Entry::Tombstone`].
    pub fn expect_tombstone(&self) -> Tombstone {
        match self {
            Entry::Tombstone(tombstone) => tombstone.clone(),
            _ => panic!("expected tombstone entry, got {:?}", self),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend;
    use crate::id::ObjectContext;
    use crate::stream;

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[tokio::test]
    async fn multipart_single_part() {
        let backend = InMemoryBackend::new("test");
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
                1,
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
                    part_number: 1,
                    etag,
                }],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "expected no error on complete");

        let (meta, body) = backend.get_object(&id).await.unwrap().unwrap();
        let payload = stream::read_to_vec(body).await.unwrap();
        assert_eq!(payload, data);
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
                1,
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
                2,
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
                3,
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
                        part_number: 1,
                        etag: etag1,
                    },
                    CompletedPart {
                        part_number: 2,
                        etag: etag2,
                    },
                    CompletedPart {
                        part_number: 3,
                        etag: etag3,
                    },
                ],
            )
            .await
            .unwrap();
        assert!(result.is_none());

        let (_, body) = backend.get_object(&id).await.unwrap().unwrap();
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
            .upload_part(&id, &upload_id, 1, 3, None, stream::single(b"aaa".to_vec()))
            .await
            .unwrap();
        let etag2 = backend
            .upload_part(&id, &upload_id, 2, 3, None, stream::single(b"bbb".to_vec()))
            .await
            .unwrap();

        let list = backend
            .list_parts(&id, &upload_id, None, None)
            .await
            .unwrap();
        assert_eq!(list.parts.len(), 2);
        assert_eq!(list.parts[0].part_number, 1);
        assert_eq!(list.parts[0].etag, etag1);
        assert_eq!(list.parts[0].size, 3);
        assert_eq!(list.parts[1].part_number, 2);
        assert_eq!(list.parts[1].etag, etag2);
        assert_eq!(list.parts[1].size, 3);

        // Pagination
        let page1 = backend
            .list_parts(&id, &upload_id, Some(1), None)
            .await
            .unwrap();
        assert_eq!(page1.parts.len(), 1);
        assert_eq!(page1.parts[0].part_number, 1);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        let page2 = backend
            .list_parts(&id, &upload_id, Some(1), page1.next_part_number_marker)
            .await
            .unwrap();
        assert_eq!(page2.parts.len(), 1);
        assert_eq!(page2.parts[0].part_number, 2);

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
                1,
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
        let backend = InMemoryBackend::new("test");
        let id = make_id();
        let metadata = Metadata::default();

        let upload_id = backend.initiate_multipart(&id, &metadata).await.unwrap();

        let etag = backend
            .upload_part(
                &id,
                &upload_id,
                1,
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
                    part_number: 1,
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
                vec![CompletedPart {
                    part_number: 1,
                    etag,
                }],
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
                1,
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
                    part_number: 99,
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
                vec![CompletedPart {
                    part_number: 1,
                    etag,
                }],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "retry with correct part should succeed");
    }
}
