//! In-memory backend for tests.
//!
//! This provides a [`Backend`](super::common::Backend) backed by a `HashMap`,
//! removing the need for filesystem tempdir management in unit tests. The
//! backend is [`Clone`] so tests can hold a handle for direct inspection while
//! the service owns a boxed copy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;
use objectstore_types::metadata::Metadata;

use super::common::{
    CasMutation, DeleteResponse, GetResponse, PutResponse, TieredGet, TieredMetadata, Tombstone,
};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::stream::ClientStream;

/// An entry in the in-memory store.
#[derive(Clone, Debug)]
enum StoreEntry {
    Object(Metadata, Bytes),
    Tombstone(Tombstone),
}

type Store = HashMap<ObjectId, StoreEntry>;

/// In-memory [`Backend`](super::common::Backend) backed by a `HashMap`.
///
/// Removes the need for filesystem tempdir management in unit tests. The
/// backend is [`Clone`] so tests can hold a handle for direct inspection while
/// the service owns a boxed copy.
#[derive(Debug, Clone)]
pub struct InMemoryBackend {
    name: &'static str,
    store: Arc<Mutex<Store>>,
}

impl InMemoryBackend {
    /// Creates a new `InMemoryBackend` with the given diagnostic `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            store: Arc::new(Mutex::new(HashMap::new())),
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
impl super::common::HighVolumeBackend for InMemoryBackend {
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

    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
        let mut store = self.store.lock().unwrap();
        if let Some(StoreEntry::Tombstone(tombstone)) = store.get(id).cloned() {
            return Ok(Some(tombstone));
        }

        store.remove(id);
        Ok(None)
    }

    async fn cas_put(
        &self,
        id: &ObjectId,
        expected_redirect: Option<&ObjectId>,
        mutation: CasMutation,
    ) -> Result<bool> {
        let mut store = self.store.lock().unwrap();
        let current = store.get(id);

        let matches = match expected_redirect {
            None => !matches!(current, Some(StoreEntry::Tombstone(_))),
            Some(target) => matches!(
                current,
                Some(StoreEntry::Tombstone(t)) if t.target == *target
            ),
        };

        if matches {
            match mutation {
                CasMutation::WriteTombstone(tombstone) => {
                    store.insert(id.clone(), StoreEntry::Tombstone(tombstone));
                }
                CasMutation::WriteInline(metadata, payload) => {
                    store.insert(id.clone(), StoreEntry::Object(metadata, payload));
                }
                CasMutation::Delete => {
                    store.remove(id);
                }
            }
        }

        Ok(matches)
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
