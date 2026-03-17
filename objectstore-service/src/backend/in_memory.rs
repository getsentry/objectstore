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
    ConditionalOutcome, DeleteResponse, GetResponse, HvGetResponse, HvMetadataResponse,
    PutResponse, Tombstone,
};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::stream::ClientStream;

/// An entry in the in-memory store.
#[derive(Clone, Debug)]
enum InMemoryEntry {
    Object(Metadata, Bytes),
    Tombstone(Tombstone),
}

type Store = HashMap<ObjectId, InMemoryEntry>;

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

    /// Returns a clone of the stored metadata and bytes, if present.
    ///
    /// For tombstone entries, synthesizes a [`Metadata`] with
    /// `is_redirect_tombstone: Some(true)` and returns empty bytes.
    pub fn get_stored(&self, id: &ObjectId) -> Option<(Metadata, Bytes)> {
        self.store.lock().unwrap().get(id).map(|entry| match entry {
            InMemoryEntry::Object(metadata, bytes) => (metadata.clone(), bytes.clone()),
            InMemoryEntry::Tombstone(tombstone) => {
                let metadata = Metadata {
                    is_redirect_tombstone: Some(true),
                    expiration_policy: tombstone.expiration_policy,
                    ..Default::default()
                };
                (metadata, Bytes::new())
            }
        })
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
            InMemoryEntry::Object(metadata.clone(), bytes.freeze()),
        );
        Ok(())
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        match entry {
            None => Ok(None),
            Some(InMemoryEntry::Tombstone(_)) => Err(Error::UnexpectedTombstone),
            Some(InMemoryEntry::Object(mut metadata, bytes)) => {
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
    ) -> Result<ConditionalOutcome> {
        let mut store = self.store.lock().unwrap();
        if store
            .get(id)
            .is_some_and(|e| matches!(e, InMemoryEntry::Tombstone(_)))
        {
            return Ok(ConditionalOutcome::Tombstone);
        }
        let mut metadata = metadata.clone();
        metadata.size = Some(payload.len());
        store.insert(id.clone(), InMemoryEntry::Object(metadata, payload));
        Ok(ConditionalOutcome::Executed)
    }

    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<ConditionalOutcome> {
        let mut store = self.store.lock().unwrap();
        if store
            .get(id)
            .is_some_and(|e| matches!(e, InMemoryEntry::Tombstone(_)))
        {
            return Ok(ConditionalOutcome::Tombstone);
        }
        store.remove(id);
        Ok(ConditionalOutcome::Executed)
    }

    async fn create_tombstone(&self, id: &ObjectId, tombstone: Tombstone) -> Result<()> {
        self.store
            .lock()
            .unwrap()
            .insert(id.clone(), InMemoryEntry::Tombstone(tombstone));
        Ok(())
    }

    async fn hv_get_object(&self, id: &ObjectId) -> Result<HvGetResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => HvGetResponse::NotFound,
            Some(InMemoryEntry::Tombstone(tombstone)) => HvGetResponse::Tombstone(tombstone),
            Some(InMemoryEntry::Object(mut metadata, bytes)) => {
                metadata.size = Some(bytes.len());
                HvGetResponse::Object(metadata, crate::stream::single(bytes))
            }
        })
    }

    async fn hv_get_metadata(&self, id: &ObjectId) -> Result<HvMetadataResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(match entry {
            None => HvMetadataResponse::NotFound,
            Some(InMemoryEntry::Tombstone(tombstone)) => HvMetadataResponse::Tombstone(tombstone),
            Some(InMemoryEntry::Object(metadata, _bytes)) => HvMetadataResponse::Object(metadata),
        })
    }
}
