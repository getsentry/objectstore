//! In-memory backend for tests.
//!
//! This provides a [`Backend`](super::common::Backend) backed by a `HashMap`,
//! removing the need for filesystem tempdir management in unit tests. The
//! backend is [`Clone`] so tests can hold a handle for direct inspection while
//! the service owns a boxed copy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::Metadata;

use super::common::{DeleteResponse, GetResponse, PutResponse};
use crate::PayloadStream;
use crate::error::Result;
use crate::id::ObjectId;

type Store = HashMap<ObjectId, (Metadata, Bytes)>;

#[derive(Debug, Clone)]
pub(crate) struct InMemoryBackend {
    name: &'static str,
    store: Arc<Mutex<Store>>,
}

impl InMemoryBackend {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a clone of the stored metadata and bytes, if present.
    pub fn get_stored(&self, id: &ObjectId) -> Option<(Metadata, Bytes)> {
        self.store.lock().unwrap().get(id).cloned()
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
        stream: PayloadStream,
    ) -> Result<PutResponse> {
        let bytes: BytesMut = stream.try_collect().await?;
        self.store
            .lock()
            .unwrap()
            .insert(id.clone(), (metadata.clone(), bytes.freeze()));
        Ok(())
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        let entry = self.store.lock().unwrap().get(id).cloned();
        Ok(entry.map(|(metadata, bytes)| {
            let stream = futures_util::stream::once(async move { Ok(bytes) }).boxed();
            (metadata, stream)
        }))
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        self.store.lock().unwrap().remove(id);
        Ok(())
    }
}
