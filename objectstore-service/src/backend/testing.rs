//! Generic test backend with per-method aspect hooks.
//!
//! [`TestBackend`] wraps an [`InMemoryBackend`] and routes every method call
//! through a [`BackendAspects`] implementation. All aspect methods default to
//! forwarding to the inner backend, so a test only needs to override the
//! specific method it wants to intercept.
//!
//! # Example
//!
//! ```rust
//! use crate::backend::testing::{Hooks, TestBackend};
//! use crate::backend::in_memory::InMemoryBackend;
//! use crate::backend::common::DeleteResponse;
//! use crate::error::Result;
//! use crate::id::ObjectId;
//!
//! #[derive(Debug)]
//! struct FailDelete;
//!
//! #[async_trait::async_trait]
//! impl Hooks for FailDelete {
//!     async fn delete_object(
//!         &self,
//!         _inner: &InMemoryBackend,
//!         _id: &ObjectId,
//!     ) -> Result<DeleteResponse> {
//!         Err(crate::error::Error::Io(std::io::Error::new(
//!             std::io::ErrorKind::ConnectionRefused,
//!             "simulated delete failure",
//!         )))
//!     }
//! }
//!
//! let backend = TestBackend::new(FailDelete);
//! ```

use std::fmt;

use bytes::Bytes;
use objectstore_types::metadata::Metadata;

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, HighVolumeBackend, MetadataResponse, PutResponse,
    TieredGet, TieredMetadata, TieredWrite, Tombstone,
};
use crate::backend::in_memory::InMemoryBackend;
use crate::error::Result;
use crate::id::ObjectId;
use crate::stream::ClientStream;

/// Hooks for [`TestBackend`].
///
/// Every method defaults to forwarding to the inner [`InMemoryBackend`].
/// Override only the methods relevant to a particular test.
///
/// The `inner` parameter is always the first argument after `&self`, giving
/// full access to the underlying backend for before, around, or after
/// interception patterns.
#[async_trait::async_trait]
pub trait Hooks: fmt::Debug + Send + Sync + 'static {
    // --- Backend methods ---

    /// Returns the diagnostic name used by this backend.
    fn name(&self) -> &'static str {
        "test-backend"
    }

    /// Intercepts [`Backend::put_object`]. Default delegates to `inner`.
    async fn put_object(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        inner.put_object(id, metadata, stream).await
    }

    /// Intercepts [`Backend::get_object`]. Default delegates to `inner`.
    async fn get_object(&self, inner: &InMemoryBackend, id: &ObjectId) -> Result<GetResponse> {
        inner.get_object(id).await
    }

    /// Intercepts [`Backend::get_metadata`]. Default delegates to `inner`.
    async fn get_metadata(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
    ) -> Result<MetadataResponse> {
        inner.get_metadata(id).await
    }

    /// Intercepts [`Backend::delete_object`]. Default delegates to `inner`.
    async fn delete_object(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
    ) -> Result<DeleteResponse> {
        inner.delete_object(id).await
    }

    /// Intercepts [`Backend::check_exists_batch`]. Default delegates to `inner`.
    async fn check_exists_batch(
        &self,
        inner: &InMemoryBackend,
        ids: &[ObjectId],
    ) -> Result<Vec<bool>> {
        inner.check_exists_batch(ids).await
    }

    /// Intercepts [`Backend::join`]. Default delegates to `inner`.
    async fn join(&self, inner: &InMemoryBackend) {
        inner.join().await
    }

    // --- HighVolumeBackend methods ---

    /// Intercepts [`HighVolumeBackend::put_non_tombstone`]. Default delegates to `inner`.
    async fn put_non_tombstone(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<Option<Tombstone>> {
        inner.put_non_tombstone(id, metadata, payload).await
    }

    /// Intercepts [`HighVolumeBackend::get_tiered_object`]. Default delegates to `inner`.
    async fn get_tiered_object(&self, inner: &InMemoryBackend, id: &ObjectId) -> Result<TieredGet> {
        inner.get_tiered_object(id).await
    }

    /// Intercepts [`HighVolumeBackend::get_tiered_metadata`]. Default delegates to `inner`.
    async fn get_tiered_metadata(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
    ) -> Result<TieredMetadata> {
        inner.get_tiered_metadata(id).await
    }

    /// Intercepts [`HighVolumeBackend::delete_non_tombstone`]. Default delegates to `inner`.
    async fn delete_non_tombstone(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
    ) -> Result<Option<Tombstone>> {
        inner.delete_non_tombstone(id).await
    }

    /// Intercepts [`HighVolumeBackend::compare_and_write`]. Default delegates to `inner`.
    async fn compare_and_write(
        &self,
        inner: &InMemoryBackend,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
    ) -> Result<bool> {
        inner.compare_and_write(id, current, write).await
    }

    /// Intercepts [`HighVolumeBackend::get_tiered_metadata_batch`]. Default delegates to `inner`.
    async fn get_tiered_metadata_batch(
        &self,
        inner: &InMemoryBackend,
        ids: &[ObjectId],
    ) -> Result<Vec<TieredMetadata>> {
        inner.get_tiered_metadata_batch(ids).await
    }
}

/// Generic test backend that implements both [`Backend`] and [`HighVolumeBackend`].
///
/// All trait methods are routed through the [`Hooks`] implementation,
/// with access to the underlying [`InMemoryBackend`] for delegation.
#[derive(Debug)]
pub struct TestBackend<H: Hooks> {
    /// The underlying in-memory backend used for delegation by default aspect impls.
    pub inner: InMemoryBackend,
    /// The aspect hooks that intercept trait method calls.
    pub hooks: H,
}

impl<H: Hooks + Clone> Clone for TestBackend<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            hooks: self.hooks.clone(),
        }
    }
}

impl<H: Hooks> TestBackend<H> {
    /// Creates a new `TestBackend` with a fresh [`InMemoryBackend`].
    pub fn new(hooks: H) -> Self {
        Self {
            inner: InMemoryBackend::new("test-backend"),
            hooks,
        }
    }

    /// Creates a new `TestBackend` with the given pre-populated [`InMemoryBackend`].
    pub fn with_inner(inner: InMemoryBackend, hooks: H) -> Self {
        Self { inner, hooks }
    }
}

#[async_trait::async_trait]
impl<H: Hooks> Backend for TestBackend<H> {
    fn name(&self) -> &'static str {
        self.hooks.name()
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        self.hooks
            .put_object(&self.inner, id, metadata, stream)
            .await
    }

    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        self.hooks.get_object(&self.inner, id).await
    }

    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        self.hooks.get_metadata(&self.inner, id).await
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        self.hooks.delete_object(&self.inner, id).await
    }

    async fn check_exists_batch(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        self.hooks.check_exists_batch(&self.inner, ids).await
    }

    async fn join(&self) {
        self.hooks.join(&self.inner).await
    }
}

#[async_trait::async_trait]
impl<H: Hooks> HighVolumeBackend for TestBackend<H> {
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<Option<Tombstone>> {
        self.hooks
            .put_non_tombstone(&self.inner, id, metadata, payload)
            .await
    }

    async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet> {
        self.hooks.get_tiered_object(&self.inner, id).await
    }

    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
        self.hooks.get_tiered_metadata(&self.inner, id).await
    }

    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
        self.hooks.delete_non_tombstone(&self.inner, id).await
    }

    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
    ) -> Result<bool> {
        self.hooks
            .compare_and_write(&self.inner, id, current, write)
            .await
    }

    async fn get_tiered_metadata_batch(&self, ids: &[ObjectId]) -> Result<Vec<TieredMetadata>> {
        self.hooks.get_tiered_metadata_batch(&self.inner, ids).await
    }
}
