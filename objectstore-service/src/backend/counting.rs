//! Defines [`CountingBackend`], a decorator for the [`Backend`] trait that emits Cost of Goods Sold
//! (COGS) compute usage metrics to the `cogs.usage` counter.
//!
//! [`CountingBackend`] is meant to wrap the outer-most [`Backend`] implementation owned by
//! [`StorageService`] so that every tracked backend operation, whether a single-object operation
//! called by [`StorageService`] or a batched operation streamed by [`StreamExecutor`], is counted
//! once. Notably, any operation that fails before it gets to [`StorageService`] (e.g. an auth or
//! rate limit failure at a higher layer) is not counted.
//!
//! For COGS purposes we use operation count as a proxy for compute cost under the assumption that
//! each request we serve has a basically flat CPU cost. Large payloads take longer, but they can be
//! streamed in the background while other requests are served so they don't really cost more.
//!
//! [`StorageService`]: crate::service::StorageService
//! [`StreamExecutor`]: crate::streaming::StreamExecutor

use std::num::NonZeroU64;

use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;
use objectstore_types::resumable::UploadProgress;
use objectstore_types::time::Timestamp;
use objectstore_types::usecase::Usecase;

use crate::backend::common::{
    Backend, DeleteResponse, ExpiryUpdate, GetResponse, MetadataResponse, PutResponse,
    SetExpiryResponse,
};
use crate::error::Result;
use crate::id::ObjectId;
use crate::resumable::{BackendToken, Session};
use crate::stream::ClientStream;

/// Increments `cogs.usage` by one operation for the given `usecase`.
///
/// Under the hood, the `usecase` is used as the `app_feature`. This allows to identify distinct
/// products and map them in the for the COGs pipeline.
fn count(usecase: &Usecase) {
    objectstore_metrics::count!("cogs.usage" += 1, app_feature = usecase.to_string());
}

/// A [`Backend`] decorator that counts each operation performed for COGS.
///
/// See the [module documentation](self) for how it should be used.
#[derive(Debug)]
pub struct CountingBackend {
    inner: Box<dyn Backend>,
}

impl CountingBackend {
    /// Creates a [`CountingBackend`] that wraps `inner` and increments `cogs.usage`
    /// before delegating operations to it.
    pub fn new(inner: Box<dyn Backend>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl Backend for CountingBackend {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn upload_granularity(&self) -> u64 {
        self.inner.upload_granularity()
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
        access_time: Timestamp,
    ) -> Result<PutResponse> {
        count(id.usecase());
        self.inner
            .put_object(id, metadata, stream, access_time)
            .await
    }

    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        count(id.usecase());
        self.inner.get_object(id, access_time, range).await
    }

    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        count(id.usecase());
        self.inner.get_metadata(id, access_time).await
    }

    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        count(id.usecase());
        self.inner.set_expiry(id, target, access_time).await
    }

    async fn delete_object(&self, id: &ObjectId, access_time: Timestamp) -> Result<DeleteResponse> {
        count(id.usecase());
        self.inner.delete_object(id, access_time).await
    }

    async fn join(&self) {
        self.inner.join().await;
    }

    async fn create_upload_session(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        upload_length: NonZeroU64,
    ) -> Result<Option<BackendToken>> {
        count(id.usecase());
        self.inner
            .create_upload_session(id, metadata, upload_length)
            .await
    }

    async fn put_chunk(
        &self,
        session: &Session,
        offset: u64,
        content_length: u64,
        stream: ClientStream,
    ) -> Result<UploadProgress> {
        count(session.object_id.usecase());
        self.inner
            .put_chunk(session, offset, content_length, stream)
            .await
    }

    async fn upload_offset(&self, session: &Session) -> Result<UploadProgress> {
        count(session.object_id.usecase());
        self.inner.upload_offset(session).await
    }

    async fn cancel_upload(&self, session: &Session) -> Result<()> {
        count(session.object_id.usecase());
        self.inner.cancel_upload(session).await
    }
}

#[cfg(test)]
mod tests {
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::id::ObjectContext;
    use crate::stream;

    fn object_id(usecase: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: usecase.parse().unwrap(),
                scopes: Scopes::from_iter([Scope::create("org", "1").unwrap()]),
            },
            "key".into(),
        )
    }

    /// Runs `f` on a current-thread runtime while capturing emitted metrics.
    ///
    /// The capturing client is thread-local, so the futures must run on the same
    /// thread that installs it.
    fn capture(f: impl std::future::Future<Output = ()>) -> Vec<String> {
        objectstore_metrics::with_capturing_test_client(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f);
        })
    }

    #[test]
    fn counts_each_core_operation_once() {
        let captured = capture(async {
            let backend = CountingBackend::new(Box::new(InMemoryBackend::new("in-memory")));
            let id = object_id("attachments");

            backend
                .put_object(
                    &id,
                    &Metadata::default(),
                    stream::single("hi"),
                    Timestamp::now(),
                )
                .await
                .unwrap();
            backend
                .get_object(&id, Timestamp::now(), None)
                .await
                .unwrap();
            backend.get_metadata(&id, Timestamp::now()).await.unwrap();
            backend.delete_object(&id, Timestamp::now()).await.unwrap();
        });

        let cogs = captured
            .iter()
            .filter(|m| m.starts_with("cogs.usage"))
            .count();
        assert_eq!(
            cogs, 4,
            "expected one count per operation, captured: {captured:?}"
        );
        assert!(
            captured
                .iter()
                .all(|m| !m.starts_with("cogs.usage")
                    || m == "cogs.usage:+1|c|#app_feature:attachments"),
            "captured: {captured:?}"
        );
    }

    #[test]
    fn counts_missing_reads_on_dispatch() {
        let captured = capture(async {
            let backend = CountingBackend::new(Box::new(InMemoryBackend::new("in-memory")));
            // Nothing stored: the read returns `None` but is still billed.
            let result = backend
                .get_object(&object_id("attachments"), Timestamp::now(), None)
                .await
                .unwrap();
            assert!(result.is_none());
        });

        assert_eq!(
            captured
                .iter()
                .filter(|m| m.starts_with("cogs.usage"))
                .count(),
            1,
            "captured: {captured:?}"
        );
    }

    #[test]
    fn new_usecase_is_app_feature() {
        let captured = capture(async {
            let backend = CountingBackend::new(Box::new(InMemoryBackend::new("in-memory")));
            backend
                .get_object(&object_id("new_usecase"), Timestamp::now(), None)
                .await
                .unwrap();
        });

        assert!(
            captured
                .iter()
                .any(|m| m == "cogs.usage:+1|c|#app_feature:new_usecase"),
            "captured: {captured:?}"
        );
    }
}
