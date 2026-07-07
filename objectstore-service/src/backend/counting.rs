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

use std::sync::Arc;

use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, MetadataResponse, MultipartUploadBackend, PutResponse,
};
use crate::error::Result;
use crate::id::ObjectId;
use crate::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use crate::stream::ClientStream;

/// Increments `cogs.usage` by one operation for the given `usecase`.
///
/// Under the hood, the `usecase` is used as the `app_feature`. This allows to identify distinct
/// products and map them in the for the COGs pipeline.
fn count(usecase: &str) {
    objectstore_metrics::count!("cogs.usage" += 1, app_feature = usecase.to_owned());
}

/// A [`Backend`] decorator that counts each operation performed for COGS. Also implements
/// [`MultipartUploadBackend`]. See the [module documentation](self) for how it should be used.
///
/// [`CountingBackend`]'s implementation clashes with how the [`MultipartUploadBackend`] trait is
/// connected to the [`Backend`] trait. The workaround is to give `CountingBackend` (up to) two
/// `Arc`s that point to the inner backend:
/// - `inner: Arc<dyn Backend>`
/// - `inner_multipart: Option<Arc<dyn MultipartUploadBackend>>` if `inner` supports it
#[derive(Debug)]
pub struct CountingBackend {
    inner: Arc<dyn Backend>,
}

impl CountingBackend {
    /// Creates a [`CountingBackend`] that wraps `inner` and increments `cogs.usage`
    /// before delegating operations to it.
    pub fn new(inner: Box<dyn Backend>) -> Self {
        let inner: Arc<dyn Backend> = Arc::from(inner);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl Backend for CountingBackend {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        count(&id.context.usecase);
        self.inner.put_object(id, metadata, stream).await
    }

    async fn get_object(&self, id: &ObjectId, range: Option<ByteRange>) -> Result<GetResponse> {
        count(&id.context.usecase);
        self.inner.get_object(id, range).await
    }

    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        count(&id.context.usecase);
        self.inner.get_metadata(id).await
    }

    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        count(&id.context.usecase);
        self.inner.delete_object(id).await
    }

    async fn join(&self) {
        self.inner.join().await;
    }

    fn as_multipart_upload_backend(&self) -> Result<&dyn MultipartUploadBackend> {
        self.inner.as_multipart_upload_backend()?;
        Ok(self)
    }
}

#[async_trait::async_trait]
impl MultipartUploadBackend for CountingBackend {
    async fn initiate_multipart(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
    ) -> Result<InitiateMultipartResponse> {
        count(&id.context.usecase);
        self.inner
            .as_multipart_upload_backend()?
            .initiate_multipart(id, metadata)
            .await
    }

    async fn upload_part(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        part_number: PartNumber,
        content_length: u64,
        content_md5: Option<&str>,
        body: ClientStream,
    ) -> Result<UploadPartResponse> {
        count(&id.context.usecase);
        self.inner
            .as_multipart_upload_backend()?
            .upload_part(
                id,
                upload_id,
                part_number,
                content_length,
                content_md5,
                body,
            )
            .await
    }

    async fn list_parts(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> Result<ListPartsResponse> {
        count(&id.context.usecase);
        self.inner
            .as_multipart_upload_backend()?
            .list_parts(id, upload_id, max_parts, part_number_marker)
            .await
    }

    async fn abort_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartResponse> {
        count(&id.context.usecase);
        self.inner
            .as_multipart_upload_backend()?
            .abort_multipart(id, upload_id)
            .await
    }

    async fn complete_multipart(
        &self,
        id: &ObjectId,
        upload_id: &UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartResponse> {
        count(&id.context.usecase);
        self.inner
            .as_multipart_upload_backend()?
            .complete_multipart(id, upload_id, parts)
            .await
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
                usecase: usecase.into(),
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
                .put_object(&id, &Metadata::default(), stream::single("hi"))
                .await
                .unwrap();
            backend.get_object(&id, None).await.unwrap();
            backend.get_metadata(&id).await.unwrap();
            backend.delete_object(&id).await.unwrap();
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
                .get_object(&object_id("attachments"), None)
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
                .get_object(&object_id("new_usecase"), None)
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

    #[test]
    fn counts_each_multipart_operation() {
        let captured = capture(async {
            let backend: Arc<dyn Backend> = Arc::new(CountingBackend::new(Box::new(
                InMemoryBackend::new("in-memory"),
            )));
            let multipart = backend.as_multipart_upload_backend().unwrap();
            let id = object_id("attachments");

            let upload_id = multipart
                .initiate_multipart(&id, &Metadata::default())
                .await
                .unwrap();
            multipart
                .upload_part(
                    &id,
                    &upload_id,
                    PartNumber::new(1).unwrap(),
                    2,
                    None,
                    stream::single("hi"),
                )
                .await
                .unwrap();
            multipart
                .list_parts(&id, &upload_id, None, None)
                .await
                .unwrap();
            multipart
                .complete_multipart(&id, &upload_id, vec![])
                .await
                .unwrap();
            // The upload was completed above, so aborting it is a no-op; counting
            // happens on dispatch regardless, which is what this asserts.
            let _ = multipart.abort_multipart(&id, &upload_id).await;
        });

        assert_eq!(
            captured
                .iter()
                .filter(|m| m == &"cogs.usage:+1|c|#app_feature:attachments")
                .count(),
            5,
            "expected one count per multipart operation, captured: {captured:?}"
        );
    }
}
