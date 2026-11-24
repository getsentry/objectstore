//! Shared helpers for unit tests.

use objectstore_service::backend::changelog::NoopChangeLog;
use objectstore_service::backend::in_memory::InMemoryBackend;
use objectstore_service::backend::tiered::TieredStorage;

/// Creates a [`TieredStorage`] with an [`InMemoryBackend`] in both tiers.
///
/// Must be called within a Tokio runtime, since [`TieredStorage::new`] spawns a recovery task.
pub(crate) fn tiered_storage() -> TieredStorage {
    TieredStorage::new(
        Box::new(InMemoryBackend::new("in-memory-hv")),
        Box::new(InMemoryBackend::new("in-memory-lt")),
        Box::new(NoopChangeLog),
    )
}
