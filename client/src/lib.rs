//! The Storage Client SDK
//!
//! This Client SDK can be used to put/get blobs.
//! It internally deals with chunking and compression of uploads and downloads,
//! making sure that it is done as efficiently as possible.
//!
//! The Client SDK is built primarily as a Rust library,
//! and can be exposed to Python through PyO3 as well.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorageScope {
    org_id: u64,
}
impl StorageScope {
    pub fn for_organization(org_id: u64) -> Self {
        Self { org_id }
    }
}

pub struct StorageService {
    usecase: &'static str,
}
impl StorageService {
    pub const fn for_usecase(usecase: &'static str) -> Self {
        Self { usecase }
    }
    pub fn with_scope(&self, scope: StorageScope) -> StorageClient {
        StorageClient {
            usecase: self.usecase,
            scope,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorageId {
    id: String,
}
impl StorageId {
    pub fn from_str(id: &str) -> Self {
        Self { id: id.into() }
    }
}

static MOCK_STORAGE: LazyLock<Mutex<HashMap<(&'static str, StorageScope, StorageId), Arc<[u8]>>>> =
    LazyLock::new(Default::default);

pub struct StorageClient {
    usecase: &'static str,
    scope: StorageScope,
}
// TODO: all this should be async
impl StorageClient {
    pub fn put_blob(&self, id: Option<StorageId>, contents: &[u8]) -> StorageId {
        let id = id.unwrap_or_else(|| StorageId {
            id: Uuid::new_v4().to_string(),
        });
        let key = (self.usecase, self.scope.clone(), id.clone());
        let contents = contents.into();

        let mut storage = MOCK_STORAGE.lock().unwrap();
        storage.insert(key, contents);
        id
    }

    pub fn get_blob(&self, id: StorageId) -> Option<Arc<[u8]>> {
        let key = (self.usecase, self.scope.clone(), id.clone());

        let storage = MOCK_STORAGE.lock().unwrap();
        storage.get(&key).cloned()
    }

    pub fn delete_blob(&self, id: StorageId) {
        let key = (self.usecase, self.scope.clone(), id.clone());

        let mut storage = MOCK_STORAGE.lock().unwrap();
        storage.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_is_usable() {
        static ATTACHMENTS: StorageService = StorageService::for_usecase("attachments");

        let scope = StorageScope::for_organization(12345);
        let client = ATTACHMENTS.with_scope(scope);

        let blob: &[u8] = b"oh hai!";
        let allocated_id = client.put_blob(None, blob);

        let stored_blob = client.get_blob(allocated_id.clone());
        assert_eq!(stored_blob.unwrap().as_ref(), blob);

        client.delete_blob(allocated_id.clone());
        assert!(client.get_blob(allocated_id).is_none());
    }
}
