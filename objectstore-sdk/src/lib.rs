//! The Storage Client SDK
//!
//! This Client SDK can be used to put/get blobs.
//! It internally deals with chunking and compression of uploads and downloads,
//! making sure that it is done as efficiently as possible.
//!
//! The Client SDK is built primarily as a Rust library,
//! and can be exposed to Python through PyO3 as well.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use uuid::Uuid;

/// The storage scope of an object.
///
/// This is used to identify the entity that the object is associated with, such as the organization
/// or project. In requests, the scope is used to prevent unauthorized access to objects.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageScope {
    /// The object belongs to this organization.
    org_id: u64,
}

impl StorageScope {
    /// Creates a new storage scope for the given organization ID.
    pub fn for_organization(org_id: u64) -> Self {
        Self { org_id }
    }
}

/// Service for storing and retrieving objects.
///
/// Services connect to an objectstore for a specific use case. To access individual objects, use
/// the `with_scope` method to create a client with the desired scope. It is not possible to access
/// objects across different scopes.
#[derive(Debug)]
pub struct StorageService {
    usecase: &'static str,
}

impl StorageService {
    /// Creates a new storage service for the given use case.
    pub const fn for_usecase(usecase: &'static str) -> Self {
        Self { usecase }
    }

    /// Creates a new storage client with the given scope.
    pub fn with_scope(&self, scope: StorageScope) -> StorageClient {
        StorageClient {
            usecase: self.usecase,
            scope,
        }
    }
}

/// The fully qualified key of a stored object.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageId {
    id: String,
}

impl StorageId {
    /// Creates a new storage ID with the given string.
    ///
    /// It must include the use case and the internal identifier of the object.
    pub fn new(id: String) -> Self {
        Self { id }
    }
}

type StorageKey = (&'static str, StorageScope, StorageId);

static MOCK_STORAGE: LazyLock<Mutex<HashMap<StorageKey, Arc<[u8]>>>> =
    LazyLock::new(Default::default);

/// A scoped objectstore client that can access objects in a specific use case and scope.
#[derive(Debug)]
pub struct StorageClient {
    usecase: &'static str,
    scope: StorageScope,
}

// TODO: all this should be async
impl StorageClient {
    /// Stores a new object.
    ///
    /// Overwrites an existing object if the ID is already in use.
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

    /// Retrieves an object by its ID.
    ///
    /// Returns `None` if the object does not exist or the scope does not match.
    pub fn get_blob(&self, id: StorageId) -> Option<Arc<[u8]>> {
        let key = (self.usecase, self.scope.clone(), id.clone());

        let storage = MOCK_STORAGE.lock().unwrap();
        storage.get(&key).cloned()
    }

    /// Deletes an object by its ID.
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
