//! Storage backend implementations.
//!
//! This module contains the [`Backend`](common::Backend) trait and its
//! implementations. Each backend adapts a specific storage system (BigTable,
//! GCS, local filesystem, S3-compatible) to a uniform interface that
//! [`StorageService`](crate::StorageService) consumes.
//!
//! Backend methods operate on single objects identified by an
//! [`ObjectId`](crate::id::ObjectId). Unlike the service-level API, backends
//! have no knowledge of the two-tier routing or redirect tombstones — those
//! concerns live in `StorageService`, which coordinates across two backend
//! instances.
//!
//! Backends are type-erased into a [`BoxedBackend`](common::BoxedBackend)
//! (`Box<dyn Backend>`) so the service can work with any combination.

pub mod bigtable;
pub mod common;
pub mod gcs;
pub mod local_fs;
pub mod s3_compatible;

#[cfg(test)]
pub(crate) mod in_memory;
