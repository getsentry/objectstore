//! The Storage Client SDK
//!
//! This Client SDK can be used to put/get blobs.
//! It internally deals with chunking and compression of uploads and downloads,
//! making sure that it is done as efficiently as possible.
//!
//! The Client SDK is built primarily as a Rust library,
//! and can be exposed to Python through PyO3 as well.
