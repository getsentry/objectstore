mod batch;
mod id;
mod service;

/// An extractor for a remote type.
///
/// This is a helper type that allows extracting a type `T` from a request, where `T` is defined in
/// another crate. There must be an implementation of `FromRequestParts` or `FromRequest` for
/// `Xt<T>` for this to work.
#[derive(Debug)]
pub struct Xt<T>(pub T);

pub use batch::{
    BatchError, BatchOperation, BatchRequest, DeleteOperation, GetOperation,
    HEADER_BATCH_OPERATION_KEY, InsertOperation,
};
