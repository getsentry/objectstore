//! Resumable upload protocol and optional manual API.

// Manual operations remain compiled internally when the public API is disabled.
#[cfg_attr(not(feature = "resumable-upload-api"), allow(dead_code))]
pub(crate) mod implementation;

#[cfg(feature = "resumable-upload-api")]
mod api;

#[cfg(feature = "resumable-upload-api")]
pub use crate::resumable::implementation::{
    CancelUploadBuilder, CreateResumableUploadBuilder, PutChunkBuilder, ResumableUpload,
    SessionToken, UploadProgress, UploadProgressBuilder,
};
