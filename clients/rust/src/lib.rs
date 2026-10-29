#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod auth;
mod client;
mod delete;
mod error;
mod expiry;
mod get;
mod head;
mod key;
mod many;
mod put;
mod response;
#[cfg_attr(not(feature = "resumable-upload-api"), allow(dead_code))]
mod resumable;
pub mod utils;

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};
pub use objectstore_types::usecase::InvalidUsecaseName;

#[cfg(feature = "resumable-upload-api")]
pub use crate::resumable::{
    CancelUploadBuilder, CreateResumableUploadBuilder, PutChunkBuilder, ResumableUpload,
    SessionToken, UploadProgress, UploadProgressBuilder,
};
pub use auth::*;
pub use client::*;
pub use delete::*;
pub use error::*;
pub use expiry::*;
pub use get::*;
pub use head::*;
pub use key::*;
pub use many::*;
pub use put::*;
