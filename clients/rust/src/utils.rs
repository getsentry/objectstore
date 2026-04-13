//! Utility functions that might be useful when working with Objectstore.

pub use crate::presign::presign_url;

/// Attempts to guess the MIME type from the given contents.
pub fn guess_mime_type<T: AsRef<[u8]>>(contents: T) -> Option<&'static str> {
    infer::get(contents.as_ref()).map(|kind| kind.mime_type())
}
