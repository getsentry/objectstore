//! Utility functions that might be useful when working with Objectstore.

use mediatype::MediaTypeBuf;

/// Attempts to guess the MIME type from the given contents.
pub fn guess_mime_type<T: AsRef<[u8]>>(contents: T) -> Option<MediaTypeBuf> {
    infer::get(contents.as_ref()).and_then(|kind| kind.mime_type().parse().ok())
}
