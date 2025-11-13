//! Utility functions that might be useful when working with Objectstore.

/// Attempts to guess the MIME type from the given contents.
pub fn guess_mime_type(contents: &[u8]) -> Option<&'static str> {
    infer::get(contents).map(|kind| kind.mime_type())
}
