use std::str::FromStr;

use axum::http::{HeaderMap, HeaderValue, header};
use objectstore_types::{Compression, ExpirationPolicy, HEADER_EXPIRATION, Metadata};

pub fn extract_metadata_from_headers(headers: &HeaderMap) -> anyhow::Result<Metadata> {
    let mut metadata = Metadata::default();
    if let Some(compression) = headers
        .get(header::CONTENT_ENCODING)
        .map(HeaderValue::to_str)
        .transpose()?
    {
        metadata.compression = Compression::from_str(compression)?;
    }
    if let Some(expiration_policy) = headers
        .get(HEADER_EXPIRATION)
        .map(HeaderValue::to_str)
        .transpose()?
    {
        metadata.expiration_policy = ExpirationPolicy::from_str(expiration_policy)?;
    }

    Ok(metadata)
}
