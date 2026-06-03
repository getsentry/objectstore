//! Axum extractor for range requests.

use axum::extract::FromRequestParts;
use http::request::Parts;
use objectstore_types::range::{ByteRange, RangeError};

use crate::{endpoints::common::ApiError, state::ServiceState};

/// Extractor that parses the `Range` request header into an optional [`ByteRange`].
#[derive(Debug, Clone)]
pub struct OptionalByteRange(pub Option<ByteRange>);

impl FromRequestParts<ServiceState> for OptionalByteRange {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let headers = &parts.headers;
        let Some(range) = headers.get(http::header::RANGE) else {
            return Ok(Self(None));
        };
        let range = range
            .to_str()
            .map_err(|_| ApiError::Client("invalid Range header".into()))?;

        match range.parse::<ByteRange>() {
            Ok(range) => Ok(Self(Some(range))),
            // Per RFC 9110:
            // > A server that supports range requests MAY ignore or reject a Range header
            //   field that contains an invalid ranges-specifier [...]
            //
            // If the client wants multiple ranges, fall back to returning the whole object.
            // We might support multiple ranges in the future, so log a warning to let us know
            // clients are trying to do this.
            Err(RangeError::MultiRange) => {
                objectstore_log::warn!(
                    "received range request with multiple range specifiers, ignoring"
                );
                Ok(Self(None))
            }
            // The client requested an invalid unit or sent a malformed header.
            // We could fall back, but better fail hard and let them know they requested
            // something we won't support.
            Err(err) => Err(ApiError::Client(format!("invalid Range header: {err}"))),
        }
    }
}
