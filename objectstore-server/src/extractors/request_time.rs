//! Request start time shared by object creation and expiry decisions.

use std::convert::Infallible;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use objectstore_types::time::Timestamp;

/// The request's creation and access timestamp.
///
/// Middleware extracts this at request entry. Subsequent extractors reuse the
/// extension, including when a handler dispatches to another handler. Without
/// the middleware, the first extraction captures the time instead.
#[derive(Clone, Copy, Debug)]
pub struct RequestTime(pub Timestamp);

impl<S: Send + Sync> FromRequestParts<S> for RequestTime {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(*parts
            .extensions
            .get_or_insert_with(|| Self(Timestamp::now())))
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequestParts;
    use axum::http::Request;
    use objectstore_types::time::Timestamp;

    use super::RequestTime;

    #[tokio::test]
    async fn reuses_request_time() {
        let (mut parts, ()) = Request::new(()).into_parts();
        let first = RequestTime::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(parts.extensions.get::<RequestTime>().unwrap().0, first.0);

        parts.extensions.insert(RequestTime(Timestamp::UNIX_EPOCH));
        let extracted = RequestTime::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(extracted.0, Timestamp::UNIX_EPOCH);
    }
}
