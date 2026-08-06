//! Contains all HTTP endpoint handlers.
//!
//! This module documents the request and response shape of every route; see the [crate
//! documentation](crate) for the layers a request passes through before reaching a handler.
//!
//! Scopes are encoded in the URL path using Matrix URI syntax: `org=123;project=456`. An
//! underscore (`_`) represents empty scopes.
//!
//! # Object Endpoints
//!
//! All object operations live under the `/v1/` prefix:
//!
//! | Method   | Path                                      | Description                  |
//! |----------|-------------------------------------------|------------------------------|
//! | `POST`   | `/v1/objects/{usecase}/{scopes}/`         | Insert with server-generated key |
//! | `GET`    | `/v1/objects/{usecase}/{scopes}/{*key}`   | Retrieve object              |
//! | `HEAD`   | `/v1/objects/{usecase}/{scopes}/{*key}`   | Retrieve metadata only       |
//! | `PUT`    | `/v1/objects/{usecase}/{scopes}/{*key}`   | Insert or overwrite with key |
//! | `DELETE` | `/v1/objects/{usecase}/{scopes}/{*key}`   | Delete object                |
//! | `POST`   | `/v1/objects:batch/{usecase}/{scopes}/`   | Batch operations (multipart) |
//!
//! Object metadata travels in request and response headers; see
//! [`objectstore_types::metadata`] for the mapping.
//!
//! # Resumable Upload Endpoints
//!
//! A resumable upload transfers a single object across several requests. The client opens a
//! session, declaring the object's total size and metadata upfront, and then sends the payload
//! as a sequence of chunks at increasing byte offsets. If a chunk fails, the client asks the
//! server which offset it holds and continues from there, so an interrupted transfer resumes
//! where it stopped instead of starting over. The server knows the total size from the
//! session, so it recognizes the chunk carrying the last byte and commits the object itself.
//!
//! Resumable uploads use the object endpoints above, selected by a query parameter:
//! `upload_type=resumable` opens a session, and `session=<token>` addresses it from then on.
//! The object is named by the request path as usual, and [`objectstore_types::resumable`]
//! holds the protocol types.
//!
//! | Method   | Path                                                       | Description                                  |
//! |----------|------------------------------------------------------------|----------------------------------------------|
//! | `POST`   | `/v1/objects/{usecase}/{scopes}/?upload_type=resumable`    | Create session (server-generated key)        |
//! | `PUT`    | `/v1/objects/{usecase}/{scopes}/{*key}?upload_type=resumable` | Create session (user-provided key)        |
//! | `PUT`    | `/v1/objects/{usecase}/{scopes}/{*key}?session=<token>`    | Upload a chunk, or query the offset          |
//! | `DELETE` | `/v1/objects/{usecase}/{scopes}/{*key}?session=<token>`    | Terminate session, discarding what was sent  |
//!
//! Session creation requires an `Upload-Length` header carrying the total size of the object
//! in bytes, and takes the same metadata headers as a regular upload. It answers `200 OK`
//! with `{"key", "session"}` and a `Location` header pointing at the object path with the
//! session appended. Metadata is fixed at this point and does not change afterwards.
//!
//! Chunk uploads and offset queries share one request shape, distinguished by the
//! `Upload-Offset` header: a byte offset submits the body as the chunk starting there, while
//! the `*` wildcard submits an empty body and asks which offset the server holds. Both answer
//! `204 No Content` with the authoritative `Upload-Offset` while bytes remain, and
//! `201 Created` with `{"key"}` once the object is committed. **The offset in the response
//! may be lower than the end of the chunk that was sent** — backends persist only aligned
//! prefixes and discard the remainder — so clients always continue from the returned offset.
//!
//! An offset query can commit an object that was assembled but not yet committed, so it
//! requires write permission despite being read-shaped. Termination likewise needs write
//! rather than delete permission: it releases an in-progress upload, not an object.
//!
//! | Status | Meaning | Client action |
//! |--------|---------|---------------|
//! | `400`  | Malformed: unusable session, missing `Upload-Length`, or a chunk exceeding the declared length | Terminal |
//! | `409`  | On creation: resumable uploads are unavailable for this object. On a chunk: offset mismatch, with the authoritative offset in `Upload-Offset` | Fall back to a regular upload, or resynchronize |
//! | `410`  | The session expired or was terminated; nothing was retained | Start a new session |
//! | `501`  | The configured backend does not implement resumable uploads | Fall back to a regular upload |
//!
//! Not every backend can support this. Session creation asks the backend that would store the
//! object to open one, and a backend that cannot declines, which the server reports as
//! `409 Conflict`. No backend implements resumable uploads yet, so every session creation is
//! currently denied.
//!
//! # Multipart Upload Endpoints
//!
//! Multipart uploads are being replaced by [resumable
//! uploads](#resumable-upload-endpoints) and will be removed once all consumers have
//! migrated. See [`objectstore_types::multipart`] for the protocol types.
//!
//! | Method    | Path                                                         | Description                          |
//! |-----------|--------------------------------------------------------------|--------------------------------------|
//! | `POST`    | `/v1/objects:multipart/{usecase}/{scopes}/`                  | Initiate upload (server-generated key) |
//! | `PUT`     | `/v1/objects:multipart/{usecase}/{scopes}/{*key}`            | Initiate upload (user-provided key)  |
//! | `PUT`     | `/v1/objects:multipart:parts/{usecase}/{scopes}/{*key}`      | Upload a part (`upload_id`, `part_number` query params) |
//! | `GET`     | `/v1/objects:multipart:parts/{usecase}/{scopes}/{*key}`      | List uploaded parts (`upload_id` query param) |
//! | `POST`    | `/v1/objects:multipart:complete/{usecase}/{scopes}/{*key}`   | Complete upload (`upload_id` query param) |
//! | `DELETE`  | `/v1/objects:multipart/{usecase}/{scopes}/{*key}`            | Abort upload (`upload_id` query param) |
//!
//! The initiate POST endpoint accepts both trailing-slash and non-trailing-slash forms.
//!
//! The complete endpoint returns `200 OK` immediately, with a streaming body that will
//! contain the error (if any) as JSON. Whitespace is sent in the streaming body to keep the
//! connection open. Clients must parse the body to determine the actual outcome, and not rely
//! on the status code.
//!
//! # Internal Endpoints
//!
//! Internal endpoints are exempt from authentication, rate limiting, and the web concurrency
//! limit so they remain available when the server is under load. [`is_internal_route`]
//! identifies them.
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `GET` | `/health` | Liveness probe (always returns 200) |
//! | `GET` | `/ready` | Readiness probe (returns 503 when `/tmp/objectstore.down` exists, enabling graceful drain) |
//! | `GET` | `/keda` | Prometheus text-format gauges for KEDA autoscaling (see [KEDA Metrics](crate#keda-metrics)) |
//!
//! # Code Usage
//!
//! Use [`routes`] to create a router with all endpoints.

use axum::Router;

use crate::state::ServiceState;

mod batch;
pub mod common;
pub mod health;
mod keda;
mod multipart;
mod objects;
#[cfg(all(target_os = "linux", feature = "profiling"))]
mod profiling;
mod resumable;

/// Returns `true` for internal endpoints that are exempt from metrics and concurrency limits.
pub fn is_internal_route(route: &str) -> bool {
    matches!(route, "/health" | "/ready" | "/keda") || route.starts_with("/debug/")
}

/// Returns a router with all objectstore HTTP endpoints mounted.
///
/// Mounts health and KEDA endpoints at the root and all object/batch
/// endpoints under `/v1/`.
pub fn routes() -> Router<ServiceState> {
    let routes_v1 = Router::new()
        .merge(objects::router())
        .merge(batch::router())
        .merge(multipart::router());

    let router = Router::new()
        .merge(health::router())
        .merge(keda::router())
        .nest("/v1/", routes_v1);

    std::cfg_select! {
        all(target_os = "linux", feature = "profiling") => {
            router.merge(profiling::router())
        }
        _ => { router }
    }
}
