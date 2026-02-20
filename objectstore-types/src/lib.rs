//! # Shared Types
//!
//! This crate defines the types shared between the objectstore server, service,
//! and client libraries. It is the common vocabulary that ensures all components
//! agree on how metadata is represented, how scopes work, what permissions exist,
//! and how objects expire.
//!
//! ## Metadata
//!
//! The [`metadata`] module defines [`Metadata`](metadata::Metadata), the
//! per-object metadata structure carried alongside every object. It travels
//! through the entire system: clients set it via HTTP headers, the server parses
//! and validates it, the service passes it to backends, and backends persist it.
//! The module also defines [`ExpirationPolicy`](metadata::ExpirationPolicy) for
//! automatic object cleanup and [`Compression`](metadata::Compression) for payload
//! encoding.
//!
//! ## Scopes
//!
//! The [`scope`] module defines [`Scope`](scope::Scope) (a single key-value pair)
//! and [`Scopes`](scope::Scopes) (an ordered collection). Scopes organize objects
//! into hierarchical namespaces and double as the authorization boundary checked
//! against JWT claims.
//!
//! ## Keys
//!
//! The [`key`] module defines [`ObjectKey`](key::ObjectKey), a validated
//! percent-encoded object key. Keys use RFC 3986 unreserved characters literally
//! and percent-encode everything else. They are limited to 128 encoded bytes and
//! cannot contain literal `/`, making them safe for use in HTTP paths, headers,
//! and storage backend paths.
//!
//! ## Auth
//!
//! The [`auth`] module defines [`Permission`](auth::Permission), the set of
//! operations that can be granted in a JWT token and checked by the server before
//! each request.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod auth;
pub mod key;
pub mod metadata;
pub mod scope;
