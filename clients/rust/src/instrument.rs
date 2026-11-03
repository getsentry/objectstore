//! Spans for Objectstore operations, emitted with the `tracing` feature.
//!
//! Every operation that talks to Objectstore runs in an `INFO` span. The spans follow the
//! conventions of the Python client, so that both clients look the same in Sentry when the spans
//! are captured with `sentry-tracing`:
//!
//! - The Sentry op is `objectstore.<operation>`, for example `objectstore.get` or
//!   `objectstore.resumable.create`, set through the `sentry.op` field.
//! - The Sentry span name is `<op> <usecase>`, set through the `sentry.name` field.
//! - All other fields are prefixed with `objectstore.` and become span data. Fields that only
//!   apply to some operations, or that are only known after the request, are recorded later and
//!   stay empty otherwise.
//!
//! This module also exports the logging macros of `tracing`, such as `debug!`. Without the
//! `tracing` feature, the no-op implementation compiles down to the uninstrumented futures and
//! replaces the logging macros with macros that discard their input.

#[cfg(feature = "tracing")]
pub use spans::*;

#[cfg(not(feature = "tracing"))]
pub use noop::*;

#[cfg(feature = "tracing")]
mod spans {
    use std::future::Future;

    use futures_util::StreamExt as _;
    use tracing::field::Empty;
    use tracing::{Instrument as _, Span};

    use crate::{ClientStream, Session};

    #[allow(unused_imports, reason = "mirrors the logging surface of `tracing`")]
    pub use tracing::{debug, error, info, trace, warn};

    /// A span covering a single Objectstore operation.
    #[derive(Debug)]
    pub struct OperationSpan(Span);

    impl OperationSpan {
        /// Creates a span for the storage operation `op` on a single object.
        ///
        /// `key` may be `None` if the server assigns the key, which can then be recorded later as
        /// `objectstore.key`.
        pub fn storage(op: &'static str, session: &Session, key: Option<&str>) -> Self {
            let usecase = session.scope.usecase().name();
            // Fields must be declared when the span is created; recording an undeclared field
            // later has no effect.
            let span = tracing::info_span!(
                "objectstore",
                sentry.op = op,
                sentry.name = %format_args!("{op} {usecase}"),
                objectstore.usecase = usecase,
                objectstore.scopes = Empty,
                objectstore.scopes.org = Empty,
                objectstore.scopes.project = Empty,
                objectstore.key = key,
                objectstore.found = Empty,
                objectstore.compression = Empty,
                objectstore.decompressed = Empty,
                objectstore.uncompressed_size = Empty,
                objectstore.compressed_size = Empty,
                objectstore.offset = Empty,
                objectstore.size = Empty,
            );
            record_scopes(&span, session);
            Self(span)
        }

        /// Creates a span for a batch request carrying `operations` operations.
        ///
        /// Unlike [`Self::storage`], this span does not declare per-object fields, so recording
        /// them while encoding the batch's parts has no effect.
        pub fn batch(session: &Session, operations: usize) -> Self {
            let usecase = session.scope.usecase().name();
            let span = tracing::info_span!(
                "objectstore",
                sentry.op = "objectstore.batch",
                sentry.name = %format_args!("objectstore.batch {usecase}"),
                objectstore.usecase = usecase,
                objectstore.scopes = Empty,
                objectstore.scopes.org = Empty,
                objectstore.scopes.project = Empty,
                objectstore.operations = operations,
            );
            record_scopes(&span, session);
            Self(span)
        }

        /// Runs `future` inside this span.
        pub async fn instrument<F: Future>(self, future: F) -> F::Output {
            future.instrument(self.0).await
        }
    }

    /// Records `value` in the field `field` of the current span.
    ///
    /// Spans that do not declare the field ignore the value.
    pub fn record_field(field: &'static str, value: impl tracing::Value) {
        Span::current().record(field, value);
    }

    /// Records the total number of bytes yielded by `stream` in the field `field` of the current
    /// span.
    ///
    /// The length is recorded once the stream ends. Streams that fail or are dropped early do not
    /// record anything. Use this for request bodies whose size is only known after sending them.
    pub fn record_stream_length(stream: ClientStream, field: &'static str) -> ClientStream {
        // The span is dropped on the first error, so a partial length is never recorded.
        let state = (stream, 0u64, Some(Span::current()));
        futures_util::stream::unfold(state, move |(mut stream, length, span)| async move {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    let length = length + chunk.len() as u64;
                    Some((Ok(chunk), (stream, length, span)))
                }
                Some(Err(error)) => Some((Err(error), (stream, length, None))),
                None => {
                    if let Some(span) = span {
                        span.record(field, length);
                    }
                    None
                }
            }
        })
        .boxed()
    }

    /// Records the scopes of `session`.
    ///
    /// Span fields must be declared statically, so all scopes are recorded together as
    /// `objectstore.scopes`, for example `org=1;project=2;app=foo`. The well-known `org` and
    /// `project` scopes are also recorded on their own, as numbers where possible.
    fn record_scopes(span: &Span, session: &Session) {
        let scopes = session.scope.scopes();
        span.record(
            "objectstore.scopes",
            tracing::field::display(scopes.as_api_path()),
        );
        for scope in scopes {
            let field = match scope.name() {
                "org" => "objectstore.scopes.org",
                "project" => "objectstore.scopes.project",
                _ => continue,
            };
            match scope.value().parse::<u64>() {
                Ok(number) => span.record(field, number),
                Err(_) => span.record(field, scope.value()),
            };
        }
    }
}

/// The implementation without the `tracing` feature, which records nothing.
#[cfg(not(feature = "tracing"))]
#[allow(unused_macros, reason = "mirrors the macros of `tracing`")]
mod noop {
    use std::future::Future;

    use crate::{ClientStream, Session};

    // Logging macros that discard their input, mirroring the ones of `tracing`.
    macro_rules! trace {
        ($($args:tt)*) => {};
    }
    macro_rules! debug {
        ($($args:tt)*) => {};
    }
    macro_rules! info {
        ($($args:tt)*) => {};
    }
    // A local `warn` macro is ambiguous with the built-in `#[warn]` attribute.
    macro_rules! noop_warn {
        ($($args:tt)*) => {};
    }
    macro_rules! error {
        ($($args:tt)*) => {};
    }

    #[allow(unused_imports, reason = "mirrors the logging surface of `tracing`")]
    pub(crate) use {debug, error, info, noop_warn as warn, trace};

    /// A span covering a single Objectstore operation.
    #[derive(Debug)]
    pub struct OperationSpan;

    impl OperationSpan {
        /// Creates a span for the storage operation `op` on a single object.
        pub fn storage(_op: &'static str, _session: &Session, _key: Option<&str>) -> Self {
            Self
        }

        /// Creates a span for a batch request carrying `operations` operations.
        pub fn batch(_session: &Session, _operations: usize) -> Self {
            Self
        }

        /// Runs `future` inside this span.
        pub async fn instrument<F: Future>(self, future: F) -> F::Output {
            future.await
        }
    }

    /// Records `value` in the field `field` of the current span.
    pub fn record_field<V>(_field: &'static str, _value: V) {}

    /// Records the total number of bytes yielded by `stream` in the field `field` of the current
    /// span.
    pub fn record_stream_length(stream: ClientStream, _field: &'static str) -> ClientStream {
        stream
    }
}
