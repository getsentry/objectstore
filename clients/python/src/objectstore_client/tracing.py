from __future__ import annotations

import weakref
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import IO, TYPE_CHECKING, Any

import sentry_sdk
from sentry_sdk.tracing import Span

from objectstore_client.scope import Scope

if TYPE_CHECKING:
    from objectstore_client.client import Usecase

# Origin recorded on every span this module creates, per Sentry's span-origin
# convention (`auto.<integration>.<library>`).
SPAN_ORIGIN = "auto.objectstore.client"

SpanDataValue = str | int | bool | list[str] | None


@contextmanager
def storage_span(
    operation: str,
    usecase: Usecase,
    scope: Scope,
    **data: SpanDataValue,
) -> Generator[Span]:
    """
    Starts a span for a single Objectstore operation.

    ``operation`` is the same literal already passed to
    ``measure_storage_operation``, so the span's ``op``
    (``objectstore.<operation>``), its name, and the ``storage.<operation>.*``
    metrics all derive from one source. Keyword arguments become
    ``objectstore.<key>`` span data, with ``None`` values dropped so call
    sites can pass optionals inline; attributes that are only known after the
    request completes can be set on the yielded span instead.
    """
    op = f"objectstore.{operation}"
    name = f"{op} {usecase.name}"
    with sentry_sdk.start_span(op=op, name=name, origin=SPAN_ORIGIN) as span:
        span.set_data("objectstore.usecase", usecase.name)
        for scope_key, scope_value in scope.dict().items():
            span.set_data(f"objectstore.scopes.{scope_key}", scope_value)
        for data_key, data_value in data.items():
            if data_value is not None:
                span.set_data(f"objectstore.{data_key}", data_value)
        yield span


def _finish_span(span: Span) -> None:
    if span.timestamp is None:
        span.finish()


class _TracedPayload:
    """
    Wraps a ``get()`` response stream in a child span covering the time from
    the first read to EOF/close, since the automatic ``http.client`` span
    ends at response headers.

    The span is created lazily on first ``read()``, so a caller that never
    reads the stream produces no span — this includes a caller that only
    iterates the stream, since iteration delegates straight to the wrapped
    stream. Finishing is idempotent and also guarded by a
    ``weakref.finalize`` fallback, so an abandoned stream still closes its
    span. Any attribute not defined here (``closed``, ``tell``, ``seekable``,
    etc.) delegates to the wrapped stream too.
    """

    def __init__(self, stream: IO[bytes], parent: Span) -> None:
        self._stream = stream
        self._parent = parent
        self._span: Span | None = None
        self._finalizer: weakref.finalize[[Span], _TracedPayload] | None = None
        self._transferred = 0

    def _ensure_span(self) -> Span:
        if self._span is None:
            self._span = self._parent.start_child(
                op="objectstore.get.stream", name="objectstore.get.stream"
            )
            self._finalizer = weakref.finalize(self, _finish_span, self._span)
        return self._span

    def _finish(self) -> None:
        if self._finalizer is None:
            return
        detached = self._finalizer.detach()
        if detached is None:
            return
        span = self._span
        assert span is not None
        span.set_data("objectstore.transferred_bytes", self._transferred)
        span.finish()

    def read(self, size: int = -1) -> bytes:
        self._ensure_span()
        chunk = self._stream.read(size)
        self._transferred += len(chunk)
        if not chunk:
            self._finish()
        return chunk

    def close(self) -> None:
        self._finish()
        self._stream.close()

    def readable(self) -> bool:
        return True

    def __enter__(self) -> _TracedPayload:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._stream, attr)

    def __iter__(self) -> Iterator[bytes]:
        return iter(self._stream)
