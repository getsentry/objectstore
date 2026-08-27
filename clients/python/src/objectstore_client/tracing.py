from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

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
