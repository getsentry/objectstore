from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import sentry_sdk
from sentry_sdk.traces import StreamedSpan

from objectstore_client.scope import Scope

if TYPE_CHECKING:
    from sentry_sdk._types import Attributes

    from objectstore_client.client import Usecase

# Origin recorded on every span this module creates, per Sentry's span-origin
# convention (`auto.<integration>.<library>`).
SPAN_ORIGIN = "auto.objectstore.client"

SpanAttributeValue = str | int | float | bool | list[str] | list[int] | list[float]


@contextmanager
def storage_span(
    operation: str,
    usecase: Usecase,
    scope: Scope,
    **attributes: SpanAttributeValue | None,
) -> Generator[StreamedSpan]:
    """
    Starts a span for a single Objectstore operation.

    ``operation`` is the same literal already passed to
    ``measure_storage_operation``, so the span's ``op``
    (``objectstore.<operation>``), its name, and the ``storage.<operation>.*``
    metrics all derive from one source. Keyword arguments become
    ``objectstore.<key>`` span attributes, with ``None`` values dropped so
    call sites can pass optionals inline; attributes that are only known after
    the request completes can be set on the yielded span instead.
    """
    op = f"objectstore.{operation}"
    name = f"{op} {usecase.name}"
    span_attributes: Attributes = {
        "sentry.op": op,
        "sentry.origin": SPAN_ORIGIN,
        "objectstore.usecase": usecase.name,
    }
    for scope_key, scope_value in scope.dict().items():
        span_attributes[f"objectstore.scopes.{scope_key}"] = scope_value
    for attribute_key, attribute_value in attributes.items():
        if attribute_value is not None:
            span_attributes[f"objectstore.{attribute_key}"] = attribute_value

    with sentry_sdk.traces.start_span(name=name, attributes=span_attributes) as span:
        yield span
