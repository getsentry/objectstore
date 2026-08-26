from __future__ import annotations

import json
from collections.abc import Generator
from typing import Any, cast

import pytest
import sentry_sdk
from objectstore_client import Client, Usecase
from objectstore_client.errors import RequestError
from objectstore_client.multipart import MultipartUpload
from sentry_sdk.envelope import Envelope
from sentry_sdk.transport import Transport
from sentry_sdk.types import Event, Hint


class FakeResponse:
    def __init__(
        self,
        status: int,
        *,
        data: bytes = b"",
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.data = data
        self.headers = headers or {}
        self._json_data = json_data

    def read(self) -> bytes:
        return self.data

    def json(self) -> dict[str, Any]:
        if self._json_data is not None:
            return self._json_data
        return json.loads(self.data.decode("utf-8"))


class _NoOpTransport(Transport):
    def capture_envelope(self, envelope: Envelope) -> None:
        pass


@pytest.fixture
def captured_transactions() -> Generator[list[Event]]:
    events: list[Event] = []

    def capture(event: Event, hint: Hint) -> Event | None:
        events.append(event)
        return event

    old_client = sentry_sdk.get_global_scope().client
    sentry_sdk.init(
        dsn="http://public@localhost/1",
        traces_sample_rate=1.0,
        before_send_transaction=capture,
        transport=_NoOpTransport,
    )
    try:
        yield events
    finally:
        sentry_sdk.get_client().close()
        sentry_sdk.get_global_scope().set_client(old_client)


def _spans_by_op(event: Event) -> dict[str, Any]:
    spans = cast(list[dict[str, Any]], event["spans"])
    return {span["op"]: span for span in spans}


SIMPLE_OPERATIONS = [
    pytest.param(
        lambda upload: upload["session"].put(b"payload", key="my-key"),
        FakeResponse(200, json_data={"key": "my-key"}),
        "objectstore.put",
        {"objectstore.key": "my-key"},
        id="put",
    ),
    pytest.param(
        lambda upload: upload["session"].get("my-key"),
        FakeResponse(200, data=b"payload", headers={}),
        "objectstore.get",
        {
            "objectstore.key": "my-key",
            "objectstore.found": True,
            "objectstore.compression": "none",
            "objectstore.decompressed": False,
        },
        id="get",
    ),
    pytest.param(
        lambda upload: upload["session"].head("my-key"),
        FakeResponse(200, headers={}),
        "objectstore.head",
        {"objectstore.key": "my-key", "objectstore.found": True},
        id="head",
    ),
    pytest.param(
        lambda upload: upload["session"].delete("my-key"),
        FakeResponse(200),
        "objectstore.delete",
        {"objectstore.key": "my-key"},
        id="delete",
    ),
    pytest.param(
        lambda upload: upload["session"].initiate_multipart_upload(key="my-key"),
        FakeResponse(200, json_data={"key": "my-key", "upload_id": "upload-1"}),
        "objectstore.multipart.initiate",
        {"objectstore.key": "my-key", "objectstore.upload_id": "upload-1"},
        id="multipart.initiate",
    ),
    pytest.param(
        lambda upload: upload["upload"].put_part(
            b"part-data", part_number=1, content_length=len(b"part-data")
        ),
        FakeResponse(200, json_data={"etag": "etag-1"}),
        "objectstore.multipart.put_part",
        {
            "objectstore.key": "my-key",
            "objectstore.upload_id": "upload-1",
            "objectstore.part_number": 1,
            "objectstore.size": len(b"part-data"),
        },
        id="multipart.put_part",
    ),
    pytest.param(
        lambda upload: upload["upload"].list_parts(),
        FakeResponse(200, json_data={"parts": [], "is_truncated": False}),
        "objectstore.multipart.list_parts",
        {
            "objectstore.key": "my-key",
            "objectstore.upload_id": "upload-1",
            "objectstore.part_count": 0,
        },
        id="multipart.list_parts",
    ),
    pytest.param(
        lambda upload: upload["upload"].complete([]),
        FakeResponse(200, data=b'{"key": "my-key"}'),
        "objectstore.multipart.complete",
        {
            "objectstore.key": "my-key",
            "objectstore.upload_id": "upload-1",
            "objectstore.part_count": 0,
        },
        id="multipart.complete",
    ),
    pytest.param(
        lambda upload: upload["upload"].abort(),
        FakeResponse(200),
        "objectstore.multipart.abort",
        {"objectstore.key": "my-key", "objectstore.upload_id": "upload-1"},
        id="multipart.abort",
    ),
]


@pytest.mark.parametrize("call,response,expected_op,expected_data", SIMPLE_OPERATIONS)
def test_storage_span_attributes(
    captured_transactions: list[Event],
    monkeypatch: pytest.MonkeyPatch,
    call: Any,
    response: FakeResponse,
    expected_op: str,
    expected_data: dict[str, Any],
) -> None:
    client = Client("http://127.0.0.1:8888")
    usecase = Usecase("testing")
    session = client.session(usecase, org=1)
    upload = MultipartUpload(session, "my-key", "upload-1")

    monkeypatch.setattr(session._pool, "request", lambda *args, **kwargs: response)

    with sentry_sdk.start_transaction(name="test"):
        call({"session": session, "upload": upload})

    event = captured_transactions[-1]
    spans = _spans_by_op(event)
    span = spans[expected_op]

    assert span["description"] == f"{expected_op} testing"
    assert span["data"]["objectstore.usecase"] == "testing"
    assert span["data"]["objectstore.scopes.org"] == 1
    for key, value in expected_data.items():
        assert span["data"][key] == value


def test_get_missing_object_sets_found_false(
    captured_transactions: list[Event],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client("http://127.0.0.1:8888")
    session = client.session(Usecase("testing"), org=1)

    monkeypatch.setattr(
        session._pool, "request", lambda *args, **kwargs: FakeResponse(404)
    )

    with sentry_sdk.start_transaction(name="test"):
        result = session.get("my-key")

    assert result is None
    event = captured_transactions[-1]
    span = _spans_by_op(event)["objectstore.get"]
    assert span["data"]["objectstore.found"] is False
    assert span.get("tags", {}).get("status") != "internal_error"


def test_request_error_marks_span_internal_error(
    captured_transactions: list[Event],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client("http://127.0.0.1:8888")
    session = client.session(Usecase("testing"), org=1)

    monkeypatch.setattr(
        session._pool,
        "request",
        lambda *args, **kwargs: FakeResponse(500, data=b"boom"),
    )

    with sentry_sdk.start_transaction(name="test"), pytest.raises(RequestError):
        session.get("my-key")

    event = captured_transactions[-1]
    span = _spans_by_op(event)["objectstore.get"]
    assert span["tags"]["status"] == "internal_error"
