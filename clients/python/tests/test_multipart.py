import json
from dataclasses import dataclass
from typing import Any

import pytest
from objectstore_client import Client, Usecase
from objectstore_client.errors import RequestError
from objectstore_client.metrics import Tags
from objectstore_client.multipart import CompletePart, MultipartUpload


@dataclass
class DistributionRecord:
    name: str
    value: int | float
    tags: Tags | None
    unit: str | None


class RecordingMetricsBackend:
    def __init__(self) -> None:
        self.distributions: list[DistributionRecord] = []

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
    ) -> None:
        return None

    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        return None

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.distributions.append(DistributionRecord(name, value, tags, unit))


class FakeResponse:
    def __init__(
        self,
        status: int,
        *,
        data: bytes = b"",
        json_data: dict[str, Any] | None = None,
    ) -> None:
        self.status = status
        self.data = data
        self._json_data = json_data

    def read(self) -> bytes:
        return self.data

    def json(self) -> dict[str, Any]:
        if self._json_data is not None:
            return self._json_data
        return json.loads(self.data.decode("utf-8"))


def test_upload_part_validates_bytes_content_length() -> None:
    client = Client("http://127.0.0.1:8888")
    session = client.session(Usecase("testing"), org=1)
    upload = MultipartUpload(session, "key", "upload-id")

    with pytest.raises(ValueError, match="content_length must match"):
        upload.put_part(b"payload", part_number=1, content_length=1)


def test_multipart_complete_raises_http_errors_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client("http://127.0.0.1:8888")
    session = client.session(Usecase("testing"), org=1)
    upload = MultipartUpload(session, "key", "upload-id")

    monkeypatch.setattr(
        session._pool,
        "request",
        lambda *args, **kwargs: FakeResponse(
            403, data=b'{"detail":"missing or expired auth"}'
        ),
    )

    with pytest.raises(RequestError) as exc_info:
        upload.complete([CompletePart(part_number=1, etag="etag")])

    assert exc_info.value.status == 403
    assert exc_info.value.response == '{"detail":"missing or expired auth"}'


def test_multipart_put_part_metrics_use_distinct_namespace_without_compression_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics_backend = RecordingMetricsBackend()
    client = Client("http://127.0.0.1:8888", metrics_backend=metrics_backend)
    session = client.session(Usecase("testing", compression="none"), org=1)
    responses = iter(
        [
            FakeResponse(200, json_data={"key": "key", "upload_id": "upload-id"}),
            FakeResponse(200, json_data={"etag": "part-etag"}),
        ]
    )

    monkeypatch.setattr(
        session._pool, "request", lambda *args, **kwargs: next(responses)
    )

    upload = session.initiate_multipart_upload(key="key", compression="zstd")
    part = upload.put_part(
        b"compressed", part_number=1, content_length=len(b"compressed")
    )

    assert part.etag == "part-etag"
    size_metrics = [
        record
        for record in metrics_backend.distributions
        if record.name == "storage.multipart.put_part.size"
    ]
    assert size_metrics == [
        DistributionRecord(
            "storage.multipart.put_part.size",
            len(b"compressed"),
            {"usecase": "testing"},
            "byte",
        )
    ]
