"""Private, single-request operations for resumable uploads.

Chunks contain the final stored bytes; compression and recovery belong to the caller.
Always continue from the server's authoritative offset, which may acknowledge only
a prefix of the submitted chunk. The upload token is opaque and remains encoded as
received from the server.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import IO, TYPE_CHECKING
from urllib.parse import urlencode

import urllib3

from objectstore_client.errors import RequestError, raise_for_status
from objectstore_client.metrics import measure_storage_operation
from objectstore_client.tracing import storage_span

if TYPE_CHECKING:
    from objectstore_client.client import Session


class ResumableUploadUnavailable(RequestError):
    """The upload session expired, was canceled, or could not be found."""


class UploadOffsetMismatch(RequestError):
    """The server rejected a chunk and reported its authoritative offset."""

    def __init__(self, offset: int, response: urllib3.BaseHTTPResponse):
        super().__init__(
            f"Upload offset mismatch (server holds {offset} bytes)",
            response.status,
            response.data.decode("utf-8", "replace"),
        )
        self.offset = offset


class ChunkTooSmall(ValueError):
    """A known non-final chunk is shorter than the upload granularity."""


@dataclass(frozen=True)
class UploadIncomplete:
    """The upload expects more bytes, starting at the authoritative offset."""

    offset: int


@dataclass(frozen=True)
class UploadComplete:
    """The upload is complete and the object is available through normal reads."""


UploadProgress = UploadIncomplete | UploadComplete


def _parse_progress(response: urllib3.BaseHTTPResponse) -> UploadProgress:
    if response.status == 409:
        raise UploadOffsetMismatch(int(response.headers["Upload-Offset"]), response)
    if response.status in (404, 410):
        raise_for_status(response, error_type=ResumableUploadUnavailable)
    raise_for_status(response)
    if response.status == 201:
        return UploadComplete()
    return UploadIncomplete(int(response.headers["Upload-Offset"]))


class ResumableUpload:
    """A handle bound to one object, scoped session, and resumable upload token."""

    def __init__(
        self,
        session: Session,
        key: str,
        token: str,
        total_length: int | None = None,
        granularity: int | None = None,
    ):
        self._session = session
        self._key = key
        self._token = token
        self._total_length = total_length
        self._granularity = granularity

    @property
    def key(self) -> str:
        return self._key

    @property
    def token(self) -> str:
        return self._token

    @property
    def granularity(self) -> int | None:
        """The persistence unit for non-final chunks, if known; zero means none."""
        return self._granularity

    def progress(self) -> UploadProgress:
        """Query authoritative progress"""
        session = self._session
        query = urlencode({"session": self.token})
        headers = session._make_headers()
        headers["Upload-Offset"] = "*"
        with (
            storage_span(
                "resumable.progress", session._usecase, session._scope, key=self.key
            ),
            measure_storage_operation(
                session._metrics_backend, "resumable.progress", session._usecase.name
            ),
        ):
            response = session._pool.request(
                "PUT",
                f"{session._make_url(self.key)}?{query}",
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            return _parse_progress(response)

    def put(
        self, offset: int, contents: bytes | tuple[IO[bytes], int]
    ) -> UploadProgress:
        """Write a chunk verbatim and return authoritative progress.

        A stream tuple supplies the stream and its exact byte length. The stream
        must yield exactly that many bytes from its current position through EOF.
        The caller is responsible for providing a stream with the declared length.
        Compression, if recorded at creation, applies to the complete object
        before it is split into chunks. Offsets and lengths count compressed bytes.

        Raises `UploadOffsetMismatch` only when the server rejects the offset,
        `ResumableUploadUnavailable` for an unavailable session, or `ChunkTooSmall`
        for a known non-final chunk shorter than the granularity. Zero-length and
        final chunks may be shorter than the granularity.
        """
        if isinstance(contents, bytes):
            body: bytes | IO[bytes] = contents
            length = len(contents)
        else:
            body, length = contents

        if offset < 0 or length < 0:
            raise ValueError("Chunk offset and length must not be negative")
        if (
            self._granularity is not None
            and self._total_length is not None
            and 0 < length < self._granularity
            and offset + length < self._total_length
        ):
            raise ChunkTooSmall(
                f"Non-final chunk {length} is smaller than upload granularity "
                f"{self._granularity}"
            )

        session = self._session
        query = urlencode({"session": self.token})
        headers = session._make_headers()
        headers["Upload-Offset"] = str(offset)
        headers["Content-Length"] = str(length)
        with (
            storage_span(
                "resumable.put",
                session._usecase,
                session._scope,
                key=self.key,
                offset=offset,
                size=length,
            ),
            measure_storage_operation(
                session._metrics_backend, "resumable.put", session._usecase.name
            ) as metrics,
        ):
            response = session._pool.request(
                "PUT",
                f"{session._make_url(self.key)}?{query}",
                headers=headers,
                body=body,
                preload_content=True,
                decode_content=True,
            )
            progress = _parse_progress(response)
            metrics.record_size(length)
            return progress

    def cancel(self) -> None:
        """Discard uploaded bytes; unavailable sessions raise a private exception."""
        session = self._session
        query = urlencode({"session": self.token})
        headers = session._make_headers()
        with (
            storage_span(
                "resumable.cancel", session._usecase, session._scope, key=self.key
            ),
            measure_storage_operation(
                session._metrics_backend, "resumable.cancel", session._usecase.name
            ),
        ):
            response = session._pool.request(
                "DELETE",
                f"{session._make_url(self.key)}?{query}",
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            error_type = (
                ResumableUploadUnavailable
                if response.status in (404, 410)
                else RequestError
            )
            raise_for_status(response, error_type=error_type)
