from __future__ import annotations

import random
import time
from dataclasses import dataclass
from io import SEEK_END
from typing import IO, TYPE_CHECKING
from urllib.parse import urlencode

import urllib3

from objectstore_client.errors import RequestError, raise_for_status
from objectstore_client.metadata import Compression, ExpirationPolicy
from objectstore_client.metrics import measure_storage_operation
from objectstore_client.tracing import storage_span

if TYPE_CHECKING:
    from objectstore_client.client import Session


class ResumableUploadUnavailable(RequestError):
    """The upload session expired, was canceled, or could not be found."""


class UploadOffsetMismatch(RequestError):
    """The server rejected a chunk and reported its offset."""

    def __init__(self, offset: int, response: urllib3.BaseHTTPResponse):
        super().__init__(
            f"Upload offset mismatch (server holds {offset} bytes)",
            response.status,
            response.data.decode("utf-8", "replace"),
        )
        self.offset = offset


class ChunkTooSmall(ValueError):
    """A non-final chunk is shorter than the upload granularity."""


@dataclass(frozen=True)
class UploadIncomplete:
    """The upload expects more bytes, starting at the given offset."""

    offset: int


@dataclass(frozen=True)
class UploadComplete:
    """The upload is complete."""


UploadProgress = UploadIncomplete | UploadComplete


def parse_progress_response(response: urllib3.BaseHTTPResponse) -> UploadProgress:
    if response.status == 409:
        raise UploadOffsetMismatch(int(response.headers["Upload-Offset"]), response)
    if response.status in (404, 410):
        raise_for_status(response, error_type=ResumableUploadUnavailable)
    raise_for_status(response)
    if response.status == 201:
        return UploadComplete()
    return UploadIncomplete(int(response.headers["Upload-Offset"]))


class ResumableUpload:
    """A handle bound to one resumable upload session."""

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
        """The persistence unit for non-final chunks, if known."""
        return self._granularity

    def progress(self) -> UploadProgress:
        """Query the server's authoritative offset.

        Raises `ResumableUploadUnavailable` for an unavailable session.
        """
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
            return parse_progress_response(response)

    def put(
        self, offset: int, contents: bytes | tuple[IO[bytes], int]
    ) -> UploadProgress:
        """Uploads `contents` starting at `offset`.

        For streaming payloads, supply a tuple of the stream and its exact length
        in bytes.

        This method doesn't perform any automatic compression of the payload, so
        the caller is responsible for applying compression to the entire payload
        beforehand and passing chunks of the already compressed payload.

        Raises `UploadOffsetMismatch` when the server rejects the offset,
        `ResumableUploadUnavailable` for an unavailable session, or `ChunkTooSmall`
        for a non-final chunk shorter than the granularity.
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
        # Disable pool retries that replay the body. The resumable upload loop
        # uses the Usecase's recovery policy and probes the persisted offset
        # before resending data; connection retries retain the pool's policy.
        retries = urllib3.Retry.from_int(session._pool.retries).new(
            read=0, status=0, other=0, raise_on_status=False
        )
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
                retries=retries,
                preload_content=True,
                decode_content=True,
            )
            progress = parse_progress_response(response)
            metrics.record_size(length)
            return progress

    def cancel(self) -> None:
        """Cancels the upload session, discarding uploaded bytes."""
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


def get_size(contents: bytes | IO[bytes]) -> int | None:
    if isinstance(contents, bytes):
        return len(contents)
    try:
        if not contents.seekable():
            return None
        start = contents.tell()
    except (OSError, ValueError):
        return None
    try:
        end = contents.seek(0, SEEK_END)
    except (OSError, ValueError):
        return None
    finally:
        contents.seek(start)
    return max(0, end - start)


def is_transient(error: Exception) -> bool:
    if isinstance(error, urllib3.exceptions.MaxRetryError):
        # Exhausted status retries carry ResponseError rather than the response.
        return isinstance(error.reason, urllib3.exceptions.ResponseError) or (
            isinstance(error.reason, Exception) and is_transient(error.reason)
        )
    if isinstance(error, RequestError):
        return error.status in (408, 429, 502, 503, 504)
    return isinstance(
        error,
        (
            urllib3.exceptions.ReadTimeoutError,
            urllib3.exceptions.ProtocolError,
        ),
    )


def upload(
    session: Session,
    body: IO[bytes],
    encoded_size: int,
    key: str | None = None,
    compression: Compression | None = None,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
    expiration_policy: ExpirationPolicy | None = None,
    origin: str | None = None,
    filename: str | None = None,
) -> str | None:
    policy = session._usecase._resumable_retries
    start = body.tell()
    try:
        handle = session._create_upload(
            encoded_size,
            key=key,
            compression=compression,
            content_type=content_type,
            metadata=metadata,
            expiration_policy=expiration_policy,
            origin=origin,
            filename=filename,
        )
    except Exception:
        handle = None
    if handle is None:
        return None

    try:
        offset = 0
        retries = 0
        probe = False
        while True:
            try:
                if probe:
                    result = handle.progress()
                else:
                    body.seek(start + offset)
                    result = handle.put(offset, (body, encoded_size - offset))
            except UploadOffsetMismatch as error:
                result = UploadIncomplete(error.offset)
            except Exception as error:
                if not is_transient(error) or retries >= policy.retries:
                    raise
                time.sleep(policy.delay * 2**retries + random.uniform(0, policy.jitter))
                retries += 1
                probe = True
                continue

            if isinstance(result, UploadComplete):
                return handle.key
            if not offset <= result.offset <= encoded_size:
                raise ValueError("Invalid upload offset")
            if result.offset == offset and not probe:
                raise ValueError("Upload made no progress")
            offset = result.offset
            probe = False
    except Exception as error:
        status = error.status if isinstance(error, RequestError) else None
        response = error.response if isinstance(error, RequestError) else None
        raise RequestError("upload failed", status, response) from error
