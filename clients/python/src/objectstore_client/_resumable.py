"""Private protocol operations for resumable uploads.

Payloads are sent verbatim. Lengths and offsets refer to the complete object's
prepared bytes, after any compression. Recovery and payload preparation belong
to the caller; this module does not replay requests after ambiguous failures.
"""

from __future__ import annotations

from dataclasses import dataclass
from json import JSONDecodeError
from typing import IO, TYPE_CHECKING
from urllib.parse import urlencode

import urllib3

from objectstore_client.errors import RequestError, raise_for_status
from objectstore_client.metadata import Compression, ExpirationPolicy
from objectstore_client.metrics import measure_storage_operation
from objectstore_client.tracing import storage_span

if TYPE_CHECKING:
    from objectstore_client.client import Session

_HEADER_UPLOAD_LENGTH = "Upload-Length"
_HEADER_UPLOAD_OFFSET = "Upload-Offset"
_MAX_U64 = (1 << 64) - 1


class _MalformedResponse(RequestError):
    """A resumable response has an unexpected status, header, or body.

    The request may have succeeded server-side, including publishing the object
    when a completion response could not be parsed.
    """


class _ResumableUploadUnavailable(RequestError):
    """The upload session is missing, expired, or canceled.

    Continuing this session is no longer possible; a new session is required.
    The original HTTP status and response body are preserved.
    """


class _ChunkTooSmall(ValueError):
    """A known non-final chunk is shorter than the upload's granularity."""

    def __init__(self, chunk_length: int, upload_granularity: int):
        super().__init__(
            f"Non-final chunk length {chunk_length} is below upload granularity "
            f"{upload_granularity}"
        )
        self.chunk_length = chunk_length
        self.upload_granularity = upload_granularity


def _validate_length(value: int, name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_U64:
        raise ValueError(f"{name} must be an unsigned 64-bit integer")


def _malformed_response(
    response: urllib3.BaseHTTPResponse, message: str
) -> _MalformedResponse:
    return _MalformedResponse(
        message,
        status=response.status,
        response=(response.data or b"").decode("utf-8", "replace"),
    )


def _parse_response_json(response: urllib3.BaseHTTPResponse, message: str) -> object:
    try:
        return response.json()
    except (JSONDecodeError, UnicodeDecodeError) as error:
        raise _malformed_response(response, message) from error


def _raise_for_upload_status(response: urllib3.BaseHTTPResponse) -> None:
    error_type = (
        _ResumableUploadUnavailable if response.status in (404, 410) else RequestError
    )
    raise_for_status(response, error_type=error_type)


def _create_upload(
    session: Session,
    object_length: int,
    *,
    key: str | None = None,
    compression: Compression | None = None,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
    expiration_policy: ExpirationPolicy | None = None,
    origin: str | None = None,
    filename: str | None = None,
) -> _ResumableUpload | None:
    """Create a private resumable upload, or return None if declined.

    ``object_length`` counts the prepared object's bytes after compression.
    ``compression`` records their encoding; it does not compress payloads
    or inherit the usecase's compression setting. Expiration defaults to the
    usecase policy. Only HTTP 501 declines creation; other HTTP failures raise
    ``RequestError`` and malformed responses raise ``_MalformedResponse``.
    """
    _validate_length(object_length, "object_length")
    if compression is not None and compression not in ("none", "zstd"):
        raise ValueError(f"Invalid compression: {compression}")
    headers = session._make_upload_metadata_headers(
        content_type=content_type,
        expiration_policy=expiration_policy,
        origin=origin,
        filename=filename,
        metadata=metadata,
    )
    headers[_HEADER_UPLOAD_LENGTH] = str(object_length)
    if compression is not None and compression != "none":
        headers["Content-Encoding"] = compression

    key = key or None
    with (
        storage_span(
            "resumable.create", session._usecase, session._scope, key=key
        ) as span,
        measure_storage_operation(
            session._metrics_backend, "resumable.create", session._usecase.name
        ),
    ):
        response = _request(
            session,
            "POST" if key is None else "PUT",
            key,
            {"upload_type": "resumable"},
            headers=headers,
        )
        if response.status == 501:
            return None
        raise_for_status(response)
        if response.status != 200:
            raise _malformed_response(response, "Unexpected upload creation status")
        data = _parse_response_json(response, "Invalid upload creation response")
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("key"), str)
            or not isinstance(data.get("session"), str)
        ):
            raise _malformed_response(response, "Invalid upload creation response")
        granularity = data.get("granularity", 0)
        if type(granularity) is not int or not 0 <= granularity <= _MAX_U64:
            raise _malformed_response(response, "Invalid upload granularity")
        span.set_attribute("objectstore.key", data["key"])
        return _ResumableUpload(
            session, data["key"], data["session"], object_length, granularity
        )


def _request(
    session: Session,
    method: str,
    key: str | None,
    query: dict[str, str],
    *,
    headers: dict[str, str] | None = None,
    body: bytes | IO[bytes] | None = None,
) -> urllib3.BaseHTTPResponse:
    request_headers = session._make_headers()
    request_headers.update(headers or {})
    # Only connection failures are safe to retry without querying progress.
    # Override custom pool settings that could replay a partially sent chunk.
    retries = urllib3.Retry.from_int(session._pool.retries).new(
        read=0,
        status=0,
        other=0,
        status_forcelist=(),
        respect_retry_after_header=False,
    )
    return session._pool.request(
        method,
        f"{session._make_url(key)}?{urlencode(query)}",
        body=body,
        headers=request_headers,
        retries=retries,
        redirect=False,
        preload_content=True,
        decode_content=True,
    )


@dataclass(frozen=True)
class _UploadProgress:
    """Authoritative persisted offset, or completion when the offset is absent.

    An incomplete offset can be below the submitted chunk's end because a
    backend may persist only a prefix. Completion requires an explicit server
    response, rather than an offset equal to the total length.
    """

    offset: int | None

    @property
    def complete(self) -> bool:
        return self.offset is None


def _parse_progress(response: urllib3.BaseHTTPResponse) -> _UploadProgress:
    if response.status in (204, 409):
        value = response.headers.get(_HEADER_UPLOAD_OFFSET, "")
        if not value or not value.isascii() or not value.isdecimal():
            raise _malformed_response(response, "Invalid Upload-Offset header")
        offset = int(value)
        if offset > _MAX_U64:
            raise _malformed_response(response, "Invalid Upload-Offset header")
        # A rejected offset still tells the caller where to continue.
        return _UploadProgress(offset)
    if response.status == 201:
        data = _parse_response_json(response, "Invalid upload completion response")
        if not isinstance(data, dict) or not isinstance(data.get("key"), str):
            raise _malformed_response(response, "Invalid upload completion response")
        return _UploadProgress(None)
    _raise_for_upload_status(response)
    raise _malformed_response(response, "Unexpected resumable upload status")


class _ResumableUpload:
    """A private handle bound to an object and an opaque upload token.

    Created by ``Session._create_upload`` or reconstructed without a network
    call by ``Session._resume_upload``. Reconstructed handles do not know the
    upload's total length or granularity; the server validates their chunks.
    """

    def __init__(
        self,
        session: Session,
        key: str,
        token: str,
        total_length: int | None = None,
        granularity: int | None = None,
    ):
        self._session = session
        self.key = key
        self.token = token
        self._total_length = total_length
        self.granularity = granularity

    def progress(self) -> _UploadProgress:
        """Query authoritative progress, including completion if still observable.

        Missing or expired sessions raise ``_ResumableUploadUnavailable``.
        Malformed responses raise ``_MalformedResponse``; other HTTP failures
        raise ``RequestError``.
        """
        with (
            storage_span(
                "resumable.progress",
                self._session._usecase,
                self._session._scope,
                key=self.key,
            ),
            measure_storage_operation(
                self._session._metrics_backend,
                "resumable.progress",
                self._session._usecase.name,
            ),
        ):
            response = _request(
                self._session,
                "PUT",
                self.key,
                {"session": self.token},
                headers={_HEADER_UPLOAD_OFFSET: "*"},
            )
            return _parse_progress(response)

    def put(self, offset: int, chunk: bytes | tuple[IO[bytes], int]) -> _UploadProgress:
        """Send a chunk verbatim and return authoritative progress.

        Byte payloads supply their own length. A stream must yield exactly its
        declared length from its current position; it is neither bounded nor
        rewound by this handle. Compression must be applied to the whole object
        before dividing it into chunks.

        Invalid offsets/lengths raise ``ValueError``; known non-final chunks below
        upload granularity raise ``_ChunkTooSmall`` (a ``ValueError`` subclass).
        HTTP 409 is normalized into incomplete progress. Missing or expired
        sessions raise ``_ResumableUploadUnavailable``; malformed responses raise
        ``_MalformedResponse``; other HTTP failures raise ``RequestError``.
        A malformed completion response does not imply the object was not saved.
        """
        _validate_length(offset, "offset")
        if isinstance(chunk, bytes):
            body: bytes | IO[bytes] = chunk
            length = len(chunk)
        else:
            body, length = chunk
        _validate_length(length, "chunk length")
        if (
            self.granularity is not None
            and self._total_length is not None
            and 0 < length < self.granularity
            and offset + length < self._total_length
        ):
            raise _ChunkTooSmall(length, self.granularity)

        with (
            storage_span(
                "resumable.put",
                self._session._usecase,
                self._session._scope,
                key=self.key,
                offset=offset,
                size=length,
            ),
            measure_storage_operation(
                self._session._metrics_backend,
                "resumable.put",
                self._session._usecase.name,
            ) as metrics,
        ):
            response = _request(
                self._session,
                "PUT",
                self.key,
                {"session": self.token},
                headers={
                    _HEADER_UPLOAD_OFFSET: str(offset),
                    "Content-Length": str(length),
                },
                body=body,
            )
            progress = _parse_progress(response)
            metrics.record_size(length)
            return progress

    def cancel(self) -> None:
        """Discard this upload's bytes.

        Missing or expired sessions raise ``_ResumableUploadUnavailable``.
        Malformed responses raise ``_MalformedResponse``; other HTTP failures
        raise ``RequestError``.
        """
        with (
            storage_span(
                "resumable.cancel",
                self._session._usecase,
                self._session._scope,
                key=self.key,
            ),
            measure_storage_operation(
                self._session._metrics_backend,
                "resumable.cancel",
                self._session._usecase.name,
            ),
        ):
            response = _request(
                self._session, "DELETE", self.key, {"session": self.token}
            )
            _raise_for_upload_status(response)
            if response.status != 204:
                raise _malformed_response(
                    response, "Unexpected upload cancellation status"
                )
