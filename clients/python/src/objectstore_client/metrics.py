from __future__ import annotations

import time
from abc import abstractmethod
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Protocol, runtime_checkable

Tags = Mapping[str, str]


@runtime_checkable
class MetricsBackend(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        """
        Sets a gauge metric to the given value.
        """
        raise NotImplementedError

    @abstractmethod
    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        """
        Records a distribution metric.
        """
        raise NotImplementedError


class NoOpMetricsBackend(MetricsBackend):
    """
    Default metrics backend that does not record anything.
    """

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
    ) -> None:
        pass

    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        pass

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        pass


class StorageMetricEmitter:
    def __init__(self, backend: MetricsBackend, operation: str, usecase: str):
        self.backend = backend
        self.operation = operation
        self.usecase = usecase

        # These may be set during or after the enclosed operation
        self.start: int | None = None
        self.elapsed: float | None = None
        self.uncompressed_size: int | None = None
        self.compressed_size: int | None = None
        self.compression: str = "unknown"

    def record_latency(self, elapsed: float) -> None:
        tags = {"usecase": self.usecase}
        self.backend.distribution(
            f"storage.{self.operation}.latency", elapsed, tags=tags
        )
        self.elapsed = elapsed

    def record_uncompressed_size(self, value: int) -> None:
        tags = {"usecase": self.usecase, "compression": "none"}
        self.backend.distribution(
            f"storage.{self.operation}.size", value, tags=tags, unit="byte"
        )
        self.uncompressed_size = value

    def record_compressed_size(self, value: int, compression: str = "unknown") -> None:
        tags = {"usecase": self.usecase, "compression": compression}
        self.backend.distribution(
            f"storage.{self.operation}.size", value, tags=tags, unit="byte"
        )
        self.compressed_size = value
        self.compression = compression

    def maybe_record_compression_ratio(self) -> None:
        if not self.uncompressed_size or not self.compressed_size:
            return None

        tags = {"usecase": self.usecase, "compression": self.compression}
        self.backend.distribution(
            f"storage.{self.operation}.compression_ratio",
            self.compressed_size / self.uncompressed_size,
            tags=tags,
        )

    def maybe_record_throughputs(self) -> None:
        if not self.elapsed or self.elapsed <= 0:
            return None

        sizes = []
        if self.uncompressed_size:
            sizes.append((self.uncompressed_size, "none"))
        if self.compressed_size:
            sizes.append((self.compressed_size, self.compression))

        for size, compression in sizes:
            tags = {"usecase": self.usecase, "compression": compression}
            self.backend.distribution(
                f"storage.{self.operation}.throughput", size / self.elapsed, tags=tags
            )
            self.backend.distribution(
                f"storage.{self.operation}.inverse_throughput",
                self.elapsed / size,
                tags=tags,
            )


@contextmanager
def measure_storage_operation(
    backend: MetricsBackend,
    operation: str,
    usecase: str,
    uncompressed_size: int | None = None,
    compressed_size: int | None = None,
    compression: str = "unknown",
) -> Generator[StorageMetricEmitter]:
    """
    Context manager which records the latency of the enclosed storage operation.
    Can also record the compressed or uncompressed size of an object, the
    compression ratio, the throughput, and the inverse throughput.

    Yields a `StorageMetricEmitter` because for some operations (GET) the size
    is not known until the inside of the enclosed block.
    """
    emitter = StorageMetricEmitter(backend, operation, usecase)

    if uncompressed_size:
        emitter.record_uncompressed_size(uncompressed_size)
    if compressed_size:
        emitter.record_compressed_size(compressed_size, compression)

    start = time.monotonic()

    # Yield an emitter in case the size becomes known inside the enclosed block
    try:
        yield emitter

    finally:
        elapsed = time.monotonic() - start
        emitter.record_latency(elapsed)

        # If `uncompressed_size` and/or `compressed_size` have been set, we have
        # extra metrics we can send.
        emitter.maybe_record_compression_ratio()
        emitter.maybe_record_throughputs()
