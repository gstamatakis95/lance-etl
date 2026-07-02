"""Shared fixtures for the lance-etl test suite.

Disables ddtrace agent flushing before any module under test imports ddtrace, and provides telemetry and dataset-builder
fixtures used across the suite. Also puts the repository root on ``sys.path`` so the un-packaged top-level ``bench``
benchmarking package is importable by the ``test_bench_*`` modules.
"""

from __future__ import annotations

import os
import random
import sys
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

os.environ.setdefault("DD_TRACE_ENABLED", "false")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lance
import pyarrow as pa
import pytest
from lance.optimize import Compaction

from lance_etl.maintenance import MaintenanceConfig, cleanup_dataset, compaction_metrics_dict
from lance_etl.telemetry import Telemetry, TelemetryConfig, commit_with_retries


@pytest.fixture
def telemetry_config() -> TelemetryConfig:
    """Return a telemetry configuration pointing at a local statsd sink.

    Returns:
        A default telemetry configuration. DogStatsD sends are fire-and-forget
        UDP so no agent needs to listen.
    """
    return TelemetryConfig(service="lance-etl-tests", env="test")


@pytest.fixture
def telemetry(telemetry_config: TelemetryConfig) -> Telemetry:
    """Return a telemetry facade without the Lance event bridge.

    Args:
        telemetry_config: The test telemetry configuration.

    Returns:
        A telemetry facade safe to use offline.
    """
    return Telemetry.create(telemetry_config, attach_lance_bridge=False)


def make_vector_table(rows: int, dim: int, seed: int = 7) -> pa.Table:
    """Build a table with id, vector, category, and text columns.

    Args:
        rows: Number of rows to generate.
        dim: Fixed-size-list vector dimension.
        seed: Random seed for reproducible vectors.

    Returns:
        The generated table.
    """
    rng: random.Random = random.Random(seed)
    values: pa.Array = pa.array([rng.random() for _ in range(rows * dim)], pa.float32())
    vectors: pa.Array = pa.FixedSizeListArray.from_arrays(values, dim)
    return pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "vector": vectors,
            "category": pa.array([f"cat{i % 4}" for i in range(rows)]),
            "text": pa.array([f"word{i % 10} common" for i in range(rows)]),
        }
    )


def write_fragmented_dataset(uri: str, table: pa.Table, max_rows_per_file: int) -> lance.LanceDataset:
    """Write a table to a Lance dataset split into multiple fragments.

    Args:
        uri: Destination dataset URI.
        table: The table to write.
        max_rows_per_file: Row cap per fragment file, controlling fragment count.

    Returns:
        The written dataset handle.
    """
    return lance.write_dataset(table, uri, max_rows_per_file=max_rows_per_file)


def compact_dataset_inline(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> dict[str, int]:
    """Compact one dataset fully in-process, a test-only harness for the concurrency suites.

    Production compaction always runs the fleet plan-execute-commit phases on Spark. The
    concurrency tests need a compaction they can race against merges and index builds from a
    plain thread without a Spark session, so this helper runs ``Compaction.execute`` (the same
    plan-execute-commit cycle in one process) with the production conflict-retry wrapper and the
    production version cleanup.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the calling thread.

    Returns:
        The compaction metrics merged with ``uri``, ``tasks``, and ``bytes_removed``.
    """

    def action() -> dict[str, int]:
        """Run the whole compaction against the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        metrics = Compaction.execute(dataset, config.execute_options())
        return compaction_metrics_dict(metrics)

    metrics: dict[str, int] = commit_with_retries(action, config.commit_retries, config.commit_backoff_seconds, None)
    bytes_removed: int = cleanup_dataset(uri, config, telemetry)
    result: dict[str, int] = {"tasks": 1, "bytes_removed": bytes_removed}
    result.update(metrics)
    return result


class FakeBroadcast:
    """Minimal stand-in for a Spark broadcast variable."""

    def __init__(self, value: object) -> None:
        """Wrap the broadcast value.

        Args:
            value: The value to expose.
        """
        self.value: object = value


class FakeRdd:
    """Minimal stand-in for a Spark RDD running everything eagerly in process."""

    def __init__(self, items: list[object]) -> None:
        """Initialize the fake RDD.

        Args:
            items: The partitioned items.
        """
        self.items: list[object] = items

    def map(self, fn: Callable[[object], object]) -> FakeRdd:
        """Apply a function to every item eagerly.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return FakeRdd([fn(item) for item in self.items])

    def mapPartitions(self, fn: Callable[[Iterator[object]], Iterator[object]]) -> FakeRdd:
        """Apply a partition function to the single in-process partition.

        Args:
            fn: The partition mapper yielding outputs.

        Returns:
            A new fake RDD with the collected outputs.
        """
        return FakeRdd(list(fn(iter(self.items))))

    def collect(self) -> list[object]:
        """Return the items.

        Returns:
            The current items.
        """
        return list(self.items)


class FakeSparkContext:
    """Minimal stand-in for a SparkContext with broadcast support."""

    def parallelize(self, items: Iterable[object], slices: int) -> FakeRdd:
        """Wrap items into a fake RDD.

        Args:
            items: The items to distribute.
            slices: Ignored partition count.

        Returns:
            The fake RDD.
        """
        del slices
        return FakeRdd(list(items))

    def broadcast(self, value: object) -> FakeBroadcast:
        """Wrap a value into a fake broadcast.

        Args:
            value: The value to broadcast.

        Returns:
            The fake broadcast handle.
        """
        return FakeBroadcast(value)


class FakeSpark:
    """Minimal stand-in for a SparkSession driving fan-outs in the driver process."""

    def __init__(self) -> None:
        """Initialize the fake session with its fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()
