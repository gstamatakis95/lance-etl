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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

os.environ.setdefault("DD_TRACE_ENABLED", "false")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lance
import pyarrow as pa
import pytest
from lance.optimize import Compaction
from pyspark import SparkContext

from lance_etl.maintenance import MaintenanceConfig, cleanup_dataset, compaction_metrics_dict
from lance_etl.telemetry import Telemetry, TelemetryConfig, commit_with_retries


@pytest.fixture
def fresh_spark_gateway() -> None:
    """Require a fresh process before a test resolves JVM-launch Spark packages.

    Iceberg catalog classes supplied by ``spark.jars.packages`` must be present when PySpark
    launches its JVM gateway. This fixture is the single documented test bridge to PySpark's
    gateway state and skips package-dependent tests after another module has launched Spark.
    """
    if getattr(SparkContext, "_gateway", None) is not None:
        pytest.skip("Iceberg integration needs its own pytest process before any Spark gateway launches")


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


@dataclass
class FakeBroadcast:
    """Minimal stand-in for a Spark broadcast variable."""

    value: object
    destroyed: bool = False
    destroy_blocking: bool | None = None

    def destroy(self, blocking: bool = False) -> None:
        """Record broadcast destruction and whether the caller waited for executor cleanup.

        Args:
            blocking: Whether destruction waits for executor-side copies to be removed.
        """
        self.destroyed = True
        self.destroy_blocking = blocking


@dataclass
class FakeRdd:
    """Minimal stand-in for a Spark RDD running everything eagerly in process."""

    items: list[object]

    def map(self, fn: Callable[[object], object]) -> FakeRdd:
        """Apply a function to every item eagerly.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return FakeRdd([fn(item) for item in self.items])

    def flatMap(self, fn: Callable[[object], Iterable[object]]) -> FakeRdd:
        """Apply a function and flatten its outputs eagerly.

        Args:
            fn: Flat mapper.

        Returns:
            A new fake RDD containing every yielded item.
        """
        return FakeRdd([result for item in self.items for result in fn(item)])

    def repartition(self, numPartitions: int) -> FakeRdd:
        """Return the same eager items after validating the requested width.

        Args:
            numPartitions: Positive simulated partition count.

        Returns:
            A new fake RDD with the same items.
        """
        if numPartitions < 1:
            raise ValueError("partition count must be positive")
        return FakeRdd(list(self.items))

    def mapPartitions(self, fn: Callable[[Iterator[object]], Iterator[object]]) -> FakeRdd:
        """Apply a partition function to the single in-process partition.

        Args:
            fn: The partition mapper yielding outputs.

        Returns:
            A new fake RDD with the collected outputs.
        """
        return FakeRdd(list(fn(iter(self.items))))

    def partitionBy(self, numPartitions: int, partitionFunc: Callable[[object], int] | None = None) -> FakeRdd:
        """Order keyed items by their simulated shuffle partition.

        The following ``mapPartitions`` still sees one in-process iterator, but ordering by the
        requested partition keeps injectively partitioned keys contiguous and preserves the
        streaming contract of the production shuffle.

        Args:
            numPartitions: Positive simulated output partition count.
            partitionFunc: Optional key-to-partition function.

        Returns:
            A new fake RDD ordered by simulated partition.
        """
        if numPartitions < 1:
            raise ValueError("partition count must be positive")
        partitioner: Callable[[object], int] = partitionFunc or hash
        return FakeRdd(sorted(self.items, key=lambda item: partitioner(item[0]) % numPartitions))

    def repartitionAndSortWithinPartitions(
        self,
        numPartitions: int,
        partitionFunc: Callable[[object], int] | None = None,
    ) -> FakeRdd:
        """Partition keyed items and sort keys within each simulated partition.

        Args:
            numPartitions: Positive simulated output partition count.
            partitionFunc: Optional key-to-partition function.

        Returns:
            A new fake RDD ordered by partition and then by key.
        """
        if numPartitions < 1:
            raise ValueError("partition count must be positive")
        partitioner: Callable[[object], int] = partitionFunc or hash
        return FakeRdd(
            sorted(
                self.items,
                key=lambda item: (partitioner(item[0]) % numPartitions, item[0]),
            )
        )

    def reduceByKey(self, fn: Callable[[Any, Any], Any], numPartitions: int) -> FakeRdd:
        """Reduce keyed values eagerly with an associative function.

        Args:
            fn: Associative value reducer.
            numPartitions: Positive requested output width.

        Returns:
            One key-value item per distinct key.
        """
        if numPartitions < 1:
            raise ValueError("partition count must be positive")
        reduced: dict[Any, Any] = {}
        for key, value in self.items:
            reduced[key] = fn(reduced[key], value) if key in reduced else value
        return FakeRdd(list(reduced.items()))

    def collect(self) -> list[object]:
        """Return the items.

        Returns:
            The current items.
        """
        return list(self.items)


class FakeSparkContext:
    """Minimal stand-in for a SparkContext with broadcast support."""

    defaultParallelism: int = 4
    """Fixed stand-in core count so derive_partitions has a small, deterministic cluster size."""

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


@dataclass
class FakeSpark:
    """Minimal stand-in for a SparkSession driving fan-outs in the driver process."""

    sparkContext: FakeSparkContext = field(default_factory=FakeSparkContext)

    def stop(self) -> None:
        """No-op session teardown, so callers built around a real SparkSession's lifecycle work unchanged."""
