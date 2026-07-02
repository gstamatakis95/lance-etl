"""Shared per-dataset Spark fan-out used by every fleet job.

One embarrassingly parallel shape covers the plan and commit phases of the maintenance and
indexing jobs plus the operator tools (manifest migration, serving-tag flips): apply an
independent per-dataset callable across executors, one telemetry facade per task.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig


def fan_out_per_dataset(
    spark: SparkSession,
    uris: list[str],
    telemetry_config: TelemetryConfig,
    per_dataset: Callable[[str, Telemetry], dict[str, Any]],
    partitions: int,
) -> list[dict[str, Any]]:
    """Run an independent per-dataset operation across executors, one task per partition.

    Each executor task creates its own telemetry facade and applies ``per_dataset`` to every URI
    in its partition. Used by the maintenance and indexing plan and commit fan-outs, the
    manifest migration, and the serving-tag flip, all of which are embarrassingly parallel
    one-call-per-dataset operations that differ only in the per-dataset callable.

    An empty ``uris`` list is returned immediately without submitting a Spark job, because
    ``sparkContext.parallelize`` with zero slices raises a Java exception.

    Args:
        spark: Active Spark session.
        uris: Dataset URIs to process.
        telemetry_config: Telemetry configuration created per executor process.
        per_dataset: The operation to apply to one URI with an executor-local telemetry facade.
        partitions: Upper bound on Spark partitions, capped at the URI count.

    Returns:
        One outcome dictionary per dataset.
    """
    if not uris:
        return []

    def partition(part: Iterable[str]) -> Iterator[dict[str, Any]]:
        """Apply the operation to one partition of dataset URIs on an executor.

        Args:
            part: Dataset URIs assigned to this executor task.

        Yields:
            One outcome dictionary per dataset.
        """
        executor_telemetry: Telemetry = Telemetry.create(telemetry_config)
        for uri in part:
            yield per_dataset(uri, executor_telemetry)

    return spark.sparkContext.parallelize(uris, min(len(uris), partitions)).mapPartitions(partition).collect()
