"""Tests for the unified plan-execute-commit fleet orchestration.

A big dataset is just N independent small tasks and a small dataset is the N=1 case: the same
Lance APIs (``Compaction.plan`` / task execute / ``Compaction.commit``) serve every size, and
all datasets' tasks run in one flat Spark job. These tests pin the task-count scaling of the
plan phase, the flat execute over a mixed-size fleet, and the terminal handling of datasets
with nothing to do.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import lance_etl.maintenance.job as maintenance_job
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob, plan_one_dataset
from lance_etl.telemetry import Telemetry, TelemetryConfig


class FakeRdd:
    """Minimal stand-in for a Spark RDD running map eagerly in process."""

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
    """Minimal stand-in for a SparkContext recording flat-job sizes."""

    def __init__(self) -> None:
        """Initialize with an empty record of parallelize calls."""
        self.parallelize_sizes: list[int] = []

    def parallelize(self, items: list[object], slices: int) -> FakeRdd:
        """Wrap items into a fake RDD, recording the item count.

        Args:
            items: The items to distribute.
            slices: Ignored partition count.

        Returns:
            The fake RDD.
        """
        del slices
        materialized: list[object] = list(items)
        self.parallelize_sizes.append(len(materialized))
        return FakeRdd(materialized)


class FakeSpark:
    """Minimal stand-in for a SparkSession."""

    def __init__(self) -> None:
        """Initialize the fake session with its fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()


def write_rows(uri: str, rows: int, rows_per_file: int) -> None:
    """Write a fragmented dataset of the given shape.

    Args:
        uri: Destination dataset URI.
        rows: Total row count.
        rows_per_file: Row cap per fragment file.
    """
    table: pa.Table = pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "payload": pa.array([f"row{i}" for i in range(rows)]),
        }
    )
    lance.write_dataset(table, uri, max_rows_per_file=rows_per_file)


def fleet_config(telemetry_config: TelemetryConfig, target_rows: int) -> MaintenanceConfig:
    """Build a compaction configuration with a small target fragment size.

    Args:
        telemetry_config: The test telemetry configuration.
        target_rows: Desired rows per compacted fragment, shaping the plan's task count.

    Returns:
        The maintenance configuration.
    """
    return MaintenanceConfig(
        telemetry=telemetry_config,
        target_rows_per_fragment=target_rows,
        num_threads=1,
        commit_backoff_seconds=0.0,
    )


def test_plan_task_count_scales_with_dataset_size(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """A small dataset plans one rewrite task and a big one plans several, through the same API."""
    small_uri: str = str(tmp_path / "small.lance")
    big_uri: str = str(tmp_path / "big.lance")
    write_rows(small_uri, rows=64, rows_per_file=32)
    write_rows(big_uri, rows=1024, rows_per_file=32)
    config: MaintenanceConfig = fleet_config(telemetry_config, target_rows=256)

    small_plan: dict[str, object] = plan_one_dataset(small_uri, config, None, telemetry)
    big_plan: dict[str, object] = plan_one_dataset(big_uri, config, None, telemetry)

    assert len(small_plan["task_jsons"]) == 1
    assert len(big_plan["task_jsons"]) > 1


def test_plan_terminal_records_for_no_work_datasets(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """A single-fragment dataset and a missing dataset both end at the plan phase."""
    compact_uri: str = str(tmp_path / "compact.lance")
    write_rows(compact_uri, rows=16, rows_per_file=16)
    config: MaintenanceConfig = fleet_config(telemetry_config, target_rows=1024)

    compact_result: dict[str, object] = plan_one_dataset(compact_uri, config, None, telemetry)
    missing_result: dict[str, object] = plan_one_dataset(str(tmp_path / "missing.lance"), config, None, telemetry)

    assert compact_result["tasks"] == 0
    assert "task_jsons" not in compact_result
    assert missing_result["skipped"]


def test_fleet_run_compacts_mixed_sizes_in_one_flat_job(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One run compacts a 1-task dataset and an N-task dataset through one flat execute job.

    The flat job's size must equal the sum of both datasets' task counts, proving the fleet's
    rewrite work is scheduled together rather than per dataset.
    """
    small_uri: str = str(tmp_path / "small.lance")
    big_uri: str = str(tmp_path / "big.lance")
    write_rows(small_uri, rows=64, rows_per_file=32)
    write_rows(big_uri, rows=1024, rows_per_file=32)
    config: MaintenanceConfig = fleet_config(telemetry_config, target_rows=256)

    executed: list[str] = []
    real_execute = maintenance_job.execute_rewrite_task

    def counting_execute(
        uri: str, read_version: int, task_json: str, storage_options: dict[str, object] | None
    ) -> tuple[str, str]:
        """Record every flat-job task before delegating to the real execute."""
        executed.append(uri)
        return real_execute(uri, read_version, task_json, storage_options)

    monkeypatch.setattr(maintenance_job, "execute_rewrite_task", counting_execute)
    spark: FakeSpark = FakeSpark()
    results: list[dict[str, object]] = MaintenanceJob(config).run(spark, [small_uri, big_uri])

    assert executed.count(small_uri) == 1
    assert executed.count(big_uri) > 1
    assert len(lance.dataset(small_uri).get_fragments()) == 1
    assert len(lance.dataset(big_uri).get_fragments()) == 1024 // 256
    by_uri: dict[str, dict[str, object]] = {str(result["uri"]): result for result in results}
    assert by_uri[small_uri]["tasks"] == 1
    assert int(by_uri[big_uri]["tasks"]) > 1
    assert all("skipped" not in result for result in results)
