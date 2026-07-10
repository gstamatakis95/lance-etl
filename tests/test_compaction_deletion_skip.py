"""Tests for the single-fragment deletion-aware compaction skip check.

Covers the fix to :func:`lance_etl.maintenance.job.compaction_skip_reason`: a single-fragment
dataset carrying soft-deletions above ``materialize_deletions_threshold`` is a genuine
``CompactItself`` candidate in Lance's own planner and must not be short-circuited before
``Compaction.plan`` ever sees it, while a single fragment with no deletions (or a
below-threshold deletion fraction) still ends up doing no rewrite work.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
from conftest import FakeSpark, make_vector_table, write_fragmented_dataset

from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob
from lance_etl.maintenance.job import compaction_skip_reason, plan_one_dataset
from lance_etl.telemetry import Telemetry, TelemetryConfig


def make_config(telemetry_config: TelemetryConfig) -> MaintenanceConfig:
    """Build a maintenance config with the default 0.1 deletion threshold.

    Args:
        telemetry_config: The test telemetry configuration.

    Returns:
        A maintenance configuration with fast commit retries suitable for tests.
    """
    return MaintenanceConfig(telemetry=telemetry_config, commit_backoff_seconds=0.0)


def test_single_fragment_above_threshold_plans_task(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """A single fragment with a deletion fraction above the threshold is not skipped.

    Deleting 20 of 100 rows (20%) exceeds the default 0.1 ``materialize_deletions_threshold``,
    so Lance's planner marks the lone fragment ``CompactItself``. The check must return ``None``
    and the real plan-execute-commit path must reclaim the deleted rows.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        telemetry: A telemetry facade for the plan phase.
    """
    uri: str = str(tmp_path / "above_threshold.lance")
    table: pa.Table = make_vector_table(100, dim=8)
    write_fragmented_dataset(uri, table, max_rows_per_file=1000)
    dataset: lance.LanceDataset = lance.dataset(uri)
    dataset.delete("id < 20")

    fresh: lance.LanceDataset = lance.dataset(uri)
    config: MaintenanceConfig = make_config(telemetry_config)
    assert compaction_skip_reason(fresh) is None

    planned: dict[str, object] = plan_one_dataset(uri, config, None, telemetry)
    assert planned.get("task_jsons")

    MaintenanceJob(config).run(FakeSpark(), [uri])

    compacted: lance.LanceDataset = lance.dataset(uri)
    stats: dict[str, object] = compacted.stats.dataset_stats()
    assert int(stats["num_deleted_rows"]) == 0
    assert compacted.count_rows() == 80


def test_single_fragment_below_threshold_skips(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """A single fragment with a deletion fraction below the threshold yields no rewrite work.

    Deleting 2 of 100 rows (2%) is below the default 0.1 threshold, so
    ``compaction_skip_reason`` still returns ``None`` (the fragment is not blindly skipped) but
    ``Compaction.plan`` produces zero tasks, and ``plan_one_dataset`` reports ``tasks == 0``
    without writing a new data version.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        telemetry: A telemetry facade for the plan phase.
    """
    uri: str = str(tmp_path / "below_threshold.lance")
    table: pa.Table = make_vector_table(100, dim=8)
    write_fragmented_dataset(uri, table, max_rows_per_file=1000)
    dataset: lance.LanceDataset = lance.dataset(uri)
    dataset.delete("id < 2")
    version_before_plan: int = lance.dataset(uri).version

    fresh: lance.LanceDataset = lance.dataset(uri)
    config: MaintenanceConfig = make_config(telemetry_config)
    assert compaction_skip_reason(fresh) is None

    result: dict[str, object] = plan_one_dataset(uri, config, None, telemetry)
    assert int(result.get("tasks", -1)) == 0
    assert "task_jsons" not in result
    assert lance.dataset(uri).version == version_before_plan


def test_single_fragment_no_deletions_skips(tmp_path: Path) -> None:
    """A single fragment with no deletions is skipped with the unchanged message.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    uri: str = str(tmp_path / "no_deletions.lance")
    table: pa.Table = make_vector_table(100, dim=8)
    write_fragmented_dataset(uri, table, max_rows_per_file=1000)

    dataset: lance.LanceDataset = lance.dataset(uri)
    reason: str | None = compaction_skip_reason(dataset)
    assert reason is not None
    assert reason.startswith("only 1 fragment")


def test_multi_fragment_not_skipped(tmp_path: Path) -> None:
    """A multi-fragment dataset is never short-circuited by the skip check.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    uri: str = str(tmp_path / "multi_fragment.lance")
    table: pa.Table = make_vector_table(100, dim=8)
    write_fragmented_dataset(uri, table, max_rows_per_file=10)

    dataset: lance.LanceDataset = lance.dataset(uri)
    assert compaction_skip_reason(dataset) is None
