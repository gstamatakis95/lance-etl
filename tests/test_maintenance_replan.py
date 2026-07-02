"""Tests for the unified compaction's replan-on-commit-conflict rounds.

A commit conflict is deterministic on retry because the conflict scan is pinned to the plan
version, so the productive retry is a fresh plan plus re-execute. These tests cover the
fleet-level replan rounds in :meth:`MaintenanceJob.run` (conflicted datasets re-enter the next
round, exhaustion defers them), the small manifest-race budget inside
:func:`commit_one_dataset`, the binary-copy compaction-mode default, and the version-cleanup
horizon floor. Spark is replaced with a minimal in-process fake since only
``parallelize().map/mapPartitions().collect()`` is exercised.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import lance_etl.maintenance.job as maintenance_job
from lance_etl.maintenance import (
    MaintenanceConfig,
    MaintenanceJob,
    cleanup_dataset,
    commit_one_dataset,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 4096
ROWS_PER_FRAGMENT: int = 512


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
    """Minimal stand-in for a SparkContext."""

    def parallelize(self, items: list[object], slices: int) -> FakeRdd:
        """Wrap items into a fake RDD.

        Args:
            items: The items to distribute.
            slices: Ignored partition count.

        Returns:
            The fake RDD.
        """
        del slices
        return FakeRdd(list(items))


class FakeSpark:
    """Minimal stand-in for a SparkSession."""

    def __init__(self) -> None:
        """Initialize the fake session with its fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()


@pytest.fixture
def dataset_uri(tmp_path: Path) -> str:
    """Write a fragmented local dataset and return its URI.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "replan.lance")
    table: pa.Table = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int64()),
            "payload": pa.array([f"row{i}" for i in range(ROWS)]),
        }
    )
    lance.write_dataset(table, uri, max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def replan_config(telemetry_config: TelemetryConfig, **overrides: object) -> MaintenanceConfig:
    """Build the compaction configuration used by the re-plan tests.

    Args:
        telemetry_config: The test telemetry configuration.
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        A configuration that plans real rewrite tasks for the test dataset.
    """
    base: dict[str, object] = {
        "telemetry": telemetry_config,
        "target_rows_per_fragment": ROWS,
        "num_threads": 1,
        "commit_backoff_seconds": 0.0,
        "replan_budget": 3,
    }
    base.update(overrides)
    return MaintenanceConfig(**base)


def test_run_replans_until_budget_then_defers(
    dataset_uri: str, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every commit conflict re-enters the next round, and exhaustion defers the dataset."""
    config: MaintenanceConfig = replan_config(telemetry_config)
    commits: list[str] = []

    def conflicting_commit(
        uri: str, rewrite_jsons: list[str], cfg: MaintenanceConfig, telemetry: Telemetry
    ) -> dict[str, object]:
        """Always report a semantic commit conflict."""
        del rewrite_jsons, cfg, telemetry
        commits.append(uri)
        return {"uri": uri, "conflict": True}

    monkeypatch.setattr(maintenance_job, "commit_one_dataset", conflicting_commit)
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [dataset_uri])
    assert len(commits) == config.replan_budget
    assert len(results) == 1
    assert "conflicted in all 3" in str(results[0]["skipped"])
    assert "fragments_removed" not in results[0]


def test_run_commits_after_one_conflict(
    dataset_uri: str, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A conflict in the first round is resolved by the second round's fresh plan and commit."""
    config: MaintenanceConfig = replan_config(telemetry_config)
    real_commit = maintenance_job.commit_one_dataset
    commits: list[str] = []

    def commit_once_conflicting(
        uri: str, rewrite_jsons: list[str], cfg: MaintenanceConfig, telemetry: Telemetry
    ) -> dict[str, object]:
        """Conflict on the first attempt, then delegate to the real commit."""
        commits.append(uri)
        if len(commits) == 1:
            return {"uri": uri, "conflict": True}
        return real_commit(uri, rewrite_jsons, cfg, telemetry)

    monkeypatch.setattr(maintenance_job, "commit_one_dataset", commit_once_conflicting)
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [dataset_uri])
    assert len(commits) == 2
    assert results[0]["fragments_removed"] == ROWS // ROWS_PER_FRAGMENT
    assert "skipped" not in results[0]
    assert len(lance.dataset(dataset_uri).get_fragments()) == 1


def test_run_propagates_non_conflict_errors(
    dataset_uri: str, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Errors that are not commit conflicts fail the run instead of re-planning."""

    def broken_commit(
        uri: str, rewrite_jsons: list[str], cfg: MaintenanceConfig, telemetry: Telemetry
    ) -> dict[str, object]:
        """Fail with a non-conflict error."""
        del uri, rewrite_jsons, cfg, telemetry
        raise RuntimeError("schema mismatch: field order differs")

    monkeypatch.setattr(maintenance_job, "commit_one_dataset", broken_commit)
    with pytest.raises(RuntimeError, match="schema mismatch"):
        MaintenanceJob(replan_config(telemetry_config)).run(FakeSpark(), [dataset_uri])


def test_commit_one_dataset_uses_small_budget(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The commit retries only the small large_commit_retries manifest-race budget, then defers.

    Exhaustion surfaces as a ``conflict`` marker rather than an exception, so the fleet run
    re-plans the dataset instead of failing.
    """
    config: MaintenanceConfig = replan_config(telemetry_config, large_commit_retries=2)
    attempts: list[int] = []

    class ConflictingCompaction:
        """Compaction stand-in whose commit always conflicts."""

        @staticmethod
        def commit(dataset: object, rewrites: list[object], options: dict[str, object] | None = None) -> object:
            """Always fail with a retryable commit conflict.

            Args:
                dataset: Ignored dataset handle.
                rewrites: Ignored rewrite results.
                options: Ignored compaction options forwarded at commit time.
            """
            del dataset, rewrites, options
            attempts.append(1)
            raise OSError("LanceError(IO): Retryable commit conflict for version 2")

    monkeypatch.setattr("lance_etl.maintenance.job.Compaction", ConflictingCompaction)
    result: dict[str, object] = commit_one_dataset(dataset_uri, [], config, telemetry)
    assert result == {"uri": dataset_uri, "conflict": True}
    assert len(attempts) == config.large_commit_retries + 1


def test_compaction_mode_defaults_to_try_binary_copy(telemetry_config: TelemetryConfig) -> None:
    """The default execute options carry the binary-copy-with-fallback mode."""
    config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
    assert config.execute_options()["compaction_mode"] == "try_binary_copy"


def test_cleanup_horizon_floor_is_enforced(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Cleanup horizons below the safe floor are rejected before any deletion."""
    config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, cleanup_older_than_seconds=60)
    with pytest.raises(ValueError, match="cleanup_older_than_seconds"):
        cleanup_dataset(dataset_uri, config, telemetry)
    safe: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, cleanup_older_than_seconds=6 * 3600)
    assert cleanup_dataset(dataset_uri, safe, telemetry) >= 0
