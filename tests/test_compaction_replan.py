"""Tests for tier-B compaction coexistence behavior.

Covers the re-plan-on-commit-conflict loop (a tier-B commit conflict is deterministic on retry because the conflict
scan is pinned to the plan version, so the productive retry is plan plus re-execute), the hot-dataset skip after the
re-plan budget, the binary-copy compaction-mode default and its force-mode rejection, and the version-cleanup horizon
floor. Spark is replaced with a minimal in-process fake since only ``parallelize().map().collect()`` and scheduler-pool
properties are exercised.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.compaction import (
    MIN_CLEANUP_HORIZON_SECONDS,
    CompactionConfig,
    LanceCompactor,
    cleanup_dataset,
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

    def setLocalProperty(self, key: str, value: str | None) -> None:
        """Accept and ignore scheduler-pool properties.

        Args:
            key: The property name.
            value: The property value.
        """
        del key, value


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


def replan_config(telemetry_config: TelemetryConfig, **overrides: object) -> CompactionConfig:
    """Build the tier-B compaction configuration used by the re-plan tests.

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
        "run_cleanup": False,
        "commit_backoff_seconds": 0.0,
        "replan_budget": 3,
    }
    base.update(overrides)
    return CompactionConfig(**base)


def test_compact_one_replans_until_budget_then_skips(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every commit conflict triggers a fresh plan, and exhaustion skips the dataset."""
    config: CompactionConfig = replan_config(telemetry_config)
    compactor: LanceCompactor = LanceCompactor(config)
    plans: list[int] = []

    def fake_execute_plan(
        self: LanceCompactor, spark: object, uri: str, plan_version: int, task_jsons: list[str]
    ) -> list[str]:
        """Record the cycle instead of executing rewrite tasks."""
        del self, spark, uri, task_jsons
        plans.append(plan_version)
        return []

    def conflicting_commit(
        self: LanceCompactor, uri: str, rewrite_jsons: list[str], telemetry: Telemetry
    ) -> dict[str, int]:
        """Always fail with a retryable commit conflict."""
        del self, uri, rewrite_jsons, telemetry
        raise RuntimeError("Retryable commit conflict for version 9: compaction lost the race")

    monkeypatch.setattr(LanceCompactor, "execute_plan", fake_execute_plan)
    monkeypatch.setattr(LanceCompactor, "commit_rewrites", conflicting_commit)
    result: dict[str, object] = compactor.compact_one(FakeSpark(), dataset_uri, telemetry)
    assert len(plans) == config.replan_budget
    assert "skipped" in result
    assert result["tier"] == "large"
    assert "fragments_removed" not in result


def test_compact_one_succeeds_after_one_replan(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A conflict on the first cycle is resolved by the second plan's commit."""
    config: CompactionConfig = replan_config(telemetry_config)
    compactor: LanceCompactor = LanceCompactor(config)
    commits: list[int] = []

    def fake_execute_plan(
        self: LanceCompactor, spark: object, uri: str, plan_version: int, task_jsons: list[str]
    ) -> list[str]:
        """Skip the executor fan-out."""
        del self, spark, uri, plan_version, task_jsons
        return []

    def commit_once_conflicting(
        self: LanceCompactor, uri: str, rewrite_jsons: list[str], telemetry: Telemetry
    ) -> dict[str, int]:
        """Conflict on the first attempt, then succeed."""
        del self, uri, rewrite_jsons, telemetry
        commits.append(1)
        if len(commits) == 1:
            raise OSError("LanceError(IO): Retryable commit conflict for version 4")
        return {"fragments_removed": 8, "fragments_added": 1, "files_removed": 8, "files_added": 1}

    monkeypatch.setattr(LanceCompactor, "execute_plan", fake_execute_plan)
    monkeypatch.setattr(LanceCompactor, "commit_rewrites", commit_once_conflicting)
    result: dict[str, object] = compactor.compact_one(FakeSpark(), dataset_uri, telemetry)
    assert len(commits) == 2
    assert result["fragments_removed"] == 8
    assert "skipped" not in result


def test_compact_one_propagates_non_conflict_errors(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Errors that are not commit conflicts fail the dataset instead of re-planning."""
    compactor: LanceCompactor = LanceCompactor(replan_config(telemetry_config))

    def fake_execute_plan(
        self: LanceCompactor, spark: object, uri: str, plan_version: int, task_jsons: list[str]
    ) -> list[str]:
        """Skip the executor fan-out."""
        del self, spark, uri, plan_version, task_jsons
        return []

    def broken_commit(self: LanceCompactor, uri: str, rewrite_jsons: list[str], telemetry: Telemetry) -> dict[str, int]:
        """Fail with a non-conflict error."""
        del self, uri, rewrite_jsons, telemetry
        raise RuntimeError("schema mismatch: field order differs")

    monkeypatch.setattr(LanceCompactor, "execute_plan", fake_execute_plan)
    monkeypatch.setattr(LanceCompactor, "commit_rewrites", broken_commit)
    with pytest.raises(RuntimeError, match="schema mismatch"):
        compactor.compact_one(FakeSpark(), dataset_uri, telemetry)


def test_commit_rewrites_uses_small_budget(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tier-B commit retries only the small large_commit_retries budget."""
    config: CompactionConfig = replan_config(telemetry_config, large_commit_retries=2)
    compactor: LanceCompactor = LanceCompactor(config)
    attempts: list[int] = []

    class ConflictingCompaction:
        """Compaction stand-in whose commit always conflicts."""

        @staticmethod
        def commit(dataset: object, rewrites: list[object]) -> object:
            """Always fail with a retryable commit conflict.

            Args:
                dataset: Ignored dataset handle.
                rewrites: Ignored rewrite results.
            """
            del dataset, rewrites
            attempts.append(1)
            raise OSError("LanceError(IO): Retryable commit conflict for version 2")

    monkeypatch.setattr("lance_etl.compaction.Compaction", ConflictingCompaction)
    with pytest.raises(OSError, match="Retryable commit conflict"):
        compactor.commit_rewrites(dataset_uri, [], telemetry)
    assert len(attempts) == config.large_commit_retries + 1


def test_compaction_mode_defaults_to_try_binary_copy(telemetry_config: TelemetryConfig) -> None:
    """The default execute options carry the binary-copy-with-fallback mode."""
    config: CompactionConfig = CompactionConfig(telemetry=telemetry_config)
    assert config.execute_options()["compaction_mode"] == "try_binary_copy"
    assert config.plan_options()["compaction_mode"] == "try_binary_copy"


def test_force_binary_copy_is_rejected(telemetry_config: TelemetryConfig) -> None:
    """force_binary_copy errors instead of falling back, so the config refuses it."""
    config: CompactionConfig = CompactionConfig(telemetry=telemetry_config, compaction_mode="force_binary_copy")
    with pytest.raises(ValueError, match="compaction_mode"):
        config.execute_options()


def test_cleanup_horizon_floor_is_enforced(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Cleanup horizons below the safe floor are rejected before any deletion."""
    config: CompactionConfig = CompactionConfig(telemetry=telemetry_config, cleanup_older_than_seconds=60)
    with pytest.raises(ValueError, match="cleanup_older_than_seconds"):
        cleanup_dataset(dataset_uri, config, telemetry)
    safe: CompactionConfig = CompactionConfig(
        telemetry=telemetry_config, cleanup_older_than_seconds=MIN_CLEANUP_HORIZON_SECONDS
    )
    assert cleanup_dataset(dataset_uri, safe, telemetry) >= 0
