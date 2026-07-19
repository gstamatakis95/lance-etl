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

from collections.abc import Callable
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark

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
    assert len(commits) == maintenance_job.REPLAN_BUDGET
    assert len(results) == 1
    assert f"conflicted in all {maintenance_job.REPLAN_BUDGET}" in str(results[0]["skipped"])
    assert "fragments_removed" not in results[0]


def test_hot_skip_cleans_versions_created_by_same_run_retention(
    dataset_uri: str, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retention-active hot dataset still cleans its same-run obsolete versions after deferral."""
    config: MaintenanceConfig = replan_config(telemetry_config, retention_seconds=10 * 24 * 3600)
    cleanup_calls: list[str] = []

    def retention_delete(
        dataset: lance.LanceDataset,
        uri: str,
        cfg: MaintenanceConfig,
        cutoff: object,
        telemetry: Telemetry,
    ) -> dict[str, object]:
        """Report same-run retention work without mutating the compaction fixture.

        Args:
            dataset: Open dataset handle.
            uri: Dataset URI.
            cfg: Maintenance configuration.
            cutoff: Run cutoff instant.
            telemetry: Executor telemetry facade.

        Returns:
            Retention evidence showing that this run created an obsolete version.
        """
        del dataset, cfg, cutoff, telemetry
        return {"uri": uri, "retention_rows_deleted": 1, "skipped": ""}

    def conflicting_commit(
        uri: str, rewrite_jsons: list[str], cfg: MaintenanceConfig, telemetry: Telemetry
    ) -> dict[str, object]:
        """Always defer the stale compaction rewrites.

        Args:
            uri: Dataset URI.
            rewrite_jsons: Serialized stale rewrites.
            cfg: Maintenance configuration.
            telemetry: Executor telemetry facade.

        Returns:
            A semantic conflict marker.
        """
        del rewrite_jsons, cfg, telemetry
        return {"uri": uri, "conflict": True}

    def record_cleanup(uri: str, cfg: MaintenanceConfig, telemetry: Telemetry) -> int:
        """Record the fresh post-conflict cleanup.

        Args:
            uri: Dataset URI.
            cfg: Maintenance configuration.
            telemetry: Executor telemetry facade.

        Returns:
            A distinctive reclaimed byte count.
        """
        del cfg, telemetry
        cleanup_calls.append(uri)
        return 321

    monkeypatch.setattr(maintenance_job, "run_retention_on_open_dataset", retention_delete)
    monkeypatch.setattr(maintenance_job, "commit_one_dataset", conflicting_commit)
    monkeypatch.setattr(maintenance_job, "cleanup_dataset", record_cleanup)

    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [dataset_uri])

    assert cleanup_calls == [dataset_uri]
    assert results[0]["retention_rows_deleted"] == 1
    assert results[0]["bytes_removed"] == 321
    assert f"conflicted in all {maintenance_job.REPLAN_BUDGET}" in str(results[0]["skipped"])


def test_run_commits_after_one_conflict(
    dataset_uri: str, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A conflict in the first round is resolved by the second round's fresh plan and commit."""
    config: MaintenanceConfig = replan_config(telemetry_config)
    real_commit: Callable[[str, list[str], MaintenanceConfig, Telemetry], dict[str, object]] = (
        maintenance_job.commit_one_dataset
    )
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


def test_run_isolates_non_conflict_errors(
    dataset_uri: str, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-conflict commit error is isolated into a per-dataset marker, not raised.

    The failing dataset lands a terminal ``{"error", "phase": "commit"}`` result and is neither
    re-planned nor committed, while every other dataset in the same run still compacts and commits
    normally. This is the fleet-level per-dataset failure isolation: one pathological dataset no
    longer discards the whole round's work.

    Args:
        dataset_uri: URI of the pre-built dataset whose commit is forced to fail.
        tmp_path: Pytest-provided temporary directory for a second, healthy dataset.
        telemetry_config: The test telemetry configuration.
        monkeypatch: Pytest monkeypatch fixture.
    """
    other_uri: str = str(tmp_path / "healthy.lance")
    other_table: pa.Table = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int64()),
            "payload": pa.array([f"row{i}" for i in range(ROWS)]),
        }
    )
    lance.write_dataset(other_table, other_uri, max_rows_per_file=ROWS_PER_FRAGMENT)
    config: MaintenanceConfig = replan_config(telemetry_config)
    real_commit: Callable[[str, list[str], MaintenanceConfig, Telemetry], dict[str, object]] = (
        maintenance_job.commit_one_dataset
    )

    def selective_commit(
        uri: str, rewrite_jsons: list[str], cfg: MaintenanceConfig, telemetry: Telemetry
    ) -> dict[str, object]:
        """Fail with a non-conflict error for the target dataset, commit the rest for real."""
        if uri == dataset_uri:
            raise RuntimeError("schema mismatch: field order differs")
        return real_commit(uri, rewrite_jsons, cfg, telemetry)

    monkeypatch.setattr(maintenance_job, "commit_one_dataset", selective_commit)
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [dataset_uri, other_uri])
    by_uri: dict[str, dict[str, object]] = {str(result["uri"]): result for result in results}

    assert "schema mismatch" in str(by_uri[dataset_uri]["error"])
    assert by_uri[dataset_uri]["phase"] == "commit"
    assert "fragments_removed" not in by_uri[dataset_uri]

    assert "error" not in by_uri[other_uri]
    assert by_uri[other_uri]["fragments_removed"] == ROWS // ROWS_PER_FRAGMENT
    assert len(lance.dataset(other_uri).get_fragments()) == 1


def test_commit_one_dataset_uses_small_budget(
    dataset_uri: str, telemetry_config: TelemetryConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The commit retries only the small large_commit_retries manifest-race budget, then defers.

    Exhaustion surfaces as a ``conflict`` marker rather than an exception, so the fleet run
    re-plans the dataset instead of failing.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry_config: The test telemetry configuration.
        telemetry: The telemetry facade fixture.
        monkeypatch: Pytest monkeypatch fixture.
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
