"""Failure-isolation tests: one poisoned dataset never aborts a fleet run and is never stamped.

The fleet jobs isolate per-dataset failures instead of aborting. Each poisoned path is exercised
against a small fleet of real local Lance datasets driven by the in-process ``FakeSpark`` fake, so
the plan, build, and commit closures run in the driver process and a per-dataset failure can be
injected with ``monkeypatch.setattr`` on the module-level business functions those closures call.

The suite pins every marker shape the isolation feature produces:

- Maintenance records a top-level ``{"error", "phase"}`` marker for a plan, execute, or commit
  failure, and a plain object-open failure is caught into a ``"skipped"`` dict.
- Indexing records a top-level ``"error"`` for a plan failure but a per-index ``{"error",
  "phase"}`` entry inside the ``indexes`` list for a build or commit failure (vector-artifact
  resolution now happens inside the build shard, so a resolution failure surfaces as a
  ``"phase": "build"`` entry).
- :func:`stamp_eligible` must exclude both indexing shapes, which is the correctness fix these
  tests guard: a dataset whose vector build failed is recorded as a per-index error with no
  top-level key, so the old one-key ``"error" not in index_stats`` check let it pass and
  HEAD-promoted the serving layer onto an incomplete index.
- The per-job CLIs surface the isolated failures as the ``EXIT_PARTIAL_FAILURE`` exit code ``3``.

The real business callables are imported under ``real_*`` aliases at module top so a wrapper can
delegate to the original object regardless of the monkeypatch installed on the module attribute.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pytest
from conftest import FakeSpark, make_vector_table, write_fragmented_dataset

import lance_etl.indexing.cli as indexing_cli
import lance_etl.indexing.runner as indexing_runner
import lance_etl.maintenance.job as maintenance_job
from lance_etl.cliutil import resolve_exit_code
from lance_etl.fanout import count_failed
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.indexing.runner import STALE_REPLAN_EXHAUSTED_PHASE, LanceIndexer
from lance_etl.indexing.runner import build_one_shard as real_build_one_shard
from lance_etl.indexing.runner import commit_one_index as real_commit_one_index
from lance_etl.indexing.runner import plan_dataset_indexes as real_plan_dataset_indexes
from lance_etl.maintenance.job import MaintenanceConfig, MaintenanceJob
from lance_etl.maintenance.job import commit_one_dataset as real_commit_one_dataset
from lance_etl.pipeline.job import PipelineConfig, PipelineJob, stamp_eligible
from lance_etl.telemetry import TelemetryConfig

ROWS: int = 40
DIM: int = 8
ROWS_PER_FRAGMENT: int = 10
INTERVAL_TAG: str = "20260611T120000Z"


class StoppableFakeSpark(FakeSpark):
    """A ``FakeSpark`` that also answers the ``stop()`` call the CLIs make in their teardown."""

    def stop(self) -> None:
        """Ignore the session-stop call so the CLI ``finally`` teardown is a no-op in tests."""


def write_vector_dataset(directory: Path, name: str, max_rows_per_file: int = ROWS_PER_FRAGMENT) -> str:
    """Write a small real vector dataset and return its URI.

    Args:
        directory: Destination directory; a subdirectory named ``name`` is created inside it.
        name: The dataset directory basename.
        max_rows_per_file: Row cap per fragment file. Passing :data:`ROWS` yields one fragment.

    Returns:
        The dataset URI string.
    """
    uri: str = str(directory / name)
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=max_rows_per_file)
    return uri


def indexing_config(**overrides: Any) -> IndexJobConfig:
    """Build a vector-indexing configuration with no row floor for the isolation tests.

    Args:
        **overrides: Field overrides applied on top of the test defaults.

    Returns:
        An indexing configuration targeting the ``vector`` column with a tiny partition count.
    """
    base: dict[str, Any] = {
        "telemetry": TelemetryConfig(),
        "vector_columns": ["vector"],
        "num_partitions": 4,
        "vector_min_rows": 1,
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return IndexJobConfig(**base)


def maintenance_config(**overrides: Any) -> MaintenanceConfig:
    """Build a maintenance configuration that compacts eagerly for the isolation tests.

    Args:
        **overrides: Field overrides applied on top of the test defaults.

    Returns:
        A maintenance configuration with a large target so small fragments merge into one.
    """
    base: dict[str, Any] = {
        "telemetry": TelemetryConfig(),
        "target_rows_per_fragment": 1000,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return MaintenanceConfig(**base)


def fake_build_spark(app_name: str | None = None) -> StoppableFakeSpark:
    """Return an in-process fake Spark session for the CLI entry points under test.

    Args:
        app_name: Ignored Spark application name, accepted for signature parity with build_spark.

    Returns:
        A :class:`StoppableFakeSpark` standing in for a real ``SparkSession``.
    """
    del app_name
    return StoppableFakeSpark()


def test_maintenance_plan_error_isolated(tmp_path: Path) -> None:
    """An unreadable dataset is isolated as a skip while the rest of the maintenance fleet compacts.

    ``plan_one_dataset`` catches the object-open failure of a missing dataset and returns a
    ``"skipped"`` dict, so the poisoned URI carries no top-level ``"error"`` yet the run still
    completes with exactly one terminal entry per URI. The healthy fragmented dataset is compacted
    to a single fragment, proving one dataset's plan failure never aborts the fleet.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    missing: str = str(tmp_path / "missing.lance")
    results: list[dict[str, Any]] = MaintenanceJob(maintenance_config()).run(FakeSpark(), [healthy, missing])

    by_uri: dict[str, dict[str, Any]] = {result["uri"]: result for result in results}
    assert len(results) == 2
    assert set(by_uri) == {healthy, missing}
    assert "error" not in by_uri[healthy]
    assert len(lance.dataset(healthy).get_fragments()) == 1
    assert "skipped" in by_uri[missing]
    assert "error" not in by_uri[missing]


def test_maintenance_commit_error_isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-conflict commit failure on one dataset is isolated with a commit-phase marker.

    ``commit_one_dataset`` is patched to raise a non-conflict ``RuntimeError`` for the poisoned
    dataset, which the commit fan-out isolates into a ``{"error", "phase": "commit"}`` marker. The
    poisoned dataset therefore keeps its four uncompacted fragments while the healthy dataset
    commits its rewrites and compacts to a single fragment.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    poison: str = write_vector_dataset(tmp_path, "poison.lance")

    def poisoned_commit(
        uri: str, rewrite_jsons: list[str], config: MaintenanceConfig, telemetry: Any
    ) -> dict[str, Any]:
        """Raise a non-conflict error for the poisoned dataset, delegating for the rest."""
        if uri == poison:
            raise RuntimeError("schema mismatch")
        return real_commit_one_dataset(uri, rewrite_jsons, config, telemetry)

    monkeypatch.setattr(maintenance_job, "commit_one_dataset", poisoned_commit)
    results: list[dict[str, Any]] = MaintenanceJob(maintenance_config()).run(FakeSpark(), [healthy, poison])

    by_uri: dict[str, dict[str, Any]] = {result["uri"]: result for result in results}
    assert "schema mismatch" in by_uri[poison]["error"]
    assert by_uri[poison]["phase"] == "commit"
    assert len(lance.dataset(poison).get_fragments()) == 4
    assert "error" not in by_uri[healthy]
    assert len(lance.dataset(healthy).get_fragments()) == 1


def test_indexing_build_error_isolated_and_not_stamped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A vector build failure is a per-index error that makes only the poisoned dataset ineligible.

    ``build_one_shard`` is patched to raise for the poisoned dataset. Its failure is recorded as a
    per-index ``{"error", "phase": "build"}`` entry inside ``indexes`` with no top-level ``"error"``
    key, while the healthy dataset builds its vector index. This is the discriminating case for the
    :func:`stamp_eligible` fix: the poisoned result has no top-level error, so the old one-key
    ``"error" not in index_stats`` check returned ``True`` and would HEAD-promote a dataset whose
    index is incomplete. The corrected check also scans the per-index entries, so ``stamp_eligible``
    is ``False`` for the poisoned dataset and ``True`` for the healthy one.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    poison: str = write_vector_dataset(tmp_path, "poison.lance")

    def poisoned_build(task: dict[str, Any], config: IndexJobConfig, telemetry: Any) -> Any:
        """Raise for the poisoned dataset's shard build, delegating for the rest."""
        if task["uri"] == poison:
            raise RuntimeError("segment build boom")
        return real_build_one_shard(task, config, telemetry)

    monkeypatch.setattr(indexing_runner, "build_one_shard", poisoned_build)
    results: list[dict[str, Any]] = LanceIndexer(indexing_config()).run(FakeSpark(), [healthy, poison])

    by_uri: dict[str, dict[str, Any]] = {result["uri"]: result for result in results}
    healthy_result: dict[str, Any] = by_uri[healthy]
    poison_result: dict[str, Any] = by_uri[poison]

    assert "error" not in poison_result
    assert any("error" in index for index in poison_result["indexes"])
    assert "error" not in healthy_result
    assert not any("error" in index for index in healthy_result["indexes"])
    assert any(index["index"] == "vector_idx" for index in healthy_result["indexes"])

    assert stamp_eligible(poison_result) is False
    assert stamp_eligible(healthy_result) is True


def test_indexing_plan_error_sets_top_level_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A plan failure sets a top-level indexing error, distinct from the caught unreadable-URI skip.

    ``plan_dataset_indexes`` catches an object-open failure internally and returns a skip, so an
    unreadable URI can never surface as a top-level error. To exercise the genuine plan-phase
    error path the function is patched to raise, which the plan fan-out isolates into a top-level
    ``{"error", "error_phase": "plan"}`` marker with an empty ``indexes`` list. Both the reality
    (a nonexistent URI yields a still-eligible ``"skipped"`` result) and the top-level error path
    (ineligible) are pinned here.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    poison: str = write_vector_dataset(tmp_path, "poison.lance")

    def poisoned_plan(uri: str, config: IndexJobConfig, telemetry: Any) -> dict[str, Any]:
        """Raise for the poisoned dataset's plan, delegating for the rest."""
        if uri == poison:
            raise RuntimeError("plan boom")
        return real_plan_dataset_indexes(uri, config, telemetry)

    monkeypatch.setattr(indexing_runner, "plan_dataset_indexes", poisoned_plan)
    results: list[dict[str, Any]] = LanceIndexer(indexing_config()).run(FakeSpark(), [healthy, poison])

    by_uri: dict[str, dict[str, Any]] = {result["uri"]: result for result in results}
    assert "plan boom" in by_uri[poison]["error"]
    assert by_uri[poison]["error_phase"] == "plan"
    assert by_uri[poison]["indexes"] == []
    assert stamp_eligible(by_uri[poison]) is False
    assert "error" not in by_uri[healthy]
    assert stamp_eligible(by_uri[healthy]) is True

    unreadable: str = str(tmp_path / "never.lance")
    skip_results: list[dict[str, Any]] = LanceIndexer(indexing_config()).run(FakeSpark(), [unreadable])
    skip_result: dict[str, Any] = skip_results[0]
    assert "skipped" in skip_result
    assert "error" not in skip_result
    assert stamp_eligible(skip_result) is True


def test_pipeline_build_failed_dataset_not_head_promoted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, telemetry_config: TelemetryConfig
) -> None:
    """A build-failed dataset is neither interval-stamped nor HEAD-promoted by the pipeline.

    The pipeline runs with an interval tag and ``serve_tag`` on so eligible datasets get both the
    interval tag and a HEAD flip. ``build_one_shard`` is patched to fail the poisoned dataset's
    vector build, which records a per-index error. With the :func:`stamp_eligible` fix the poisoned
    dataset is excluded from stamping, so its HEAD tag never advances onto the incomplete index,
    while the healthy dataset receives both tags. Without the fix the poisoned dataset would pass
    the one-key eligibility check and be HEAD-promoted here.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
        telemetry_config: The shared test telemetry configuration.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance", max_rows_per_file=ROWS)
    poison: str = write_vector_dataset(tmp_path, "poison.lance", max_rows_per_file=ROWS)

    def poisoned_build(task: dict[str, Any], config: IndexJobConfig, telemetry: Any) -> Any:
        """Raise for the poisoned dataset's shard build, delegating for the rest."""
        if task["uri"] == poison:
            raise RuntimeError("segment build boom")
        return real_build_one_shard(task, config, telemetry)

    monkeypatch.setattr(indexing_runner, "build_one_shard", poisoned_build)

    config: PipelineConfig = PipelineConfig(
        telemetry=telemetry_config,
        maintenance=maintenance_config(),
        indexing=indexing_config(),
        tag_keep_last=None,
        tag_stamp=INTERVAL_TAG,
        serve_tag=True,
    )
    result: dict[str, Any] = PipelineJob(config).run(FakeSpark(), [healthy, poison])

    assert result["counts"]["failed"] >= 1
    healthy_tags: set[str] = set(lance.dataset(healthy).tags.list())
    poison_tags: set[str] = set(lance.dataset(poison).tags.list())
    assert "HEAD" in healthy_tags
    assert INTERVAL_TAG in healthy_tags
    assert "HEAD" not in poison_tags
    assert INTERVAL_TAG not in poison_tags

    stamped_uris: set[str] = {entry["uri"] for entry in result["stamp_results"]}
    assert healthy in stamped_uris
    assert poison not in stamped_uris


def test_cli_main_returns_3_on_partial_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The indexing CLI returns ``EXIT_PARTIAL_FAILURE`` when one dataset fails in isolation.

    A BTREE build is targeted so the tiny datasets are not skipped by the vector row floor.
    ``build_one_shard`` is patched to fail one dataset's build, which the fleet isolates into a
    per-index error, so ``main`` returns exit code ``3`` rather than raising or returning ``0``.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    poison: str = write_vector_dataset(tmp_path, "poison.lance")

    def poisoned_build(task: dict[str, Any], config: IndexJobConfig, telemetry: Any) -> Any:
        """Raise for the poisoned dataset's shard build, delegating for the rest."""
        if task["uri"] == poison:
            raise RuntimeError("segment build boom")
        return real_build_one_shard(task, config, telemetry)

    monkeypatch.setattr(indexing_cli, "build_spark", fake_build_spark)
    monkeypatch.setattr(indexing_runner, "build_one_shard", poisoned_build)
    argv: list[str] = ["--dataset-uri", healthy, "--dataset-uri", poison, "--scalar-column", "id"]
    assert indexing_cli.main(argv) == 3


def test_cli_main_returns_0_when_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The indexing CLI returns ``0`` when every dataset in the fleet succeeds.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    first: str = write_vector_dataset(tmp_path, "first.lance")
    second: str = write_vector_dataset(tmp_path, "second.lance")

    monkeypatch.setattr(indexing_cli, "build_spark", fake_build_spark)
    argv: list[str] = ["--dataset-uri", first, "--dataset-uri", second, "--scalar-column", "id"]
    assert indexing_cli.main(argv) == 0


def test_indexing_stale_replan_exhaustion_fails_dataset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A dataset that stays stale through every replan round becomes a failed dataset, not a clean exit.

    ``commit_one_index`` is patched to return a stale marker for the poisoned dataset on every
    round, simulating a compaction that keeps rewriting the planned fragments before each commit.
    After ``MAX_STALE_REPLANS`` rounds the run must record a dataset-level error with the
    ``index-stale-exhausted`` phase instead of silently deferring: the dataset counts toward the
    failed total (so the CLI exits ``3`` through :func:`resolve_exit_code`) and is excluded from
    stamping and HEAD promotion by :func:`stamp_eligible`, while the healthy dataset commits its
    index in the first round and stays eligible.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    healthy: str = write_vector_dataset(tmp_path, "healthy.lance")
    poison: str = write_vector_dataset(tmp_path, "poison.lance")

    def stale_commit(
        uri: str, spec: dict[str, Any], payloads: list[dict[str, Any]], config: IndexJobConfig, telemetry: Any
    ) -> dict[str, Any]:
        """Return a permanently stale outcome for the poisoned dataset, delegating for the rest."""
        if uri == poison:
            return {
                "column": spec["column"],
                "index": spec["index_name"],
                "segments": 0,
                "fragments": 0,
                "stale": True,
            }
        return real_commit_one_index(uri, spec, payloads, config, telemetry)

    monkeypatch.setattr(indexing_runner, "commit_one_index", stale_commit)
    config: IndexJobConfig = indexing_config(vector_columns=[], scalar_columns=["id"])
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [healthy, poison])

    by_uri: dict[str, dict[str, Any]] = {result["uri"]: result for result in results}
    poison_result: dict[str, Any] = by_uri[poison]
    healthy_result: dict[str, Any] = by_uri[healthy]

    assert "stale-replan exhausted" in poison_result["error"]
    assert poison_result["error_phase"] == STALE_REPLAN_EXHAUSTED_PHASE
    assert "error" not in healthy_result
    assert any(index["index"] == "id_idx" for index in healthy_result["indexes"])

    failed: int = count_failed(results)
    assert failed == 1
    assert resolve_exit_code(failed) == 3
    assert stamp_eligible(poison_result) is False
    assert stamp_eligible(healthy_result) is True
