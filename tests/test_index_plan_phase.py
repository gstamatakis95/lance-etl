"""The indexing plan-phase failure marker.

``LanceIndexer.run`` records a plan fan-out failure as a dataset-level error tagged
``error_phase="plan"`` (runner.py:871), isolating that dataset while the rest of the fleet run
proceeds. This drives the stable indexing runner under an in-process fake Spark session, so it does
not depend on the concurrently-edited reconciler surface.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark

import lance_etl.indexing.cli as indexing_cli
import lance_etl.indexing.runner as indexing_runner
from lance_etl.cliutil import EXIT_PARTIAL_FAILURE
from lance_etl.fanout import count_failed
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.telemetry import TelemetryConfig


def fake_build_spark(*args: object, **kwargs: object) -> FakeSpark:
    """Return a fake session regardless of the requested Spark app name or configuration.

    Args:
        args: Ignored positional arguments.
        kwargs: Ignored keyword arguments.

    Returns:
        A fresh fake Spark session.
    """
    del args, kwargs
    return FakeSpark()


def test_index_plan_failure_records_plan_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A plan fan-out failure lands a dataset-level error tagged ``error_phase='plan'``."""
    uri: str = str(tmp_path / "plan.lance")
    lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)

    def boom(dataset_uri: str, config: IndexJobConfig, telemetry: object) -> dict[str, Any]:
        """Fail the plan phase for the dataset.

        Args:
            dataset_uri: Ignored dataset URI.
            config: Ignored indexing configuration.
            telemetry: Ignored executor telemetry facade.

        Raises:
            RuntimeError: Always, to drive the plan-phase failure path.
        """
        del dataset_uri, config, telemetry
        raise RuntimeError("plan resolution failed")

    monkeypatch.setattr(indexing_runner, "plan_dataset_indexes", boom)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [uri])

    assert len(results) == 1
    assert results[0]["error_phase"] == "plan"
    assert "plan resolution failed" in str(results[0]["error"])


def test_fleet_run_counts_unopenable_dataset_as_failed(tmp_path: Path) -> None:
    """A fleet-level LanceIndexer.run counts a nonexistent dataset URI as failed.

    Regression guard for PR-02 finding 1: `plan_dataset_indexes`'s own open-failure catch must
    return an `{"error", "phase"}` marker, not a benign `"skipped"` one, so it is visible to
    :func:`~lance_etl.fanout.count_failed` and, in turn, to the indexing CLI's exit code.
    """
    missing_uri: str = str(tmp_path / "does_not_exist.lance")
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [missing_uri])
    assert count_failed(results) == 1
    assert results[0]["error_phase"] == "open"


def test_indexing_cli_exits_partial_failure_on_unopenable_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The indexing CLI's exit code reflects a fleet run over one unopenable dataset.

    Regression guard for PR-02: `run_migrate_namespace`'s sibling CLIs (indexing, maintenance)
    must map an isolated open failure to :data:`~lance_etl.cliutil.EXIT_PARTIAL_FAILURE`, not exit
    ``0`` as if the whole fleet run succeeded.
    """
    monkeypatch.setattr(indexing_cli, "build_spark", fake_build_spark)
    missing_uri: str = str(tmp_path / "does_not_exist.lance")
    exit_code: int = indexing_cli.main(["--dataset-uri", missing_uri, "--scalar-column", "id"])
    assert exit_code == EXIT_PARTIAL_FAILURE
