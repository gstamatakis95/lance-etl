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

import lance_etl.indexing.runner as indexing_runner
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.telemetry import TelemetryConfig


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
