"""Unit tests for the composed :func:`bench.experiment.run_experiment` loop.

Drives the full aggregation with a stubbed e2e runner so the assertions cover only the
composition logic in ``run_experiment`` itself: build-seconds summation, publication
verification counting, headline distillation, and the on-disk ``metrics.json`` and
``experiments.jsonl`` artifacts. No Spark session, no real Iceberg ingest, and no network calls
are exercised.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from bench import experiment
from bench.config import BenchConfig, build_parser


def build_experiment_config(tmp_path: Path, run_id: str, extra: list[str] | None = None) -> BenchConfig:
    """Parse an experiment configuration rooted in the test tmp dir.

    Args:
        tmp_path: The test workspace root.
        run_id: The run identifier.
        extra: Additional CLI flags.

    Returns:
        The parsed configuration.
    """
    argv: list[str] = [
        "experiment",
        "--workspace",
        str(tmp_path / "workspace"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        run_id,
        *(extra or []),
    ]
    return BenchConfig.from_args(build_parser().parse_args(argv))


def fake_ensure_prepared(config: BenchConfig) -> dict[str, Any]:
    """Stand in for :func:`bench.experiment.ensure_prepared` with no download or prepare.

    Args:
        config: Benchmark configuration; its workspace is echoed back for traceability.

    Returns:
        A small deterministic preparation record.
    """
    return {"queries": 10, "workspace": str(config.workspace)}


def fake_measure_dataset_sizes(lance_root: Path) -> dict[str, Any]:
    """Stand in for :func:`bench.experiment.measure_dataset_sizes` with a fixed footprint.

    Args:
        lance_root: The Lance base directory; echoed back for traceability.

    Returns:
        A minimal size record carrying the keys ``headline_numbers`` requires.
    """
    return {"data_bytes": 0, "index_bytes": 0, "total_bytes": 0, "root": str(lance_root)}


def build_fake_e2e_doc(config: BenchConfig) -> dict[str, Any]:
    """Build a synthetic e2e document with known batch seconds and publication outcomes.

    Args:
        config: Benchmark configuration; its run id is echoed back for traceability.

    Returns:
        A document shaped like the real :func:`bench.e2e.run_e2e` output, with three batches
        summing to 4.0 seconds and two of three publications marked published.
    """
    return {
        "run_id": config.run_id,
        "batches": [
            {"batch": 1, "seconds": 1.5, "reconcile": {"status": "ok"}},
            {"batch": 2, "seconds": 2.0, "reconcile": {"status": "ok"}},
            {"batch": 3, "seconds": 0.5, "reconcile": {"status": "ok"}},
        ],
        "publication_verification": {"ok": True},
        "publications": [
            {"target": "org0", "published": True},
            {"target": "org1", "published": True},
            {"target": "org2", "published": False},
        ],
    }


def test_run_experiment_aggregates_stubbed_e2e(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The composed loop sums batch seconds, counts publications, and writes both artifacts."""
    config: BenchConfig = build_experiment_config(tmp_path, "run-stub")

    def fake_run_e2e(passed_config: BenchConfig) -> dict[str, Any]:
        """Stand in for :func:`bench.experiment.run_e2e` with a synthetic document.

        Args:
            passed_config: Benchmark configuration passed straight through to the builder.

        Returns:
            The synthetic e2e document.
        """
        return build_fake_e2e_doc(passed_config)

    monkeypatch.setattr(experiment, "ensure_prepared", fake_ensure_prepared)
    monkeypatch.setattr(experiment, "run_e2e", fake_run_e2e)
    monkeypatch.setattr(experiment, "measure_dataset_sizes", fake_measure_dataset_sizes)

    doc: dict[str, Any] = experiment.run_experiment(config)

    assert doc["run_id"] == config.run_id
    assert doc["build"]["total_seconds"] == 4.0
    assert len(doc["build"]["batches"]) == 3
    assert [batch["seconds"] for batch in doc["build"]["batches"]] == [1.5, 2.0, 0.5]
    assert doc["publications"]["verified"] is True
    assert doc["publications"]["published"] == 2
    assert isinstance(doc["headline"], dict)
    assert doc["baseline_delta"] is None

    metrics_path: Path = config.run_dir() / "metrics.json"
    history_path: Path = config.results_root / "experiments.jsonl"
    assert metrics_path.exists()
    assert history_path.exists()

    saved_metrics: dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert saved_metrics["run_id"] == config.run_id
    assert saved_metrics["build"]["total_seconds"] == 4.0

    history_lines: list[dict[str, Any]] = [
        json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()
    ]
    assert history_lines[-1]["run_id"] == config.run_id
