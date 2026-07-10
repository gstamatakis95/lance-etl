"""Tests for the object-store centroid sidecar cache (ADR 0040).

Exercises the full :meth:`LanceIndexer.run` on a real local-fs vector dataset through the
in-process ``FakeSpark`` fake, so the plan, build, and commit closures run in the driver process
and the sidecar reads and writes execute on the same handle production uses. Covers:

- A streaming bootstrap writes the centroid sidecar keyed by ``rows_at_train`` and the sidecar
  reload round-trips to the same centroids.
- A second (segments-mode) run reuses the sidecar and never re-reads the committed index via
  ``get_ivf_model``.
- A missing sidecar falls back to ``get_ivf_model``, backfills the sidecar, and still builds.
- A ``save`` failure at bootstrap is swallowed: the index still commits and the run completes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark, make_vector_table, write_fragmented_dataset
from lance.indices import IvfModel

from lance_etl.indexing import (
    IndexJobConfig,
    LanceIndexer,
    centroid_sidecar_uri,
    load_centroids,
    load_vector_config,
)
from lance_etl.telemetry import TelemetryConfig

ROWS: int = 512
DIM: int = 8
ROWS_PER_FRAGMENT: int = 512
APPEND_ROWS: int = 128


def vector_config(**overrides: Any) -> IndexJobConfig:
    """Build a vector-only indexing configuration with no row floor and a tiny partition count.

    Args:
        **overrides: Field overrides applied on top of the test defaults.

    Returns:
        An indexing configuration targeting the ``vector`` column.
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


def write_vector_dataset(directory: Path, name: str) -> str:
    """Write a single-fragment local vector dataset and return its URI.

    Args:
        directory: Destination directory.
        name: The dataset directory basename.

    Returns:
        The dataset URI string.
    """
    uri: str = str(directory / name)
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def append_fragment(uri: str, rows: int, start_id: int) -> None:
    """Append one new fragment of rows to a dataset, creating uncovered fragments for a segments run.

    Args:
        uri: The dataset URI.
        rows: How many rows to append.
        start_id: The first id value of the appended range.
    """
    table: pa.Table = make_vector_table(rows=rows, dim=DIM, seed=start_id)
    reindexed: pa.Table = table.set_column(0, "id", pa.array(range(start_id, start_id + rows), pa.int64()))
    lance.write_dataset(reindexed, uri, mode="append")


def spy_get_ivf_model(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Install a spy over ``LanceDataset.get_ivf_model`` recording every index name it is asked for.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The list the spy appends each requested index name to.
    """
    calls: list[str] = []
    real = lance.LanceDataset.get_ivf_model

    def spy(self: lance.LanceDataset, index_name: str) -> Any:
        """Record the requested index name, then delegate to the real ``get_ivf_model``."""
        calls.append(index_name)
        return real(self, index_name)

    monkeypatch.setattr(lance.LanceDataset, "get_ivf_model", spy)
    return calls


def test_bootstrap_writes_sidecar_and_reload_round_trips(tmp_path: Path) -> None:
    """A bootstrap caches the trained centroids to the sidecar and the reload round-trips exactly.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    uri: str = write_vector_dataset(tmp_path, "sidecar.lance")
    LanceIndexer(vector_config()).run(FakeSpark(), [uri])

    dataset: lance.LanceDataset = lance.dataset(uri)
    cfg: dict[str, Any] | None = load_vector_config(dataset, "vector")
    assert cfg is not None
    rows_at_train: int = int(cfg["rows_at_train"])

    sidecar: str = centroid_sidecar_uri(uri, "vector_idx", rows_at_train)
    assert Path(sidecar).exists()

    loaded: pa.Array | None = load_centroids(uri, "vector_idx", rows_at_train, None)
    assert loaded is not None
    committed: pa.Array = dataset.get_ivf_model("vector_idx").centroids
    assert loaded.to_pylist() == committed.to_pylist()


def test_second_run_reuses_sidecar_without_reading_committed_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A segments-mode run reads centroids from the sidecar and never re-opens the committed index.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    uri: str = write_vector_dataset(tmp_path, "reuse.lance")
    config: IndexJobConfig = vector_config()
    LanceIndexer(config).run(FakeSpark(), [uri])
    append_fragment(uri, APPEND_ROWS, ROWS)

    calls: list[str] = spy_get_ivf_model(monkeypatch)
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [uri])

    assert calls == [], f"the sidecar hit should have avoided every get_ivf_model read, saw {calls}"
    by_index: dict[str, dict[str, Any]] = {item["index"]: item for item in results[0]["indexes"]}
    assert int(by_index["vector_idx"]["segments"]) >= 1, "the segments-mode reuse run must build a delta"
    dataset: lance.LanceDataset = lance.dataset(uri)
    assert dataset.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 3}).num_rows == 3


def test_missing_sidecar_falls_back_to_committed_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A deleted sidecar falls back to ``get_ivf_model``, backfills the sidecar, and still builds.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    uri: str = write_vector_dataset(tmp_path, "fallback.lance")
    config: IndexJobConfig = vector_config()
    LanceIndexer(config).run(FakeSpark(), [uri])

    cfg: dict[str, Any] | None = load_vector_config(lance.dataset(uri), "vector")
    assert cfg is not None
    rows_at_train: int = int(cfg["rows_at_train"])
    Path(centroid_sidecar_uri(uri, "vector_idx", rows_at_train)).unlink()
    assert load_centroids(uri, "vector_idx", rows_at_train, None) is None
    append_fragment(uri, APPEND_ROWS, ROWS)

    calls: list[str] = spy_get_ivf_model(monkeypatch)
    LanceIndexer(config).run(FakeSpark(), [uri])

    assert calls, "a sidecar miss must fall back to reading the committed index"
    assert load_centroids(uri, "vector_idx", rows_at_train, None) is not None, "the miss must backfill the sidecar"
    dataset: lance.LanceDataset = lance.dataset(uri)
    assert dataset.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 3}).num_rows == 3


def test_save_failure_does_not_fail_bootstrap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A sidecar-write failure at bootstrap is swallowed: the index commits and the run succeeds.

    Args:
        tmp_path: Pytest-provided temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    uri: str = write_vector_dataset(tmp_path, "savefail.lance")

    def boom(self: IvfModel, target: str, *, storage_options: dict[str, str] | None = None) -> None:
        """Fail every sidecar write to prove the best-effort wrapper never aborts the bootstrap."""
        del self, target, storage_options
        raise RuntimeError("sidecar save boom")

    monkeypatch.setattr(IvfModel, "save", boom)
    results: list[dict[str, Any]] = LanceIndexer(vector_config()).run(FakeSpark(), [uri])

    stats: dict[str, Any] = results[0]
    assert "error" not in stats
    assert not any("error" in item for item in stats["indexes"])
    by_index: dict[str, dict[str, Any]] = {item["index"]: item for item in stats["indexes"]}
    assert "vector_idx" in by_index

    dataset: lance.LanceDataset = lance.dataset(uri)
    assert dataset.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 3}).num_rows == 3
    cfg: dict[str, Any] | None = load_vector_config(dataset, "vector")
    assert cfg is not None
    assert not Path(centroid_sidecar_uri(uri, "vector_idx", int(cfg["rows_at_train"]))).exists()
