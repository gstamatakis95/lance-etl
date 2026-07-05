"""Offline tiny-data end-to-end test of the benchmark chain through the bigann adapter.

Writes a minimal bigann workspace fixture (2000 base vectors, 128 dims, uint8, seeded rng; 50
queries) directly into ``{tmp_path}/workspace/bigann/`` using :func:`bench.bigann_io.write_u8bin`,
with the exact filenames :class:`bench.datasets.BigannAdapter` expects for ``limit=2000``. The
download phase short-circuits (base and query files already present). ``gt_member_name(2000)``
returns ``None`` so the prepare phase computes exact brute-force ground truth. The remaining
flow drives prepare -> ingest -> index -> compact -> report through Spark local mode and asserts
that all artifacts materialize.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pytest

from bench.bigann_io import write_u8bin
from bench.compaction import run_compact
from bench.config import BenchConfig, build_parser
from bench.datasets import BigannAdapter
from bench.experiment import run_experiment
from bench.indexes import run_index
from bench.ingest import run_ingest
from bench.prepare import run_prepare
from bench.report import run_report
from bench.results import read_json
from bench.server import resolve_binary

pytestmark = pytest.mark.integration

BASE_ROWS: int = 2_000
DIMENSION: int = 128
QUERY_ROWS: int = 50
TENANTS: int = 2
GT_DEPTH: int = min(BigannAdapter().gt_depth, BASE_ROWS // TENANTS)
EXPECTED_INDEX_NAMES: frozenset[str] = frozenset({"vector_idx", "vector_id_idx", "category_bitmap_idx", "text_fts_idx"})


def make_bigann_fixture(corpus_root: Path, seed: int = 11) -> None:
    """Write a minimal bigann corpus fixture that bypasses network download.

    Creates ``{corpus_root}/bigann/base.2000.u8bin`` and
    ``{corpus_root}/bigann/query.10K.u8bin`` using seeded random uint8 data.
    The filenames match exactly what :class:`bench.datasets.BigannAdapter` expects
    for ``limit=2000`` and the default query path.

    Args:
        corpus_root: The shared corpus cache directory.
        seed: RNG seed for reproducible vectors.
    """
    corpus_root.mkdir(parents=True, exist_ok=True)
    rng: np.random.Generator = np.random.default_rng(seed)
    base: np.ndarray = rng.integers(0, 256, size=(BASE_ROWS, DIMENSION), dtype=np.uint8).astype(np.float32)
    queries: np.ndarray = rng.integers(0, 256, size=(QUERY_ROWS, DIMENSION), dtype=np.uint8).astype(np.float32)
    adapter: BigannAdapter = BigannAdapter(limit=BASE_ROWS)
    write_u8bin(adapter.base_path(corpus_root), base)
    write_u8bin(adapter.query_path(corpus_root), queries)


def tiny_config(tmp_path: Path) -> BenchConfig:
    """Build the benchmark configuration of the offline tiny bigann run.

    Args:
        tmp_path: The temporary workspace root.

    Returns:
        The parsed configuration targeting the bigann adapter.
    """
    argv: list[str] = [
        "all",
        "--dataset",
        "bigann",
        "--workspace",
        str(tmp_path / "workspace"),
        "--corpus-root",
        str(tmp_path / "corpora"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        "e2e",
        "--limit",
        str(BASE_ROWS),
        "--tenants",
        str(TENANTS),
        "--batches",
        "2",
        "--num-clusters",
        "4",
        "--words-per-cluster",
        "10",
        "--common-words",
        "5",
        "--rows-per-slice",
        "500",
        "--etl-partitions",
        "2",
        "--num-shards",
        "2",
        "--vector-row-floor",
        "100",
        "--spark-master",
        "local[2]",
        "--driver-memory",
        "2g",
    ]
    return BenchConfig.from_args(build_parser().parse_args(argv))


def assert_prepare_artifacts(config: BenchConfig, outcome: dict[str, Any]) -> None:
    """Assert the prepare phase produced every artifact with the tiny corpus shape.

    Args:
        config: The benchmark configuration.
        outcome: The prepare phase document.
    """
    assert outcome["skipped"] is False
    prepared: Path = config.prepared_dir()
    assert prepared.name == f"bigann-n{BASE_ROWS}-t{TENANTS}-s42-c4"
    queries: np.ndarray = np.load(prepared / "queries.npy")
    assert queries.shape == (QUERY_ROWS, DIMENSION)
    ground_truth = np.load(prepared / "ground_truth.npz")
    assert sorted(ground_truth.files) == ["org0", "org1"]
    for org in ground_truth.files:
        assert ground_truth[org].shape == (QUERY_ROWS, GT_DEPTH)
    manifest: dict[str, Any] = read_json(prepared / "manifest.json")
    assert manifest["ground_truth_source"] == "brute_force"
    assert manifest["limit"] == BASE_ROWS


def test_offline_tiny_end_to_end(tmp_path: Path) -> None:
    """Prepare -> ingest -> index -> compact -> report runs offline against the bigann fixture."""
    corpus_root: Path = tmp_path / "corpora"
    make_bigann_fixture(corpus_root)

    config: BenchConfig = tiny_config(tmp_path)

    download_outcome: dict[str, Any] = BigannAdapter(limit=BASE_ROWS).download(corpus_root)
    assert download_outcome["skipped"] is True, "download must short-circuit when base+query files exist"

    assert_prepare_artifacts(config, run_prepare(config))

    ingest_outcome: dict[str, Any] = run_ingest(config)
    assert ingest_outcome["total_rows"] == BASE_ROWS
    assert len(ingest_outcome["batches"]) == 2
    uris: list[str] = config.dataset_uris()
    assert len(uris) == TENANTS
    for uri in uris:
        dataset: lance.LanceDataset = lance.dataset(uri)
        assert dataset.count_rows() == BASE_ROWS // TENANTS
        field = dataset.schema.field("vector")
        assert field.type.list_size == DIMENSION

    index_outcome: dict[str, Any] = run_index(config)
    assert [stage["stage"] for stage in index_outcome["stages"]] == [
        "vector_ivf_rq",
        "btree_vector_id",
        "bitmap_category",
        "fts_text",
    ]
    for uri in uris:
        names: set[str] = {description.name for description in lance.dataset(uri).describe_indices()}
        assert names >= EXPECTED_INDEX_NAMES

    compact_outcome: dict[str, Any] = run_compact(config)
    for uri in uris:
        assert compact_outcome["fragments_before"][uri] >= 2
        assert compact_outcome["fragments_after"][uri] <= compact_outcome["fragments_before"][uri]

    report_outcome: dict[str, Any] = run_report(config)
    assert set(report_outcome["files"]) == {"results.csv", "summary.md"}
    run_dir: Path = config.run_dir()
    for artifact in ("prepare.json", "ingest.json", "index.json", "compact.json", "report.json"):
        assert (run_dir / artifact).exists()
    assert "# BIGANN benchmark run" in (run_dir / "summary.md").read_text(encoding="utf-8")


def experiment_argv(tmp_path: Path) -> list[str]:
    """Build the argv of the offline experiment-loop run.

    Args:
        tmp_path: The temporary workspace root.

    Returns:
        The argument vector for the ``experiment`` subcommand with a tiny sweep grid.
    """
    return [
        "experiment",
        "--dataset",
        "bigann",
        "--workspace",
        str(tmp_path / "workspace"),
        "--corpus-root",
        str(tmp_path / "corpora"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        "exp1",
        "--limit",
        str(BASE_ROWS),
        "--tenants",
        str(TENANTS),
        "--batches",
        "2",
        "--num-clusters",
        "4",
        "--words-per-cluster",
        "10",
        "--common-words",
        "5",
        "--rows-per-slice",
        "500",
        "--etl-partitions",
        "2",
        "--num-shards",
        "2",
        "--vector-row-floor",
        "100",
        "--nprobes",
        "1,4",
        "--refine-factors",
        "none",
        "--max-queries",
        "20",
        "--spark-master",
        "local[2]",
        "--driver-memory",
        "2g",
    ]


def test_offline_experiment_full_loop(tmp_path: Path) -> None:
    """The experiment loop produces metrics.json, sizes, and a history line in one command.

    When a search-api binary exists (the release or debug build), the run spawns it and the
    sweep and cold/warm first queries must be populated. Without a binary the run must still
    complete with the server and sweep recorded as skipped, so the loop degrades exactly like
    the other server-dependent legs.
    """
    corpus_root: Path = tmp_path / "corpora"
    make_bigann_fixture(corpus_root)
    config: BenchConfig = BenchConfig.from_args(build_parser().parse_args(experiment_argv(tmp_path)))

    metrics: dict[str, Any] = run_experiment(config)

    run_dir: Path = config.run_dir()
    assert (run_dir / "metrics.json").exists()
    assert (run_dir / "e2e.json").exists()

    sizes: dict[str, Any] = metrics["sizes"]
    assert sizes["dataset_count"] == TENANTS
    assert sizes["data_bytes"] > 0
    assert sizes["index_bytes"] > 0, "index builds must produce on-disk index bytes"
    assert sizes["total_bytes"] == sizes["data_bytes"] + sizes["index_bytes"] + sizes["meta_bytes"]

    assert metrics["build"]["total_seconds"] > 0
    assert len(metrics["build"]["batches"]) == 2
    assert metrics["tags"]["verified"] is True

    history: Path = config.results_root / "experiments.jsonl"
    assert history.exists()
    lines: list[dict[str, Any]] = [json.loads(line) for line in history.read_text().splitlines()]
    assert lines[-1]["run_id"] == "exp1"
    assert lines[-1]["knobs"]["batches"] == 2

    if resolve_binary(config) is None:
        assert "skipped" in metrics["sweep"]
        assert "skipped" in metrics["server"]
    else:
        assert metrics["server"]["endpoint"].startswith("localhost:")
        points: list[dict[str, Any]] = metrics["sweep"]["points"]
        assert len(points) == 2, "the nprobes x refine grid must produce one point per combination"
        for point in points:
            assert 0.0 <= point["recall_at_10"] <= 1.0
            assert point["p95_ms"] > 0
        assert metrics["headline"]["best_recall_at_10"] == max(point["recall_at_10"] for point in points)
        first_query: dict[str, Any] = metrics["sweep"]["first_query"]
        assert any("cold_ms" in timing for timing in first_query.values())
        assert (run_dir / "server.log").exists()
