"""Offline tiny-data end-to-end test of the benchmark chain through a registered fake dataset adapter.

Registers a tiny in-memory :class:`bench.datasets.SyntheticAdapter` (2000 base vectors, dimension 16, 50 queries) and
drives prepare -> ingest -> index -> compact -> report with two tenants and two ETL batches through Spark local mode,
asserting that the prepared artifacts, the per-tenant Lance datasets, all four index types, the compaction stats, and
the report files materialize. The search phase is deliberately not exercised: it requires the Rust gRPC server.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import numpy as np
import pytest

from bench.compaction import run_compact
from bench.config import BenchConfig, build_parser
from bench.datasets import DATASET_ADAPTERS, SyntheticAdapter, register_adapter
from bench.indexes import run_index
from bench.ingest import run_ingest
from bench.prepare import run_prepare
from bench.report import run_report
from bench.results import read_json

pytestmark = pytest.mark.integration

DATASET_NAME: str = "tiny-fake"
BASE_ROWS: int = 2_000
DIMENSION: int = 16
QUERY_ROWS: int = 50
TENANTS: int = 2
EXPECTED_INDEX_NAMES: frozenset[str] = frozenset({"vector_idx", "vector_id_idx", "category_bitmap_idx", "text_fts_idx"})


def tiny_config(tmp_path: Path) -> BenchConfig:
    """Build the benchmark configuration of the offline tiny run.

    Args:
        tmp_path: The temporary workspace root.

    Returns:
        The parsed configuration targeting the registered fake adapter.
    """
    argv: list[str] = [
        "all",
        "--dataset",
        DATASET_NAME,
        "--workspace",
        str(tmp_path / "workspace"),
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
    assert prepared.name == f"{DATASET_NAME}-n{BASE_ROWS}-t{TENANTS}-s42-c4"
    queries: np.ndarray = np.load(prepared / "queries.npy")
    assert queries.shape == (QUERY_ROWS, DIMENSION)
    ground_truth = np.load(prepared / "ground_truth.npz")
    assert sorted(ground_truth.files) == ["org0", "org1"]
    for org in ground_truth.files:
        assert ground_truth[org].shape == (QUERY_ROWS, 100)
    manifest: dict[str, Any] = read_json(prepared / "manifest.json")
    assert manifest["ground_truth_source"] == "brute_force"
    assert manifest["limit"] == BASE_ROWS


def test_offline_tiny_end_to_end(tmp_path: Path) -> None:
    """prepare -> ingest -> index -> compact -> report runs offline against the fake adapter."""
    adapter: SyntheticAdapter = SyntheticAdapter(
        dataset_name=DATASET_NAME,
        vector_dimension=DIMENSION,
        base_rows=BASE_ROWS,
        query_rows=QUERY_ROWS,
        seed=11,
    )
    register_adapter(adapter)
    try:
        config: BenchConfig = tiny_config(tmp_path)
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
        assert f"# {DATASET_NAME.upper()} benchmark run" in (run_dir / "summary.md").read_text(encoding="utf-8")
    finally:
        DATASET_ADAPTERS.pop(DATASET_NAME, None)
