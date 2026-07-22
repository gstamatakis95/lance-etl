"""Offline tiny-data test of the reconciler-driven benchmark e2e flow through the bigann adapter.

Writes a minimal bigann workspace fixture (1200 base vectors, 128 dims, uint8, seeded rng; 30
queries) directly into ``{tmp_path}/workspace/bigann/`` using :func:`bench.bigann_io.write_u8bin`,
with the exact filenames :class:`bench.datasets.BigannAdapter` expects for ``limit=1200``. The
download phase short-circuits (base and query files already present). The full ``run_e2e`` flow runs
with 2 batches, driving the production PostgreSQL reconciler over a partitioned Iceberg source
table. Asserts:

- One batch record per appended snapshot, each carrying reconciliation counts and per-org servings.
- Every organization's terminal publication opens at its exact version with the cumulative row count.
- Every index the installed bench specification declares, including the INVERTED full-text index,
  is present on every published dataset.
- The e2e phase artifact is saved as ``e2e.json`` in the run directory.
- The gRPC legs are recorded as ``NOT_RUN`` because ``--search-api-binary ""`` explicitly disables
  self-hosting the search leg (independent of whether a real release binary happens to be built on
  the machine running this test).

The test requires the isolated-schema integration database (``LANCE_ETL_TEST_DATABASE_URL``) and is
skipped when it is unset, matching the other PostgreSQL-backed integration tests.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from bench.bigann_io import write_u8bin
from bench.config import BenchConfig, build_parser
from bench.datasets import BigannAdapter
from bench.e2e import run_e2e
from bench.reconcile import bench_spec_index_names
from bench.results import read_json

pytestmark = pytest.mark.integration

BASE_ROWS: int = 1_200
DIMENSION: int = 128
QUERY_ROWS: int = 30
TENANTS: int = 2
BATCHES: int = 2
POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"


def make_bigann_fixture(corpus_root: Path, seed: int = 7) -> None:
    """Write a minimal bigann corpus fixture that bypasses network download.

    Creates ``{corpus_root}/bigann/base.1200.u8bin`` and ``{corpus_root}/bigann/query.10K.u8bin``
    using seeded random uint8 data. The filenames match exactly what
    :class:`bench.datasets.BigannAdapter` expects for ``limit=1200`` and the default query path.

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


def reconciled_config(tmp_path: Path) -> BenchConfig:
    """Build a tiny benchmark config for the reconciler-driven e2e flow using the bigann adapter.

    Args:
        tmp_path: Temporary workspace root.

    Returns:
        The parsed configuration targeting the bigann adapter with two batches.
    """
    argv: list[str] = [
        "e2e",
        "--dataset",
        "bigann",
        "--workspace",
        str(tmp_path / "workspace"),
        "--corpus-root",
        str(tmp_path / "corpora"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        "e2e-reconciled",
        "--limit",
        str(BASE_ROWS),
        "--tenants",
        str(TENANTS),
        "--batches",
        str(BATCHES),
        "--num-clusters",
        "4",
        "--rows-per-slice",
        "300",
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
        "--search-api-binary",
        "",
    ]
    return BenchConfig.from_args(build_parser().parse_args(argv))


def test_e2e_reconciler_publishes(tmp_path: Path) -> None:
    """run_e2e drives the reconciler over two snapshots and publishes every organization."""
    if os.environ.get(POSTGRES_URL_ENV) is None:
        pytest.skip(f"set {POSTGRES_URL_ENV} to run the reconciler-driven benchmark e2e test")
    corpus_root: Path = tmp_path / "corpora"
    make_bigann_fixture(corpus_root)

    config: BenchConfig = reconciled_config(tmp_path)

    download_outcome: dict[str, Any] = BigannAdapter(limit=BASE_ROWS).download(corpus_root)
    assert download_outcome["skipped"] is True, "download must short-circuit when base+query files exist"

    outcome: dict[str, Any] = run_e2e(config)

    assert len(outcome["batches"]) == BATCHES, "expected one batch record per appended snapshot"
    for batch_record in outcome["batches"]:
        assert batch_record["reconcile"]["cycles"] >= 1
        assert batch_record["reconcile"]["blocked"] == 0
        assert len(batch_record["servings"]) == TENANTS

    verification: dict[str, Any] = outcome["publication_verification"]
    assert verification["ok"] is True, f"publication verification failed: {verification['checks']}"

    per_org_rows: int = BASE_ROWS // TENANTS
    expected_indexes: frozenset[str] = bench_spec_index_names(config)
    assert "text_fts_idx" in expected_indexes, "bench spec must declare the INVERTED full-text index"
    for check in outcome["publications"]:
        assert check["published"] is True
        assert check["row_count"] == per_org_rows, f"org {check['org']}: expected {per_org_rows} rows"
        assert not check["missing_indexes"], f"org {check['org']}: missing indexes {check['missing_indexes']}"
        assert expected_indexes.issubset(set(check["indexes"])), (
            f"org {check['org']}: published indexes {check['indexes']} miss declared {sorted(expected_indexes)}"
        )

    run_dir: Path = config.run_dir()
    assert (run_dir / "e2e.json").exists(), "e2e.json phase artifact not written"
    artifact: dict[str, Any] = read_json(run_dir / "e2e.json")
    assert artifact["phase"] == "e2e"

    assert outcome["final_catalog_grpc"]["status"] == "NOT_RUN"
    assert outcome["final_catalog_recall"] == {}
