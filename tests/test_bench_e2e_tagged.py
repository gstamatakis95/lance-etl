"""Offline tiny-data test of the e2e batch-major flow through the bigann adapter.

Writes a minimal bigann workspace fixture (1200 base vectors, 128 dims, uint8, seeded rng; 30
queries) directly into ``{tmp_path}/workspace/bigann/`` using :func:`bench.bigann_io.write_u8bin`,
with the exact filenames :class:`bench.datasets.BigannAdapter` expects for ``limit=1200``. The
download phase short-circuits (base and query files already present). ``gt_member_name(1200)``
returns ``None`` so the prepare phase computes exact brute-force ground truth. The full
``run_e2e`` flow runs with 2 batches and ``--no-text`` through Spark local mode. Asserts:

- One tag is created per batch.
- Tag names match the colon-free window-end format ``%Y%m%dT%H%M%SZ``.
- ``lance.dataset(uri, version=tag).count_rows()`` equals cumulative rows for each tag.
- The older tag is still readable after the second batch's compaction.
- Only 3 indexes exist under ``--no-text`` (no INVERTED index).
- The e2e phase artifact is saved as ``e2e.json`` in the run directory.
- The gRPC legs are skipped gracefully (server unreachable in the test environment).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import numpy as np
import pytest

from bench.bigann_io import write_u8bin
from bench.config import BenchConfig, build_parser
from bench.datasets import BigannAdapter
from bench.e2e import run_e2e, window_tag_name
from bench.ingest import batch_windows
from bench.prepare import run_prepare
from bench.results import read_json

pytestmark = pytest.mark.integration

BASE_ROWS: int = 1_200
DIMENSION: int = 128
QUERY_ROWS: int = 30
TENANTS: int = 2
BATCHES: int = 2
EXPECTED_NO_TEXT_INDEX_NAMES: frozenset[str] = frozenset({"vector_idx", "vector_id_idx", "category_bitmap_idx"})


def make_bigann_fixture(workspace: Path, seed: int = 7) -> None:
    """Write a minimal bigann workspace fixture that bypasses network download.

    Creates ``{workspace}/bigann/base.1200.u8bin`` and
    ``{workspace}/bigann/query.10K.u8bin`` using seeded random uint8 data.
    The filenames match exactly what :class:`bench.datasets.BigannAdapter` expects
    for ``limit=1200`` and the default query path.

    Args:
        workspace: The benchmark workspace directory.
        seed: RNG seed for reproducible vectors.
    """
    bigann_dir: Path = workspace / "bigann"
    bigann_dir.mkdir(parents=True, exist_ok=True)
    rng: np.random.Generator = np.random.default_rng(seed)
    base: np.ndarray = rng.integers(0, 256, size=(BASE_ROWS, DIMENSION), dtype=np.uint8).astype(np.float32)
    queries: np.ndarray = rng.integers(0, 256, size=(QUERY_ROWS, DIMENSION), dtype=np.uint8).astype(np.float32)
    adapter: BigannAdapter = BigannAdapter(limit=BASE_ROWS)
    write_u8bin(adapter.base_path(workspace), base)
    write_u8bin(adapter.query_path(workspace), queries)


def tagged_config(tmp_path: Path) -> BenchConfig:
    """Build a tiny benchmark config for the e2e tagged flow using the bigann adapter.

    Args:
        tmp_path: Temporary workspace root.

    Returns:
        The parsed configuration targeting the bigann adapter with no_text enabled.
    """
    argv: list[str] = [
        "e2e",
        "--dataset",
        "bigann",
        "--workspace",
        str(tmp_path / "workspace"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        "e2e-tagged",
        "--limit",
        str(BASE_ROWS),
        "--tenants",
        str(TENANTS),
        "--batches",
        str(BATCHES),
        "--num-clusters",
        "4",
        "--words-per-cluster",
        "10",
        "--common-words",
        "5",
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
        "--no-text",
    ]
    return BenchConfig.from_args(build_parser().parse_args(argv))


def test_e2e_tagged_no_text(tmp_path: Path) -> None:
    """run_e2e with 2 batches and --no-text creates per-batch tags and passes historical verification."""
    workspace: Path = tmp_path / "workspace"
    make_bigann_fixture(workspace)

    config: BenchConfig = tagged_config(tmp_path)

    download_outcome: dict[str, Any] = BigannAdapter(limit=BASE_ROWS).download(workspace)
    assert download_outcome["skipped"] is True, "download must short-circuit when base+query files exist"

    run_prepare(config)

    outcome: dict[str, Any] = run_e2e(config)

    assert len(outcome["batches"]) == BATCHES, "expected one batch record per ETL window"

    tags_created: list[str] = [b["tag"] for b in outcome["batches"]]
    assert len(tags_created) == BATCHES

    windows: list[tuple[str, str]] = batch_windows(BATCHES)
    for batch_index, window in enumerate(windows):
        expected_tag: str = window_tag_name(window[1])
        assert tags_created[batch_index] == expected_tag, (
            f"batch {batch_index}: expected tag {expected_tag!r}, got {tags_created[batch_index]!r}"
        )
        assert ":" not in expected_tag, f"tag {expected_tag!r} contains a colon"

    uris: list[str] = config.dataset_uris()

    tag_to_row_counts: dict[str, dict[str, int]] = {}
    for batch_record in outcome["batches"]:
        tag: str = batch_record["tag"]
        tag_to_row_counts[tag] = {stat["uri"]: stat["row_count"] for stat in batch_record["tag_stats"]}
        pipeline_block: dict[str, Any] = batch_record["pipeline"]
        assert "seconds" in pipeline_block, f"batch {batch_record['batch']}: pipeline block missing 'seconds'"
        assert "counts" in pipeline_block, f"batch {batch_record['batch']}: pipeline block missing 'counts'"
        assert "maintenance_datasets" in pipeline_block, (
            f"batch {batch_record['batch']}: pipeline block missing 'maintenance_datasets'"
        )
        assert "index_datasets" in pipeline_block, (
            f"batch {batch_record['batch']}: pipeline block missing 'index_datasets'"
        )

    for tag, uri_counts in tag_to_row_counts.items():
        for uri, expected_count in uri_counts.items():
            ds = lance.dataset(uri, version=tag)
            actual: int = ds.count_rows()
            assert actual == expected_count, f"tag {tag!r} on {uri}: expected {expected_count} rows, got {actual}"

    first_tag: str = tags_created[0]
    first_tag_counts: dict[str, int] = tag_to_row_counts[first_tag]
    for uri in uris:
        ds_old = lance.dataset(uri, version=first_tag)
        expected_first: int = first_tag_counts[uri]
        assert ds_old.count_rows() == expected_first, (
            f"older tag {first_tag!r} on {uri} is unreadable after second batch compaction"
        )

    last_tag: str = tags_created[-1]
    last_tag_counts: dict[str, int] = tag_to_row_counts[last_tag]
    for uri in uris:
        assert last_tag_counts[uri] >= first_tag_counts[uri], (
            f"last tag {last_tag!r} has fewer rows than first tag {first_tag!r} on {uri}"
        )

    for uri in uris:
        ds_latest = lance.dataset(uri)
        index_names: set[str] = {desc.name for desc in ds_latest.describe_indices()}
        assert index_names >= EXPECTED_NO_TEXT_INDEX_NAMES, (
            f"{uri}: missing indexes; got {index_names}, expected superset of {EXPECTED_NO_TEXT_INDEX_NAMES}"
        )
        inverted_names: set[str] = {n for n in index_names if "fts" in n.lower() or "inverted" in n.lower()}
        assert not inverted_names, f"{uri}: found unexpected FTS/INVERTED indexes: {inverted_names}"

    verification: dict[str, Any] = outcome["historical_tag_verification"]
    assert verification["ok"] is True, f"historical tag verification failed: {verification['checks']}"

    run_dir: Path = config.run_dir()
    assert (run_dir / "e2e.json").exists(), "e2e.json phase artifact not written"
    artifact: dict[str, Any] = read_json(run_dir / "e2e.json")
    assert artifact["phase"] == "e2e"

    for batch_record in outcome["batches"]:
        grpc_result: dict[str, Any] = outcome["grpc_legs_per_tag"].get(batch_record["tag"], {})
        assert "skipped" in grpc_result, (
            f"gRPC leg for tag {batch_record['tag']!r} was not skipped as expected when no server is running"
        )
