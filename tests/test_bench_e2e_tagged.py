"""Offline tiny-data test of the e2e batch-major flow through a registered fake dataset adapter.

Registers a tiny in-memory SyntheticAdapter (1200 base vectors, dimension 16, 30 queries) and runs
the full ``run_e2e`` flow with 2 batches and ``--no-text`` set, using Spark local mode. Asserts:

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
import pytest

from bench.config import BenchConfig, build_parser
from bench.datasets import DATASET_ADAPTERS, SyntheticAdapter, register_adapter
from bench.e2e import run_e2e, window_tag_name
from bench.ingest import batch_windows
from bench.prepare import run_prepare
from bench.results import read_json

pytestmark = pytest.mark.integration

DATASET_NAME: str = "tiny-e2e-tagged"
BASE_ROWS: int = 1_200
DIMENSION: int = 16
QUERY_ROWS: int = 30
TENANTS: int = 2
BATCHES: int = 2
EXPECTED_NO_TEXT_INDEX_NAMES: frozenset[str] = frozenset({"vector_idx", "vector_id_idx", "category_bitmap_idx"})


def tagged_config(tmp_path: Path) -> BenchConfig:
    """Build a tiny benchmark config for the e2e tagged flow.

    Args:
        tmp_path: Temporary workspace root.

    Returns:
        The parsed configuration targeting the registered fake adapter with no_text enabled.
    """
    argv: list[str] = [
        "e2e",
        "--dataset",
        DATASET_NAME,
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
    adapter: SyntheticAdapter = SyntheticAdapter(
        dataset_name=DATASET_NAME,
        vector_dimension=DIMENSION,
        base_rows=BASE_ROWS,
        query_rows=QUERY_ROWS,
        seed=7,
    )
    register_adapter(adapter)
    try:
        config: BenchConfig = tagged_config(tmp_path)

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

    finally:
        DATASET_ADAPTERS.pop(DATASET_NAME, None)
