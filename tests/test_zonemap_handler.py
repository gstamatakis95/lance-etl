"""Tests for ZonemapIndexHandler: naming, registry wiring, and the segment-API build path.

Drives the executor-task layer directly on a local-fs dataset, with no Spark involved, plus one
end-to-end pass through the unified fleet run. Covers:

- :func:`lance_etl.indexing.config.zonemap_index_name` naming convention.
- :class:`lance_etl.indexing.handlers.ZonemapIndexHandler` type and merge-before-commit policy.
- Registration in :data:`lance_etl.indexing.runner.KIND_TO_HANDLER` and dispatch through
  :func:`lance_etl.indexing.runner.make_handler`.
- The real segment-API flow: per-shard ``create_index_uncommitted``, driver-side
  ``merge_existing_index_segments`` (unlike BTREE and BITMAP, which commit unmerged), then
  ``commit_existing_index_segments``, followed by an incremental build over only the newly
  uncovered fragments.
- ZONEMAP is explicit-config-only: it carries no column role, so it is never discovered from a
  dataset's ``lance-etl.columns`` metadata.
- The unified :class:`~lance_etl.indexing.runner.LanceIndexer` plan-artifacts-build-commit run
  builds a ZONEMAP index end-to-end alongside the other index types.
- The delta-bound maintenance pass (:func:`~lance_etl.indexing.optimize.merge_index_deltas`,
  ``optimize_indices`` under the hood) also accepts ZONEMAP and merges its accumulated deltas.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark, make_vector_table, write_fragmented_dataset
from lance.dataset import Index

from lance_etl.indexing import (
    BTreeIndexHandler,
    IndexJobConfig,
    LanceIndexer,
    ZonemapIndexHandler,
    commit_segments,
    merge_index_deltas,
    resolve_index_targets,
    serialize_segment,
    split_evenly,
    zonemap_index_name,
)
from lance_etl.indexing.runner import KIND_TO_HANDLER, ZONEMAP_KIND, make_handler
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 1024
DIM: int = 8
ROWS_PER_FRAGMENT: int = 256


@pytest.fixture
def dataset_uri(tmp_path: Path) -> str:
    """Write a four-fragment local dataset and return its URI.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "zonemap.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def zonemap_config(**overrides: object) -> IndexJobConfig:
    """Build an indexing configuration with one zonemap column.

    Args:
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        A configuration targeting the ``id`` column with zonemap.
    """
    base: dict[str, object] = {
        "telemetry": TelemetryConfig(),
        "zonemap_columns": ["id"],
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return IndexJobConfig(**base)


def append_fragment(uri: str, rows: int, start_id: int) -> None:
    """Append one new fragment of rows to a dataset.

    Args:
        uri: The dataset URI.
        rows: How many rows to append.
        start_id: The first id value of the appended range.
    """
    table: pa.Table = make_vector_table(rows=rows, dim=DIM, seed=start_id)
    reindexed: pa.Table = table.set_column(0, "id", pa.array(range(start_id, start_id + rows), pa.int64()))
    lance.write_dataset(reindexed, uri, mode="append")


def fragment_ids_of(uri: str) -> list[int]:
    """Return the fragment ids of a dataset.

    Args:
        uri: The dataset URI.

    Returns:
        The fragment ids in dataset order.
    """
    return [fragment.fragment_id for fragment in lance.dataset(uri).get_fragments()]


def run_segment_path(uri: str, handler: ZonemapIndexHandler, shards: int, telemetry: Telemetry) -> None:
    """Run the segment-API path end-to-end without Spark.

    Mirrors the executor task and driver steps the runner uses: per-shard ``build_segment``
    against a version-pinned handle, driver-side serialization, then ``commit_segments`` with the
    handler's merge policy.

    Args:
        uri: The dataset URI.
        handler: The zonemap handler supplying the per-shard segment build.
        shards: How many shards to split the fragments into.
        telemetry: The driver telemetry facade.
    """
    version: int = lance.dataset(uri).version
    documents: list[str] = []
    for group in split_evenly(fragment_ids_of(uri), shards):
        segment: Index = handler.build_segment(lance.dataset(uri, version=version), group, None)
        documents.append(serialize_segment(segment))
    commit_segments(uri, documents, handler.column, handler.index_name, handler.merges(), handler.config, telemetry)


def index_segments(uri: str, index_name: str) -> list[object]:
    """Return the committed segments of an index.

    Args:
        uri: The dataset URI.
        index_name: The index to inspect.

    Returns:
        The segment descriptions, in index order.
    """
    segments: list[object] = []
    for description in lance.dataset(uri).describe_indices():
        if description.name == index_name:
            segments.extend(description.segments)
    return segments


def listed_index_names(uri: str) -> list[str]:
    """Return the committed index names of a dataset.

    Args:
        uri: The dataset URI.

    Returns:
        The index names.
    """
    return [item["name"] for item in lance.dataset(uri).list_indices()]


def index_coverage(uri: str, index_name: str) -> set[int]:
    """Return the union of fragment ids covered by an index's segments.

    Args:
        uri: The dataset URI.
        index_name: The index to inspect.

    Returns:
        The covered fragment ids.
    """
    covered: set[int] = set()
    for description in lance.dataset(uri).describe_indices():
        if description.name == index_name:
            for segment in description.segments:
                covered.update(segment.fragment_ids)
    return covered


def test_zonemap_index_name() -> None:
    """``zonemap_index_name`` follows the ``{column}_zonemap_idx`` convention."""
    assert zonemap_index_name("score") == "score_zonemap_idx"
    assert zonemap_index_name("id") == "id_zonemap_idx"


def test_zonemap_handler_index_type() -> None:
    """``ZonemapIndexHandler.index_type()`` returns the string ``ZONEMAP``."""
    config: IndexJobConfig = zonemap_config()
    handler: ZonemapIndexHandler = ZonemapIndexHandler(config, "id", "id_zonemap_idx")
    assert handler.index_type() == "ZONEMAP"


def test_zonemap_handler_merges_before_commit() -> None:
    """``ZonemapIndexHandler.merges()`` returns ``True``, unlike BTREE and BITMAP.

    ZONEMAP is the only scalar type that merges its per-shard segments into one before
    publishing.
    """
    config: IndexJobConfig = zonemap_config()
    handler: ZonemapIndexHandler = ZonemapIndexHandler(config, "id", "id_zonemap_idx")
    assert handler.merges() is True
    assert BTreeIndexHandler(config, "id", "id_idx").merges() is False


def test_zonemap_registered_in_kind_to_handler() -> None:
    """``ZONEMAP_KIND`` dispatches to :class:`ZonemapIndexHandler` through ``make_handler``."""
    assert KIND_TO_HANDLER[ZONEMAP_KIND] is ZonemapIndexHandler
    config: IndexJobConfig = zonemap_config()
    handler = make_handler(ZONEMAP_KIND, "id", "id_zonemap_idx", config)
    assert isinstance(handler, ZonemapIndexHandler)


def test_zonemap_has_no_role_based_discovery(dataset_uri: str) -> None:
    """ZONEMAP is explicit-config-only: it is never discovered from column-role metadata.

    Only ``vector``, ``scalar``, and ``text`` roles drive role-based discovery, so a dataset
    whose ``lance-etl.columns`` metadata marks ``id`` as ``scalar`` still resolves to a BTREE
    target, never a ZONEMAP one, absent an explicit ``zonemap_columns`` entry.
    """
    lance.dataset(dataset_uri).update_config({"lance-etl.columns": '{"id": "scalar"}'})
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig())
    targets = resolve_index_targets(lance.dataset(dataset_uri), config)
    assert ("zonemap", "id", "id_zonemap_idx") not in targets
    assert ("btree", "id", "id_idx") in targets


def test_zonemap_segment_path_end_to_end(dataset_uri: str, telemetry: Telemetry) -> None:
    """ZONEMAP: per-shard uncommitted build, merged into one segment, then filter with the index."""
    config: IndexJobConfig = zonemap_config()
    handler: ZonemapIndexHandler = ZonemapIndexHandler(config, "id", "id_zonemap_idx")
    run_segment_path(dataset_uri, handler, shards=4, telemetry=telemetry)

    assert "id_zonemap_idx" in listed_index_names(dataset_uri)
    segments: list[object] = index_segments(dataset_uri, "id_zonemap_idx")
    assert len(segments) == 1, "four shard segments must merge into one before commit"
    assert index_coverage(dataset_uri, "id_zonemap_idx") == set(fragment_ids_of(dataset_uri))

    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    descriptions = {description.name: description for description in dataset.describe_indices()}
    assert descriptions["id_zonemap_idx"].index_type == "ZoneMap"

    with_index = dataset.scanner(filter="id >= 100 AND id < 200", use_scalar_index=True).to_table()
    without_index = dataset.scanner(filter="id >= 100 AND id < 200", use_scalar_index=False).to_table()
    assert with_index.num_rows == without_index.num_rows == 100
    plan: str = dataset.scanner(filter="id >= 100 AND id < 200").explain_plan(True)
    assert "ScalarIndexQuery" in plan


def test_zonemap_segment_path_incremental_covers_new_fragment(dataset_uri: str, telemetry: Telemetry) -> None:
    """A second build over an appended fragment extends coverage and stays merged to one segment."""
    config: IndexJobConfig = zonemap_config()
    handler: ZonemapIndexHandler = ZonemapIndexHandler(config, "id", "id_zonemap_idx")
    run_segment_path(dataset_uri, handler, shards=4, telemetry=telemetry)
    first_coverage: set[int] = index_coverage(dataset_uri, "id_zonemap_idx")
    assert first_coverage == set(fragment_ids_of(dataset_uri))

    append_fragment(dataset_uri, ROWS_PER_FRAGMENT, ROWS)
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    new_targets: list[int] = handler.target_fragments(dataset)
    assert len(new_targets) == 1

    version: int = dataset.version
    segment: Index = handler.build_segment(lance.dataset(dataset_uri, version=version), new_targets, None)
    commit_segments(
        dataset_uri, [serialize_segment(segment)], "id", "id_zonemap_idx", handler.merges(), config, telemetry
    )

    assert index_coverage(dataset_uri, "id_zonemap_idx") == set(fragment_ids_of(dataset_uri))
    committed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert committed.to_table(filter=f"id = {ROWS}").num_rows == 1


def test_unified_run_builds_zonemap_end_to_end(dataset_uri: str) -> None:
    """The unified fleet run builds a ZONEMAP index end-to-end through all phases."""
    config: IndexJobConfig = zonemap_config(fragments_per_index_task=1)
    results: list[dict[str, object]] = LanceIndexer(config).run(FakeSpark(), [dataset_uri])

    assert "id_zonemap_idx" in listed_index_names(dataset_uri)
    segments: list[object] = index_segments(dataset_uri, "id_zonemap_idx")
    assert len(segments) == 1, "the unified run must merge zonemap segments before commit"
    by_index: dict[str, dict[str, object]] = {item["index"]: item for item in results[0]["indexes"]}
    assert by_index["id_zonemap_idx"]["segments"] == len(fragment_ids_of(dataset_uri))

    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert dataset.to_table(filter="id = 7").num_rows == 1


def test_zonemap_delta_bound_merges_accumulated_deltas(dataset_uri: str, telemetry: Telemetry) -> None:
    """``merge_index_deltas`` (``optimize_indices``) also accepts and merges ZONEMAP deltas.

    Each fragment is committed as its own single-segment delta (one commit per fragment, so
    ``merges()`` never fires within a call), simulating one incremental run per fragment. Once
    the accumulated delta count exceeds ``max_index_deltas``, the fleet's delta-bound pass merges
    them into one, the same maintenance path BTREE, BITMAP, and IVF_RQ share.
    """
    config: IndexJobConfig = zonemap_config(max_index_deltas=0)
    handler: ZonemapIndexHandler = ZonemapIndexHandler(config, "id", "id_zonemap_idx")
    version: int = lance.dataset(dataset_uri).version
    for fragment_id in fragment_ids_of(dataset_uri):
        segment: Index = handler.build_segment(lance.dataset(dataset_uri, version=version), [fragment_id], None)
        commit_segments(dataset_uri, [serialize_segment(segment)], "id", "id_zonemap_idx", False, config, telemetry)

    before: list[object] = index_segments(dataset_uri, "id_zonemap_idx")
    assert len(before) == len(fragment_ids_of(dataset_uri))

    merged: bool = merge_index_deltas(dataset_uri, "id_zonemap_idx", config, telemetry)
    assert merged is True

    after: list[object] = index_segments(dataset_uri, "id_zonemap_idx")
    assert len(after) == 1
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert dataset.to_table(filter="id >= 100 AND id < 200").num_rows == 100
