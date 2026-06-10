"""Integration tests for the distributed index segment paths, pure Lance.

Each test drives the executor-task layer directly on a local-fs dataset, with no Spark involved: per-shard build,
driver-side merge, then commit, asserting the index is listed and queryable afterwards.
"""

from __future__ import annotations

import json
import pickle
import uuid
from pathlib import Path

import lance
import pytest
from conftest import make_vector_table, write_fragmented_dataset
from lance.dataset import Index

from lance_etl.indexing import (
    BitmapIndexHandler,
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    IndexJobConfig,
    VectorIndexHandler,
    centroids_from_ipc,
    commit_segments,
    index_dataset_locally,
    lance_field_id,
    load_vector_config,
    serialize_segment,
    split_evenly,
    vector_config_key,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 2048
DIM: int = 8
ROWS_PER_FRAGMENT: int = 512


@pytest.fixture
def dataset_uri(tmp_path: Path) -> str:
    """Write a four-fragment local dataset and return its URI.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "segments.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def index_config() -> IndexJobConfig:
    """Build the indexing configuration used by the segment-path tests.

    Returns:
        A configuration with a small explicit partition count and no row floor.
    """
    return IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_columns=["vector"],
        num_partitions=4,
        vector_min_rows=1,
        scalar_columns=["id"],
        bitmap_columns=["category"],
        text_columns=["text"],
        commit_retries=5,
        commit_backoff_seconds=0.0,
    )


def test_handler_segment_builder_is_picklable() -> None:
    """The ``segment_builder`` callable captured by the Spark closure pickles cleanly and stays small.

    ``IndexHandler.build`` ships ``segment_builder()`` to executors inside the closure. It is a
    :func:`functools.partial` over a module-level function bound to primitive values only, so it must round-trip
    through pickle without dragging the handler instance (and its ``config`` with ``storage_options`` and
    ``telemetry``) onto every task.
    """
    config: IndexJobConfig = index_config()
    handlers: list[IndexHandler] = [
        VectorIndexHandler(config, "vector", "vector_idx"),
        BTreeIndexHandler(config, "id", "id_idx"),
        BitmapIndexHandler(config, "category", "category_idx"),
    ]
    for handler in handlers:
        builder: object = handler.segment_builder()
        payload: bytes = pickle.dumps(builder)
        restored: object = pickle.loads(payload)
        assert callable(restored)
        assert len(payload) < len(pickle.dumps(handler))


def fragment_ids_of(uri: str) -> list[int]:
    """Return the fragment ids of a dataset.

    Args:
        uri: The dataset URI.

    Returns:
        The fragment ids in dataset order.
    """
    return [fragment.fragment_id for fragment in lance.dataset(uri).get_fragments()]


def run_fts_segment_path(uri: str, handler: FtsIndexHandler, shards: int, **params: object) -> None:
    """Run the distributed inverted-index path end-to-end without Spark.

    Mirrors the executor task and driver steps: per-shard ``create_scalar_index`` under one shared index uuid, driver
    ``merge_index_metadata``, then a ``LanceOperation.CreateIndex`` commit.

    Args:
        uri: The dataset URI.
        handler: The full-text index handler.
        shards: How many shards to split the fragments into.
        params: Extra keyword arguments for ``create_scalar_index``.
    """
    dataset: lance.LanceDataset = lance.dataset(uri)
    fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
    version: int = dataset.version
    index_uuid: str = str(uuid.uuid4())
    for group in split_evenly(fragment_ids, shards):
        shard_dataset: lance.LanceDataset = lance.dataset(uri, version=version)
        shard_dataset.create_scalar_index(
            column=handler.column,
            index_type="INVERTED",
            name=handler.index_name,
            replace=False,
            index_uuid=index_uuid,
            fragment_ids=group,
            **params,
        )
    dataset.merge_index_metadata(index_uuid, index_type=handler.index_type())
    current: lance.LanceDataset = lance.dataset(uri)
    index: Index = Index(
        uuid=index_uuid,
        name=handler.index_name,
        fields=[lance_field_id(current, handler.column)],
        dataset_version=current.version,
        fragment_ids=set(fragment_ids),
        index_version=0,
    )
    operation = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=[])
    lance.LanceDataset.commit(uri, operation, read_version=current.version)


def run_segment_path(uri: str, handler: IndexHandler, shards: int, telemetry: Telemetry) -> None:
    """Run the segment-API path end-to-end without Spark.

    Mirrors the executor task and driver steps the repaired handlers use: per-shard ``build_segment`` against a
    version-pinned handle, driver-side serialization, then ``commit_segments`` with the handler's merge policy.

    Args:
        uri: The dataset URI.
        handler: The handler supplying the per-shard segment build.
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


def test_vector_segment_path_end_to_end(dataset_uri: str, telemetry: Telemetry) -> None:
    """IVF_RQ: train, per-shard uncommitted build, merge, commit, then query."""
    config: IndexJobConfig = index_config()
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    handler.validate(dataset)
    artifacts: object | None = handler.prepare(dataset, dataset_uri, telemetry)
    assert artifacts is not None
    centroids_bytes, rabitq_model, num_bits, num_partitions = artifacts
    assert isinstance(centroids_bytes, bytes)
    assert isinstance(rabitq_model, str)
    json.loads(rabitq_model)
    assert num_bits == 1
    assert num_partitions == 4
    version: int = dataset.version
    documents: list[str] = []
    for group in split_evenly(fragment_ids_of(dataset_uri), 2):
        shard_dataset: lance.LanceDataset = lance.dataset(dataset_uri, version=version)
        segment: Index = handler.build_segment(shard_dataset, group, artifacts)
        documents.append(serialize_segment(segment))
    assert len(documents) == 2
    commit_segments(dataset_uri, documents, "vector", "vector_idx", True, config, telemetry)

    committed: lance.LanceDataset = lance.dataset(dataset_uri)
    indices: list[dict[str, object]] = committed.list_indices()
    assert [(item["name"], item["type"]) for item in indices] == [("vector_idx", "IVF_RQ")]
    assert index_coverage(dataset_uri, "vector_idx") == set(fragment_ids_of(dataset_uri))
    result = committed.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 5})
    assert result.num_rows == 5


def test_vector_segment_path_reuses_artifacts(dataset_uri: str, telemetry: Telemetry) -> None:
    """A second prepare reads centroids from the committed index and the rotation from the dataset config.

    Centroids are recovered via ``get_ivf_model`` and IPC-serialized, so the round-trip is checked by
    array equality through ``centroids_from_ipc``. The ``rabitq_model`` string must match the first prepare
    so independently built segments remain mergeable across runs. No ``.artifacts`` directory is created.
    """
    config: IndexJobConfig = index_config()
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    first: object | None = handler.prepare(dataset, dataset_uri, telemetry)
    assert handler.reused_artifacts is False

    version: int = dataset.version
    documents: list[str] = []
    for group in split_evenly(fragment_ids_of(dataset_uri), 2):
        segment: Index = handler.build_segment(lance.dataset(dataset_uri, version=version), group, first)
        documents.append(serialize_segment(segment))
    commit_segments(dataset_uri, documents, "vector", "vector_idx", True, config, telemetry)

    second_handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    second: object | None = second_handler.prepare(lance.dataset(dataset_uri), dataset_uri, telemetry)
    assert second_handler.reused_artifacts is True

    first_centroids = centroids_from_ipc(first[0])
    second_centroids = centroids_from_ipc(second[0])
    assert first_centroids.equals(second_centroids)
    assert second[1] == first[1]
    assert second[2] == first[2]
    assert second[3] == first[3]

    cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert cfg is not None
    assert "rabitq_model" in cfg
    assert cfg["rabitq_model"] == first[1]

    assert vector_config_key("vector") in lance.dataset(dataset_uri).config()
    assert not Path(f"{dataset_uri}.artifacts").exists()


def test_btree_segment_path_end_to_end(dataset_uri: str, telemetry: Telemetry) -> None:
    """BTREE: per-shard uncommitted build, unmerged commit, then filter by the column."""
    config: IndexJobConfig = index_config()
    handler: BTreeIndexHandler = BTreeIndexHandler(config, "id", "id_idx")
    assert handler.merges() is False
    run_segment_path(dataset_uri, handler, shards=2, telemetry=telemetry)
    assert "id_idx" in listed_index_names(dataset_uri)
    segments: list[object] = index_segments(dataset_uri, "id_idx")
    assert len(segments) == 2
    covered: set[int] = set()
    for segment in segments:
        covered.update(segment.fragment_ids)
    assert covered == set(fragment_ids_of(dataset_uri))
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert dataset.to_table(filter="id = 7").num_rows == 1
    plan: str = dataset.scanner(filter="id = 7").explain_plan(True)
    assert "ScalarIndexQuery" in plan


def test_bitmap_segment_path_end_to_end(dataset_uri: str, telemetry: Telemetry) -> None:
    """BITMAP: per-shard uncommitted build, driver merge to one segment, then filter."""
    config: IndexJobConfig = index_config()
    handler: BitmapIndexHandler = BitmapIndexHandler(config, "category", "category_bitmap_idx")
    assert handler.merges() is True
    run_segment_path(dataset_uri, handler, shards=2, telemetry=telemetry)
    assert "category_bitmap_idx" in listed_index_names(dataset_uri)
    segments: list[object] = index_segments(dataset_uri, "category_bitmap_idx")
    assert len(segments) == 1
    assert set(segments[0].fragment_ids) == set(fragment_ids_of(dataset_uri))
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert dataset.to_table(filter="category = 'cat1'").num_rows == ROWS // 4
    plan: str = dataset.scanner(filter="category = 'cat1'").explain_plan(True)
    assert "ScalarIndexQuery" in plan


def test_inverted_segment_path_end_to_end(dataset_uri: str) -> None:
    """INVERTED: per-shard build, metadata merge, commit, then full-text query."""
    config: IndexJobConfig = index_config()
    handler: FtsIndexHandler = FtsIndexHandler(config, "text", "text_fts_idx")
    run_fts_segment_path(dataset_uri, handler, shards=2, **config.fts_params())
    assert "text_fts_idx" in listed_index_names(dataset_uri)
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    expected: int = sum(1 for i in range(ROWS) if i % 10 == 3)
    result = dataset.to_table(full_text_query="word3")
    assert result.num_rows == expected
    assert set(value % 10 for value in result["id"].to_pylist()) == {3}


def test_scalar_fragment_sharding_requires_segment_api(dataset_uri: str) -> None:
    """The retired uuid-sharing scalar path is rejected for BTREE and BITMAP.

    Pins updated-main behavior so a regression back to per-shard ``create_scalar_index(index_uuid=, fragment_ids=)`` for
    BTREE or BITMAP is caught immediately: those types must go through ``create_index_uncommitted`` without a
    caller-supplied ``index_uuid``.
    """
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    first_fragment: int = dataset.get_fragments()[0].fragment_id
    with pytest.raises(ValueError, match="create_index_uncommitted"):
        dataset.create_scalar_index(column="id", index_type="BTREE", fragment_ids=[first_fragment])
    with pytest.raises((ValueError, RuntimeError), match="index_uuid"):
        dataset.create_index_uncommitted(
            column="id",
            index_type="BTREE",
            name="id_guard_idx",
            fragment_ids=[first_fragment],
            index_uuid=str(uuid.uuid4()),
        )


def test_index_dataset_locally_builds_all_types(dataset_uri: str) -> None:
    """The tier-A executor task builds every configured index in one process."""
    config: IndexJobConfig = index_config()
    result: dict[str, object] = index_dataset_locally(dataset_uri, config)
    names: list[str] = listed_index_names(dataset_uri)
    assert {"vector_idx", "id_idx", "category_bitmap_idx", "text_fts_idx"} <= set(names)
    by_index: dict[str, dict[str, object]] = {item["index"]: item for item in result["indexes"]}
    assert by_index["vector_idx"]["num_partitions"] == 4
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert dataset.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 3}).num_rows == 3
    expected: int = sum(1 for i in range(ROWS) if i % 10 == 2)
    assert dataset.to_table(full_text_query="word2").num_rows == expected
