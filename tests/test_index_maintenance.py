"""Tests for incremental index maintenance, delta bounding, retrain triggers, and stale-commit guards.

Drives the executor-task layer directly on local-fs datasets, with no Spark involved, covering the small-tier
maintain-instead-of-rebuild path, ``optimize_indices`` coverage extension, delta merging via ``index_stats``, the
IVF ``rows_at_train`` growth retrain trigger, the FTS incremental-maintenance gate, and the guards that stop stale
index work from being published after a concurrent compaction.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
import pytest
from conftest import make_vector_table, write_fragmented_dataset
from lance.dataset import Index
from lance.optimize import Compaction

from lance_etl.indexing import (
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexJobConfig,
    VectorIndexHandler,
    bootstrap_vector_index,
    commit_segments,
    index_delta_count,
    load_vector_config,
    merge_index_deltas,
    optimize_existing_index,
    plan_dataset_indexes,
    publish_fts_index,
    serialize_segment,
    split_evenly,
    vector_config_key,
    write_vector_config,
)
from lance_etl.maintenance import MaintenanceConfig
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
    uri: str = str(tmp_path / "maintenance.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def vector_only_config(**overrides: object) -> IndexJobConfig:
    """Build an explicit vector-only indexing configuration for plan-phase assertions.

    Args:
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        The indexing configuration targeting only the ``vector`` column.
    """
    base: dict[str, object] = {
        "telemetry": TelemetryConfig(),
        "vector_columns": ["vector"],
        "num_partitions": 4,
        "vector_min_rows": 10,
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return IndexJobConfig(**base)


def maintenance_config(**overrides: object) -> IndexJobConfig:
    """Build the indexing configuration used by the maintenance tests.

    Args:
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        A configuration with a small explicit partition count and no row floor.
    """
    base: dict[str, object] = {
        "telemetry": TelemetryConfig(),
        "vector_columns": ["vector"],
        "num_partitions": 4,
        "vector_min_rows": 1,
        "scalar_columns": ["id"],
        "bitmap_columns": ["category"],
        "text_columns": ["text"],
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return IndexJobConfig(**base)


def fragment_ids_of(uri: str) -> list[int]:
    """Return the fragment ids of a dataset.

    Args:
        uri: The dataset URI.

    Returns:
        The fragment ids in dataset order.
    """
    return [fragment.fragment_id for fragment in lance.dataset(uri).get_fragments()]


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


def build_btree_segments(uri: str, config: IndexJobConfig, telemetry: Telemetry, shards: int) -> None:
    """Build and commit BTREE segments over every fragment, mirroring the segment path.

    Args:
        uri: The dataset URI.
        config: Indexing configuration.
        telemetry: Telemetry facade.
        shards: How many shards to split the fragments into.
    """
    handler: BTreeIndexHandler = BTreeIndexHandler(config, "id", "id_idx")
    version: int = lance.dataset(uri).version
    documents: list[str] = []
    for group in split_evenly(fragment_ids_of(uri), shards):
        segment: Index = handler.build_segment(lance.dataset(uri, version=version), group, None)
        documents.append(serialize_segment(segment))
    commit_segments(uri, documents, "id", "id_idx", False, config, telemetry)


def test_plan_skips_when_all_indices_current(dataset_uri: str, telemetry: Telemetry) -> None:
    """The plan phase is a no-op when every targeted index exists with full coverage.

    The ``index_skip_reason`` pre-flight guard returns ``"all indices current"`` when every index
    exists and has zero unindexed fragments, so the plan exits early with no build specs rather
    than issuing redundant maintenance work. The dataset remains fully queryable.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry: The telemetry facade fixture.
    """
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(), scalar_columns=["id"], text_columns=["text"], commit_backoff_seconds=0.0
    )
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    dataset.create_scalar_index("id", "BTREE", name="id_idx")
    lance.dataset(dataset_uri).create_scalar_index("text", "INVERTED", name="text_fts_idx")

    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, config, telemetry)
    assert plan.get("skipped") == "all indices current"
    refreshed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert refreshed.to_table(filter="id = 7").num_rows == 1


def test_plan_rebuild_flag_forces_specs(dataset_uri: str, telemetry: Telemetry) -> None:
    """The rebuild flag plans fresh build specs even when every index exists and is current."""
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    dataset.create_scalar_index("id", "BTREE", name="id_idx")
    lance.dataset(dataset_uri).create_scalar_index("text", "INVERTED", name="text_fts_idx")

    rebuild_config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        scalar_columns=["id"],
        text_columns=["text"],
        rebuild=True,
        commit_backoff_seconds=0.0,
    )
    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, rebuild_config, telemetry)
    assert "skipped" not in plan
    assert {spec["index_name"] for spec in plan["specs"]} == {"id_idx", "text_fts_idx"}
    assert all(spec["shards"] for spec in plan["specs"])


def test_optimize_existing_index_covers_new_fragments(dataset_uri: str, telemetry: Telemetry) -> None:
    """Incremental maintenance extends coverage to fragments appended after the build."""
    config: IndexJobConfig = maintenance_config()
    build_btree_segments(dataset_uri, config, telemetry, shards=2)
    append_fragment(dataset_uri, rows=ROWS_PER_FRAGMENT, start_id=ROWS)
    assert index_coverage(dataset_uri, "id_idx") != set(fragment_ids_of(dataset_uri))
    optimize_existing_index(dataset_uri, "id_idx", config, telemetry)
    assert index_coverage(dataset_uri, "id_idx") == set(fragment_ids_of(dataset_uri))


def test_merge_index_deltas_bounds_accumulation(dataset_uri: str, telemetry: Telemetry) -> None:
    """Deltas above the cap are merged into one, and a merged index is left alone."""
    config: IndexJobConfig = maintenance_config(max_index_deltas=1)
    build_btree_segments(dataset_uri, config, telemetry, shards=2)
    assert index_delta_count(lance.dataset(dataset_uri), "id_idx") == 2
    assert merge_index_deltas(dataset_uri, "id_idx", config, telemetry) is True
    assert index_delta_count(lance.dataset(dataset_uri), "id_idx") == 1
    assert merge_index_deltas(dataset_uri, "id_idx", config, telemetry) is False
    assert lance.dataset(dataset_uri).to_table(filter="id = 7").num_rows == 1


def test_bootstrap_records_rows_at_train(dataset_uri: str, telemetry: Telemetry) -> None:
    """The streaming bootstrap persists the trained row count in the dataset config."""
    config: IndexJobConfig = maintenance_config()
    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert cfg is not None
    assert cfg["rows_at_train"] == ROWS


def test_growth_trigger_plans_bootstrap_and_retrain_refreshes_config(dataset_uri: str, telemetry: Telemetry) -> None:
    """Rows growing past the factor route the index back to a streaming bootstrap that refreshes the config."""
    config: IndexJobConfig = maintenance_config()
    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)

    covered_handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    assert covered_handler.target_fragments(lance.dataset(dataset_uri)) == []

    cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert cfg is not None
    patched_cfg: dict[str, object] = dict(cfg)
    patched_cfg["rows_at_train"] = ROWS // 8
    write_vector_config(dataset_uri, "vector", patched_cfg, config, telemetry)

    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    vector_spec: dict[str, object] = next(spec for spec in plan["specs"] if spec["index_name"] == "vector_idx")
    assert vector_spec["mode"] == "bootstrap"

    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    refreshed_cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert refreshed_cfg is not None
    assert refreshed_cfg["rows_at_train"] == ROWS

    healthy_plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    assert all(spec["mode"] != "bootstrap" for spec in healthy_plan.get("specs", []))


def test_manifest_without_rows_at_train_plans_bootstrap(dataset_uri: str, telemetry: Telemetry) -> None:
    """A config predating the retrain trigger routes the index to one refreshing bootstrap."""
    config: IndexJobConfig = maintenance_config()
    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert cfg is not None
    patched_cfg: dict[str, object] = {k: v for k, v in cfg.items() if k != "rows_at_train"}
    write_vector_config(dataset_uri, "vector", patched_cfg, config, telemetry)

    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    vector_spec: dict[str, object] = next(spec for spec in plan["specs"] if spec["index_name"] == "vector_idx")
    assert vector_spec["mode"] == "bootstrap"

    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    refreshed_cfg: dict[str, object] | None = load_vector_config(lance.dataset(dataset_uri), "vector")
    assert refreshed_cfg is not None
    assert refreshed_cfg["rows_at_train"] == ROWS


def test_within_growth_factor_reuses_artifacts(dataset_uri: str, telemetry: Telemetry) -> None:
    """Artifacts keep being reused while rows stay within the growth factor."""
    config: IndexJobConfig = maintenance_config()
    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    second: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    second.prepare(lance.dataset(dataset_uri), dataset_uri, telemetry)
    assert second.reused_artifacts is True

    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    assert all(spec["mode"] != "bootstrap" for spec in plan.get("specs", []))


def test_vector_config_key_is_stored_in_dataset(dataset_uri: str, telemetry: Telemetry) -> None:
    """After the bootstrap, the vector config key is present in the dataset config KV with no sidecar directory."""
    config: IndexJobConfig = maintenance_config()
    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    stored: dict[str, str] = lance.dataset(dataset_uri).config()
    assert vector_config_key("vector") in stored
    assert not Path(f"{dataset_uri}.artifacts").exists()


def test_fts_maintainable_gates(dataset_uri: str) -> None:
    """The FTS incremental gate requires an existing index, no rebuild, and a small backlog."""
    config: IndexJobConfig = maintenance_config()
    handler: FtsIndexHandler = FtsIndexHandler(config, "text", "text_fts_idx")
    dataset: lance.LanceDataset = lance.dataset(dataset_uri)
    assert handler.maintainable(dataset) is False

    dataset.create_scalar_index("text", "INVERTED", name="text_fts_idx", **config.fts_params())
    indexed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert handler.maintainable(indexed) is True

    rebuild_handler: FtsIndexHandler = FtsIndexHandler(maintenance_config(rebuild=True), "text", "text_fts_idx")
    assert rebuild_handler.maintainable(indexed) is False

    append_fragment(dataset_uri, rows=ROWS_PER_FRAGMENT, start_id=ROWS)
    backlog_handler: FtsIndexHandler = FtsIndexHandler(
        maintenance_config(fts_max_unindexed_fragments=0), "text", "text_fts_idx"
    )
    assert backlog_handler.maintainable(lance.dataset(dataset_uri)) is False


def compact_fragments(uri: str, max_source_fragments: int | None, target_rows_per_fragment: int = ROWS * 2) -> None:
    """Compact a dataset in process, optionally bounding the consumed fragments.

    Args:
        uri: The dataset URI.
        max_source_fragments: Cap on source fragments, or ``None`` for all. The cap admits whole rewrite tasks
            oldest-first, so partial compaction needs a target small enough to split the plan into tasks within it.
        target_rows_per_fragment: Desired rows per compacted fragment, controlling task sizes.
    """
    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=TelemetryConfig(),
        target_rows_per_fragment=target_rows_per_fragment,
        max_source_fragments=max_source_fragments,
        num_threads=1,
    )
    Compaction.execute(lance.dataset(uri), config.execute_options())


def test_commit_segments_skips_when_all_segments_stale(dataset_uri: str, telemetry: Telemetry) -> None:
    """A compaction landing between build and commit drops every stale segment."""
    config: IndexJobConfig = maintenance_config()
    handler: BTreeIndexHandler = BTreeIndexHandler(config, "id", "id_idx")
    version: int = lance.dataset(dataset_uri).version
    documents: list[str] = []
    for group in split_evenly(fragment_ids_of(dataset_uri), 2):
        segment: Index = handler.build_segment(lance.dataset(dataset_uri, version=version), group, None)
        documents.append(serialize_segment(segment))
    compact_fragments(dataset_uri, max_source_fragments=None)
    commit_segments(dataset_uri, documents, "id", "id_idx", False, config, telemetry)
    names: list[str] = [description.name for description in lance.dataset(dataset_uri).describe_indices()]
    assert "id_idx" not in names


def test_commit_segments_keeps_fresh_segments(dataset_uri: str, telemetry: Telemetry) -> None:
    """Only the segments whose fragments were rewritten are dropped from the commit."""
    config: IndexJobConfig = maintenance_config()
    handler: BTreeIndexHandler = BTreeIndexHandler(config, "id", "id_idx")
    original_ids: list[int] = fragment_ids_of(dataset_uri)
    version: int = lance.dataset(dataset_uri).version
    documents: list[str] = []
    for fragment_id in original_ids:
        segment: Index = handler.build_segment(lance.dataset(dataset_uri, version=version), [fragment_id], None)
        documents.append(serialize_segment(segment))
    compact_fragments(dataset_uri, max_source_fragments=2, target_rows_per_fragment=ROWS_PER_FRAGMENT * 2)
    surviving: set[int] = set(original_ids) & set(fragment_ids_of(dataset_uri))
    assert 0 < len(surviving) < len(original_ids)
    commit_segments(dataset_uri, documents, "id", "id_idx", False, config, telemetry)
    assert index_coverage(dataset_uri, "id_idx") == surviving


def test_fts_commit_index_raises_on_missing_fragments(dataset_uri: str, telemetry: Telemetry) -> None:
    """The inverted-index publish refuses coverage of fragments that no longer exist."""
    config: IndexJobConfig = maintenance_config()
    stale_ids: list[int] = [*fragment_ids_of(dataset_uri), 9999]
    with pytest.raises(ValueError, match="no longer exist"):
        publish_fts_index(
            dataset_uri, "text", "text_fts_idx", "00000000-0000-0000-0000-000000000000", stale_ids, config, telemetry
        )


def test_artifact_less_index_plans_bootstrap(dataset_uri: str, telemetry: Telemetry) -> None:
    """An index built outside the pipeline (no stored config) routes to a streaming bootstrap.

    A plain ``create_index`` (as pre-unification deployments produced) mints its own model and
    writes no config KV. Appending segments built from fresh artifacts would put deltas on
    mismatched models, so the plan phase replaces the whole index with a bootstrap instead.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry: The telemetry facade fixture.
    """
    config: IndexJobConfig = maintenance_config()
    lance.dataset(dataset_uri).create_index(
        "vector", "IVF_RQ", name="vector_idx", replace=True, num_partitions=4, num_bits=1
    )
    assert load_vector_config(lance.dataset(dataset_uri), "vector") is None

    append_fragment(dataset_uri, rows=ROWS_PER_FRAGMENT, start_id=ROWS)
    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    vector_spec: dict[str, object] = next(spec for spec in plan["specs"] if spec["index_name"] == "vector_idx")
    assert vector_spec["mode"] == "bootstrap"

    bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)
    refreshed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert index_delta_count(refreshed, "vector_idx") == 1
    assert load_vector_config(refreshed, "vector") is not None
    fresh_handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    assert fresh_handler.target_fragments(refreshed) == []


def test_config_without_committed_index_plans_bootstrap(dataset_uri: str, telemetry: Telemetry) -> None:
    """A stored config without a committed index routes to a bootstrap, and prepare refuses to reuse it."""
    config: IndexJobConfig = maintenance_config()
    write_vector_config(
        dataset_uri,
        "vector",
        {"rows_at_train": ROWS, "dimension": DIM, "metric": config.metric, "num_bits": 1, "rabitq_model": "{}"},
        config,
        telemetry,
    )
    names: set[str] = {description.name for description in lance.dataset(dataset_uri).describe_indices()}
    assert "vector_idx" not in names

    plan: dict[str, object] = plan_dataset_indexes(dataset_uri, vector_only_config(), telemetry)
    vector_spec: dict[str, object] = next(spec for spec in plan["specs"] if spec["index_name"] == "vector_idx")
    assert vector_spec["mode"] == "bootstrap"

    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    with pytest.raises(RuntimeError, match="not reusable"):
        handler.prepare(lance.dataset(dataset_uri), dataset_uri, telemetry)
