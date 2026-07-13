"""Job-level end-to-end tests for the clustered rewrite, driven through MaintenanceJob on FakeSpark.

Covers eligibility skips falling through to normal compaction, the happy-path invariants (row
multiset preservation, non-decreasing partition ids across fragment order, surviving config KVs,
an identically-centroided rebuilt vector index, and true nearest-neighbor correctness against a
brute-force numpy check), null-vector rows landing in the tail region, the derived-state skip (a
second run over an unwritten dataset is a cheap no-op and a post-rewrite write re-enables
eligibility), the internal-only production-disabled configuration, and per-dataset
failure isolation at the rebuild-commit, rewrite-read, and segment-build phases that keeps every
healthy dataset clustering and every poisoned dataset intact while the run never raises.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import pytest
from conftest import FakeSpark, make_vector_table, write_fragmented_dataset

from lance_etl.column_roles import COLUMN_ROLES_KEY, VECTOR_ROLE, merge_column_roles
from lance_etl.indexing import IndexJobConfig, bootstrap_vector_index, load_vector_config, plan_dataset_indexes
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob, plan_cluster_rewrite
from lance_etl.maintenance import cluster as cluster_module
from lance_etl.maintenance import job as maintenance_job_module
from lance_etl.maintenance.cli import count_failed
from lance_etl.maintenance.cluster import centroids_to_matrix, commit_cluster_overwrite, partition_ids_for_batch
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 512
DIM: int = 8
NUM_PARTITIONS: int = 8
ROWS_PER_FRAGMENT: int = 64
CLUSTER_TARGET_ROWS_PER_FRAGMENT: int = 80
"""Small target_rows_per_fragment so the clustered rewrite splits its 512-row test dataset into
several fragments instead of one, since cluster_max_rows_per_file was removed and the rewrite now
always sizes its output fragments off target_rows_per_fragment."""
INDEX_NAME: str = "vector_idx"


def vector_index_config(**overrides: object) -> IndexJobConfig:
    """Build a small explicit vector-only indexing configuration for the cluster-rewrite tests.

    Args:
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        An indexing configuration targeting the ``vector`` column with a tiny partition count.
    """
    base: dict[str, object] = {
        "telemetry": TelemetryConfig(),
        "vector_columns": ["vector"],
        "num_partitions": NUM_PARTITIONS,
        "vector_min_rows": 1,
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return IndexJobConfig(**base)


def build_cluster_ready_dataset(uri: str, rows: int, dim: int, telemetry: Telemetry) -> None:
    """Write a fragmented vector dataset, bootstrap its vector index, and stamp its column roles.

    Args:
        uri: Destination dataset URI.
        rows: Row count of the base table.
        dim: Vector dimension.
        telemetry: Telemetry facade used for the bootstrap build.
    """
    write_fragmented_dataset(uri, make_vector_table(rows=rows, dim=dim), max_rows_per_file=ROWS_PER_FRAGMENT)
    bootstrap_vector_index(uri, "vector", INDEX_NAME, vector_index_config(), telemetry)
    merge_column_roles(uri, {"vector": VECTOR_ROLE}, None, retries=3, backoff_seconds=0.0)


def cluster_config(**overrides: object) -> MaintenanceConfig:
    """Build a maintenance configuration with the clustered rewrite turned on.

    Args:
        overrides: Field overrides applied on top of the test defaults.

    Returns:
        A maintenance configuration ready for a fast, in-process clustered rewrite run.
    """
    base: dict[str, object] = {
        "telemetry": TelemetryConfig(),
        "cluster_rewrite": True,
        "target_rows_per_fragment": CLUSTER_TARGET_ROWS_PER_FRAGMENT,
        "commit_retries": 5,
        "commit_backoff_seconds": 0.0,
        "large_commit_retries": 5,
    }
    base.update(overrides)
    return MaintenanceConfig(**base)


def make_vector_table_with_nulls(rows: int, dim: int, null_ids: set[int], seed: int = 11) -> pa.Table:
    """Build a table like :func:`conftest.make_vector_table` with some rows carrying a null vector.

    Args:
        rows: Number of rows to generate.
        dim: Fixed-size-list vector dimension.
        null_ids: The zero-based row indices whose vector is null.
        seed: Random seed for reproducible vectors.

    Returns:
        The generated table, with a genuine fixed-size-list null in every ``null_ids`` row.
    """
    rng: random.Random = random.Random(seed)
    rows_or_none: list[list[float] | None] = [
        None if index in null_ids else [rng.random() for _ in range(dim)] for index in range(rows)
    ]
    vectors: pa.Array = pa.array(rows_or_none, pa.list_(pa.float32(), dim))
    return pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "vector": vectors,
            "category": pa.array([f"cat{i % 4}" for i in range(rows)]),
            "text": pa.array([f"word{i % 10} common" for i in range(rows)]),
        }
    )


def per_row_vector_matrix(table: pa.Table, dim: int) -> np.ndarray:
    """Return a dense float32 matrix of a table's ``vector`` column for a brute-force comparison.

    Args:
        table: A table carrying a ``vector`` fixed-size-list column with no nulls.
        dim: The vector dimension.

    Returns:
        A ``(num_rows, dim)`` float32 matrix.
    """
    vectors: pa.Array = table.column("vector").combine_chunks()
    flat: np.ndarray = vectors.values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    return flat.reshape(len(vectors), dim)


def true_nearest_id(table: pa.Table, query: list[float], dim: int) -> int:
    """Return the id of the true nearest row to a query vector by brute-force L2 distance.

    Args:
        table: A table carrying ``id`` and a non-null ``vector`` column.
        query: The query vector.
        dim: The vector dimension.

    Returns:
        The id of the closest row under L2 distance.
    """
    matrix: np.ndarray = per_row_vector_matrix(table, dim)
    distances: np.ndarray = np.linalg.norm(matrix - np.asarray(query, dtype=np.float32), axis=1)
    ids: list[int] = table.column("id").to_pylist()
    return int(ids[int(np.argmin(distances))])


def assert_pids_non_decreasing_across_fragments(
    dataset: lance.LanceDataset, centroids: np.ndarray, distance_type: str
) -> None:
    """Assert every fragment's partition-id range starts at or after the previous fragment's peak.

    Recomputes each row's partition id from the pre-rewrite centroids (independent of whatever the
    rewrite itself computed) and walks the dataset's fragments in order, so a passing assertion
    proves the global sort by partition id survived the write-fragments split into files.

    Args:
        dataset: The post-rewrite dataset handle.
        centroids: The pre-rewrite centroid matrix.
        distance_type: The Lance distance type used to train the index.
    """
    num_partitions: int = centroids.shape[0]
    running_max: int = -1
    for fragment in dataset.get_fragments():
        table: pa.Table = dataset.scanner(columns=["vector"], fragments=[fragment]).to_table()
        vectors: pa.FixedSizeListArray = table.column("vector").combine_chunks()
        pids: np.ndarray = partition_ids_for_batch(vectors, centroids, distance_type, num_partitions)
        assert int(pids.min()) >= running_max, "a fragment's partition ids regressed against the previous fragment"
        running_max = int(pids.max())


def test_eligibility_skip_falls_through_to_normal_compaction(tmp_path: Path, telemetry: Telemetry) -> None:
    """A dataset with no vector index is cluster_skipped, then still compacted by normal maintenance."""
    uri: str = str(tmp_path / "no_index.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=64, dim=DIM), max_rows_per_file=8)
    assert len(lance.dataset(uri).get_fragments()) == 8

    plan: dict[str, object] = plan_cluster_rewrite(uri, cluster_config(), None, telemetry)
    assert "cluster_skipped" in plan

    config: MaintenanceConfig = cluster_config(target_rows_per_fragment=1000)
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert len(results) == 1
    assert "clustered" not in results[0]
    assert "error" not in results[0]
    assert len(lance.dataset(uri).get_fragments()) == 1
    assert lance.dataset(uri).count_rows() == 64


def test_cluster_rewrite_end_to_end_preserves_and_reindexes(tmp_path: Path, telemetry: Telemetry) -> None:
    """The full happy path preserves rows, orders them by partition id, and rebuilds an identical index."""
    uri: str = str(tmp_path / "cluster_e2e.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)

    pre_dataset: lance.LanceDataset = lance.dataset(uri)
    pre_ids: set[int] = set(pre_dataset.to_table(columns=["id"]).column("id").to_pylist())
    pre_centroids: np.ndarray = centroids_to_matrix(pre_dataset.get_ivf_model(INDEX_NAME).centroids)
    pre_vector_cfg: dict[str, object] | None = load_vector_config(pre_dataset, "vector")
    assert pre_vector_cfg is not None
    pre_rows_at_train: object = pre_vector_cfg["rows_at_train"]

    config: MaintenanceConfig = cluster_config()
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert len(results) == 1
    result: dict[str, object] = results[0]
    assert "error" not in result
    assert result["clustered"] is True
    assert result["fragments_added"] > 0

    post_dataset: lance.LanceDataset = lance.dataset(uri)
    assert set(post_dataset.to_table(columns=["id"]).column("id").to_pylist()) == pre_ids
    assert len(post_dataset.get_fragments()) > 1

    assert_pids_non_decreasing_across_fragments(post_dataset, pre_centroids, "l2")

    post_config_kv: dict[str, str] = post_dataset.config()
    assert COLUMN_ROLES_KEY in post_config_kv
    post_vector_cfg: dict[str, object] | None = load_vector_config(post_dataset, "vector")
    assert post_vector_cfg is not None
    assert post_vector_cfg["rows_at_train"] == pre_rows_at_train

    names: set[str] = {description.name for description in post_dataset.describe_indices()}
    assert INDEX_NAME in names
    post_centroids: np.ndarray = centroids_to_matrix(post_dataset.get_ivf_model(INDEX_NAME).centroids)
    np.testing.assert_allclose(post_centroids, pre_centroids)

    plan: dict[str, object] = plan_dataset_indexes(uri, vector_index_config(), telemetry)
    assert plan.get("skipped") == "all indices current"

    full_table: pa.Table = post_dataset.to_table(columns=["id", "vector"])
    query: list[float] = list(random.Random(99).random() for _ in range(DIM))
    expected_id: int = true_nearest_id(full_table, query, DIM)
    nearest: pa.Table = post_dataset.to_table(
        columns=["id"],
        nearest={"column": "vector", "q": query, "k": 1, "nprobes": NUM_PARTITIONS, "refine_factor": ROWS},
    )
    assert nearest["id"][0].as_py() == expected_id


def test_null_vector_rows_placed_in_tail(tmp_path: Path, telemetry: Telemetry) -> None:
    """Null-vector rows survive the rewrite and land contiguously at the tail of the dataset."""
    base_rows: int = 256
    null_count: int = 16
    uri: str = str(tmp_path / "cluster_nulls.lance")
    build_cluster_ready_dataset(uri, base_rows, DIM, telemetry)

    null_table: pa.Table = make_vector_table_with_nulls(null_count, DIM, set(range(null_count)), seed=23)
    reindexed: pa.Table = null_table.set_column(0, "id", pa.array(range(base_rows, base_rows + null_count), pa.int64()))
    lance.write_dataset(reindexed, uri, mode="append")
    assert lance.dataset(uri).count_rows() == base_rows + null_count

    config: MaintenanceConfig = cluster_config()
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "error" not in results[0]

    post_dataset: lance.LanceDataset = lance.dataset(uri)
    assert post_dataset.count_rows() == base_rows + null_count
    full_table: pa.Table = post_dataset.to_table(columns=["vector"])
    null_flags: list[bool] = [value is None for value in full_table.column("vector").to_pylist()]
    assert sum(null_flags) == null_count

    first_null_index: int = null_flags.index(True)
    assert all(null_flags[first_null_index:]), "null-vector rows are not contiguous at the tail"
    assert first_null_index == base_rows, "null-vector rows are not placed after every real partition"


def test_second_run_skips_already_clustered(tmp_path: Path, telemetry: Telemetry) -> None:
    """The second run over an unwritten dataset skips via the generation stamp, invariants intact."""
    uri: str = str(tmp_path / "cluster_idempotent.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)

    pre_dataset: lance.LanceDataset = lance.dataset(uri)
    pre_ids: set[int] = set(pre_dataset.to_table(columns=["id"]).column("id").to_pylist())
    pre_centroids: np.ndarray = centroids_to_matrix(pre_dataset.get_ivf_model(INDEX_NAME).centroids)

    config: MaintenanceConfig = cluster_config()
    first: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "error" not in first[0]
    assert first[0]["clustered"] is True
    first_version: int = lance.dataset(uri).version
    assert lance.dataset(uri).config().get(cluster_module.CLUSTER_GENERATION_KEY) is not None

    second: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "error" not in second[0]
    assert "clustered" not in second[0]
    assert "already clustered" in str(second[0]["skipped"])
    assert count_failed(second) == 0
    assert lance.dataset(uri).version == first_version, "the skipped run must commit nothing"

    post_dataset: lance.LanceDataset = lance.dataset(uri)
    assert set(post_dataset.to_table(columns=["id"]).column("id").to_pylist()) == pre_ids
    assert_pids_non_decreasing_across_fragments(post_dataset, pre_centroids, "l2")
    names: set[str] = {description.name for description in post_dataset.describe_indices()}
    assert INDEX_NAME in names
    post_centroids: np.ndarray = centroids_to_matrix(post_dataset.get_ivf_model(INDEX_NAME).centroids)
    np.testing.assert_allclose(post_centroids, pre_centroids)


def test_write_after_cluster_reenables_eligibility(tmp_path: Path, telemetry: Telemetry) -> None:
    """An append after a clustered rewrite invalidates the generation stamp and re-clusters."""
    uri: str = str(tmp_path / "cluster_reenable.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)
    pre_centroids: np.ndarray = centroids_to_matrix(lance.dataset(uri).get_ivf_model(INDEX_NAME).centroids)

    config: MaintenanceConfig = cluster_config()
    first: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert first[0]["clustered"] is True

    extra_rows: int = 64
    extra: pa.Table = make_vector_table(rows=extra_rows, dim=DIM, seed=41)
    reindexed: pa.Table = extra.set_column(0, "id", pa.array(range(ROWS, ROWS + extra_rows), pa.int64()))
    lance.write_dataset(reindexed, uri, mode="append")

    plan: dict[str, object] = plan_cluster_rewrite(uri, config, None, telemetry)
    assert "cluster_current" not in plan
    assert "cluster_skipped" not in plan

    second: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "error" not in second[0]
    assert second[0]["clustered"] is True

    post_dataset: lance.LanceDataset = lance.dataset(uri)
    assert post_dataset.count_rows() == ROWS + extra_rows
    assert_pids_non_decreasing_across_fragments(post_dataset, pre_centroids, "l2")


def test_already_clustered_dataset_not_passed_to_normal_compaction(tmp_path: Path, telemetry: Telemetry) -> None:
    """A generation-stamped dataset is terminal-skipped, never compacted back toward insertion order."""
    uri: str = str(tmp_path / "cluster_no_compact.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)

    config: MaintenanceConfig = cluster_config()
    first: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert first[0]["clustered"] is True
    clustered_fragments: int = len(lance.dataset(uri).get_fragments())
    assert clustered_fragments > 1

    second: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "already clustered" in str(second[0]["skipped"])
    assert len(lance.dataset(uri).get_fragments()) == clustered_fragments


def test_rebuild_failure_is_isolated_and_data_intact(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A vector-index rebuild failure is isolated: the run does not raise and the data stays intact."""
    uri: str = str(tmp_path / "cluster_rebuild_failure.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)
    pre_ids: set[int] = set(lance.dataset(uri).to_table(columns=["id"]).column("id").to_pylist())

    def failing_commit_segments(*args: object, **kwargs: object) -> int:
        """Simulate a vector-index rebuild commit that always fails."""
        del args, kwargs
        raise RuntimeError("simulated index rebuild failure")

    monkeypatch.setattr("lance_etl.maintenance.cluster.commit_segments", failing_commit_segments)

    config: MaintenanceConfig = cluster_config()
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert len(results) == 1
    result: dict[str, object] = results[0]
    assert result["error"] == "simulated index rebuild failure"
    assert result["phase"] == "cluster_index"
    assert count_failed(results) == 1

    post_dataset: lance.LanceDataset = lance.dataset(uri)
    assert set(post_dataset.to_table(columns=["id"]).column("id").to_pylist()) == pre_ids
    names: set[str] = {description.name for description in post_dataset.describe_indices()}
    assert INDEX_NAME not in names


def test_read_task_failure_is_isolated_per_dataset(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing rewrite read task fails only its dataset, leaving it byte-intact while the other clusters."""
    poisoned_uri: str = str(tmp_path / "cluster_read_poison.lance")
    healthy_uri: str = str(tmp_path / "cluster_read_healthy.lance")
    build_cluster_ready_dataset(poisoned_uri, ROWS, DIM, telemetry)
    build_cluster_ready_dataset(healthy_uri, ROWS, DIM, telemetry)
    pre_poison_ids: set[int] = set(lance.dataset(poisoned_uri).to_table(columns=["id"]).column("id").to_pylist())
    pre_poison_fragments: int = len(lance.dataset(poisoned_uri).get_fragments())

    real_read = cluster_module.read_rewrite_chunks

    def failing_read(uri: str, *args: object, **kwargs: object) -> list[tuple[int, bytes]]:
        """Fail every read task of the poisoned dataset while the healthy dataset reads normally."""
        if uri == poisoned_uri:
            raise RuntimeError("simulated read failure")
        return real_read(uri, *args, **kwargs)

    monkeypatch.setattr("lance_etl.maintenance.cluster.read_rewrite_chunks", failing_read)

    config: MaintenanceConfig = cluster_config()
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [poisoned_uri, healthy_uri])
    by_uri: dict[str, dict[str, object]] = {str(result["uri"]): result for result in results}

    poisoned: dict[str, object] = by_uri[poisoned_uri]
    assert poisoned["phase"] == "cluster-rewrite"
    assert poisoned["error"] == "simulated read failure"
    assert "clustered" not in poisoned

    post_poison: lance.LanceDataset = lance.dataset(poisoned_uri)
    assert set(post_poison.to_table(columns=["id"]).column("id").to_pylist()) == pre_poison_ids
    assert post_poison.count_rows() == ROWS
    assert len(post_poison.get_fragments()) == pre_poison_fragments
    poison_names: set[str] = {description.name for description in post_poison.describe_indices()}
    assert INDEX_NAME in poison_names, "the refused overwrite must not have dropped the poisoned dataset's index"

    healthy: dict[str, object] = by_uri[healthy_uri]
    assert "error" not in healthy
    assert healthy["clustered"] is True
    healthy_names: set[str] = {description.name for description in lance.dataset(healthy_uri).describe_indices()}
    assert INDEX_NAME in healthy_names


def test_segment_build_failure_is_isolated_per_dataset(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing index-segment build leaves its dataset clustered-but-unindexed while the other finishes."""
    poisoned_uri: str = str(tmp_path / "cluster_build_poison.lance")
    healthy_uri: str = str(tmp_path / "cluster_build_healthy.lance")
    build_cluster_ready_dataset(poisoned_uri, ROWS, DIM, telemetry)
    build_cluster_ready_dataset(healthy_uri, ROWS, DIM, telemetry)
    pre_poison_ids: set[int] = set(lance.dataset(poisoned_uri).to_table(columns=["id"]).column("id").to_pylist())

    real_build = cluster_module.build_cluster_index_segment

    def failing_build(uri: str, *args: object, **kwargs: object) -> str:
        """Fail every segment build of the poisoned dataset while the healthy dataset builds normally."""
        if uri == poisoned_uri:
            raise RuntimeError("simulated segment build failure")
        return real_build(uri, *args, **kwargs)

    monkeypatch.setattr("lance_etl.maintenance.cluster.build_cluster_index_segment", failing_build)

    config: MaintenanceConfig = cluster_config()
    results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [poisoned_uri, healthy_uri])
    by_uri: dict[str, dict[str, object]] = {str(result["uri"]): result for result in results}

    poisoned: dict[str, object] = by_uri[poisoned_uri]
    assert poisoned["phase"] == "cluster_index"
    assert poisoned["clustered"] is True
    assert poisoned["error"] == "simulated segment build failure"

    post_poison: lance.LanceDataset = lance.dataset(poisoned_uri)
    assert set(post_poison.to_table(columns=["id"]).column("id").to_pylist()) == pre_poison_ids
    assert post_poison.count_rows() == ROWS
    poison_names: set[str] = {description.name for description in post_poison.describe_indices()}
    assert INDEX_NAME not in poison_names, "the poisoned dataset is committed-but-unindexed after the failed build"

    healthy: dict[str, object] = by_uri[healthy_uri]
    assert "error" not in healthy
    assert healthy["clustered"] is True
    healthy_names: set[str] = {description.name for description in lance.dataset(healthy_uri).describe_indices()}
    assert INDEX_NAME in healthy_names


def test_commit_cluster_overwrite_preserves_config(tmp_path: Path, telemetry: Telemetry) -> None:
    """Overwriting a dataset with the same rows preserves its config KV, tested against the commit directly."""
    uri: str = str(tmp_path / "cluster_overwrite.lance")
    table: pa.Table = make_vector_table(rows=64, dim=DIM)
    lance.write_dataset(table, uri, max_rows_per_file=16)
    merge_column_roles(uri, {"vector": VECTOR_ROLE}, None, retries=3, backoff_seconds=0.0)

    dataset: lance.LanceDataset = lance.dataset(uri)
    schema: pa.Schema = dataset.schema
    metadatas = lance.fragment.write_fragments(
        dataset.to_table().to_reader(), uri, schema=schema, mode="overwrite", data_storage_version="2.1"
    )
    fragment_documents: list[str] = [json.dumps(metadata.to_json()) for metadata in metadatas]

    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=TelemetryConfig(), commit_backoff_seconds=0.0, large_commit_retries=5
    )
    fragments_committed: int = commit_cluster_overwrite(uri, fragment_documents, schema, config, telemetry)
    assert fragments_committed == len(metadatas)

    refreshed: lance.LanceDataset = lance.dataset(uri)
    assert refreshed.count_rows() == 64
    assert refreshed.config().get(COLUMN_ROLES_KEY) is not None


def test_idle_clustered_dataset_still_runs_version_cleanup(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later run over an already-clustered dataset still runs idle version cleanup, not a full skip.

    D2: the terminal ``cluster_current`` skip used to bypass cleanup entirely, so the pre-rewrite
    generation the Overwrite left was never reclaimed once its tags unpinned it. The skip now routes
    through the same rotation-gated idle cleanup the normal compaction-skip path uses.
    ``cleanup_rotation_slots=1`` makes the rotation deterministic so the cleanup always fires.
    """
    uri: str = str(tmp_path / "cluster_idle_cleanup.lance")
    build_cluster_ready_dataset(uri, ROWS, DIM, telemetry)

    config: MaintenanceConfig = cluster_config(cleanup_rotation_slots=1)
    first: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert first[0]["clustered"] is True

    real_cleanup = maintenance_job_module.cleanup_dataset
    cleaned: list[str] = []

    def spy_cleanup(
        cleanup_uri: str, cfg: MaintenanceConfig, tel: Telemetry, dataset: lance.LanceDataset | None = None
    ) -> int:
        """Record every version-cleanup call and delegate to the real implementation."""
        cleaned.append(cleanup_uri)
        return real_cleanup(cleanup_uri, cfg, tel, dataset)

    monkeypatch.setattr(maintenance_job_module, "cleanup_dataset", spy_cleanup)

    second: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
    assert "clustered" not in second[0]
    assert "already clustered" in str(second[0]["skipped"])
    assert uri in cleaned, "an idle already-clustered dataset must still run version cleanup"


def test_count_preserving_merge_invalidates_fingerprint(tmp_path: Path, telemetry: Telemetry) -> None:
    """A full-refresh merge that preserves row and fragment counts still re-enables re-clustering.

    D3: the old ``(num_fragments, num_rows)`` fingerprint false-matched a re-ingest merge that
    dropped the one old fragment and wrote one new fragment with identical counts, so a dataset
    whose every row changed was permanently excluded from re-clustering. The fragment-id signature
    invalidates on the new fragment id while an untouched dataset still matches.
    """
    uri: str = str(tmp_path / "cluster_fingerprint.lance")
    table: pa.Table = make_vector_table(rows=64, dim=DIM)
    lance.write_dataset(table, uri, max_rows_per_file=1000)
    assert len(lance.dataset(uri).get_fragments()) == 1

    config: MaintenanceConfig = cluster_config()
    cluster_module.stamp_cluster_generation(uri, config, telemetry)

    stamped: lance.LanceDataset = lance.dataset(uri)
    assert cluster_module.cluster_generation_skip_reason(stamped) is not None, "an untouched dataset must still skip"
    before_ids: list[int] = sorted(fragment.fragment_id for fragment in stamped.get_fragments())
    before_rows: int = stamped.count_rows()

    category_index: int = table.schema.get_field_index("category")
    refreshed_table: pa.Table = table.set_column(
        category_index, "category", pa.array([f"changed{i}" for i in range(before_rows)])
    )
    stamped.merge_insert(on="id").when_matched_update_all().when_not_matched_insert_all().execute(refreshed_table)

    reopened: lance.LanceDataset = lance.dataset(uri)
    after_ids: list[int] = sorted(fragment.fragment_id for fragment in reopened.get_fragments())
    assert reopened.count_rows() == before_rows, "the merge must preserve the row count"
    assert len(after_ids) == len(before_ids), "the merge must preserve the fragment count"
    assert after_ids != before_ids, "the merge must mint a new fragment id"
    assert cluster_module.cluster_generation_skip_reason(reopened) is None, "re-clustering must be eligible again"
