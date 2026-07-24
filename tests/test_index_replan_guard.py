"""Compaction-orphan replan guard for the segment-API index build.

Two recovery behaviours are covered:

- Orphan-fragment recovery in ``commit_segments`` (segments.py:314-369, matching the
  ``STALE_FRAGMENT_MARKERS`` including ``"would orphan fragments"``). A compaction that rewrites a
  fragment between a shard build and its commit remaps a wider existing segment over the survivor,
  so publishing the fresh shard would orphan fragments. The in-process build loop must re-resolve
  the fragment set at the latest version and rebuild instead of dying. Ported from the deleted
  ``tests/test_concurrent_coexistence.py`` (``test_vector_/scalar_segment_commit_survives_compaction_orphan``).
- The fleet replan loop in ``LanceIndexer.run`` (runner.py): a stale-fragment commit re-enters the
  next round, and a dataset still stale after every ``max_stale_replans`` round lands the terminal
  ``error_phase="index-stale-exhausted"`` marker (runner.py:1289) rather than being silently
  deferred.

These are deterministic single-thread reproductions: the racing compaction is interleaved through a
hooked ``commit_segments``, and the exhaustion path forces every commit to report a stale fragment.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark, compact_dataset_inline

import lance_etl.indexing.runner as indexing_runner
import lance_etl.indexing.segments as indexing_segments
from lance_etl.indexing import (
    BTreeIndexHandler,
    IndexHandler,
    IndexJobConfig,
    LanceIndexer,
    VectorIndexHandler,
    bootstrap_vector_index,
    is_stale_fragment_error,
    merge_index_deltas,
    scalar_index_name,
    serialize_segment,
    shard_count,
    split_evenly,
    vector_index_name,
)
from lance_etl.indexing.config import MAX_STALE_REPLANS
from lance_etl.maintenance import MaintenanceConfig
from lance_etl.telemetry import Telemetry, TelemetryConfig

DIM: int = 8


def deterministic_vector(identifier: int, dim: int) -> list[float]:
    """Return a per-id unique vector so an exact nearest query has one zero-distance answer.

    Args:
        identifier: The integer row id seeding the vector.
        dim: The vector dimension.

    Returns:
        A vector of ``dim`` floats.
    """
    generator: random.Random = random.Random(identifier * 2_654_435_761)
    return [generator.random() for _ in range(dim)]


def make_vector_range_table(start: int, count: int, dim: int) -> pa.Table:
    """Build a contiguous block of id and vector rows.

    Args:
        start: First id in the block.
        count: Number of rows.
        dim: Vector dimension.

    Returns:
        A table with an ``id`` column and a fixed-size-list ``vector`` column.
    """
    ids: list[int] = list(range(start, start + count))
    flat: pa.Array = pa.array(
        [value for identifier in ids for value in deterministic_vector(identifier, dim)], pa.float32()
    )
    return pa.table({"id": pa.array(ids, pa.int64()), "vector": pa.FixedSizeListArray.from_arrays(flat, dim)})


def write_two_fragment_dataset(uri: str) -> None:
    """Write a dataset of two fragments: a large clean one and a smaller one to be partly deleted.

    Fragment 0 carries ids ``0..299`` and fragment 1 carries ids ``300..499``. Keeping fragment 0
    above the compaction target while fragment 1 accrues deletions lets a later compaction rewrite
    fragment 1 alone, remapping a wider index segment over the survivor and triggering the orphan
    race.

    Args:
        uri: Dataset URI.
    """
    lance.write_dataset(make_vector_range_table(0, 300, DIM), uri, mode="create", max_rows_per_file=1_000_000)
    lance.write_dataset(make_vector_range_table(300, 200, DIM), uri, mode="append", max_rows_per_file=1_000_000)


def unindexed_fragment_count(dataset: lance.LanceDataset, index_name: str) -> int:
    """Return how many fragments an index does not cover.

    Args:
        dataset: The dataset to inspect.
        index_name: The index name.

    Returns:
        The ``num_unindexed_fragments`` statistic.
    """
    return int(dataset.stats.index_stats(index_name).get("num_unindexed_fragments") or 0)


def build_segment_index(uri: str, handler: IndexHandler, config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Build one index increment through the production segment API, mirroring the replan loop.

    Resolves the target fragments, builds one uncommitted segment per shard against a version-pinned
    handle, and publishes through :func:`lance_etl.indexing.commit_segments`, which drops stale
    segments after a concurrent rewrite. When the commit would orphan fragments held by a wider
    existing segment that a compaction remapped, the loop re-resolves the fragment set at the latest
    version and rebuilds instead of letting the orphan ``ValueError`` propagate. The accumulated
    deltas are then bounded with :func:`merge_index_deltas`.

    Args:
        uri: Dataset URI.
        handler: The per-type index handler.
        config: Indexing configuration.
        telemetry: Telemetry facade for the calling thread.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    if handler.skip_reason(dataset) is not None:
        return
    handler.validate(dataset)

    def build_documents(groups: list[list[int]], version: int, artifacts: object | None) -> list[str]:
        """Build one serialized segment per shard against the pinned version.

        Args:
            groups: Fragment-id shards to build.
            version: Dataset version to pin every shard build to.
            artifacts: Broadcast artifacts for the segment builder, if any.

        Returns:
            The serialized segments for the shards.
        """
        documents: list[str] = []
        for group in groups:
            shard: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=config.storage_options)
            documents.append(serialize_segment(handler.build_segment(shard, list(group), artifacts)))
        return documents

    for attempt in range(MAX_STALE_REPLANS):
        del attempt
        current: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        targets: list[int] = handler.target_fragments(current)
        if not targets:
            break
        artifacts: object | None = handler.prepare(current, uri, telemetry)
        groups: list[list[int]] = split_evenly(targets, shard_count(len(targets), config))
        documents: list[str] = build_documents(groups, current.version, artifacts)
        try:
            committed: int = indexing_segments.commit_segments(
                uri, documents, handler.column, handler.index_name, handler.merges(), config, telemetry
            )
        except ValueError as exc:
            if not is_stale_fragment_error(exc):
                raise
            continue
        if committed == len(documents):
            break
    refreshed: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    if handler.index_name in {description.name for description in refreshed.describe_indices()}:
        merge_index_deltas(uri, handler.index_name, config, telemetry)


def racing_compaction_commit(
    uri: str, compaction_config: MaintenanceConfig, telemetry: Telemetry, state: dict[str, bool]
) -> Callable[..., int]:
    """Build a ``commit_segments`` replacement that compacts once before the first commit.

    The first segment commit runs a compaction that rewrites the smaller fragment and remaps the
    wider existing segment over the survivor, exactly the window the production code must survive.
    The real commit is then invoked. An orphan-fragment ``ValueError`` is recorded and re-raised so
    the build loop re-resolves the fragment set and re-commits.

    Args:
        uri: Dataset URI.
        compaction_config: Configuration for the racing compaction.
        telemetry: Telemetry facade.
        state: Mutable flags recording whether the compaction ran and whether an orphan error rose.

    Returns:
        A drop-in replacement for :func:`lance_etl.indexing.commit_segments`.
    """
    real_commit: Callable[..., int] = indexing_segments.commit_segments

    def commit(*args: object, **kwargs: object) -> int:
        """Compact once, then commit, recording any orphan-fragment error.

        Args:
            args: Positional arguments forwarded to the real commit.
            kwargs: Keyword arguments forwarded to the real commit.

        Returns:
            The number of segments committed by the real commit.
        """
        if not state["compacted"]:
            state["compacted"] = True
            compact_dataset_inline(uri, compaction_config, telemetry)
        try:
            return real_commit(*args, **kwargs)
        except ValueError as exc:
            if is_stale_fragment_error(exc):
                state["orphan"] = True
            raise

    return commit


def make_telemetry() -> Telemetry:
    """Build a telemetry facade for the calling thread.

    Returns:
        A telemetry facade safe to use offline.
    """
    return Telemetry.create(TelemetryConfig(service="lance-etl-tests", env="test"), False)


def test_vector_segment_commit_survives_compaction_orphan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A compaction that remaps a wider IVF_RQ segment over a fresh shard must not kill the indexer."""
    uri: str = str(tmp_path / "orphan_vector")
    write_two_fragment_dataset(uri)
    telemetry_config: TelemetryConfig = TelemetryConfig(service="orphan-vector-test", env="test")
    telemetry: Telemetry = Telemetry.create(telemetry_config, False)
    shared: dict[str, Any] = {
        "telemetry": telemetry_config,
        "vector_columns": ["vector"],
        "num_partitions": 4,
        "vector_min_rows": 10,
        "fragments_per_index_task": 1,
        "commit_retries": 10,
        "commit_backoff_seconds": 0.0,
    }
    build_config: IndexJobConfig = IndexJobConfig(**shared)
    index_name: str = vector_index_name("vector")
    bootstrap_vector_index(uri, "vector", index_name, build_config, telemetry)
    assert unindexed_fragment_count(lance.dataset(uri), index_name) == 0

    lance.dataset(uri).delete("id >= 300 and id < 450")
    rebuild_config: IndexJobConfig = IndexJobConfig(rebuild=True, **shared)
    compaction_config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config, target_rows_per_fragment=250, commit_retries=10, commit_backoff_seconds=0.0
    )
    state: dict[str, bool] = {"compacted": False, "orphan": False}
    monkeypatch.setattr(
        indexing_segments, "commit_segments", racing_compaction_commit(uri, compaction_config, telemetry, state)
    )
    build_segment_index(uri, VectorIndexHandler(rebuild_config, "vector", index_name), rebuild_config, telemetry)
    monkeypatch.undo()

    assert state["compacted"], "the racing compaction never ran"
    assert state["orphan"], "the concurrent compaction did not trigger the orphan-fragment race"
    final: lance.LanceDataset = lance.dataset(uri)
    assert unindexed_fragment_count(final, index_name) == 0, "the vector index left fragments uncovered"
    probe: int = 100
    nearest: pa.Table = final.to_table(
        columns=["id"],
        nearest={"column": "vector", "q": deterministic_vector(probe, DIM), "k": 1, "nprobes": 4, "refine_factor": 50},
    )
    assert nearest["id"][0].as_py() == probe, "vector query missed the exact-match id after the orphan rebuild"
    remaining: set[int] = set(final.to_table(columns=["id"]).column("id").to_pylist())
    assert remaining == set(range(0, 300)) | set(range(450, 500)), "row content diverged after the orphan rebuild"


def test_scalar_segment_commit_survives_compaction_orphan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A compaction that remaps a wider BTREE segment over a fresh shard must not kill the indexer."""
    uri: str = str(tmp_path / "orphan_btree")
    write_two_fragment_dataset(uri)
    telemetry_config: TelemetryConfig = TelemetryConfig(service="orphan-scalar-test", env="test")
    telemetry: Telemetry = Telemetry.create(telemetry_config, False)
    index_name: str = scalar_index_name("id")
    wide_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        scalar_columns=["id"],
        fragments_per_index_task=10_000,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    build_segment_index(uri, BTreeIndexHandler(wide_config, "id", index_name), wide_config, telemetry)
    assert unindexed_fragment_count(lance.dataset(uri), index_name) == 0

    lance.dataset(uri).delete("id >= 300 and id < 450")
    rebuild_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        scalar_columns=["id"],
        fragments_per_index_task=1,
        rebuild=True,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    compaction_config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config, target_rows_per_fragment=250, commit_retries=10, commit_backoff_seconds=0.0
    )
    state: dict[str, bool] = {"compacted": False, "orphan": False}
    monkeypatch.setattr(
        indexing_segments, "commit_segments", racing_compaction_commit(uri, compaction_config, telemetry, state)
    )
    build_segment_index(uri, BTreeIndexHandler(rebuild_config, "id", index_name), rebuild_config, telemetry)
    monkeypatch.undo()

    assert state["compacted"], "the racing compaction never ran"
    assert state["orphan"], "the concurrent compaction did not trigger the orphan-fragment race"
    final: lance.LanceDataset = lance.dataset(uri)
    assert unindexed_fragment_count(final, index_name) == 0, "the scalar index left fragments uncovered"
    hits: pa.Table = final.to_table(filter="id = 100")
    assert hits.num_rows == 1, "scalar query did not return exactly one row after the orphan rebuild"
    remaining: set[int] = set(final.to_table(columns=["id"]).column("id").to_pylist())
    assert remaining == set(range(0, 300)) | set(range(450, 500)), "row content diverged after the orphan rebuild"


def make_scalar_dataset(uri: str) -> None:
    """Write a small two-fragment scalar dataset for the runner replan tests.

    Args:
        uri: Dataset URI.
    """
    lance.write_dataset(pa.table({"id": pa.array(range(200), pa.int64())}), uri, mode="create", max_rows_per_file=100)


def test_real_lance_orphan_fragment_error_matches_stale_fragment_marker(tmp_path: Path) -> None:
    """The genuine Lance ``CreateIndex`` orphan-fragment error is provoked and detected.

    ``STALE_FRAGMENT_MARKERS`` includes ``"would orphan fragments"``, a substring lifted from
    Lance's own Rust message in ``rust/lance/src/index.rs`` (``CreateIndex: incoming segments for
    '{}' would orphan fragments {:?} from existing segment {}``). The racing-compaction tests above
    exercise this module's own pre-commit ``"no longer exist"`` guard, not Lance's internal
    wording, because ``commit_segments`` re-validates fragment coverage against the live fragment
    set before ever reaching ``commit_existing_index_segments``, so a real upstream orphan never
    gets that far. This test bypasses that guard and calls the segment-commit API directly, the
    same way ``commit_segments`` and ``build_scalar_segment``/``build_vector_segment`` do, so the
    real upstream error text is what ``is_stale_fragment_error`` is checked against: a wide BTREE
    segment is committed over both fragments of a two-fragment dataset, then a same-name segment
    covering only one fragment is committed on top of it, leaving the other fragment orphaned. If a
    future lance release rewords this message, this test fails and flags the drift instead of the
    replan guard silently going quiet.

    Args:
        tmp_path: Isolated dataset root.
    """
    uri: str = str(tmp_path / "real_orphan.lance")
    make_scalar_dataset(uri)
    dataset: lance.LanceDataset = lance.dataset(uri)
    fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
    assert len(fragment_ids) == 2

    wide_segment: Any = dataset.create_index_uncommitted(
        column="id", index_type="BTREE", name="idx", fragment_ids=fragment_ids
    )
    dataset.commit_existing_index_segments("idx", "id", [wide_segment])

    narrower: lance.LanceDataset = lance.dataset(uri)
    partial_segment: Any = narrower.create_index_uncommitted(
        column="id", index_type="BTREE", name="idx", replace=True, fragment_ids=[fragment_ids[0]]
    )

    committer: lance.LanceDataset = lance.dataset(uri)
    with pytest.raises(ValueError, match="would orphan fragments") as excinfo:
        committer.commit_existing_index_segments("idx", "id", [partial_segment])

    assert is_stale_fragment_error(excinfo.value)


def test_stale_replan_exhausts_to_terminal_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every commit reporting a stale fragment exhausts the rounds into the terminal error phase."""
    uri: str = str(tmp_path / "exhaust.lance")
    make_scalar_dataset(uri)
    calls: list[int] = []

    def always_orphan(*args: object, **kwargs: object) -> int:
        """Always raise the lance orphan-fragment marker so the commit re-plans forever.

        Args:
            args: Ignored positional commit arguments.
            kwargs: Ignored keyword commit arguments.

        Raises:
            ValueError: Always, carrying the ``would orphan fragments`` marker.
        """
        del args, kwargs
        calls.append(1)
        raise ValueError("commit would orphan fragments held by a wider existing segment")

    monkeypatch.setattr(indexing_runner, "commit_segments", always_orphan)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        fragments_per_index_task=10_000,
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [uri])

    assert len(results) == 1
    assert results[0]["error_phase"] == indexing_runner.STALE_REPLAN_EXHAUSTED_PHASE
    assert "stale-replan exhausted" in str(results[0]["error"])
    assert len(calls) == config.max_stale_replans, "the commit should be attempted once per replan round"


def test_stale_replan_converges_after_one_replan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A single stale commit re-enters the next round and the fresh plan then commits cleanly."""
    uri: str = str(tmp_path / "converge.lance")
    make_scalar_dataset(uri)
    real_commit: Callable[..., int] = indexing_runner.commit_segments
    calls: list[int] = []

    def stale_once(*args: object, **kwargs: object) -> int:
        """Report a stale fragment on the first commit, then delegate to the real commit.

        Args:
            args: Positional commit arguments forwarded to the real commit.
            kwargs: Keyword commit arguments forwarded to the real commit.

        Returns:
            The number of segments the real commit published.
        """
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("prepared segments cover fragments that no longer exist after a concurrent rewrite")
        return real_commit(*args, **kwargs)

    monkeypatch.setattr(indexing_runner, "commit_segments", stale_once)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        fragments_per_index_task=10_000,
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [uri])

    assert len(results) == 1
    assert "error" not in results[0], "a single stale round must not fail the dataset"
    assert len(calls) >= 2, "the build must re-commit after the stale replan"
    index_name: str = scalar_index_name("id")
    assert unindexed_fragment_count(lance.dataset(uri), index_name) == 0
