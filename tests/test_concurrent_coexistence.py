"""Concurrent coexistence proof: ingestion, compaction, and indexing against the same Lance datasets.

Three actors run as plain Python threads against local-fs datasets, exercising the production helpers directly
with no Spark involved:

- INGESTER threads drive deterministic rounds of :func:`lance_etl.etl.apply_merge`, the exact merge_insert builder
  configuration production uses, mixing fresh inserts, updates of existing keys, and deletes of a known subset.
  The expected terminal state is tracked exactly in memory as ``{key_index: last_upsert_round}``.
- A COMPACTOR thread sweeps every dataset, compacting whenever the fragment count exceeds a small threshold. The
  head dataset uses the tier-B plan/execute/commit triad with the production re-plan-on-conflict loop
  (:meth:`lance_etl.compaction.LanceCompactor.commit_rewrites` plus re-plan, mirroring ``compact_one``), and the
  tail datasets use :func:`lance_etl.compaction.compact_small_dataset`. Version cleanup runs only at the end,
  through :func:`lance_etl.compaction.cleanup_dataset` with the default retention horizon, so concurrent readers
  pinned to older versions are never broken mid-run.
- An INDEXER thread loops incremental maintenance. The head dataset uses the real segment-API paths: vector IVF_RQ
  and BTREE increments through ``create_index_uncommitted`` plus :func:`lance_etl.indexing.commit_segments` (which
  drops stale segments after a concurrent rewrite), the inverted index through the shared-uuid metadata-merge path
  published by :meth:`lance_etl.indexing.FtsIndexHandler.commit_index`, and delta bounding through
  :func:`lance_etl.indexing.merge_index_deltas`. Tail datasets run the production small-tier
  :func:`lance_etl.indexing.index_dataset_locally` build-then-maintain path. Every commit goes through
  :func:`lance_etl.telemetry.commit_with_retries`.

The fleet is the 30,000-org shape in miniature: one head-org-sized dataset (about 200k keys, dim 16) plus a
handful of tiny tail-org datasets, all written, compacted, and indexed concurrently.

Strict assertions:

1. ZERO DATA LOSS: every dataset's final content equals the tracked expected state exactly. Every surviving key is
   present exactly once with the payload checksum of its last upsert round, deleted keys are absent, and row
   counts match exactly.
2. NO ACTOR DIED: every commit conflict was absorbed by a retry or re-plan. The observed conflict-retry count is
   recorded and any count including zero is tolerated.
3. CONVERGENCE: after the final compaction and the final index maintenance pass, the fragment count sits at or
   below the configured target band, every index reports zero unindexed fragments, and a vector nearest query, a
   full-text query, and a scalar filtered query each return exactly the expected rows.

An inverted-index publish that collides with a compaction rewrite raises ``ValueError`` by design (the
stale-publish guard fails loudly instead of publishing dead row addresses), so the indexer treats that one error
as "defer to the next pass", exactly as the orchestrator would treat a failed run.

REAL BUG FOUND AND FIXED by this test: the segment builders in ``lance_etl.indexing`` called
``create_index_uncommitted`` without ``replace=True``, and the uncommitted build path applies the same
same-name existence guard as the committed path (``rust/lance/src/index/create.rs:200-205``). The very first
incremental maintenance pass after an index was committed therefore raised ``Index name '...' already exists``
and the large-tier segment path could never extend coverage. ``replace`` is consumed only by the committed
``execute`` removal logic (``create.rs:497-510``), so passing it on the uncommitted path is purely a guard
bypass and existing deltas are preserved. Fixed in :class:`VectorIndexHandler`, :class:`BTreeIndexHandler`, and
:class:`BitmapIndexHandler`.

SECOND REAL BUG FOUND AND MITIGATED by this test: on the pinned lance build, compaction's inline eager index
remap silently corrupts IVF_RQ indexes. Measured here: a clean index with 40/40 exact top-1 recall drops to
22/40 after one ``Compaction.execute`` rewrote its covered fragments, while ``num_unindexed_fragments`` stays 0
and ``num_indexed_rows`` stays exact, so no maintenance trigger ever fires. Deferred remap is also broken (vector
queries fail with a missing fragment-id error, the caveat recorded in ``compaction.py``), and the corruption
reproduces identically for ``create_index``-built indexes, so it is upstream, not a segment-flow artifact. BTREE
and FTS remaps measured sound (40/40 after the same rewrite). Mitigation in
:meth:`VectorIndexHandler.remap_requires_rebuild`: the artifact sidecar records the live fragment ids covered at
each build, and a later pass finding any of them gone forces a full segment rebuild from the intact row data,
reusing the trained centroids and rotation.

Convergence finding (Lance behavior, not a repository bug): the compaction planner never bins fragments whose
covering index sets differ (``rust/lance/src/dataset/optimize.rs:662-694``) and every index delta carries its own
fragment bitmap (``load_index_fragmaps``, ``optimize.rs:1378-1391``). Fragments covered by different deltas of
the same logical index therefore fence compaction bins, leaving isolated sub-target fragments uncompactable. The
final convergence sequence must collapse each index's deltas to one (``optimize_indices`` with
``num_indices_to_merge``) before the last compaction, which is exactly what the bounded
``merge_index_deltas`` cadence achieves over time in production.
"""

from __future__ import annotations

import math
import random
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from lance.optimize import Compaction, CompactionTask

from lance_etl import indexing
from lance_etl.compaction import CompactionConfig, LanceCompactor, cleanup_dataset, compact_small_dataset
from lance_etl.etl import ETLConfig, apply_merge, dataset_uri
from lance_etl.indexing import (
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    IndexJobConfig,
    VectorIndexHandler,
    build_and_commit_segments,
    fts_index_name,
    index_dataset_locally,
    index_delta_count,
    is_stale_fragment_error,
    merge_index_deltas,
    optimize_existing_index,
    scalar_index_name,
    serialize_segment,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig, is_commit_conflict_error

pytestmark = pytest.mark.integration

DIM: int = 16
HEAD_ROUNDS: int = 16
HEAD_INSERTS_PER_ROUND: int = 12_500
HEAD_UPDATES_PER_ROUND: int = 2_000
HEAD_DELETES_PER_ROUND: int = 600
TAIL_DATASETS: int = 4
TAIL_ROUNDS: int = 8
TAIL_INSERTS_PER_ROUND: int = 30
TAIL_UPDATES_PER_ROUND: int = 8
TAIL_DELETES_PER_ROUND: int = 4
HEAD_TARGET_ROWS_PER_FRAGMENT: int = 100_000
HEAD_COMPACT_FRAGMENT_THRESHOLD: int = 6
TAIL_COMPACT_FRAGMENT_THRESHOLD: int = 3
HEAD_ROUTING: tuple[str, str, str] = ("org-head", "tenant1", "ns1")
HEAD_INGEST_PAUSE_SECONDS: float = 0.02
TAIL_INGEST_PAUSE_SECONDS: float = 0.05
COMPACTOR_SWEEP_PAUSE_SECONDS: float = 0.15
INDEXER_SWEEP_PAUSE_SECONDS: float = 0.2
FINAL_COMPACT_CYCLES: int = 6
JOIN_TIMEOUT_SECONDS: float = 600.0


def key_name(index: int) -> str:
    """Return the deterministic string key for a key index.

    Args:
        index: The integer key index.

    Returns:
        The key column value.
    """
    return f"k{index:07d}"


def checksum(index: int, round_number: int) -> int:
    """Return the deterministic payload checksum for a key and round.

    Args:
        index: The integer key index.
        round_number: The ingestion round that last upserted the key.

    Returns:
        The expected ``val`` column value.
    """
    return index * 1_000_003 + round_number * 97


def vector_values(index: int, round_number: int) -> list[float]:
    """Return the deterministic vector for a key and round.

    Each ``(index, round)`` pair seeds its own pseudo-random generator, so vectors are deterministic, unique, and
    uniformly spread. An exact nearest query for a stored vector has exactly one zero-distance answer while every
    other key sits at a typical 16-dimensional uniform distance, which keeps the quantized candidate set honest.

    Args:
        index: The integer key index.
        round_number: The ingestion round that last upserted the key.

    Returns:
        A vector of :data:`DIM` floats in ``[0, 1)``.
    """
    generator: random.Random = random.Random(index * 1_000_003 + round_number * 97)
    return [generator.random() for _ in range(DIM)]


def text_value(index: int, round_number: int) -> str:
    """Return the deterministic text payload for a key and round.

    The leading token is unique per key so a full-text query for it matches exactly one row.

    Args:
        index: The integer key index.
        round_number: The ingestion round that last upserted the key.

    Returns:
        The text column value.
    """
    return f"token{index} round{round_number} common filler"


def build_group(routing: tuple[str, str, str], upserts: dict[int, int], deletes: list[int]) -> pa.Table:
    """Build one routing key's change rows for :func:`apply_merge`.

    Args:
        routing: The routing key values for the constant routing columns.
        upserts: Map of key index to the round stamping its payload.
        deletes: Key indices to delete in this round.

    Returns:
        A table shaped like one routed ETL group, carrying the op column.
    """
    indices: list[int] = [*upserts.keys(), *deletes]
    rounds: list[int] = [*upserts.values(), *([0] * len(deletes))]
    ops: list[str] = ["insert"] * len(upserts) + ["delete"] * len(deletes)
    count: int = len(indices)
    flat: pa.Array = pa.array(
        [value for index, rnd in zip(indices, rounds, strict=True) for value in vector_values(index, rnd)],
        pa.float32(),
    )
    return pa.table(
        {
            "vector_id": pa.array([key_name(index) for index in indices], pa.string()),
            "org_id": pa.array([routing[0]] * count, pa.string()),
            "tenant_id": pa.array([routing[1]] * count, pa.string()),
            "namespace": pa.array([routing[2]] * count, pa.string()),
            "timestamp": pa.array(rounds, pa.int64()),
            "op": pa.array(ops, pa.string()),
            "val": pa.array([checksum(index, rnd) for index, rnd in zip(indices, rounds, strict=True)], pa.int64()),
            "category": pa.array([f"cat{index % 8}" for index in indices], pa.string()),
            "text": pa.array([text_value(index, rnd) for index, rnd in zip(indices, rounds, strict=True)], pa.string()),
            "vector": pa.FixedSizeListArray.from_arrays(flat, DIM),
        }
    )


def open_or_none(uri: str) -> lance.LanceDataset | None:
    """Open a dataset, returning ``None`` when it does not exist yet.

    Args:
        uri: Dataset URI.

    Returns:
        The dataset handle, or ``None`` before the first ingest round created it.
    """
    try:
        return lance.dataset(uri)
    except (FileNotFoundError, ValueError):
        return None


def fragment_count(uri: str) -> int:
    """Return a dataset's current fragment count, zero when absent.

    Args:
        uri: Dataset URI.

    Returns:
        The number of live fragments.
    """
    dataset: lance.LanceDataset | None = open_or_none(uri)
    return 0 if dataset is None else len(dataset.get_fragments())


def unindexed_fragment_count(dataset: lance.LanceDataset, index_name: str) -> int:
    """Return how many fragments an index does not cover.

    Args:
        dataset: The dataset to inspect.
        index_name: The index name.

    Returns:
        The ``num_unindexed_fragments`` statistic.
    """
    return int(dataset.stats.index_stats(index_name).get("num_unindexed_fragments") or 0)


def run_ingester(
    etl_config: ETLConfig,
    telemetry: Telemetry,
    routing: tuple[str, str, str],
    rounds: int,
    inserts_per_round: int,
    updates_per_round: int,
    deletes_per_round: int,
    expected: dict[int, int],
    pause_seconds: float,
) -> None:
    """Drive deterministic merge_insert rounds against one dataset.

    Each round inserts a fresh block of keys, updates a deterministic subset of live keys, and deletes a disjoint
    deterministic subset, all through the production :func:`apply_merge` path. The expected state map is updated
    only after the merge succeeds, so it always reflects exactly what was committed.

    Args:
        etl_config: ETL configuration rooted at the test directory.
        telemetry: Telemetry facade shared by the actors.
        routing: The routing key for this dataset.
        rounds: Number of ingestion rounds.
        inserts_per_round: Fresh keys inserted per round.
        updates_per_round: Existing keys re-upserted per round.
        deletes_per_round: Existing keys deleted per round.
        expected: The in-memory expected state, mutated by this thread only.
        pause_seconds: Sleep between rounds to interleave with the other actors.
    """
    for round_number in range(rounds):
        upserts: dict[int, int] = {}
        start_index: int = round_number * inserts_per_round
        for index in range(start_index, start_index + inserts_per_round):
            upserts[index] = round_number
        deletes: list[int] = []
        if round_number > 0:
            live: list[int] = sorted(expected)
            updates: list[int] = [index for index in live if index % 7 == round_number % 7][:updates_per_round]
            for index in updates:
                upserts[index] = round_number
            deletes = [index for index in live if index % 13 == round_number % 13 and index not in upserts][
                :deletes_per_round
            ]
        apply_merge(etl_config, telemetry, routing, build_group(routing, upserts, deletes))
        expected.update(upserts)
        for index in deletes:
            expected.pop(index)
        time.sleep(pause_seconds)


def compact_head_with_replan(uri: str, compactor: LanceCompactor, telemetry: Telemetry) -> str:
    """Run one tier-B plan/execute/commit cycle with the production re-plan loop.

    Mirrors :meth:`LanceCompactor.compact_one` without Spark: the rewrite tasks execute in process and the commit
    goes through :meth:`LanceCompactor.commit_rewrites` with its deliberately small manifest-race budget. A
    semantic commit conflict triggers a re-plan at the latest version instead of a re-commit, up to the configured
    ``replan_budget``, after which the dataset is skipped for this sweep.

    Args:
        uri: Dataset URI.
        compactor: The compactor carrying the tier-B configuration.
        telemetry: Telemetry facade shared by the actors.

    Returns:
        ``"noop"`` when nothing needed compacting, ``"committed"`` on success, or ``"skipped"`` when every
        re-plan cycle conflicted.
    """
    config: CompactionConfig = compactor.config
    cycles: int = 0
    while cycles < config.replan_budget:
        cycles += 1
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        plan = Compaction.plan(dataset, options=config.plan_options())
        task_jsons: list[str] = [task.json() for task in plan.tasks]
        if not task_jsons:
            return "noop"
        rewrites: list[str] = []
        for task_json in task_jsons:
            shard: lance.LanceDataset = lance.dataset(uri, version=plan.read_version)
            rewrites.append(CompactionTask.from_json(task_json).execute(shard).json())
        try:
            compactor.commit_rewrites(uri, rewrites, telemetry)
            return "committed"
        except (OSError, RuntimeError) as exc:
            if not is_commit_conflict_error(exc):
                raise
            telemetry.incr("dataset.replanned")
    telemetry.incr("dataset.hot_skipped")
    return "skipped"


def run_compactor_loop(
    head_uri: str,
    tail_uris: list[str],
    head_compactor: LanceCompactor,
    tail_config: CompactionConfig,
    telemetry: Telemetry,
    stop: threading.Event,
) -> None:
    """Sweep every dataset, compacting whichever exceeds its fragment threshold.

    Cleanup never runs inside this loop. The final cleanup happens once after every actor stops, so readers and
    committers pinned to older versions keep their transaction files for the whole run.

    Args:
        head_uri: The head dataset URI, compacted through the tier-B triad.
        tail_uris: Tail dataset URIs, compacted with the small-tier helper.
        head_compactor: The compactor carrying the tier-B configuration.
        tail_config: Small-tier compaction configuration with cleanup disabled.
        telemetry: Telemetry facade shared by the actors.
        stop: Set when ingestion finished and the loop should exit.
    """
    while not stop.is_set():
        if fragment_count(head_uri) > HEAD_COMPACT_FRAGMENT_THRESHOLD:
            compact_head_with_replan(head_uri, head_compactor, telemetry)
        for uri in tail_uris:
            if fragment_count(uri) > TAIL_COMPACT_FRAGMENT_THRESHOLD:
                compact_small_dataset(uri, tail_config, telemetry)
        time.sleep(COMPACTOR_SWEEP_PAUSE_SECONDS)


def build_segment_index(uri: str, handler: IndexHandler, config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Build one index increment through the production segment API in process.

    Mirrors :meth:`IndexHandler.build` without Spark by delegating to the production
    :func:`build_and_commit_segments` rebuild loop with an in-process segment builder: it resolves the target
    fragments, builds one uncommitted segment per shard against a version-pinned handle, and publishes through
    :func:`lance_etl.indexing.commit_segments`, which drops stale segments after a concurrent rewrite. When the
    commit would orphan fragments held by a wider existing segment that a compaction remapped, the loop re-resolves
    the fragment set at the latest version and rebuilds instead of letting the orphan ``ValueError`` kill the actor.
    The index's accumulated deltas are then bounded with :func:`merge_index_deltas`.

    Args:
        uri: Dataset URI.
        handler: The per-type index handler.
        config: Indexing configuration.
        telemetry: Telemetry facade shared by the actors.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    if handler.skip_reason(dataset) is not None:
        return
    handler.validate(dataset)

    def build_documents(groups: list[list[int]], version: int, artifacts: object | None) -> list[str]:
        """Build one serialized segment per shard in process against the pinned version.

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

    build_and_commit_segments(uri, handler, config, telemetry, build_documents)
    refreshed: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    if handler.index_name in {description.name for description in refreshed.describe_indices()}:
        merge_index_deltas(uri, handler.index_name, config, telemetry)


def fts_first_build(uri: str, handler: FtsIndexHandler, config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Build the inverted index through the production shared-uuid metadata-merge path.

    Each fragment is built under one shared index id against a version-pinned handle, the per-fragment metadata is
    merged, and the index is published with :meth:`FtsIndexHandler.commit_index`, whose stale-publish guard raises
    ``ValueError`` when a concurrent compaction rewrote covered fragments between build and commit.

    Args:
        uri: Dataset URI.
        handler: The FTS handler carrying the index name and publish path.
        config: Indexing configuration.
        telemetry: Telemetry facade shared by the actors.

    Raises:
        ValueError: When covered fragments no longer exist at publish time. The caller defers to the next pass.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
    if not fragment_ids:
        return
    pinned: lance.LanceDataset = lance.dataset(uri, version=dataset.version, storage_options=config.storage_options)
    shared_uuid: str = str(uuid.uuid4())
    params: dict[str, object] = config.fts_params()
    for fragment_id in fragment_ids:
        pinned.create_scalar_index(
            column=handler.column,
            index_type="INVERTED",
            name=handler.index_name,
            replace=False,
            index_uuid=shared_uuid,
            fragment_ids=[fragment_id],
            **params,
        )
    refreshed: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    refreshed.merge_index_metadata(shared_uuid, index_type="INVERTED")
    handler.commit_index(uri, refreshed, shared_uuid, fragment_ids, telemetry)


def maintain_head_indexes(
    uri: str,
    vector_handler: VectorIndexHandler,
    btree_handler: BTreeIndexHandler,
    fts_handler: FtsIndexHandler,
    config: IndexJobConfig,
    telemetry: Telemetry,
    events: Counter[str],
) -> None:
    """Run one incremental maintenance pass over the head dataset's indexes.

    Vector and BTREE indexes go through the segment-API increment, which covers exactly the uncovered fragments.
    The inverted index is built once through the metadata-merge path and afterwards maintained with
    ``optimize_indices`` plus delta bounding. A stale FTS publish (its loud-failure guard) is deferred to the next
    pass instead of killing the actor, matching how an orchestrated run would retry.

    Args:
        uri: Dataset URI.
        vector_handler: The IVF_RQ handler.
        btree_handler: The BTREE handler.
        fts_handler: The inverted-index handler.
        config: Indexing configuration.
        telemetry: Telemetry facade shared by the actors.
        events: Test-side event counter recording deferred FTS builds.
    """
    build_segment_index(uri, vector_handler, config, telemetry)
    build_segment_index(uri, btree_handler, config, telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    existing: set[str] = {description.name for description in dataset.describe_indices()}
    if fts_handler.index_name in existing:
        optimize_existing_index(uri, fts_handler.index_name, config, telemetry)
        merge_index_deltas(uri, fts_handler.index_name, config, telemetry)
    else:
        try:
            fts_first_build(uri, fts_handler, config, telemetry)
        except ValueError:
            events["fts_build_deferred"] += 1


def run_indexer_loop(
    head_uri: str,
    tail_uris: list[str],
    vector_handler: VectorIndexHandler,
    btree_handler: BTreeIndexHandler,
    fts_handler: FtsIndexHandler,
    config: IndexJobConfig,
    telemetry: Telemetry,
    stop: threading.Event,
    events: Counter[str],
) -> None:
    """Loop incremental index maintenance over every dataset until told to stop.

    Args:
        head_uri: The head dataset URI, maintained through the segment API.
        tail_uris: Tail dataset URIs, maintained with the small-tier local path.
        vector_handler: The IVF_RQ handler for the head dataset.
        btree_handler: The BTREE handler for the head dataset.
        fts_handler: The inverted-index handler for the head dataset.
        config: Indexing configuration.
        telemetry: Telemetry facade shared by the actors.
        stop: Set when ingestion finished and the loop should exit.
        events: Test-side event counter shared with the maintenance pass.
    """
    while not stop.is_set():
        if open_or_none(head_uri) is not None:
            maintain_head_indexes(head_uri, vector_handler, btree_handler, fts_handler, config, telemetry, events)
        for uri in tail_uris:
            if open_or_none(uri) is not None:
                index_dataset_locally(uri, config)
        time.sleep(INDEXER_SWEEP_PAUSE_SECONDS)


def run_actor(label: str, action: Callable[[], None], failures: list[str]) -> None:
    """Run one actor, capturing any failure instead of dying silently.

    Args:
        label: The actor name used in failure messages.
        action: The actor body.
        failures: Shared failure sink the test asserts empty.
    """
    try:
        action()
    except BaseException as exc:
        failures.append(f"{label}: {type(exc).__name__}: {exc}")


def collapse_index_deltas(uri: str, index_names: set[str], config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Merge every named index's accumulated deltas into one so compaction can bin freely.

    The Lance compaction planner never bins fragments whose covering index sets differ, and each index delta
    carries its own fragment bitmap, so fragments covered by different deltas of one logical index fence
    compaction bins and isolated sub-target fragments stay uncompactable. Collapsing each index to a single delta
    gives every covered fragment the same index set, letting the final compaction reach the target band.

    Args:
        uri: Dataset URI.
        index_names: The index names to collapse.
        config: Indexing configuration.
        telemetry: Telemetry facade shared by the actors.
    """
    dataset: lance.LanceDataset = lance.dataset(uri)
    existing: set[str] = {description.name for description in dataset.describe_indices()}
    for name in sorted(index_names & existing):
        deltas: int = index_delta_count(dataset, name)
        if deltas > 1:
            optimize_existing_index(uri, name, config, telemetry, num_indices_to_merge=deltas)


def assert_dataset_state(uri: str, expected: dict[int, int]) -> None:
    """Assert a dataset's content equals the expected state exactly.

    Args:
        uri: Dataset URI.
        expected: Map of surviving key index to its last upsert round.
    """
    table: pa.Table = lance.dataset(uri).to_table(columns=["vector_id", "val"])
    actual: dict[str, int] = dict(zip(table["vector_id"].to_pylist(), table["val"].to_pylist(), strict=True))
    assert len(actual) == table.num_rows, f"{uri} has duplicate keys"
    assert table.num_rows == len(expected), f"{uri} row count {table.num_rows} != expected {len(expected)}"
    expected_state: dict[str, int] = {
        key_name(index): checksum(index, round_number) for index, round_number in expected.items()
    }
    assert actual == expected_state, f"{uri} content diverged from the expected state"


def assert_full_index_coverage(uri: str, required_names: set[str]) -> None:
    """Assert the required indexes exist and every index covers every fragment.

    Args:
        uri: Dataset URI.
        required_names: Index names that must exist on the dataset.
    """
    dataset: lance.LanceDataset = lance.dataset(uri)
    names: set[str] = {description.name for description in dataset.describe_indices()}
    assert required_names <= names, f"{uri} is missing indexes: {sorted(required_names - names)}"
    for name in names:
        remaining: int = unindexed_fragment_count(dataset, name)
        assert remaining == 0, f"{uri} index {name} leaves {remaining} fragments unindexed"


def test_concurrent_ingest_compact_index_coexistence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three concurrent actors converge with zero data loss, full index coverage, and a bounded fragment count.

    Args:
        tmp_path: Pytest-provided temporary directory hosting the datasets.
        monkeypatch: Pytest monkeypatch used to count conflict metrics process-wide.
    """
    started: float = time.perf_counter()
    metric_counts: Counter[str] = Counter()
    metric_lock: threading.Lock = threading.Lock()
    original_incr: Callable[..., None] = Telemetry.incr

    def counting_incr(self: Telemetry, name: str, value: float = 1, tags: list[str] | None = None) -> None:
        """Count every counter metric emitted by any telemetry instance.

        Args:
            self: The telemetry facade the metric was emitted through.
            name: Metric name.
            value: Increment amount.
            tags: Optional metric tags.
        """
        with metric_lock:
            metric_counts[name] += int(value)
        original_incr(self, name, value, tags)

    monkeypatch.setattr(Telemetry, "incr", counting_incr)

    telemetry_config: TelemetryConfig = TelemetryConfig(service="coexistence-test", env="test")
    telemetry: Telemetry = Telemetry.create(telemetry_config, attach_lance_bridge=False)
    etl_config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    head_uri: str = dataset_uri(etl_config, *HEAD_ROUTING)
    tail_routings: list[tuple[str, str, str]] = [
        (f"org-tail{position}", "tenant1", "ns1") for position in range(TAIL_DATASETS)
    ]
    tail_uris: list[str] = [dataset_uri(etl_config, *routing) for routing in tail_routings]

    head_compactor: LanceCompactor = LanceCompactor(
        CompactionConfig(
            telemetry=telemetry_config,
            target_rows_per_fragment=HEAD_TARGET_ROWS_PER_FRAGMENT,
            commit_backoff_seconds=0.05,
            large_commit_retries=2,
            replan_budget=4,
        )
    )
    tail_compaction_config: CompactionConfig = CompactionConfig(
        telemetry=telemetry_config,
        commit_retries=30,
        commit_backoff_seconds=0.05,
    )
    index_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        vector_column="vector",
        num_partitions=8,
        vector_min_rows=1000,
        scalar_columns=["vector_id"],
        text_columns=["text"],
        num_shards=4,
        commit_retries=30,
        commit_backoff_seconds=0.05,
        max_index_deltas=4,
    )
    vector_handler: VectorIndexHandler = VectorIndexHandler(
        index_config, "vector", index_config.resolved_vector_index_name()
    )
    btree_handler: BTreeIndexHandler = BTreeIndexHandler(index_config, "vector_id", scalar_index_name("vector_id"))
    fts_handler: FtsIndexHandler = FtsIndexHandler(index_config, "text", fts_index_name("text"))

    expected_head: dict[int, int] = {}
    expected_tails: list[dict[int, int]] = [{} for _ in range(TAIL_DATASETS)]
    failures: list[str] = []
    events: Counter[str] = Counter()
    stop: threading.Event = threading.Event()

    ingester_threads: list[threading.Thread] = [
        threading.Thread(
            target=run_actor,
            args=(
                "ingester-head",
                partial(
                    run_ingester,
                    etl_config,
                    telemetry,
                    HEAD_ROUTING,
                    HEAD_ROUNDS,
                    HEAD_INSERTS_PER_ROUND,
                    HEAD_UPDATES_PER_ROUND,
                    HEAD_DELETES_PER_ROUND,
                    expected_head,
                    HEAD_INGEST_PAUSE_SECONDS,
                ),
                failures,
            ),
            name="ingester-head",
            daemon=True,
        )
    ]
    for position, routing in enumerate(tail_routings):
        ingester_threads.append(
            threading.Thread(
                target=run_actor,
                args=(
                    f"ingester-tail{position}",
                    partial(
                        run_ingester,
                        etl_config,
                        telemetry,
                        routing,
                        TAIL_ROUNDS,
                        TAIL_INSERTS_PER_ROUND,
                        TAIL_UPDATES_PER_ROUND,
                        TAIL_DELETES_PER_ROUND,
                        expected_tails[position],
                        TAIL_INGEST_PAUSE_SECONDS,
                    ),
                    failures,
                ),
                name=f"ingester-tail{position}",
                daemon=True,
            )
        )
    service_threads: list[threading.Thread] = [
        threading.Thread(
            target=run_actor,
            args=(
                "compactor",
                partial(
                    run_compactor_loop,
                    head_uri,
                    tail_uris,
                    head_compactor,
                    tail_compaction_config,
                    telemetry,
                    stop,
                ),
                failures,
            ),
            name="compactor",
            daemon=True,
        ),
        threading.Thread(
            target=run_actor,
            args=(
                "indexer",
                partial(
                    run_indexer_loop,
                    head_uri,
                    tail_uris,
                    vector_handler,
                    btree_handler,
                    fts_handler,
                    index_config,
                    telemetry,
                    stop,
                    events,
                ),
                failures,
            ),
            name="indexer",
            daemon=True,
        ),
    ]

    for thread in [*ingester_threads, *service_threads]:
        thread.start()
    for thread in ingester_threads:
        thread.join(JOIN_TIMEOUT_SECONDS)
    assert not any(thread.is_alive() for thread in ingester_threads), "ingestion did not finish in time"
    stop.set()
    for thread in service_threads:
        thread.join(JOIN_TIMEOUT_SECONDS)
    assert not any(thread.is_alive() for thread in service_threads), "a service actor did not stop in time"
    assert not failures, f"actors died: {failures}"

    head_required: set[str] = {
        index_config.resolved_vector_index_name(),
        scalar_index_name("vector_id"),
        fts_index_name("text"),
    }
    tail_required: set[str] = {scalar_index_name("vector_id"), fts_index_name("text")}

    maintain_head_indexes(head_uri, vector_handler, btree_handler, fts_handler, index_config, telemetry, events)
    for uri in tail_uris:
        index_dataset_locally(uri, index_config)
    collapse_index_deltas(head_uri, head_required, index_config, telemetry)
    for uri in tail_uris:
        collapse_index_deltas(uri, tail_required, index_config, telemetry)
    final_cycles: int = 0
    while final_cycles < FINAL_COMPACT_CYCLES:
        final_cycles += 1
        if compact_head_with_replan(head_uri, head_compactor, telemetry) == "noop":
            break
    for uri in tail_uris:
        compact_small_dataset(uri, tail_compaction_config, telemetry)
    maintain_head_indexes(head_uri, vector_handler, btree_handler, fts_handler, index_config, telemetry, events)
    for uri in tail_uris:
        index_dataset_locally(uri, index_config)

    cleanup_config: CompactionConfig = CompactionConfig(telemetry=telemetry_config)
    bytes_removed: int = 0
    for uri in [head_uri, *tail_uris]:
        bytes_removed += cleanup_dataset(uri, cleanup_config, telemetry)
    assert bytes_removed >= 0

    assert_dataset_state(head_uri, expected_head)
    for position, uri in enumerate(tail_uris):
        assert_dataset_state(uri, expected_tails[position])

    head_dataset: lance.LanceDataset = lance.dataset(head_uri)
    head_fragments: int = len(head_dataset.get_fragments())
    head_band: int = math.ceil(len(expected_head) / HEAD_TARGET_ROWS_PER_FRAGMENT) + 1
    assert head_fragments <= head_band, f"head has {head_fragments} fragments, band is {head_band}"
    tail_fragment_counts: list[int] = [fragment_count(uri) for uri in tail_uris]
    assert all(count <= TAIL_COMPACT_FRAGMENT_THRESHOLD for count in tail_fragment_counts), tail_fragment_counts

    assert_full_index_coverage(head_uri, head_required)
    for uri in tail_uris:
        assert_full_index_coverage(uri, tail_required)

    probe: int = sorted(expected_head)[len(expected_head) // 2]
    probe_round: int = expected_head[probe]
    nearest: pa.Table = head_dataset.to_table(
        columns=["vector_id"],
        nearest={
            "column": "vector",
            "q": vector_values(probe, probe_round),
            "k": 5,
            "nprobes": 8,
            "refine_factor": 100,
        },
    )
    assert nearest["vector_id"][0].as_py() == key_name(probe), "vector query missed the exact-match key"
    fts_hits: pa.Table = head_dataset.to_table(columns=["vector_id"], full_text_query=f"token{probe}")
    assert fts_hits["vector_id"].to_pylist() == [key_name(probe)], "full-text query returned the wrong rows"
    scalar_hits: pa.Table = head_dataset.to_table(filter=f"vector_id = '{key_name(probe)}'")
    assert scalar_hits.num_rows == 1, "scalar query did not return exactly one row"
    assert scalar_hits["val"][0].as_py() == checksum(probe, probe_round), "scalar query returned a stale payload"

    tail_probe: int = sorted(expected_tails[0])[0]
    tail_dataset: lance.LanceDataset = lance.dataset(tail_uris[0])
    tail_fts: pa.Table = tail_dataset.to_table(columns=["vector_id"], full_text_query=f"token{tail_probe}")
    assert tail_fts["vector_id"].to_pylist() == [key_name(tail_probe)]

    conflicts_retried: int = sum(value for name, value in metric_counts.items() if "commit_conflict" in name)
    assert conflicts_retried >= 0
    elapsed: float = time.perf_counter() - started
    print(
        "coexistence summary: "
        f"runtime_s={elapsed:.1f} "
        f"conflicts_retried={conflicts_retried} "
        f"replans={metric_counts['dataset.replanned']} "
        f"hot_skips={metric_counts['dataset.hot_skipped']} "
        f"stale_segments_dropped={metric_counts['index.stale_segments_dropped']} "
        f"fts_builds_deferred={events['fts_build_deferred']} "
        f"deltas_merged={metric_counts['index.deltas_merged']} "
        f"head_rows={len(expected_head)} head_fragments={head_fragments} "
        f"tail_fragments={tail_fragment_counts}"
    )


def deterministic_vector(identifier: int, dim: int) -> list[float]:
    """Return a deterministic, per-id unique vector so an exact nearest query has a single zero-distance answer.

    Args:
        identifier: The integer row id seeding the vector.
        dim: The vector dimension.

    Returns:
        A vector of ``dim`` floats in ``[0, 1)``.
    """
    generator: random.Random = random.Random(identifier * 2_654_435_761)
    return [generator.random() for _ in range(dim)]


def make_vector_table(start: int, count: int, dim: int) -> pa.Table:
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
    """Write a dataset of exactly two fragments: a large clean one and a smaller one to be partly deleted.

    Fragment 0 carries ids ``0..299`` and fragment 1 carries ids ``300..499``. Keeping fragment 0 above the
    compaction target while fragment 1 accrues deletions lets a later compaction rewrite fragment 1 alone, which is
    what remaps a wider existing index segment over the surviving fragment and triggers the orphan-fragment race.

    Args:
        uri: Dataset URI.
    """
    lance.write_dataset(make_vector_table(0, 300, DIM), uri, mode="create", max_rows_per_file=1_000_000)
    lance.write_dataset(make_vector_table(300, 200, DIM), uri, mode="append", max_rows_per_file=1_000_000)


def racing_compaction_commit(
    uri: str,
    compaction_config: CompactionConfig,
    telemetry: Telemetry,
    state: dict[str, bool],
) -> Callable[..., int]:
    """Build a ``commit_segments`` replacement that compacts once before the first commit, then records orphans.

    The first segment commit runs a compaction that rewrites the smaller fragment and remaps the wider existing
    segment over the surviving one, exactly the window the production code must survive. The real commit is then
    invoked. An orphan-fragment ``ValueError`` is recorded and re-raised so the production rebuild loop in
    :func:`build_and_commit_segments` re-resolves the fragment set and re-commits.

    Args:
        uri: Dataset URI.
        compaction_config: Configuration for the racing compaction.
        telemetry: Telemetry facade.
        state: Mutable flags recording whether the compaction ran and whether an orphan error was raised.

    Returns:
        A drop-in replacement for :func:`lance_etl.indexing.commit_segments`.
    """
    real_commit: Callable[..., int] = indexing.commit_segments

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
            compact_small_dataset(uri, compaction_config, telemetry)
        try:
            return real_commit(*args, **kwargs)
        except ValueError as exc:
            if is_stale_fragment_error(exc):
                state["orphan"] = True
            raise

    return commit


def test_vector_segment_commit_survives_compaction_orphan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A compaction that remaps a wider IVF_RQ segment over a freshly built shard must not kill the indexer.

    Reproduces the orphan-fragment race deterministically: an existing merged vector segment covers both fragments,
    a rebuild pass builds one shard segment per fragment, and a hooked compaction rewrites the smaller fragment and
    remaps the wider segment over the survivor between the build and the commit. Publishing the surviving shard then
    raises lance's ``"would orphan fragments"`` ``ValueError`` (``rust/lance/src/index.rs:1233``). The fix re-resolves
    the fragment set at the latest version, rebuilds, and re-commits, so every live fragment ends up covered and a
    vector query stays exact.

    Args:
        tmp_path: Pytest-provided temporary directory hosting the dataset.
        monkeypatch: Pytest monkeypatch used to interleave the compaction with the commit.
    """
    uri: str = str(tmp_path / "orphan_vector")
    write_two_fragment_dataset(uri)
    telemetry_config: TelemetryConfig = TelemetryConfig(service="orphan-vector-test", env="test")
    telemetry: Telemetry = Telemetry.create(telemetry_config, attach_lance_bridge=False)
    shared: dict[str, Any] = {
        "telemetry": telemetry_config,
        "vector_column": "vector",
        "num_partitions": 4,
        "vector_min_rows": 10,
        "num_shards": 8,
        "commit_retries": 10,
        "commit_backoff_seconds": 0.0,
    }
    build_config: IndexJobConfig = IndexJobConfig(**shared)
    index_name: str = build_config.resolved_vector_index_name()
    build_segment_index(uri, VectorIndexHandler(build_config, "vector", index_name), build_config, telemetry)
    assert unindexed_fragment_count(lance.dataset(uri), index_name) == 0

    lance.dataset(uri).delete("id >= 300 and id < 450")
    rebuild_config: IndexJobConfig = IndexJobConfig(rebuild=True, **shared)
    compaction_config: CompactionConfig = CompactionConfig(
        telemetry=telemetry_config,
        target_rows_per_fragment=250,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    state: dict[str, bool] = {"compacted": False, "orphan": False}
    monkeypatch.setattr(indexing, "commit_segments", racing_compaction_commit(uri, compaction_config, telemetry, state))
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
    """A compaction that remaps a wider BTREE segment over a freshly built shard must not kill the indexer.

    The scalar segment path shares the same rebuild loop as the vector path, so the orphan-fragment guard is proven
    here too: a wide single-segment BTREE covers both fragments, a sharded rebuild builds one segment per fragment,
    and a hooked compaction rewrites the smaller fragment and remaps the wider segment over the survivor. The
    surviving shard would orphan the rewritten fragment, so the commit raises and the loop re-resolves and rebuilds.

    Args:
        tmp_path: Pytest-provided temporary directory hosting the dataset.
        monkeypatch: Pytest monkeypatch used to interleave the compaction with the commit.
    """
    uri: str = str(tmp_path / "orphan_btree")
    write_two_fragment_dataset(uri)
    telemetry_config: TelemetryConfig = TelemetryConfig(service="orphan-scalar-test", env="test")
    telemetry: Telemetry = Telemetry.create(telemetry_config, attach_lance_bridge=False)
    index_name: str = scalar_index_name("id")
    wide_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        scalar_columns=["id"],
        num_shards=1,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    build_segment_index(uri, BTreeIndexHandler(wide_config, "id", index_name), wide_config, telemetry)
    assert unindexed_fragment_count(lance.dataset(uri), index_name) == 0

    lance.dataset(uri).delete("id >= 300 and id < 450")
    rebuild_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        scalar_columns=["id"],
        num_shards=8,
        rebuild=True,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    compaction_config: CompactionConfig = CompactionConfig(
        telemetry=telemetry_config,
        target_rows_per_fragment=250,
        commit_retries=10,
        commit_backoff_seconds=0.0,
    )
    state: dict[str, bool] = {"compacted": False, "orphan": False}
    monkeypatch.setattr(indexing, "commit_segments", racing_compaction_commit(uri, compaction_config, telemetry, state))
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
