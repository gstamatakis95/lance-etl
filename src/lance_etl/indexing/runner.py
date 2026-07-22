"""LanceIndexer: unified fleet orchestration for index builds.

Every dataset, regardless of size, follows the same phases built on the same Lance segment
APIs, and a small dataset is simply the one-shard case:

- Plan (:func:`plan_dataset_indexes`): a per-dataset executor fan-out resolves which indexes to
  build (explicit config columns, or the ``lance-etl.columns`` role metadata written by the ETL
  sink), runs the derived-state skip check, and shards each index's target fragments into
  build tasks sized by ``fragments_per_index_task``. A vector index whose artifacts are absent,
  mismatched, or growth-stale plans one ``bootstrap`` task instead of shards (ADR 0030).
- Build (:meth:`LanceIndexer.build_fleet_segments`): ONE flat Spark job over every dataset's
  shard tasks. Each vector segment shard resolves its OWN dataset's IVF_RQ artifacts on the
  executor — centroids read sidecar-first from the object-store cache with a ``get_ivf_model``
  fallback, the RaBitQ rotation from the stored config (ADR 0040), so no fleet-wide centroid
  broadcast is ever built. Vector and scalar shards build uncommitted segments, a vector
  bootstrap task runs a committed ``create_index`` whose internal streaming k-means trains the
  centroids and caches them to the sidecar, FTS rebuild shards build per-fragment inverted
  indices under their dataset's shared index id, and FTS maintain runs as a single task per
  dataset.
- Commit (:func:`commit_one_index`): a per-(dataset, index) executor fan-out merges vector
  segments and publishes through the production commit paths, keeping the heavy merge off the
  driver. A stale-fragment commit (a concurrent compaction rewrote planned fragments) marks the
  index for the next replan round instead of failing the run.
- Delta bound (:func:`~lance_etl.indexing.optimize.merge_index_deltas`): a final per-(dataset, index) fan-out merges
  accumulated index deltas once they exceed ``max_index_deltas``.

:meth:`LanceIndexer.run` repeats plan-build-commit for stale indexes up to
:data:`~lance_etl.indexing.config.MAX_STALE_REPLANS` rounds. A dataset still stale after every
round is recorded as a failed dataset (:data:`STALE_REPLAN_EXHAUSTED_PHASE`) rather than deferred
silently, so the fleet's failed-dataset count, the ``index.stale_replans_exhausted`` metric, and the
CLI exit code all reflect the partially indexed dataset instead of a clean run masking it.
"""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Callable
from typing import Any

import lance
from lance.lance import indices as native_indices
from pyspark.sql import SparkSession

from lance_etl.column_roles import SCALAR_ROLE, TEXT_ROLE, VECTOR_ROLE, load_column_roles
from lance_etl.fanout import (
    BUILD_PARTITION_FACTOR,
    FANOUT_PARTITION_FACTOR,
    derive_partitions,
    fan_out_per_dataset,
    report_fleet_failures,
)
from lance_etl.indexing.config import (
    IndexJobConfig,
    bitmap_index_name,
    degrade_num_partitions,
    derive_num_partitions,
    fts_index_name,
    growth_exceeds_retrain_factor,
    scalar_index_name,
    vector_index_name,
    zonemap_index_name,
)
from lance_etl.indexing.handlers import (
    BitmapIndexHandler,
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    VectorIndexHandler,
    ZonemapIndexHandler,
    commit_fts_index,
)
from lance_etl.indexing.optimize import (
    index_delta_count,
    load_vector_config,
    maintain_index_locally,
    merge_index_deltas,
    save_centroids,
    write_vector_config,
)
from lance_etl.indexing.segments import (
    commit_index_with_retries,
    commit_segments,
    is_stale_fragment_error,
    serialize_segment,
    split_evenly,
)
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)

VECTOR_KIND: str = "vector"
"""Index kind for IVF_RQ vector indexes."""

BTREE_KIND: str = "btree"
"""Index kind for BTREE scalar indexes."""

BITMAP_KIND: str = "bitmap"
"""Index kind for BITMAP scalar indexes."""

ZONEMAP_KIND: str = "zonemap"
"""Index kind for ZONEMAP scalar indexes."""

FTS_KIND: str = "fts"
"""Index kind for BM25 INVERTED full-text indexes."""

KIND_TO_HANDLER: dict[str, type[IndexHandler]] = {
    VECTOR_KIND: VectorIndexHandler,
    BTREE_KIND: BTreeIndexHandler,
    BITMAP_KIND: BitmapIndexHandler,
    ZONEMAP_KIND: ZonemapIndexHandler,
    FTS_KIND: FtsIndexHandler,
}
"""Maps an index kind to the handler class owning its type-specific logic."""

STALE_REPLAN_EXHAUSTED_PHASE: str = "index-stale-exhausted"
"""``error_phase`` marker for a dataset still stale after :data:`MAX_STALE_REPLANS` rounds."""

STALE_REPLANS_EXHAUSTED_METRIC: str = "index.stale_replans_exhausted"
"""Metric incremented once per dataset that exhausts every stale-replan round unresolved."""


def index_failure_phase(result: dict[str, Any]) -> str:
    """Return the constant ``dataset.failed`` metric phase tag for a failed indexing dataset.

    Args:
        result: The failed dataset's terminal result. Unused: indexing tags every failure with the
            single ``index`` phase, unlike maintenance, whose failures carry a per-phase marker.

    Returns:
        The literal phase tag ``"index"``.
    """
    del result
    return "index"


def make_handler(kind: str, column: str, index_name: str, config: IndexJobConfig) -> IndexHandler:
    """Instantiate the handler for one index kind.

    Args:
        kind: One of the ``*_KIND`` constants.
        column: The column to index.
        index_name: The index name to publish under.
        config: Indexing configuration.

    Returns:
        The handler instance.
    """
    return KIND_TO_HANDLER[kind](config, column, index_name)


def resolve_index_targets(dataset: lance.LanceDataset, config: IndexJobConfig) -> list[tuple[str, str, str]]:
    """Resolve which indexes a dataset should carry, from explicit config or role metadata.

    When any column list is set on the config, the explicit lists win unchanged (which is also
    the opt-out for role discovery). Otherwise the dataset's own ``lance-etl.columns`` role
    metadata (written by the ETL sink) drives the decision per dataset: every ``vector`` role
    column gets an IVF_RQ index, every ``scalar`` role column gets a BTREE index, and every
    ``text`` role column gets a BM25 INVERTED index (ADR 0029). This lets one fleet run serve
    heterogeneous per-tenant schemas without per-dataset CLI flags. BITMAP and ZONEMAP carry no
    role of their own, so they are only ever selected through the explicit config columns.

    Args:
        dataset: The open dataset.
        config: Indexing configuration.

    Returns:
        ``(kind, column, index_name)`` triples in vector, btree, bitmap, zonemap, text order.
    """
    explicit: bool = bool(
        config.vector_columns
        or config.scalar_columns
        or config.bitmap_columns
        or config.zonemap_columns
        or config.text_columns
    )
    if explicit:
        targets: list[tuple[str, str, str]] = []
        targets.extend(
            (VECTOR_KIND, column, config.index_name(column, vector_index_name(column)))
            for column in config.vector_columns
        )
        targets.extend(
            (BTREE_KIND, column, config.index_name(column, scalar_index_name(column)))
            for column in config.scalar_columns
        )
        targets.extend(
            (BITMAP_KIND, column, config.index_name(column, bitmap_index_name(column)))
            for column in config.bitmap_columns
        )
        targets.extend(
            (ZONEMAP_KIND, column, config.index_name(column, zonemap_index_name(column)))
            for column in config.zonemap_columns
        )
        targets.extend(
            (FTS_KIND, column, config.index_name(column, fts_index_name(column))) for column in config.text_columns
        )
        return targets

    roles: dict[str, str] = load_column_roles(dataset)
    columns: set[str] = set(dataset.schema.names)
    discovered: list[tuple[str, str, str]] = []
    column: Any
    for column in sorted(name for name, role in roles.items() if role == VECTOR_ROLE and name in columns):
        discovered.append((VECTOR_KIND, column, vector_index_name(column)))
    for column in sorted(name for name, role in roles.items() if role == SCALAR_ROLE and name in columns):
        discovered.append((BTREE_KIND, column, scalar_index_name(column)))
    for column in sorted(name for name, role in roles.items() if role == TEXT_ROLE and name in columns):
        discovered.append((FTS_KIND, column, fts_index_name(column)))
    return discovered


def vector_index_needs_retrain(
    dataset: lance.LanceDataset,
    column: str,
    count_rows: Callable[[], int],
    retrain_growth_factor: float = 4.0,
) -> bool:
    """Report whether an existing vector index needs a full retrain.

    The index needs work when its ``lance-etl.vector.{column}`` config entry is absent or carries
    no positive ``rows_at_train`` (an index built outside the segment path awaiting a full
    rebuild), or when the row count grew past :data:`~lance_etl.indexing.config.RETRAIN_GROWTH_FACTOR`
    times the recorded ``rows_at_train``, per :func:`~lance_etl.indexing.config.growth_exceeds_retrain_factor`.
    The config read comes from the already-loaded manifest.

    Args:
        dataset: The already-open dataset handle.
        column: The indexed vector column.
        count_rows: Cached row-count supplier shared across the dataset's per-index checks.
        retrain_growth_factor: Dataset growth multiple that forces artifact rotation.

    Returns:
        ``True`` when the vector index requires a retrain.
    """
    cfg: dict[str, Any] | None = load_vector_config(dataset, column)
    if cfg is None:
        return True
    rows_at_train: int = int(cfg.get("rows_at_train") or 0)
    if rows_at_train <= 0:
        return True
    return growth_exceeds_retrain_factor(count_rows(), rows_at_train, retrain_growth_factor)


def index_needs_work(
    dataset: lance.LanceDataset,
    config: IndexJobConfig,
    existing: set[str],
    kind: str,
    column: str,
    name: str,
    count_rows: Callable[[], int],
) -> bool:
    """Report whether one targeted index requires a build, delta merge, or retrain.

    An absent index needs work, unless it is a vector index and the row count is below
    ``config.vector_min_rows`` (intended skip — flat KNN suffices). A present index needs work
    when ``dataset.stats.index_stats(name)`` shows unindexed fragments or more deltas than
    ``config.max_index_deltas``, and a present vector index additionally when
    :func:`vector_index_needs_retrain` reports so.

    Args:
        dataset: The already-open dataset handle.
        config: Indexing configuration.
        existing: Names of the indexes the dataset currently carries.
        kind: One of the ``*_KIND`` constants.
        column: The indexed column.
        name: The index name.
        count_rows: Cached row-count supplier shared across the dataset's per-index checks.

    Returns:
        ``True`` when the index requires attention this run.
    """
    if name not in existing:
        return kind != VECTOR_KIND or count_rows() >= config.vector_min_rows
    stats: dict[str, Any] = dataset.stats.index_stats(name)
    if int(stats.get("num_unindexed_fragments") or 0) > 0:
        return True
    if int(stats.get("num_indices") or 0) > config.max_index_deltas:
        return True
    return kind == VECTOR_KIND and vector_index_needs_retrain(
        dataset,
        column,
        count_rows,
        config.retrain_growth_factor,
    )


def index_skip_reason(
    dataset: lance.LanceDataset, config: IndexJobConfig, targets: list[tuple[str, str, str]]
) -> str | None:
    """Return a reason string when all targeted indices are current, or None to proceed.

    Evaluates derived dataset state from the already-open handle so no extra object-store I/O is
    needed. The check is bypassed when ``config.rebuild`` is True. Each target is judged by
    :func:`index_needs_work`, with the row count computed at most once per dataset.

    Args:
        dataset: The already-open dataset handle.
        config: Indexing configuration.
        targets: The resolved ``(kind, column, index_name)`` triples for this dataset.

    Returns:
        A human-readable skip reason when all indices are current, or ``None`` when at least one
        index requires attention.
    """
    if config.rebuild:
        return None
    if not targets:
        return "no indices configured"

    existing: set[str] = {description.name for description in dataset.describe_indices()}
    rows: int | None = None

    def count_rows() -> int:
        """Count the dataset rows once and cache the result across the per-index checks.

        Returns:
            The dataset row count.
        """
        nonlocal rows
        if rows is None:
            rows = dataset.count_rows()
        return rows

    kind: Any
    column: Any
    name: Any
    for kind, column, name in targets:
        if index_needs_work(dataset, config, existing, kind, column, name, count_rows):
            return None
    return "all indices current"


def shard_count(target_fragments: int, config: IndexJobConfig) -> int:
    """Derive the build-task count for one index from its target fragment count.

    Args:
        target_fragments: Number of fragments the index build must cover.
        config: Indexing configuration supplying ``fragments_per_index_task``.

    Returns:
        The shard count: one task per ``fragments_per_index_task`` fragments, at least one.
    """
    return max(1, math.ceil(target_fragments / config.fragments_per_index_task))


def index_preflight_outcome(
    handler: IndexHandler,
    dataset: lance.LanceDataset,
    uri: str,
    column: str,
    index_name: str,
    telemetry: Telemetry,
) -> dict[str, Any] | None:
    """Return a terminal skip or validation-error outcome for one index.

    Args:
        handler: Type-specific index handler.
        dataset: Dataset being planned.
        uri: Dataset URI for diagnostics.
        column: Indexed column.
        index_name: Published index name.
        telemetry: Executor telemetry facade.

    Returns:
        A terminal result when the index should skip or fails validation, otherwise ``None``.
    """
    reason: str | None = handler.skip_reason(dataset)
    if reason is not None:
        telemetry.incr("index.skipped", tags=[f"index:{index_name}"])
        return {"column": column, "index": index_name, "segments": 0, "fragments": 0, "skipped": reason}
    try:
        handler.validate(dataset)
    except Exception as exc:
        telemetry.incr("index.validation_error", tags=[f"index:{index_name}"])
        logger.warning("index validation failed for %s on %s, isolating: %s", index_name, uri, exc)
        return {
            "column": column,
            "index": index_name,
            "segments": 0,
            "fragments": 0,
            "error": str(exc),
            "phase": "validation",
        }
    return None


def plan_dataset_indexes(
    uri: str,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run the plan phase for one dataset on an executor.

    Opens the dataset once (failure isolation: an unreadable dataset returns a counted ``error``
    record, since it cannot be planned at all, not benign "nothing to do"), resolves the index
    targets, applies the fleet-level and per-index skip checks, and shards each index's target
    fragments into build tasks. A vector index whose artifacts are absent, mismatched, or
    growth-stale (or a ``rebuild`` run) plans one ``bootstrap`` task: a committed ``create_index``
    whose internal streaming k-means trains the centroids (ADR 0030). A vector index with reusable
    artifacts plans incremental ``segments`` shards as usual.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A dict with ``uri`` and either ``error``, ``skipped``, or ``version`` plus per-index
        ``specs``. Each spec carries ``kind``, ``column``, ``index_name``, ``mode``, ``shards``,
        and the FTS rebuild extra ``index_uuid``. Indexes with nothing to do land in ``done`` as
        finished stats.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, OSError, ValueError) as exc:
        logger.warning("indexing: cannot open dataset %s, failing: %s", uri, exc)
        telemetry.incr("dataset.index_open_error")
        return {"uri": uri, "indexes": [], "error": str(exc), "phase": "open"}

    targets: list[tuple[str, str, str]] = resolve_index_targets(dataset, config)
    skip: str | None = index_skip_reason(dataset, config, targets)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        return {"uri": uri, "indexes": [], "skipped": skip}

    existing_names: set[str] = {description.name for description in dataset.describe_indices()}
    specs: list[dict[str, Any]] = []
    done: list[dict[str, Any]] = []
    kind: Any
    column: Any
    index_name: Any
    for kind, column, index_name in targets:
        handler: IndexHandler = make_handler(kind, column, index_name, config)
        preflight: dict[str, Any] | None = index_preflight_outcome(handler, dataset, uri, column, index_name, telemetry)
        if preflight is not None:
            done.append(preflight)
            continue

        if kind == FTS_KIND:
            fts_handler: FtsIndexHandler = handler
            if fts_handler.maintainable(dataset):
                specs.append(
                    {"kind": kind, "column": column, "index_name": index_name, "mode": "maintain", "shards": []}
                )
                continue
            fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
            if not fragment_ids:
                done.append({"column": column, "index": index_name, "segments": 0, "fragments": 0})
                continue
            specs.append(
                {
                    "kind": kind,
                    "column": column,
                    "index_name": index_name,
                    "mode": "rebuild",
                    "shards": split_evenly(fragment_ids, shard_count(len(fragment_ids), config)),
                    "fragments": fragment_ids,
                    "index_uuid": str(uuid.uuid4()),
                }
            )
            continue

        if kind == VECTOR_KIND:
            vector_handler: VectorIndexHandler = handler
            if config.rebuild or index_name not in existing_names or vector_handler.needs_bootstrap(dataset):
                specs.append(
                    {
                        "kind": kind,
                        "column": column,
                        "index_name": index_name,
                        "mode": "bootstrap",
                        "shards": [],
                        "fragments": len(dataset.get_fragments()),
                    }
                )
                continue
        target_ids: list[int] = handler.target_fragments(dataset)
        if not target_ids:
            stats: dict[str, Any] = {"column": column, "index": index_name, "segments": 0, "fragments": 0}
            if index_name in existing_names and index_delta_count(dataset, index_name) > config.max_index_deltas:
                stats["needs_delta_merge"] = True
            done.append(stats)
            continue
        specs.append(
            {
                "kind": kind,
                "column": column,
                "index_name": index_name,
                "mode": "segments",
                "shards": split_evenly(target_ids, shard_count(len(target_ids), config)),
                "fragments": len(target_ids),
            }
        )

    return {"uri": uri, "version": dataset.version, "specs": specs, "done": done}


def bootstrap_vector_index(
    uri: str,
    column: str,
    index_name: str,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Build one vector index from scratch with streaming k-means, committed in one task.

    Runs a committed ``create_index`` whose internal training uses lance's streaming k-means
    (bounded memory regardless of partition count), sharing a freshly minted RaBitQ rotation so
    later incremental segments stay on the same model. The rotation is minted by
    ``lance.lance.indices.build_rq_model``, imported at module scope as ``native_indices``. That
    module is the compiled PyO3 extension backing pylance, not the public ``lance.indices``
    package, so this is the least-protected import in the repo: no deprecation contract covers it,
    and pylance could relocate or rename it across majors without warning. It is verified present
    with this exact signature (``build_rq_model(dimension, num_bits=1, dtype="float32")``) on the
    pinned ``pylance==8.0.0``, per ``src/lance_etl/AGENTS.md``'s API ground truth section. Any
    pylance version bump must re-verify this import deliberately rather than assuming it survives
    unchanged. After the commit the artifact config is stored and the trained centroids are cached
    to the object-store sidecar keyed by ``rows_at_train`` (:func:`persist_bootstrap_centroids`),
    so future runs reuse them sidecar-first with a ``get_ivf_model`` fallback (ADR 0040).
    ``replace=True`` makes a growth retrain a wholesale index replacement.

    The commit is wrapped in :func:`commit_index_with_retries` (``config.commit_retries``
    budget) so a concurrent maintenance, compaction, or ETL commit on the same dataset no longer
    aborts the whole fleet build round: this was the one commit path in the repo with no retry
    wrapper. Each retry re-opens the dataset fresh so it rebases on the latest version before
    retrying ``create_index``. Because the committed path trains and commits in a single call, a
    retry re-runs streaming k-means rather than only the cheap commit. That is acceptable here:
    bootstrap conflicts are rare (a fresh index build, with ingest and indexing serialized per
    dataset by the pipeline), and the alternative is the entire fleet build aborting.

    Args:
        uri: Dataset URI.
        column: The vector column to index.
        index_name: The index name to publish under.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        The finished index stats dict.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    handler: VectorIndexHandler = VectorIndexHandler(config, column, index_name)
    dimension: int = handler.dimension(dataset)
    rows: int = dataset.count_rows()
    planned: int = derive_num_partitions(
        rows,
        config.num_partitions,
        config.minimum_partitions,
        config.maximum_partitions,
        config.target_rows_per_partition,
    )
    partitions: int = degrade_num_partitions(planned, rows, config.streaming_sample_rate)
    rabitq_model: str = native_indices.build_rq_model(dimension=dimension, num_bits=config.num_bits)

    def action() -> lance.LanceDataset:
        """Re-open the dataset at the latest version and run the committed create_index."""
        fresh: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            fresh.create_index(
                column,
                "IVF_RQ",
                name=index_name,
                metric=config.metric,
                replace=True,
                num_partitions=partitions,
                num_bits=config.num_bits,
                rabitq_model=rabitq_model,
                streaming_sample_rate=config.streaming_sample_rate,
                streaming_refine_passes=config.streaming_refine_passes,
            )
        return fresh

    committed_dataset: lance.LanceDataset = commit_index_with_retries(
        action, config, telemetry, tags=[f"index:{index_name}"]
    )
    telemetry.incr("index.committed", tags=[f"index:{index_name}"])
    telemetry.incr("artifacts.trained")
    write_vector_config(
        uri,
        column,
        {
            "rows_at_train": rows,
            "dimension": dimension,
            "metric": config.metric,
            "num_bits": config.num_bits,
            "num_partitions": partitions,
            "rabitq_model": rabitq_model,
        },
        config,
        telemetry,
    )
    persist_bootstrap_centroids(committed_dataset, uri, index_name, rows, config, telemetry)
    return {
        "column": column,
        "index": index_name,
        "segments": 1,
        "fragments": len(committed_dataset.get_fragments()),
        "num_partitions": partitions,
        "reused_artifacts": False,
    }


def persist_bootstrap_centroids(
    dataset: lance.LanceDataset,
    uri: str,
    index_name: str,
    rows_at_train: int,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> None:
    """Cache the freshly trained centroids to the object-store sidecar, best-effort.

    Reads the committed centroids back via ``get_ivf_model`` and writes them to the sidecar keyed
    by ``rows_at_train`` so future runs reuse them without re-reading the index (ADR 0040). This is
    a pure cache write: the index is already committed and the RaBitQ rotation plus fingerprint are
    already in the config, so a sidecar-write failure is counted, logged, and swallowed rather than
    re-raised. A missing sidecar simply routes the next run through the ``get_ivf_model`` fallback.

    Args:
        dataset: The dataset whose committed index carries the trained centroids.
        uri: Dataset URI.
        index_name: The vector index name.
        rows_at_train: The staleness fingerprint keying the sidecar generation.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.
    """
    try:
        centroids: Any = dataset.get_ivf_model(index_name).centroids
        save_centroids(uri, index_name, centroids, config.metric, rows_at_train, config.storage_options)
        telemetry.incr("artifacts.centroid_sidecar_written")
    except Exception as exc:
        telemetry.incr("index.centroid_sidecar_write_error")
        logger.warning("centroid sidecar write failed for %s on %s: %s", index_name, uri, exc)


def build_one_shard(
    task: dict[str, Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> tuple[str, str, dict[str, Any]]:
    """Build one flat-job task on an executor: a segment shard, FTS fragment shard, or FTS maintain.

    A non-bootstrap, non-FTS shard dispatches through :func:`make_handler`: the handler's
    ``prepare`` resolves any artifacts to broadcast to the shard build and its ``build_segment``
    builds the uncommitted segment, so each index kind's segment-API call is owned by its handler
    instead of being duplicated here. A vector segment shard resolves its OWN dataset's IVF_RQ
    artifacts from the already-open version-pinned handle through :meth:`VectorIndexHandler.prepare`
    — centroids read sidecar-first from the object-store cache with a ``get_ivf_model`` fallback,
    the RaBitQ rotation from the stored config (ADR 0040). There is no fleet-wide artifact
    broadcast: each task reads only the one dataset it builds.

    Args:
        task: The shard task spec from the plan phase, flattened with ``uri`` and ``version``.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        ``(uri, index_name, payload)`` where the payload carries a serialized ``segment``, an
        FTS ``built`` count, or a finished ``stats`` dict for maintain tasks.
    """
    uri: str = task["uri"]
    kind: str = task["kind"]
    column: str = task["column"]
    index_name: str = task["index_name"]
    tags: list[str] = [f"index_type:{kind}"]

    if kind == VECTOR_KIND and task["mode"] == "bootstrap":
        bootstrap_stats: dict[str, Any] = bootstrap_vector_index(uri, column, index_name, config, telemetry)
        return uri, index_name, {"stats": bootstrap_stats}

    if kind == FTS_KIND and task["mode"] == "maintain":
        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            merged: bool = maintain_index_locally(uri, index_name, config, telemetry)
        stats: dict[str, Any] = {
            "column": column,
            "index": index_name,
            "segments": 0,
            "fragments": 0,
            "maintained": True,
            "deltas_merged": merged,
        }
        return uri, index_name, {"stats": stats}

    shard: list[int] = task["shard"]
    dataset: lance.LanceDataset = lance.dataset(uri, version=task["version"], storage_options=config.storage_options)
    if kind == FTS_KIND:
        built: int = 0
        fragment_id: Any
        for fragment_id in shard:
            with telemetry.timed("segment.build_ms", tags=tags):
                dataset.create_scalar_index(
                    column=column,
                    index_type="INVERTED",
                    name=index_name,
                    replace=True,
                    index_uuid=task["index_uuid"],
                    fragment_ids=[fragment_id],
                    **config.fts_params(),
                )
            built += 1
            telemetry.incr("segment.built", tags=tags)
        return uri, index_name, {"built": built}

    with telemetry.timed("segment.build_ms", tags=tags):
        handler: IndexHandler = make_handler(kind, column, index_name, config)
        artifacts: object | None = handler.prepare(dataset, uri, telemetry)
        segment: Any = handler.build_segment(dataset, shard, artifacts)
    telemetry.incr("segment.built", tags=tags)
    return uri, index_name, {"segment": serialize_segment(segment)}


def commit_one_index(
    uri: str,
    spec: dict[str, Any],
    payloads: list[dict[str, Any]],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Commit one dataset's index on an executor (phase C), classifying stale-fragment failures.

    Segment kinds go through the production :func:`commit_segments`. Whether segments are merged
    before publishing is decided by the index kind's handler ``merges()`` (vector and zonemap
    merge, BTREE and BITMAP commit unmerged deltas instead) — running here keeps the merge off the
    driver. FTS rebuilds merge the per-fragment metadata and publish through a single
    ``CreateIndex`` commit that atomically removes the old same-name index, so the old index
    stayed live for the whole build and there is never a window without a committed FTS index. A
    stale-fragment error returns a ``stale`` marker so the fleet re-plans this index in the next
    round.

    Args:
        uri: Dataset URI.
        spec: The index spec from the plan phase.
        payloads: The build payloads collected for this index.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        The index stats dict, or ``{"stale": True, ...}`` when the index must be re-planned.
    """
    kind: str = spec["kind"]
    column: str = spec["column"]
    index_name: str = spec["index_name"]

    try:
        if kind == FTS_KIND:
            built: int = sum(int(payload.get("built", 0)) for payload in payloads)
            commit_fts_index(
                uri,
                column,
                index_name,
                spec["index_uuid"],
                spec["fragments"],
                config,
                telemetry,
            )
            return {"column": column, "index": index_name, "segments": built, "fragments": len(spec["fragments"])}

        documents: list[str] = [payload["segment"] for payload in payloads if "segment" in payload]
        merge: bool = make_handler(kind, column, index_name, config).merges()
        with telemetry.timed("index.commit_ms", tags=[f"index:{index_name}"]):
            committed: int = commit_segments(uri, documents, column, index_name, merge, config, telemetry)
        stats: dict[str, Any] = {
            "column": column,
            "index": index_name,
            "segments": committed,
            "fragments": int(spec.get("fragments", 0)),
        }
        if committed < len(documents):
            stats["stale"] = True
        return stats
    except ValueError as exc:
        if not is_stale_fragment_error(exc):
            raise
        telemetry.incr("index.stale_fragment_replan", tags=[f"index:{index_name}"])
        logger.warning(
            "re-planning %s on %s: a concurrent compaction invalidated the planned fragment set (%s)",
            index_name,
            uri,
            exc,
        )
        return {"column": column, "index": index_name, "segments": 0, "fragments": 0, "stale": True}


def build_shard_task(spec: dict[str, Any], uri: str, version: int, shard: list[int]) -> dict[str, Any]:
    """Build one minimal shard task carrying only the keys :func:`build_one_shard` reads.

    Args:
        spec: The index spec from the plan phase.
        uri: Dataset URI.
        version: The plan-time dataset version.
        shard: The fragment ids this task builds, empty for bootstrap and FTS maintain specs.

    Returns:
        A task dict with ``kind``, ``column``, ``index_name``, ``mode``, ``uri``, ``version``,
        and ``shard``, plus ``index_uuid`` for FTS rebuild specs.
    """
    task: dict[str, Any] = {
        "kind": spec["kind"],
        "column": spec["column"],
        "index_name": spec["index_name"],
        "mode": spec["mode"],
        "uri": uri,
        "version": version,
        "shard": shard,
    }
    if "index_uuid" in spec:
        task["index_uuid"] = spec["index_uuid"]
    return task


def flatten_shard_tasks(
    specs_by_uri: dict[str, list[dict[str, Any]]], version_by_uri: dict[str, int]
) -> list[dict[str, Any]]:
    """Expand every dataset's index specs into the flat build-task list for one Spark job.

    Each task is minimal: only the keys :func:`build_one_shard` actually reads (``kind``,
    ``column``, ``index_name``, ``mode``, ``uri``, ``version``, ``shard``, and ``index_uuid`` for
    FTS rebuilds), not the full spec. This keeps spec-only fields such as an FTS rebuild's entire
    ``fragments`` list out of every one of that index's shard tasks, since the build phase never
    reads them. Specs without shards (vector bootstraps and FTS maintains) become a single task
    with an empty shard.

    Args:
        specs_by_uri: The plan phase's index specs, keyed by dataset URI.
        version_by_uri: The plan-time dataset version, keyed by dataset URI.

    Returns:
        The flattened task specs across the fleet.
    """
    shard_tasks: list[dict[str, Any]] = []
    uri: Any
    specs: Any
    for uri, specs in specs_by_uri.items():
        version: int = version_by_uri[uri]
        spec: Any
        for spec in specs:
            if not spec["shards"]:
                shard_tasks.append(build_shard_task(spec, uri, version, []))
                continue
            shard: Any
            for shard in spec["shards"]:
                shard_tasks.append(build_shard_task(spec, uri, version, list(shard)))
    return shard_tasks


def collect_round_specs(
    plans: list[dict[str, Any]],
    stats_by_uri: dict[str, dict[str, Any]],
    kind_by_index: dict[tuple[str, str], str],
) -> dict[str, list[dict[str, Any]]]:
    """Fold one round's plan records into the fleet accumulators and collect buildable specs.

    A skipped dataset records its skip reason, finished indexes append their stats, and each
    dataset with buildable specs registers every spec's kind and pins its plan-time version.
    A dataset whose plan fan-out failed carries an ``{"error", "phase"}`` marker instead of specs
    (the plan fan-out uses ``phase="plan"``): its error is recorded on the dataset and it builds
    nothing this run, isolating the failure from the rest of the fleet. On a stale-replan round
    the re-plan reports every already-current index in ``done`` again, so entries whose index name
    is already recorded for the dataset are not appended twice — the result stats stay one entry
    per index per run.

    Args:
        plans: The per-dataset plan records from the plan fan-out.
        stats_by_uri: Per-dataset result records, mutated in place.
        kind_by_index: Index kinds keyed by ``(uri, index_name)``, mutated in place.

    Returns:
        The buildable index specs, keyed by dataset URI.
    """
    specs_by_uri: dict[str, list[dict[str, Any]]] = {}
    plan: Any
    for plan in plans:
        uri: str = plan["uri"]
        if "error" in plan:
            stats_by_uri[uri]["error"] = plan["error"]
            stats_by_uri[uri]["error_phase"] = plan.get("phase", "plan")
            continue
        if "skipped" in plan:
            stats_by_uri[uri]["skipped"] = plan["skipped"]
            continue
        recorded: set[str] = {entry.get("index", "") for entry in stats_by_uri[uri]["indexes"]}
        stats_by_uri[uri]["indexes"].extend(
            entry for entry in plan.get("done", []) if entry.get("index") not in recorded
        )
        if plan["specs"]:
            specs_by_uri[uri] = plan["specs"]
            spec: Any
            for spec in plan["specs"]:
                kind_by_index[(uri, spec["index_name"])] = spec["kind"]
            stats_by_uri[uri]["version"] = plan["version"]
    return specs_by_uri


def fold_build_payloads(
    built: list[tuple[str, str, dict[str, Any]]],
    stats_by_uri: dict[str, dict[str, Any]],
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], set[tuple[str, str]]]:
    """Fold the flat build job's payloads into terminal stats and pending commit payloads.

    A finished ``stats`` payload (vector bootstrap or FTS maintain) is appended as a terminal
    index result. An ``error`` payload records one per-index error entry (deduplicated per index)
    and marks the ``(uri, index_name)`` errored so the caller excludes it from the commit phase,
    because a failed build must never publish a partial index. Every other payload is a segment or
    FTS-shard build gathered for its index's commit.

    Args:
        built: The ``(uri, index_name, payload)`` triples collected from the build job.
        stats_by_uri: Per-dataset result records, mutated in place.

    Returns:
        The build payloads keyed by ``(uri, index_name)`` for the commit phase, and the set of
        ``(uri, index_name)`` pairs whose build failed and must be excluded from commit.
    """
    payloads_by_index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    errored_indexes: set[tuple[str, str]] = set()
    uri: Any
    index_name: Any
    payload: Any
    for uri, index_name, payload in built:
        build_key: tuple[str, str] = (uri, index_name)
        if "error" in payload:
            if build_key not in errored_indexes:
                stats_by_uri[uri]["indexes"].append(
                    {
                        "column": payload.get("column", ""),
                        "index": index_name,
                        "error": payload["error"],
                        "phase": payload.get("phase", "build"),
                    }
                )
                errored_indexes.add(build_key)
            continue
        if "stats" in payload:
            stats_by_uri[uri]["indexes"].append(payload["stats"])
            continue
        payloads_by_index.setdefault(build_key, []).append(payload)
    return payloads_by_index, errored_indexes


def record_commit_outcomes(
    outcomes: list[tuple[str, dict[str, Any]]],
    stats_by_uri: dict[str, dict[str, Any]],
) -> set[str]:
    """Fold the commit fan-out's outcomes into per-dataset stats, returning the stale URIs.

    A stale outcome routes its dataset back for a re-plan. An error outcome appends one per-index
    error entry (terminal, never re-planned). A success outcome appends the committed stats
    directly: segment-mode vector commits no longer carry the ``reused_artifacts`` or
    ``num_partitions`` extras (each shard resolves its own artifacts now, so there is no fleet
    artifact phase to source them), while a vector bootstrap's stats still carry ``num_partitions``
    and ``reused_artifacts=False`` from :func:`bootstrap_vector_index`.

    Args:
        outcomes: The ``(uri, stats)`` pairs collected from the commit fan-out.
        stats_by_uri: Per-dataset result records, mutated in place.

    Returns:
        The datasets whose commit hit stale fragments, for the next stale-replan round.
    """
    stale_uris: set[str] = set()
    uri: Any
    stats: Any
    for uri, stats in outcomes:
        if stats.pop("stale", False):
            stale_uris.add(uri)
            continue
        if "error" in stats:
            stats_by_uri[uri]["indexes"].append(
                {
                    "column": stats.get("column", ""),
                    "index": stats.get("index", ""),
                    "error": stats["error"],
                    "phase": stats.get("phase", "index_commit"),
                }
            )
            continue
        stats_by_uri[uri]["indexes"].append(stats)
    return stale_uris


class LanceIndexer:
    """Builds the configured indices over a Lance fleet with unified task-based phases."""

    def __init__(self, config: IndexJobConfig) -> None:
        """Initialize the indexer.

        Args:
            config: Indexing configuration.
        """
        self.config: IndexJobConfig = config

    def build_fleet_segments(
        self,
        spark: SparkSession,
        shard_tasks: list[dict[str, Any]],
    ) -> list[tuple[str, str, dict[str, Any]]]:
        """Run every dataset's build tasks in one flat Spark job.

        Each vector segment shard resolves its own dataset's IVF_RQ artifacts on the executor
        (centroids sidecar-first, ``get_ivf_model`` fallback, rotation from the stored config), so
        no fleet-wide artifact dict is collected to the driver or broadcast to the tasks (ADR 0040).

        Args:
            spark: Active Spark session.
            shard_tasks: Flattened shard task specs across the fleet.

        Returns:
            ``(uri, index_name, payload)`` triples collected from the executors.
        """
        config: IndexJobConfig = self.config
        if not shard_tasks:
            return []

        def build_partition(items: Any) -> Any:
            """Build the shard tasks assigned to this executor task.

            Per-index failure isolation: a shard build failure is caught and turned into an error
            payload carrying the shard's ``(uri, index_name)`` so the driver drops the whole index
            from the commit phase (a failed build must never publish a partial index) and records
            the error on its dataset, without aborting the rest of the fleet's build tasks. A vector
            shard whose artifacts cannot be resolved raises inside ``build_one_shard`` and is
            isolated on exactly this path, replacing the deleted fleet artifact phase.

            Args:
                items: The task specs for this partition.

            Yields:
                One ``(uri, index_name, payload)`` triple per task, an error payload when the
                shard build raised.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            with executor_telemetry.span("lance.indexing.build_segment"):
                task: Any
                for task in items:
                    try:
                        yield build_one_shard(task, config, executor_telemetry)
                    except Exception as exc:
                        executor_telemetry.incr("segment.build_error", tags=[f"index:{task['index_name']}"])
                        logger.warning(
                            "index build shard failed for %s on %s, isolating: %s",
                            task["index_name"],
                            task["uri"],
                            exc,
                        )
                        yield (
                            task["uri"],
                            task["index_name"],
                            {"error": str(exc), "phase": "build", "column": task["column"]},
                        )

        max_build_tasks_resolved: int = derive_partitions(spark, BUILD_PARTITION_FACTOR)
        slices: int = max(1, min(len(shard_tasks), max_build_tasks_resolved))
        return spark.sparkContext.parallelize(shard_tasks, slices).mapPartitions(build_partition).collect()

    def commit_fleet(
        self, spark: SparkSession, entries: list[tuple[str, dict[str, Any], list[dict[str, Any]]]]
    ) -> list[tuple[str, dict[str, Any]]]:
        """Commit every built index in a per-(dataset, index) executor fan-out.

        Args:
            spark: Active Spark session.
            entries: ``(uri, spec, payloads)`` triples for the fleet's built indexes.

        Returns:
            ``(uri, stats)`` pairs, stale-marked where a re-plan is needed.
        """
        config: IndexJobConfig = self.config
        if not entries:
            return []

        def commit_partition(items: Any) -> Any:
            """Commit the indexes assigned to this executor task.

            Per-index failure isolation: ``commit_one_index`` returns its own stale marker and
            only raises on a genuine non-stale error, so any exception reaching here is caught and
            turned into an error marker identifying its ``(uri, column, index)`` instead of
            aborting the fan-out.

            Args:
                items: The ``(uri, spec, payloads)`` triples for this partition.

            Yields:
                One ``(uri, stats)`` pair per index, an error marker when the commit raised.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            uri: Any
            spec: Any
            payloads: Any
            for uri, spec, payloads in items:
                try:
                    yield uri, commit_one_index(uri, spec, payloads, config, executor_telemetry)
                except Exception as exc:
                    executor_telemetry.incr("index.commit_error", tags=[f"index:{spec['index_name']}"])
                    logger.warning("index commit failed for %s on %s, isolating: %s", spec["index_name"], uri, exc)
                    yield (
                        uri,
                        {
                            "uri": uri,
                            "column": spec["column"],
                            "index": spec["index_name"],
                            "error": str(exc),
                            "phase": "index_commit",
                            "stale": False,
                        },
                    )

        batch_partitions_resolved: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        slices: int = max(1, min(len(entries), batch_partitions_resolved))
        return spark.sparkContext.parallelize(entries, slices).mapPartitions(commit_partition).collect()

    def bound_fleet_deltas(self, spark: SparkSession, entries: list[tuple[str, str]]) -> dict[tuple[str, str], bool]:
        """Bound accumulated index deltas across the fleet in one fan-out.

        Args:
            spark: Active Spark session.
            entries: ``(uri, index_name)`` pairs for indexes that committed segments this run.

        Returns:
            Whether a delta merge ran, keyed by ``(uri, index_name)``.
        """
        config: IndexJobConfig = self.config
        if not entries:
            return {}

        def merge_partition(items: Any) -> Any:
            """Bound the deltas of the indexes assigned to this executor task.

            Delta bounding is best-effort: a failure for one index is logged and reported as an
            unmerged result so the deltas are simply left for the next run instead of aborting the
            fleet's delta-bound pass.

            Args:
                items: The ``(uri, index_name)`` pairs for this partition.

            Yields:
                ``(uri, index_name, merged)`` per index, ``merged=False`` when the bound raised.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            uri: Any
            index_name: Any
            for uri, index_name in items:
                try:
                    yield uri, index_name, merge_index_deltas(uri, index_name, config, executor_telemetry)
                except Exception as exc:
                    logger.warning(
                        "index delta bound failed for %s on %s, leaving deltas for next run: %s",
                        index_name,
                        uri,
                        exc,
                    )
                    yield uri, index_name, False

        batch_partitions_resolved: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        slices: int = max(1, min(len(entries), batch_partitions_resolved))
        merged: list[tuple[str, str, bool]] = (
            spark.sparkContext.parallelize(entries, slices).mapPartitions(merge_partition).collect()
        )
        return {(uri, index_name): flag for uri, index_name, flag in merged}

    def run_round(
        self,
        spark: SparkSession,
        round_index: int,
        pending_uris: list[str],
        stats_by_uri: dict[str, dict[str, Any]],
        kind_by_index: dict[tuple[str, str], str],
        driver_telemetry: Telemetry,
    ) -> list[str]:
        """Run one plan-build-commit round over the pending datasets.

        The plan fan-out resolves each dataset's index specs (folded into the accumulators by
        :func:`collect_round_specs`), one flat Spark job builds every shard task (each vector shard
        resolving its own artifacts sidecar-first, ADR 0040), and the commit fan-out publishes per
        index. Finished index stats accumulate into ``stats_by_uri`` and every spec's kind is
        recorded in ``kind_by_index`` for the final delta bound.

        Per-index failure isolation: a plan, build, or commit failure is recorded as an error entry
        on the affected dataset's ``indexes`` list (or a dataset-level error for a plan failure) and
        the affected index is dropped from the rest of the round, so a failed build never publishes
        a partial index and one pathological index never aborts the fleet round. A vector index
        whose artifacts cannot be resolved now fails inside its build shard and is isolated on that
        path. The other indexes and datasets still build and commit.

        Args:
            spark: Active Spark session.
            round_index: Zero-based round number, for logging.
            pending_uris: Datasets to plan and build this round.
            stats_by_uri: Per-dataset result records, mutated in place.
            kind_by_index: Index kinds keyed by ``(uri, index_name)``, mutated in place.
            driver_telemetry: The driver's telemetry facade.

        Returns:
            The datasets whose commits hit stale fragments, sorted, for the next round.
        """
        config: IndexJobConfig = self.config
        plan_batch_partitions: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        plans: list[dict[str, Any]] = fan_out_per_dataset(
            spark,
            pending_uris,
            config.telemetry,
            lambda uri, telemetry: plan_dataset_indexes(uri, config, telemetry),
            plan_batch_partitions,
            phase="plan",
        )

        specs_by_uri: dict[str, list[dict[str, Any]]] = collect_round_specs(plans, stats_by_uri, kind_by_index)
        if not specs_by_uri:
            return []

        version_by_uri: dict[str, int] = {uri: stats_by_uri[uri]["version"] for uri in specs_by_uri}
        shard_tasks: list[dict[str, Any]] = flatten_shard_tasks(specs_by_uri, version_by_uri)
        logger.info(
            "indexing round %d/%d: %d datasets, %d build tasks",
            round_index + 1,
            config.max_stale_replans,
            len(specs_by_uri),
            len(shard_tasks),
        )
        with driver_telemetry.timed("run.build_ms"):
            built: list[tuple[str, str, dict[str, Any]]] = self.build_fleet_segments(spark, shard_tasks)

        payloads_by_index: Any
        errored_indexes: Any
        payloads_by_index, errored_indexes = fold_build_payloads(built, stats_by_uri)

        commit_entries: list[tuple[str, dict[str, Any], list[dict[str, Any]]]] = []
        uri: Any
        specs: Any
        for uri, specs in specs_by_uri.items():
            spec: Any
            for spec in specs:
                key: tuple[str, str] = (uri, spec["index_name"])
                if key not in errored_indexes and key in payloads_by_index:
                    commit_entries.append((uri, spec, payloads_by_index[key]))
        with driver_telemetry.timed("run.commit_ms"):
            outcomes: list[tuple[str, dict[str, Any]]] = self.commit_fleet(spark, commit_entries)

        return sorted(record_commit_outcomes(outcomes, stats_by_uri))

    def run(self, spark: SparkSession, dataset_uris: list[str]) -> list[dict[str, Any]]:
        """Index every dataset through the unified plan-build-commit rounds.

        Per-dataset failure isolation: a dataset whose plan failed carries a dataset-level
        ``"error"`` key, and a dataset with a failed index carries an error entry in its
        ``indexes`` list. Either way the dataset still lands one terminal record and every other
        dataset completes. Callers detect failures by scanning for a dataset-level ``"error"`` or
        a per-index ``"error"`` entry. A dataset still stale after every
        :data:`~lance_etl.indexing.config.MAX_STALE_REPLANS` round is folded into the same
        dataset-level ``"error"`` shape (``error_phase="index-stale-exhausted"``) rather than
        silently deferred, so it counts toward the failed-dataset total the caller reports through
        :func:`~lance_etl.fanout.count_failed` and the CLI's ``EXIT_PARTIAL_FAILURE`` exit code.
        Every other failed dataset is re-planned by the next scheduled run, since the job is
        cursor-free.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per dataset, in input order. A failed dataset carries an
            ``"error"`` key or an error entry among its ``indexes``.
        """
        config: IndexJobConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.indexing.run") as run_span:
            run_span.set_tag("dataset_count", len(dataset_uris))
            if not dataset_uris:
                return []

            stats_by_uri: dict[str, dict[str, Any]] = {uri: {"uri": uri, "indexes": []} for uri in dataset_uris}
            pending_uris: list[str] = list(dataset_uris)
            kind_by_index: dict[tuple[str, str], str] = {}

            round_index: Any
            for round_index in range(config.max_stale_replans):
                pending_uris = self.run_round(
                    spark, round_index, pending_uris, stats_by_uri, kind_by_index, driver_telemetry
                )
                if not pending_uris:
                    break

            uri: Any
            for uri in pending_uris:
                logger.warning(
                    "index build on %s still has uncovered fragments after %d stale-replan rounds; "
                    "marking the dataset failed so this run's exit code and metrics reflect it",
                    uri,
                    config.max_stale_replans,
                )
                stats_by_uri[uri]["error"] = (
                    f"stale-replan exhausted after {config.max_stale_replans} rounds: a concurrent compaction kept "
                    "invalidating the planned fragment set before every index could commit"
                )
                stats_by_uri[uri]["error_phase"] = STALE_REPLAN_EXHAUSTED_PHASE
                driver_telemetry.incr(STALE_REPLANS_EXHAUSTED_METRIC)

            delta_entries: list[tuple[str, str]] = sorted(
                {
                    (uri, item["index"])
                    for uri, stats in stats_by_uri.items()
                    for item in stats["indexes"]
                    if (
                        (int(item.get("segments", 0)) > 0 and kind_by_index.get((uri, item["index"])) != FTS_KIND)
                        or bool(item.get("needs_delta_merge", False))
                    )
                }
            )
            merged_flags: dict[tuple[str, str], bool] = self.bound_fleet_deltas(spark, delta_entries)
            stats: Any
            for uri, stats in stats_by_uri.items():
                item: Any
                for item in stats["indexes"]:
                    key: Any = (uri, item["index"])
                    if key in merged_flags:
                        item["deltas_merged"] = merged_flags[key]
                    item.pop("needs_delta_merge", None)
                stats.pop("version", None)

            results: list[dict[str, Any]] = [stats_by_uri[uri] for uri in dataset_uris]
            skipped: int = sum(1 for stats in results if stats.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)

            report_fleet_failures(results, run_span, driver_telemetry, "indexing run", index_failure_phase, logger)

            logger.info("indexing run: %d datasets (%d skipped)", len(results), skipped)
            return results
