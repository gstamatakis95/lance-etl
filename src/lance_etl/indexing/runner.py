"""LanceIndexer: unified fleet orchestration for index builds.

Every dataset, regardless of size, follows the same phases built on the same Lance segment
APIs, and a small dataset is simply the one-shard case:

- Plan (:func:`plan_dataset_indexes`): a per-dataset executor fan-out resolves which indexes to
  build (explicit config columns, or the ``lance-etl.columns`` role metadata written by the ETL
  sink), runs the derived-state skip check, and emits bounded dataset/version/count seeds. Build
  executors enumerate exact fragment shards sized by ``fragments_per_index_task``. A vector index
  whose artifacts are absent, mismatched, or growth-stale plans one ``bootstrap`` task instead of
  shards (ADR 0030).
- Build and commit (:meth:`LanceIndexer.build_and_commit_fleet`): ONE flat Spark job over every
  dataset's bounded index seeds. Each vector segment shard resolves its OWN dataset's IVF_RQ
  artifacts on the executor, with a bounded worker-local cache keyed by exact dataset version and
  index. Shard results reduce by dataset and index directly to commit executors, so serialized
  segment metadata never crosses the driver. Vector and scalar shards build uncommitted segments.
  A vector bootstrap task runs a committed ``create_index`` whose internal streaming k-means
  trains the centroids and caches them to the sidecar. FTS rebuild shards build per-fragment
  inverted indices under their dataset's shared index id. The per-index reducer publishes through
  :func:`commit_one_index`, keeping the heavy merge off the driver. A stale-fragment commit marks
  the index for the next replan round instead of failing the run.
- Delta bound (:func:`~lance_etl.indexing.optimize.merge_index_deltas`): a final per-(dataset, index) fan-out merges
  accumulated index deltas once they exceed ``max_index_deltas``.

:meth:`LanceIndexer.run` repeats plan-build-commit for stale indexes up to
:data:`~lance_etl.indexing.config.MAX_STALE_REPLANS` rounds. A dataset still stale after every
round is recorded as a failed dataset (:data:`STALE_REPLAN_EXHAUSTED_PHASE`) rather than deferred
silently, so the fleet's failed-dataset count, the ``index.stale_replans_exhausted`` metric, and the
CLI exit code all reflect the partially indexed dataset instead of a clean run masking it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
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
    all_fragment_ids,
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

VECTOR_ARTIFACT_CACHE: dict[tuple[str, int, str, str, str, int, int, str], object] = {}
"""The one worker-local IVF_RQ artifact generation retained between Spark tasks."""


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
        validate_unique_index_targets(targets)
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
    validate_unique_index_targets(discovered)
    return discovered


def validate_unique_index_targets(targets: list[tuple[str, str, str]]) -> None:
    """Reject duplicate index names before they collapse in the build-result maps.

    The fleet runner groups shard payloads by ``(dataset_uri, index_name)``. If two configured
    targets share a name, their payloads become indistinguishable and can be handed to the wrong
    commit recipe. Exact duplicate column entries have the same failure mode because they schedule
    the same build twice. Rejecting the plan before any shard runs keeps the error isolated to the
    dataset and prevents partial or cross-type publication.

    Args:
        targets: Resolved ``(kind, column, index_name)`` triples for one dataset.

    Raises:
        ValueError: If more than one target resolves to the same index name.
    """
    target_by_name: dict[str, tuple[str, str]] = {}
    for kind, column, index_name in targets:
        previous: tuple[str, str] | None = target_by_name.get(index_name)
        if previous is not None:
            raise ValueError(
                f"index name {index_name!r} is configured more than once: "
                f"{previous[0]} on {previous[1]!r} and {kind} on {column!r}"
            )
        target_by_name[index_name] = (kind, column)


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


def dataset_fragment_count(dataset: lance.LanceDataset) -> int:
    """Read the bounded fragment count without materializing fragment IDs.

    Args:
        dataset: Version-pinned dataset being planned.

    Returns:
        The exact live fragment count reported by Lance dataset statistics.
    """
    return int(dataset.stats.dataset_stats()["num_fragments"])


def target_fragment_count(
    dataset: lance.LanceDataset,
    handler: IndexHandler,
) -> int:
    """Return the target inventory size without sending fragment IDs to the driver.

    Args:
        dataset: Version-pinned dataset being planned.
        handler: Type-specific fragment coverage policy.

    Returns:
        Exact target count computed on the plan executor.
    """
    return len(handler.target_fragments(dataset))


def segment_plan_spec(
    dataset: lance.LanceDataset,
    kind: str,
    column: str,
    index_name: str,
    fragments: int,
    config: IndexJobConfig,
) -> dict[str, Any]:
    """Build one bounded segment-mode plan spec.

    Args:
        dataset: Exact version-pinned dataset.
        kind: Index kind.
        column: Indexed column.
        index_name: Published index name.
        fragments: Exact target fragment count.
        config: Indexing policy controlling shard width.

    Returns:
        Constant-size build seed fields, including vector artifact identity when required.
    """
    spec: dict[str, Any] = {
        "kind": kind,
        "column": column,
        "index_name": index_name,
        "mode": "segments",
        "fragments": fragments,
        "shard_count": shard_count(fragments, config),
    }
    if kind == VECTOR_KIND:
        artifact_partitions: int
        artifact_generation: str
        artifact_partitions, artifact_generation = vector_artifact_generation(dataset, column, index_name)
        spec["artifact_num_partitions"] = artifact_partitions
        spec["artifact_generation"] = artifact_generation
    return spec


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
    targets, applies the fleet-level and per-index skip checks, and returns only bounded
    dataset/version/count seeds. Fragment IDs are enumerated later on build executors. A vector
    index whose artifacts are absent, mismatched, or growth-stale (or a ``rebuild`` run) plans one
    ``bootstrap`` task: a committed ``create_index`` whose internal streaming k-means trains the
    centroids (ADR 0030). A vector index with reusable artifacts plans incremental ``segments``
    shards as usual.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A dict with ``uri`` and either ``error``, ``skipped``, or ``version`` plus per-index
        ``specs``. Each spec carries ``kind``, ``column``, ``index_name``, ``mode``, bounded
        fragment and shard counts, and the FTS rebuild extra ``index_uuid``. Indexes with nothing
        to do land in ``done`` as finished stats.
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
                    {
                        "kind": kind,
                        "column": column,
                        "index_name": index_name,
                        "mode": "maintain",
                        "fragments": 0,
                        "shard_count": 1,
                    }
                )
                continue
            fragments: int = dataset_fragment_count(dataset)
            if fragments == 0:
                done.append({"column": column, "index": index_name, "segments": 0, "fragments": 0})
                continue
            specs.append(
                {
                    "kind": kind,
                    "column": column,
                    "index_name": index_name,
                    "mode": "rebuild",
                    "fragments": fragments,
                    "shard_count": shard_count(fragments, config),
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
                        "fragments": dataset_fragment_count(dataset),
                        "shard_count": 1,
                    }
                )
                continue
        fragments = target_fragment_count(dataset, handler)
        if fragments == 0:
            stats: dict[str, Any] = {"column": column, "index": index_name, "segments": 0, "fragments": 0}
            if index_name in existing_names and index_delta_count(dataset, index_name) > config.max_index_deltas:
                stats["needs_delta_merge"] = True
            done.append(stats)
            continue
        specs.append(segment_plan_spec(dataset, kind, column, index_name, fragments, config))

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
    rabitq_model: str = native_indices.build_rq_model(dimension=dimension, num_bits=config.num_bits)

    def action() -> tuple[lance.LanceDataset, int, int]:
        """Re-open, derive training parameters from the latest rows, and build the index."""
        fresh: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        fresh_rows: int = fresh.count_rows()
        planned: int = derive_num_partitions(
            fresh_rows,
            config.num_partitions,
            config.minimum_partitions,
            config.maximum_partitions,
            config.target_rows_per_partition,
        )
        partitions: int = degrade_num_partitions(planned, fresh_rows, config.streaming_sample_rate)
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
        return fresh, fresh_rows, partitions

    committed_dataset: lance.LanceDataset
    rows_at_train: int
    partitions: int
    committed_dataset, rows_at_train, partitions = commit_index_with_retries(
        action, config, telemetry, tags=[f"index:{index_name}"]
    )
    telemetry.incr("index.committed", tags=[f"index:{index_name}"])
    telemetry.incr("artifacts.trained")
    write_vector_config(
        uri,
        column,
        {
            "rows_at_train": rows_at_train,
            "dimension": dimension,
            "metric": config.metric,
            "num_bits": config.num_bits,
            "num_partitions": partitions,
            "rabitq_model": rabitq_model,
        },
        config,
        telemetry,
    )
    persist_bootstrap_centroids(committed_dataset, uri, index_name, rows_at_train, config, telemetry)
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


def vector_artifact_generation(dataset: lance.LanceDataset, column: str, index_name: str) -> tuple[int, str]:
    """Fingerprint the stored IVF_RQ policy and committed segment generation.

    Args:
        dataset: Exact version-pinned dataset.
        column: Indexed vector column.
        index_name: Published vector index name.

    Returns:
        Stored partition count and a SHA-256 generation digest. A missing config yields a zero
        partition count and still produces a distinct digest before normal handler validation
        raises.
    """
    cfg: dict[str, Any] | None = load_vector_config(dataset, column)
    segment_uuids: list[str] = sorted(
        str(segment.uuid)
        for description in dataset.describe_indices()
        if description.name == index_name and column in description.field_names
        for segment in description.segments
    )
    encoded: str = json.dumps(
        {"config": cfg, "segments": segment_uuids},
        sort_keys=True,
        separators=(",", ":"),
    )
    partitions: int = int((cfg or {}).get("num_partitions") or 0)
    return partitions, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def vector_artifact_cache_key(
    dataset: lance.LanceDataset,
    task: dict[str, Any],
    config: IndexJobConfig,
) -> tuple[str, int, str, str, str, int, int, str]:
    """Build the exact worker-cache key for one vector index generation.

    Args:
        dataset: Exact version-pinned dataset.
        task: Version-pinned vector shard task.
        config: Indexing policy controlling artifact compatibility.

    Returns:
        Dataset URI, immutable Lance version, column, index name, metric, RaBitQ bit count,
        partition count, and committed artifact-generation digest.
    """
    partitions: int = int(task.get("artifact_num_partitions") or 0)
    generation: str = str(task.get("artifact_generation") or "")
    if not generation:
        partitions, generation = vector_artifact_generation(
            dataset,
            str(task["column"]),
            str(task["index_name"]),
        )
    return (
        str(task["uri"]),
        int(task["version"]),
        str(task["column"]),
        str(task["index_name"]),
        config.metric.lower(),
        config.num_bits,
        partitions,
        generation,
    )


def prepare_vector_artifacts(
    handler: IndexHandler,
    dataset: lance.LanceDataset,
    task: dict[str, Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> object:
    """Resolve IVF_RQ artifacts once per exact generation in a reusable Python worker.

    The single-entry LRU prevents a long-lived Spark worker from retaining an unbounded fleet of
    centroid arrays. Lance version, policy, partition count, stored config, and committed segment
    UUIDs form the key, so an artifact cannot leak into a recreated URI or another shard generation.

    Args:
        handler: Vector index handler owning artifact validation and loading.
        dataset: Exact version-pinned dataset handle.
        task: Vector shard task carrying generation identity.
        config: Indexing policy.
        telemetry: Executor telemetry facade.

    Returns:
        Reusable artifact tuple accepted by the vector segment builder.
    """
    key: tuple[str, int, str, str, str, int, int, str] = vector_artifact_cache_key(dataset, task, config)
    if key in VECTOR_ARTIFACT_CACHE:
        telemetry.incr("artifacts.worker_cache_hit")
        return VECTOR_ARTIFACT_CACHE[key]
    prepared: object | None = handler.prepare(dataset, str(task["uri"]), telemetry)
    if prepared is None:
        raise RuntimeError("vector index handler returned no reusable artifacts")
    VECTOR_ARTIFACT_CACHE.clear()
    VECTOR_ARTIFACT_CACHE[key] = prepared
    return prepared


def build_one_shard(
    task: dict[str, Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> tuple[str, str, dict[str, Any]]:
    """Build one flat-job task on an executor: a segment shard, FTS fragment shard, or FTS maintain.

    A non-bootstrap, non-FTS shard dispatches through :func:`make_handler`. Vector shards resolve
    their own dataset's IVF_RQ artifacts sidecar-first and retain one exact generation in the
    Spark Python worker through :func:`prepare_vector_artifacts`. There is no fleet-wide
    artifact broadcast.

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

    if task.get("inventory_error"):
        raise RuntimeError(str(task["inventory_error"]))

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
        with telemetry.timed("segment.build_ms", tags=tags):
            dataset.create_scalar_index(
                column=column,
                index_type="INVERTED",
                name=index_name,
                replace=True,
                index_uuid=task["index_uuid"],
                fragment_ids=shard,
                **config.fts_params(),
            )
        telemetry.incr("segment.built", tags=tags)
        return uri, index_name, {"built": len(shard)}

    with telemetry.timed("segment.build_ms", tags=tags):
        handler: IndexHandler = make_handler(kind, column, index_name, config)
        artifacts: object | None = (
            prepare_vector_artifacts(handler, dataset, task, config, telemetry)
            if kind == VECTOR_KIND
            else handler.prepare(dataset, uri, telemetry)
        )
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
            expected_fragments: int = int(spec["fragments"])
            if built != expected_fragments:
                raise RuntimeError(
                    f"inverted index {index_name} built {built} fragments but planned {expected_fragments}"
                )
            pinned: lance.LanceDataset = lance.dataset(
                uri,
                version=int(spec["read_version"]),
                storage_options=config.storage_options,
            )
            fragment_ids: list[int] = all_fragment_ids(pinned)
            if len(fragment_ids) != expected_fragments:
                raise RuntimeError(
                    f"fragment inventory changed for {uri}: planned {expected_fragments}, "
                    f"found {len(fragment_ids)} at pinned version"
                )
            commit_fts_index(
                uri,
                column,
                index_name,
                spec["index_uuid"],
                fragment_ids,
                config,
                telemetry,
            )
            return {
                "column": column,
                "index": index_name,
                "segments": built,
                "fragments": expected_fragments,
            }

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
        ``shard``, and the bounded total fragment count, plus ``index_uuid`` for FTS rebuild specs
        or bounded artifact-generation identity for vector segment specs.
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
    if "artifact_generation" in spec:
        task["artifact_generation"] = spec["artifact_generation"]
        task["artifact_num_partitions"] = int(spec["artifact_num_partitions"])
    if "fragments" in spec:
        task["fragments"] = int(spec["fragments"])
    return task


def build_shard_seed(spec: dict[str, Any], uri: str, version: int) -> dict[str, Any]:
    """Build one bounded index seed for executor-side fragment enumeration.

    Args:
        spec: Bounded plan spec carrying fragment and shard counts.
        uri: Dataset URI.
        version: Exact plan-time dataset version.

    Returns:
        A constant-size seed without fragment IDs.
    """
    seed: dict[str, Any] = build_shard_task(spec, uri, version, [])
    seed["fragments"] = int(spec["fragments"])
    seed["shard_count"] = int(spec["shard_count"])
    return seed


def enumerate_shard_tasks(seed: dict[str, Any], config: IndexJobConfig) -> Iterator[dict[str, Any]]:
    """Expand one bounded seed into exact fragment shards on an executor.

    Args:
        seed: Dataset, version, count, and index seed without fragment IDs.
        config: Indexing configuration used to reproduce handler coverage policy.

    Yields:
        Exact build tasks, or one error-bearing task when the pinned inventory cannot be
        reproduced.
    """
    mode: str = str(seed["mode"])
    if mode in ("bootstrap", "maintain"):
        yield build_shard_task(seed, str(seed["uri"]), int(seed["version"]), [])
        return
    try:
        dataset: lance.LanceDataset = lance.dataset(
            str(seed["uri"]),
            version=int(seed["version"]),
            storage_options=config.storage_options,
        )
        if seed["kind"] == VECTOR_KIND and seed.get("artifact_generation"):
            partitions: int
            generation: str
            partitions, generation = vector_artifact_generation(
                dataset,
                str(seed["column"]),
                str(seed["index_name"]),
            )
            if generation != str(seed["artifact_generation"]) or partitions != int(seed["artifact_num_partitions"]):
                raise RuntimeError(f"vector artifact generation changed for {seed['index_name']} on {seed['uri']}")
        fragment_ids: list[int]
        if seed["kind"] == FTS_KIND:
            fragment_ids = all_fragment_ids(dataset)
        else:
            handler: IndexHandler = make_handler(
                str(seed["kind"]),
                str(seed["column"]),
                str(seed["index_name"]),
                config,
            )
            fragment_ids = handler.target_fragments(dataset)
        expected: int = int(seed["fragments"])
        if len(fragment_ids) != expected:
            raise RuntimeError(
                f"fragment inventory changed for {seed['uri']}: planned {expected}, found {len(fragment_ids)}"
            )
        shards: list[list[int]] = split_evenly(fragment_ids, int(seed["shard_count"]))
    except Exception as exc:
        failed: dict[str, Any] = build_shard_task(seed, str(seed["uri"]), int(seed["version"]), [])
        failed["inventory_error"] = str(exc)
        yield failed
        return
    for shard in shards:
        yield build_shard_task(seed, str(seed["uri"]), int(seed["version"]), shard)


def flatten_shard_tasks(
    specs_by_uri: dict[str, list[dict[str, Any]]], version_by_uri: dict[str, int]
) -> list[dict[str, Any]]:
    """Reduce every dataset's index specs to bounded build seeds for one Spark job.

    One constant-size seed crosses the driver per index. Exact fragment IDs are enumerated from
    the pinned dataset version inside :func:`enumerate_shard_tasks` after Spark receives the seeds.

    Args:
        specs_by_uri: The plan phase's index specs, keyed by dataset URI.
        version_by_uri: The plan-time dataset version, keyed by dataset URI.

    Returns:
        One bounded seed per index across the fleet.
    """
    shard_seeds: list[dict[str, Any]] = []
    uri: Any
    specs: Any
    for uri, specs in specs_by_uri.items():
        version: int = version_by_uri[uri]
        spec: Any
        for spec in specs:
            shard_seeds.append(build_shard_seed(spec, uri, version))
    return shard_seeds


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
                spec["read_version"] = int(plan["version"])
                kind_by_index[(uri, spec["index_name"])] = spec["kind"]
            stats_by_uri[uri]["version"] = plan["version"]
    return specs_by_uri


def commit_spec_from_task(task: dict[str, Any]) -> dict[str, Any]:
    """Recover the bounded commit specification carried by one build task.

    Args:
        task: Exact-version build task emitted by :func:`enumerate_shard_tasks`.

    Returns:
        The fields :func:`commit_one_index` needs, without the task's fragment shard.
    """
    spec: dict[str, Any] = {
        "kind": task["kind"],
        "column": task["column"],
        "index_name": task["index_name"],
        "fragments": int(task["fragments"]),
        "read_version": int(task["version"]),
    }
    if "index_uuid" in task:
        spec["index_uuid"] = task["index_uuid"]
    return spec


def build_aggregate(task: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Wrap one build result in a compact per-index reduction accumulator.

    Args:
        task: Build task that produced the payload.
        payload: Segment, FTS count, terminal stats, or isolated build error.

    Returns:
        A reduction accumulator retaining segment metadata only on Spark executors.
    """
    aggregate: dict[str, Any] = {
        "spec": commit_spec_from_task(task),
        "segments": [],
        "built": 0,
    }
    if "error" in payload:
        aggregate["error"] = payload
    elif "stats" in payload:
        aggregate["stats"] = payload["stats"]
    elif "segment" in payload:
        aggregate["segments"].append(payload["segment"])
    elif "built" in payload:
        aggregate["built"] = int(payload["built"])
    else:
        aggregate["error"] = {
            "column": task["column"],
            "error": "index build returned an unsupported payload",
            "phase": "build",
        }
    return aggregate


def invalidate_build_aggregate(aggregate: dict[str, Any], error: dict[str, Any]) -> dict[str, Any]:
    """Turn a reduction accumulator into a terminal build error without retaining payloads.

    Args:
        aggregate: Per-index build accumulator to invalidate.
        error: Error payload to retain for the terminal outcome.

    Returns:
        The invalidated accumulator.
    """
    aggregate["error"] = error
    aggregate.pop("stats", None)
    aggregate["segments"] = []
    aggregate["built"] = 0
    return aggregate


def merge_build_aggregates(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Reduce two build accumulators without sending their segment payloads through the driver.

    Args:
        left: First per-index accumulator.
        right: Second per-index accumulator.

    Returns:
        A merged accumulator. Any shard error wins and discards successful shard metadata so a
        partial index can never commit.
    """
    if left["spec"] != right["spec"]:
        return invalidate_build_aggregate(
            left,
            {
                "column": left["spec"]["column"],
                "error": "index build tasks carried inconsistent commit specifications",
                "phase": "build",
            },
        )
    left_error: dict[str, Any] | None = left.get("error")
    right_error: dict[str, Any] | None = right.get("error")
    if left_error is not None or right_error is not None:
        return invalidate_build_aggregate(left, left_error or right_error or {})
    if "stats" in left or "stats" in right:
        if "stats" in left and "stats" in right:
            return invalidate_build_aggregate(
                left,
                {
                    "column": left["spec"]["column"],
                    "error": "index build produced more than one terminal result",
                    "phase": "build",
                },
            )
        terminal: dict[str, Any] = left if "stats" in left else right
        other: dict[str, Any] = right if terminal is left else left
        if other["segments"] or int(other["built"]):
            return invalidate_build_aggregate(
                left,
                {
                    "column": left["spec"]["column"],
                    "error": "index build mixed a terminal result with shard payloads",
                    "phase": "build",
                },
            )
        left["stats"] = terminal["stats"]
        return left
    left["segments"].extend(right["segments"])
    left["built"] = int(left["built"]) + int(right["built"])
    return left


def commit_build_aggregate(
    key: tuple[str, str],
    aggregate: dict[str, Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> tuple[str, dict[str, Any]]:
    """Commit one executor-reduced index build or return its terminal build outcome.

    Args:
        key: Dataset URI and index name used by the Spark reduction.
        aggregate: Reduced build payload for the index.
        config: Indexing configuration.
        telemetry: Executor telemetry facade.

    Returns:
        Dataset URI and terminal index statistics or error.
    """
    uri, index_name = key
    spec: dict[str, Any] = aggregate["spec"]
    error: dict[str, Any] | None = aggregate.get("error")
    if error is not None:
        return uri, {
            "column": error.get("column", spec["column"]),
            "index": index_name,
            "error": error.get("error", "index build failed"),
            "phase": error.get("phase", "build"),
            "stale": False,
        }
    if "stats" in aggregate:
        return uri, aggregate["stats"]
    payloads: list[dict[str, Any]]
    if spec["kind"] == FTS_KIND:
        payloads = [{"built": int(aggregate["built"])}]
    else:
        payloads = [{"segment": document} for document in aggregate["segments"]]
    return uri, commit_one_index(uri, spec, payloads, config, telemetry)


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


@dataclass
class LanceIndexer:
    """Builds the configured indices over a Lance fleet with unified task-based phases."""

    config: IndexJobConfig

    def build_and_commit_fleet(
        self,
        spark: SparkSession,
        shard_seeds: list[dict[str, Any]],
    ) -> list[tuple[str, dict[str, Any]]]:
        """Build and commit every index without collecting shard metadata on the driver.

        The driver sends one bounded seed per index. Build executors reopen the exact version,
        verify the planned fragment count, enumerate IDs, and fan them into shard tasks. Vector
        artifacts remain executor-owned and use a small worker-local generation cache. The build
        results reduce by ``(uri, index_name)`` inside Spark, then each reducer commits its index.
        Only one bounded terminal outcome per index reaches the driver.

        Args:
            spark: Active Spark session.
            shard_seeds: Bounded dataset/version/count seeds across the fleet.

        Returns:
            One ``(uri, stats)`` terminal outcome per index.
        """
        config: IndexJobConfig = self.config
        if not shard_seeds:
            return []

        def build_partition(items: Any) -> Any:
            """Build the shard tasks assigned to this executor task.

            Per-index failure isolation turns a shard exception into a keyed error accumulator.
            The per-index reducer discards successful segment metadata whenever any shard failed,
            so a partial index cannot publish and no build payload needs to cross the driver.

            Args:
                items: The task specs for this partition.

            Yields:
                One keyed build accumulator per shard.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            with executor_telemetry.span("lance.indexing.build_segment"):
                task: Any
                for task in items:
                    try:
                        result_uri: str
                        result_index: str
                        payload: dict[str, Any]
                        result_uri, result_index, payload = build_one_shard(task, config, executor_telemetry)
                        if result_uri != task["uri"] or result_index != task["index_name"]:
                            raise RuntimeError("index build returned a result for a different task")
                    except Exception as exc:
                        executor_telemetry.incr("segment.build_error", tags=[f"index:{task['index_name']}"])
                        logger.warning(
                            "index build shard failed for %s on %s, isolating: %s",
                            task["index_name"],
                            task["uri"],
                            exc,
                        )
                        payload = {"error": str(exc), "phase": "build", "column": task["column"]}
                    yield (task["uri"], task["index_name"]), build_aggregate(task, payload)

        def commit_partition(items: Any) -> Any:
            """Commit executor-reduced index builds with per-index failure isolation.

            Args:
                items: Keyed build accumulators reduced to one item per index.

            Yields:
                One bounded ``(uri, stats)`` outcome per index.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            item: Any
            for item in items:
                key: tuple[str, str]
                aggregate: dict[str, Any]
                key, aggregate = item
                uri, index_name = key
                spec: dict[str, Any] = aggregate["spec"]
                try:
                    yield commit_build_aggregate(key, aggregate, config, executor_telemetry)
                except Exception as exc:
                    executor_telemetry.incr("index.commit_error", tags=[f"index:{index_name}"])
                    logger.warning("index commit failed for %s on %s, isolating: %s", index_name, uri, exc)
                    yield (
                        uri,
                        {
                            "uri": uri,
                            "column": spec["column"],
                            "index": index_name,
                            "error": str(exc),
                            "phase": "index_commit",
                            "stale": False,
                        },
                    )

        expected_tasks: int = sum(int(seed["shard_count"]) for seed in shard_seeds)
        max_build_tasks_resolved: int = derive_partitions(spark, BUILD_PARTITION_FACTOR)
        build_slices: int = max(1, min(expected_tasks, max_build_tasks_resolved))
        enumeration_slices: int = max(1, min(len(shard_seeds), max_build_tasks_resolved))
        max_commit_tasks_resolved: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        commit_slices: int = max(1, min(len(shard_seeds), max_commit_tasks_resolved))
        return (
            spark.sparkContext.parallelize(shard_seeds, enumeration_slices)
            .flatMap(lambda seed: enumerate_shard_tasks(seed, config))
            .repartition(build_slices)
            .mapPartitions(build_partition)
            .reduceByKey(merge_build_aggregates, commit_slices)
            .mapPartitions(commit_partition)
            .collect()
        )

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
        :func:`collect_round_specs`), one flat Spark job expands bounded seeds and builds every
        shard task, reduces shard metadata per index entirely inside Spark, and commits from those
        reducers. Finished index stats accumulate into ``stats_by_uri`` and every spec's kind is
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
        shard_seeds: list[dict[str, Any]] = flatten_shard_tasks(specs_by_uri, version_by_uri)
        logger.info(
            "indexing round %d/%d: %d datasets, %d index seeds",
            round_index + 1,
            config.max_stale_replans,
            len(specs_by_uri),
            len(shard_seeds),
        )
        with driver_telemetry.timed("run.build_ms"):
            outcomes: list[tuple[str, dict[str, Any]]] = self.build_and_commit_fleet(spark, shard_seeds)

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
        :func:`~lance_etl.fanout.count_failed` for library callers.
        Every other failed dataset is re-planned by the next scheduled run, since the job is
        cursor-free.

        Duplicate URIs are processed once, in first-occurrence order. Running two index plans for
        the same dataset concurrently can mix their name-keyed shard payloads and create avoidable
        commit conflicts, so duplicate work is removed before any Spark job is submitted.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per unique dataset, in first-occurrence order. A failed
            dataset carries an ``"error"`` key or an error entry among its ``indexes``.
        """
        config: IndexJobConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.indexing.run") as run_span:
            unique_uris: list[str] = list(dict.fromkeys(dataset_uris))
            run_span.set_tag("dataset_count", len(unique_uris))
            if not unique_uris:
                return []

            stats_by_uri: dict[str, dict[str, Any]] = {uri: {"uri": uri, "indexes": []} for uri in unique_uris}
            pending_uris: list[str] = list(unique_uris)
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

            results: list[dict[str, Any]] = [stats_by_uri[uri] for uri in unique_uris]
            skipped: int = sum(1 for stats in results if stats.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)

            report_fleet_failures(results, run_span, driver_telemetry, "indexing run", index_failure_phase, logger)

            logger.info("indexing run: %d datasets (%d skipped)", len(results), skipped)
            return results
