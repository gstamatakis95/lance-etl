"""LanceIndexer: unified fleet orchestration for index builds.

Every dataset, regardless of size, follows the same phases built on the same Lance segment
APIs, and a small dataset is simply the one-shard case:

- Plan (:func:`plan_dataset_indexes`): a per-dataset executor fan-out resolves which indexes to
  build (explicit config columns, or the ``lance-etl.columns`` role metadata written by the ETL
  sink), runs the derived-state skip check, and shards each index's target fragments into
  build tasks sized by ``fragments_per_index_task``. A vector index whose artifacts are absent,
  mismatched, or growth-stale plans one ``bootstrap`` task instead of shards (ADR 0030).
- Artifacts (:meth:`LanceIndexer.resolve_fleet_artifacts`): one flat Spark job reads the IVF_RQ
  artifacts back for every ``segments``-mode vector index in the fleet: centroids from the
  committed index via ``get_ivf_model`` and the RaBitQ rotation from the stored config. This
  phase is reuse-only, so no training sample ever exists here.
- Build (:meth:`LanceIndexer.build_fleet_segments`): ONE flat Spark job over every dataset's
  shard tasks. Vector and scalar shards build uncommitted segments, a vector bootstrap task
  runs a committed ``create_index`` whose internal streaming k-means trains the centroids, FTS
  rebuild shards build per-fragment inverted indices under their dataset's shared index id,
  and FTS maintain runs as a single task per dataset.
- Commit (:func:`commit_one_index`): a per-(dataset, index) executor fan-out merges vector
  segments and publishes through the production commit paths, keeping the heavy merge off the
  driver. A stale-fragment commit (a concurrent compaction rewrote planned fragments) marks the
  index for the next replan round instead of failing the run.
- Delta bound (:func:`merge_deltas_if_needed`): a final per-(dataset, index) fan-out merges
  accumulated index deltas once they exceed ``max_index_deltas``.

:meth:`LanceIndexer.run` repeats plan-artifacts-build-commit for stale indexes up to
``max_stale_replans`` rounds, then defers the survivors to the next scheduled run.
"""

from __future__ import annotations

import logging
import math
import uuid
from typing import Any

import lance
from lance.lance import indices as native_indices
from pyspark.sql import SparkSession

from lance_etl.column_roles import SCALAR_ROLE, TEXT_ROLE, VECTOR_ROLE, load_column_roles
from lance_etl.fanout import fan_out_per_dataset
from lance_etl.indexing.config import (
    IndexJobConfig,
    bitmap_index_name,
    degrade_num_partitions,
    derive_num_partitions,
    fts_index_name,
    scalar_index_name,
    vector_index_name,
)
from lance_etl.indexing.handlers import (
    BitmapIndexHandler,
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    VectorIndexHandler,
    commit_fts_index,
)
from lance_etl.indexing.optimize import (
    index_delta_count,
    load_vector_config,
    maintain_index_locally,
    write_vector_config,
)
from lance_etl.indexing.optimize import merge_index_deltas as merge_index_deltas_now
from lance_etl.indexing.segments import (
    build_scalar_segment,
    build_vector_segment,
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

FTS_KIND: str = "fts"
"""Index kind for BM25 INVERTED full-text indexes."""

KIND_TO_HANDLER: dict[str, type[IndexHandler]] = {
    VECTOR_KIND: VectorIndexHandler,
    BTREE_KIND: BTreeIndexHandler,
    BITMAP_KIND: BitmapIndexHandler,
    FTS_KIND: FtsIndexHandler,
}
"""Maps an index kind to the handler class owning its type-specific logic."""


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
    heterogeneous per-tenant schemas without per-dataset CLI flags.

    Args:
        dataset: The open dataset.
        config: Indexing configuration.

    Returns:
        ``(kind, column, index_name)`` triples in vector, btree, bitmap, text order.
    """
    explicit: bool = bool(
        config.vector_columns or config.scalar_columns or config.bitmap_columns or config.text_columns
    )
    if explicit:
        targets: list[tuple[str, str, str]] = []
        targets.extend((VECTOR_KIND, column, vector_index_name(column)) for column in config.vector_columns)
        targets.extend((BTREE_KIND, column, scalar_index_name(column)) for column in config.scalar_columns)
        targets.extend((BITMAP_KIND, column, bitmap_index_name(column)) for column in config.bitmap_columns)
        targets.extend((FTS_KIND, column, fts_index_name(column)) for column in config.text_columns)
        return targets

    roles: dict[str, str] = load_column_roles(dataset)
    columns: set[str] = set(dataset.schema.names)
    discovered: list[tuple[str, str, str]] = []
    for column in sorted(name for name, role in roles.items() if role == VECTOR_ROLE and name in columns):
        discovered.append((VECTOR_KIND, column, vector_index_name(column)))
    for column in sorted(name for name, role in roles.items() if role == SCALAR_ROLE and name in columns):
        discovered.append((BTREE_KIND, column, scalar_index_name(column)))
    for column in sorted(name for name, role in roles.items() if role == TEXT_ROLE and name in columns):
        discovered.append((FTS_KIND, column, fts_index_name(column)))
    return discovered


def index_skip_reason(
    dataset: lance.LanceDataset, config: IndexJobConfig, targets: list[tuple[str, str, str]]
) -> str | None:
    """Return a reason string when all targeted indices are current, or None to proceed.

    Evaluates derived dataset state from the already-open handle so no extra object-store I/O is
    needed. The check is bypassed when ``config.rebuild`` is True.

    For each targeted index the check proceeds as follows. When the index is absent the dataset
    needs indexing, unless it is a vector index and the row count is below
    ``config.vector_min_rows`` (intended skip — flat KNN suffices). When the index is present,
    ``dataset.stats.index_stats(name)`` is consulted: if ``num_unindexed_fragments`` is positive
    or ``num_indices`` exceeds ``config.max_index_deltas``, the dataset needs work. An existing
    vector index additionally needs work when its ``lance-etl.vector.{column}`` config entry is
    absent (an index built outside the segment path awaiting a full rebuild) or when the row
    count grew past ``config.retrain_growth_factor`` times the recorded ``rows_at_train``. Both
    reads come from the already-loaded manifest.

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

    for kind, column, name in targets:
        if name not in existing:
            if kind == VECTOR_KIND:
                if rows is None:
                    rows = dataset.count_rows()
                if rows < config.vector_min_rows:
                    continue
            return None

        stats: dict[str, Any] = dataset.stats.index_stats(name)
        if int(stats.get("num_unindexed_fragments") or 0) > 0:
            return None
        if int(stats.get("num_indices") or 0) > config.max_index_deltas:
            return None

        if kind == VECTOR_KIND:
            cfg: dict[str, Any] | None = load_vector_config(dataset, column)
            if cfg is None:
                return None
            rows_at_train: int = int(cfg.get("rows_at_train") or 0)
            if rows_at_train <= 0:
                return None
            if rows is None:
                rows = dataset.count_rows()
            if rows > config.retrain_growth_factor * rows_at_train:
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


def plan_dataset_indexes(
    uri: str,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run the plan phase for one dataset on an executor.

    Opens the dataset once (failure isolation: an unreadable dataset returns a skip record),
    resolves the index targets, applies the fleet-level and per-index skip checks, and shards
    each index's target fragments into build tasks. A vector index whose artifacts are absent,
    mismatched, or growth-stale (or a ``rebuild`` run) plans one ``bootstrap`` task: a committed
    ``create_index`` whose internal streaming k-means trains the centroids (ADR 0030). A vector
    index with reusable artifacts plans incremental ``segments`` shards as usual.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A dict with ``uri`` and either ``skipped`` or ``version`` plus per-index ``specs``.
        Each spec carries ``kind``, ``column``, ``index_name``, ``mode``, ``shards``, and the
        FTS extras (``index_uuid``, ``has_existing``). Indexes with nothing to do land in
        ``done`` as finished stats.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, OSError, ValueError) as exc:
        logger.warning("indexing: cannot open dataset %s, skipping: %s", uri, exc)
        telemetry.incr("dataset.index_open_error")
        return {"uri": uri, "indexes": [], "skipped": str(exc)}

    targets: list[tuple[str, str, str]] = resolve_index_targets(dataset, config)
    skip: str | None = index_skip_reason(dataset, config, targets)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        return {"uri": uri, "indexes": [], "skipped": skip}

    existing_names: set[str] = {description.name for description in dataset.describe_indices()}
    specs: list[dict[str, Any]] = []
    done: list[dict[str, Any]] = []
    for kind, column, index_name in targets:
        handler: IndexHandler = make_handler(kind, column, index_name, config)
        reason: str | None = handler.skip_reason(dataset)
        if reason is not None:
            telemetry.incr("index.skipped", tags=[f"index:{index_name}"])
            done.append({"column": column, "index": index_name, "segments": 0, "fragments": 0, "skipped": reason})
            continue
        handler.validate(dataset)

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
                    "has_existing": bool(fts_handler.covered_fragments(dataset)),
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
            done.append({"column": column, "index": index_name, "segments": 0, "fragments": 0})
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


def resolve_vector_artifacts(
    uri: str,
    column: str,
    index_name: str,
    config: IndexJobConfig,
) -> tuple[str, str, tuple, bool, int | None]:
    """Read one vector index's reusable IVF_RQ artifacts back on an executor.

    Delegates to the reuse-only :meth:`VectorIndexHandler.prepare`: centroids come from the
    committed index via ``get_ivf_model`` and the RaBitQ rotation from the stored config. Only
    ``segments``-mode specs reach this phase — datasets needing training plan a streaming
    bootstrap build instead (ADR 0030).

    Args:
        uri: Dataset URI.
        column: The vector column.
        index_name: The index name.
        config: Indexing configuration.

    Returns:
        ``(uri, column, artifacts, reused, num_partitions)`` where ``artifacts`` is the tuple
        the segment builders expect.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    handler: VectorIndexHandler = VectorIndexHandler(config, column, index_name)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    artifacts: tuple = handler.prepare(dataset, uri, telemetry)
    return uri, column, artifacts, handler.reused_artifacts, handler.num_partitions_used


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
    later incremental segments stay on the same model. After the commit the artifact config is
    stored so future runs reuse the centroids through ``get_ivf_model``. ``replace=True`` makes
    a growth retrain a wholesale index replacement.

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
    planned: int = derive_num_partitions(rows, config.num_partitions, config)
    partitions: int = degrade_num_partitions(planned, rows, config.streaming_sample_rate)
    rabitq_model: str = native_indices.build_rq_model(dimension=dimension, num_bits=config.ivf_rq_num_bits)
    streaming_kwargs: dict[str, Any] = {
        "streaming_sample_rate": config.streaming_sample_rate,
        "streaming_refine_passes": config.streaming_refine_passes,
    }
    if config.streaming_coreset_rate is not None:
        streaming_kwargs["streaming_coreset_rate"] = config.streaming_coreset_rate
    with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
        dataset.create_index(
            column,
            "IVF_RQ",
            name=index_name,
            metric=config.metric,
            replace=True,
            num_partitions=partitions,
            num_bits=config.ivf_rq_num_bits,
            rabitq_model=rabitq_model,
            **streaming_kwargs,
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
            "num_bits": config.ivf_rq_num_bits,
            "num_partitions": partitions,
            "rabitq_model": rabitq_model,
        },
        config,
        telemetry,
    )
    return {
        "column": column,
        "index": index_name,
        "segments": 1,
        "fragments": len(dataset.get_fragments()),
        "num_partitions": partitions,
        "reused_artifacts": False,
    }


def build_one_shard(
    task: dict[str, Any],
    artifacts_by_key: dict[tuple[str, str], tuple],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> tuple[str, str, dict[str, Any]]:
    """Build one flat-job task on an executor: a segment shard, FTS fragment shard, or FTS maintain.

    Args:
        task: The shard task spec from the plan phase, flattened with ``uri`` and ``version``.
        artifacts_by_key: The fleet's vector artifacts keyed by ``(uri, column)``.
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
        if kind == VECTOR_KIND:
            segment = build_vector_segment(
                dataset,
                shard,
                artifacts_by_key[(uri, column)],
                column=column,
                index_name=index_name,
                metric=config.metric,
            )
        else:
            index_type: str = "BTREE" if kind == BTREE_KIND else "BITMAP"
            segment = build_scalar_segment(
                dataset, shard, None, column=column, index_name=index_name, index_type=index_type
            )
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

    Segment kinds go through the production :func:`commit_segments`, which merges vector
    segments before publishing — running here keeps the merge off the driver. FTS rebuilds drop
    the old index only now, after the executor builds finished, then merge the per-fragment
    metadata and publish, so the old index stayed live for the whole build. A stale-fragment
    error returns a ``stale`` marker so the fleet re-plans this index in the next round.

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
            handler: FtsIndexHandler = FtsIndexHandler(config, column, index_name)
            built: int = sum(int(payload.get("built", 0)) for payload in payloads)
            commit_fts_index(
                uri,
                column,
                index_name,
                spec["index_uuid"],
                spec["fragments"],
                spec["has_existing"],
                config,
                telemetry,
            )
            del handler
            return {"column": column, "index": index_name, "segments": built, "fragments": len(spec["fragments"])}

        documents: list[str] = [payload["segment"] for payload in payloads if "segment" in payload]
        merge: bool = kind == VECTOR_KIND
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


def flatten_shard_tasks(
    specs_by_uri: dict[str, list[dict[str, Any]]], version_by_uri: dict[str, int]
) -> list[dict[str, Any]]:
    """Expand every dataset's index specs into the flat build-task list for one Spark job.

    Each shard becomes one task carrying its spec fields plus ``uri``, ``version``, and
    ``shard``. Specs without shards (vector bootstraps and FTS maintains) become a single task
    with an empty shard.

    Args:
        specs_by_uri: The plan phase's index specs, keyed by dataset URI.
        version_by_uri: The plan-time dataset version, keyed by dataset URI.

    Returns:
        The flattened task specs across the fleet.
    """
    shard_tasks: list[dict[str, Any]] = []
    for uri, specs in specs_by_uri.items():
        version: int = version_by_uri[uri]
        for spec in specs:
            base: dict[str, Any] = {**spec, "uri": uri, "version": version}
            if not spec["shards"]:
                shard_tasks.append({**base, "shard": []})
                continue
            for shard in spec["shards"]:
                shard_tasks.append({**base, "shard": list(shard)})
    return shard_tasks


def merge_deltas_if_needed(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> bool:
    """Merge one index's accumulated deltas on an executor when over the configured cap.

    Args:
        uri: Dataset URI.
        index_name: The index whose deltas to bound.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        ``True`` if a merge ran.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    if index_name not in {description.name for description in dataset.describe_indices()}:
        return False
    if index_delta_count(dataset, index_name) <= config.max_index_deltas:
        return False
    with telemetry.timed("index.delta_merge_ms", tags=[f"index:{index_name}"]):
        return merge_index_deltas_now(uri, index_name, config, telemetry)


class LanceIndexer:
    """Builds the configured indices over a Lance fleet with unified task-based phases."""

    def __init__(self, config: IndexJobConfig) -> None:
        """Initialize the indexer.

        Args:
            config: Indexing configuration.
        """
        self.config: IndexJobConfig = config

    def resolve_fleet_artifacts(
        self, spark: SparkSession, vector_specs: list[tuple[str, dict[str, Any]]]
    ) -> tuple[dict[tuple[str, str], tuple], dict[tuple[str, str], dict[str, Any]]]:
        """Resolve every vector index's artifacts in one flat Spark job.

        Args:
            spark: Active Spark session.
            vector_specs: ``(uri, spec)`` pairs for the fleet's vector indexes.

        Returns:
            The artifacts keyed by ``(uri, column)``, and per-key extra stats
            (``reused_artifacts``, ``num_partitions``).
        """
        config: IndexJobConfig = self.config
        if not vector_specs:
            return {}, {}

        def resolve_one(item: tuple[str, str, str]) -> tuple[str, str, tuple, bool, int | None]:
            """Resolve one vector index's artifacts on an executor.

            Args:
                item: ``(uri, column, index_name)``.

            Returns:
                The artifacts and reuse stats for the index.
            """
            return resolve_vector_artifacts(item[0], item[1], item[2], config)

        items: list[tuple[str, str, str]] = [(uri, spec["column"], spec["index_name"]) for uri, spec in vector_specs]
        slices: int = max(1, min(len(items), config.max_build_tasks))
        resolved: list[tuple[str, str, tuple, bool, int | None]] = (
            spark.sparkContext.parallelize(items, slices).map(resolve_one).collect()
        )
        artifacts: dict[tuple[str, str], tuple] = {}
        extras: dict[tuple[str, str], dict[str, Any]] = {}
        for uri, column, artifact, reused, partitions in resolved:
            artifacts[(uri, column)] = artifact
            extras[(uri, column)] = {"reused_artifacts": reused, "num_partitions": partitions}
        return artifacts, extras

    def build_fleet_segments(
        self,
        spark: SparkSession,
        shard_tasks: list[dict[str, Any]],
        artifacts: dict[tuple[str, str], tuple],
    ) -> list[tuple[str, str, dict[str, Any]]]:
        """Run every dataset's build tasks in one flat Spark job.

        Args:
            spark: Active Spark session.
            shard_tasks: Flattened shard task specs across the fleet.
            artifacts: The fleet's vector artifacts, broadcast once to all tasks.

        Returns:
            ``(uri, index_name, payload)`` triples collected from the executors.
        """
        config: IndexJobConfig = self.config
        if not shard_tasks:
            return []
        broadcast = spark.sparkContext.broadcast(artifacts)

        def build_partition(items: Any) -> Any:
            """Build the shard tasks assigned to this executor task.

            Args:
                items: The task specs for this partition.

            Yields:
                One build payload per task.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            with executor_telemetry.span("lance.indexing.build_segment"):
                for task in items:
                    yield build_one_shard(task, broadcast.value, config, executor_telemetry)

        slices: int = max(1, min(len(shard_tasks), config.max_build_tasks))
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

            Args:
                items: The ``(uri, spec, payloads)`` triples for this partition.

            Yields:
                One ``(uri, stats)`` pair per index.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            for uri, spec, payloads in items:
                yield uri, commit_one_index(uri, spec, payloads, config, executor_telemetry)

        slices: int = max(1, min(len(entries), config.batch_partitions))
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

            Args:
                items: The ``(uri, index_name)`` pairs for this partition.

            Yields:
                ``(uri, index_name, merged)`` per index.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            for uri, index_name in items:
                yield uri, index_name, merge_deltas_if_needed(uri, index_name, config, executor_telemetry)

        slices: int = max(1, min(len(entries), config.batch_partitions))
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
        """Run one plan-artifacts-build-commit round over the pending datasets.

        The plan fan-out resolves each dataset's index specs, one flat Spark job resolves the
        fleet's vector artifacts, one flat Spark job builds every shard task, and the commit
        fan-out publishes per index. Finished index stats accumulate into ``stats_by_uri`` and
        every spec's kind is recorded in ``kind_by_index`` for the final delta bound.

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
        plans: list[dict[str, Any]] = fan_out_per_dataset(
            spark,
            pending_uris,
            config.telemetry,
            lambda uri, telemetry: plan_dataset_indexes(uri, config, telemetry),
            config.batch_partitions,
        )

        specs_by_uri: dict[str, list[dict[str, Any]]] = {}
        for plan in plans:
            uri: str = plan["uri"]
            if "skipped" in plan:
                stats_by_uri[uri]["skipped"] = plan["skipped"]
                continue
            stats_by_uri[uri]["indexes"].extend(plan.get("done", []))
            if plan["specs"]:
                specs_by_uri[uri] = plan["specs"]
                for spec in plan["specs"]:
                    kind_by_index[(uri, spec["index_name"])] = spec["kind"]
                stats_by_uri[uri]["version"] = plan["version"]
        if not specs_by_uri:
            return []

        vector_specs: list[tuple[str, dict[str, Any]]] = [
            (uri, spec)
            for uri, specs in specs_by_uri.items()
            for spec in specs
            if spec["kind"] == VECTOR_KIND and spec["mode"] == "segments"
        ]
        with driver_telemetry.timed("run.artifacts_ms"):
            artifacts, artifact_extras = self.resolve_fleet_artifacts(spark, vector_specs)

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
            built: list[tuple[str, str, dict[str, Any]]] = self.build_fleet_segments(spark, shard_tasks, artifacts)

        payloads_by_index: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for uri, index_name, payload in built:
            if "stats" in payload:
                stats_by_uri[uri]["indexes"].append(payload["stats"])
                continue
            payloads_by_index.setdefault((uri, index_name), []).append(payload)

        commit_entries: list[tuple[str, dict[str, Any], list[dict[str, Any]]]] = []
        for uri, specs in specs_by_uri.items():
            for spec in specs:
                key: tuple[str, str] = (uri, spec["index_name"])
                if key in payloads_by_index:
                    commit_entries.append((uri, spec, payloads_by_index[key]))
        with driver_telemetry.timed("run.commit_ms"):
            outcomes: list[tuple[str, dict[str, Any]]] = self.commit_fleet(spark, commit_entries)

        stale_uris: set[str] = set()
        for uri, stats in outcomes:
            if stats.pop("stale", False):
                stale_uris.add(uri)
                continue
            extra: dict[str, Any] = artifact_extras.get((uri, stats["column"]), {})
            stats_by_uri[uri]["indexes"].append({**stats, **extra})
        return sorted(stale_uris)

    def run(self, spark: SparkSession, dataset_uris: list[str]) -> list[dict[str, Any]]:
        """Index every dataset through the unified plan-artifacts-build-commit rounds.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per dataset, in input order.
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

            for round_index in range(config.max_stale_replans):
                pending_uris = self.run_round(
                    spark, round_index, pending_uris, stats_by_uri, kind_by_index, driver_telemetry
                )
                if not pending_uris:
                    break

            for uri in pending_uris:
                logger.warning(
                    "index build on %s still has uncovered fragments after %d stale-replan rounds; "
                    "next scheduled run re-covers",
                    uri,
                    config.max_stale_replans,
                )

            delta_entries: list[tuple[str, str]] = sorted(
                {
                    (uri, item["index"])
                    for uri, stats in stats_by_uri.items()
                    for item in stats["indexes"]
                    if int(item.get("segments", 0)) > 0 and kind_by_index.get((uri, item["index"])) != FTS_KIND
                }
            )
            merged_flags: dict[tuple[str, str], bool] = self.bound_fleet_deltas(spark, delta_entries)
            for uri, stats in stats_by_uri.items():
                for item in stats["indexes"]:
                    key = (uri, item["index"])
                    if key in merged_flags:
                        item["deltas_merged"] = merged_flags[key]
                stats.pop("version", None)

            results: list[dict[str, Any]] = [stats_by_uri[uri] for uri in dataset_uris]
            skipped: int = sum(1 for stats in results if stats.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)
            logger.info("indexing run: %d datasets (%d skipped)", len(results), skipped)
            return results
