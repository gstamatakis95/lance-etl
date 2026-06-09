"""Distributed vector, scalar, and full-text indexing for Lance datasets.

Each index type is built by its own handler: :class:`VectorIndexHandler` for IVF_RQ, :class:`BTreeIndexHandler`
and :class:`BitmapIndexHandler` for scalars, and :class:`FtsIndexHandler` for full-text (BM25). Every commit
retries conflicts with randomised exponential backoff and guards against stale fragments from concurrent
compaction. :class:`LanceIndexer.run` orchestrates many datasets in two tiers. Requires pylance and the
Datadog Agent on the executors.
"""

from __future__ import annotations

import base64
import functools
import json
import logging
import threading
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import lance
import pyarrow as pa
from lance.dataset import Index
from lance.indices import IndicesBuilder
from lance.lance import indices as native_indices
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import object_exists, read_object, resolve_filesystem, write_object
from lance_etl.telemetry import DEFAULT_COMMIT_RETRIES, Telemetry, TelemetryConfig, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

METRIC_TO_DISTANCE: dict[str, str] = {"l2": "l2", "cosine": "cosine", "dot": "dot"}
"""Maps lowercase metric names to Lance distance-type strings."""

FTS_OPTIONAL_PARAMS: tuple[str, ...] = (
    "base_tokenizer",
    "language",
    "lower_case",
    "stem",
    "remove_stop_words",
    "ascii_folding",
)
"""FTS keyword arguments forwarded to ``create_scalar_index`` only when set on the config."""

STALE_FRAGMENT_MARKERS: tuple[str, ...] = ("would orphan fragments", "no longer exist")
"""Error-message substrings that identify a segment commit invalidated by a concurrent compaction."""

TRAIN_SEMAPHORE: threading.Semaphore = threading.Semaphore(1)
"""Process-level semaphore that serialises concurrent IVF trainings from the driver thread pool."""


@dataclass
class IndexJobConfig:
    """Configuration for :class:`LanceIndexer`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        vector_columns: Vector columns to index with IVF_RQ; each gets its own handler and artifact sidecar.
        num_partitions: IVF partitions; derived from ``min_ivf_partitions``/``max_ivf_partitions`` when unset.
        vector_min_rows: Skip the vector index below this row count; flat KNN is sufficient.
        metric: Distance metric such as ``L2``, ``cosine``, or ``dot``.
        distance_type: IVF training distance; derived from ``metric`` when unset.
        scalar_columns: Columns to index with btree.
        bitmap_columns: Columns to index with bitmap.
        text_columns: Columns to index with a full-text inverted index.
        fts_with_position: Store token positions for phrase queries.
        fts_base_tokenizer: FTS base tokenizer name.
        fts_language: FTS stemming and stop-word language.
        fts_lower_case: Lowercase FTS tokens when set.
        fts_stem: Apply FTS stemming when set.
        fts_remove_stop_words: Remove FTS stop words when set.
        fts_ascii_folding: Apply FTS ASCII folding when set.
        num_shards: Parallel segment builders per dataset.
        rebuild: Reindex every fragment; forces the small tier and FTS handler to rebuild instead of maintaining.
        max_index_deltas: Merge accumulated index deltas into one when the count exceeds this cap.
        fts_max_unindexed_fragments: Maintain an inverted index incrementally only within this unindexed backlog.
        commit_retries: Retry budget for commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
        small_dataset_fragment_threshold: Datasets with fewer fragments are indexed end-to-end on one executor.
        small_tier_slices: Spark partition count for the batched small-dataset and classification jobs.
        driver_concurrency: Concurrent large-dataset submissions from the driver thread pool.
        scheduler_pool: Spark FAIR scheduler pool name for large-dataset jobs.
        min_ivf_partitions: Floor on the derived IVF partition count.
        max_ivf_partitions: Cap on the derived IVF partition count; operators needing more set ``num_partitions``.
        target_rows_per_ivf_partition: Target rows per partition for the size-aware policy.
        ivf_rq_num_bits: RaBitQ bits per sub-dimension; 1 gives maximum compression with a refine pass.
        train_sample_rate: Rows sampled per IVF partition when training centroids.
        train_max_iters: Maximum k-means iterations when training the IVF.
        retrain_growth_factor: Retrain when row count exceeds this multiple of ``rows_at_train`` in the sidecar.
        train_sample_memory_budget_bytes: Driver RAM cap for the IVF training sample; caps partition count.
        max_stale_replans: Rebuild-everything cycles in ``build_and_commit_segments`` before giving up.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    vector_columns: list[str] = field(default_factory=list)
    num_partitions: int | None = None
    vector_min_rows: int = 10_000
    metric: str = "L2"
    distance_type: str | None = None
    scalar_columns: list[str] = field(default_factory=list)
    bitmap_columns: list[str] = field(default_factory=list)
    text_columns: list[str] = field(default_factory=list)
    fts_with_position: bool = False
    fts_base_tokenizer: str | None = None
    fts_language: str | None = None
    fts_lower_case: bool | None = None
    fts_stem: bool | None = None
    fts_remove_stop_words: bool | None = None
    fts_ascii_folding: bool | None = None
    num_shards: int = 64
    rebuild: bool = False
    max_index_deltas: int = 4
    fts_max_unindexed_fragments: int = 32
    commit_retries: int = DEFAULT_COMMIT_RETRIES
    commit_backoff_seconds: float = 0.5
    small_dataset_fragment_threshold: int = 32
    small_tier_slices: int = 256
    driver_concurrency: int = 8
    scheduler_pool: str = "lance-indexing"
    min_ivf_partitions: int = 16
    max_ivf_partitions: int = 32768
    target_rows_per_ivf_partition: int = 8192
    ivf_rq_num_bits: int = 1
    train_sample_rate: int = 256
    train_max_iters: int = 50
    retrain_growth_factor: float = 4.0
    train_sample_memory_budget_bytes: int = 8 * 1024**3
    max_stale_replans: int = 3

    def resolved_distance_type(self) -> str:
        """Return the IVF training distance derived from the metric if unset.

        Returns:
            A Lance distance type string.
        """
        if self.distance_type is not None:
            return self.distance_type
        return METRIC_TO_DISTANCE.get(self.metric.lower(), "l2")

    def fts_params(self) -> dict[str, Any]:
        """Build the inverted-index parameters, omitting unset options.

        Returns:
            Keyword arguments for an ``INVERTED`` index build.
        """
        params: dict[str, Any] = {"with_position": self.fts_with_position}
        values: dict[str, object | None] = {
            "base_tokenizer": self.fts_base_tokenizer,
            "language": self.fts_language,
            "lower_case": self.fts_lower_case,
            "stem": self.fts_stem,
            "remove_stop_words": self.fts_remove_stop_words,
            "ascii_folding": self.fts_ascii_folding,
        }
        for name in FTS_OPTIONAL_PARAMS:
            if values[name] is not None:
                params[name] = values[name]
        return params


def scalar_index_name(column: str) -> str:
    """Return the btree index name for a scalar column.

    Args:
        column: The scalar column name.

    Returns:
        The derived index name.
    """
    return f"{column}_idx"


def bitmap_index_name(column: str) -> str:
    """Return the bitmap index name for a column.

    Args:
        column: The column name.

    Returns:
        The derived index name.
    """
    return f"{column}_bitmap_idx"


def fts_index_name(column: str) -> str:
    """Return the full-text index name for a text column.

    Args:
        column: The text column name.

    Returns:
        The derived index name.
    """
    return f"{column}_fts_idx"


def vector_index_name(column: str) -> str:
    """Return the IVF_RQ vector index name for a vector column.

    Mirrors the naming convention of :func:`scalar_index_name`, :func:`bitmap_index_name`, and
    :func:`fts_index_name`. Each vector column listed in :attr:`IndexJobConfig.vector_columns` receives an
    index with the name returned by this function.

    Args:
        column: The vector column name.

    Returns:
        The derived index name.
    """
    return f"{column}_idx"


def derive_num_partitions(rows: int, configured: int | None, config: IndexJobConfig) -> int:
    """Return the IVF partition count for a dataset size.

    Follows the size-aware policy
    ``clamp(rows // config.target_rows_per_ivf_partition, config.min_ivf_partitions, config.max_ivf_partitions)``
    unless an explicit partition count was configured. The caller may apply
    :func:`memory_bounded_num_partitions` before training to stay within the driver memory budget.

    Args:
        rows: The dataset row count.
        configured: An explicit partition count, taking precedence when set.
        config: Indexing configuration supplying the policy bounds.

    Returns:
        The planned IVF partition count.
    """
    if configured is not None:
        return configured
    return min(
        config.max_ivf_partitions,
        max(config.min_ivf_partitions, rows // config.target_rows_per_ivf_partition),
    )


def degrade_num_partitions(planned: int, rows: int, sample_rate: int) -> int:
    """Lower the partition count when training rows are insufficient.

    ``train_ivf`` samples ``num_partitions * sample_rate`` rows. When the dataset cannot supply that many, the partition
    count is degraded to what the available rows can train.

    Args:
        planned: The planned IVF partition count.
        rows: The dataset row count.
        sample_rate: Rows sampled per partition during IVF training.

    Returns:
        A partition count trainable from the available rows, at least 1.
    """
    supportable: int = rows // sample_rate
    return max(1, min(planned, supportable))


def memory_bounded_num_partitions(planned: int, dimension: int, config: IndexJobConfig) -> int:
    """Cap the planned IVF partition count so the training sample fits within the driver memory budget.

    ``train_ivf`` loads ``planned * config.train_sample_rate`` float32 vectors of length ``dimension`` into driver RAM.
    This function floors the planned count to what ``config.train_sample_memory_budget_bytes`` can accommodate.
    The result is always at least 1.

    Args:
        planned: The partition count derived by policy or degraded for row count.
        dimension: The vector dimension of the column being indexed.
        config: Indexing configuration supplying the memory budget and sample rate.

    Returns:
        A partition count whose training sample fits within the configured budget, at least 1.
    """
    budget: int = config.train_sample_memory_budget_bytes // (config.train_sample_rate * dimension * 4)
    return max(1, min(planned, budget))


def artifact_directory(uri: str, column: str) -> str:
    """Return the per-dataset artifact sidecar location for a column.

    Args:
        uri: Dataset URI.
        column: Vector column the artifacts belong to.

    Returns:
        A sidecar directory beside the dataset, scoped to the column.
    """
    return f"{uri.rstrip('/')}.artifacts/{column}"


def sidecar_locations(uri: str, column: str, storage_options: dict[str, Any] | None) -> tuple[Any, str, str]:
    """Resolve the artifact sidecar filesystem and its manifest and centroids paths.

    Args:
        uri: Dataset URI.
        column: Vector column the artifacts belong to.
        storage_options: Object-store options forwarded to pyarrow.

    Returns:
        A ``(filesystem, manifest_path, centroids_path)`` triple for the sidecar.
    """
    filesystem, base_path = resolve_filesystem(artifact_directory(uri, column), storage_options)
    base: str = base_path.rstrip("/")
    return filesystem, f"{base}/manifest.json", f"{base}/ivf_centroids.arrow"


def centroids_to_ipc(centroids: pa.Array) -> bytes:
    """Serialize IVF centroids to an Arrow IPC stream.

    Args:
        centroids: The fixed-size-list centroid array.

    Returns:
        The IPC stream bytes.
    """
    table: pa.Table = pa.table({"centroids": centroids})
    sink: pa.BufferOutputStream = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def centroids_from_ipc(data: bytes) -> pa.Array:
    """Deserialize IVF centroids from an Arrow IPC stream.

    Args:
        data: The IPC stream bytes.

    Returns:
        The fixed-size-list centroid array.
    """
    reader = pa.ipc.open_stream(pa.BufferReader(data))
    return reader.read_all().column("centroids").combine_chunks()


def split_evenly(values: list[int], shards: int) -> list[list[int]]:
    """Split ids into balanced shards by round-robin assignment.

    Args:
        values: The fragment ids to split.
        shards: The desired number of shards.

    Returns:
        A list of non-empty shards.
    """
    count: int = max(1, min(shards, len(values)))
    groups: list[list[int]] = [values[index::count] for index in range(count)]
    return [group for group in groups if group]


def all_fragment_ids(dataset: lance.LanceDataset) -> list[int]:
    """Return every fragment id of a dataset in fragment order.

    Args:
        dataset: The dataset to inspect.

    Returns:
        The fragment ids in the order ``get_fragments`` reports them.
    """
    return [fragment.fragment_id for fragment in dataset.get_fragments()]


def live_fragment_ids(dataset: lance.LanceDataset) -> set[int]:
    """Return the set of fragment ids currently live in a dataset.

    Args:
        dataset: The dataset to inspect.

    Returns:
        The live fragment ids as a set.
    """
    return set(all_fragment_ids(dataset))


def serialize_segment(segment: Index) -> str:
    """Serialize uncommitted segment metadata to a JSON document.

    Args:
        segment: The segment metadata returned by an uncommitted build.

    Returns:
        A JSON string carrying everything the commit needs.

    Raises:
        ValueError: If the segment is missing the index details required to commit it.
    """
    if segment.index_details is None:
        raise ValueError(f"segment {segment.uuid} is missing index details")
    type_url, detail_bytes = segment.index_details
    payload: dict[str, Any] = {
        "uuid": segment.uuid,
        "name": segment.name,
        "fields": list(segment.fields),
        "dataset_version": segment.dataset_version,
        "fragment_ids": sorted(segment.fragment_ids),
        "index_version": segment.index_version,
        "index_details_type_url": type_url,
        "index_details_b64": base64.b64encode(detail_bytes).decode("ascii"),
    }
    return json.dumps(payload)


def deserialize_segment(document: str) -> Index:
    """Reconstruct segment metadata from its JSON document.

    Args:
        document: The JSON string produced by :func:`serialize_segment`.

    Returns:
        An index-metadata object the commit accepts.
    """
    payload: dict[str, Any] = json.loads(document)
    details: tuple[str, bytes] = (
        payload["index_details_type_url"],
        base64.b64decode(payload["index_details_b64"]),
    )
    return Index(
        uuid=payload["uuid"],
        name=payload["name"],
        fields=payload["fields"],
        dataset_version=payload["dataset_version"],
        fragment_ids=set(payload["fragment_ids"]),
        index_version=payload["index_version"],
        index_details=details,
    )


def commit_index_with_retries(
    action: Callable[[], Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
    tags: list[str],
) -> Any:
    """Run an index commit action, retrying commit conflicts with the configured budget.

    Centralizes the retry budget, backoff, and the shared ``index.commit_conflict`` conflict metric used by every
    index commit path so each call site states only its action and its metric tags.

    Args:
        action: The commit to attempt, returning any result. It must re-read the dataset so each retry rebases.
        config: Indexing configuration supplying the retry budget and backoff.
        telemetry: Telemetry facade for the current process.
        tags: Metric tags applied to the conflict counter.

    Returns:
        Whatever ``action`` returns on its first non-conflicting attempt.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    return commit_with_retries(
        action,
        config.commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("index.commit_conflict", tags=tags),
    )


def optimize_existing_index(
    uri: str,
    index_name: str,
    config: IndexJobConfig,
    telemetry: Telemetry,
    num_indices_to_merge: int | None = None,
) -> None:
    """Run incremental maintenance for one existing index, retrying conflicts.

    Appends unindexed fragments to the existing index without retraining and no-ops cheaply when the index already
    covers everything. Runs in the calling process, so call it from an executor task. Each retry re-opens the dataset
    at the latest version, which makes the retry productive against concurrent ingestion and compaction.

    Args:
        uri: Dataset URI.
        index_name: The existing index to maintain.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.
        num_indices_to_merge: When set, also merge the delta with this many existing indices.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    tags: list[str] = [f"index:{index_name}"]

    def action() -> None:
        """Optimize the index against the latest dataset version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        kwargs: dict[str, Any] = {"index_names": [index_name]}
        if num_indices_to_merge is not None:
            kwargs["num_indices_to_merge"] = num_indices_to_merge
        dataset.optimize.optimize_indices(**kwargs)
        telemetry.incr("index.optimized", tags=tags)

    commit_index_with_retries(action, config, telemetry, tags)


def index_delta_count(dataset: lance.LanceDataset, index_name: str) -> int:
    """Return how many deltas (per-name index metadata entries) an index has.

    Args:
        dataset: The dataset to inspect.
        index_name: The index name.

    Returns:
        The ``num_indices`` value from the index statistics.
    """
    stats: dict[str, Any] = dataset.stats.index_stats(index_name)
    return int(stats.get("num_indices") or 0)


def merge_index_deltas(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> bool:
    """Merge an index's accumulated deltas into one when over the configured cap.

    Incremental runs add one delta per run per index, and every query consults all of them. The merge rewrites the
    deltas against current row addresses, which also permanently retires deferred frag-reuse remap debt. Runs in the
    calling process, so call it from an executor task, and only for an index that exists.

    Args:
        uri: Dataset URI.
        index_name: The existing index whose deltas to bound.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        ``True`` if a merge ran, ``False`` when the delta count was within the cap.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    deltas: int = index_delta_count(dataset, index_name)
    if deltas <= config.max_index_deltas:
        return False
    optimize_existing_index(uri, index_name, config, telemetry, num_indices_to_merge=deltas)
    telemetry.incr("index.deltas_merged", tags=[f"index:{index_name}"])
    logger.info("merged %d index deltas into one for %s on %s", deltas, index_name, uri)
    return True


def is_stale_fragment_error(exc: BaseException) -> bool:
    """Report whether an exception marks a segment commit invalidated by a concurrent compaction.

    Shared by the vector, scalar, and inverted-index paths so the stale-fragment guard is detected in one place
    rather than reimplemented per index type.

    Args:
        exc: The exception to inspect.

    Returns:
        ``True`` when the exception means the planned fragment set is stale and the build must be redone.
    """
    if not isinstance(exc, ValueError):
        return False
    message: str = str(exc)
    return any(marker in message for marker in STALE_FRAGMENT_MARKERS)


def commit_segments(
    uri: str,
    segment_documents: list[str],
    column: str,
    index_name: str,
    merge: bool,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> int:
    """Commit built segments, retrying conflicts to coexist with writers.

    Each attempt validates the segments against the latest fragment set first. A concurrent compaction can rewrite
    fragments between the segment build and this commit, and a blind retry at the new head version would then publish
    segments pointing at fragments that no longer exist, silently corrupting search results. Stale segments are
    dropped with a metric instead. When every segment is stale the commit is skipped and ``0`` is returned. When a
    surviving segment overlaps a wider existing segment that a compaction remapped over a rewritten fragment, lance
    raises the ``"would orphan fragments"`` ``ValueError``, which propagates so the caller can re-resolve and rebuild.

    Args:
        uri: Dataset URI.
        segment_documents: Serialized segments returned by the executors.
        column: The indexed column.
        index_name: The index name to publish under.
        merge: Whether to merge segments before committing, used for IVF_RQ and BITMAP.
        config: Indexing configuration.
        telemetry: Driver telemetry facade.

    Returns:
        The number of fresh (non-stale) segments that were committed. ``0`` when every segment was stale and the
        commit was skipped.

    Raises:
        ValueError: If publishing the fresh segments would orphan fragments held by a wider existing segment, so the
            caller must re-resolve the fragment set and rebuild.
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    segments: list[Index] = [deserialize_segment(document) for document in segment_documents]
    tags: list[str] = [f"index:{index_name}"]

    def action() -> int:
        """Drop stale segments, merge if needed, and commit at the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        live: set[int] = live_fragment_ids(dataset)
        fresh: list[Index] = [segment for segment in segments if set(segment.fragment_ids) <= live]
        stale: int = len(segments) - len(fresh)
        if stale:
            telemetry.incr("index.stale_segments_dropped", value=stale, tags=tags)
            logger.warning(
                "dropping %d stale segments for %s on %s: their fragments were rewritten between build and commit",
                stale,
                index_name,
                uri,
            )
        if not fresh:
            logger.warning("every segment for %s on %s is stale; skipping commit, next run re-covers", index_name, uri)
            return 0
        if merge and len(fresh) > 1:
            merged = dataset.merge_existing_index_segments(fresh)
            dataset.commit_existing_index_segments(index_name, column, [merged])
        else:
            dataset.commit_existing_index_segments(index_name, column, fresh)
        telemetry.incr("index.committed", tags=tags)
        return len(fresh)

    return commit_index_with_retries(action, config, telemetry, tags)


def index_holds_dead_fragments(dataset: lance.LanceDataset, index_name: str) -> bool:
    """Report whether a committed index segment references fragments that no longer exist.

    A compaction's inline index remap can leave a committed segment pointing at fragments it rewrote away. The orphan
    guard in ``commit_existing_index_segments`` then refuses to publish any new segment that would orphan those dead
    fragments, and no rebuild can ever cover a fragment that does not exist, so the index can only be repaired by
    dropping and rebuilding it from the intact rows.

    Args:
        dataset: A dataset handle at the latest version.
        index_name: The index name to inspect.

    Returns:
        ``True`` when any segment of the index references a fragment that is not live.
    """
    live: set[int] = live_fragment_ids(dataset)
    for description in dataset.describe_indices():
        if description.name == index_name:
            for segment in description.segments:
                if not set(segment.fragment_ids) <= live:
                    return True
    return False


def drop_stale_index(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Drop a corrupt index whose committed segments reference rewritten-away fragments, retrying conflicts.

    Used by :func:`build_and_commit_segments` when a compaction remap orphaned dead fragments inside an existing
    segment. Dropping clears the unrepairable segment so the next build re-covers every live fragment from the intact
    rows. Runs in the calling process.

    Args:
        uri: Dataset URI.
        index_name: The index to drop.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    tags: list[str] = [f"index:{index_name}"]

    def action() -> None:
        """Drop the index at the latest version when it still exists."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if index_name in {description.name for description in dataset.describe_indices()}:
            dataset.drop_index(index_name)
            telemetry.incr("index.dropped_stale", tags=tags)

    commit_index_with_retries(action, config, telemetry, tags)


def build_and_commit_segments(
    uri: str,
    handler: IndexHandler,
    config: IndexJobConfig,
    telemetry: Telemetry,
    build_documents: Callable[[list[list[int]], int, object | None], list[str]],
) -> dict[str, int]:
    """Build per-shard segments and commit them, rebuilding when a concurrent compaction makes the plan stale.

    The segment build plans over a fragment set resolved at one version. A concurrent compaction can rewrite some of
    those fragments and remap an existing wider index segment over them between the build and the commit, so
    publishing the freshly built segments would either orphan fragments the existing segment still holds
    (:func:`commit_segments` re-raises the ``"would orphan fragments"`` ``ValueError``) or cover fragments that no
    longer exist (:func:`commit_segments` drops them). Both mean the fragment set is stale. This mirrors the
    compactor's re-plan-on-conflict loop: it re-reads the dataset at the latest version, re-resolves the target
    fragments (dropping fragments that no longer exist), rebuilds the affected segments, and re-commits, bounded by
    ``config.commit_retries``. When the orphan is caused by an existing segment still pointing at fragments a
    compaction rewrote away (:func:`index_holds_dead_fragments`), no rebuild can cover the dead fragments, so the
    corrupt index is dropped (:func:`drop_stale_index`) and rebuilt clean from the intact rows. Every live target
    therefore ends up covered rather than silently skipped. The full-rebuild replan loop is bounded by
    ``config.max_stale_replans`` (not by ``config.commit_retries``): each replan rebuilds every remaining target
    fragment from scratch which can be terabytes of I/O for a whale org, so it is bounded tightly and independently
    from the cheap commit-conflict budget. If the budget is exhausted while a writer keeps rewriting, the remaining
    fragments are left for the next scheduled run, which re-covers them once the contention clears.

    Args:
        uri: Dataset URI.
        handler: The per-type index handler resolving targets and recording coverage.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.
        build_documents: Builds one serialized segment per shard for the given fragment groups, pinned to the given
            dataset version, using the broadcast artifacts. Supplied by the distributed and in-process callers so the
            rebuild loop is shared.

    Returns:
        A mapping with the total ``segments`` committed and the ``fragments`` targeted on the first attempt.
    """
    tags: list[str] = [f"index:{handler.index_name}"]
    total_segments: int = 0
    first_targets: int = 0
    for attempt in range(config.max_stale_replans):
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        targets: list[int] = handler.target_fragments(dataset)
        if attempt == 0:
            first_targets = len(targets)
        if not targets:
            return {"segments": total_segments, "fragments": first_targets}
        artifacts: object | None = handler.prepare(dataset, uri, telemetry)
        version: int = dataset.version
        groups: list[list[int]] = split_evenly(targets, config.num_shards)
        documents: list[str] = build_documents(groups, version, artifacts)
        try:
            committed: int = commit_segments(
                uri, documents, handler.column, handler.index_name, handler.merges(), config, telemetry
            )
        except ValueError as exc:
            if not is_stale_fragment_error(exc):
                raise
            telemetry.incr("index.stale_fragment_replan", tags=tags)
            refreshed: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            if index_holds_dead_fragments(refreshed, handler.index_name):
                logger.warning(
                    "dropping corrupt index %s on %s: a compaction remap left a segment pointing at "
                    "rewritten-away fragments; rebuilding from intact rows (%s)",
                    handler.index_name,
                    uri,
                    exc,
                )
                drop_stale_index(uri, handler.index_name, config, telemetry)
            else:
                logger.warning(
                    "rebuilding %s on %s: a concurrent compaction orphaned the planned fragment set (%s)",
                    handler.index_name,
                    uri,
                    exc,
                )
            continue
        if committed:
            total_segments += committed
            handler.record_coverage(uri, lance.dataset(uri, storage_options=config.storage_options))
        if committed == len(documents):
            return {"segments": total_segments, "fragments": first_targets}
        telemetry.incr("index.stale_fragment_replan", tags=tags)
    logger.warning(
        "index %s on %s still has uncovered fragments after %d stale-replan attempts; next scheduled run re-covers",
        handler.index_name,
        uri,
        config.max_stale_replans,
    )
    return {"segments": total_segments, "fragments": first_targets}


def lance_field_id(dataset: lance.LanceDataset, column: str) -> int:
    """Return the Lance field id for a top-level column.

    Uses the internal Lance schema rather than the Arrow positional index so the field id remains stable across schema
    evolution. ``dataset._ds`` is the only way to access the Lance schema from Python. Access is wrapped here to
    contain the private-attribute usage.

    Args:
        dataset: The dataset to inspect.
        column: The column name to look up.

    Returns:
        The Lance field id for the column.

    Raises:
        ValueError: If the column is not present in the Lance schema.
    """
    lance_field = dataset._ds.lance_schema.field_case_insensitive(column)
    if lance_field is None:
        raise ValueError(f"column {column!r} not found in Lance schema")
    return lance_field.id()


def build_scalar_segment(
    dataset: lance.LanceDataset,
    fragment_ids: list[int],
    artifacts: object | None,
    column: str,
    index_name: str,
    index_type: str,
) -> Index:
    """Build one artifact-free scalar segment (BTREE or BITMAP) over a shard of fragments.

    This is a module-level function so the Spark closure can capture it through a small
    :func:`functools.partial` of primitive values rather than a bound method, which would pickle the entire handler
    instance onto every task. ``index_uuid`` must not be passed for these segment builds. Lance mints segment ids
    itself. ``replace=True`` bypasses the same-name existence guard on the uncommitted path so incremental segments can
    extend an existing index. It removes nothing, since delta removal happens only on the committed path.

    Args:
        dataset: A dataset handle pinned to the build version.
        fragment_ids: The fragment ids for this shard.
        artifacts: Unused by scalar builds. Present so the callable signature matches the vector builder.
        column: The column to index.
        index_name: The name of the index.
        index_type: The scalar index type (``BTREE`` or ``BITMAP``).

    Returns:
        The uncommitted segment metadata.
    """
    del artifacts
    return dataset.create_index_uncommitted(
        column=column,
        index_type=index_type,
        name=index_name,
        replace=True,
        fragment_ids=fragment_ids,
    )


def build_vector_segment(
    dataset: lance.LanceDataset,
    fragment_ids: list[int],
    artifacts: object | None,
    column: str,
    index_name: str,
    metric: str,
) -> Index:
    """Build one IVF_RQ segment over a shard of fragments.

    Module-level for the same closure-pickling reason as :func:`build_scalar_segment`. ``replace=True`` is required for
    incremental coverage: the uncommitted build path applies the same-name existence guard, and without it a segment
    build for an index that already exists raises. The flag only bypasses that guard. Removal of prior deltas happens
    solely on the committed ``execute`` path, never here, so existing coverage is preserved and :func:`commit_segments`
    publishes the new segments as a delta.

    Args:
        dataset: A dataset handle pinned to the build version.
        fragment_ids: The fragment ids for this shard.
        artifacts: The centroids bytes, the shared RaBitQ model string, num_bits, and the IVF partition count.
        column: The column to index.
        index_name: The name of the index.
        metric: The distance metric for the vector index.

    Returns:
        The uncommitted segment metadata.

    Raises:
        ValueError: If ``artifacts`` is ``None``. Vector segment builds require the artifact tuple produced by
            :meth:`VectorIndexHandler.prepare`.
    """
    if artifacts is None:
        raise ValueError("build_vector_segment requires artifacts from prepare; got None")
    centroids_bytes, rabitq_model, num_bits, num_partitions = artifacts
    centroids: pa.Array = centroids_from_ipc(centroids_bytes)
    return dataset.create_index_uncommitted(
        column=column,
        index_type="IVF_RQ",
        name=index_name,
        metric=metric,
        replace=True,
        num_partitions=num_partitions,
        num_bits=num_bits,
        ivf_centroids=centroids,
        rabitq_model=rabitq_model,
        fragment_ids=fragment_ids,
    )


class IndexHandler:
    """Base handler that builds one index over a dataset's fragments.

    The default :meth:`build` implements the segment-API flow shared by the vector handler: split target fragments into
    shards, build one uncommitted segment per shard across executors, and commit the collected segments. Subclasses
    override the build steps or the whole flow.
    """

    def __init__(self, config: IndexJobConfig, column: str, index_name: str) -> None:
        """Initialize the handler.

        Args:
            config: Indexing configuration.
            column: The column to index.
            index_name: The index name to publish under.
        """
        self.config: IndexJobConfig = config
        self.column: str = column
        self.index_name: str = index_name

    def index_type(self) -> str:
        """Return the Lance index type string.

        Returns:
            The index type, such as ``BTREE``.
        """
        raise NotImplementedError

    def merges(self) -> bool:
        """Report whether segments are merged before commit.

        Returns:
            ``True`` to merge segments into one before committing.
        """
        return False

    def validate(self, dataset: lance.LanceDataset) -> None:
        """Validate that the dataset supports this index.

        Subclasses override this to raise ``ValueError`` when the dataset does not satisfy the index's prerequisites.
        The base implementation accepts any dataset.

        Args:
            dataset: The dataset to validate against.
        """
        del dataset

    def skip_reason(self, dataset: lance.LanceDataset) -> str | None:
        """Return why this index should be skipped for the dataset, if at all.

        Subclasses override this to opt out of indexing, for example when the dataset is too small to benefit. The base
        implementation never skips.

        Args:
            dataset: The dataset to inspect.

        Returns:
            A human-readable reason to skip, or ``None`` to proceed.
        """
        del dataset
        return None

    def extra_stats(self) -> dict[str, Any]:
        """Return handler-specific fields to merge into the result.

        Returns:
            Additional statistics, empty by default.
        """
        return {}

    def covered_fragments(self, dataset: lance.LanceDataset) -> set[int]:
        """Return fragments already covered by this index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The set of covered fragment ids.
        """
        covered: set[int] = set()
        for description in dataset.describe_indices():
            if description.name == self.index_name and self.column in description.field_names:
                for segment in description.segments:
                    covered.update(segment.fragment_ids)
        return covered

    def target_fragments(self, dataset: lance.LanceDataset) -> list[int]:
        """Return fragments to index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            Every fragment when rebuilding, otherwise only uncovered fragments.
        """
        all_ids: list[int] = all_fragment_ids(dataset)
        if self.config.rebuild:
            return all_ids
        covered: set[int] = self.covered_fragments(dataset)
        return [fragment_id for fragment_id in all_ids if fragment_id not in covered]

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> object | None:
        """Build artifacts to broadcast to the segment builders.

        Subclasses override this to train or load artifacts that are broadcast to each executor shard. The base
        implementation requires no artifacts.

        Args:
            dataset: The dataset being indexed.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A broadcastable artifact, or ``None`` when none is needed.
        """
        del dataset, uri, telemetry
        return None

    def record_coverage(self, uri: str, dataset: lance.LanceDataset) -> None:
        """Record this index's committed fragment coverage after a successful build.

        Subclasses override this to persist coverage for staleness detection. The base implementation records
        nothing, which is correct for scalar and inverted indexes whose inline compaction remap is sound.

        Args:
            uri: Dataset URI.
            dataset: A dataset handle refreshed after the commit.
        """
        del uri, dataset

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one uncommitted scalar segment over a shard of fragments.

        This base implementation covers the artifact-free scalar types (BTREE and BITMAP). It delegates to the
        module-level :func:`build_scalar_segment` so the same logic backs both direct calls and the closure-friendly
        builder returned by :meth:`segment_builder`. Handlers that need broadcast artifacts override this method.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The broadcast artifact, unused by scalar builds.

        Returns:
            The uncommitted segment metadata.
        """
        return build_scalar_segment(
            dataset,
            fragment_ids,
            artifacts,
            column=self.column,
            index_name=self.index_name,
            index_type=self.index_type(),
        )

    def segment_builder(self) -> Callable[[lance.LanceDataset, list[int], object | None], Index]:
        """Return a picklable per-shard segment builder that does not capture the handler instance.

        The Spark closure in :meth:`build` ships this callable to executors. Returning a
        :func:`functools.partial` over the module-level :func:`build_scalar_segment` with only primitive values keeps
        the serialized task small. Capturing the bound ``self.build_segment`` instead would pickle the whole handler,
        including its ``config`` with ``storage_options`` and ``telemetry``, onto every task.

        Returns:
            A callable taking the shard dataset, fragment ids, and broadcast artifacts.
        """
        return functools.partial(
            build_scalar_segment,
            column=self.column,
            index_name=self.index_name,
            index_type=self.index_type(),
        )

    def merge_deltas(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> bool:
        """Merge this index's accumulated deltas on one executor when over the cap.

        The driver only reads the index statistics. The merge itself, which can approach a rebuild for sort-merge
        scalar types, runs in a single-task Spark job so heavy work stays off the driver.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            ``True`` if a merge ran.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if self.index_name not in {description.name for description in dataset.describe_indices()}:
            return False
        if index_delta_count(dataset, self.index_name) <= config.max_index_deltas:
            return False
        index_name: str = self.index_name

        def merge_one(target: str) -> bool:
            """Merge the index deltas inside an executor task.

            Args:
                target: Dataset URI.

            Returns:
                ``True`` if a merge ran.
            """
            return merge_index_deltas(target, index_name, config, Telemetry.create(config.telemetry))

        with telemetry.timed("index.delta_merge_ms", tags=[f"index:{index_name}"]):
            merged: list[bool] = spark.sparkContext.parallelize([uri], 1).map(merge_one).collect()
        return bool(merged and merged[0])

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build and commit this index across executors, then bound its deltas.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        reason: str | None = self.skip_reason(dataset)
        if reason is not None:
            telemetry.incr("index.skipped", tags=[f"index:{self.index_name}"])
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0, "skipped": reason}
        self.validate(dataset)

        spark_context = spark.sparkContext
        build_segment: Callable[[lance.LanceDataset, list[int], object | None], Index] = self.segment_builder()
        storage_options: dict[str, Any] | None = config.storage_options
        index_type: str = self.index_type()

        def build_documents(groups: list[list[int]], version: int, artifacts: object | None) -> list[str]:
            """Build one serialized segment per shard across executors at the pinned version.

            Args:
                groups: Fragment-id shards to build.
                version: Dataset version to pin every shard build to.
                artifacts: Broadcast artifacts for the segment builder, if any.

            Returns:
                The serialized segments collected from the executors.
            """
            broadcast_artifacts = spark_context.broadcast(artifacts) if artifacts is not None else None

            def build_partition(group_iterator: Iterator[list[int]]) -> Iterator[str]:
                """Build one segment per shard on an executor.

                Args:
                    group_iterator: Fragment-id shards assigned to this task.

                Yields:
                    The serialized segment for each shard.
                """
                telemetry_local: Telemetry = Telemetry.create(config.telemetry)
                local_artifacts: object | None = broadcast_artifacts.value if broadcast_artifacts is not None else None
                tags: list[str] = [f"index_type:{index_type}"]
                with telemetry_local.span("lance.indexing.build_segment"):
                    for group in group_iterator:
                        shard_dataset: lance.LanceDataset = lance.dataset(
                            uri, version=version, storage_options=storage_options
                        )
                        with telemetry_local.timed("segment.build_ms", tags=tags):
                            segment = build_segment(shard_dataset, list(group), local_artifacts)
                        telemetry_local.incr("segment.built", tags=tags)
                        yield serialize_segment(segment)

            return spark_context.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()

        with telemetry.timed("index.build_ms", tags=[f"index:{self.index_name}"]):
            stats: dict[str, int] = build_and_commit_segments(uri, self, config, telemetry, build_documents)
        result: dict[str, Any] = {
            "column": self.column,
            "index": self.index_name,
            "segments": stats["segments"],
            "fragments": stats["fragments"],
            "deltas_merged": self.merge_deltas(spark, uri, telemetry),
        }
        result.update(self.extra_stats())
        return result


class VectorIndexHandler(IndexHandler):
    """Builds an IVF_RQ vector index, training or reusing per-dataset artifacts."""

    def __init__(self, config: IndexJobConfig, column: str, index_name: str) -> None:
        """Initialize the handler.

        Args:
            config: Indexing configuration.
            column: The vector column to index.
            index_name: The index name to publish under.
        """
        super().__init__(config, column, index_name)
        self.reused_artifacts: bool = False
        self.num_partitions_used: int | None = None

    def index_type(self) -> str:
        """Return the vector index type.

        Returns:
            The string ``IVF_RQ``.
        """
        return "IVF_RQ"

    def merges(self) -> bool:
        """Report that IVF_RQ segments are merged before commit.

        Returns:
            Always ``True``.
        """
        return True

    def extra_stats(self) -> dict[str, Any]:
        """Return artifact reuse and the partition count actually used.

        When ``reused_artifacts`` is true, the sidecar supplied the centroids, ``num_partitions``, and the
        ``rabitq_model`` rotation string.

        Returns:
            A mapping with the artifact reuse flag and IVF partition count.
        """
        return {"reused_artifacts": self.reused_artifacts, "num_partitions": self.num_partitions_used}

    def skip_reason(self, dataset: lance.LanceDataset) -> str | None:
        """Skip the vector index when the dataset is below the row floor.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The skip reason for small datasets, or ``None`` to proceed.
        """
        rows: int = dataset.count_rows()
        if rows < self.config.vector_min_rows:
            return f"{rows} rows below vector_min_rows={self.config.vector_min_rows}; flat KNN suffices"
        return None

    def dimension(self, dataset: lance.LanceDataset) -> int:
        """Return the vector dimension of the indexed column.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The fixed vector dimension.
        """
        return IndicesBuilder(dataset, self.column).dimension

    def validate(self, dataset: lance.LanceDataset) -> None:
        """Validate the IVF_RQ parameters against the column.

        Args:
            dataset: The dataset to validate against.

        Raises:
            ValueError: If the dimension is unsupported.
        """
        if self.dimension(dataset) % 8 != 0:
            raise ValueError("IVF_RQ requires the vector dimension to be divisible by 8")

    def load_manifest(self, uri: str) -> dict[str, Any] | None:
        """Load the artifact sidecar manifest for this column, if present.

        Args:
            uri: Dataset URI.

        Returns:
            The parsed manifest, or ``None`` when no sidecar exists.
        """
        filesystem, manifest_path = sidecar_locations(uri, self.column, self.config.storage_options)[:2]
        if not object_exists(filesystem, manifest_path):
            return None
        return json.loads(read_object(filesystem, manifest_path))

    def growth_requires_retrain(self, manifest: dict[str, Any], rows: int) -> bool:
        """Decide whether dataset growth since training forces a centroid retrain.

        A manifest without ``rows_at_train`` predates the retrain trigger and retrains once to record it.

        Args:
            manifest: The stored artifact manifest.
            rows: The dataset's current row count.

        Returns:
            ``True`` when the artifacts must be retrained instead of reused.
        """
        rows_at_train: Any = manifest.get("rows_at_train")
        if rows_at_train is None:
            return True
        return rows > self.config.retrain_growth_factor * int(rows_at_train)

    def remap_requires_rebuild(self, manifest: dict[str, Any], dataset: lance.LanceDataset) -> bool:
        """Decide whether a compaction since the last build forces a full rebuild.

        On the pinned lance build, compaction's inline eager remap silently corrupts IVF_RQ indexes (about half the
        rows become unsearchable while the index still reports full coverage), and the deferred remap fails vector
        queries loudly, so a remapped vector index can never be trusted. The manifest records the live fragment ids
        covered when this handler last committed segments. Any of those fragments disappearing means a rewrite
        consumed indexed fragments and the surviving index content went through the corrupting remap. A manifest
        without the field predates this guard and reports no debt, since the field is recorded on every build pass.

        Args:
            manifest: The stored artifact manifest.
            dataset: The dataset to inspect.

        Returns:
            ``True`` when fragments covered at the last build no longer exist.
        """
        covered: Any = manifest.get("covered_fragment_ids")
        if covered is None:
            return False
        live: set[int] = live_fragment_ids(dataset)
        return not set(covered) <= live

    def record_coverage(self, uri: str, dataset: lance.LanceDataset) -> None:
        """Persist the live fragment ids this index covers after a successful commit.

        Stored in the artifact sidecar manifest so the next maintenance pass can detect that a compaction rewrote
        indexed fragments (see :meth:`remap_requires_rebuild`). Recorded only when a sidecar already exists, which is
        always true after :meth:`prepare` ran for this build.

        Args:
            uri: Dataset URI.
            dataset: A dataset handle refreshed after the commit.
        """
        manifest: dict[str, Any] | None = self.load_manifest(uri)
        if manifest is None:
            return
        live: set[int] = live_fragment_ids(dataset)
        manifest["covered_fragment_ids"] = sorted(self.covered_fragments(dataset) & live)
        filesystem, manifest_path = sidecar_locations(uri, self.column, self.config.storage_options)[:2]
        write_object(filesystem, manifest_path, json.dumps(manifest).encode("utf-8"))

    def manifest_reusable(self, manifest: dict[str, Any], dimension: int) -> bool:
        """Return whether a sidecar manifest can be reused for the current configuration.

        A manifest is reusable when it contains a ``rabitq_model`` entry and its pinned ``dimension``, ``metric``, and
        ``num_bits`` all match the current configuration. The partition count is not checked: reused centroids define
        it, so the build always adopts ``num_partitions`` from the manifest when reusing.

        Callers that receive ``False`` must fall through to the training path rather than raising, so a changed
        dimension or metric triggers a retrain once and then proceeds normally instead of bricking the dataset.

        Args:
            manifest: The stored artifact manifest.
            dimension: The vector dimension of the column being indexed.

        Returns:
            ``True`` when the manifest is safe to reuse, ``False`` when a retrain is required.
        """
        if "rabitq_model" not in manifest:
            return False
        config: IndexJobConfig = self.config
        expected: dict[str, Any] = {
            "dimension": dimension,
            "metric": config.metric,
            "num_bits": config.ivf_rq_num_bits,
        }
        return all(manifest.get(name) == value for name, value in expected.items())

    def target_fragments(self, dataset: lance.LanceDataset) -> list[int]:
        """Return fragments to index, expanding to all of them on a retrain or after a corrupting remap.

        Retrained centroids and rotation cannot merge with segments built from the old artifacts, so when the growth
        trigger fires every fragment is rebuilt, exactly as on a ``rebuild`` run. The same full rebuild runs when
        :meth:`remap_requires_rebuild` reports that a compaction rewrote covered fragments, because the inline remap
        corrupts IVF_RQ content while leaving coverage statistics clean. A non-reusable manifest (changed dimension or
        metric) is also treated as a full-rebuild trigger so the index self-heals rather than remaining broken. Full
        incoming coverage replaces every prior delta on commit, restoring recall from the intact row data.

        Args:
            dataset: The dataset to inspect.

        Returns:
            Every fragment when retraining, rebuilding, repairing remap debt, or recovering from a config mismatch,
            otherwise only uncovered fragments.
        """
        config: IndexJobConfig = self.config
        if not config.rebuild:
            manifest: dict[str, Any] | None = self.load_manifest(dataset.uri)
            if manifest is not None:
                dimension: int = self.dimension(dataset)
                if not self.manifest_reusable(manifest, dimension):
                    logger.warning(
                        "pinned artifacts for %s on %s no longer match the current configuration; "
                        "the index will be retrained and fully rebuilt",
                        self.index_name,
                        dataset.uri,
                    )
                    return all_fragment_ids(dataset)
                if self.growth_requires_retrain(manifest, dataset.count_rows()):
                    return all_fragment_ids(dataset)
                if self.remap_requires_rebuild(manifest, dataset):
                    logger.info(
                        "rebuilding %s on %s: a compaction rewrote covered fragments and the inline remap "
                        "cannot be trusted for IVF_RQ",
                        self.index_name,
                        dataset.uri,
                    )
                    return all_fragment_ids(dataset)
        return super().target_fragments(dataset)

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> object | None:
        """Load this dataset's IVF_RQ artifacts, or train and persist them.

        Trains the IVF centroid model and mints one shared RaBitQ rotation via ``lance.lance.indices.build_rq_model``.
        Both are persisted to the sidecar manifest and broadcast. The SAME ``rabitq_model`` JSON string must reach every
        executor because it pins the rotation, so per-fragment segments produce comparable binary codes and remain
        mergeable. If it were omitted, each ``create_index_uncommitted`` call would generate its own random rotation,
        which is only safe for a single non-merged segment.

        The partition count follows the size-aware policy, degraded when the dataset cannot supply
        ``num_partitions * config.train_sample_rate`` training rows, and further capped by
        :func:`memory_bounded_num_partitions` so the training sample stays within
        ``config.train_sample_memory_budget_bytes``. The training block is serialised behind :data:`TRAIN_SEMAPHORE`
        so at most one training runs at a time across the driver thread pool.

        Persisted artifacts are reused only while the dataset stays within ``config.retrain_growth_factor`` times the
        ``rows_at_train`` recorded at training time and :meth:`manifest_reusable` returns ``True``. When the manifest
        is non-reusable (changed dimension or metric), the training path runs and overwrites the sidecar so the index
        self-heals on the next build rather than raising forever.

        Args:
            dataset: The dataset to train on if artifacts are absent.
            uri: Dataset URI used to locate the artifact sidecar.
            telemetry: Driver telemetry facade.

        Returns:
            The centroids IPC bytes, the RaBitQ model JSON string, num_bits,
            and the IVF partition count.
        """
        config: IndexJobConfig = self.config
        dimension: int = self.dimension(dataset)
        rows: int = dataset.count_rows()
        filesystem, manifest_path, centroids_path = sidecar_locations(uri, self.column, config.storage_options)

        if not config.rebuild and object_exists(filesystem, manifest_path):
            manifest: dict[str, Any] = json.loads(read_object(filesystem, manifest_path))
            if not self.manifest_reusable(manifest, dimension):
                logger.warning(
                    "sidecar manifest for %s is not reusable (missing rabitq_model or config mismatch); "
                    "retraining artifacts",
                    uri,
                )
            elif self.growth_requires_retrain(manifest, rows):
                telemetry.incr("artifacts.retrained_for_growth")
                logger.info(
                    "retraining IVF artifacts for %s: %d rows exceed %.1fx rows_at_train=%s",
                    uri,
                    rows,
                    config.retrain_growth_factor,
                    manifest.get("rows_at_train"),
                )
            else:
                self.reused_artifacts = True
                self.num_partitions_used = int(manifest["num_partitions"])
                telemetry.incr("artifacts.reused")
                return (
                    read_object(filesystem, centroids_path),
                    manifest["rabitq_model"],
                    config.ivf_rq_num_bits,
                    self.num_partitions_used,
                )

        planned: int = derive_num_partitions(rows, config.num_partitions, config)
        after_degrade: int = degrade_num_partitions(planned, rows, config.train_sample_rate)
        partitions: int = memory_bounded_num_partitions(after_degrade, dimension, config)
        if partitions < planned:
            telemetry.incr("artifacts.partitions_degraded")
            if partitions < after_degrade:
                logger.warning(
                    "capped num_partitions %d -> %d for %s (dim=%d): training sample would exceed memory budget",
                    after_degrade,
                    partitions,
                    uri,
                    dimension,
                )
            else:
                logger.info("degraded num_partitions %d -> %d for %s (%d rows)", planned, partitions, uri, rows)
        with TRAIN_SEMAPHORE, telemetry.timed("artifacts.train_ms"):
            ivf_model = IndicesBuilder(dataset, self.column).train_ivf(
                num_partitions=partitions,
                distance_type=config.resolved_distance_type(),
                sample_rate=config.train_sample_rate,
                max_iters=config.train_max_iters,
            )
            centroids_bytes: bytes = centroids_to_ipc(ivf_model.centroids)
            rabitq_model: str = native_indices.build_rq_model(dimension=dimension, num_bits=config.ivf_rq_num_bits)
        new_manifest: dict[str, Any] = {
            "dimension": dimension,
            "metric": config.metric,
            "num_partitions": partitions,
            "num_bits": config.ivf_rq_num_bits,
            "distance_type": config.resolved_distance_type(),
            "rabitq_model": rabitq_model,
            "rows_at_train": rows,
            "created_at": datetime.now(UTC).isoformat(),
        }
        write_object(filesystem, centroids_path, centroids_bytes)
        write_object(filesystem, manifest_path, json.dumps(new_manifest).encode("utf-8"))
        self.reused_artifacts = False
        self.num_partitions_used = partitions
        telemetry.incr("artifacts.trained")
        return centroids_bytes, rabitq_model, config.ivf_rq_num_bits, partitions

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one IVF_RQ segment over a shard of fragments.

        Delegates to the module-level :func:`build_vector_segment` so the same logic backs both direct calls and the
        closure-friendly builder returned by :meth:`segment_builder`.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The centroids bytes, the shared RaBitQ model string, num_bits, and the IVF partition count.

        Returns:
            The uncommitted segment metadata.

        Raises:
            ValueError: If ``artifacts`` is ``None``. Vector segment builds require the artifact tuple produced by
                :meth:`prepare`.
        """
        return build_vector_segment(
            dataset,
            fragment_ids,
            artifacts,
            column=self.column,
            index_name=self.index_name,
            metric=self.config.metric,
        )

    def segment_builder(self) -> Callable[[lance.LanceDataset, list[int], object | None], Index]:
        """Return a picklable IVF_RQ segment builder that does not capture the handler instance.

        Mirrors :meth:`IndexHandler.segment_builder` but binds the vector-specific :func:`build_vector_segment` with
        the metric. The centroids and RaBitQ model are not bound here. They reach executors through the separate
        artifact broadcast and arrive as the ``artifacts`` argument at call time.

        Returns:
            A callable taking the shard dataset, fragment ids, and broadcast artifacts.
        """
        return functools.partial(
            build_vector_segment,
            column=self.column,
            index_name=self.index_name,
            metric=self.config.metric,
        )


class BTreeIndexHandler(IndexHandler):
    """Builds a btree scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver publishes the collected segments with
    ``commit_existing_index_segments``. BTREE segments do not support driver-side merging, so they are committed
    unmerged. The segment build and incremental fragment coverage are inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the btree index type.

        Returns:
            The string ``BTREE``.
        """
        return "BTREE"


class BitmapIndexHandler(IndexHandler):
    """Builds a bitmap scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver merges the collected segments into one with
    ``merge_existing_index_segments`` before publishing via ``commit_existing_index_segments``. The segment build and
    incremental fragment coverage are inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the bitmap index type.

        Returns:
            The string ``BITMAP``.
        """
        return "BITMAP"

    def merges(self) -> bool:
        """Report that bitmap segments are merged before commit.

        Returns:
            Always ``True``.
        """
        return True


class FtsIndexHandler(IndexHandler):
    """Maintains a full-text BM25 inverted index, rebuilding only when it must.

    An existing index whose unindexed backlog is within ``fts_max_unindexed_fragments`` is maintained incrementally on
    one executor with ``optimize_indices``, which merges INVERTED deltas natively and falls back internally to an
    old-plus-new rebuild only when the index's update criteria require it. The distributed metadata-merge rebuild
    remains for first builds, large backlogs, and ``rebuild`` runs after tokenizer-parameter changes. Inverted indices
    are not built through the segment API: each shard builds its fragments under one shared index id, the driver merges
    the per-fragment metadata, and the index is published with a create-index commit.
    """

    def index_type(self) -> str:
        """Return the inverted index type.

        Returns:
            The string ``INVERTED``.
        """
        return "INVERTED"

    def maintainable(self, dataset: lance.LanceDataset) -> bool:
        """Decide whether the existing index can be maintained incrementally.

        Args:
            dataset: The dataset to inspect.

        Returns:
            ``True`` when the index exists, no rebuild was requested, and the unindexed backlog is within the
            configured fragment threshold.
        """
        if self.config.rebuild or not self.covered_fragments(dataset):
            return False
        stats: dict[str, Any] = dataset.stats.index_stats(self.index_name)
        return int(stats.get("num_unindexed_fragments") or 0) <= self.config.fts_max_unindexed_fragments

    def maintain(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Maintain the existing inverted index incrementally on one executor.

        Runs ``optimize_indices`` for this index in a single-task Spark job, then bounds the delta count.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index with ``maintained`` set.
        """
        config: IndexJobConfig = self.config
        index_name: str = self.index_name

        def maintain_one(target: str) -> bool:
            """Optimize and delta-bound the index inside an executor task.

            Args:
                target: Dataset URI.

            Returns:
                ``True`` if a delta merge ran.
            """
            return maintain_index_locally(target, index_name, config, Telemetry.create(config.telemetry))

        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            merged: list[bool] = spark.sparkContext.parallelize([uri], 1).map(maintain_one).collect()
        return {
            "column": self.column,
            "index": index_name,
            "segments": 0,
            "fragments": 0,
            "maintained": True,
            "deltas_merged": bool(merged and merged[0]),
        }

    def commit_index(
        self,
        uri: str,
        dataset: lance.LanceDataset,
        index_uuid: str,
        fragment_ids: list[int],
        telemetry: Telemetry,
    ) -> None:
        """Publish the merged inverted index, retrying conflicts.

        Each attempt validates that every covered fragment still exists at the latest version. A concurrent compaction
        can rewrite covered fragments between the executor build and this commit, and a blind retry at the new head
        version would then publish an index whose row addresses point at compacted-away fragments. That state raises
        instead, failing the dataset loudly so the next run rebuilds.

        Args:
            uri: Dataset URI.
            dataset: A dataset handle refreshed to the latest version after the executor build.
            index_uuid: The shared index id the shards built under.
            fragment_ids: The fragments the index covers.
            telemetry: Driver telemetry facade.

        Raises:
            ValueError: If covered fragments no longer exist because a compaction rewrote them.
            OSError | RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: IndexJobConfig = self.config
        field_id: int = lance_field_id(dataset, self.column)
        index_name: str = self.index_name
        fragments: set[int] = set(fragment_ids)
        storage_options: dict[str, Any] | None = config.storage_options
        tags: list[str] = ["index_type:INVERTED"]

        def action() -> None:
            """Publish the merged inverted index at the latest version."""
            current: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
            live: set[int] = live_fragment_ids(current)
            missing: set[int] = fragments - live
            if missing:
                raise ValueError(
                    f"inverted index {index_name} on {uri} covers fragments {sorted(missing)} that no longer exist; "
                    "a compaction rewrote them between build and commit, so this build must be redone"
                )
            index: Index = Index(
                uuid=index_uuid,
                name=index_name,
                fields=[field_id],
                dataset_version=current.version,
                fragment_ids=fragments,
                index_version=0,
            )
            operation = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=[])
            lance.LanceDataset.commit(uri, operation, read_version=current.version, storage_options=storage_options)
            telemetry.incr("index.committed", tags=tags)

        commit_index_with_retries(action, config, telemetry, tags)

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Maintain the inverted index incrementally, or rebuild it across executors.

        An existing index with a small unindexed backlog is maintained with ``optimize_indices`` on one executor.
        Otherwise the full distributed rebuild runs: the dataset handle is refreshed after the executor build so the
        metadata merge and the publish commit both operate against the latest committed version rather than the
        snapshot captured before the Spark job ran.

        On the distributed rebuild path an existing same-name index is dropped AFTER the executor builds complete,
        just before the metadata merge and the publish commit. This shrinks the availability gap compared to dropping
        before the Spark job: the old index stays live for the entire (potentially hours-long) executor build phase
        and is absent only for the short merge-plus-commit window. The executor builds use ``replace=True`` to bypass
        the same-name existence guard on the uncommitted per-fragment path. That guard (in
        ``rust/lance/src/index/create.rs``) raises when an index with the same name exists and ``replace=False``.
        Passing ``replace=True`` to ``execute_uncommitted`` skips the guard without deleting the committed index:
        deletion via ``removed_indices`` only happens in the ``execute`` (commit-requiring) path, which the
        per-fragment uncommitted call never reaches. The early drop then removes the old index just before the merge
        so the subsequent ``commit_index`` publishes the new index cleanly with an empty ``removed_indices``.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if self.maintainable(dataset):
            return self.maintain(spark, uri, telemetry)
        has_existing: bool = bool(self.covered_fragments(dataset))

        fragment_ids: list[int] = all_fragment_ids(dataset)
        if not fragment_ids:
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0}

        version: int = dataset.version
        index_uuid: str = str(uuid.uuid4())
        params: dict[str, Any] = config.fts_params()
        groups: list[list[int]] = split_evenly(fragment_ids, config.num_shards)
        column: str = self.column
        index_name: str = self.index_name
        storage_options: dict[str, Any] | None = config.storage_options

        def build_partition(group_iterator: Iterator[list[int]]) -> Iterator[int]:
            """Build per-fragment inverted indices under the shared id.

            Uses ``replace=True`` to bypass the same-name existence guard on the uncommitted per-fragment path so the
            old committed index remains live and searchable while the executor builds run.

            Args:
                group_iterator: Fragment-id shards assigned to this task.

            Yields:
                The count of fragments this task built.
            """
            telemetry_local: Telemetry = Telemetry.create(config.telemetry)
            built: int = 0
            with telemetry_local.span("lance.indexing.build_fts_segment"):
                for group in group_iterator:
                    shard_dataset: lance.LanceDataset = lance.dataset(
                        uri, version=version, storage_options=storage_options
                    )
                    for fragment_id in group:
                        with telemetry_local.timed("segment.build_ms", tags=["index_type:INVERTED"]):
                            shard_dataset.create_scalar_index(
                                column=column,
                                index_type="INVERTED",
                                name=index_name,
                                replace=True,
                                index_uuid=index_uuid,
                                fragment_ids=[fragment_id],
                                **params,
                            )
                        built += 1
                        telemetry_local.incr("segment.built", tags=["index_type:INVERTED"])
            yield built

        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            counts: list[int] = (
                spark.sparkContext.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()
            )
        if has_existing:
            drop_stale_index(uri, self.index_name, config, telemetry)
        dataset = lance.dataset(uri, storage_options=config.storage_options)
        with telemetry.timed("index.merge_ms", tags=[f"index:{index_name}"]):
            dataset.merge_index_metadata(index_uuid, index_type="INVERTED")
        with telemetry.timed("index.commit_ms", tags=[f"index:{index_name}"]):
            self.commit_index(uri, dataset, index_uuid, fragment_ids, telemetry)
        return {
            "column": self.column,
            "index": index_name,
            "segments": sum(counts),
            "fragments": len(fragment_ids),
        }


def maintain_index_locally(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> bool:
    """Maintain one existing index in process, then bound its delta count.

    Args:
        uri: Dataset URI.
        index_name: The existing index to maintain.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        ``True`` if a delta merge ran after the maintenance pass.
    """
    optimize_existing_index(uri, index_name, config, telemetry)
    return merge_index_deltas(uri, index_name, config, telemetry)


def maintained_stats(column: str, index_name: str, fragments: int, deltas_merged: bool) -> dict[str, Any]:
    """Build the statistics dictionary for an incrementally maintained index.

    Args:
        column: The indexed column.
        index_name: The maintained index name.
        fragments: The dataset's fragment count.
        deltas_merged: Whether a delta merge ran after the maintenance pass.

    Returns:
        A statistics dictionary matching the large-tier shape with ``maintained`` set.
    """
    return {
        "column": column,
        "index": index_name,
        "segments": 0,
        "fragments": fragments,
        "maintained": True,
        "deltas_merged": deltas_merged,
    }


def index_dataset_locally(uri: str, config: IndexJobConfig) -> dict[str, Any]:
    """Build or maintain every configured index for one small dataset on one executor.

    This is the small-dataset tier: no segment fan-out. An index that already exists is maintained incrementally with
    ``optimize_indices``, which appends only unindexed fragments and no-ops cheaply when the index is fully covered,
    so sweeping the unchanged power-law tail costs near zero. Missing indices, or every index on a ``rebuild`` run
    (the path for parameter changes), are built with plain ``create_index`` / ``create_scalar_index`` calls that build
    and commit end-to-end. After each maintenance pass the index's deltas are merged once they exceed
    ``max_index_deltas``. Single-process ``create_index`` needs no shared RaBitQ model — a lone non-merged segment may
    use its own random rotation. Each vector column listed in :attr:`IndexJobConfig.vector_columns` follows the same
    size-aware policy as the distributed path and is skipped below the configured row floor. Incremental vector
    maintenance assigns new rows to existing IVF partitions without retraining, so a grown dataset retrains via the
    large tier's growth trigger once it crosses the fragment threshold, or earlier via a ``rebuild`` run.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.

    Returns:
        A statistics dictionary matching the large-tier shape. Maintained indices carry ``maintained: True``.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragments: int = len(dataset.get_fragments())
    existing: set[str] = {description.name for description in dataset.describe_indices()}
    indexes: list[dict[str, Any]] = []
    with telemetry.span("lance.indexing.local_dataset"):
        rows: int = dataset.count_rows()
        for vec_col in config.vector_columns:
            idx_name: str = vector_index_name(vec_col)
            if rows < config.vector_min_rows:
                telemetry.incr("index.skipped", tags=[f"index:{idx_name}"])
                reason: str = f"{rows} rows below vector_min_rows={config.vector_min_rows}; flat KNN suffices"
                indexes.append(
                    {
                        "column": vec_col,
                        "index": idx_name,
                        "segments": 0,
                        "fragments": 0,
                        "skipped": reason,
                    }
                )
            elif idx_name in existing and not config.rebuild:
                with telemetry.timed("index.build_ms", tags=[f"index:{idx_name}"]):
                    merged: bool = maintain_index_locally(uri, idx_name, config, telemetry)
                indexes.append(maintained_stats(vec_col, idx_name, fragments, merged))
            else:
                planned: int = derive_num_partitions(rows, config.num_partitions, config)
                partitions: int = degrade_num_partitions(planned, rows, config.train_sample_rate)
                with telemetry.timed("index.build_ms", tags=[f"index:{idx_name}"]):
                    dataset.create_index(
                        vec_col,
                        "IVF_RQ",
                        name=idx_name,
                        metric=config.metric,
                        replace=True,
                        num_partitions=partitions,
                        num_bits=config.ivf_rq_num_bits,
                    )
                telemetry.incr("index.committed", tags=[f"index:{idx_name}"])
                indexes.append(
                    {
                        "column": vec_col,
                        "index": idx_name,
                        "segments": 1,
                        "fragments": fragments,
                        "num_partitions": partitions,
                    }
                )
        scalar_targets: list[tuple[str, str, str, dict[str, Any]]] = [
            *((column, "BTREE", scalar_index_name(column), {}) for column in config.scalar_columns),
            *((column, "BITMAP", bitmap_index_name(column), {}) for column in config.bitmap_columns),
            *((column, "INVERTED", fts_index_name(column), config.fts_params()) for column in config.text_columns),
        ]
        for column, index_type, name, params in scalar_targets:
            if name in existing and not config.rebuild:
                with telemetry.timed("index.build_ms", tags=[f"index:{name}"]):
                    merged = maintain_index_locally(uri, name, config, telemetry)
                indexes.append(maintained_stats(column, name, fragments, merged))
            else:
                with telemetry.timed("index.build_ms", tags=[f"index:{name}"]):
                    dataset.create_scalar_index(column, index_type, name=name, replace=True, **params)
                telemetry.incr("index.committed", tags=[f"index:{name}"])
                indexes.append({"column": column, "index": name, "segments": 1, "fragments": fragments})
    return {"uri": uri, "indexes": indexes, "tier": "small"}


class LanceIndexer:
    """Builds the configured indices on Lance datasets via per-type handlers."""

    def __init__(self, config: IndexJobConfig) -> None:
        """Initialize the indexer.

        Args:
            config: Indexing configuration.
        """
        self.config: IndexJobConfig = config

    def handlers(self) -> list[IndexHandler]:
        """Build the index handlers selected by the configuration.

        One :class:`VectorIndexHandler` is created for each column in :attr:`IndexJobConfig.vector_columns`.
        All vector handlers share the same metric, partition policy, and row floor but each gets its own column and
        derived index name from :func:`vector_index_name`.

        Returns:
            One handler per configured index column, in the order vector → scalar → bitmap → text.
        """
        config: IndexJobConfig = self.config
        result: list[IndexHandler] = []
        for column in config.vector_columns:
            result.append(VectorIndexHandler(config, column, vector_index_name(column)))
        for column in config.scalar_columns:
            result.append(BTreeIndexHandler(config, column, scalar_index_name(column)))
        for column in config.bitmap_columns:
            result.append(BitmapIndexHandler(config, column, bitmap_index_name(column)))
        for column in config.text_columns:
            result.append(FtsIndexHandler(config, column, fts_index_name(column)))
        return result

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build every configured index for one dataset.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset's indices.
        """
        indexes: list[dict[str, Any]] = []
        for handler in self.handlers():
            with telemetry.span("lance.indexing.index") as index_span:
                index_span.set_tag("index", handler.index_name)
                index_span.set_tag("index_type", handler.index_type())
                indexes.append(handler.build(spark, uri, telemetry))
        return {"uri": uri, "indexes": indexes}

    def classify(self, spark: SparkSession, dataset_uris: list[str]) -> tuple[list[str], list[str]]:
        """Split datasets into the small and large tiers by fragment count.

        Fragment counts are gathered with one distributed job so the driver never opens datasets itself.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to classify.

        Returns:
            The small-tier URIs and the large-tier URIs.
        """
        config: IndexJobConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options
        threshold: int = config.small_dataset_fragment_threshold

        def fragment_count(uri: str) -> tuple[str, int]:
            """Count one dataset's fragments on an executor.

            Args:
                uri: Dataset URI.

            Returns:
                The URI paired with its fragment count.
            """
            return uri, len(lance.dataset(uri, storage_options=storage_options).get_fragments())

        slices: int = max(1, min(config.small_tier_slices, len(dataset_uris)))
        counts: list[tuple[str, int]] = (
            spark.sparkContext.parallelize(dataset_uris, slices).map(fragment_count).collect()
        )
        small: list[str] = [uri for uri, count in counts if count < threshold]
        large: list[str] = [uri for uri, count in counts if count >= threshold]
        return small, large

    def run_small_tier(self, spark: SparkSession, uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Index many small datasets in one batched Spark job.

        Each executor task indexes one whole dataset end-to-end with plain non-distributed index builds. The driver only
        collects statistics.

        Args:
            spark: Active Spark session.
            uris: Small-tier dataset URIs.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset.
        """
        config: IndexJobConfig = self.config

        def index_one(uri: str) -> dict[str, Any]:
            """Index one whole dataset on an executor.

            Args:
                uri: Dataset URI.

            Returns:
                The dataset's statistics dictionary.
            """
            return index_dataset_locally(uri, config)

        slices: int = max(1, min(config.small_tier_slices, len(uris)))
        with telemetry.timed("tier.small_ms"):
            results: list[dict[str, Any]] = spark.sparkContext.parallelize(uris, slices).map(index_one).collect()
        telemetry.gauge("tier.small_datasets", len(results))
        return results

    def run_large_tier(self, spark: SparkSession, uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Index large datasets concurrently with the segment fan-out.

        Each dataset keeps its distributed per-segment build, but multiple datasets are driven concurrently from a
        driver thread pool. Every submission is tagged with the configured Spark FAIR scheduler pool so concurrent jobs
        share the cluster fairly. ``spark.scheduler.mode=FAIR`` must be set on the session for the pools to take effect.

        Args:
            spark: Active Spark session.
            uris: Large-tier dataset URIs.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset, in input order.
        """
        config: IndexJobConfig = self.config

        def index_one(uri: str) -> dict[str, Any]:
            """Drive one dataset's distributed build from a worker thread.

            Args:
                uri: Dataset URI.

            Returns:
                The dataset's statistics dictionary.
            """
            try:
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", config.scheduler_pool)
                with telemetry.timed("dataset.total_ms"):
                    stats: dict[str, Any] = self.build(spark, uri, telemetry)
            except Exception:
                telemetry.error(f"indexing failed for {uri}")
                raise
            finally:
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
            segment_total: int = sum(int(item["segments"]) for item in stats["indexes"])
            telemetry.gauge("dataset.segments", segment_total)
            logger.info("indexed %s: %d indices, %d segments", uri, len(stats["indexes"]), segment_total)
            stats["tier"] = "large"
            return stats

        workers: int = max(1, min(config.driver_concurrency, len(uris)))
        with telemetry.timed("tier.large_ms"), ThreadPoolExecutor(max_workers=workers) as pool:
            results: list[dict[str, Any]] = list(pool.map(index_one, uris))
        telemetry.gauge("tier.large_datasets", len(results))
        return results

    def run(self, spark: SparkSession, dataset_uris: list[str]) -> list[dict[str, Any]]:
        """Index every dataset with two-tier orchestration.

        Datasets are classified by fragment count: small datasets are batched into one Spark job where each executor
        task indexes a whole dataset, and large datasets keep the distributed segment fan-out, driven concurrently from
        the driver. Any failure propagates and fails the run.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per dataset, in input order.
        """
        driver_telemetry: Telemetry = Telemetry.create(self.config.telemetry)
        with driver_telemetry.span("lance.indexing.run") as run_span:
            run_span.set_tag("dataset_count", len(dataset_uris))
            if not dataset_uris:
                return []
            small, large = self.classify(spark, dataset_uris)
            run_span.set_tag("small_datasets", len(small))
            run_span.set_tag("large_datasets", len(large))
            stats_by_uri: dict[str, dict[str, Any]] = {}
            if small:
                for stats in self.run_small_tier(spark, small, driver_telemetry):
                    stats_by_uri[stats["uri"]] = stats
            if large:
                for stats in self.run_large_tier(spark, large, driver_telemetry):
                    stats_by_uri[stats["uri"]] = stats
            results: list[dict[str, Any]] = [stats_by_uri[uri] for uri in dataset_uris]
            driver_telemetry.gauge("run.datasets", len(results))
            logger.info("indexing run: %d datasets (%d small, %d large)", len(results), len(small), len(large))
            return results
