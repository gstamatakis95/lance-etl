"""Clustered rewrite: reorder a Lance dataset so same-centroid rows share fragments.

An operator occasionally wants to fully read and rewrite a Lance dataset so that rows assigned to
the same IVF centroid (vector-index partition) land contiguously, in the same fragment where
possible. Lance has no built-in clustered compaction, so the mechanism here is a manual distributed
pipeline: nearest-centroid assignment on the raw vector, a global sort by partition id, a
``write_fragments`` shuffle, a ``LanceOperation.Overwrite`` commit, then a vector-index rebuild that
preserves the old centroids through the segment API.

The flow mirrors the fleet-phase shape of :mod:`lance_etl.maintenance.job` and
:mod:`lance_etl.migrate_namespace`: the driver plans, broadcasts read-only artifacts, and commits,
while every row-level read and write runs inside an executor closure. The vector-index rebuild
reuses the exact segment-API artifact tuple :meth:`VectorIndexHandler.prepare` produces, with the
same centroids and the stored RaBitQ model, so no training happens on the rebuild path and
``rows_at_train`` stays unchanged (ADR 0041).

A clustered rewrite is a non-transactional maintenance-window operation. It subsumes normal
compaction for the datasets it touches, requires the targeted datasets to be quiesced, and is off
by default (:attr:`~lance_etl.maintenance.job.MaintenanceConfig.cluster_rewrite`).
"""

from __future__ import annotations

import json
import logging
import math
from typing import TYPE_CHECKING, Any

import lance
import numpy as np
import pyarrow as pa
from lance.fragment import FragmentMetadata, write_fragments
from pyspark.sql import SparkSession

import lance_etl.maintenance.job as maintenance_job
from lance_etl.column_roles import VECTOR_ROLE, load_column_roles
from lance_etl.etl.sink import DATA_STORAGE_VERSION
from lance_etl.fanout import (
    BUILD_PARTITION_FACTOR,
    FANOUT_PARTITION_FACTOR,
    FLAT_ERROR,
    FLAT_OK,
    REWRITE_PARTITION_FACTOR,
    derive_partitions,
    fan_out_per_dataset,
    run_flat_tagged_job,
)
from lance_etl.indexing.config import METRIC_TO_DISTANCE, IndexJobConfig, vector_index_name
from lance_etl.indexing.optimize import load_centroids, load_vector_config, save_centroids
from lance_etl.indexing.segments import (
    all_fragment_ids,
    build_vector_segment,
    commit_segments,
    serialize_segment,
    split_evenly,
)
from lance_etl.telemetry import DEFAULT_COMMIT_RETRIES, Telemetry, commit_with_retries

if TYPE_CHECKING:
    from datetime import datetime

    from lance_etl.maintenance.job import MaintenanceConfig

logger: logging.Logger = logging.getLogger(__name__)

CLUSTER_PARTITION_COLUMN: str = "lance_etl_cluster_partition"
"""Temporary int32 partition-id column added for the sort, dropped before the fragment write."""

CLUSTER_READ_SHARDS: int = 64
"""Default fragment shards per dataset for the histogram and rewrite read jobs."""

ASSIGN_BLOCK_ROWS: int = 65_536
"""Rows assigned per numpy block so the centroid distance matrix stays bounded in memory."""

SHUFFLE_CHUNK_BYTES: int = 32 * 1024 * 1024
"""Soft byte cap per Arrow IPC chunk emitted into the rewrite shuffle."""

COSINE_NORM_EPS: float = 1e-12
"""Floor on a vector or centroid norm before cosine normalisation, guarding the zero vector."""

REWRITE_ERROR_KEY: int = -1
"""Shuffle key marking a read-task failure sentinel routed through the rewrite shuffle. Real global
bucket ids start at 0, so this key never collides with a live bucket, and ``partitionBy`` hashes it
onto a valid partition where ``write_partition`` passes it straight through."""

REWRITE_OK: str = "ok"
"""Tag on a collected rewrite-shuffle result carrying a written fragment document."""

REWRITE_ERROR: str = "error"
"""Tag on a collected rewrite-shuffle result carrying a per-dataset failure message."""

CLUSTER_GENERATION_KEY: str = "lance-etl.cluster_generation"
"""Dataset config KV key recording the fingerprint of the last clustered generation, used to skip
re-clustering a dataset that has not been written to since it was clustered (ADR 0041). The
fingerprint is the sorted list of data-fragment ids plus the logical row count: any write that
replaces or adds fragments (append, merge rewrite, compaction) mints new fragment ids and any
delete lowers the row count, so either kind of change invalidates the match."""


def release_broadcast(handle: Any) -> None:
    """Release a Spark broadcast's driver and executor copies once its phase has collected.

    The three clustered-rewrite flat jobs (histogram, rewrite, index rebuild) each broadcast the
    per-dataset centroid bytes, so a fleet-wide run would otherwise pin three generations of the
    (potentially ~100 MB per dataset) centroid maps on the driver heap at once. Destroying each
    broadcast right after its job's ``collect`` returns bounds the driver footprint to one live
    generation. The in-process broadcast double used by the unit tests exposes no ``destroy``
    method, so the call is skipped when the handle lacks one rather than requiring a test-only
    shim on the production path.

    Args:
        handle: The Spark broadcast handle to release, or a test double without ``destroy``.
    """
    destroy = getattr(handle, "destroy", None)
    if destroy is not None:
        destroy()


def fragment_id_signature(dataset: lance.LanceDataset) -> list[int]:
    """Return a dataset's data-fragment ids as a sorted list, read from the open manifest.

    The sorted id list is the identity half of the clustered-generation fingerprint. Fragment ids
    are minted monotonically and never reused, so any write that adds, replaces, or compacts
    fragments changes this list even when the fragment count is preserved (a count-preserving merge
    rewrite drops one fragment and mints one new id). Reading ``get_fragments`` costs no additional
    object-store I/O over the already-open manifest.

    Args:
        dataset: The open dataset to inspect.

    Returns:
        The dataset's fragment ids in ascending order.
    """
    return sorted(fragment.fragment_id for fragment in dataset.get_fragments())


def load_cluster_generation(dataset: lance.LanceDataset) -> dict[str, Any] | None:
    """Read the stored clustered-generation fingerprint from a dataset's config KV.

    The config KV is already in-memory from the open manifest, so this performs no additional
    object-store I/O. Returns ``None`` when the key is absent or its value cannot be parsed. An
    older-format value that lacks ``fragment_ids`` parses to an empty signature, which no live
    dataset matches, so it simply re-enables clustering once rather than raising.

    Args:
        dataset: The open dataset whose config to read.

    Returns:
        The parsed ``{"fragment_ids", "num_rows"}`` fingerprint, or ``None`` when absent or
        malformed.
    """
    raw: str | None = dataset.config().get(CLUSTER_GENERATION_KEY)
    if raw is None:
        return None
    try:
        parsed: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.warning("cluster: malformed cluster generation config on %s; treating as absent", dataset.uri)
        return None
    return {
        "fragment_ids": [int(value) for value in parsed.get("fragment_ids", [])],
        "num_rows": int(parsed.get("num_rows", -1)),
    }


def cluster_generation_skip_reason(dataset: lance.LanceDataset) -> str | None:
    """Return a skip reason when a dataset was already clustered and has not been written since.

    The clustered-generation fingerprint (the sorted fragment-id list and the logical row count) is
    stamped at overwrite-commit time and survives the later index-rebuild and cleanup commits,
    which add no data fragments and change no logical row count. The two halves catch disjoint
    kinds of write: a fragment-replacing write (append, merge rewrite, compaction) mints new
    fragment ids so the id list diverges even when the count is unchanged, while a pure delete
    lowers the row count without touching the ids. Both reads come from the already-open manifest,
    so the check costs no object-store I/O. Retention is deliberately not run before this check, so on a
    clustered fleet with retention active an idle already-clustered dataset defers time-based expiry
    until a write re-enables it.

    Args:
        dataset: The open dataset to inspect.

    Returns:
        A human-readable skip reason when the dataset is already clustered and unchanged, or
        ``None`` when it must be (re-)clustered.
    """
    stored: dict[str, Any] | None = load_cluster_generation(dataset)
    if stored is None:
        return None
    current_ids: list[int] = fragment_id_signature(dataset)
    current_rows: int = dataset.count_rows()
    if stored["fragment_ids"] == current_ids and stored["num_rows"] == current_rows:
        return f"already clustered at {len(current_ids)} fragments and {current_rows} rows; no writes since"
    return None


def stamp_cluster_generation(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> None:
    """Stamp the clustered-generation fingerprint into a dataset's config KV.

    Mirrors :func:`~lance_etl.indexing.optimize.write_vector_config`: the ``update_config`` write
    surfaces conflicts as ``OSError`` through the pyo3 binding, so it is wrapped in
    :func:`~lance_etl.telemetry.commit_with_retries`, which re-opens the dataset at the latest
    version before each attempt. The fingerprint (the freshly clustered generation's sorted
    fragment-id list and logical row count) is read from the same re-opened handle each attempt, so
    it always reflects the version it is stamped onto, and it lets the next clustered-rewrite run
    skip a dataset that has not been written to since it was clustered (ADR 0041).

    Args:
        uri: Dataset URI.
        config: Maintenance configuration supplying the retry budget and backoff.
        telemetry: Telemetry facade for the current process.
    """

    def action() -> None:
        """Capture the fingerprint at the latest dataset version and write it into the config KV."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        payload: str = json.dumps({"fragment_ids": fragment_id_signature(dataset), "num_rows": dataset.count_rows()})
        dataset.update_config({CLUSTER_GENERATION_KEY: payload})

    commit_with_retries(
        action,
        config.commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("cluster.generation_stamp_conflict"),
    )
    telemetry.incr("cluster.generation_stamped")


def encode_centroids(centroids: pa.Array) -> bytes:
    """Serialise an IVF centroid array to Arrow IPC bytes for broadcast.

    A bare :class:`pyarrow.Array` cannot be IPC-framed on its own, so the centroids are wrapped in a
    single-column record batch and written through the Arrow stream writer. The round trip through
    :func:`decode_centroids` preserves the fixed-size-list type and every centroid value exactly.

    Args:
        centroids: The IVF centroid array, a fixed-size-list array of one centroid per partition.

    Returns:
        The Arrow IPC stream bytes carrying the single ``centroids`` column.
    """
    batch: pa.RecordBatch = pa.RecordBatch.from_arrays([centroids], names=["centroids"])
    sink: pa.BufferOutputStream = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def decode_centroids(data: bytes) -> pa.Array:
    """Reconstruct the IVF centroid array from its Arrow IPC bytes.

    Args:
        data: The bytes produced by :func:`encode_centroids`.

    Returns:
        The centroid array in the same fixed-size-list type it was encoded from.
    """
    with pa.ipc.open_stream(pa.BufferReader(data)) as reader:
        table: pa.Table = reader.read_all()
    return table.column("centroids").combine_chunks()


def table_to_ipc(table: pa.Table) -> bytes:
    """Serialise an Arrow table to IPC stream bytes for the rewrite shuffle.

    Args:
        table: The table slice to serialise.

    Returns:
        The Arrow IPC stream bytes carrying every batch of the table.
    """
    sink: pa.BufferOutputStream = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def table_from_ipc(data: bytes) -> pa.Table:
    """Reconstruct an Arrow table from its IPC stream bytes.

    Args:
        data: The bytes produced by :func:`table_to_ipc`.

    Returns:
        The reconstructed table.
    """
    with pa.ipc.open_stream(pa.BufferReader(data)) as reader:
        return reader.read_all()


def centroids_to_matrix(centroids: pa.Array) -> np.ndarray:
    """Convert an IVF centroid fixed-size-list array to a 2-D float32 numpy matrix.

    Args:
        centroids: The centroid array, one fixed-size-list value per partition.

    Returns:
        A ``(num_partitions, dimension)`` float32 matrix.
    """
    dimension: int = centroids.type.list_size
    flat: np.ndarray = centroids.values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    return flat.reshape(len(centroids), dimension)


def normalise_rows(matrix: np.ndarray) -> np.ndarray:
    """L2-normalise every row of a matrix with a zero-vector eps guard.

    Args:
        matrix: A ``(rows, dimension)`` float32 matrix.

    Returns:
        The row-normalised matrix; a zero row stays zero because its norm is floored at
        :data:`COSINE_NORM_EPS` rather than producing a division by zero.
    """
    norms: np.ndarray = np.sqrt(np.einsum("ij,ij->i", matrix, matrix))
    norms = np.maximum(norms, COSINE_NORM_EPS)
    return matrix / norms[:, None]


def assign_block(
    block: np.ndarray,
    centroids: np.ndarray,
    normalised_centroids: np.ndarray,
    centroid_norms_sq: np.ndarray,
    distance_type: str,
) -> np.ndarray:
    """Assign one block of vectors to their nearest centroid.

    Args:
        block: A ``(rows, dimension)`` float32 block of vectors.
        centroids: The ``(num_partitions, dimension)`` centroid matrix.
        normalised_centroids: The row-normalised centroid matrix, precomputed for cosine.
        centroid_norms_sq: The per-centroid squared norms, precomputed for l2.
        distance_type: The Lance distance type, one of ``l2``, ``cosine``, or ``dot``.

    Returns:
        A ``(rows,)`` int64 array of partition ids for this block.
    """
    if distance_type == "l2":
        scores: np.ndarray = block @ centroids.T
        distances: np.ndarray = centroid_norms_sq[None, :] - 2.0 * scores
        return np.argmin(distances, axis=1).astype(np.int64)
    if distance_type == "cosine":
        similarities: np.ndarray = normalise_rows(block) @ normalised_centroids.T
        return np.argmax(similarities, axis=1).astype(np.int64)
    dots: np.ndarray = block @ centroids.T
    return np.argmax(dots, axis=1).astype(np.int64)


def assign_partition_ids(vectors: pa.FixedSizeListArray, centroids: np.ndarray, distance_type: str) -> np.ndarray:
    """Assign each vector to its nearest IVF centroid under the index distance type.

    This is the pure, executor-side assigner that reproduces Lance's own IVF partition assignment:
    nearest centroid on the raw vector, with the RaBitQ rotation applying only to residuals after
    assignment. The work runs in blocks of :data:`ASSIGN_BLOCK_ROWS` rows so the row-by-centroid
    distance matrix stays bounded regardless of partition count. ``l2`` minimises the reduced
    squared distance ``-2 x C^T + ||C||^2`` (the per-row ``||x||^2`` term is constant and dropped),
    ``cosine`` normalises both sides with a zero-vector eps guard and maximises the dot product, and
    ``dot`` maximises the raw dot product.

    Null vector rows are not special-cased here: their underlying buffer values yield some
    argmin/argmax that the caller overwrites with the tail partition id. Only assign on
    freshly-scanned batches, since the numpy view ignores any array slice offset.

    Args:
        vectors: The vector column as a fixed-size-list array.
        centroids: The ``(num_partitions, dimension)`` centroid matrix.
        distance_type: The Lance distance type, one of ``l2``, ``cosine``, or ``dot``.

    Returns:
        A ``(len(vectors),)`` int64 array of partition ids in ``range(num_partitions)``.

    Raises:
        ValueError: If ``distance_type`` is not one of the three supported metrics.
    """
    if distance_type not in ("l2", "cosine", "dot"):
        raise ValueError(f"unsupported distance_type {distance_type!r}; expected l2, cosine, or dot")
    dimension: int = vectors.type.list_size
    matrix: np.ndarray = vectors.values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    matrix = matrix.reshape(len(vectors), dimension)
    centroid_matrix: np.ndarray = centroids.astype(np.float32, copy=False)
    centroid_norms_sq: np.ndarray = np.einsum("ij,ij->i", centroid_matrix, centroid_matrix)
    normalised_centroids: np.ndarray = normalise_rows(centroid_matrix)
    out: np.ndarray = np.empty(len(vectors), dtype=np.int64)
    for start in range(0, len(vectors), ASSIGN_BLOCK_ROWS):
        block: np.ndarray = matrix[start : start + ASSIGN_BLOCK_ROWS]
        out[start : start + ASSIGN_BLOCK_ROWS] = assign_block(
            block, centroid_matrix, normalised_centroids, centroid_norms_sq, distance_type
        )
    return out


def partition_ids_for_batch(
    vectors: pa.FixedSizeListArray, centroids: np.ndarray, distance_type: str, num_partitions: int
) -> np.ndarray:
    """Assign partition ids for one batch, routing null vectors to the tail partition.

    Args:
        vectors: The vector column of one scanned batch as a fixed-size-list array.
        centroids: The ``(num_partitions, dimension)`` centroid matrix.
        distance_type: The Lance distance type.
        num_partitions: The IVF partition count; the tail (null) partition id equals this value.

    Returns:
        A ``(len(vectors),)`` int64 array of partition ids in ``range(num_partitions + 1)``, where
        ``num_partitions`` marks a null or missing vector.
    """
    pids: np.ndarray = assign_partition_ids(vectors, centroids, distance_type)
    if vectors.null_count:
        null_mask: np.ndarray = np.asarray(vectors.is_null())
        pids = np.where(null_mask, num_partitions, pids)
    return pids


def derive_buckets(counts: list[int], rows_per_task: int) -> list[tuple[int, int, int, int]]:
    """Pack a partition-id histogram into bounded contiguous write buckets.

    Contiguous partition ids are packed into buckets whose total row count stays at or below
    ``rows_per_task``, so a global sort by partition id follows bucket order and at most one centroid
    straddles a fragment boundary. A single partition larger than the cap cannot be packed with its
    neighbours, so it splits into ``ceil(count / rows_per_task)`` salted sub-buckets: rows within one
    centroid need no internal order, so they fan out across sub-buckets to bound every write task's
    memory regardless of centroid skew (mirrors the ETL salted shuffle). The last histogram slot is
    the null tail partition and is packed uniformly with the rest, so it always appears in a bucket
    when it carries rows.

    Args:
        counts: Per-partition row counts, length ``num_partitions + 1`` with the null tail last.
        rows_per_task: The row cap per bucket and per rewritten fragment.

    Returns:
        A list of ``(start_pid, end_pid, salt, num_salts)`` tuples in ascending partition order. A
        contiguous range bucket carries ``salt=0`` and ``num_salts=1``. A salted oversized partition
        emits one tuple per sub-bucket with ``start_pid == end_pid`` and ``salt`` in
        ``range(num_salts)``.

    Raises:
        ValueError: If ``rows_per_task`` is not positive.
    """
    if rows_per_task <= 0:
        raise ValueError(f"rows_per_task must be positive, got {rows_per_task}")
    buckets: list[tuple[int, int, int, int]] = []
    index: int = 0
    total_pids: int = len(counts)
    while index < total_pids:
        count: int = counts[index]
        if count == 0:
            index += 1
            continue
        if count > rows_per_task:
            num_salts: int = math.ceil(count / rows_per_task)
            buckets.extend((index, index, salt, num_salts) for salt in range(num_salts))
            index += 1
            continue
        low: int = index
        running: int = 0
        while index < total_pids and counts[index] <= rows_per_task and running + counts[index] <= rows_per_task:
            running += counts[index]
            index += 1
        buckets.append((low, index - 1, 0, 1))
    return buckets


def resolve_cluster_column(dataset: lance.LanceDataset, config: MaintenanceConfig) -> tuple[str | None, str | None]:
    """Resolve the vector column to cluster on, from config or the single vector-role column.

    Args:
        dataset: The open dataset whose column roles are consulted when no explicit column is set.
        config: Maintenance configuration carrying the optional ``cluster_column`` override.

    Returns:
        A ``(column, skip_reason)`` pair. Exactly one is non-``None``: the resolved column name, or
        a skip reason when zero or more than one vector-role column exists without an override.
    """
    if config.cluster_column is not None:
        return config.cluster_column, None
    roles: dict[str, str] = load_column_roles(dataset)
    vector_columns: list[str] = sorted(name for name, role in roles.items() if role == VECTOR_ROLE)
    if not vector_columns:
        return None, "no vector-role column; set cluster_column to cluster this dataset"
    if len(vector_columns) > 1:
        return None, f"ambiguous vector columns {vector_columns}; set cluster_column to disambiguate"
    return vector_columns[0], None


def cluster_guard_reason(dataset: lance.LanceDataset, column: str, index_name: str) -> str | None:
    """Return a skip reason when the dataset is ineligible for a clustered rewrite.

    Args:
        dataset: The open dataset to inspect.
        column: The resolved vector column.
        index_name: The vector index name expected on that column.

    Returns:
        A human-readable skip reason, or ``None`` when the dataset is eligible.
    """
    committed: set[str] = {description.name for description in dataset.describe_indices()}
    if index_name not in committed:
        return f"no vector index {index_name!r}; nothing to cluster against"
    cfg: dict[str, Any] | None = load_vector_config(dataset, column)
    if cfg is None:
        return f"no stored vector config for column {column!r}; cannot reuse centroids"
    rows_at_train: Any = cfg.get("rows_at_train")
    if not isinstance(rows_at_train, int) or rows_at_train <= 0:
        return f"vector config for {column!r} has no positive rows_at_train; cannot key centroids"
    if not cfg.get("rabitq_model"):
        return f"vector config for {column!r} has no rabitq_model; cannot rebuild the index"
    if dataset.count_rows() == 0:
        return "empty dataset; nothing to cluster"
    return None


def resolve_cluster_centroids(
    dataset: lance.LanceDataset,
    uri: str,
    index_name: str,
    rows_at_train: int,
    metric: str,
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> pa.Array:
    """Resolve the reusable IVF centroids sidecar-first, backfilling the sidecar on a miss.

    Reads the centroids from the object-store sidecar keyed by ``rows_at_train`` and falls back to
    the committed index via :meth:`lance.LanceDataset.get_ivf_model`, backfilling the sidecar
    best-effort so a manual re-drive after the index is dropped still finds them (ADR 0041).

    Args:
        dataset: The open dataset whose committed index carries the centroids.
        uri: Dataset URI, for the sidecar path.
        index_name: The vector index name.
        rows_at_train: The staleness fingerprint keying the sidecar generation.
        metric: The distance metric stored as the sidecar model's distance type.
        config: Maintenance configuration supplying the object-store options.
        telemetry: Telemetry facade for the current process.

    Returns:
        The IVF centroid ``pa.Array``.

    Raises:
        RuntimeError: If neither the sidecar nor the committed index yields centroids.
    """
    cached: pa.Array | None = load_centroids(uri, index_name, rows_at_train, config.storage_options)
    if cached is not None:
        return cached
    ivf_model = dataset.get_ivf_model(index_name)
    if ivf_model is None or ivf_model.centroids is None:
        raise RuntimeError(f"vector index {index_name!r} on {uri} carries no reusable centroids")
    centroids: pa.Array = ivf_model.centroids
    try:
        save_centroids(uri, index_name, centroids, metric, rows_at_train, config.storage_options)
        telemetry.incr("cluster.centroid_sidecar_backfilled")
    except Exception as exc:
        telemetry.incr("cluster.centroid_sidecar_write_error")
        logger.warning("cluster: centroid sidecar backfill failed for %s on %s: %s", index_name, uri, exc)
    return centroids


def cluster_rows_per_task(config: MaintenanceConfig) -> int:
    """Return the row cap per rewritten fragment and per write task.

    Args:
        config: Maintenance configuration.

    Returns:
        ``target_rows_per_fragment``.
    """
    return config.target_rows_per_fragment


def plan_cluster_rewrite(
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime | None,
    telemetry: Telemetry,
    cleanup_slot: int | None = None,
) -> dict[str, Any]:
    """Plan one dataset's clustered rewrite on an executor (phase ``cluster-plan``).

    Resolves the vector column and guards eligibility, runs retention now so expired rows are never
    rewritten, resolves the reusable centroids sidecar-first, and pins the post-retention read version
    with its schema, row count, and fragment shards. A dataset already clustered and unwritten
    since (:func:`cluster_generation_skip_reason`) returns a terminal ``cluster_current`` dict so
    it is neither re-clustered nor routed into normal compaction, which would undo its centroid
    ordering. It is not fully skipped, though: it first runs the same rotation-gated idle version
    cleanup the normal compaction-skip path uses (:func:`~lance_etl.maintenance.job.idle_cleanup_bytes`),
    so the pre-rewrite generation left by the Overwrite is reclaimed on a later run once it ages
    past the cleanup horizon and its pinning tags are gone. An otherwise-ineligible dataset returns
    a ``cluster_skipped`` dict so the orchestrator routes it back into normal maintenance.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        cutoff: retention cutoff instant, or ``None`` to skip the retention step.
        telemetry: Telemetry facade for the current executor process.
        cleanup_slot: The active fleet-wide rotation slot for this run, threaded into the
            already-clustered idle cleanup, or ``None`` to always clean (the pre-rotation behavior
            direct callers rely on).

    Returns:
        A plan dict carrying every artifact the histogram and rewrite phases need, a ``{"uri",
        "cluster_current", "bytes_removed"}`` dict when the dataset is already clustered and
        unchanged, or a ``{"uri", "cluster_skipped"}`` dict when the dataset is otherwise
        ineligible.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    current: str | None = cluster_generation_skip_reason(dataset)
    if current is not None:
        telemetry.incr("cluster.skipped_already_clustered")
        bytes_removed: int = maintenance_job.idle_cleanup_bytes(uri, config, telemetry, dataset, False, cleanup_slot)
        return {"uri": uri, "cluster_current": current, "bytes_removed": bytes_removed}
    column, skip = resolve_cluster_column(dataset, config)
    if column is None:
        return {"uri": uri, "cluster_skipped": skip}
    index_name: str = vector_index_name(column)
    guard: str | None = cluster_guard_reason(dataset, column, index_name)
    if guard is not None:
        return {"uri": uri, "cluster_skipped": guard}
    cfg: dict[str, Any] | None = load_vector_config(dataset, column)
    if cfg is None:
        return {"uri": uri, "cluster_skipped": "vector config vanished after the guard check"}

    retention_rows_deleted: int = 0
    if config.retention_active() and cutoff is not None:
        retention_result: dict[str, Any] = maintenance_job.run_retention_on_open_dataset(
            dataset, uri, config, cutoff, telemetry
        )
        retention_rows_deleted = int(retention_result.get("retention_rows_deleted", 0))
        if retention_rows_deleted > 0:
            dataset = lance.dataset(uri, storage_options=config.storage_options)

    rows_at_train: int = int(cfg["rows_at_train"])
    metric: str = str(cfg["metric"])
    distance_type: str = METRIC_TO_DISTANCE.get(metric.lower(), "l2")
    centroids: pa.Array = resolve_cluster_centroids(dataset, uri, index_name, rows_at_train, metric, config, telemetry)
    fragment_ids: list[int] = all_fragment_ids(dataset)
    telemetry.incr("cluster.planned")
    return {
        "uri": uri,
        "column": column,
        "index_name": index_name,
        "metric": metric,
        "distance_type": distance_type,
        "num_bits": int(cfg["num_bits"]),
        "rabitq_model": str(cfg["rabitq_model"]),
        "num_partitions": len(centroids),
        "rows_at_train": rows_at_train,
        "read_version": dataset.version,
        "total_rows": dataset.count_rows(),
        "schema": dataset.schema,
        "shards": split_evenly(fragment_ids, CLUSTER_READ_SHARDS),
        "centroids_ipc": encode_centroids(centroids),
        "retention_rows_deleted": retention_rows_deleted,
    }


def read_shard_tasks(plans: list[dict[str, Any]]) -> list[tuple[str, int, list[int], str, str]]:
    """Flatten eligible plans into per-shard read tasks shared by the histogram and rewrite scans.

    Both the histogram scan and the rewrite scan read the same fragment shards at the same pinned
    version, so they share one task shape: one ``(uri, read_version, shard, column, distance_type)``
    tuple per fragment shard of every eligible dataset. Keeping the flattening in one place stops the
    two phases from drifting apart.

    Args:
        plans: The eligible plan dicts.

    Returns:
        One read task per fragment shard across every eligible dataset.
    """
    return [
        (plan["uri"], plan["read_version"], shard, plan["column"], plan["distance_type"])
        for plan in plans
        for shard in plan["shards"]
    ]


def partition_histogram(
    uri: str,
    version: int,
    shard: list[int],
    column: str,
    centroids_ipc: bytes,
    distance_type: str,
    storage_options: dict[str, Any] | None,
) -> list[int]:
    """Count rows per partition over one fragment shard (phase ``cluster-histogram``).

    Scans only the vector column at the pinned version, assigns each batch, and accumulates a
    ``num_partitions + 1`` histogram whose last slot counts null-vector rows.

    Args:
        uri: Dataset URI.
        version: The pinned read version every shard reads.
        shard: The fragment ids assigned to this task.
        column: The vector column to assign on.
        centroids_ipc: The broadcast centroid array as IPC bytes.
        distance_type: The Lance distance type.
        storage_options: Object-store options forwarded to lance.

    Returns:
        The per-partition row counts as a list of length ``num_partitions + 1``.
    """
    centroids: np.ndarray = centroids_to_matrix(decode_centroids(centroids_ipc))
    num_partitions: int = centroids.shape[0]
    dataset: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=storage_options)
    wanted: set[int] = set(shard)
    fragments: list[Any] = [fragment for fragment in dataset.get_fragments() if fragment.fragment_id in wanted]
    totals: np.ndarray = np.zeros(num_partitions + 1, dtype=np.int64)
    reader: pa.RecordBatchReader = dataset.scanner(columns=[column], fragments=fragments).to_reader()
    for batch in reader:
        pids: np.ndarray = partition_ids_for_batch(batch.column(column), centroids, distance_type, num_partitions)
        totals += np.bincount(pids, minlength=num_partitions + 1)
    return totals.tolist()


def build_bucket_lookup(
    global_buckets: list[tuple[int, int, int, int, int]], num_partitions: int
) -> tuple[np.ndarray, dict[int, np.ndarray]]:
    """Build the per-partition lookup tables the rewrite read side uses to route rows.

    Args:
        global_buckets: ``(global_id, start_pid, end_pid, salt, num_salts)`` tuples for one dataset.
        num_partitions: The IVF partition count; the tail partition id is ``num_partitions``.

    Returns:
        A ``(pid_to_global, salted)`` pair. ``pid_to_global`` maps every non-salted partition id to
        its global bucket id (``-1`` where salted). ``salted`` maps a salted partition id to the
        array of its sub-bucket global ids indexed by salt.
    """
    pid_to_global: np.ndarray = np.full(num_partitions + 1, -1, dtype=np.int64)
    salted: dict[int, np.ndarray] = {}
    for global_id, start_pid, end_pid, salt, num_salts in global_buckets:
        if num_salts == 1:
            pid_to_global[start_pid : end_pid + 1] = global_id
        else:
            row: np.ndarray = salted.setdefault(start_pid, np.full(num_salts, -1, dtype=np.int64))
            row[salt] = global_id
    return pid_to_global, salted


def assign_global_buckets(pids: np.ndarray, pid_to_global: np.ndarray, salted: dict[int, np.ndarray]) -> np.ndarray:
    """Map each row's partition id to its global bucket id, salting oversized partitions evenly.

    Args:
        pids: The per-row partition ids for one batch.
        pid_to_global: The non-salted partition-to-global lookup from :func:`build_bucket_lookup`.
        salted: The salted partition sub-bucket lookup from :func:`build_bucket_lookup`.

    Returns:
        A ``(len(pids),)`` int64 array of global bucket ids.
    """
    globals_out: np.ndarray = pid_to_global[pids]
    for pid, sub_buckets in salted.items():
        mask: np.ndarray = pids == pid
        selected: int = int(mask.sum())
        if selected:
            salt: np.ndarray = np.arange(selected) % len(sub_buckets)
            globals_out[mask] = sub_buckets[salt]
    return globals_out


def read_rewrite_chunks(
    uri: str,
    version: int,
    shard: list[int],
    column: str,
    centroids_ipc: bytes,
    distance_type: str,
    global_buckets: list[tuple[int, int, int, int, int]],
    storage_options: dict[str, Any] | None,
) -> list[tuple[int, bytes]]:
    """Read a fragment shard's full rows and emit them keyed by global bucket (phase ``cluster-rewrite``).

    Each batch is assigned, tagged with the temporary partition-id column, and split by global
    bucket. Per-bucket buffers flush to Arrow IPC once they exceed :data:`SHUFFLE_CHUNK_BYTES`, and
    any remainder flushes at the end, so a shard's memory is bounded by the chunk cap.

    Args:
        uri: Dataset URI.
        version: The pinned read version every shard reads.
        shard: The fragment ids assigned to this task.
        column: The vector column to assign on.
        centroids_ipc: The broadcast centroid array as IPC bytes.
        distance_type: The Lance distance type.
        global_buckets: ``(global_id, start_pid, end_pid, salt, num_salts)`` tuples for this dataset.
        storage_options: Object-store options forwarded to lance.

    Returns:
        ``(global_bucket, ipc_bytes)`` pairs carrying the tagged rows for the shuffle.
    """
    centroids: np.ndarray = centroids_to_matrix(decode_centroids(centroids_ipc))
    num_partitions: int = centroids.shape[0]
    pid_to_global, salted = build_bucket_lookup(global_buckets, num_partitions)
    dataset: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=storage_options)
    wanted: set[int] = set(shard)
    fragments: list[Any] = [fragment for fragment in dataset.get_fragments() if fragment.fragment_id in wanted]
    buffers: dict[int, list[pa.Table]] = {}
    sizes: dict[int, int] = {}
    output: list[tuple[int, bytes]] = []
    reader: pa.RecordBatchReader = dataset.scanner(fragments=fragments).to_reader()
    for batch in reader:
        pids: np.ndarray = partition_ids_for_batch(batch.column(column), centroids, distance_type, num_partitions)
        tagged: pa.Table = pa.Table.from_batches([batch]).append_column(
            CLUSTER_PARTITION_COLUMN, pa.array(pids.astype(np.int32), pa.int32())
        )
        globals_out: np.ndarray = assign_global_buckets(pids, pid_to_global, salted)
        flush_batch_into_buckets(tagged, globals_out, buffers, sizes, output)
    for global_id, tables in buffers.items():
        output.append((global_id, table_to_ipc(pa.concat_tables(tables))))
    return output


def flush_batch_into_buckets(
    tagged: pa.Table,
    globals_out: np.ndarray,
    buffers: dict[int, list[pa.Table]],
    sizes: dict[int, int],
    output: list[tuple[int, bytes]],
) -> None:
    """Split one tagged batch by global bucket into buffers, flushing oversized buckets.

    Args:
        tagged: The batch as a table already carrying the partition-id column.
        globals_out: The per-row global bucket ids for the batch.
        buffers: Per-bucket accumulated table slices, mutated in place.
        sizes: Per-bucket accumulated byte sizes, mutated in place.
        output: The emitted ``(global_bucket, ipc_bytes)`` pairs, appended to in place.
    """
    for global_id in np.unique(globals_out):
        mask: np.ndarray = globals_out == global_id
        slice_table: pa.Table = tagged.filter(pa.array(mask))
        key: int = int(global_id)
        buffers.setdefault(key, []).append(slice_table)
        sizes[key] = sizes.get(key, 0) + slice_table.nbytes
        if sizes[key] >= SHUFFLE_CHUNK_BYTES:
            output.append((key, table_to_ipc(pa.concat_tables(buffers.pop(key)))))
            sizes[key] = 0


def write_bucket(
    global_bucket: int,
    chunks: list[bytes],
    uri: str,
    schema: pa.Schema,
    rows_per_task: int,
    storage_options: dict[str, Any] | None,
) -> list[tuple[int, int, str, int]]:
    """Sort one global bucket's rows by partition id and write them as new fragment files.

    Concatenates the shuffled chunks, sorts by the temporary partition-id column so same-centroid
    rows stay contiguous, drops that column, and writes fragments capped at ``rows_per_task`` rows.

    Unlike the fresh-namespace copy in :mod:`lance_etl.migrate_namespace`, ``uri`` here is the
    dataset actually being rewritten, so it already exists. ``write_fragments`` rejects
    ``mode="create"`` against an existing dataset directory with ``Error::dataset_already_exists``
    (lance validates ``WriteMode::Create`` only against a destination with no committed dataset
    yet). ``mode="overwrite"`` assigns the same fresh field ids as ``"create"`` — required so the
    written fragments carry the pinned schema's ids for the coming
    :func:`~lance_etl.maintenance.job` ``LanceOperation.Overwrite`` commit — while being accepted
    against an existing destination; it commits nothing by itself, since ``write_fragments`` only
    ever returns uncommitted fragment metadata for the driver to commit later.

    Args:
        global_bucket: The global bucket id, for stable ordering on the driver.
        chunks: The Arrow IPC chunks routed to this bucket.
        uri: Target dataset URI the fragment files are written under.
        schema: The pinned dataset schema the fragments are created with.
        rows_per_task: Row cap per written fragment file.
        storage_options: Object-store options forwarded to lance.

    Returns:
        One ``(global_bucket, seq, fragment_json, rows)`` tuple per written fragment.
    """
    combined: pa.Table = pa.concat_tables([table_from_ipc(chunk) for chunk in chunks])
    ordered: pa.Table = combined.sort_by(CLUSTER_PARTITION_COLUMN).select(schema.names)
    reader: pa.RecordBatchReader = ordered.to_reader()
    metadatas: list[FragmentMetadata] = write_fragments(
        reader,
        uri,
        schema=schema,
        mode="overwrite",
        max_rows_per_file=rows_per_task,
        storage_options=storage_options,
        data_storage_version=DATA_STORAGE_VERSION,
    )
    return [
        (global_bucket, seq, json.dumps(metadata.to_json()), metadata.physical_rows)
        for seq, metadata in enumerate(metadatas)
    ]


def commit_cluster_overwrite(
    uri: str,
    fragment_documents: list[str],
    schema: pa.Schema,
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> int:
    """Commit the rewritten fragments over a dataset with ``LanceOperation.Overwrite``.

    The overwrite preserves version history, tags, and the dataset config KV (column roles and the
    vector config survive), and drops every index so the rebuild phase can re-create the vector
    index. A concurrent write between the pinned read version and this commit is clobbered, which is
    why a clustered rewrite requires the dataset quiesced.

    Immediately after the overwrite commits, the clustered-generation fingerprint (the sorted
    fragment-id list and logical row count read back from the committed version) is stamped into
    the config KV via :func:`stamp_cluster_generation`, so the next scheduled clustered-rewrite run
    skips this dataset unless it is written to in the meantime. The stamp is written before the
    later index rebuild deliberately: a rebuild that fails still leaves the data clustered, and the
    fingerprint keeps the next run from wastefully re-clustering it (the indexing job repairs the
    missing index instead). The index rebuild adds only index segments, not data fragments, so it
    leaves the stamped fingerprint matching.

    Args:
        uri: Dataset URI.
        fragment_documents: JSON fragment metadata collected from the rewrite shuffle.
        schema: The pinned dataset schema the new version is created with.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        The number of fragments committed.
    """
    fragments: list[FragmentMetadata] = [FragmentMetadata.from_json(document) for document in fragment_documents]

    def action() -> None:
        """Commit the overwrite operation against the target."""
        operation = lance.LanceOperation.Overwrite(schema, fragments)
        lance.LanceDataset.commit(uri, operation, storage_options=config.storage_options, enable_v2_manifest_paths=True)
        telemetry.incr("cluster.overwritten")

    commit_with_retries(
        action,
        config.large_commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("cluster.overwrite_conflict"),
    )
    stamp_cluster_generation(uri, config, telemetry)
    return len(fragments)


def cluster_index_config(config: MaintenanceConfig, num_partitions: int, metric: str) -> IndexJobConfig:
    """Build a minimal indexing configuration for the vector-index rebuild commit.

    ``cluster.py`` constructs its own :class:`IndexJobConfig` rather than importing one from the
    indexing job, so indexing never imports maintenance and no cycle forms. Only the fields
    :func:`~lance_etl.indexing.segments.commit_segments` and the segment build read are set.

    The index-rebuild commit uses the standard index-path retry budget
    (:data:`~lance_etl.telemetry.DEFAULT_COMMIT_RETRIES`), NOT the small ``large_commit_retries``
    budget the Overwrite uses: a segment index commit is an ordinary index commit whose conflicts a
    retry can resolve, whereas ``large_commit_retries`` is reserved for the genuinely large
    Overwrite manifest write.

    Args:
        config: Maintenance configuration supplying telemetry, storage, and retry budgets.
        num_partitions: The IVF partition count carried by the preserved centroids.
        metric: The distance metric from the stored config.

    Returns:
        An indexing configuration wired for the segment-path rebuild.
    """
    return IndexJobConfig(
        telemetry=config.telemetry,
        storage_options=config.storage_options,
        num_partitions=num_partitions,
        metric=metric,
        commit_retries=DEFAULT_COMMIT_RETRIES,
        commit_backoff_seconds=config.commit_backoff_seconds,
    )


def build_cluster_index_segment(
    uri: str,
    version: int,
    shard: list[int],
    column: str,
    index_name: str,
    metric: str,
    num_bits: int,
    num_partitions: int,
    centroids_ipc: bytes,
    rabitq_model: str,
    storage_options: dict[str, Any] | None,
) -> str:
    """Build one IVF_RQ segment over a shard of the rewritten dataset with preserved centroids.

    Reuses the exact segment-API artifact tuple :meth:`VectorIndexHandler.prepare` produces, so the
    rebuild carries the SAME centroids and the STORED RaBitQ model and never retrains.

    Args:
        uri: Dataset URI.
        version: The committed post-rewrite version to build against.
        shard: The fresh fragment ids assigned to this task.
        column: The vector column.
        index_name: The vector index name.
        metric: The distance metric.
        num_bits: The RaBitQ bits per sub-dimension.
        num_partitions: The IVF partition count.
        centroids_ipc: The broadcast centroid array as IPC bytes.
        rabitq_model: The stored RaBitQ model JSON string.
        storage_options: Object-store options forwarded to lance.

    Returns:
        The serialised uncommitted segment document.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=storage_options)
    centroids: pa.Array = decode_centroids(centroids_ipc)
    artifacts: tuple[Any, str, int, int] = (centroids, rabitq_model, num_bits, num_partitions)
    segment = build_vector_segment(dataset, shard, artifacts, column, index_name, metric)
    return serialize_segment(segment)


def run_cluster_rewrites(
    spark: SparkSession,
    uris: list[str],
    config: MaintenanceConfig,
    cutoff: datetime | None,
    driver_telemetry: Telemetry,
    cleanup_slot: int | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Cluster-rewrite eligible datasets, returning results and the passthrough set.

    The orchestration runs the fleet phases in order: plan fan-out, one flat histogram job, driver
    bucket derivation, one flat rewrite shuffle, per-dataset overwrite commit, per-dataset vector
    index rebuild, then cleanup. No serving tag is ever touched here: promotion happens exclusively
    through the pipeline stamp phase after the index phase (ADR 0041). Per-dataset failure isolation
    holds at every phase: a failed dataset carries an ``{"error", "phase"}`` marker and drops out of
    all later cluster phases and out of normal compaction. A cluster-ineligible dataset is returned
    in the passthrough list so the caller continues it into normal maintenance, while an
    already-clustered unchanged dataset (its ``lance-etl.cluster_generation`` fingerprint still
    matches) is a terminal skip that enters neither.

    Args:
        spark: Active Spark session.
        uris: Datasets to consider for a clustered rewrite.
        config: Maintenance configuration.
        cutoff: retention cutoff instant, or ``None`` when retention is inactive.
        driver_telemetry: The driver's telemetry facade.
        cleanup_slot: The active fleet-wide rotation slot for this run, threaded into the
            already-clustered idle cleanup, or ``None`` to always clean.

    Returns:
        A ``(results_by_uri, passthrough_uris)`` pair. ``results_by_uri`` holds one result dict per
        clustered or errored dataset; ``passthrough_uris`` lists the cluster-skipped datasets.
    """
    results: dict[str, dict[str, Any]] = {}
    passthrough: list[str] = []
    state = ClusterRunState(spark, config, driver_telemetry, results, passthrough, cleanup_slot)
    with driver_telemetry.span("lance.cluster_rewrite.run") as run_span:
        run_span.set_tag("dataset_count", len(uris))
        eligible: list[dict[str, Any]] = state.plan(uris, cutoff)
        if not eligible:
            return results, passthrough
        counts_by_uri: dict[str, list[int]] = state.histogram(eligible)
        eligible = [plan for plan in eligible if plan["uri"] in counts_by_uri]
        documents_by_uri: dict[str, list[str]] = state.rewrite(eligible, counts_by_uri)
        eligible = [plan for plan in eligible if plan["uri"] in documents_by_uri]
        state.commit_and_index(eligible, documents_by_uri)
        run_span.set_tag("clustered", sum(1 for item in results.values() if item.get("clustered")))
    return results, passthrough


class ClusterRunState:
    """Carries the shared driver state threaded through the clustered-rewrite phases."""

    def __init__(
        self,
        spark: SparkSession,
        config: MaintenanceConfig,
        driver_telemetry: Telemetry,
        results: dict[str, dict[str, Any]],
        passthrough: list[str],
        cleanup_slot: int | None = None,
    ) -> None:
        """Initialize the run state.

        Args:
            spark: Active Spark session.
            config: Maintenance configuration.
            driver_telemetry: The driver's telemetry facade.
            results: The per-dataset result accumulator, mutated in place across phases.
            passthrough: The cluster-skipped URI accumulator, mutated in place.
            cleanup_slot: The active fleet-wide rotation slot for this run, threaded into the
                already-clustered idle cleanup, or ``None`` to always clean.
        """
        self.spark: SparkSession = spark
        self.config: MaintenanceConfig = config
        self.telemetry: Telemetry = driver_telemetry
        self.results: dict[str, dict[str, Any]] = results
        self.passthrough: list[str] = passthrough
        self.cleanup_slot: int | None = cleanup_slot
        self.plans_by_uri: dict[str, dict[str, Any]] = {}

    def fanout_partitions(self) -> int:
        """Return the Spark partition count for the per-dataset fan-out phases.

        Returns:
            The resolved fan-out partition count.
        """
        return derive_partitions(self.spark, FANOUT_PARTITION_FACTOR)

    def flat_partitions(self, task_count: int) -> int:
        """Return the Spark partition count for a flat fleet job of ``task_count`` tasks.

        Args:
            task_count: The number of flat-job tasks.

        Returns:
            The resolved partition count, capped at the task count.
        """
        resolved: int = derive_partitions(self.spark, BUILD_PARTITION_FACTOR)
        return max(1, min(resolved, task_count))

    def plan(self, uris: list[str], cutoff: datetime | None) -> list[dict[str, Any]]:
        """Run the plan fan-out and split datasets into eligible, passthrough, skipped, and errored.

        A dataset already clustered and unchanged since (``cluster_current``) lands directly in the
        shared results as a terminal ``skipped`` outcome rather than the passthrough list, so it is
        never handed to normal compaction, which would re-merge its fragments toward insertion
        order and undo the centroid ordering. It still carries the ``bytes_removed`` its
        rotation-gated idle version cleanup reclaimed in the plan phase, so the pre-rewrite
        generation is retired on a later run rather than pinned forever.

        Args:
            uris: Datasets to consider.
            cutoff: retention cutoff instant, or ``None``.

        Returns:
            The eligible plan dicts. Passthrough, already-clustered, and errored datasets are
            recorded in the shared accumulators.
        """
        config: MaintenanceConfig = self.config
        cleanup_slot: int | None = self.cleanup_slot
        plans: list[dict[str, Any]] = fan_out_per_dataset(
            self.spark,
            uris,
            config.telemetry,
            lambda uri, telemetry, cutoff_value=cutoff, slot=cleanup_slot: plan_cluster_rewrite(
                uri, config, cutoff_value, telemetry, slot
            ),
            self.fanout_partitions(),
            phase="cluster-plan",
        )
        eligible: list[dict[str, Any]] = []
        for plan in plans:
            uri: str = plan["uri"]
            if "error" in plan:
                self.results[uri] = plan
            elif "cluster_current" in plan:
                self.results[uri] = {
                    "uri": uri,
                    "skipped": plan["cluster_current"],
                    "bytes_removed": plan.get("bytes_removed", 0),
                }
            elif "cluster_skipped" in plan:
                self.passthrough.append(uri)
            else:
                self.plans_by_uri[uri] = plan
                eligible.append(plan)
        return eligible

    def broadcast_centroids(self, plans: list[dict[str, Any]]) -> Any:
        """Broadcast the per-dataset centroid IPC bytes once for the flat jobs.

        Args:
            plans: The eligible plan dicts.

        Returns:
            The Spark broadcast handle wrapping ``{uri: centroids_ipc}``.
        """
        return self.spark.sparkContext.broadcast({plan["uri"]: plan["centroids_ipc"] for plan in plans})

    def histogram(self, plans: list[dict[str, Any]]) -> dict[str, list[int]]:
        """Run the flat histogram job and aggregate per-dataset partition counts.

        Args:
            plans: The eligible plan dicts.

        Returns:
            The per-dataset histograms, excluding any dataset whose histogram task failed.
        """
        centroids = self.broadcast_centroids(plans)
        storage_options: dict[str, Any] | None = self.config.storage_options
        tasks: list[tuple[str, int, list[int], str, str]] = read_shard_tasks(plans)

        def run_one(item: tuple[str, int, list[int], str, str]) -> tuple[str, str, Any]:
            """Count one shard's histogram, tagging success or failure by dataset."""
            uri: str = item[0]
            try:
                counts: list[int] = partition_histogram(
                    uri, item[1], item[2], item[3], centroids.value[uri], item[4], storage_options
                )
                return FLAT_OK, uri, counts
            except Exception as exc:
                return FLAT_ERROR, uri, str(exc)

        grouped, errors = run_flat_tagged_job(self.spark, tasks, run_one, self.flat_partitions(len(tasks)))
        release_broadcast(centroids)
        return self.reduce_histograms(grouped, errors)

    def reduce_histograms(self, grouped: dict[str, list[Any]], errors: dict[str, str]) -> dict[str, list[int]]:
        """Sum per-shard histograms per dataset, error-marking any dataset with a failed shard.

        Every successful shard result is a full-width per-partition-count list, so the sum's width
        is implied by the shard data itself rather than a separately threaded plan width.

        Args:
            grouped: The per-dataset lists of successful shard histograms.
            errors: The first failure message per dataset with any failed shard.

        Returns:
            The per-dataset summed histograms for datasets whose every shard succeeded.
        """
        for uri, message in errors.items():
            self.results[uri] = {"uri": uri, "error": message, "phase": "cluster-histogram", "bytes_removed": 0}
            grouped.pop(uri, None)
        return {uri: np.sum(np.asarray(counts, dtype=np.int64), axis=0).tolist() for uri, counts in grouped.items()}

    def derive_global_buckets(
        self, plans: list[dict[str, Any]], counts_by_uri: dict[str, list[int]]
    ) -> tuple[dict[str, list[tuple[int, int, int, int, int]]], dict[int, str]]:
        """Derive and globally enumerate write buckets across every eligible dataset.

        Args:
            plans: The eligible plan dicts.
            counts_by_uri: The per-dataset histograms.

        Returns:
            A ``(global_buckets_by_uri, owner_by_bucket)`` pair. The first maps a URI to its
            ``(global_id, start_pid, end_pid, salt, num_salts)`` tuples; the second maps every global
            bucket id back to its owning dataset URI.
        """
        rows_per_task: int = cluster_rows_per_task(self.config)
        global_buckets_by_uri: dict[str, list[tuple[int, int, int, int, int]]] = {}
        owner_by_bucket: dict[int, str] = {}
        next_id: int = 0
        for plan in plans:
            uri: str = plan["uri"]
            local: list[tuple[int, int, int, int]] = derive_buckets(counts_by_uri[uri], rows_per_task)
            enumerated: list[tuple[int, int, int, int, int]] = []
            for start_pid, end_pid, salt, num_salts in local:
                enumerated.append((next_id, start_pid, end_pid, salt, num_salts))
                owner_by_bucket[next_id] = uri
                next_id += 1
            global_buckets_by_uri[uri] = enumerated
        return global_buckets_by_uri, owner_by_bucket

    def rewrite(self, plans: list[dict[str, Any]], counts_by_uri: dict[str, list[int]]) -> dict[str, list[str]]:
        """Run the flat rewrite shuffle and validate each dataset's row multiset.

        Args:
            plans: The eligible plan dicts.
            counts_by_uri: The per-dataset histograms.

        Returns:
            The per-dataset ordered fragment documents for datasets whose row count validated.
        """
        global_buckets_by_uri, owner_by_bucket = self.derive_global_buckets(plans, counts_by_uri)
        centroids = self.broadcast_centroids(plans)
        buckets = self.spark.sparkContext.broadcast(global_buckets_by_uri)
        owners = self.spark.sparkContext.broadcast(owner_by_bucket)
        schema_rows: dict[str, tuple[pa.Schema, int]] = {
            plan["uri"]: (plan["schema"], cluster_rows_per_task(self.config)) for plan in plans
        }
        meta = self.spark.sparkContext.broadcast(schema_rows)
        total_buckets: int = len(owner_by_bucket)
        collected: list[tuple[Any, ...]] = run_rewrite_shuffle(
            self.spark,
            plans,
            buckets,
            centroids,
            owners,
            meta,
            total_buckets,
            self.config.storage_options,
        )
        for handle in (centroids, buckets, owners, meta):
            release_broadcast(handle)
        return self.validate_rewrite(plans, collected)

    def validate_rewrite(self, plans: list[dict[str, Any]], collected: list[tuple[Any, ...]]) -> dict[str, list[str]]:
        """Order fragment documents per dataset and validate the written row total.

        The collected shuffle results are tagged: :data:`REWRITE_OK` tuples carry a written
        fragment document, while :data:`REWRITE_ERROR` tuples carry a per-dataset failure message
        emitted by an isolated read task or bucket write. A dataset with any dropped read or bucket
        loses rows and fails the row-count check, so it is error-marked while every other dataset
        proceeds. When such a dataset carries a captured failure message, the marker prefers it over
        the generic row-count mismatch so the diagnostics name the real cause.

        Args:
            plans: The eligible plan dicts.
            collected: Tagged shuffle results, either ``(REWRITE_OK, uri, global_bucket, seq,
                fragment_json, rows)`` or ``(REWRITE_ERROR, uri, message)``.

        Returns:
            The ordered fragment documents per dataset whose written rows equalled ``total_rows``.
        """
        grouped: dict[str, list[tuple[int, int, str, int]]] = {}
        errors: dict[str, str] = {}
        for row in collected:
            if row[0] == REWRITE_OK:
                grouped.setdefault(row[1], []).append((row[2], row[3], row[4], row[5]))
            else:
                errors.setdefault(row[1], row[2])
        documents_by_uri: dict[str, list[str]] = {}
        for plan in plans:
            uri: str = plan["uri"]
            entries: list[tuple[int, int, str, int]] = sorted(grouped.get(uri, []))
            written: int = sum(entry[3] for entry in entries)
            if written != plan["total_rows"]:
                fallback: str = f"rewrite wrote {written} rows but planned {plan['total_rows']}; refusing to commit"
                self.results[uri] = {
                    "uri": uri,
                    "error": errors.get(uri, fallback),
                    "phase": "cluster-rewrite",
                    "bytes_removed": 0,
                }
                continue
            documents_by_uri[uri] = [entry[2] for entry in entries]
        return documents_by_uri

    def commit_and_index(self, plans: list[dict[str, Any]], documents_by_uri: dict[str, list[str]]) -> None:
        """Commit the overwrite, rebuild the vector index, then flip the tag and clean up.

        Args:
            plans: The eligible plan dicts that produced validated fragment documents.
            documents_by_uri: The per-dataset ordered fragment documents.
        """
        committed: list[dict[str, Any]] = self.commit_overwrites(plans, documents_by_uri)
        if committed:
            self.rebuild_indexes(committed)

    def commit_overwrites(
        self, plans: list[dict[str, Any]], documents_by_uri: dict[str, list[str]]
    ) -> list[dict[str, Any]]:
        """Commit each dataset's overwrite in a per-dataset fan-out (phase ``cluster-commit``).

        Args:
            plans: The eligible plan dicts.
            documents_by_uri: The per-dataset ordered fragment documents.

        Returns:
            The plan dicts whose overwrite committed successfully.
        """
        config: MaintenanceConfig = self.config
        plan_by_uri: dict[str, dict[str, Any]] = {plan["uri"]: plan for plan in plans}
        outcomes: list[dict[str, Any]] = fan_out_per_dataset(
            self.spark,
            list(documents_by_uri),
            config.telemetry,
            lambda uri, telemetry: commit_one_overwrite(
                uri, documents_by_uri[uri], plan_by_uri[uri], config, telemetry
            ),
            self.fanout_partitions(),
            phase="cluster-commit",
        )
        committed: list[dict[str, Any]] = []
        for outcome in outcomes:
            uri: str = outcome["uri"]
            if "error" in outcome:
                self.results[uri] = {**outcome, "bytes_removed": 0}
            else:
                plan_by_uri[uri]["fragments_added"] = outcome.get("fragments_added", 0)
                committed.append(plan_by_uri[uri])
        return committed

    def rebuild_indexes(self, plans: list[dict[str, Any]]) -> None:
        """Rebuild each committed dataset's vector index, then finalise it.

        The post-commit open and fragment sharding run inside an executor fan-out, not a driver
        loop, so no dataset is opened on the driver. Both a shard-planning failure and a
        segment-build failure are non-fatal like any rebuild failure: the data stays committed but
        unindexed, the dataset carries a ``cluster_index`` error marker, and it drops out of the
        finalise fan-out so a partially-built segment set never reaches ``commit_segments``.

        Args:
            plans: The plan dicts whose overwrite committed.
        """
        config: MaintenanceConfig = self.config
        plan_by_uri: dict[str, dict[str, Any]] = {plan["uri"]: plan for plan in plans}
        tasks: list[tuple[dict[str, Any], list[int]]] = self.shard_committed(plan_by_uri)
        if not plan_by_uri:
            return
        centroids = self.broadcast_centroids(list(plan_by_uri.values()))
        segments_by_uri, build_errors = self.build_rebuild_segments(tasks, centroids, config.storage_options)
        release_broadcast(centroids)
        for uri, message in build_errors.items():
            self.results[uri] = mark_rebuild_failure(plan_by_uri[uri], message)
            plan_by_uri.pop(uri, None)
        if not plan_by_uri:
            return
        outcomes: list[dict[str, Any]] = fan_out_per_dataset(
            self.spark,
            list(plan_by_uri),
            config.telemetry,
            lambda uri, telemetry: finalise_cluster_dataset(
                uri, plan_by_uri[uri], segments_by_uri.get(uri, []), config, telemetry
            ),
            self.fanout_partitions(),
            phase="cluster-index",
        )
        for outcome in outcomes:
            self.results[outcome["uri"]] = outcome

    def shard_committed(self, plan_by_uri: dict[str, dict[str, Any]]) -> list[tuple[dict[str, Any], list[int]]]:
        """Shard each committed dataset's fresh fragment ids in an executor fan-out.

        Opens each committed dataset on an executor to read its fresh fragment ids (reset from 0 by
        the overwrite) and pins the committed version onto its plan. A dataset whose shard planning
        fails drops out with a ``cluster_index`` error marker and its plan is removed in place.

        Args:
            plan_by_uri: The committed plan dicts by URI, mutated in place to drop failed datasets.

        Returns:
            The ``(plan, shard)`` rebuild tasks across every still-eligible committed dataset.
        """
        config: MaintenanceConfig = self.config
        shard_plans: list[dict[str, Any]] = fan_out_per_dataset(
            self.spark,
            list(plan_by_uri),
            config.telemetry,
            lambda uri, telemetry: plan_rebuild_shards(uri, config.storage_options, telemetry),
            self.fanout_partitions(),
            phase="cluster-index",
        )
        tasks: list[tuple[dict[str, Any], list[int]]] = []
        for shard_plan in shard_plans:
            uri: str = shard_plan["uri"]
            if "error" in shard_plan:
                self.results[uri] = mark_rebuild_failure(plan_by_uri[uri], shard_plan["error"])
                plan_by_uri.pop(uri, None)
                continue
            plan_by_uri[uri]["committed_version"] = shard_plan["committed_version"]
            tasks.extend((plan_by_uri[uri], shard) for shard in shard_plan["shards"])
        return tasks

    def build_rebuild_segments(
        self, tasks: list[tuple[dict[str, Any], list[int]]], centroids: Any, storage_options: dict[str, Any] | None
    ) -> tuple[dict[str, list[str]], dict[str, str]]:
        """Build every committed dataset's index segments in one flat job, isolating per dataset.

        Each shard build is tagged like the histogram phase: a failing
        :func:`build_cluster_index_segment` yields a ``(FLAT_ERROR, uri, message)`` result instead
        of killing the job, so one dataset's failed segment build never aborts the run after other
        datasets' overwrites already committed. A dataset with any errored shard is reported through
        the returned error map so :meth:`rebuild_indexes` can drop it before the finalise fan-out,
        ensuring a partially-built dataset never reaches ``commit_segments`` with an incomplete
        segment set.

        Args:
            tasks: ``(plan, shard)`` rebuild tasks over the post-commit fragment ids.
            centroids: The broadcast centroid handle.
            storage_options: Object-store options forwarded to lance.

        Returns:
            A ``(segments_by_uri, errors_by_uri)`` pair. The first maps each fully-built dataset URI
            to its serialised segment documents; the second maps each dataset with a failed shard to
            its first failure message.
        """

        def build_one(item: tuple[dict[str, Any], list[int]]) -> tuple[str, str, str]:
            """Build one shard's segment, tagging success or failure by dataset URI."""
            plan, shard = item
            uri: str = plan["uri"]
            try:
                document: str = build_cluster_index_segment(
                    uri,
                    plan["committed_version"],
                    shard,
                    plan["column"],
                    plan["index_name"],
                    plan["metric"],
                    plan["num_bits"],
                    plan["num_partitions"],
                    centroids.value[uri],
                    plan["rabitq_model"],
                    storage_options,
                )
                return FLAT_OK, uri, document
            except Exception as exc:
                return FLAT_ERROR, uri, str(exc)

        grouped, errors = run_flat_tagged_job(self.spark, tasks, build_one, self.flat_partitions(len(tasks)))
        for uri in errors:
            grouped.pop(uri, None)
        return grouped, errors


def run_rewrite_shuffle(
    spark: SparkSession,
    plans: list[dict[str, Any]],
    buckets: Any,
    centroids: Any,
    owners: Any,
    meta: Any,
    total_buckets: int,
    storage_options: dict[str, Any] | None,
) -> list[tuple[Any, ...]]:
    """Run the flat read-shuffle-write rewrite job across every eligible dataset, isolating per task.

    The read job emits ``(global_bucket, ipc_bytes)`` pairs, ``partitionBy`` co-locates each global
    bucket on one partition, and the write side groups by bucket and writes fragments. Each global
    bucket belongs to exactly one dataset, looked up through the broadcast owner map.

    Per-task failures are non-fatal and attributed to their dataset rather than aborting the whole
    run. A failing read task or bucket write emits a :data:`REWRITE_ERROR` sentinel through the same
    shuffle instead of raising, so the owning dataset loses rows and fails the downstream row-count
    validation while every other dataset still commits. Read-failure sentinels ride a reserved
    :data:`REWRITE_ERROR_KEY` shuffle key that ``partitionBy`` hashes onto a valid partition and
    ``write_partition`` passes straight through.

    The read fan-out is sized by :func:`~lance_etl.fanout.derive_partitions` at
    :data:`~lance_etl.fanout.REWRITE_PARTITION_FACTOR`, capped by the read task count, independent
    of ``total_buckets``. The subsequent ``partitionBy`` shuffle decouples read width from write
    bucket width, so capping the read scan (the heaviest I/O: a full all-columns dataset scan) at
    the write bucket count would throttle it for no benefit to the write side.

    Args:
        spark: Active Spark session.
        plans: The eligible plan dicts.
        buckets: The broadcast per-dataset enumerated global buckets, shipped once per executor
            rather than captured in the read closure and re-serialized per read task.
        centroids: The broadcast centroid handle.
        owners: The broadcast global-bucket-to-URI owner map.
        meta: The broadcast per-URI ``(schema, rows_per_task)`` map.
        total_buckets: The global bucket count, the partition width for ``partitionBy``.
        storage_options: Object-store options forwarded to lance.

    Returns:
        Tagged results, either ``(REWRITE_OK, uri, global_bucket, seq, fragment_json, rows)`` for a
        written fragment or ``(REWRITE_ERROR, uri, message)`` for an isolated read or write failure.
    """
    read_tasks: list[tuple[str, int, list[int], str, str]] = read_shard_tasks(plans)
    if not read_tasks or total_buckets == 0:
        return []

    def read_partition(items: Any) -> Any:
        """Emit ``(global_bucket, ipc_bytes)`` pairs, isolating a failing read task per dataset.

        A read task whose :func:`read_rewrite_chunks` raises yields one
        ``(REWRITE_ERROR_KEY, (uri, message))`` sentinel and nothing else, so the failure travels
        the shuffle attributed to its dataset instead of killing the job.
        """
        for uri, version, shard, column, distance_type in items:
            try:
                yield from read_rewrite_chunks(
                    uri,
                    version,
                    shard,
                    column,
                    centroids.value[uri],
                    distance_type,
                    buckets.value[uri],
                    storage_options,
                )
            except Exception as exc:
                logger.warning("cluster: rewrite read task failed for %s, dataset will fail validation: %s", uri, exc)
                yield (REWRITE_ERROR_KEY, (uri, str(exc)))

    def write_partition(items: Any) -> Any:
        """Group co-located chunks by global bucket and write each bucket's fragments, isolating failures.

        Read-failure sentinels keyed by :data:`REWRITE_ERROR_KEY` pass straight through as
        ``(REWRITE_ERROR, uri, message)`` results. A bucket whose :func:`write_bucket` raises yields
        one ``(REWRITE_ERROR, uri, message)`` result instead of killing the job, so only the owning
        dataset fails validation.
        """
        chunks_by_bucket: dict[int, list[bytes]] = {}
        for global_bucket, payload in items:
            if global_bucket == REWRITE_ERROR_KEY:
                error_uri, message = payload
                yield (REWRITE_ERROR, error_uri, message)
                continue
            chunks_by_bucket.setdefault(global_bucket, []).append(payload)
        for global_bucket, chunks in chunks_by_bucket.items():
            uri: str = owners.value[global_bucket]
            schema, rows_per_task = meta.value[uri]
            try:
                for entry in write_bucket(global_bucket, chunks, uri, schema, rows_per_task, storage_options):
                    yield (REWRITE_OK, uri, entry[0], entry[1], entry[2], entry[3])
            except Exception as exc:
                logger.warning("cluster: rewrite write bucket %d failed for %s: %s", global_bucket, uri, exc)
                yield (REWRITE_ERROR, uri, str(exc))

    slices: int = max(1, min(len(read_tasks), derive_partitions(spark, REWRITE_PARTITION_FACTOR)))
    return (
        spark.sparkContext.parallelize(read_tasks, slices)
        .mapPartitions(read_partition)
        .partitionBy(total_buckets)
        .mapPartitions(write_partition)
        .collect()
    )


def plan_rebuild_shards(uri: str, storage_options: dict[str, Any] | None, telemetry: Telemetry) -> dict[str, Any]:
    """Pin the committed version and shard a rewritten dataset's fresh fragment ids on an executor.

    Opens the dataset at its latest version (the just-committed overwrite), reads the fresh fragment
    ids reset from 0, and shards them for the flat segment-build job. Runs inside the rebuild
    fan-out so no dataset is opened on the driver.

    Args:
        uri: Dataset URI.
        storage_options: Object-store options forwarded to lance.
        telemetry: Telemetry facade for the current process.

    Returns:
        A ``{"uri", "committed_version", "shards"}`` dict.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    telemetry.incr("cluster.rebuild_planned")
    return {
        "uri": uri,
        "committed_version": dataset.version,
        "shards": split_evenly(all_fragment_ids(dataset), CLUSTER_READ_SHARDS),
    }


def mark_rebuild_failure(plan: dict[str, Any], error: str) -> dict[str, Any]:
    """Build the non-fatal rebuild-failure result marker for one clustered dataset.

    The data is complete but unindexed, so the marker keeps the clustered outcome fields and adds
    an ``{"error", "phase": "cluster_index"}`` pair. Version cleanup is skipped for a failed rebuild.

    Args:
        plan: The dataset's plan dict.
        error: The rebuild failure message.

    Returns:
        The result marker carrying the clustered data plus the error.
    """
    return {
        "uri": plan["uri"],
        "clustered": True,
        "fragments_added": plan.get("fragments_added", 0),
        "retention_rows_deleted": plan.get("retention_rows_deleted", 0),
        "error": error,
        "phase": "cluster_index",
        "bytes_removed": 0,
    }


def commit_one_overwrite(
    uri: str,
    fragment_documents: list[str],
    plan: dict[str, Any],
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Commit one dataset's clustered overwrite on an executor.

    Args:
        uri: Dataset URI.
        fragment_documents: The ordered fragment documents for this dataset.
        plan: The dataset's plan dict carrying the pinned schema.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A ``{"uri", "fragments_added"}`` dict on success.
    """
    fragments_added: int = commit_cluster_overwrite(uri, fragment_documents, plan["schema"], config, telemetry)
    return {"uri": uri, "fragments_added": fragments_added}


def finalise_cluster_dataset(
    uri: str,
    plan: dict[str, Any],
    segment_documents: list[str],
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Commit the rebuilt vector index and clean up, without touching any serving tag.

    The vector-index commit is best-effort: a rebuild failure is non-fatal because the data is
    complete, just unindexed, so the result carries an ``{"error", "phase": "cluster_index"}`` marker
    and the data stays committed. On success old versions are pruned.

    Serving promotion is deliberately not done here. Flipping ``HEAD`` right after the vector-index
    commit would expose a generation whose scalar and FTS indexes have not yet been rebuilt (those
    are left to the next indexing run, ADR 0041), so a text query could see a clustered-but-unindexed
    generation as the served one. Exact promotion belongs to the durable reconciler after every
    required index is rebuilt, validated, and prewarmed.

    Args:
        uri: Dataset URI.
        plan: The dataset's plan dict.
        segment_documents: The serialised index segments built for this dataset.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A success result dict, or one carrying an ``{"error", "phase": "cluster_index"}`` marker
        while keeping the rewritten data.
    """
    index_config: IndexJobConfig = cluster_index_config(config, plan["num_partitions"], plan["metric"])
    try:
        commit_segments(uri, segment_documents, plan["column"], plan["index_name"], True, index_config, telemetry)
    except Exception as exc:
        telemetry.incr("cluster.index_rebuild_failed")
        logger.warning("cluster: vector index rebuild failed for %s, data intact: %s", uri, exc)
        return mark_rebuild_failure(plan, str(exc))
    bytes_removed: int = maintenance_job.cleanup_dataset(uri, config, telemetry)
    telemetry.incr("cluster.clustered")
    return {
        "uri": uri,
        "clustered": True,
        "fragments_added": plan.get("fragments_added", 0),
        "retention_rows_deleted": plan.get("retention_rows_deleted", 0),
        "bytes_removed": bytes_removed,
    }
