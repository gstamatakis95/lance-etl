"""Clustered rewrite: reorder a Lance dataset so same-centroid rows share fragments.

An operator occasionally wants to fully read and rewrite a Lance dataset so that rows assigned to
the same IVF centroid (vector-index partition) land contiguously, in the same fragment where
possible. Lance has no built-in clustered compaction, so the mechanism here is a manual distributed
pipeline: nearest-centroid assignment on the raw vector, a global partition-range shuffle, a
``write_fragments`` rewrite, a ``LanceOperation.Overwrite`` commit, then a vector-index rebuild that
preserves the old centroids through the segment API.

The flow mirrors the fleet-phase shape of :mod:`lance_etl.maintenance.job`. The driver plans,
broadcasts read-only artifacts, and commits, while every row-level read and write runs inside an
executor closure. The vector-index rebuild
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
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import groupby
from typing import TYPE_CHECKING, Any

import lance
import numpy as np
import pyarrow as pa
from lance.fragment import FragmentMetadata, write_fragments
from pyspark.sql import SparkSession

import lance_etl.maintenance.job as maintenance_job
from lance_etl.column_roles import VECTOR_ROLE, load_column_roles
from lance_etl.etl.storage import DATA_STORAGE_VERSION
from lance_etl.fanout import (
    BUILD_PARTITION_FACTOR,
    FANOUT_PARTITION_FACTOR,
    FLAT_ERROR,
    FLAT_OK,
    REWRITE_PARTITION_FACTOR,
    derive_partitions,
    fan_out_per_dataset,
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
from lance_etl.telemetry import DEFAULT_COMMIT_RETRIES, Telemetry, TelemetryConfig, commit_with_retries

if TYPE_CHECKING:
    from datetime import datetime

    from lance_etl.maintenance.job import MaintenanceConfig

logger: logging.Logger = logging.getLogger(__name__)

CLUSTER_PARTITION_COLUMN: str = "lance_etl_cluster_partition"
"""Temporary int32 partition-id column used for bucket routing and dropped before fragment writes."""

CLUSTER_READ_SHARDS: int = 64
"""Default fragment shards per dataset for the histogram and rewrite read jobs."""

ASSIGN_BLOCK_ROWS: int = 65_536
"""Maximum rows assigned per numpy block."""

ASSIGN_WORKING_BYTES: int = 64 * 1024 * 1024
"""Maximum dense temporary bytes retained by one centroid-assignment block."""

SHUFFLE_CHUNK_BYTES: int = 32 * 1024 * 1024
"""Soft byte cap across pending Arrow tables in one rewrite-shard reader."""

CLUSTER_DATASET_BATCH_SIZE: int = 1
"""Hard dataset count per clustered batch, limiting the driver to one centroid artifact at a time."""

COSINE_NORM_EPS: float = 1e-12
"""Floor on a vector or centroid norm before cosine normalisation, guarding the zero vector."""

REWRITE_OK: str = "ok"
"""Tag on a collected rewrite-shuffle result carrying a written fragment document."""

REWRITE_ERROR: str = "error"
"""Tag on a collected rewrite-shuffle result carrying a per-dataset failure message."""

ClusterReadSeed = tuple[str, int, int, str, str]
"""Dataset-level seed for executor-side fragment-shard enumeration."""

ClusterReadTask = tuple[str, int, list[int], str, str, str | None]
"""Read shard or isolated inventory error emitted by executor-side enumeration."""

ClusterIndexSeed = tuple[str, int, int, str, str, str, int, int]
"""Dataset-level index-rebuild seed excluding broadcast artifacts."""

ClusterIndexShardTask = tuple[str, int, list[int], str, str, str, int, int, str | None]
"""Small rebuild shard or isolated inventory error excluding broadcast artifacts."""


@dataclass(frozen=True)
class ClusterCommitPayload:
    """Minimal executor payload for one clustered overwrite commit."""

    uri: str
    fragment_documents: list[str]
    schema: pa.Schema
    read_version: int


@dataclass(frozen=True)
class ClusterOverwriteCommit:
    """Committed clustered data fingerprint needed for safe generation stamping."""

    fragments_added: int
    fragment_ids: list[int] | None
    num_rows: int
    fingerprint_error: str | None


@dataclass(frozen=True)
class ClusterFinalisePayload:
    """Minimal executor payload for one clustered index finalisation."""

    uri: str
    segment_documents: list[str]
    column: str
    index_name: str
    metric: str
    num_partitions: int
    fragments_added: int
    retention_rows_deleted: int
    fragment_ids: list[int] | None
    num_rows: int
    fingerprint_error: str | None


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
    generation. ``blocking=True`` waits for executor-side copies to be removed before the next
    artifact-heavy phase starts.

    Args:
        handle: The Spark broadcast handle to release.
    """
    handle.destroy(blocking=True)


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
    object-store I/O. Returns ``None`` when the key is absent or its exact current shape cannot be
    parsed.

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
        fragment_ids: list[int] = [int(value) for value in parsed["fragment_ids"]]
        num_rows: int = int(parsed["num_rows"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        logger.warning("cluster: malformed cluster generation config on %s; treating as absent", dataset.uri)
        return None
    return {"fragment_ids": fragment_ids, "num_rows": num_rows}


def cluster_generation_skip_reason(dataset: lance.LanceDataset) -> str | None:
    """Return a skip reason when a dataset was already clustered and has not been written since.

    The clustered-generation fingerprint (the sorted fragment-id list and the logical row count) is
    stamped at overwrite-commit time and survives the later index-rebuild and cleanup commits,
    which add no data fragments and change no logical row count. The two halves catch disjoint
    kinds of write: a fragment-replacing write (append, merge rewrite, compaction) mints new
    fragment ids so the id list diverges even when the count is unchanged, while a pure delete
    lowers the row count without touching the ids. Both reads come from the already-open manifest,
    so the check costs no object-store I/O. The clustered plan runs time-based retention before
    honoring a matching fingerprint, so an idle generation cannot defer record expiry indefinitely.

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


def stamp_cluster_generation(
    uri: str,
    config: MaintenanceConfig,
    telemetry: Telemetry,
    expected_fragment_ids: list[int] | None = None,
    expected_num_rows: int | None = None,
) -> None:
    """Stamp the clustered-generation fingerprint into a dataset's config KV.

    Mirrors :func:`~lance_etl.indexing.optimize.write_vector_config`: the ``update_config`` write
    surfaces conflicts as ``OSError`` through the pyo3 binding, so it is wrapped in
    :func:`~lance_etl.telemetry.commit_with_retries`, which re-opens the dataset at the latest
    version before each attempt. The overwrite path supplies its committed fragment and row
    fingerprint. A later index-only commit may safely be rebased because it preserves that
    fingerprint, while any append, delete, or fragment rewrite fails closed instead of being
    mislabeled as clustered. Direct callers may omit both expected values to stamp the current
    generation deliberately.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration supplying the retry budget and backoff.
        telemetry: Telemetry facade for the current process.
        expected_fragment_ids: Optional sorted data-fragment identity that must still be current.
        expected_num_rows: Optional logical row count that must still be current.

    Raises:
        ValueError: If only one expected fingerprint component is supplied.
        RuntimeError: If the data generation changed before the stamp committed.
    """
    if (expected_fragment_ids is None) != (expected_num_rows is None):
        raise ValueError("cluster generation stamp requires both expected fingerprint components")

    def action() -> None:
        """Verify and stamp the latest dataset generation."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        fragment_ids: list[int] = fragment_id_signature(dataset)
        num_rows: int = dataset.count_rows()
        if expected_fragment_ids is not None and (
            fragment_ids != expected_fragment_ids or num_rows != expected_num_rows
        ):
            raise RuntimeError(f"clustered data generation changed before it could be stamped on {uri}")
        payload: str = json.dumps({"fragment_ids": fragment_ids, "num_rows": num_rows})
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


def assignment_block_rows(num_partitions: int, dimension: int, distance_type: str) -> int:
    """Choose a row block that respects the dense assignment-working-set budget.

    Every distance type retains one dense row-by-centroid score matrix. L2 transforms its dot
    products into reduced squared distances in place. Cosine also retains one normalized
    row-by-dimension matrix. The result is capped by
    :data:`ASSIGN_BLOCK_ROWS` and floored at one row, so increasing IVF partition count cannot make
    one assignment block consume unbounded executor memory and high-dimensional cosine input is
    included in the same bound.

    Args:
        num_partitions: Positive IVF centroid count.
        dimension: Positive vector dimension.
        distance_type: Lance distance type.

    Returns:
        Maximum rows to assign in one numpy block.

    Raises:
        ValueError: If the partition count, dimension, or distance type is invalid.
    """
    if num_partitions < 1 or dimension < 1:
        raise ValueError("num_partitions and dimension must be positive")
    if distance_type not in ("l2", "cosine", "dot"):
        raise ValueError(f"unsupported distance_type {distance_type!r}; expected l2, cosine, or dot")
    live_values_per_row: int = num_partitions + (dimension if distance_type == "cosine" else 0)
    bytes_per_row: int = live_values_per_row * np.dtype(np.float32).itemsize
    return max(1, min(ASSIGN_BLOCK_ROWS, ASSIGN_WORKING_BYTES // bytes_per_row))


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
        scores *= -2.0
        scores += centroid_norms_sq[None, :]
        return np.argmin(scores, axis=1).astype(np.int64)
    if distance_type == "cosine":
        similarities: np.ndarray = normalise_rows(block) @ normalised_centroids.T
        return np.argmax(similarities, axis=1).astype(np.int64)
    dots: np.ndarray = block @ centroids.T
    return np.argmax(dots, axis=1).astype(np.int64)


def assign_partition_ids(vectors: pa.FixedSizeListArray, centroids: np.ndarray, distance_type: str) -> np.ndarray:
    """Assign each vector to its nearest IVF centroid under the index distance type.

    This is the pure, executor-side assigner that reproduces Lance's own IVF partition assignment:
    nearest centroid on the raw vector, with the RaBitQ rotation applying only to residuals after
    assignment. The work scales its row block from :data:`ASSIGN_WORKING_BYTES`, the centroid
    count so the dense row-by-centroid matrices stay bounded. ``l2`` minimises the reduced
    squared distance ``-2 x C^T + ||C||^2`` (the per-row ``||x||^2`` term is constant and dropped),
    ``cosine`` normalises both sides with a zero-vector eps guard and maximises the dot product, and
    ``dot`` maximises the raw dot product.

    Null vector rows are not special-cased here: their underlying buffer values yield some
    argmin/argmax that the caller overwrites with the tail partition id. The child-value slice is
    aligned to the list array's offset so sliced Arrow batches are assigned from their own rows.

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
    values: pa.Array = vectors.values.slice(vectors.offset * dimension, len(vectors) * dimension)
    matrix: np.ndarray = values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    matrix = matrix.reshape(len(vectors), dimension)
    centroid_matrix: np.ndarray = centroids.astype(np.float32, copy=False)
    centroid_norms_sq: np.ndarray = np.einsum("ij,ij->i", centroid_matrix, centroid_matrix)
    normalised_centroids: np.ndarray = normalise_rows(centroid_matrix) if distance_type == "cosine" else centroid_matrix
    out: np.ndarray = np.empty(len(vectors), dtype=np.int64)
    block_rows: int = assignment_block_rows(len(centroid_matrix), dimension, distance_type)
    for start in range(0, len(vectors), block_rows):
        block: np.ndarray = matrix[start : start + block_rows]
        out[start : start + block_rows] = assign_block(
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
    """Pack a partition-id histogram into contiguous write buckets near a target size.

    Contiguous partition ids are packed into buckets whose total row count stays at or below
    ``rows_per_task``, so global bucket order follows partition order and at most one centroid
    straddles a fragment boundary. A single partition larger than the cap cannot be packed with its
    neighbours, so it splits into ``ceil(count / rows_per_task)`` salted sub-buckets. Rows within one
    centroid need no internal order, so they fan out across sub-buckets near the target. Independent
    read shards can produce a small imbalance, bounded by one row per contributing shard. The write
    side remains memory-bounded by streamed shuffle chunks, and ``write_fragments`` enforces
    ``rows_per_task`` as the hard per-file cap even when a salted bucket exceeds its target. The last
    histogram slot is the null tail partition and is packed uniformly with the rest, so it always
    appears in a bucket when it carries rows.

    Args:
        counts: Per-partition row counts, length ``num_partitions + 1`` with the null tail last.
        rows_per_task: Target rows per bucket and hard row cap per rewritten fragment.

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

    Resolves the vector column and guards eligibility, runs retention so expired rows are never
    rewritten, resolves the reusable centroids sidecar-first, and pins the post-retention read version
    with its schema, row count, and fragment shards. Retention runs before an already-clustered
    fingerprint is honored, so wall-clock expiry still invalidates an otherwise unchanged generation.
    A dataset that remains current returns a terminal ``cluster_current`` dict so it is neither
    re-clustered nor routed into normal compaction, which would undo its centroid ordering. It is not
    fully skipped, though: it first runs the same rotation-gated idle version
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
    retention_rows_deleted: int = 0
    retention_checked: bool = False
    current: str | None = cluster_generation_skip_reason(dataset)
    if current is not None and config.retention_active() and cutoff is not None:
        retention_result: dict[str, Any] = maintenance_job.run_retention_on_open_dataset(
            dataset, uri, config, cutoff, telemetry
        )
        retention_checked = True
        retention_rows_deleted = int(retention_result.get("retention_rows_deleted", 0))
        if retention_result.get("error"):
            return {**retention_result, "bytes_removed": 0}
        if retention_rows_deleted > 0:
            dataset = lance.dataset(uri, storage_options=config.storage_options)
            current = cluster_generation_skip_reason(dataset)
    if current is not None:
        telemetry.incr("cluster.skipped_already_clustered")
        bytes_removed: int = maintenance_job.idle_cleanup_bytes(
            uri, config, telemetry, dataset, retention_rows_deleted > 0, cleanup_slot
        )
        return {
            "uri": uri,
            "cluster_current": current,
            "bytes_removed": bytes_removed,
            "retention_rows_deleted": retention_rows_deleted,
        }
    column, skip = resolve_cluster_column(dataset, config)
    if column is None:
        return {"uri": uri, "cluster_skipped": skip, "retention_rows_deleted": retention_rows_deleted}
    index_name: str = vector_index_name(column)
    guard: str | None = cluster_guard_reason(dataset, column, index_name)
    if guard is not None:
        return {"uri": uri, "cluster_skipped": guard, "retention_rows_deleted": retention_rows_deleted}
    cfg: dict[str, Any] | None = load_vector_config(dataset, column)
    if cfg is None:
        return {
            "uri": uri,
            "cluster_skipped": "vector config vanished after the guard check",
            "retention_rows_deleted": retention_rows_deleted,
        }

    if not retention_checked and config.retention_active() and cutoff is not None:
        retention_result: dict[str, Any] = maintenance_job.run_retention_on_open_dataset(
            dataset, uri, config, cutoff, telemetry
        )
        retention_rows_deleted = int(retention_result.get("retention_rows_deleted", 0))
        if retention_result.get("error"):
            return {**retention_result, "bytes_removed": 0}
        if retention_rows_deleted > 0:
            dataset = lance.dataset(uri, storage_options=config.storage_options)
    total_rows: int = dataset.count_rows()
    if total_rows == 0:
        return {
            "uri": uri,
            "cluster_skipped": "empty dataset after retention; normal maintenance will materialize deletions",
            "retention_rows_deleted": retention_rows_deleted,
        }

    rows_at_train: int = int(cfg["rows_at_train"])
    metric: str = str(cfg["metric"])
    distance_type: str = METRIC_TO_DISTANCE.get(metric.lower(), "l2")
    centroids: pa.Array = resolve_cluster_centroids(dataset, uri, index_name, rows_at_train, metric, config, telemetry)
    fragment_count: int = int(dataset.stats.dataset_stats()["num_fragments"])
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
        "total_rows": total_rows,
        "schema": dataset.schema,
        "fragment_count": fragment_count,
        "centroids_ipc": encode_centroids(centroids),
        "retention_rows_deleted": retention_rows_deleted,
    }


def read_task_seeds(plans: list[dict[str, Any]]) -> list[ClusterReadSeed]:
    """Build bounded dataset-level seeds for histogram and rewrite fragment enumeration.

    Fragment identifiers stay off the driver. Each seed carries only the pinned version and probed
    fragment count needed for an executor to verify and shard the inventory.

    Args:
        plans: The eligible plan dicts.

    Returns:
        One bounded seed per eligible dataset.
    """
    return [
        (
            str(plan["uri"]),
            int(plan["read_version"]),
            int(plan["fragment_count"]),
            str(plan["column"]),
            str(plan["distance_type"]),
        )
        for plan in plans
    ]


def exact_fragment_shards(
    uri: str,
    version: int,
    expected_count: int,
    storage_options: dict[str, Any] | None,
) -> list[list[int]]:
    """Open one pinned dataset on an executor and verify its fragment inventory before sharding.

    Args:
        uri: Dataset URI.
        version: Exact pinned dataset version.
        expected_count: Fragment count observed by the plan phase.
        storage_options: Object-store options forwarded to lance.

    Returns:
        At most :data:`CLUSTER_READ_SHARDS` non-empty fragment-id shards.

    Raises:
        RuntimeError: If the pinned inventory differs from the plan.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=storage_options)
    fragment_ids: list[int] = all_fragment_ids(dataset)
    if len(fragment_ids) != expected_count:
        raise RuntimeError(f"fragment inventory changed for {uri}: planned {expected_count}, found {len(fragment_ids)}")
    return split_evenly(fragment_ids, CLUSTER_READ_SHARDS)


def enumerate_cluster_read_tasks(
    seed: ClusterReadSeed,
    storage_options: dict[str, Any] | None,
) -> Iterator[ClusterReadTask]:
    """Expand one dataset seed into fragment shards on an executor.

    Args:
        seed: Dataset URI, pinned version, fragment count, vector column, and distance type.
        storage_options: Object-store options forwarded to lance.

    Yields:
        Read shards, or one error-bearing task when the inventory cannot be reproduced.
    """
    uri, version, expected_count, column, distance_type = seed
    try:
        shards: list[list[int]] = exact_fragment_shards(uri, version, expected_count, storage_options)
    except Exception as exc:
        yield uri, version, [], column, distance_type, str(exc)
        return
    for shard in shards:
        yield uri, version, shard, column, distance_type, None


def enumerate_cluster_index_tasks(
    seed: ClusterIndexSeed,
    storage_options: dict[str, Any] | None,
) -> Iterator[ClusterIndexShardTask]:
    """Expand one committed dataset's rebuild seed into verified fragment shards on an executor.

    Args:
        seed: Small index-rebuild dataset seed.
        storage_options: Object-store options forwarded to lance.

    Yields:
        Index shard tasks, or one error-bearing task when the inventory cannot be reproduced.
    """
    uri, version, expected_count, column, index_name, metric, num_bits, num_partitions = seed
    try:
        shards: list[list[int]] = exact_fragment_shards(uri, version, expected_count, storage_options)
    except Exception as exc:
        yield uri, version, [], column, index_name, metric, num_bits, num_partitions, str(exc)
        return
    for shard in shards:
        yield uri, version, shard, column, index_name, metric, num_bits, num_partitions, None


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


def initial_salt_offsets(salted: dict[int, np.ndarray], shard: list[int]) -> dict[int, int]:
    """Choose deterministic salted-bucket offsets from one fragment shard's identity.

    Each rewrite task owns its own mutable offsets, so starting every task at salt zero would send
    the first row of every tiny fragment shard to the same global bucket. A stable weighted fragment
    signature rotates adjacent singleton shards across salts while remaining independent of Spark
    scheduling and Python's randomized hash seed. Batch-level assignment advances these offsets
    normally after this initial rotation.

    Args:
        salted: Salted partition sub-bucket lookups.
        shard: Exact fragment ids scanned by one rewrite task.

    Returns:
        Initial next-salt offsets keyed by salted partition id.
    """
    shard_signature: int = sum((position + 1) * fragment_id for position, fragment_id in enumerate(shard))
    return {pid: int((shard_signature + pid) % len(sub_buckets)) for pid, sub_buckets in salted.items()}


def assign_global_buckets(
    pids: np.ndarray,
    pid_to_global: np.ndarray,
    salted: dict[int, np.ndarray],
    salt_offsets: dict[int, int],
) -> np.ndarray:
    """Map rows to global buckets while continuing oversized-partition salts across batches.

    Args:
        pids: The per-row partition ids for one batch.
        pid_to_global: The non-salted partition-to-global lookup from :func:`build_bucket_lookup`.
        salted: The salted partition sub-bucket lookup from :func:`build_bucket_lookup`.
        salt_offsets: Mutable next-salt offsets by partition id for the current read shard. Callers
            seed them from :func:`initial_salt_offsets` so separate shards do not all start at zero.

    Returns:
        A ``(len(pids),)`` int64 array of global bucket ids.
    """
    globals_out: np.ndarray = pid_to_global[pids]
    for pid, sub_buckets in salted.items():
        mask: np.ndarray = pids == pid
        selected: int = int(mask.sum())
        if selected:
            offset: int = salt_offsets.get(pid, 0)
            salt: np.ndarray = (np.arange(selected) + offset) % len(sub_buckets)
            globals_out[mask] = sub_buckets[salt]
            salt_offsets[pid] = int((offset + selected) % len(sub_buckets))
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
) -> Iterator[tuple[tuple[int, int], bytes]]:
    """Read a fragment shard and emit chunks keyed by global bucket and centroid partition.

    Each batch is assigned, tagged with the temporary partition-id column, and split by global
    bucket plus exact centroid partition. The composite shuffle key lets Spark sort partitions
    within each output bucket without retaining the bucket in Python. The largest buffered key
    flushes to Arrow IPC whenever the combined shard buffer reaches
    :data:`SHUFFLE_CHUNK_BYTES`, and emitted chunks are yielded immediately rather than retained
    until the shard scan ends.

    Args:
        uri: Dataset URI.
        version: The pinned read version every shard reads.
        shard: The fragment ids assigned to this task.
        column: The vector column to assign on.
        centroids_ipc: The broadcast centroid array as IPC bytes.
        distance_type: The Lance distance type.
        global_buckets: ``(global_id, start_pid, end_pid, salt, num_salts)`` tuples for this dataset.
        storage_options: Object-store options forwarded to lance.

    Yields:
        ``((global_bucket, partition_id), ipc_bytes)`` pairs carrying tagged rows for the sorted shuffle.
    """
    centroids: np.ndarray = centroids_to_matrix(decode_centroids(centroids_ipc))
    num_partitions: int = centroids.shape[0]
    pid_to_global, salted = build_bucket_lookup(global_buckets, num_partitions)
    dataset: lance.LanceDataset = lance.dataset(uri, version=version, storage_options=storage_options)
    wanted: set[int] = set(shard)
    fragments: list[Any] = [fragment for fragment in dataset.get_fragments() if fragment.fragment_id in wanted]
    buffers: dict[tuple[int, int], list[pa.Table]] = {}
    sizes: dict[tuple[int, int], int] = {}
    buffered_bytes: int = 0
    salt_offsets: dict[int, int] = initial_salt_offsets(salted, shard)
    reader: pa.RecordBatchReader = dataset.scanner(fragments=fragments).to_reader()
    for batch in reader:
        pids: np.ndarray = partition_ids_for_batch(batch.column(column), centroids, distance_type, num_partitions)
        tagged: pa.Table = pa.Table.from_batches([batch]).append_column(
            CLUSTER_PARTITION_COLUMN, pa.array(pids.astype(np.int32), pa.int32())
        )
        globals_out: np.ndarray = assign_global_buckets(pids, pid_to_global, salted, salt_offsets)
        for global_id in np.unique(globals_out):
            global_mask: np.ndarray = globals_out == global_id
            for partition_id in np.unique(pids[global_mask]):
                mask: np.ndarray = global_mask & (pids == partition_id)
                slice_table: pa.Table = tagged.filter(pa.array(mask))
                key: tuple[int, int] = (int(global_id), int(partition_id))
                buffers.setdefault(key, []).append(slice_table)
                sizes[key] = sizes.get(key, 0) + slice_table.nbytes
                buffered_bytes += slice_table.nbytes
                if buffered_bytes >= SHUFFLE_CHUNK_BYTES:
                    flushed_key, chunk, released_bytes = pop_largest_bucket(buffers, sizes)
                    buffered_bytes -= released_bytes
                    yield flushed_key, chunk
    while buffers:
        flushed_key, chunk, released_bytes = pop_largest_bucket(buffers, sizes)
        buffered_bytes -= released_bytes
        yield flushed_key, chunk
    if buffered_bytes != 0:
        raise RuntimeError("rewrite bucket byte accounting did not drain to zero")


def pop_largest_bucket(
    buffers: dict[tuple[int, int], list[pa.Table]],
    sizes: dict[tuple[int, int], int],
) -> tuple[tuple[int, int], bytes, int]:
    """Remove and serialize the largest buffered rewrite bucket.

    Args:
        buffers: Non-empty per-bucket accumulated table slices, mutated in place.
        sizes: Matching per-bucket accumulated byte sizes, mutated in place.

    Returns:
        Global bucket and partition key, serialized IPC chunk, and released approximate bytes.

    Raises:
        ValueError: If the buffers and sizes are empty or inconsistent.
    """
    if not buffers or set(buffers) != set(sizes):
        raise ValueError("rewrite bucket buffers and sizes must be non-empty and aligned")
    key: tuple[int, int] = max(sizes, key=lambda candidate: sizes[candidate])
    released_bytes: int = sizes.pop(key)
    tables: list[pa.Table] = buffers.pop(key)
    return key, table_to_ipc(pa.concat_tables(tables)), released_bytes


def bucket_record_batches(chunks: Iterator[bytes], schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    """Decode one centroid-sorted shuffled bucket incrementally and drop its routing column.

    Args:
        chunks: Arrow IPC chunks sorted by centroid partition for one global bucket.
        schema: Pinned output dataset schema.

    Yields:
        Output-schema record batches while retaining only one decoded chunk at a time.
    """
    for chunk in chunks:
        table: pa.Table = table_from_ipc(chunk).select(schema.names)
        yield from table.to_batches()


def write_bucket(
    global_bucket: int,
    chunks: Iterator[bytes],
    uri: str,
    schema: pa.Schema,
    rows_per_task: int,
    storage_options: dict[str, Any] | None,
) -> list[tuple[int, int, str, int]]:
    """Stream one global bucket into new fragment files.

    Bucket derivation assigns each global bucket a contiguous IVF partition range and caps it at
    ``rows_per_task`` rows. The bucket therefore maps to at most one normal output fragment without
    an in-memory sort. Streaming decoded record batches into ``write_fragments`` keeps executor
    memory bounded by one shuffle chunk even for wide rows.

    ``uri`` is the dataset being rewritten, so it already exists. ``write_fragments`` rejects
    ``mode="create"`` against an existing dataset directory with ``Error::dataset_already_exists``
    (lance validates ``WriteMode::Create`` only against a destination with no committed dataset
    yet). ``mode="overwrite"`` assigns the same fresh field ids as ``"create"`` — required so the
    written fragments carry the pinned schema's ids for the coming
    :func:`~lance_etl.maintenance.job` ``LanceOperation.Overwrite`` commit — while being accepted
    against an existing destination; it commits nothing by itself, since ``write_fragments`` only
    ever returns uncommitted fragment metadata for the driver to commit later.

    Args:
        global_bucket: The global bucket id, for stable ordering on the driver.
        chunks: Streaming Arrow IPC chunks routed to this bucket.
        uri: Target dataset URI the fragment files are written under.
        schema: The pinned dataset schema the fragments are created with.
        rows_per_task: Row cap per written fragment file.
        storage_options: Object-store options forwarded to lance.

    Returns:
        One ``(global_bucket, seq, fragment_json, rows)`` tuple per written fragment.
    """
    reader: pa.RecordBatchReader = pa.RecordBatchReader.from_batches(schema, bucket_record_batches(chunks, schema))
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
    read_version: int | None = None,
) -> int:
    """Commit the rewritten fragments over a dataset with ``LanceOperation.Overwrite``.

    The overwrite preserves version history, tags, and the dataset config KV (column roles and the
    vector config survive), and drops every index so the rebuild phase can re-create the vector
    index. The commit is pinned to the plan's read version and every retry re-opens the latest
    dataset first. A concurrent post-plan write therefore fails the clustered rewrite instead of
    being silently clobbered. Quiescing the dataset is still required for a successful run.

    This primitive commits data only. The orchestrated path stamps the clustered-generation
    fingerprint after the preserved-centroid index rebuild succeeds. A failed rebuild therefore
    cannot leave a generation marked current while its replacement index is absent or trained from
    different centroids.

    Args:
        uri: Dataset URI.
        fragment_documents: JSON fragment metadata collected from the rewrite shuffle.
        schema: The pinned dataset schema the new version is created with.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.
        read_version: Dataset version the rewrite scanned. Direct callers may omit it to pin the
            latest version visible when this function starts.

    Returns:
        The number of fragments committed.
    """
    committed: ClusterOverwriteCommit = commit_cluster_data(
        uri,
        fragment_documents,
        schema,
        config,
        telemetry,
        read_version,
    )
    return committed.fragments_added


def commit_cluster_data(
    uri: str,
    fragment_documents: list[str],
    schema: pa.Schema,
    config: MaintenanceConfig,
    telemetry: Telemetry,
    read_version: int | None = None,
) -> ClusterOverwriteCommit:
    """Commit only the clustered data overwrite and return its exact fingerprint.

    Separating the destructive overwrite from the derived generation stamp lets the orchestrator
    recover the preserved-centroid vector index before declaring the rewritten generation current.

    Args:
        uri: Dataset URI.
        fragment_documents: JSON fragment metadata collected from the rewrite shuffle.
        schema: Pinned dataset schema.
        config: Maintenance configuration.
        telemetry: Executor telemetry facade.
        read_version: Dataset version the rewrite scanned, or the latest version for direct callers.

    Returns:
        Committed fragment count and exact data-generation fingerprint.
    """
    fragments: list[FragmentMetadata] = [FragmentMetadata.from_json(document) for document in fragment_documents]
    planned_version: int = (
        read_version if read_version is not None else lance.dataset(uri, storage_options=config.storage_options).version
    )

    def action() -> lance.LanceDataset:
        """Verify the plan version is still current, then commit the overwrite."""
        current: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if current.version != planned_version:
            raise RuntimeError(
                f"cluster rewrite for {uri} planned version {planned_version}, but the dataset advanced to "
                f"version {current.version}; refusing to overwrite concurrent writes"
            )
        operation = lance.LanceOperation.Overwrite(schema, fragments)
        committed: lance.LanceDataset = lance.LanceDataset.commit(
            uri,
            operation,
            read_version=planned_version,
            storage_options=config.storage_options,
            enable_v2_manifest_paths=True,
            max_retries=0,
        )
        telemetry.incr("cluster.overwritten")
        return committed

    committed_dataset: lance.LanceDataset = commit_with_retries(
        action,
        config.large_commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("cluster.overwrite_conflict"),
    )
    fragment_ids: list[int] | None = None
    fingerprint_error: str | None = None
    try:
        fragment_ids = fragment_id_signature(committed_dataset)
    except Exception as exc:
        fingerprint_error = str(exc)
        telemetry.incr("cluster.committed_fingerprint_error")
        logger.warning(
            "cluster: committed overwrite fingerprint could not be read for %s, continuing to index recovery: %s",
            uri,
            exc,
        )
    return ClusterOverwriteCommit(
        fragments_added=len(fragments),
        fragment_ids=fragment_ids,
        num_rows=sum(int(fragment.physical_rows) for fragment in fragments),
        fingerprint_error=fingerprint_error,
    )


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


def fan_out_cluster_payloads(
    spark: SparkSession,
    payloads: list[Any],
    telemetry_config: TelemetryConfig,
    per_payload: Callable[[Any, Telemetry], dict[str, Any]],
    partitions: int,
    phase: str,
) -> list[dict[str, Any]]:
    """Run phase-specific clustered payloads without capturing a driver-side lookup map.

    The generic fleet fan-out accepts only URI strings, which tempts callers to close over a full
    ``uri -> plan`` map. Cluster plans carry schemas, centroid IPC, and RaBitQ artifacts, so that
    closure serializes every dataset's heavy plan into every Spark task. This variant parallelizes
    already-trimmed commit or finalisation payloads directly and retains the same per-dataset
    failure isolation and executor-local telemetry contract.

    Args:
        spark: Active Spark session.
        payloads: Minimal phase-specific payloads carrying a ``uri`` attribute.
        telemetry_config: Telemetry configuration created per executor process.
        per_payload: Operation applied to one payload and executor-local telemetry facade.
        partitions: Upper bound on Spark partitions.
        phase: Failure phase written to isolated error results and telemetry.

    Returns:
        One outcome per payload, with raised errors converted to per-dataset error markers.
    """
    if not payloads:
        return []

    def run_partition(items: Iterable[Any]) -> Iterator[dict[str, Any]]:
        """Apply one clustered phase to the payloads in an executor partition.

        Args:
            items: Phase payloads assigned to this executor partition.

        Yields:
            Successful outcomes or isolated error markers.
        """
        executor_telemetry: Telemetry = Telemetry.create(telemetry_config)
        for payload in items:
            uri: str = str(payload.uri)
            try:
                yield per_payload(payload, executor_telemetry)
            except Exception as exc:
                logger.warning("cluster: %s failed in phase %s, isolating: %s", uri, phase, exc)
                executor_telemetry.incr("dataset.fanout_error", tags=[f"phase:{phase}"])
                yield {"uri": uri, "error": str(exc), "phase": phase}

    slices: int = min(len(payloads), partitions)
    return spark.sparkContext.parallelize(payloads, slices).mapPartitions(run_partition).collect()


def run_cluster_rewrites(
    spark: SparkSession,
    uris: list[str],
    config: MaintenanceConfig,
    cutoff: datetime | None,
    driver_telemetry: Telemetry,
    cleanup_slot: int | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Cluster-rewrite eligible datasets, returning results and the passthrough set.

    The orchestration processes bounded dataset batches through plan fan-out, an executor-reduced
    histogram job, driver bucket derivation, a flat rewrite shuffle, per-dataset overwrite commit,
    vector-index rebuild, and cleanup. No serving tag is ever touched here: promotion happens
    exclusively through the pipeline stamp phase after the index phase (ADR 0041). Per-dataset
    failure isolation holds at every phase: a failed dataset carries an ``{"error", "phase"}`` marker and drops out of
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
        for offset in range(0, len(uris), CLUSTER_DATASET_BATCH_SIZE):
            eligible: list[dict[str, Any]] = state.plan(uris[offset : offset + CLUSTER_DATASET_BATCH_SIZE], cutoff)
            if not eligible:
                continue
            counts_by_uri: dict[str, list[int]] = state.histogram(eligible)
            eligible = [plan for plan in eligible if plan["uri"] in counts_by_uri]
            documents_by_uri: dict[str, list[str]] = state.rewrite(eligible, counts_by_uri)
            eligible = [plan for plan in eligible if plan["uri"] in documents_by_uri]
            state.commit_and_index(eligible, documents_by_uri)
        run_span.set_tag("clustered", sum(1 for item in results.values() if item.get("clustered")))
    return results, passthrough


@dataclass
class ClusterRunState:
    """Carries the shared driver state threaded through the clustered-rewrite phases."""

    spark: SparkSession
    config: MaintenanceConfig
    telemetry: Telemetry
    results: dict[str, dict[str, Any]]
    passthrough: list[str]
    cleanup_slot: int | None = None

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
                    "retention_rows_deleted": plan.get("retention_rows_deleted", 0),
                }
            elif "cluster_skipped" in plan:
                if int(plan.get("retention_rows_deleted", 0)) > 0:
                    self.results[uri] = {
                        "uri": uri,
                        "retention_rows_deleted": plan["retention_rows_deleted"],
                    }
                self.passthrough.append(uri)
            else:
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
        """Run the flat histogram job and reduce shard counts on executors.

        Args:
            plans: The eligible plan dicts.

        Returns:
            The per-dataset histograms, excluding any dataset whose histogram task failed.
        """
        centroids = self.broadcast_centroids(plans)
        storage_options: dict[str, Any] | None = self.config.storage_options
        seeds: list[ClusterReadSeed] = read_task_seeds(plans)
        expected_shards: int = sum(min(int(plan["fragment_count"]), CLUSTER_READ_SHARDS) for plan in plans)

        def run_one(item: ClusterReadTask) -> tuple[str, tuple[str, Any]]:
            """Count one shard's histogram, keyed and tagged by dataset."""
            uri: str = item[0]
            if item[5] is not None:
                return uri, (FLAT_ERROR, item[5])
            try:
                counts: list[int] = partition_histogram(
                    uri, item[1], item[2], item[3], centroids.value[uri], item[4], storage_options
                )
                return uri, (FLAT_OK, counts)
            except Exception as exc:
                return uri, (FLAT_ERROR, str(exc))

        def merge_counts(left: tuple[str, Any], right: tuple[str, Any]) -> tuple[str, Any]:
            """Associatively reduce histogram results while preserving a failure.

            Args:
                left: First tagged shard result.
                right: Second tagged shard result.

            Returns:
                Summed counts or one deterministic dataset error.
            """
            if left[0] == FLAT_ERROR and right[0] == FLAT_ERROR:
                return FLAT_ERROR, min(str(left[1]), str(right[1]))
            if left[0] == FLAT_ERROR:
                return left
            if right[0] == FLAT_ERROR:
                return right
            left_counts: list[int] = left[1]
            right_counts: list[int] = right[1]
            if len(left_counts) != len(right_counts):
                return FLAT_ERROR, "histogram shards returned different partition widths"
            return FLAT_OK, [first + second for first, second in zip(left_counts, right_counts, strict=True)]

        try:
            partitions: int = self.flat_partitions(expected_shards)
            enumeration_slices: int = max(1, min(self.flat_partitions(len(seeds)), len(seeds)))
            reduced: list[tuple[str, tuple[str, Any]]] = (
                self.spark.sparkContext.parallelize(seeds, enumeration_slices)
                .flatMap(lambda seed: enumerate_cluster_read_tasks(seed, storage_options))
                .repartition(partitions)
                .map(run_one)
                .reduceByKey(merge_counts, partitions)
                .collect()
            )
        finally:
            release_broadcast(centroids)
        counts_by_uri: dict[str, list[int]] = {}
        for uri, result in reduced:
            if result[0] == FLAT_ERROR:
                self.results[uri] = {
                    "uri": uri,
                    "error": str(result[1]),
                    "phase": "cluster-histogram",
                    "bytes_removed": 0,
                }
            else:
                counts_by_uri[uri] = result[1]
        return counts_by_uri

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
        try:
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
        finally:
            for handle in (centroids, buckets, owners, meta):
                release_broadcast(handle)
        return self.validate_rewrite(plans, collected)

    def validate_rewrite(self, plans: list[dict[str, Any]], collected: list[tuple[Any, ...]]) -> dict[str, list[str]]:
        """Order fragment documents per dataset and validate the written row total.

        The collected shuffle results are tagged: :data:`REWRITE_OK` tuples carry a written
        fragment document, while :data:`REWRITE_ERROR` tuples carry a per-dataset failure message
        emitted by an isolated read task or bucket write. A dataset with any dropped read or bucket
        fails closed even if chunks emitted before the failure happen to cover the planned row
        count. A plain count mismatch is also rejected. Every other dataset proceeds.

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
            if uri in errors or written != plan["total_rows"]:
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
        payloads: list[ClusterCommitPayload] = [
            ClusterCommitPayload(
                uri=uri,
                fragment_documents=documents,
                schema=plan_by_uri[uri]["schema"],
                read_version=int(plan_by_uri[uri]["read_version"]),
            )
            for uri, documents in documents_by_uri.items()
        ]
        outcomes: list[dict[str, Any]] = fan_out_cluster_payloads(
            self.spark,
            payloads,
            config.telemetry,
            lambda payload, telemetry: commit_one_overwrite(payload, config, telemetry),
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
                plan_by_uri[uri]["cluster_fragment_ids"] = outcome["fragment_ids"]
                plan_by_uri[uri]["cluster_num_rows"] = outcome["num_rows"]
                plan_by_uri[uri]["cluster_fingerprint_error"] = outcome["fingerprint_error"]
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
        seeds: list[ClusterIndexSeed] = self.shard_committed(plan_by_uri)
        if not plan_by_uri:
            return
        artifacts = self.spark.sparkContext.broadcast(
            {uri: (plan["centroids_ipc"], plan["rabitq_model"]) for uri, plan in plan_by_uri.items()}
        )
        try:
            segments_by_uri, build_errors = self.build_rebuild_segments(seeds, artifacts, config.storage_options)
        finally:
            release_broadcast(artifacts)
        for uri, message in build_errors.items():
            self.results[uri] = mark_rebuild_failure(plan_by_uri[uri], message)
            plan_by_uri.pop(uri, None)
        if not plan_by_uri:
            return
        payloads: list[ClusterFinalisePayload] = [
            ClusterFinalisePayload(
                uri=uri,
                segment_documents=segments_by_uri.get(uri, []),
                column=str(plan["column"]),
                index_name=str(plan["index_name"]),
                metric=str(plan["metric"]),
                num_partitions=int(plan["num_partitions"]),
                fragments_added=int(plan.get("fragments_added", 0)),
                retention_rows_deleted=int(plan.get("retention_rows_deleted", 0)),
                fragment_ids=list(plan["cluster_fragment_ids"]) if plan["cluster_fragment_ids"] is not None else None,
                num_rows=int(plan["cluster_num_rows"]),
                fingerprint_error=plan["cluster_fingerprint_error"],
            )
            for uri, plan in plan_by_uri.items()
        ]
        outcomes: list[dict[str, Any]] = fan_out_cluster_payloads(
            self.spark,
            payloads,
            config.telemetry,
            lambda payload, telemetry: finalise_cluster_dataset(payload, config, telemetry),
            self.fanout_partitions(),
            phase="cluster-index",
        )
        for outcome in outcomes:
            self.results[outcome["uri"]] = outcome

    def shard_committed(self, plan_by_uri: dict[str, dict[str, Any]]) -> list[ClusterIndexSeed]:
        """Probe each committed dataset and return bounded rebuild seeds.

        Opens each committed dataset on an executor to pin its version and fragment count. Exact
        fragment identifiers are enumerated later inside the flat rebuild job, so they never cross
        the driver. A dataset whose probe fails drops out with a ``cluster_index`` error marker.

        Args:
            plan_by_uri: The committed plan dicts by URI, mutated in place to drop failed datasets.

        Returns:
            One small rebuild seed per still-eligible committed dataset.
        """
        config: MaintenanceConfig = self.config
        shard_plans: list[dict[str, Any]] = fan_out_per_dataset(
            self.spark,
            list(plan_by_uri),
            config.telemetry,
            lambda uri, telemetry: plan_rebuild_inventory(uri, config.storage_options, telemetry),
            self.fanout_partitions(),
            phase="cluster-index",
        )
        seeds: list[ClusterIndexSeed] = []
        for shard_plan in shard_plans:
            uri: str = shard_plan["uri"]
            if "error" in shard_plan:
                self.results[uri] = mark_rebuild_failure(plan_by_uri[uri], shard_plan["error"])
                plan_by_uri.pop(uri, None)
                continue
            plan_by_uri[uri]["committed_version"] = shard_plan["committed_version"]
            plan: dict[str, Any] = plan_by_uri[uri]
            seeds.append(
                (
                    uri,
                    int(shard_plan["committed_version"]),
                    int(shard_plan["fragment_count"]),
                    str(plan["column"]),
                    str(plan["index_name"]),
                    str(plan["metric"]),
                    int(plan["num_bits"]),
                    int(plan["num_partitions"]),
                )
            )
        return seeds

    def build_rebuild_segments(
        self, seeds: list[ClusterIndexSeed], artifacts: Any, storage_options: dict[str, Any] | None
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
            seeds: Small dataset-level rebuild seeds.
            artifacts: Broadcast ``{uri: (centroids_ipc, rabitq_model)}`` handle.
            storage_options: Object-store options forwarded to lance.

        Returns:
            A ``(segments_by_uri, errors_by_uri)`` pair. The first maps each fully-built dataset URI
            to its serialised segment documents; the second maps each dataset with a failed shard to
            its first failure message.
        """

        def build_one(item: ClusterIndexShardTask) -> tuple[str, str, str]:
            """Build one shard's segment, tagging success or failure by dataset URI."""
            uri, version, shard, column, index_name, metric, num_bits, num_partitions, inventory_error = item
            if inventory_error is not None:
                return FLAT_ERROR, uri, inventory_error
            centroids_ipc, rabitq_model = artifacts.value[uri]
            try:
                document: str = build_cluster_index_segment(
                    uri,
                    version,
                    shard,
                    column,
                    index_name,
                    metric,
                    num_bits,
                    num_partitions,
                    centroids_ipc,
                    rabitq_model,
                    storage_options,
                )
                return FLAT_OK, uri, document
            except Exception as exc:
                return FLAT_ERROR, uri, str(exc)

        if not seeds:
            return {}, {}
        expected_shards: int = sum(min(seed[2], CLUSTER_READ_SHARDS) for seed in seeds)
        partitions: int = self.flat_partitions(expected_shards)
        enumeration_slices: int = max(1, min(self.flat_partitions(len(seeds)), len(seeds)))
        tagged: list[tuple[str, str, str]] = (
            self.spark.sparkContext.parallelize(seeds, enumeration_slices)
            .flatMap(lambda seed: enumerate_cluster_index_tasks(seed, storage_options))
            .repartition(partitions)
            .map(build_one)
            .collect()
        )
        grouped: dict[str, list[str]] = {}
        errors: dict[str, str] = {}
        for tag, uri, value in tagged:
            if tag == FLAT_OK:
                grouped.setdefault(uri, []).append(value)
            else:
                errors.setdefault(uri, value)
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

    The read job emits ``((global_bucket, partition_id), ipc_bytes)`` pairs.
    ``repartitionAndSortWithinPartitions`` co-locates each global bucket and sorts its chunks by
    centroid partition before the write side streams fragments. Each global bucket belongs to
    exactly one dataset, looked up through the broadcast owner map.

    Per-task failures are non-fatal and attributed to their dataset rather than aborting the whole
    run. A failing read task or bucket write emits a :data:`REWRITE_ERROR` sentinel through the same
    shuffle instead of raising, so the owning dataset loses rows and fails the downstream row-count
    validation while every other dataset still commits. Read-failure sentinels use the partition
    immediately after the last live global bucket, so they never collide with bucket data.

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
        total_buckets: The global bucket count. The shuffle adds one error partition.
        storage_options: Object-store options forwarded to lance.

    Returns:
        Tagged results, either ``(REWRITE_OK, uri, global_bucket, seq, fragment_json, rows)`` for a
        written fragment or ``(REWRITE_ERROR, uri, message)`` for an isolated read or write failure.
    """
    read_seeds: list[ClusterReadSeed] = read_task_seeds(plans)
    if not read_seeds or total_buckets == 0:
        return []

    def read_partition(items: Any) -> Any:
        """Emit ``(global_bucket, ipc_bytes)`` pairs, isolating a failing read task per dataset.

        A read task whose :func:`read_rewrite_chunks` raises yields one error sentinel and nothing
        else, so the failure travels the shuffle attributed to its dataset instead of killing the job.
        """
        for uri, version, shard, column, distance_type, inventory_error in items:
            if inventory_error is not None:
                yield ((total_buckets, -1), (uri, inventory_error))
                continue
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
                yield ((total_buckets, -1), (uri, str(exc)))

    def write_partition(items: Any) -> Any:
        """Group co-located chunks by global bucket and write each bucket's fragments, isolating failures.

        The dedicated read-failure partition passes through ``(REWRITE_ERROR, uri, message)``
        results. A bucket whose :func:`write_bucket` raises yields one error result instead of
        killing the job, so only the owning dataset fails validation.
        """
        for global_bucket, grouped_items in groupby(items, key=lambda item: item[0][0]):
            if global_bucket == total_buckets:
                for item in grouped_items:
                    error_uri, message = item[1]
                    yield (REWRITE_ERROR, error_uri, message)
                continue
            uri: str = owners.value[global_bucket]
            schema, rows_per_task = meta.value[uri]
            chunks: Iterator[bytes] = (item[1] for item in grouped_items)
            try:
                for entry in write_bucket(global_bucket, chunks, uri, schema, rows_per_task, storage_options):
                    yield (REWRITE_OK, uri, entry[0], entry[1], entry[2], entry[3])
            except Exception as exc:
                logger.warning("cluster: rewrite write bucket %d failed for %s: %s", global_bucket, uri, exc)
                yield (REWRITE_ERROR, uri, str(exc))

    expected_shards: int = sum(min(int(plan["fragment_count"]), CLUSTER_READ_SHARDS) for plan in plans)
    slices: int = max(1, min(expected_shards, derive_partitions(spark, REWRITE_PARTITION_FACTOR)))
    enumeration_slices: int = max(1, min(len(read_seeds), derive_partitions(spark, FANOUT_PARTITION_FACTOR)))
    return (
        spark.sparkContext.parallelize(read_seeds, enumeration_slices)
        .flatMap(lambda seed: enumerate_cluster_read_tasks(seed, storage_options))
        .repartition(slices)
        .mapPartitions(read_partition)
        .repartitionAndSortWithinPartitions(
            numPartitions=total_buckets + 1,
            partitionFunc=lambda key: int(key[0]),
        )
        .mapPartitions(write_partition)
        .collect()
    )


def plan_rebuild_inventory(uri: str, storage_options: dict[str, Any] | None, telemetry: Telemetry) -> dict[str, Any]:
    """Pin a rewritten dataset's committed version and fragment count on an executor.

    Runs inside the rebuild fan-out so no dataset is opened on the driver. Fragment identifiers are
    enumerated later inside the flat segment-build job.

    Args:
        uri: Dataset URI.
        storage_options: Object-store options forwarded to lance.
        telemetry: Telemetry facade for the current process.

    Returns:
        A ``{"uri", "committed_version", "fragment_count"}`` dict.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    telemetry.incr("cluster.rebuild_planned")
    return {
        "uri": uri,
        "committed_version": dataset.version,
        "fragment_count": int(dataset.stats.dataset_stats()["num_fragments"]),
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
    payload: ClusterCommitPayload,
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Commit one dataset's clustered overwrite on an executor.

    Args:
        payload: Minimal commit payload with fragment documents and the pinned schema and version.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A committed fingerprint for index recovery and post-index generation stamping.
    """
    committed: ClusterOverwriteCommit = commit_cluster_data(
        payload.uri,
        payload.fragment_documents,
        payload.schema,
        config,
        telemetry,
        read_version=payload.read_version,
    )
    return {
        "uri": payload.uri,
        "fragments_added": committed.fragments_added,
        "fragment_ids": committed.fragment_ids,
        "num_rows": committed.num_rows,
        "fingerprint_error": committed.fingerprint_error,
    }


def finalise_cluster_dataset(
    payload: ClusterFinalisePayload,
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Commit the rebuilt vector index and clean up, without touching any serving tag.

    The vector-index commit is best-effort: a rebuild failure is non-fatal because the data is
    complete, just unindexed, so the result carries an ``{"error", "phase": "cluster_index"}`` marker
    and the data stays committed. The generation is stamped only after this index commit succeeds.
    On full success old versions are pruned.

    Serving promotion is deliberately not done here. Flipping ``HEAD`` right after the vector-index
    commit would expose a generation whose scalar and FTS indexes have not yet been rebuilt (those
    are left to the next indexing run, ADR 0041), so a text query could see a clustered-but-unindexed
    generation as the served one. Exact promotion belongs to the durable reconciler after every
    required index is rebuilt, validated, and prewarmed.

    Args:
        payload: Minimal finalisation payload with index identity, segments, and result counters.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A success result dict, or one carrying an ``{"error", "phase": "cluster_index"}`` marker
        while keeping the rewritten data.
    """
    index_config: IndexJobConfig = cluster_index_config(config, payload.num_partitions, payload.metric)
    try:
        commit_segments(
            payload.uri,
            payload.segment_documents,
            payload.column,
            payload.index_name,
            True,
            index_config,
            telemetry,
        )
    except Exception as exc:
        telemetry.incr("cluster.index_rebuild_failed")
        logger.warning("cluster: vector index rebuild failed for %s, data intact: %s", payload.uri, exc)
        return {
            "uri": payload.uri,
            "clustered": True,
            "fragments_added": payload.fragments_added,
            "retention_rows_deleted": payload.retention_rows_deleted,
            "error": str(exc),
            "phase": "cluster_index",
            "bytes_removed": 0,
        }
    if payload.fragment_ids is None:
        return {
            "uri": payload.uri,
            "clustered": True,
            "fragments_added": payload.fragments_added,
            "retention_rows_deleted": payload.retention_rows_deleted,
            "error": payload.fingerprint_error or "committed overwrite fingerprint is unavailable",
            "phase": "cluster_stamp",
            "bytes_removed": 0,
        }
    try:
        stamp_cluster_generation(payload.uri, config, telemetry, payload.fragment_ids, payload.num_rows)
    except Exception as exc:
        telemetry.incr("cluster.generation_stamp_failed")
        logger.warning("cluster: generation stamp failed after index recovery for %s: %s", payload.uri, exc)
        return {
            "uri": payload.uri,
            "clustered": True,
            "fragments_added": payload.fragments_added,
            "retention_rows_deleted": payload.retention_rows_deleted,
            "error": str(exc),
            "phase": "cluster_stamp",
            "bytes_removed": 0,
        }
    bytes_removed: int = maintenance_job.cleanup_dataset(payload.uri, config, telemetry)
    telemetry.incr("cluster.clustered")
    return {
        "uri": payload.uri,
        "clustered": True,
        "fragments_added": payload.fragments_added,
        "retention_rows_deleted": payload.retention_rows_deleted,
        "bytes_removed": bytes_removed,
    }
