"""Configuration and naming helpers for the Lance indexing job.

Owns :class:`IndexJobConfig`, the four index-name derivation functions, and the
numeric-policy helpers for IVF partition counts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lance_etl.telemetry import DEFAULT_COMMIT_RETRIES, TelemetryConfig

METRIC_TO_DISTANCE: dict[str, str] = {"l2": "l2", "cosine": "cosine", "dot": "dot"}
"""Maps lowercase metric names to Lance distance-type strings."""

MIN_IVF_PARTITIONS: int = 16
"""Floor on the derived IVF partition count, never varied."""

MAX_IVF_PARTITIONS: int = 32768
"""Cap on the derived IVF partition count, never varied; operators needing more set ``num_partitions``."""

TARGET_ROWS_PER_IVF_PARTITION: int = 8192
"""Target rows per partition for the size-aware IVF partition policy, never varied."""

IVF_RQ_NUM_BITS: int = 1
"""RaBitQ bits per sub-dimension, never varied; 1 gives maximum compression with a refine pass."""

STREAMING_SAMPLE_RATE: int = 32
"""Streaming k-means chunk rate for bootstrap builds, never varied.

The trainer loads at most ``num_partitions * STREAMING_SAMPLE_RATE`` vectors per step instead of
one giant sample, so training memory is bounded regardless of partition count. For more than 256
partitions lance compresses chunks into a weighted coreset and trains final centroids with
weighted hierarchical k-means.
"""

STREAMING_REFINE_PASSES: int = 1
"""Extra streaming Lloyd refinement passes after coreset training, never varied.

Each pass loads at most ``num_partitions * STREAMING_SAMPLE_RATE`` raw vectors.
"""

RETRAIN_GROWTH_FACTOR: float = 4.0
"""Retrain when row count exceeds this multiple of ``rows_at_train``, never varied."""


def growth_exceeds_retrain_factor(rows: int, rows_at_train: int) -> bool:
    """Report whether dataset growth since centroid training exceeds the retrain factor.

    Shared comparison behind both retrain triggers. Callers keep their own
    absent-config semantics. Only the threshold arithmetic lives here.

    Args:
        rows: The dataset's current row count.
        rows_at_train: The row count recorded when the centroids were last trained.

    Returns:
        ``True`` when growth since training exceeds :data:`RETRAIN_GROWTH_FACTOR`.
    """
    return rows > RETRAIN_GROWTH_FACTOR * rows_at_train


MAX_STALE_REPLANS: int = 3
"""Fleet-level plan-build-commit rounds for stale indexes before giving up, never varied."""


@dataclass
class IndexJobConfig:
    """Configuration for :class:`lance_etl.indexing.runner.LanceIndexer`.

    The IVF partition policy bounds (:data:`MIN_IVF_PARTITIONS`, :data:`MAX_IVF_PARTITIONS`,
    :data:`TARGET_ROWS_PER_IVF_PARTITION`), the RaBitQ bit width (:data:`IVF_RQ_NUM_BITS`), the
    streaming k-means knobs (:data:`STREAMING_SAMPLE_RATE`, :data:`STREAMING_REFINE_PASSES`), the
    retrain trigger (:data:`RETRAIN_GROWTH_FACTOR`), and the
    stale-replan bound (:data:`MAX_STALE_REPLANS`) are fixed module-level constants, not fields,
    because they are never varied. The IVF training distance is always derived from ``metric`` via
    :meth:`resolved_distance_type`, and the four fine-grained FTS tokenizer toggles
    (lowercase/stem/stop-words/ASCII-folding) are always omitted from ``fts_params()``.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        vector_columns: Vector columns to index with IVF_RQ; each gets its own handler and config entry.
        num_partitions: IVF partitions; derived from the size-aware policy when unset.
        vector_min_rows: Skip the vector index below this row count; flat KNN is sufficient.
        metric: Distance metric such as ``L2``, ``cosine``, or ``dot``.
        scalar_columns: Columns to index with btree.
        bitmap_columns: Columns to index with bitmap.
        zonemap_columns: Columns to index with zonemap, an inexact index effective only when the
            column is approximately sorted.
        text_columns: Columns to index with a full-text inverted index.
        fts_with_position: Store token positions for phrase queries.
        fts_base_tokenizer: FTS base tokenizer name.
        fts_language: FTS stemming and stop-word language.
        fragments_per_index_task: Target fragments covered by one segment-build task. The shard
            count per index is ``ceil(target_fragments / fragments_per_index_task)``, so a small
            dataset builds in exactly one task and a big one fans out across many, through the
            same segment API.
        rebuild: Reindex every fragment; forces every handler to rebuild instead of maintaining.
        max_index_deltas: Merge accumulated index deltas into one when the count exceeds this cap.
        fts_max_unindexed_fragments: Maintain an inverted index incrementally only within this unindexed backlog.
        commit_retries: Retry budget for commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    vector_columns: list[str] = field(default_factory=list)
    num_partitions: int | None = None
    vector_min_rows: int = 10_000
    metric: str = "L2"
    scalar_columns: list[str] = field(default_factory=list)
    bitmap_columns: list[str] = field(default_factory=list)
    zonemap_columns: list[str] = field(default_factory=list)
    text_columns: list[str] = field(default_factory=list)
    fts_with_position: bool = False
    fts_base_tokenizer: str | None = None
    fts_language: str | None = None
    fragments_per_index_task: int = 8
    rebuild: bool = False
    max_index_deltas: int = 4
    fts_max_unindexed_fragments: int = 32
    commit_retries: int = DEFAULT_COMMIT_RETRIES
    commit_backoff_seconds: float = 0.5

    def resolved_distance_type(self) -> str:
        """Return the IVF training distance derived from the metric.

        Returns:
            A Lance distance type string.
        """
        return METRIC_TO_DISTANCE.get(self.metric.lower(), "l2")

    def fts_params(self) -> dict[str, Any]:
        """Build the inverted-index parameters, omitting unset options.

        Returns:
            Keyword arguments for an ``INVERTED`` index build.
        """
        params: dict[str, Any] = {"with_position": self.fts_with_position}
        if self.fts_base_tokenizer is not None:
            params["base_tokenizer"] = self.fts_base_tokenizer
        if self.fts_language is not None:
            params["language"] = self.fts_language
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


def zonemap_index_name(column: str) -> str:
    """Return the zonemap index name for a column.

    Args:
        column: The column name.

    Returns:
        The derived index name.
    """
    return f"{column}_zonemap_idx"


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
    :func:`fts_index_name`. Each vector column listed in :attr:`IndexJobConfig.vector_columns`
    receives an index with the name returned by this function.

    Args:
        column: The vector column name.

    Returns:
        The derived index name.
    """
    return f"{column}_idx"


def vector_config_key(column: str) -> str:
    """Return the dataset config key for the vector artifacts of a column.

    The key is stored in the dataset's own config KV via ``update_config`` and persists across
    compaction and version cleanup. One key exists per vector column on the dataset.

    Args:
        column: The vector column name.

    Returns:
        The config key string.
    """
    return f"lance-etl.vector.{column}"


def config_reusable(cfg: dict[str, Any], dimension: int, metric: str, num_bits: int) -> bool:
    """Return whether a stored vector config can be reused for the current index parameters.

    A config is reusable when it contains a ``rabitq_model`` entry and its stored ``dimension``,
    ``metric``, and ``num_bits`` all match the current values. The partition count is not checked:
    centroids are read back from the committed index via
    :meth:`lance.LanceDataset.get_ivf_model`, and ``num_partitions`` is derived from
    ``len(centroids)`` at reuse time.

    Callers that receive ``False`` must fall through to the training path rather than raising, so a
    changed dimension or metric triggers a retrain once and then proceeds normally instead of
    bricking the dataset.

    Args:
        cfg: The stored config dict.
        dimension: The vector dimension of the column being indexed.
        metric: The distance metric name from the current configuration.
        num_bits: The RaBitQ bits per sub-dimension from the current configuration.

    Returns:
        ``True`` when the config is safe to reuse, ``False`` when a retrain is required.
    """
    if "rabitq_model" not in cfg:
        return False
    expected: dict[str, Any] = {
        "dimension": dimension,
        "metric": metric,
        "num_bits": num_bits,
    }
    return all(cfg.get(name) == value for name, value in expected.items())


def derive_num_partitions(rows: int, configured: int | None) -> int:
    """Return the IVF partition count for a dataset size.

    Follows the size-aware policy
    ``clamp(rows // TARGET_ROWS_PER_IVF_PARTITION, MIN_IVF_PARTITIONS, MAX_IVF_PARTITIONS)``
    unless an explicit partition count was configured. No memory bound applies: streaming k-means
    trains in fixed-size chunks regardless of partition count.

    Args:
        rows: The dataset row count.
        configured: An explicit partition count, taking precedence when set.

    Returns:
        The planned IVF partition count.
    """
    if configured is not None:
        return configured
    return min(
        MAX_IVF_PARTITIONS,
        max(MIN_IVF_PARTITIONS, rows // TARGET_ROWS_PER_IVF_PARTITION),
    )


def degrade_num_partitions(planned: int, rows: int, sample_rate: int) -> int:
    """Lower the partition count when training rows are insufficient.

    ``train_ivf`` samples ``num_partitions * sample_rate`` rows. When the dataset cannot supply
    that many, the partition count is degraded to what the available rows can train.

    Args:
        planned: The planned IVF partition count.
        rows: The dataset row count.
        sample_rate: Rows sampled per partition during IVF training.

    Returns:
        A partition count trainable from the available rows, at least 1.
    """
    supportable: int = rows // sample_rate
    return max(1, min(planned, supportable))
