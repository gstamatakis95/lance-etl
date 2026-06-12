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

FTS_OPTIONAL_PARAMS: tuple[str, ...] = (
    "base_tokenizer",
    "language",
    "lower_case",
    "stem",
    "remove_stop_words",
    "ascii_folding",
)
"""FTS keyword arguments forwarded to ``create_scalar_index`` only when set on the config."""


@dataclass
class IndexJobConfig:
    """Configuration for :class:`lance_etl.indexing.runner.LanceIndexer`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        vector_columns: Vector columns to index with IVF_RQ; each gets its own handler and config entry.
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
        large_dataset_row_threshold: Row count at or above which a dataset uses the distributed
            segment fan-out even when its fragment count is below
            ``small_dataset_fragment_threshold``. The small tier builds the whole index, including
            IVF training and assignment, in one executor task. A dataset that is large by rows but
            holds few large fragments would overload that single task, so it is routed to the
            distributed tier instead. ``None`` disables the row dimension. The row count is read from
            fragment metadata (no data scan).
        small_tier_slices: Spark partition count for the batched small-dataset and classification jobs.
        driver_concurrency: Concurrent large-dataset submissions from the driver thread pool.
        scheduler_pool: Spark FAIR scheduler pool name for large-dataset jobs.
        min_ivf_partitions: Floor on the derived IVF partition count.
        max_ivf_partitions: Cap on the derived IVF partition count; operators needing more set ``num_partitions``.
        target_rows_per_ivf_partition: Target rows per partition for the size-aware policy.
        ivf_rq_num_bits: RaBitQ bits per sub-dimension; 1 gives maximum compression with a refine pass.
        train_sample_rate: Rows sampled per IVF partition when training centroids.
        train_max_iters: Maximum k-means iterations when training the IVF.
        retrain_growth_factor: Retrain when row count exceeds this multiple of ``rows_at_train`` in the config.
        train_sample_memory_budget_bytes: Executor RAM cap for the IVF training sample; caps partition count.
            The sample lands in executor heap, so this budget should track executor sizing (the default 8g
            executor covers the 8 GiB default).
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
    large_dataset_row_threshold: int | None = 5_000_000
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


def derive_num_partitions(rows: int, configured: int | None, config: IndexJobConfig) -> int:
    """Return the IVF partition count for a dataset size.

    Follows the size-aware policy
    ``clamp(rows // config.target_rows_per_ivf_partition, config.min_ivf_partitions,
    config.max_ivf_partitions)`` unless an explicit partition count was configured. The caller may
    apply :func:`memory_bounded_num_partitions` before training to stay within the driver memory
    budget.

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


def memory_bounded_num_partitions(planned: int, dimension: int, config: IndexJobConfig) -> int:
    """Cap the planned IVF partition count so the training sample fits within the executor memory budget.

    ``train_ivf`` loads ``planned * config.train_sample_rate`` float32 vectors of length
    ``dimension`` into executor heap (training is offloaded to a single Spark task). This function
    floors the planned count to what ``config.train_sample_memory_budget_bytes`` can accommodate on
    a single executor. The result is always at least 1.

    Args:
        planned: The partition count derived by policy or degraded for row count.
        dimension: The vector dimension of the column being indexed.
        config: Indexing configuration supplying the memory budget and sample rate.

    Returns:
        A partition count whose training sample fits within the configured budget, at least 1.
    """
    budget: int = config.train_sample_memory_budget_bytes // (config.train_sample_rate * dimension * 4)
    return max(1, min(planned, budget))
