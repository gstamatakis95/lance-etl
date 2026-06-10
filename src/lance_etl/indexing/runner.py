"""LanceIndexer orchestrator and small-dataset in-process path.

Provides :class:`LanceIndexer` (two-tier orchestration), :func:`index_dataset_locally` (the
small-tier in-process build), and the new derived-state :func:`index_skip_reason` check that lets
a dataset skip indexing when all configured indices are current.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import lance
from pyspark.sql import SparkSession

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
)
from lance_etl.indexing.optimize import maintain_index_locally
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)


def index_skip_reason(dataset: lance.LanceDataset, config: IndexJobConfig) -> str | None:
    """Return a reason string when all configured indices are current, or None to proceed.

    Evaluates derived dataset state from the already-open handle so no extra object-store I/O is
    needed. The check is bypassed when ``config.rebuild`` is True.

    For each configured index name the check proceeds as follows. When the index is absent the
    dataset needs indexing, unless it is a vector index and the row count is below
    ``config.vector_min_rows`` (intended skip — flat KNN suffices). When the index is present,
    ``dataset.stats.index_stats(name)`` is consulted: if ``num_unindexed_fragments`` is positive
    or ``num_indices`` exceeds ``config.max_index_deltas``, the dataset needs work. When every
    configured index passes all checks without returning None, a short reason string is returned
    and the caller skips the dataset.

    Args:
        dataset: The already-open dataset handle.
        config: Indexing configuration.

    Returns:
        A human-readable skip reason when all indices are current and no work is needed, or
        ``None`` when at least one index requires attention.
    """
    if config.rebuild:
        return None

    existing: set[str] = {description.name for description in dataset.describe_indices()}
    rows: int | None = None

    all_names: list[tuple[str, bool]] = []
    for column in config.vector_columns:
        all_names.append((vector_index_name(column), True))
    for column in config.scalar_columns:
        all_names.append((scalar_index_name(column), False))
    for column in config.bitmap_columns:
        all_names.append((bitmap_index_name(column), False))
    for column in config.text_columns:
        all_names.append((fts_index_name(column), False))

    if not all_names:
        return "no indices configured"

    for name, is_vector in all_names:
        if name not in existing:
            if is_vector:
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

    return "all indices current"


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

    This is the small-dataset tier: no segment fan-out. An index that already exists is maintained
    incrementally with ``optimize_indices``, which appends only unindexed fragments and no-ops
    cheaply when the index is fully covered, so sweeping the unchanged power-law tail costs near
    zero. Missing indices, or every index on a ``rebuild`` run (the path for parameter changes),
    are built with plain ``create_index`` / ``create_scalar_index`` calls that build and commit
    end-to-end. After each maintenance pass the index's deltas are merged once they exceed
    ``max_index_deltas``. Single-process ``create_index`` needs no shared RaBitQ model — a lone
    non-merged segment may use its own random rotation. Each vector column listed in
    :attr:`IndexJobConfig.vector_columns` follows the same size-aware policy as the distributed
    path and is skipped below the configured row floor. Incremental vector maintenance assigns new
    rows to existing IVF partitions without retraining, so a grown dataset retrains via the large
    tier's growth trigger once it crosses the fragment threshold, or earlier via a ``rebuild``
    run. No artifact files or sidecar directories are created by this path.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.

    Returns:
        A statistics dictionary matching the large-tier shape. Maintained indices carry
        ``maintained: True``.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)

    skip: str | None = index_skip_reason(dataset, config)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        return {"uri": uri, "indexes": [], "tier": "small", "skipped": skip}

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

        One :class:`VectorIndexHandler` is created for each column in
        :attr:`IndexJobConfig.vector_columns`. All vector handlers share the same metric,
        partition policy, and row floor but each gets its own column and derived index name from
        :func:`~lance_etl.indexing.config.vector_index_name`.

        Returns:
            One handler per configured index column, in the order vector to scalar to bitmap to
            text.
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

        Fragment counts are gathered with one distributed job so the driver never opens datasets
        itself.

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

        Each executor task indexes one whole dataset end-to-end with plain non-distributed index
        builds. The driver only collects statistics.

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

        Each dataset keeps its distributed per-segment build, but multiple datasets are driven
        concurrently from a driver thread pool. Every submission is tagged with the configured
        Spark FAIR scheduler pool so concurrent jobs share the cluster fairly.
        ``spark.scheduler.mode=FAIR`` must be set on the session for the pools to take effect.

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
            dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            skip: str | None = index_skip_reason(dataset, config)
            if skip is not None:
                telemetry.incr("dataset.skipped_no_work")
                return {"uri": uri, "indexes": [], "tier": "large", "skipped": skip}
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

        Datasets are classified by fragment count: small datasets are batched into one Spark job
        where each executor task indexes a whole dataset, and large datasets keep the distributed
        segment fan-out, driven concurrently from the driver. Any failure propagates and fails the
        run.

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
            skipped: int = sum(1 for stats in results if stats.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)
            logger.info(
                "indexing run: %d datasets (%d small, %d large, %d skipped)",
                len(results),
                len(small),
                len(large),
                skipped,
            )
            return results
