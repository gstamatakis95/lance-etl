"""Unified pipeline job: interval-tag pruning, compaction, indexing, and interval-tag stamping.

Owns :class:`PipelineConfig` (composed from :class:`~lance_etl.maintenance.job.MaintenanceConfig`
and :class:`~lance_etl.indexing.config.IndexJobConfig`) and :class:`PipelineJob`, which
serializes the four fleet-level phases in dependency order so compaction always precedes indexing.

The phase sequence is:

1. **Prune**: delete old interval tags (skipped when ``tag_keep_last`` is ``None``).
2. **Maintenance**: per-row TTL expiration, unified task-based compaction, and version cleanup.
3. **Index**: unified task-based index builds with derived-state skip.
4. **Stamp**: write an interval tag and optionally advance the HEAD tag (skipped when
   ``tag_stamp`` is ``None``).

Each fleet phase isolates per-dataset failures rather than aborting the whole run: a dataset that
fails compaction or indexing is recorded with an error marker and excluded from stamping (a failed
dataset is never HEAD-promoted), while every other dataset completes and is stamped. Failed
datasets carry no cursor and are simply re-processed by the next scheduled run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.fanout import dataset_result_failed
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.indexing.runner import LanceIndexer
from lance_etl.maintenance.job import MaintenanceConfig, MaintenanceJob
from lance_etl.maintenance.tools import prune_interval_tags_fleet, update_serving_tags
from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

TAG_PARTITIONS: int = 512
"""Maximum Spark partitions for the prune and stamp fan-outs."""


@dataclass
class PipelineConfig:
    """Configuration for :class:`PipelineJob`.

    Composes a :class:`~lance_etl.maintenance.job.MaintenanceConfig` and an
    :class:`~lance_etl.indexing.config.IndexJobConfig` and applies cross-cutting
    settings (``telemetry``, ``storage_options``) into both at post-init time so callers
    do not have to set them twice. ``__post_init__`` is the single source of truth for those
    two cross-cutting fields on the sub-configs: callers should not set them again on
    ``maintenance``/``indexing`` before constructing a ``PipelineConfig``, since this
    overwrites them unconditionally.

    Attributes:
        telemetry: Telemetry configuration shared across both sub-jobs.
        storage_options: Object-store options forwarded to pylance for all phases.
        maintenance: Maintenance sub-configuration (TTL, compaction, cleanup).
        indexing: Indexing sub-configuration (vector, scalar, FTS columns).
        tag_keep_last: How many newest interval tags to retain when pruning.  ``None``
            disables pruning entirely.
        tag_stamp: The interval tag name to write after a successful run.  ``None``
            disables the stamp phase.
        serve_tag: When ``True`` and ``tag_stamp`` is set, advance the ``HEAD`` tag to the
            dataset's latest version after stamping the interval tag.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    maintenance: MaintenanceConfig = field(default_factory=lambda: MaintenanceConfig(telemetry=TelemetryConfig()))
    indexing: IndexJobConfig = field(default_factory=lambda: IndexJobConfig(telemetry=TelemetryConfig()))
    tag_keep_last: int | None = 48
    tag_stamp: str | None = None
    serve_tag: bool = False

    def __post_init__(self) -> None:
        """Push cross-cutting settings into the composed sub-configurations.

        Copies ``telemetry`` and ``storage_options`` from this config into both
        ``maintenance`` and ``indexing`` so every phase shares the same identity and
        object-store credentials without requiring callers to set them on each sub-config
        individually.
        """
        self.maintenance.telemetry = self.telemetry
        self.maintenance.storage_options = self.storage_options
        self.indexing.telemetry = self.telemetry
        self.indexing.storage_options = self.storage_options


def stamp_eligible(index_stats: dict[str, Any]) -> bool:
    """Return whether a dataset's index statistics qualify it for interval-tag stamping.

    A dataset is eligible only when it carries neither a dataset-level error marker nor any
    per-index error entry, the same rule :func:`~lance_etl.fanout.dataset_result_failed`
    implements.  A plan failure sets a dataset-level ``"error"`` key, while a build or
    commit failure is recorded as a per-index ``{"error", "phase"}``
    entry inside the ``"indexes"`` list with no top-level key set (see :meth:`LanceIndexer.run`),
    so both failure shapes must be excluded here.  Checking only the top-level key would let a
    dataset whose vector index build failed still pass, HEAD-promoting the serving layer onto an
    incomplete index and degrading recall for that org while the same dataset is reported failed
    and will be retried.  A dataset that was skipped because all its indices were already current
    (``"skipped"`` key present, no error entries) is still eligible because the indices are valid
    and current.  An ineligible dataset is neither interval-stamped nor HEAD-promoted, so a
    partially-indexed dataset is never HEAD-promoted and its serving version stays at its last good
    state.

    Args:
        index_stats: One entry from the list returned by :meth:`LanceIndexer.run`.

    Returns:
        ``True`` when the dataset is eligible for stamping.
    """
    return not dataset_result_failed(index_stats)


class PipelineJob:
    """Runs the full pipeline: prune → maintenance → index → stamp."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize the pipeline job.

        Args:
            config: Pipeline configuration with composed sub-configurations.
        """
        self.config: PipelineConfig = config

    def prune_phase(
        self, spark: SparkSession, uris: list[str], run_span: Any, driver_telemetry: Telemetry
    ) -> list[dict[str, Any]]:
        """Run the interval-tag prune phase, or skip it when ``tag_keep_last`` is ``None``.

        Args:
            spark: Active Spark session.
            uris: Dataset URIs to prune.
            run_span: The pipeline run span, tagged with the pruned-tag count.
            driver_telemetry: The driver's telemetry facade.

        Returns:
            The raw per-dataset prune results, empty when pruning is disabled.
        """
        config: PipelineConfig = self.config
        if config.tag_keep_last is None:
            return []
        logger.info("pipeline: pruning interval tags (keep_last=%d)", config.tag_keep_last)
        with driver_telemetry.timed("run.prune_ms"):
            prune_results: list[dict[str, Any]] = prune_interval_tags_fleet(
                spark,
                uris,
                config.telemetry,
                config.storage_options,
                config.tag_keep_last,
                partitions=TAG_PARTITIONS,
            )
        pruned_tags_total: int = sum(int(r.get("tags_pruned", 0)) for r in prune_results)
        run_span.set_tag("pruned_tags", pruned_tags_total)
        driver_telemetry.gauge("run.pruned_tags", pruned_tags_total)
        logger.info("pipeline: pruned %d interval tags across %d datasets", pruned_tags_total, len(prune_results))
        return prune_results

    def stamp_phase(
        self,
        spark: SparkSession,
        uris: list[str],
        index_results: list[dict[str, Any]],
        maintenance_results: list[dict[str, Any]],
        run_span: Any,
        driver_telemetry: Telemetry,
    ) -> list[dict[str, Any]]:
        """Run the interval-tag stamp phase, or skip it when ``tag_stamp`` is ``None``.

        Stamps the configured interval tag on every dataset whose index stats pass
        :func:`stamp_eligible` and whose maintenance result carries no error, then optionally
        advances the ``HEAD`` tag on the same datasets when ``serve_tag`` is set. A dataset whose
        compaction failed is never HEAD-promoted, so a failed dataset's serving version never
        advances past its last good state.

        Args:
            spark: Active Spark session.
            uris: Dataset URIs processed by the run, in input order.
            index_results: The indexing phase's per-dataset stats, gating eligibility.
            maintenance_results: The maintenance phase's per-dataset stats; a dataset with an
                ``"error"`` marker is excluded from stamping.
            run_span: The pipeline run span, tagged with the stamped-dataset count.
            driver_telemetry: The driver's telemetry facade.

        Returns:
            The raw tag-update results (interval stamps plus any HEAD flips), empty when
            stamping is disabled.
        """
        config: PipelineConfig = self.config
        if config.tag_stamp is None:
            return []
        index_by_uri: dict[str, dict[str, Any]] = {r["uri"]: r for r in index_results}
        maint_by_uri: dict[str, dict[str, Any]] = {r["uri"]: r for r in maintenance_results}
        eligible_uris: list[str] = [
            u
            for u in uris
            if stamp_eligible(index_by_uri.get(u, {})) and not dataset_result_failed(maint_by_uri.get(u, {}))
        ]
        logger.info(
            "pipeline: stamping tag %r on %d/%d eligible datasets",
            config.tag_stamp,
            len(eligible_uris),
            len(uris),
        )
        with driver_telemetry.timed("run.stamp_ms"):
            stamp_results: list[dict[str, Any]] = update_serving_tags(
                spark,
                eligible_uris,
                config.telemetry,
                config.storage_options,
                tag=config.tag_stamp,
                partitions=TAG_PARTITIONS,
            )
        run_span.set_tag("stamped", len(stamp_results))
        driver_telemetry.gauge("run.stamped_datasets", len(stamp_results))

        if config.serve_tag:
            logger.info("pipeline: advancing HEAD tag on %d datasets", len(eligible_uris))
            with driver_telemetry.timed("run.head_tag_ms"):
                head_results: list[dict[str, Any]] = update_serving_tags(
                    spark,
                    eligible_uris,
                    config.telemetry,
                    config.storage_options,
                    tag="HEAD",
                    partitions=TAG_PARTITIONS,
                )
            stamp_results = stamp_results + head_results
            driver_telemetry.gauge("run.head_tags_flipped", len(head_results))
        return stamp_results

    def run(self, spark: SparkSession, uris: list[str]) -> dict[str, Any]:
        """Execute all four pipeline phases in order over the supplied dataset URIs.

        Phases run serially so compaction always precedes indexing (which needs compact
        fragments for accurate IVF coverage) and pruning always precedes compaction (so
        the same run's cleanup can reclaim versions no longer pinned by deleted interval
        tags).

        Args:
            spark: Active Spark session.
            uris: Dataset URIs to process through all phases.

        Returns:
            A result dictionary with keys:

            - ``datasets``: per-URI merged view (maintenance stats + index stats, keyed by URI).
            - ``maintenance_results``: raw list from :meth:`MaintenanceJob.run`.
            - ``index_results``: raw list from :meth:`LanceIndexer.run`.
            - ``prune_results``: raw list from :meth:`prune_interval_tags_fleet` (empty list
              when pruning is skipped).
            - ``stamp_results``: raw list from :meth:`update_serving_tags` calls (empty list
              when stamping is skipped).
            - ``tag_stamp``: the interval tag name that was written, or ``None``.
            - ``counts``: summary counts with keys ``total``, ``pruned_tags``,
              ``maintenance_skipped``, ``index_skipped``, ``stamped``, and ``failed`` (datasets
              that failed compaction or indexing in isolation this run).
        """
        config: PipelineConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)

        with driver_telemetry.span("lance.pipeline.run") as run_span:
            run_span.set_tag("dataset_count", len(uris))
            run_span.set_tag("tag_stamp", config.tag_stamp or "")
            run_span.set_tag("tag_keep_last", config.tag_keep_last if config.tag_keep_last is not None else -1)

            prune_results: list[dict[str, Any]] = self.prune_phase(spark, uris, run_span, driver_telemetry)
            pruned_tags_total: int = sum(int(r.get("tags_pruned", 0)) for r in prune_results)

            logger.info("pipeline: starting maintenance phase")
            with driver_telemetry.timed("run.maintenance_ms"):
                maintenance_results: list[dict[str, Any]] = MaintenanceJob(config.maintenance).run(spark, uris)

            logger.info("pipeline: starting indexing phase")
            with driver_telemetry.timed("run.index_ms"):
                index_results: list[dict[str, Any]] = LanceIndexer(config.indexing).run(spark, uris)

            stamp_results: list[dict[str, Any]] = self.stamp_phase(
                spark, uris, index_results, maintenance_results, run_span, driver_telemetry
            )

            maint_by_uri: dict[str, dict[str, Any]] = {r["uri"]: r for r in maintenance_results}
            idx_by_uri: dict[str, dict[str, Any]] = {r["uri"]: r for r in index_results}
            datasets: list[dict[str, Any]] = []
            for uri in uris:
                merged: dict[str, Any] = {"uri": uri}
                merged.update(maint_by_uri.get(uri, {}))
                merged.update(idx_by_uri.get(uri, {}))
                datasets.append(merged)

            maintenance_skipped: int = sum(1 for r in maintenance_results if r.get("skipped"))
            index_skipped: int = sum(1 for r in index_results if r.get("skipped"))
            failed: int = sum(
                1
                for u in uris
                if dataset_result_failed(maint_by_uri.get(u, {})) or dataset_result_failed(idx_by_uri.get(u, {}))
            )

            counts: dict[str, int] = {
                "total": len(uris),
                "pruned_tags": pruned_tags_total,
                "maintenance_skipped": maintenance_skipped,
                "index_skipped": index_skipped,
                "stamped": len([r for r in stamp_results if r.get("tag") == config.tag_stamp]),
                "failed": failed,
            }
            run_span.set_tag("maintenance_skipped", maintenance_skipped)
            run_span.set_tag("index_skipped", index_skipped)
            run_span.set_tag("failed_datasets", failed)
            driver_telemetry.gauge("run.total_datasets", len(uris))
            driver_telemetry.gauge("run.datasets_failed", failed)
            logger.info(
                "pipeline run complete: %d datasets, %d pruned tags, %d maintenance-skipped, %d index-skipped, "
                "%d stamped, %d failed",
                len(uris),
                pruned_tags_total,
                maintenance_skipped,
                index_skipped,
                counts["stamped"],
                failed,
            )

            return {
                "datasets": datasets,
                "maintenance_results": maintenance_results,
                "index_results": index_results,
                "prune_results": prune_results,
                "stamp_results": stamp_results,
                "tag_stamp": config.tag_stamp,
                "counts": counts,
            }
