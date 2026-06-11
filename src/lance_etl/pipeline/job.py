"""Unified pipeline job: interval-tag pruning, compaction, indexing, and interval-tag stamping.

Owns :class:`PipelineConfig` (composed from :class:`~lance_etl.maintenance.job.MaintenanceConfig`
and :class:`~lance_etl.indexing.config.IndexJobConfig`) and :class:`PipelineJob`, which
serializes the four fleet-level phases in dependency order so compaction always precedes indexing.

The phase sequence is:

1. **Prune**: delete old interval tags (skipped when ``tag_keep_last`` is ``None``).
2. **Maintenance**: per-row TTL expiration, two-tier compaction, and version cleanup.
3. **Index**: two-tier index builds with derived-state skip.
4. **Stamp**: write an interval tag and optionally advance the HEAD tag (skipped when
   ``tag_stamp`` is ``None``).

Indexer failures propagate and abort the run before stamping, so a tag is only written when
all three upstream phases completed successfully.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.indexing.config import IndexJobConfig
from lance_etl.indexing.runner import LanceIndexer
from lance_etl.maintenance.job import MaintenanceConfig, MaintenanceJob
from lance_etl.maintenance.tools import prune_interval_tags_fleet, update_serving_tags
from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for :class:`PipelineJob`.

    Composes a :class:`~lance_etl.maintenance.job.MaintenanceConfig` and an
    :class:`~lance_etl.indexing.config.IndexJobConfig` and applies cross-cutting
    settings (``scheduler_pool``, ``telemetry``, ``storage_options``) into both at
    post-init time so callers do not have to set them twice.

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
        scheduler_pool: Spark FAIR scheduler pool injected into both sub-configurations
            at post-init.
        tag_partitions: Maximum Spark partitions for the prune and stamp fan-outs.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    maintenance: MaintenanceConfig = field(default_factory=lambda: MaintenanceConfig(telemetry=TelemetryConfig()))
    indexing: IndexJobConfig = field(default_factory=lambda: IndexJobConfig(telemetry=TelemetryConfig()))
    tag_keep_last: int | None = 48
    tag_stamp: str | None = None
    serve_tag: bool = False
    scheduler_pool: str = "lance-pipeline"
    tag_partitions: int = 512

    def __post_init__(self) -> None:
        """Push cross-cutting settings into the composed sub-configurations.

        Copies ``telemetry``, ``storage_options``, and ``scheduler_pool`` from this config
        into both ``maintenance`` and ``indexing`` so every phase shares the same identity
        and object-store credentials without requiring callers to set them on each
        sub-config individually.
        """
        self.maintenance.telemetry = self.telemetry
        self.maintenance.storage_options = self.storage_options
        self.maintenance.scheduler_pool = self.scheduler_pool
        self.indexing.telemetry = self.telemetry
        self.indexing.storage_options = self.storage_options
        self.indexing.scheduler_pool = self.scheduler_pool


def stamp_eligible(index_stats: dict[str, Any]) -> bool:
    """Return whether a dataset's index statistics qualify it for interval-tag stamping.

    A dataset is eligible when its per-dataset index-stats dictionary contains no error
    marker.  A dataset that was skipped because all its indices were already current
    (``"skipped"`` key present) is still eligible because the indices are valid and current.
    Only a dataset that failed with an exception (typically propagated as a fleet-level
    abort before this function is called) would be ineligible, but the guard is kept for
    future partial-failure modes.

    Args:
        index_stats: One entry from the list returned by :meth:`LanceIndexer.run`.

    Returns:
        ``True`` when the dataset is eligible for stamping.
    """
    return "error" not in index_stats


class PipelineJob:
    """Runs the full pipeline: prune → maintenance → index → stamp."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize the pipeline job.

        Args:
            config: Pipeline configuration with composed sub-configurations.
        """
        self.config: PipelineConfig = config

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
              ``maintenance_skipped``, ``index_skipped``, ``stamped``.
        """
        config: PipelineConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)

        with driver_telemetry.span("lance.pipeline.run") as run_span:
            run_span.set_tag("dataset_count", len(uris))
            run_span.set_tag("tag_stamp", config.tag_stamp or "")
            run_span.set_tag("tag_keep_last", config.tag_keep_last if config.tag_keep_last is not None else -1)

            prune_results: list[dict[str, Any]] = []
            if config.tag_keep_last is not None:
                logger.info("pipeline: pruning interval tags (keep_last=%d)", config.tag_keep_last)
                with driver_telemetry.timed("run.prune_ms"):
                    prune_results = prune_interval_tags_fleet(
                        spark,
                        uris,
                        config.telemetry,
                        config.storage_options,
                        config.tag_keep_last,
                        partitions=config.tag_partitions,
                    )
            pruned_tags_total: int = sum(int(r.get("tags_pruned", 0)) for r in prune_results)
            if config.tag_keep_last is not None:
                run_span.set_tag("pruned_tags", pruned_tags_total)
                driver_telemetry.gauge("run.pruned_tags", pruned_tags_total)
                logger.info(
                    "pipeline: pruned %d interval tags across %d datasets", pruned_tags_total, len(prune_results)
                )

            logger.info("pipeline: starting maintenance phase")
            with driver_telemetry.timed("run.maintenance_ms"):
                maintenance_results: list[dict[str, Any]] = MaintenanceJob(config.maintenance).run(spark, uris)

            logger.info("pipeline: starting indexing phase")
            with driver_telemetry.timed("run.index_ms"):
                index_results: list[dict[str, Any]] = LanceIndexer(config.indexing).run(spark, uris)

            stamp_results: list[dict[str, Any]] = []
            if config.tag_stamp is not None:
                index_by_uri: dict[str, dict[str, Any]] = {r["uri"]: r for r in index_results}
                eligible_uris: list[str] = [u for u in uris if stamp_eligible(index_by_uri.get(u, {}))]
                logger.info(
                    "pipeline: stamping tag %r on %d/%d eligible datasets",
                    config.tag_stamp,
                    len(eligible_uris),
                    len(uris),
                )
                with driver_telemetry.timed("run.stamp_ms"):
                    stamp_results = update_serving_tags(
                        spark,
                        eligible_uris,
                        config.telemetry,
                        config.storage_options,
                        tag=config.tag_stamp,
                        partitions=config.tag_partitions,
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
                            partitions=config.tag_partitions,
                        )
                    stamp_results = stamp_results + head_results
                    driver_telemetry.gauge("run.head_tags_flipped", len(head_results))

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

            counts: dict[str, int] = {
                "total": len(uris),
                "pruned_tags": pruned_tags_total,
                "maintenance_skipped": maintenance_skipped,
                "index_skipped": index_skipped,
                "stamped": len([r for r in stamp_results if r.get("tag") == config.tag_stamp]),
            }
            run_span.set_tag("maintenance_skipped", maintenance_skipped)
            run_span.set_tag("index_skipped", index_skipped)
            driver_telemetry.gauge("run.total_datasets", len(uris))
            logger.info(
                "pipeline run complete: %d datasets, %d pruned tags, %d maintenance-skipped, %d index-skipped, "
                "%d stamped",
                len(uris),
                pruned_tags_total,
                maintenance_skipped,
                index_skipped,
                counts["stamped"],
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
