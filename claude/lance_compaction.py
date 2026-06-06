"""Distributed compaction for per-tenant Lance datasets via the plan API.

Each dataset is compacted with the distributed plan API: the driver builds a
``Compaction.plan``, the rewrite tasks are fanned out across Spark executors,
and the resulting rewrites are committed in one transaction on the driver. Tasks
and rewrite results cross the executor boundary as JSON via their ``json`` and
``from_json`` methods, so no opaque objects are pickled.

Index remap is deferred (``defer_index_remap``): compaction does not rebuild the
vector or scalar indices inline. Instead a Fragment Reuse Index records how the
rewritten fragments map to the old ones, so existing IVF_RQ and btree indices
keep serving queries against compacted data, and the actual remap is applied
later by the indexing job. This keeps compaction fast and decoupled from
indexing.

The commit runs in a conflict-retry loop with exponential backoff so it coexists
with concurrent ingestion and indexing; any non-conflict failure propagates and
an exhausted retry budget raises, so the whole job fails fast. Big datasets are
handled by the plan splitting the work into many independent tasks that run in
parallel across the cluster.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional

import lance
from lance.optimize import Compaction, CompactionTask, RewriteResult
from pyspark.sql import SparkSession

from telemetry import Telemetry, TelemetryConfig, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class CompactionConfig:
    """Configuration for :class:`LanceCompactor`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        target_rows_per_fragment: Desired rows per compacted fragment.
        max_rows_per_group: Maximum rows per group within a fragment.
        max_bytes_per_file: Maximum bytes per compacted file.
        materialize_deletions: Whether to physically remove deleted rows.
        materialize_deletions_threshold: Deletion fraction above which a
            fragment is rewritten to drop deleted rows.
        defer_index_remap: Defer index remap and rely on the Fragment Reuse
            Index so existing indices survive compaction without a rebuild.
        num_threads: Worker threads inside a single rewrite task.
        batch_size: Rows per batch when rewriting.
        compaction_mode: ``"reencode"`` or ``"try_binary_copy"``.
        max_tasks: Maximum number of Spark tasks for one dataset's rewrites.
        run_cleanup: Whether to prune old versions after committing.
        cleanup_older_than_seconds: Age threshold for version cleanup.
        retain_versions: Number of recent versions to retain.
        commit_retries: Retry budget for commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
    """

    telemetry: TelemetryConfig
    storage_options: Optional[Dict[str, Any]] = None
    target_rows_per_fragment: Optional[int] = None
    max_rows_per_group: Optional[int] = None
    max_bytes_per_file: Optional[int] = None
    materialize_deletions: Optional[bool] = True
    materialize_deletions_threshold: Optional[float] = None
    defer_index_remap: bool = True
    num_threads: Optional[int] = None
    batch_size: Optional[int] = None
    compaction_mode: Optional[str] = None
    max_tasks: int = 256
    run_cleanup: bool = True
    cleanup_older_than_seconds: Optional[int] = None
    retain_versions: Optional[int] = None
    commit_retries: int = 20
    commit_backoff_seconds: float = 0.5

    def plan_options(self) -> Dict[str, Any]:
        """Build the compaction options dict, omitting unset values.

        Returns:
            Options accepted by ``Compaction.plan``.
        """
        candidates: Dict[str, Any] = {
            "target_rows_per_fragment": self.target_rows_per_fragment,
            "max_rows_per_group": self.max_rows_per_group,
            "max_bytes_per_file": self.max_bytes_per_file,
            "materialize_deletions": self.materialize_deletions,
            "materialize_deletions_threshold": self.materialize_deletions_threshold,
            "defer_index_remap": self.defer_index_remap,
            "num_threads": self.num_threads,
            "batch_size": self.batch_size,
            "compaction_mode": self.compaction_mode,
        }
        return {name: value for name, value in candidates.items() if value is not None}


class LanceCompactor:
    """Compacts Lance datasets with the distributed plan API."""

    def __init__(self, config: CompactionConfig) -> None:
        """Initialize the compactor.

        Args:
            config: Compaction configuration.
        """
        self.config: CompactionConfig = config

    def commit_rewrites(
        self, uri: str, rewrite_jsons: List[str], telemetry: Telemetry
    ) -> Dict[str, Any]:
        """Commit serialized rewrites, retrying conflicts to coexist with writers.

        Args:
            uri: Dataset URI.
            rewrite_jsons: Serialized rewrite results from the executors.
            telemetry: Telemetry facade.

        Returns:
            A metrics dictionary for the committed compaction.

        Raises:
            RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: CompactionConfig = self.config
        rewrites: List[RewriteResult] = [RewriteResult.from_json(j) for j in rewrite_jsons]

        def action() -> Dict[str, int]:
            """Commit the rewrites against the latest dataset version.

            Returns:
                The compaction metrics for this commit.
            """
            dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
            metrics = Compaction.commit(dataset, rewrites)
            telemetry.incr("dataset.committed")
            return {
                "fragments_removed": metrics.fragments_removed,
                "fragments_added": metrics.fragments_added,
                "files_removed": metrics.files_removed,
                "files_added": metrics.files_added,
            }

        return commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.commit_conflict"),
        )

    def cleanup(self, uri: str, telemetry: Telemetry) -> int:
        """Prune old versions of a dataset after compaction.

        Args:
            uri: Dataset URI.
            telemetry: Telemetry facade.

        Returns:
            The number of bytes reclaimed.
        """
        config: CompactionConfig = self.config
        older_than: Optional[timedelta] = (
            timedelta(seconds=config.cleanup_older_than_seconds)
            if config.cleanup_older_than_seconds is not None
            else None
        )
        dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
        with telemetry.timed("dataset.cleanup_ms"):
            stats = dataset.cleanup_old_versions(
                older_than=older_than, retain_versions=config.retain_versions
            )
        telemetry.distribution("dataset.bytes_removed", stats.bytes_removed)
        return int(stats.bytes_removed)

    def compact_one(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> Dict[str, Any]:
        """Plan, execute across executors, and commit one dataset's compaction.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset.
        """
        config: CompactionConfig = self.config
        dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
        plan = Compaction.plan(dataset, options=config.plan_options())
        task_jsons: List[str] = [task.json() for task in plan.tasks]
        if not task_jsons:
            bytes_removed: int = self.cleanup(uri, telemetry) if config.run_cleanup else 0
            return {"uri": uri, "tasks": 0, "fragments_removed": 0, "bytes_removed": bytes_removed}

        plan_version: int = plan.read_version
        storage_options: Optional[Dict[str, Any]] = config.storage_options

        def execute_task(task_json: str) -> str:
            """Execute one rewrite task on an executor and return its JSON.

            Args:
                task_json: The serialized compaction task.

            Returns:
                The serialized rewrite result.
            """
            shard_dataset: "lance.LanceDataset" = lance.dataset(
                uri, version=plan_version, storage_options=storage_options
            )
            task: CompactionTask = CompactionTask.from_json(task_json)
            return task.execute(shard_dataset).json()

        with telemetry.timed("dataset.rewrite_ms"):
            rewrite_jsons: List[str] = (
                spark.sparkContext.parallelize(task_jsons, min(len(task_jsons), config.max_tasks))
                .map(execute_task)
                .collect()
            )
        with telemetry.timed("dataset.commit_ms"):
            metrics: Dict[str, Any] = self.commit_rewrites(uri, rewrite_jsons, telemetry)

        bytes_removed = self.cleanup(uri, telemetry) if config.run_cleanup else 0
        telemetry.incr("dataset.compacted")
        result: Dict[str, Any] = {"uri": uri, "tasks": len(task_jsons), "bytes_removed": bytes_removed}
        result.update(metrics)
        return result

    def run(self, spark: SparkSession, dataset_uris: Iterable[str]) -> List[Dict[str, Any]]:
        """Compact each dataset as its own distributed plan.

        One distributed job is run per dataset, which suits the few large
        datasets that accumulate fragments; any failure propagates and fails the
        job. Datasets needing no compaction yield an empty plan and are skipped.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to compact, typically those changed recently.

        Returns:
            One statistics dictionary per dataset.
        """
        config: CompactionConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.compaction.run") as run_span:
            uris: List[str] = list(dataset_uris)
            run_span.set_tag("dataset_count", len(uris))
            results: List[Dict[str, Any]] = []
            for uri in uris:
                with driver_telemetry.span("lance.compaction.dataset") as dataset_span:
                    dataset_span.set_tag("uri", uri)
                    try:
                        with driver_telemetry.timed("dataset.total_ms", tags=[f"uri:{uri}"]):
                            stats: Dict[str, Any] = self.compact_one(spark, uri, driver_telemetry)
                    except Exception:
                        driver_telemetry.error(f"compaction failed for {uri}", tags=[f"uri:{uri}"])
                        raise
                    fragments_removed: int = int(stats.get("fragments_removed", 0))
                    dataset_span.set_tag("tasks", stats["tasks"])
                    dataset_span.set_tag("fragments_removed", fragments_removed)
                    dataset_span.set_tag("fragments_added", int(stats.get("fragments_added", 0)))
                    driver_telemetry.gauge(
                        "dataset.fragments_removed", fragments_removed, tags=[f"uri:{uri}"]
                    )
                    logger.info(
                        "compacted %s: %d tasks, %d fragments removed",
                        uri,
                        stats["tasks"],
                        fragments_removed,
                    )
                    results.append(stats)
            bytes_removed: int = sum(int(item.get("bytes_removed", 0)) for item in results)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.bytes_removed", bytes_removed)
            logger.info(
                "compaction run: %d datasets, %d bytes reclaimed", len(results), bytes_removed
            )
            return results
