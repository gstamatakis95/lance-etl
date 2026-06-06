"""Two-tier distributed compaction for a fleet of per-tenant Lance datasets.

Tier A (small datasets) batches every dataset URI into one Spark job: each executor task opens its dataset, counts
fragments, and when the count is at or below ``large_dataset_fragment_threshold`` runs the whole compaction in
process with ``Compaction.execute`` followed by ``cleanup_old_versions``. Because ``Compaction.execute`` goes
through the full options parser, every option in :class:`CompactionConfig` — including ``defer_index_remap`` and
``max_source_fragments`` — is honored on this tier. Datasets above the threshold are only classified by the executor
and returned to the driver as tier-B candidates.

Tier B (large datasets) keeps the distributed plan/execute/commit triad: the driver builds a ``Compaction.plan``,
rewrite tasks fan out across executors as JSON, and the driver commits the collected rewrites in one transaction. The
Python ``Compaction.commit`` binding hard-codes default compaction options, so ``defer_index_remap`` cannot take effect
on this tier: every index covering a rewritten fragment is remapped inline during the commit. The rewrite tasks
themselves capture the row addresses deferral needs, so this is a gap in the Python binding, not a format limitation.
Budget driver commit time accordingly for heavily indexed head datasets. The rewrite I/O itself still runs on executors.
``max_source_fragments`` is applied at plan time and caps how many fragments one run consumes, enabling incremental
compaction of head datasets. Multiple tier-B datasets run concurrently from a driver thread pool. Each worker thread
pins its Spark jobs to the FAIR scheduler pool named by ``scheduler_pool``, so set ``spark.scheduler.mode=FAIR`` (and
optionally an allocation file defining the pool) on the session.

A tier-B commit conflict is never resolved by re-committing: ``commit_compaction`` pins its conflict scan to the plan
version, so the same conflicting transaction is found on every attempt. The compactor instead treats a commit conflict
as "rewrite results are stale" and loops back to plan plus re-execute, up to ``replan_budget`` cycles. A small
``large_commit_retries`` budget remains around the commit itself purely for the raw manifest-write race. When every
cycle conflicts, the dataset is skipped for this run with a hot-dataset metric and picked up by the next cycle.

When ``defer_index_remap`` takes effect, the commit records a ``__lance_frag_reuse`` system index, visible in
``describe_indices()``, instead of rewriting the covering indices. No explicit follow-up step is required: the
frag-reuse index is applied lazily at read time, with index fragment bitmaps and row ids remapped through it whenever an
index is loaded, so queries stay correct against compacted data. Indices catch up permanently the next time they are
rebuilt or optimized. Pruning stale frag-reuse versions is Rust-only at this Lance commit with no Python API, so no
cleanup step for it is scheduled here.

Commits run through :func:`lance_etl.telemetry.commit_with_retries`, which retries ``OSError`` / ``RuntimeError`` whose
message marks a Lance commit conflict and re-raises the original exception when the budget is exhausted, so the jobs
coexist with concurrent ingestion and indexing while still failing fast on real errors.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import lance
from lance.optimize import Compaction, CompactionMetrics, CompactionTask, RewriteResult
from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig, commit_with_retries, is_commit_conflict_error

logger: logging.Logger = logging.getLogger(__name__)

COMPACTION_MODES: tuple[str, ...] = ("reencode", "try_binary_copy")
"""Accepted compaction modes. ``force_binary_copy`` is rejected because it errors instead of falling back when a
fragment is incompatible with binary copy, failing whole rewrite tasks on deletion-bearing fragments."""

MIN_CLEANUP_HORIZON_SECONDS: int = 6 * 3600
"""Floor for ``cleanup_older_than_seconds``. Version cleanup is not a transaction: an aggressive horizon can delete the
transaction files an in-flight committer needs to rebase from its read version, breaking the longest-running tier-B
plan-to-commit cycle on a head dataset. Several hours comfortably exceeds any single job."""


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
        materialize_deletions_threshold: Deletion fraction above which a fragment is rewritten to drop deleted rows.
        defer_index_remap: Defer index remap instead of rewriting indices inline. Honored only on the small-dataset
            tier, where ``Compaction.execute`` parses all options. The large-dataset tier ignores it because the Python
            ``Compaction.commit`` binding commits with default options and always remaps indices inline. Defaults to
            ``False``: on the pinned lance build a deferred remap leaves indexed vector queries failing with a missing
            fragment-id error until the remap runs, so deferral is opt-in for pipelines that remap before serving.
        max_source_fragments: Cap on source fragments consumed per run, oldest first, for incremental compaction of
            large datasets. ``None`` means no limit. ``0`` is rejected: it is not a disable sentinel and would be
            refused by Lance's option parser.
        num_threads: Worker threads inside a single rewrite task.
        batch_size: Rows per batch when rewriting.
        compaction_mode: ``"reencode"`` or ``"try_binary_copy"``. Defaults to ``"try_binary_copy"``, which skips
            decode and re-encode entirely when fragments are compatible and falls back to reencode per task otherwise.
            Fragments with deletion files fall back automatically. ``"force_binary_copy"`` is rejected because it
            errors instead of falling back.
        max_tasks: Maximum number of Spark tasks for one dataset's rewrites.
        large_dataset_fragment_threshold: Fragment count above which a dataset is compacted with the distributed plan
            instead of in one executor task.
        batch_partitions: Maximum Spark partitions for the small-dataset batch job.
        max_concurrent_large: Driver threads running large-dataset compactions concurrently.
        scheduler_pool: Spark FAIR scheduler pool for large-dataset jobs.
        run_cleanup: Whether to prune old versions after committing.
        cleanup_older_than_seconds: Age threshold for version cleanup. ``None`` keeps the Lance default. Explicit
            values below :data:`MIN_CLEANUP_HORIZON_SECONDS` are rejected because cleanup is not a transaction and can
            delete the transaction files an in-flight committer needs to rebase.
        retain_versions: Number of recent versions to retain.
        commit_retries: Retry budget for commit conflicts on the small tier, where the retry action re-plans and
            re-executes the whole compaction so each attempt is productive.
        commit_backoff_seconds: Base backoff between commit retries.
        large_commit_retries: Retry budget around the tier-B ``Compaction.commit`` call. Kept small because the commit
            pins its conflict scan to the plan version, so a semantic conflict re-fails deterministically and only the
            raw manifest-write race benefits from a retry.
        replan_budget: Plan/execute/commit cycles attempted per tier-B dataset before the run skips it as hot and
            defers it to the next cycle.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    target_rows_per_fragment: int | None = None
    max_rows_per_group: int | None = None
    max_bytes_per_file: int | None = None
    materialize_deletions: bool | None = True
    materialize_deletions_threshold: float | None = None
    defer_index_remap: bool = False
    max_source_fragments: int | None = None
    num_threads: int | None = None
    batch_size: int | None = None
    compaction_mode: str = "try_binary_copy"
    max_tasks: int = 256
    large_dataset_fragment_threshold: int = 128
    batch_partitions: int = 512
    max_concurrent_large: int = 4
    scheduler_pool: str = "lance-compaction"
    run_cleanup: bool = True
    cleanup_older_than_seconds: int | None = None
    retain_versions: int | None = None
    commit_retries: int = 20
    commit_backoff_seconds: float = 0.5
    large_commit_retries: int = 2
    replan_budget: int = 3

    def execute_options(self) -> dict[str, Any]:
        """Build the full options dict for single-process ``Compaction.execute``.

        Returns:
            Options accepted by ``Compaction.execute``, omitting unset values.

        Raises:
            ValueError: If ``max_source_fragments`` is ``0`` (use ``None`` for unlimited) or if ``compaction_mode`` is
                not one of :data:`COMPACTION_MODES`.
        """
        if self.max_source_fragments == 0:
            raise ValueError("max_source_fragments=0 is not supported; use None to disable the limit")
        if self.compaction_mode not in COMPACTION_MODES:
            raise ValueError(f"compaction_mode must be one of {COMPACTION_MODES}, got {self.compaction_mode!r}")
        candidates: dict[str, Any] = {
            "target_rows_per_fragment": self.target_rows_per_fragment,
            "max_rows_per_group": self.max_rows_per_group,
            "max_bytes_per_file": self.max_bytes_per_file,
            "materialize_deletions": self.materialize_deletions,
            "materialize_deletions_threshold": self.materialize_deletions_threshold,
            "defer_index_remap": self.defer_index_remap,
            "max_source_fragments": self.max_source_fragments,
            "num_threads": self.num_threads,
            "batch_size": self.batch_size,
            "compaction_mode": self.compaction_mode,
        }
        return {name: value for name, value in candidates.items() if value is not None}

    def plan_options(self) -> dict[str, Any]:
        """Build the options dict for the distributed ``Compaction.plan`` path.

        ``defer_index_remap`` is excluded because the distributed commit uses default options and remaps indices inline
        regardless of the plan-time setting. A non-default ``defer_index_remap=False`` therefore has no effect on this
        tier, and a warning is logged so operators are not silently surprised. It still applies on the small-dataset
        tier, where ``Compaction.execute`` parses all options.

        Returns:
            Options accepted by ``Compaction.plan``, omitting unset values.
        """
        options: dict[str, Any] = self.execute_options()
        options.pop("defer_index_remap", None)
        if not self.defer_index_remap:
            logger.warning(
                "defer_index_remap=False is ignored on the large-dataset tier: the distributed Compaction.commit "
                "binding uses default options and remaps indices inline. The setting only affects the "
                "small-dataset tier."
            )
        return options


def compaction_metrics_dict(metrics: CompactionMetrics) -> dict[str, int]:
    """Convert Lance compaction metrics into a plain dictionary.

    Args:
        metrics: Metrics returned by ``Compaction.execute`` or ``Compaction.commit``.

    Returns:
        The four fragment and file counters as a dictionary.
    """
    return {
        "fragments_removed": metrics.fragments_removed,
        "fragments_added": metrics.fragments_added,
        "files_removed": metrics.files_removed,
        "files_added": metrics.files_added,
    }


def cleanup_dataset(uri: str, config: CompactionConfig, telemetry: Telemetry) -> int:
    """Prune old versions of a dataset after compaction.

    ``delete_unverified`` is never passed, so the 7-day unverified threshold keeps protecting executor-written rewrite
    and index-segment files that are unreferenced until their driver commit.

    Args:
        uri: Dataset URI.
        config: Compaction configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        The number of bytes reclaimed.

    Raises:
        ValueError: If ``cleanup_older_than_seconds`` is set below :data:`MIN_CLEANUP_HORIZON_SECONDS`. The horizon
            must exceed the longest-running concurrent job so its rebase can still read old transaction files.
    """
    if (
        config.cleanup_older_than_seconds is not None
        and config.cleanup_older_than_seconds < MIN_CLEANUP_HORIZON_SECONDS
    ):
        raise ValueError(
            f"cleanup_older_than_seconds={config.cleanup_older_than_seconds} is below the safe floor of "
            f"{MIN_CLEANUP_HORIZON_SECONDS}; cleanup horizons must exceed the longest concurrent job"
        )
    older_than: timedelta | None = (
        timedelta(seconds=config.cleanup_older_than_seconds) if config.cleanup_older_than_seconds is not None else None
    )
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    with telemetry.timed("dataset.cleanup_ms"):
        stats = dataset.cleanup_old_versions(older_than=older_than, retain_versions=config.retain_versions)
    telemetry.distribution("dataset.bytes_removed", stats.bytes_removed)
    return int(stats.bytes_removed)


def compact_small_dataset(uri: str, config: CompactionConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Compact one small dataset entirely inside the current executor task.

    Runs ``Compaction.execute``, which honors every configured option including ``defer_index_remap`` and
    ``max_source_fragments``, then prunes old versions. Commit conflicts are retried by re-running the whole compaction
    against the latest version.

    Args:
        uri: Dataset URI.
        config: Compaction configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A statistics dictionary for the dataset with ``tier`` set to ``"small"``.
    """

    def action() -> dict[str, int]:
        """Run the full in-process compaction against the latest version.

        Returns:
            The compaction metrics for this attempt.
        """
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        metrics: CompactionMetrics = Compaction.execute(dataset, config.execute_options())
        telemetry.incr("dataset.committed")
        return compaction_metrics_dict(metrics)

    with telemetry.timed("dataset.total_ms", tags=[f"uri:{uri}"]):
        metrics: dict[str, int] = commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.commit_conflict"),
        )
        bytes_removed: int = cleanup_dataset(uri, config, telemetry) if config.run_cleanup else 0
    telemetry.incr("dataset.compacted")
    result: dict[str, Any] = {"uri": uri, "tier": "small", "tasks": 1, "bytes_removed": bytes_removed}
    result.update(metrics)
    return result


def classify_or_compact(uri: str, config: CompactionConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Compact a small dataset in process, or flag a large one for tier B.

    Args:
        uri: Dataset URI.
        config: Compaction configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        Small-tier statistics, or ``{"uri", "tier": "large", "fragments"}`` for
        datasets whose fragment count exceeds the threshold.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragments: int = int(dataset.stats.dataset_stats()["num_fragments"])
    if fragments > config.large_dataset_fragment_threshold:
        telemetry.incr("dataset.deferred_to_large_tier")
        return {"uri": uri, "tier": "large", "fragments": fragments}
    return compact_small_dataset(uri, config, telemetry)


class LanceCompactor:
    """Compacts a fleet of Lance datasets with two-tier orchestration."""

    def __init__(self, config: CompactionConfig) -> None:
        """Initialize the compactor.

        Args:
            config: Compaction configuration.
        """
        self.config: CompactionConfig = config

    def commit_rewrites(self, uri: str, rewrite_jsons: list[str], telemetry: Telemetry) -> dict[str, Any]:
        """Commit serialized rewrites with a deliberately small retry budget.

        The commit remaps every index touching the rewritten fragments inline, since the Python binding commits with
        default compaction options. Retrying the commit cannot resolve a semantic conflict: the conflict scan is pinned
        to the plan version, so the same conflicting transaction is found on every attempt. The small
        ``large_commit_retries`` budget only covers the raw manifest-write race. Semantic conflicts escape to
        :meth:`compact_one`, whose re-plan loop is the productive retry.

        Args:
            uri: Dataset URI.
            rewrite_jsons: Serialized rewrite results from the executors.
            telemetry: Telemetry facade.

        Returns:
            A metrics dictionary for the committed compaction.

        Raises:
            OSError | RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: CompactionConfig = self.config
        rewrites: list[RewriteResult] = [RewriteResult.from_json(j) for j in rewrite_jsons]

        def action() -> dict[str, int]:
            """Commit the rewrites against the latest dataset version.

            Returns:
                The compaction metrics for this commit.
            """
            dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            metrics: CompactionMetrics = Compaction.commit(dataset, rewrites)
            telemetry.incr("dataset.committed")
            return compaction_metrics_dict(metrics)

        return commit_with_retries(
            action,
            config.large_commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.commit_conflict"),
        )

    def execute_plan(self, spark: SparkSession, uri: str, plan_version: int, task_jsons: list[str]) -> list[str]:
        """Fan one compaction plan's rewrite tasks out across executors.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            plan_version: The dataset version the plan was built against.
            task_jsons: Serialized compaction tasks from the plan.

        Returns:
            The serialized rewrite results, one per task.
        """
        config: CompactionConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options

        def execute_task(task_json: str) -> str:
            """Execute one rewrite task on an executor and return its JSON.

            Args:
                task_json: The serialized compaction task.

            Returns:
                The serialized rewrite result.
            """
            shard_dataset: lance.LanceDataset = lance.dataset(
                uri, version=plan_version, storage_options=storage_options
            )
            task: CompactionTask = CompactionTask.from_json(task_json)
            return task.execute(shard_dataset).json()

        return (
            spark.sparkContext.parallelize(task_jsons, min(len(task_jsons), config.max_tasks))
            .map(execute_task)
            .collect()
        )

    def compact_one(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Plan, execute across executors, and commit one large dataset's compaction.

        The driver only plans and commits. Rewrite I/O runs on executors. Index remap happens inline during the driver
        commit (the binding ignores ``defer_index_remap`` here, a binding gap rather than a format limitation), so
        commit duration grows with the number and size of indices covering rewritten fragments. With
        ``max_source_fragments`` set, each run consumes a bounded slice of the oldest fragments for incremental
        compaction. Spark jobs submitted from the calling thread are pinned to the configured FAIR scheduler pool.

        A commit conflict means the rewrite results are stale, so the loop re-plans and re-executes against the latest
        version instead of re-committing, which would re-fail deterministically. After ``replan_budget`` conflicting
        cycles the dataset is skipped for this run with a hot-dataset metric and deferred to the next cycle.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset with ``tier`` set to ``"large"``. Skipped hot datasets carry a
            ``"skipped"`` reason instead of commit metrics.
        """
        config: CompactionConfig = self.config
        try:
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", config.scheduler_pool)
            tasks_attempted: int = 0
            for cycle in range(1, config.replan_budget + 1):
                dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
                plan = Compaction.plan(dataset, options=config.plan_options())
                task_jsons: list[str] = [task.json() for task in plan.tasks]
                if not task_jsons:
                    bytes_removed: int = cleanup_dataset(uri, config, telemetry) if config.run_cleanup else 0
                    return {
                        "uri": uri,
                        "tier": "large",
                        "tasks": 0,
                        "fragments_removed": 0,
                        "bytes_removed": bytes_removed,
                    }

                tasks_attempted = len(task_jsons)
                with telemetry.timed("dataset.rewrite_ms"):
                    rewrite_jsons: list[str] = self.execute_plan(spark, uri, plan.read_version, task_jsons)
                try:
                    with telemetry.timed("dataset.commit_ms"):
                        metrics: dict[str, Any] = self.commit_rewrites(uri, rewrite_jsons, telemetry)
                except (OSError, RuntimeError) as exc:
                    if not is_commit_conflict_error(exc):
                        raise
                    telemetry.incr("dataset.replanned", tags=[f"uri:{uri}"])
                    logger.warning(
                        "compaction commit conflicted for %s (cycle %d/%d); re-planning at the latest version",
                        uri,
                        cycle,
                        config.replan_budget,
                    )
                    continue

                bytes_removed = cleanup_dataset(uri, config, telemetry) if config.run_cleanup else 0
                telemetry.incr("dataset.compacted")
                result: dict[str, Any] = {
                    "uri": uri,
                    "tier": "large",
                    "tasks": tasks_attempted,
                    "bytes_removed": bytes_removed,
                }
                result.update(metrics)
                return result

            telemetry.incr("dataset.hot_skipped", tags=[f"uri:{uri}"])
            logger.warning(
                "skipping compaction of hot dataset %s: commit conflicted on all %d plan/execute/commit cycles",
                uri,
                config.replan_budget,
            )
            return {
                "uri": uri,
                "tier": "large",
                "tasks": tasks_attempted,
                "bytes_removed": 0,
                "skipped": f"commit conflicted on all {config.replan_budget} re-plan cycles; deferred to the next run",
            }
        finally:
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

    def compact_large_tier(self, spark: SparkSession, uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Compact large datasets concurrently from a driver thread pool.

        Each worker thread runs one dataset's plan/execute/commit cycle and pins its Spark jobs to the FAIR scheduler
        pool, so several large datasets share the cluster instead of queueing FIFO. All datasets are attempted. The
        first failure is re-raised after the pool drains.

        Args:
            spark: Active Spark session.
            uris: Large-dataset URIs from the classification pass.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset.

        Raises:
            Exception: The first per-dataset failure, after all datasets finish.
        """
        config: CompactionConfig = self.config
        results: list[dict[str, Any]] = []
        failures: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=config.max_concurrent_large) as pool:
            futures: dict[Future[dict[str, Any]], str] = {
                pool.submit(self.compact_one, spark, uri, telemetry): uri for uri in uris
            }
            for future in as_completed(futures):
                uri: str = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    telemetry.error(f"compaction failed for {uri}", tags=[f"uri:{uri}"])
                    failures.append(exc)
        if failures:
            raise failures[0]
        return results

    def run(self, spark: SparkSession, dataset_uris: Iterable[str]) -> list[dict[str, Any]]:
        """Compact every dataset with two-tier orchestration.

        One Spark job covers all URIs: each executor task classifies its dataset by fragment count and compacts it in
        process (including version cleanup) when small. Datasets above the fragment threshold are then compacted with
        the distributed plan path, several at a time from a driver thread pool on the FAIR scheduler pool. Failures
        propagate and fail the job.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to compact, typically those changed recently.

        Returns:
            One statistics dictionary per dataset.
        """
        config: CompactionConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.compaction.run") as run_span:
            uris: list[str] = list(dataset_uris)
            run_span.set_tag("dataset_count", len(uris))
            if not uris:
                return []

            def small_tier(partition: Iterable[str]) -> Iterator[dict[str, Any]]:
                """Classify and compact one partition of dataset URIs on an executor.

                Args:
                    partition: Dataset URIs assigned to this executor task.

                Yields:
                    One outcome dictionary per dataset.
                """
                executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
                for uri in partition:
                    yield classify_or_compact(uri, config, executor_telemetry)

            partitions: int = min(len(uris), config.batch_partitions)
            with driver_telemetry.timed("run.small_tier_ms"):
                outcomes: list[dict[str, Any]] = (
                    spark.sparkContext.parallelize(uris, partitions).mapPartitions(small_tier).collect()
                )
            results: list[dict[str, Any]] = [item for item in outcomes if item["tier"] == "small"]
            large_uris: list[str] = [item["uri"] for item in outcomes if item["tier"] == "large"]
            run_span.set_tag("small_datasets", len(results))
            run_span.set_tag("large_datasets", len(large_uris))
            logger.info("small tier compacted %d datasets; %d deferred to large tier", len(results), len(large_uris))

            if large_uris:
                with driver_telemetry.timed("run.large_tier_ms"):
                    results.extend(self.compact_large_tier(spark, large_uris, driver_telemetry))

            bytes_removed: int = sum(int(item.get("bytes_removed", 0)) for item in results)
            fragments_removed: int = sum(int(item.get("fragments_removed", 0)) for item in results)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.bytes_removed", bytes_removed)
            driver_telemetry.gauge("run.fragments_removed", fragments_removed)
            logger.info(
                "compaction run: %d datasets, %d fragments removed, %d bytes reclaimed",
                len(results),
                fragments_removed,
                bytes_removed,
            )
            return results
