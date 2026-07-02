"""Fleet maintenance job: TTL expiration, unified task-based compaction, and version cleanup.

Owns :class:`MaintenanceConfig`, :class:`MaintenanceJob`, and the three phase functions of the
unified compaction flow. Every dataset, regardless of size, follows the same
plan-execute-commit path built on the same Lance APIs:

- Phase P (:func:`plan_one_dataset`): a per-dataset executor fan-out runs the TTL delete, the
  derived-state skip check, and ``Compaction.plan``, returning serialized rewrite tasks. A
  small dataset yields one task and a large one yields many.
- Phase E (:meth:`MaintenanceJob.execute_fleet_tasks`): every dataset's rewrite tasks run in
  ONE flat Spark job, so Spark schedules the whole fleet's work instead of driver thread pools.
- Phase C (:func:`commit_one_dataset`): a per-dataset executor fan-out commits the collected
  rewrites and prunes old versions. A commit conflict marks the dataset for the next replan
  round instead of retrying stale rewrites.

:meth:`MaintenanceJob.run` repeats the three phases for conflicted datasets up to
``replan_budget`` rounds, then defers the survivors to the next scheduled run.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import lance
from lance.optimize import Compaction, CompactionMetrics, CompactionTask, RewriteResult
from pyspark.sql import SparkSession

from lance_etl.telemetry import (
    DEFAULT_COMMIT_RETRIES,
    DEFAULT_LARGE_COMMIT_RETRIES,
    Telemetry,
    TelemetryConfig,
    commit_with_retries,
    is_commit_conflict_error,
)

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class MaintenanceConfig:
    """Configuration for :class:`MaintenanceJob`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        ttl_column: Per-row TTL column holding each row's lifetime as an Arrow ``Duration``.
            ``None`` (default) turns TTL off. When set, rows where
            ``ts_column + ttl_column < now`` are deleted before compaction.
        ts_column: Event timestamp column used as the TTL clock; must match ``ETLConfig.ts_col``.
        target_rows_per_fragment: Desired rows per compacted fragment; matches lance's
            ``CompactionOptions`` default of ``1_048_576`` so the default is explicit and immune
            to upstream shifts.
        max_rows_per_group: Maximum rows per group within a fragment.
        max_bytes_per_file: Maximum bytes per compacted file.
        materialize_deletions_threshold: Deletion fraction above which a fragment is rewritten;
            default ``0.1`` matches lance's own default. An inline index remap is triggered when
            covered fragments are rewritten, so budget commit time accordingly for heavily
            indexed head datasets.
        defer_index_remap: Defer index remap at commit time through the options passed to
            ``Compaction.commit``.
        max_source_fragments: Cap on source fragments consumed per run for incremental
            compaction; ``None`` is unbounded, ``0`` is rejected.
        num_threads: Worker threads inside a single rewrite task.
        batch_size: Rows per batch when rewriting.
        max_tasks: Upper bound on Spark partitions for the flat fleet-wide rewrite job.
        batch_partitions: Maximum Spark partitions for the per-dataset plan and commit fan-outs.
        cleanup_older_than_seconds: Age threshold for version cleanup; default ``172_800`` (2
            days) with the HEAD-tag exemption keeps rollback headroom while cutting manifest
            storage. ``None`` defers to lance's 14-day default. Values below
            ``min_cleanup_horizon_seconds`` are rejected.
        retain_versions: Number of recent versions to retain regardless of age.
        commit_retries: Retry budget for TTL delete commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
        large_commit_retries: Retry budget around ``Compaction.commit``; kept small because
            semantic conflicts re-fail deterministically and only the raw manifest-write race
            benefits from a retry.
        replan_budget: Plan/execute/commit rounds a conflicted dataset participates in before
            it is skipped as hot and deferred to the next scheduled run.
        compaction_mode: Lance compaction mode for ``Compaction.execute`` and
            ``Compaction.plan``; ``try_binary_copy`` falls back to reencode per task, never
            errors on deletion-bearing fragments unlike ``force_binary_copy``.
        min_cleanup_horizon_seconds: Floor for ``cleanup_older_than_seconds``; cleanup is not
            transactional, so the floor must exceed the longest concurrent job to protect
            in-flight committer rebase files.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    ttl_column: str | None = None
    ts_column: str = "event_timestamp"
    target_rows_per_fragment: int = 1_048_576
    max_rows_per_group: int | None = None
    max_bytes_per_file: int | None = None
    materialize_deletions_threshold: float = 0.1
    defer_index_remap: bool = False
    max_source_fragments: int | None = 256
    num_threads: int | None = None
    batch_size: int | None = None
    max_tasks: int = 256
    batch_partitions: int = 512
    cleanup_older_than_seconds: int | None = 172_800
    retain_versions: int | None = None
    commit_retries: int = DEFAULT_COMMIT_RETRIES
    commit_backoff_seconds: float = 0.5
    large_commit_retries: int = DEFAULT_LARGE_COMMIT_RETRIES
    replan_budget: int = 3
    compaction_mode: str = "try_binary_copy"
    min_cleanup_horizon_seconds: int = 6 * 3600

    def ttl_active(self) -> bool:
        """Report whether the TTL step runs for this configuration.

        Returns:
            ``True`` when a per-row TTL column is configured, ``False`` otherwise (the default
            no-op).
        """
        return self.ttl_column is not None

    def execute_options(self) -> dict[str, Any]:
        """Build the compaction options dict shared by ``execute``, ``plan``, and ``commit``.

        Deleted rows are always materialized and the mode is taken from
        :attr:`compaction_mode`. The same dict is passed to ``Compaction.plan`` and to
        ``Compaction.commit`` so commit-time options such as ``defer_index_remap`` take effect
        uniformly.

        Returns:
            Options accepted by the Compaction entry points, omitting unset optional values.

        Raises:
            ValueError: If ``max_source_fragments`` is ``0`` (use ``None`` for unlimited).
        """
        if self.max_source_fragments == 0:
            raise ValueError("max_source_fragments=0 is not supported; use None to disable the limit")
        candidates: dict[str, Any] = {
            "target_rows_per_fragment": self.target_rows_per_fragment,
            "max_rows_per_group": self.max_rows_per_group,
            "max_bytes_per_file": self.max_bytes_per_file,
            "materialize_deletions": True,
            "materialize_deletions_threshold": self.materialize_deletions_threshold,
            "max_source_fragments": self.max_source_fragments,
            "num_threads": self.num_threads,
            "batch_size": self.batch_size,
            "compaction_mode": self.compaction_mode,
        }
        if self.defer_index_remap:
            candidates["defer_index_remap"] = True
        return {name: value for name, value in candidates.items() if value is not None}


def validate_column_name(column: str, schema: Any) -> None:
    """Validate that a TTL predicate column exists in the dataset schema.

    Catches silent misconfigurations where the column name was changed but the config was not
    updated, before any predicate is constructed.

    Args:
        column: The column name from :attr:`MaintenanceConfig.ttl_column` or
            :attr:`MaintenanceConfig.ts_column`.
        schema: The pyarrow schema of the target dataset.

    Raises:
        KeyError: If the column is not present in the dataset schema.
    """
    column_names: list[str] = schema.names
    if column not in column_names:
        raise KeyError(f"column {column!r} is not present in the dataset schema. Available columns: {column_names}")


def build_ttl_predicate(ts_column: str, ttl_column: str, cutoff: datetime) -> str:
    """Build the Lance SQL delete predicate for per-row TTL expiration.

    The predicate is ``{ts_column} + {ttl_column} < TIMESTAMP '{iso_cutoff}'`` which deletes
    every row whose event timestamp plus its own lifetime is strictly before the cutoff instant.
    Lance evaluates the timestamp-plus-duration column arithmetic natively. Both column names have
    already been validated against the dataset schema by :func:`validate_column_name` before this
    function is called.

    The cutoff is formatted in UTC with microsecond resolution as
    ``YYYY-MM-DDTHH:MM:SS.ffffff``, which DataFusion accepts as a timestamp literal.

    Args:
        ts_column: The validated event timestamp column name.
        ttl_column: The validated per-row TTL (``Duration``) column name.
        cutoff: The UTC cutoff instant. Rows whose timestamp plus lifetime is strictly before this
            are expired.

    Returns:
        A Lance SQL predicate string safe for passing to :meth:`lance.LanceDataset.delete`.
    """
    literal: str = cutoff.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return f"{ts_column} + {ttl_column} < TIMESTAMP '{literal}'"


def compute_cutoff() -> datetime:
    """Compute the TTL cutoff as the current instant in UTC.

    Per-row expiry is decided by each row's own timestamp plus lifetime against this single
    instant, so the cutoff is simply now in UTC rather than a global ``now - retention`` window.

    Returns:
        The current UTC instant.
    """
    return datetime.now(tz=UTC)


def run_ttl_on_open_dataset(
    dataset: lance.LanceDataset,
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run the TTL delete step against an already-open dataset handle for schema validation.

    Validates the TTL and timestamp column names against the schema of the supplied open handle,
    then delegates the actual delete to a re-opening retry action so each commit attempt operates
    against the latest version (required for rebase correctness). A dataset that lacks the TTL or
    timestamp column is skipped with a warning and a metric rather than failing the task.

    Args:
        dataset: An already-open Lance dataset handle used only for schema validation.
        uri: Dataset URI matching the open handle.
        config: Maintenance configuration with ``ttl_column`` set.
        cutoff: The precomputed cutoff instant shared across the run.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A result dictionary with keys ``uri``, ``ttl_rows_deleted``, and ``skipped``.
    """
    ttl_column: str = config.ttl_column if config.ttl_column is not None else ""
    try:
        validate_column_name(config.ts_column, dataset.schema)
        validate_column_name(ttl_column, dataset.schema)
    except KeyError as exc:
        logger.warning("ttl: TTL column missing in %s, skipping expiration: %s", uri, exc)
        telemetry.incr("dataset.ttl_column_missing")
        return {"uri": uri, "ttl_rows_deleted": 0, "skipped": str(exc)}

    predicate: str = build_ttl_predicate(config.ts_column, ttl_column, cutoff)

    def action() -> int:
        """Re-open the dataset and execute the delete on the latest version.

        Returns:
            The number of rows deleted.
        """
        fresh: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        delete_result: dict[str, Any] = fresh.delete(predicate, conflict_retries=config.commit_retries)
        return int(delete_result.get("num_deleted_rows", 0))

    with telemetry.timed("dataset.ttl_delete_ms"):
        rows_deleted: int = commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.ttl_commit_conflict"),
        )
    telemetry.distribution("dataset.ttl_rows_deleted", float(rows_deleted))
    if rows_deleted:
        telemetry.incr("dataset.ttl_expired")
    return {"uri": uri, "ttl_rows_deleted": rows_deleted, "skipped": ""}


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


def fan_out_per_dataset(
    spark: SparkSession,
    uris: list[str],
    telemetry_config: TelemetryConfig,
    per_dataset: Callable[[str, Telemetry], dict[str, Any]],
    partitions: int,
) -> list[dict[str, Any]]:
    """Run an independent per-dataset operation across executors, one task per partition.

    Each executor task creates its own telemetry facade and applies ``per_dataset`` to every URI
    in its partition. Used by the consolidated maintenance pass, the manifest migration, and the
    serving-tag flip, all of which are embarrassingly parallel one-call-per-dataset operations
    that differ only in the per-dataset callable.

    An empty ``uris`` list is returned immediately without submitting a Spark job, because
    ``sparkContext.parallelize`` with zero slices raises a Java exception.

    Args:
        spark: Active Spark session.
        uris: Dataset URIs to process.
        telemetry_config: Telemetry configuration created per executor process.
        per_dataset: The operation to apply to one URI with an executor-local telemetry facade.
        partitions: Upper bound on Spark partitions, capped at the URI count.

    Returns:
        One outcome dictionary per dataset.
    """
    if not uris:
        return []

    def partition(part: Iterable[str]) -> Iterator[dict[str, Any]]:
        """Apply the operation to one partition of dataset URIs on an executor.

        Args:
            part: Dataset URIs assigned to this executor task.

        Yields:
            One outcome dictionary per dataset.
        """
        executor_telemetry: Telemetry = Telemetry.create(telemetry_config)
        for uri in part:
            yield per_dataset(uri, executor_telemetry)

    return spark.sparkContext.parallelize(uris, min(len(uris), partitions)).mapPartitions(partition).collect()


def cleanup_dataset(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> int:
    """Prune old versions of a dataset after compaction.

    ``delete_unverified`` is never passed, so the 7-day unverified threshold keeps protecting
    executor-written rewrite and index-segment files that are unreferenced until their driver
    commit. ``error_if_tagged_old_versions=False`` is passed so a tagged version a serving layer
    is pinned to (see :func:`~lance_etl.maintenance.tools.update_serving_tag`) is left in place
    silently instead of raising: cleanup skips tagged versions regardless of age, and the
    blue-green serving tag must keep its version readable until the serving layer is flipped to a
    newer one.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        The number of bytes reclaimed.

    Raises:
        ValueError: If ``cleanup_older_than_seconds`` is set below
            ``config.min_cleanup_horizon_seconds``. The horizon must exceed the
            longest-running concurrent job so its rebase can still read old transaction files.
    """
    if (
        config.cleanup_older_than_seconds is not None
        and config.cleanup_older_than_seconds < config.min_cleanup_horizon_seconds
    ):
        raise ValueError(
            f"cleanup_older_than_seconds={config.cleanup_older_than_seconds} is below the safe floor of "
            f"{config.min_cleanup_horizon_seconds}; cleanup horizons must exceed the longest concurrent job"
        )
    older_than: timedelta | None = (
        timedelta(seconds=config.cleanup_older_than_seconds) if config.cleanup_older_than_seconds is not None else None
    )
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    with telemetry.timed("dataset.cleanup_ms"):
        stats = dataset.cleanup_old_versions(
            older_than=older_than,
            retain_versions=config.retain_versions,
            error_if_tagged_old_versions=False,
        )
    telemetry.distribution("dataset.bytes_removed", stats.bytes_removed)
    return int(stats.bytes_removed)


def compaction_skip_reason(dataset: lance.LanceDataset) -> str | None:
    """Return a reason string when the dataset has one or fewer fragments and needs no compaction.

    This is a conservative derived-state check using only the already-open dataset handle.
    A dataset with at most one fragment has nothing to compact. The check reads
    ``dataset.stats.dataset_stats()["num_fragments"]``, which is available from the in-memory
    manifest and requires no additional object-store I/O.

    Args:
        dataset: The already-open dataset handle.

    Returns:
        A human-readable skip reason when compaction is unnecessary, or ``None`` when work is
        needed.
    """
    num_fragments: int = int(dataset.stats.dataset_stats()["num_fragments"])
    if num_fragments <= 1:
        return f"only {num_fragments} fragment(s); nothing to compact"
    return None


def plan_one_dataset(
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime | None,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run phase P for one dataset on an executor: TTL delete, skip check, and compaction plan.

    Opens the dataset once, with failure isolation: a missing, corrupt, or unreadable dataset
    returns a skip dict and never aborts the fleet run. When TTL is active and a cutoff is
    supplied, expired rows are deleted first because the delete creates compaction work. The
    derived-state :func:`compaction_skip_reason` check and an empty ``Compaction.plan`` both end
    the dataset's run early with a cleanup pass. Otherwise the plan's rewrite tasks are
    serialized for the fleet-wide execute phase.

    The same function serves every dataset size: a small dataset yields one rewrite task and a
    large one yields many, so no separate in-process compaction path exists.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        cutoff: TTL cutoff instant, or ``None`` to skip the TTL step (replan rounds pass None so
            TTL runs exactly once per fleet run).
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A terminal result dict (``skipped`` or ``tasks: 0``), or a planned dict carrying
        ``read_version`` and ``task_jsons`` for the execute phase.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, OSError, ValueError) as exc:
        logger.warning("maintenance: cannot open dataset %s, skipping: %s", uri, exc)
        telemetry.incr("dataset.maintenance_open_error")
        return {"uri": uri, "skipped": str(exc), "bytes_removed": 0}

    result: dict[str, Any] = {"uri": uri}

    if config.ttl_active() and cutoff is not None:
        ttl_result: dict[str, Any] = run_ttl_on_open_dataset(dataset, uri, config, cutoff, telemetry)
        result["ttl_rows_deleted"] = ttl_result.get("ttl_rows_deleted", 0)
        if ttl_result.get("skipped"):
            result["ttl_skipped"] = ttl_result["skipped"]

    skip: str | None = compaction_skip_reason(dataset)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        result.update({"skipped": skip, "tasks": 0, "bytes_removed": cleanup_dataset(uri, config, telemetry)})
        return result

    plan = Compaction.plan(lance.dataset(uri, storage_options=config.storage_options), options=config.execute_options())
    task_jsons: list[str] = [task.json() for task in plan.tasks]
    if not task_jsons:
        result.update({"tasks": 0, "fragments_removed": 0, "bytes_removed": cleanup_dataset(uri, config, telemetry)})
        return result

    result.update({"read_version": plan.read_version, "task_jsons": task_jsons})
    return result


def execute_rewrite_task(
    uri: str,
    read_version: int,
    task_json: str,
    storage_options: dict[str, Any] | None,
) -> tuple[str, str]:
    """Execute one serialized rewrite task against its dataset's plan version on an executor.

    Module-level and parameterized by primitives only, so the Spark closure ships a small
    ``functools.partial`` instead of any job state.

    Args:
        uri: Dataset URI the task belongs to.
        read_version: The dataset version the plan was built against.
        task_json: The serialized compaction task.
        storage_options: Object-store options forwarded to lance.

    Returns:
        The dataset URI paired with the serialized rewrite result.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, version=read_version, storage_options=storage_options)
    task: CompactionTask = CompactionTask.from_json(task_json)
    return uri, task.execute(dataset).json()


def commit_one_dataset(
    uri: str,
    rewrite_jsons: list[str],
    config: MaintenanceConfig,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run phase C for one dataset on an executor: commit the rewrites and prune old versions.

    The configured compaction options are passed to ``Compaction.commit`` so commit-time options
    such as ``defer_index_remap`` take effect (pylance 8.0.0 carries the ``options`` parameter).

    Retrying the commit cannot resolve a semantic conflict: the conflict scan is pinned to the
    plan version, so the same conflicting transaction is found on every attempt. The small
    ``large_commit_retries`` budget only covers the raw manifest-write race. A semantic conflict
    returns a ``conflict`` marker so :meth:`MaintenanceJob.run` re-plans the dataset in the next
    round, which is the productive retry. Any non-conflict error propagates and fails the run.

    Args:
        uri: Dataset URI.
        rewrite_jsons: Serialized rewrite results collected from the execute phase.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A committed result dict with metrics and ``bytes_removed``, or ``{"uri", "conflict":
        True}`` when the dataset must be re-planned.
    """
    rewrites: list[RewriteResult] = [RewriteResult.from_json(document) for document in rewrite_jsons]

    def action() -> dict[str, int]:
        """Commit the rewrites against the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        metrics: CompactionMetrics = Compaction.commit(dataset, rewrites, options=config.execute_options())
        telemetry.incr("dataset.committed")
        return compaction_metrics_dict(metrics)

    try:
        with telemetry.timed("dataset.commit_ms"):
            metrics_dict: dict[str, int] = commit_with_retries(
                action,
                config.large_commit_retries,
                config.commit_backoff_seconds,
                lambda: telemetry.incr("dataset.commit_conflict"),
            )
    except (OSError, RuntimeError) as exc:
        if not is_commit_conflict_error(exc):
            raise
        telemetry.incr("dataset.replanned")
        logger.warning("compaction commit conflicted for %s; re-planning at the latest version", uri)
        return {"uri": uri, "conflict": True}

    bytes_removed: int = cleanup_dataset(uri, config, telemetry)
    telemetry.incr("dataset.compacted")
    result: dict[str, Any] = {"uri": uri, "tasks": len(rewrite_jsons), "bytes_removed": bytes_removed}
    result.update(metrics_dict)
    return result


class MaintenanceJob:
    """Runs TTL expiration, unified task-based compaction, and version cleanup over a Lance fleet."""

    def __init__(self, config: MaintenanceConfig) -> None:
        """Initialize the maintenance job.

        Args:
            config: Maintenance configuration.
        """
        self.config: MaintenanceConfig = config

    def execute_fleet_tasks(self, spark: SparkSession, tasks: list[tuple[str, int, str]]) -> dict[str, list[str]]:
        """Run every dataset's rewrite tasks in one flat Spark job (phase E).

        All datasets' tasks share one job, so Spark schedules the fleet's rewrite work across
        the cluster: a large dataset contributes many tasks and a small one contributes one,
        with no per-dataset job submission or driver thread pool.

        Args:
            spark: Active Spark session.
            tasks: ``(uri, read_version, task_json)`` triples flattened across the fleet.

        Returns:
            The serialized rewrite results grouped by dataset URI.
        """
        config: MaintenanceConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options

        def run_one(item: tuple[str, int, str]) -> tuple[str, str]:
            """Execute one rewrite task on an executor.

            Args:
                item: The ``(uri, read_version, task_json)`` triple.

            Returns:
                The URI paired with the serialized rewrite result.
            """
            return execute_rewrite_task(item[0], item[1], item[2], storage_options)

        slices: int = max(1, min(config.max_tasks, len(tasks)))
        pairs: list[tuple[str, str]] = spark.sparkContext.parallelize(tasks, slices).map(run_one).collect()
        grouped: dict[str, list[str]] = {}
        for uri, rewrite_json in pairs:
            grouped.setdefault(uri, []).append(rewrite_json)
        return grouped

    def commit_fleet(self, spark: SparkSession, pending: list[tuple[str, list[str]]]) -> list[dict[str, Any]]:
        """Commit every planned dataset's rewrites in a per-dataset executor fan-out (phase C).

        Args:
            spark: Active Spark session.
            pending: ``(uri, rewrite_jsons)`` pairs, one per dataset with executed rewrites.

        Returns:
            One outcome dict per dataset, committed or conflict-marked.
        """
        config: MaintenanceConfig = self.config

        def partition(items: Iterable[tuple[str, list[str]]]) -> Iterator[dict[str, Any]]:
            """Commit the datasets assigned to this executor task.

            Args:
                items: ``(uri, rewrite_jsons)`` pairs for this partition.

            Yields:
                One outcome dict per dataset.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            for uri, rewrite_jsons in items:
                yield commit_one_dataset(uri, rewrite_jsons, config, executor_telemetry)

        slices: int = max(1, min(config.batch_partitions, len(pending)))
        return spark.sparkContext.parallelize(pending, slices).mapPartitions(partition).collect()

    def run(self, spark: SparkSession, dataset_uris: Iterable[str]) -> list[dict[str, Any]]:
        """Maintain every dataset through the unified plan-execute-commit rounds.

        Round structure: phase P fans out per dataset (TTL runs only in the first round),
        phase E runs the whole fleet's rewrite tasks in one flat Spark job, and phase C fans the
        commits out per dataset. Datasets whose commit hit a semantic conflict re-enter the next
        round to be re-planned against the latest version, up to ``replan_budget`` rounds, after
        which they are deferred to the next scheduled run with a ``dataset.hot_skipped`` metric.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to maintain, typically those changed recently.

        Returns:
            One statistics dictionary per dataset, in input order.
        """
        config: MaintenanceConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.maintenance.run") as run_span:
            uris: list[str] = list(dataset_uris)
            run_span.set_tag("dataset_count", len(uris))
            run_span.set_tag("ttl_active", config.ttl_active())
            if not uris:
                return []

            cutoff: datetime | None = compute_cutoff() if config.ttl_active() else None
            results_by_uri: dict[str, dict[str, Any]] = {}
            base_by_uri: dict[str, dict[str, Any]] = {}
            pending_uris: list[str] = uris

            with driver_telemetry.timed("run.maintain_ms"):
                for round_index in range(config.replan_budget):
                    round_cutoff: datetime | None = cutoff if round_index == 0 else None
                    plans: list[dict[str, Any]] = fan_out_per_dataset(
                        spark,
                        pending_uris,
                        config.telemetry,
                        lambda uri, telemetry, cutoff_value=round_cutoff: plan_one_dataset(
                            uri, config, cutoff_value, telemetry
                        ),
                        config.batch_partitions,
                    )
                    planned: list[dict[str, Any]] = []
                    for plan in plans:
                        uri = plan["uri"]
                        if round_index == 0:
                            base_by_uri[uri] = {
                                field: plan[field] for field in ("ttl_rows_deleted", "ttl_skipped") if field in plan
                            }
                        if plan.get("task_jsons"):
                            planned.append(plan)
                        else:
                            results_by_uri[uri] = {**base_by_uri.get(uri, {}), **plan}
                    if not planned:
                        pending_uris = []
                        break

                    flat_tasks: list[tuple[str, int, str]] = [
                        (plan["uri"], plan["read_version"], task_json)
                        for plan in planned
                        for task_json in plan["task_jsons"]
                    ]
                    logger.info(
                        "compaction round %d/%d: %d datasets, %d rewrite tasks",
                        round_index + 1,
                        config.replan_budget,
                        len(planned),
                        len(flat_tasks),
                    )
                    with driver_telemetry.timed("run.rewrite_ms"):
                        rewrites_by_uri: dict[str, list[str]] = self.execute_fleet_tasks(spark, flat_tasks)

                    commit_pairs: list[tuple[str, list[str]]] = [
                        (plan["uri"], rewrites_by_uri.get(plan["uri"], [])) for plan in planned
                    ]
                    outcomes: list[dict[str, Any]] = self.commit_fleet(spark, commit_pairs)
                    conflicted: list[str] = []
                    for outcome in outcomes:
                        uri = outcome["uri"]
                        if outcome.get("conflict"):
                            conflicted.append(uri)
                        else:
                            results_by_uri[uri] = {**base_by_uri.get(uri, {}), **outcome}
                    pending_uris = conflicted
                    if not pending_uris:
                        break

            for uri in pending_uris:
                driver_telemetry.incr("dataset.hot_skipped")
                logger.warning(
                    "skipping compaction of hot dataset %s: commit conflicted in all %d rounds",
                    uri,
                    config.replan_budget,
                )
                results_by_uri[uri] = {
                    **base_by_uri.get(uri, {}),
                    "uri": uri,
                    "bytes_removed": 0,
                    "skipped": f"commit conflicted in all {config.replan_budget} re-plan rounds",
                }

            results: list[dict[str, Any]] = [results_by_uri[uri] for uri in uris]

            if config.ttl_active():
                rows_deleted: int = sum(int(item.get("ttl_rows_deleted", 0)) for item in results)
                datasets_expired: int = sum(1 for item in results if int(item.get("ttl_rows_deleted", 0)) > 0)
                run_span.set_tag("ttl_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.ttl_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.ttl_datasets_expired", datasets_expired)
                logger.info("ttl: %d datasets expired, %d rows deleted", datasets_expired, rows_deleted)

            bytes_removed: int = sum(int(item.get("bytes_removed", 0)) for item in results)
            fragments_removed: int = sum(int(item.get("fragments_removed", 0)) for item in results)
            skipped: int = sum(1 for item in results if item.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)
            driver_telemetry.gauge("run.bytes_removed", bytes_removed)
            driver_telemetry.gauge("run.fragments_removed", fragments_removed)
            logger.info(
                "maintenance run: %d datasets, %d fragments removed, %d bytes reclaimed",
                len(results),
                fragments_removed,
                bytes_removed,
            )
            return results
