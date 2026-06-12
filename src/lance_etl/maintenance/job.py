"""Per-dataset maintenance job: TTL expiration, two-tier compaction, and version cleanup.

Owns :class:`MaintenanceConfig`, :class:`MaintenanceJob`, and all the supporting functions
including :func:`classify_or_compact`, :func:`maintain_one_dataset`, and the new
:func:`compaction_skip_reason` derived-state check.

:func:`classify_or_compact` and :func:`maintain_one_dataset` are defined here (same module) so
existing monkeypatch interception in tests keeps a single defining site.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
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
            default ``0.1`` matches lance's own default. An inline index remap on tier B is
            triggered when covered fragments are rewritten, so budget commit time accordingly for
            heavily indexed head datasets.
        defer_index_remap: Defer index remap on both tiers. The distributed tier passes the
            options to ``Compaction.commit`` and needs a pylance built from the
            ``fix/compaction-commit-options`` lance branch.
        max_source_fragments: Cap on source fragments consumed per run for incremental
            compaction; ``None`` is unbounded, ``0`` is rejected.
        num_threads: Worker threads inside a single rewrite task.
        batch_size: Rows per batch when rewriting.
        max_tasks: Maximum Spark tasks for one dataset's rewrites.
        large_dataset_fragment_threshold: Fragment count above which a dataset uses the
            distributed plan/execute/commit path instead of in-process ``Compaction.execute``.
        large_dataset_row_threshold: Row count at or above which a dataset uses the distributed
            tier even when its fragment count is below ``large_dataset_fragment_threshold``. A
            dataset that is large by rows but has few (large) fragments would otherwise be rewritten
            entirely inside one in-process executor task, which streams but runs single-threaded and
            slow. Routing it to the distributed tier shards the rewrite across executors. ``None``
            disables the row dimension and classifies on fragment count alone. The row count is read
            from fragment metadata (no data scan).
        batch_partitions: Maximum Spark partitions for the small-dataset batch job.
        max_concurrent_large: Driver threads running large-dataset compactions concurrently.
        scheduler_pool: Spark FAIR scheduler pool for large-dataset jobs.
        cleanup_older_than_seconds: Age threshold for version cleanup; default ``172_800`` (2
            days) with the HEAD-tag exemption keeps rollback headroom while cutting manifest
            storage. ``None`` defers to lance's 14-day default. Values below
            ``min_cleanup_horizon_seconds`` are rejected.
        retain_versions: Number of recent versions to retain regardless of age.
        commit_retries: Retry budget for small-tier compaction and TTL delete commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
        large_commit_retries: Retry budget around tier-B ``Compaction.commit``; kept small
            because semantic conflicts re-fail deterministically and only the raw manifest-write
            race benefits from a retry.
        replan_budget: Plan/execute/commit cycles per tier-B dataset before skipping as hot.
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
    large_dataset_fragment_threshold: int = 128
    large_dataset_row_threshold: int | None = 20_000_000
    batch_partitions: int = 512
    max_concurrent_large: int = 4
    scheduler_pool: str = "lance-maintenance"
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
        :attr:`compaction_mode`. The same dict is passed to single-process
        ``Compaction.execute``, to the distributed ``Compaction.plan``, and to the distributed
        ``Compaction.commit`` so commit-time options such as ``defer_index_remap`` take effect
        on both tiers.

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


def compact_small_dataset(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Compact one small dataset entirely inside the current executor task.

    Runs ``Compaction.execute``, which honors every configured option including
    ``defer_index_remap`` and ``max_source_fragments``, then prunes old versions. Commit conflicts
    are retried by re-running the whole compaction against the latest version.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
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

    with telemetry.timed("dataset.total_ms"):
        metrics: dict[str, int] = commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.commit_conflict"),
        )
        bytes_removed: int = cleanup_dataset(uri, config, telemetry)
    telemetry.incr("dataset.compacted")
    result: dict[str, Any] = {"uri": uri, "tier": "small", "tasks": 1, "bytes_removed": bytes_removed}
    result.update(metrics)
    return result


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


def classify_or_compact(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Compact a small dataset in process, or flag a large one for tier B.

    A dataset is routed to the distributed tier when its fragment count exceeds
    ``large_dataset_fragment_threshold`` or, when ``large_dataset_row_threshold`` is set, when its
    row count reaches that threshold. The row dimension catches datasets that are large by rows but
    hold few large fragments, which would otherwise be rewritten single-threaded in one in-process
    executor task. The row count is read from fragment metadata only when the fragment check did not
    already defer the dataset, so the common small-dataset path adds no extra read.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        Small-tier statistics, or ``{"uri", "tier": "large", "fragments"}`` for
        datasets that cross either the fragment or the row threshold.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragments: int = int(dataset.stats.dataset_stats()["num_fragments"])
    large_by_fragments: bool = fragments > config.large_dataset_fragment_threshold
    large_by_rows: bool = (
        not large_by_fragments
        and config.large_dataset_row_threshold is not None
        and dataset.count_rows() >= config.large_dataset_row_threshold
    )
    if large_by_fragments or large_by_rows:
        telemetry.incr("dataset.deferred_to_large_tier")
        return {"uri": uri, "tier": "large", "fragments": fragments}
    return compact_small_dataset(uri, config, telemetry)


def maintain_one_dataset(
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime | None,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run the full per-dataset maintenance pass in one executor task with one initial dataset open.

    Opens the dataset once and runs the TTL delete and classify-or-compact steps in sequence,
    sharing the open handle where possible. A single ``try/except`` around the open call provides
    failure isolation: a missing, corrupt, or unreadable dataset returns a skip dict and never
    aborts the fleet run.

    Steps executed (each conditional on its guard):

    - TTL delete: when ``config.ttl_active()`` is ``True`` and ``cutoff`` is not ``None``, runs
      :func:`run_ttl_on_open_dataset` for schema validation then issues the delete via a
      re-opening retry action.
    - Compaction skip check: after the TTL step (TTL may create compaction work), or right after
      open when TTL is off, :func:`compaction_skip_reason` is consulted. When it returns a reason
      the dataset is skipped: :func:`cleanup_dataset` still runs, the telemetry counter
      ``dataset.skipped_no_work`` is incremented, and a dict with ``skipped`` is returned.
    - Classify and compact: reads the fragment count from the open handle's stats to gate the
      large-tier fast path, then delegates to :func:`classify_or_compact` so that callers patching
      that module-level function in tests continue to intercept the compaction step.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        cutoff: TTL cutoff instant precomputed on the driver, or ``None`` when TTL is not active.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A result dictionary merging TTL and compaction fields. Always contains ``uri``, ``tier``,
        and ``bytes_removed``. A skipped dataset has ``tier="skipped"`` and ``skipped`` holding
        the error message. A dataset with TTL active includes ``ttl_rows_deleted``.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, OSError, ValueError) as exc:
        logger.warning("maintenance: cannot open dataset %s, skipping: %s", uri, exc)
        telemetry.incr("dataset.maintenance_open_error")
        return {"uri": uri, "tier": "skipped", "skipped": str(exc), "bytes_removed": 0}

    result: dict[str, Any] = {"uri": uri}

    if config.ttl_active() and cutoff is not None:
        ttl_result: dict[str, Any] = run_ttl_on_open_dataset(dataset, uri, config, cutoff, telemetry)
        result["ttl_rows_deleted"] = ttl_result.get("ttl_rows_deleted", 0)
        if ttl_result.get("skipped"):
            result["ttl_skipped"] = ttl_result["skipped"]

    skip: str | None = compaction_skip_reason(dataset)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        bytes_removed: int = cleanup_dataset(uri, config, telemetry)
        result.update({"tier": "small", "skipped": skip, "bytes_removed": bytes_removed})
        return result

    compact_result: dict[str, Any] = classify_or_compact(uri, config, telemetry)
    result.update(compact_result)
    return result


class MaintenanceJob:
    """Runs TTL expiration, two-tier compaction, and version cleanup over a Lance fleet."""

    def __init__(self, config: MaintenanceConfig) -> None:
        """Initialize the maintenance job.

        Args:
            config: Maintenance configuration.
        """
        self.config: MaintenanceConfig = config

    def commit_rewrites(self, uri: str, rewrite_jsons: list[str], telemetry: Telemetry) -> dict[str, Any]:
        """Commit serialized rewrites with a deliberately small retry budget.

        The configured compaction options are passed to ``Compaction.commit`` so
        ``defer_index_remap`` is honored at commit time. The ``options`` parameter exists on
        pylance builds carrying the ``fix/compaction-commit-options`` lance patch. Older bindings
        reject the keyword with a ``TypeError``, so the commit falls back to the bare two-argument
        call, which is behaviorally identical except that ``defer_index_remap`` is silently
        impossible: the old binding always remaps covering indices inline. The fallback therefore
        logs a warning and emits ``dataset.commit_options_unsupported`` when ``defer_index_remap``
        was requested, so an operator can see the setting is not taking effect. Remove the
        fallback once every deployment runs the patched pylance.

        Retrying the commit cannot resolve a semantic conflict: the conflict scan is pinned to the
        plan version, so the same conflicting transaction is found on every attempt. The small
        ``large_commit_retries`` budget only covers the raw manifest-write race. Semantic
        conflicts escape to :meth:`compact_one`, whose re-plan loop is the productive retry.

        Args:
            uri: Dataset URI.
            rewrite_jsons: Serialized rewrite results from the executors.
            telemetry: Telemetry facade.

        Returns:
            A metrics dictionary for the committed compaction.

        Raises:
            OSError | RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: MaintenanceConfig = self.config
        rewrites: list[RewriteResult] = [RewriteResult.from_json(j) for j in rewrite_jsons]

        def action() -> dict[str, int]:
            """Commit the rewrites against the latest version, falling back on old bindings.

            A ``TypeError`` naming the ``options`` keyword means the installed pylance predates
            the ``fix/compaction-commit-options`` patch. Any other ``TypeError`` propagates.

            Returns:
                The compaction metrics for this commit.
            """
            dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            try:
                metrics: CompactionMetrics = Compaction.commit(dataset, rewrites, options=config.execute_options())
            except TypeError as exc:
                if "options" not in str(exc):
                    raise
                if config.defer_index_remap:
                    telemetry.incr("dataset.commit_options_unsupported")
                    logger.warning(
                        "installed pylance does not accept Compaction.commit(options=...): "
                        "defer_index_remap is NOT taking effect on the distributed tier for %s. "
                        "Rebuild pylance from the fix/compaction-commit-options lance branch.",
                        uri,
                    )
                metrics = Compaction.commit(dataset, rewrites)
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
        config: MaintenanceConfig = self.config
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

        The driver only plans and commits. Rewrite I/O runs on executors. With
        ``defer_index_remap`` off, index remap happens inline during the driver commit, so commit
        duration grows with the number and size of indices covering rewritten fragments. With it
        on, the commit records a frag-reuse index instead and stays cheap. With
        ``max_source_fragments`` set, each run consumes a bounded slice of the oldest fragments
        for incremental compaction. Spark jobs submitted from the calling thread are pinned to the
        configured FAIR scheduler pool.

        A commit conflict means the rewrite results are stale, so the loop re-plans and
        re-executes against the latest version instead of re-committing, which would re-fail
        deterministically. After ``replan_budget`` conflicting cycles the dataset is skipped for
        this run with a hot-dataset metric and deferred to the next cycle.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset with ``tier`` set to ``"large"``. Skipped hot
            datasets carry a ``"skipped"`` reason instead of commit metrics.
        """
        config: MaintenanceConfig = self.config
        try:
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", config.scheduler_pool)
            tasks_attempted: int = 0
            for cycle in range(1, config.replan_budget + 1):
                dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
                plan = Compaction.plan(dataset, options=config.execute_options())
                task_jsons: list[str] = [task.json() for task in plan.tasks]
                if not task_jsons:
                    bytes_removed: int = cleanup_dataset(uri, config, telemetry)
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
                    telemetry.incr("dataset.replanned")
                    logger.warning(
                        "compaction commit conflicted for %s (cycle %d/%d); re-planning at the latest version",
                        uri,
                        cycle,
                        config.replan_budget,
                    )
                    continue

                bytes_removed = cleanup_dataset(uri, config, telemetry)
                telemetry.incr("dataset.compacted")
                result: dict[str, Any] = {
                    "uri": uri,
                    "tier": "large",
                    "tasks": tasks_attempted,
                    "bytes_removed": bytes_removed,
                }
                result.update(metrics)
                return result

            telemetry.incr("dataset.hot_skipped")
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

        Each worker thread runs one dataset's plan/execute/commit cycle and pins its Spark jobs to
        the FAIR scheduler pool, so several large datasets share the cluster instead of queueing
        FIFO. All datasets are attempted. The first failure is re-raised after the pool drains.

        Args:
            spark: Active Spark session.
            uris: Large-dataset URIs from the classification pass.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset.

        Raises:
            Exception: The first per-dataset failure, after all datasets finish.
        """
        config: MaintenanceConfig = self.config
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
                    telemetry.error(f"compaction failed for {uri}")
                    failures.append(exc)
        if failures:
            raise failures[0]
        return results

    def run(self, spark: SparkSession, dataset_uris: Iterable[str]) -> list[dict[str, Any]]:
        """Maintain every dataset in one consolidated fan-out pass, then compact large datasets from the driver.

        The run is ordered: TTL expiration (when configured) runs first inside the per-dataset
        executor task, followed by two-tier compaction, followed by version cleanup. A single
        :func:`maintain_one_dataset` executor function opens each dataset once and runs the TTL
        delete and classify-or-compact steps in sequence, sharing the open handle. The TTL cutoff
        is computed once on the driver before the fan-out and shared across all executors so every
        dataset decides expiry against the same clock. Large-tier datasets returned by the fan-out
        are then compacted with the distributed plan/execute/commit triad, several at a time from
        a driver thread pool on the FAIR scheduler pool.

        Run-level aggregates extracted from the outcome dicts include TTL rows deleted and datasets
        expired, bytes and fragments reclaimed, and the standard dataset/tier counts.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to maintain, typically those changed recently.

        Returns:
            One statistics dictionary per dataset.
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

            with driver_telemetry.timed("run.maintain_ms"):
                outcomes: list[dict[str, Any]] = fan_out_per_dataset(
                    spark,
                    uris,
                    config.telemetry,
                    lambda uri, telemetry: maintain_one_dataset(uri, config, cutoff, telemetry),
                    config.batch_partitions,
                )

            if config.ttl_active():
                rows_deleted: int = sum(int(item.get("ttl_rows_deleted", 0)) for item in outcomes)
                datasets_expired: int = sum(1 for item in outcomes if int(item.get("ttl_rows_deleted", 0)) > 0)
                run_span.set_tag("ttl_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.ttl_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.ttl_datasets_expired", datasets_expired)
                logger.info("ttl: %d datasets expired, %d rows deleted", datasets_expired, rows_deleted)

            results: list[dict[str, Any]] = [item for item in outcomes if item.get("tier") == "small"]
            large_uris: list[str] = [item["uri"] for item in outcomes if item.get("tier") == "large"]
            run_span.set_tag("small_datasets", len(results))
            run_span.set_tag("large_datasets", len(large_uris))
            logger.info("small tier compacted %d datasets; %d deferred to large tier", len(results), len(large_uris))

            if large_uris:
                with driver_telemetry.timed("run.large_tier_ms"):
                    results.extend(self.compact_large_tier(spark, large_uris, driver_telemetry))

            bytes_removed: int = sum(int(item.get("bytes_removed", 0)) for item in results)
            fragments_removed: int = sum(int(item.get("fragments_removed", 0)) for item in results)
            skipped: int = sum(1 for item in outcomes if item.get("skipped"))
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
