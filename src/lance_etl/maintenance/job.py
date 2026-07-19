"""Fleet maintenance job: retention expiry, unified task-based compaction, and version cleanup.

Owns :class:`MaintenanceConfig`, :class:`MaintenanceJob`, and the three phase functions of the
unified compaction flow. Every dataset, regardless of size, follows the same
plan-execute-commit path built on the same Lance APIs:

- Phase P (:func:`plan_one_dataset`): a per-dataset executor fan-out runs the retention delete, the
  derived-state skip check, and ``Compaction.plan``, returning serialized rewrite tasks. A
  small dataset yields one task and a large one yields many.
- Phase E (:meth:`MaintenanceJob.execute_fleet_tasks`): every dataset's rewrite tasks run in
  ONE flat Spark job, so Spark schedules the whole fleet's work instead of driver thread pools.
- Phase C (:func:`commit_one_dataset`): a per-dataset executor fan-out commits the collected
  rewrites and prunes old versions. A commit conflict marks the dataset for the next replan
  round instead of retrying stale rewrites.

:meth:`MaintenanceJob.run` repeats the three phases for conflicted datasets up to
:data:`REPLAN_BUDGET` rounds, then defers the survivors to the next scheduled run.

Before phase P, an opt-in clustered-rewrite pass (:attr:`MaintenanceConfig.cluster_rewrite`,
:func:`~lance_etl.maintenance.cluster.run_cluster_rewrites`) reorders eligible datasets so
same-centroid rows share fragments (ADR 0041). A clustered or cluster-errored dataset never
enters the plan-execute-commit rounds; only cluster-ineligible datasets pass through into normal
compaction.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import lance
from lance.optimize import Compaction, CompactionMetrics, CompactionTask, RewriteResult
from pyspark.sql import SparkSession

import lance_etl.maintenance.cluster as maintenance_cluster
from lance_etl.fanout import (
    FANOUT_PARTITION_FACTOR,
    FLAT_ERROR,
    FLAT_OK,
    REWRITE_PARTITION_FACTOR,
    derive_partitions,
    fan_out_per_dataset,
    report_fleet_failures,
    run_flat_tagged_job,
)
from lance_etl.telemetry import (
    DEFAULT_COMMIT_RETRIES,
    DEFAULT_LARGE_COMMIT_RETRIES,
    Telemetry,
    TelemetryConfig,
    commit_with_retries,
    is_commit_conflict_error,
)

logger: logging.Logger = logging.getLogger(__name__)

MATERIALIZE_DELETIONS_THRESHOLD: float = 0.1
"""Deletion fraction above which ``Compaction.plan`` rewrites a fragment; matches lance's own
default. An inline index remap is triggered when covered fragments are rewritten, so budget commit
time accordingly for heavily indexed head datasets."""

COMPACTION_MODE: str = "try_binary_copy"
"""Lance compaction mode for ``Compaction.execute`` and ``Compaction.plan``; falls back to
reencode per task, never errors on deletion-bearing fragments unlike ``force_binary_copy``."""

REPLAN_BUDGET: int = 3
"""Plan/execute/commit rounds a conflicted dataset participates in before it is skipped as hot and
deferred to the next scheduled run."""

MIN_CLEANUP_HORIZON_SECONDS: int = 6 * 3600
"""Floor for ``cleanup_older_than_seconds``; cleanup is not transactional, so the floor must exceed
the longest concurrent job to protect in-flight committer rebase files."""


@dataclass
class MaintenanceConfig:
    """Configuration for :class:`MaintenanceJob`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        retention_seconds: Retention window in seconds derived from the spec revision policy.
            ``None`` (default) turns record expiry off. When set, rows where
            ``ts_column < now - retention_seconds`` are deleted before compaction.
        ts_column: The ``ts`` column used as the retention clock; must match ``ETLConfig.ts_col``.
        target_rows_per_fragment: Desired rows per compacted fragment; matches lance's
            ``CompactionOptions`` default of ``1_048_576`` so the default is explicit and immune
            to upstream shifts.
        materialize_deletions: Whether compaction physically removes deleted rows.
        materialize_deletions_threshold: Deleted-row fraction that makes a fragment eligible.
        compaction_mode: Lance rewrite strategy, normally ``try_binary_copy`` or ``reencode``.
        defer_index_remap: Defer index remap at commit time through the options passed to
            ``Compaction.commit``.
        max_source_fragments: Cap on source fragments consumed per run for incremental
            compaction; ``None`` is unbounded, ``0`` is rejected.
        num_threads: Worker threads inside a single rewrite task.
        cleanup_older_than_seconds: Age threshold for version cleanup; default ``216_000`` (60
            hours) with the HEAD-tag exemption keeps rollback headroom while cutting manifest
            storage. The 60-hour default deliberately exceeds the pipeline's interval-tag
            retention window (``tag_keep_last`` hourly tags plus slack, ADR 0013) so a run that
            prunes the oldest interval tag never also reclaims the version that tag pinned while a
            replica mid-scan still holds it. ``None`` defers to lance's 14-day default. Values below
            :data:`MIN_CLEANUP_HORIZON_SECONDS` are rejected.
        retain_versions: Number of recent versions to retain regardless of age.
        commit_retries: Retry budget for retention delete commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
        large_commit_retries: Retry budget around ``Compaction.commit``; kept small because
            semantic conflicts re-fail deterministically and only the raw manifest-write race
            benefits from a retry.
        cleanup_rotation_slots: Idle datasets are cleaned once per this many maintenance runs
            via a wall-clock rotation slot. 1 cleans every run.
        cleanup_rotation_cadence_hours: Run cadence in hours used to derive the current rotation
            slot from wall-clock time.
        cluster_rewrite: Opt-in clustered full rewrite; rewritten datasets skip normal compaction
            this run.
        cluster_column: Explicit vector column to cluster on; ``None`` auto-selects the single
            vector-role column from the dataset's stored column roles.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    retention_seconds: int | None = None
    ts_column: str = "ts"
    target_rows_per_fragment: int = 1_048_576
    materialize_deletions: bool = True
    materialize_deletions_threshold: float = MATERIALIZE_DELETIONS_THRESHOLD
    compaction_mode: str = COMPACTION_MODE
    defer_index_remap: bool = False
    max_source_fragments: int | None = 256
    num_threads: int | None = None
    cleanup_older_than_seconds: int | None = 216_000
    retain_versions: int | None = None
    commit_retries: int = DEFAULT_COMMIT_RETRIES
    commit_backoff_seconds: float = 0.5
    large_commit_retries: int = DEFAULT_LARGE_COMMIT_RETRIES
    cleanup_rotation_slots: int = 8
    cleanup_rotation_cadence_hours: int = 1
    cluster_rewrite: bool = False
    cluster_column: str | None = None

    def retention_active(self) -> bool:
        """Report whether the record-retention step runs for this configuration.

        Returns:
            ``True`` when a retention window is configured, ``False`` otherwise (the default
            no-op).
        """
        return self.retention_seconds is not None

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
            "materialize_deletions": self.materialize_deletions,
            "materialize_deletions_threshold": self.materialize_deletions_threshold,
            "max_source_fragments": self.max_source_fragments,
            "num_threads": self.num_threads,
            "compaction_mode": self.compaction_mode,
        }
        if self.defer_index_remap:
            candidates["defer_index_remap"] = True
        return {name: value for name, value in candidates.items() if value is not None}


def validate_column_name(column: str, schema: Any) -> None:
    """Validate that a retention predicate column exists in the dataset schema.

    Catches silent misconfigurations where the column name was changed but the config was not
    updated, before any predicate is constructed.

    Args:
        column: The column name from :attr:`MaintenanceConfig.ts_column`.
        schema: The pyarrow schema of the target dataset.

    Raises:
        KeyError: If the column is not present in the dataset schema.
    """
    column_names: list[str] = schema.names
    if column not in column_names:
        raise KeyError(f"column {column!r} is not present in the dataset schema. Available columns: {column_names}")


def build_retention_predicate(ts_column: str, cutoff: datetime) -> str:
    """Build the Lance SQL delete predicate for retention-window expiration.

    The predicate is
    ``arrow_cast({ts_column}, 'Timestamp(Microsecond, "UTC")') <
    arrow_cast('{iso_cutoff}', 'Timestamp(Microsecond, "UTC")')`` which deletes every row whose
    ``ts`` is strictly before the retention cutoff instant. The column name has already been
    validated against the dataset schema by :func:`validate_column_name` before this function is
    called.

    Both sides of the comparison are cast to ``Timestamp(Microsecond, "UTC")`` so the deletion
    decision is a direct UTC-instant comparison with no timezone-naive operand and no implicit
    coercion. A bare ``TIMESTAMP '...'`` literal is always parsed timezone-naive (Lance's SQL
    layer rejects a timezone in the type itself), which would leave the reconciliation against a
    timezone-aware ``ts_column`` to implicit coercion and risk a silent offset. Casting the
    cutoff literal to explicit UTC as well removes that ambiguity entirely. The cutoff is
    formatted in UTC with microsecond resolution as ``YYYY-MM-DDTHH:MM:SS.ffffff``, which is a
    UTC wall-clock instant because :attr:`cutoff` is UTC, so attaching UTC on the cast is correct.

    Args:
        ts_column: The validated ``ts`` column name.
        cutoff: The UTC cutoff instant. Rows whose ``ts`` is strictly before this are expired.

    Returns:
        A Lance SQL predicate string safe for passing to :meth:`lance.LanceDataset.delete`.
    """
    literal: str = cutoff.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")
    utc_ts: str = f"arrow_cast({ts_column}, 'Timestamp(Microsecond, \"UTC\")')"
    utc_cutoff: str = f"arrow_cast('{literal}', 'Timestamp(Microsecond, \"UTC\")')"
    return f"{utc_ts} < {utc_cutoff}"


def compute_cutoff(retention_seconds: int) -> datetime:
    """Compute the retention cutoff as ``now - retention_seconds`` in UTC.

    Every row whose ``ts`` is strictly before this instant is expired, so the retention window is
    applied uniformly against a single wall-clock cutoff.

    Args:
        retention_seconds: Retention window in seconds from the spec revision policy.

    Returns:
        The UTC instant marking the oldest ``ts`` that survives.
    """
    return datetime.now(tz=UTC) - timedelta(seconds=retention_seconds)


def dataset_cleanup_slot(uri: str, slots: int) -> int:
    """Map a dataset URI to a deterministic rotation slot in ``range(slots)``.

    Uses ``hashlib.sha256`` rather than the builtin ``hash()`` because the builtin is salted
    per interpreter process (``PYTHONHASHSEED`` randomization by default), so the same URI would
    hash to different slots on the driver and on each executor, and even to a different slot on
    the same process across runs. That would break both determinism (a dataset must land in the
    same slot every time it is evaluated) and full-coverage (every dataset must eventually be
    cleaned as the active slot rotates through ``range(slots)``). SHA-256 is stable across
    processes and versions, which rotation correctness depends on.

    Args:
        uri: Dataset URI.
        slots: Total number of rotation slots.

    Returns:
        The dataset's fixed slot index, in ``range(slots)``.
    """
    digest: bytes = hashlib.sha256(uri.encode()).digest()[:8]
    return int.from_bytes(digest, "big") % slots


def active_cleanup_slot(config: MaintenanceConfig, now: datetime) -> int:
    """Derive the rotation slot that is active for cleanup at the given instant.

    The active slot advances every ``cleanup_rotation_cadence_hours`` hours and cycles through
    ``range(cleanup_rotation_slots)``, so over ``cleanup_rotation_slots`` consecutive runs (at
    the configured cadence) every rotation slot becomes active exactly once.

    Args:
        config: Maintenance configuration carrying the rotation size and cadence.
        now: The current instant, passed explicitly so tests can pin it.

    Returns:
        The active rotation slot index, in ``range(config.cleanup_rotation_slots)``.
    """
    hours: int = int(now.timestamp()) // 3600 // config.cleanup_rotation_cadence_hours
    return hours % config.cleanup_rotation_slots


def should_clean_idle(uri: str, config: MaintenanceConfig, cleanup_slot: int | None) -> bool:
    """Decide whether an idle dataset should be cleaned on this run.

    ``cleanup_slot=None`` is the sentinel a direct caller (a unit test, or any future caller not
    participating in fleet-wide rotation) uses to mean "always clean," which preserves the
    pre-rotation behavior for callers that do not thread a rotation slot through. Rotation is
    also bypassed when ``cleanup_rotation_slots <= 1``, since a single slot degenerates to
    cleaning every run. Otherwise the dataset is cleaned only when its own deterministic slot
    matches the currently active one.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration carrying the rotation size.
        cleanup_slot: The active rotation slot for this run, or ``None`` to always clean.

    Returns:
        ``True`` when this idle dataset should be cleaned this run.
    """
    if cleanup_slot is None:
        return True
    if config.cleanup_rotation_slots <= 1:
        return True
    return dataset_cleanup_slot(uri, config.cleanup_rotation_slots) == cleanup_slot


def run_retention_on_open_dataset(
    dataset: lance.LanceDataset,
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Run the retention delete step against an already-open dataset handle for schema validation.

    Validates the ``ts`` column name against the schema of the supplied open handle, then delegates
    the actual delete to a re-opening retry action so each commit attempt operates against the
    latest version (required for rebase correctness). A dataset that lacks the ``ts`` column is
    skipped with a warning and a metric rather than failing the task.

    Args:
        dataset: An already-open Lance dataset handle used only for schema validation.
        uri: Dataset URI matching the open handle.
        config: Maintenance configuration with ``retention_seconds`` set.
        cutoff: The precomputed cutoff instant shared across the run.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A result dictionary with keys ``uri``, ``retention_rows_deleted``, and ``skipped``.
    """
    try:
        validate_column_name(config.ts_column, dataset.schema)
    except KeyError as exc:
        logger.warning("retention: ts column missing in %s, skipping expiration: %s", uri, exc)
        telemetry.incr("dataset.retention_column_missing")
        return {"uri": uri, "retention_rows_deleted": 0, "skipped": str(exc)}

    predicate: str = build_retention_predicate(config.ts_column, cutoff)

    def action() -> int:
        """Re-open the dataset and execute the delete on the latest version.

        Returns:
            The number of rows deleted.
        """
        fresh: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        delete_result: dict[str, Any] = fresh.delete(predicate, conflict_retries=config.commit_retries)
        return int(delete_result.get("num_deleted_rows", 0))

    with telemetry.timed("dataset.retention_delete_ms"):
        rows_deleted: int = commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("dataset.retention_commit_conflict"),
        )
    telemetry.distribution("dataset.retention_rows_deleted", float(rows_deleted))
    if rows_deleted:
        telemetry.incr("dataset.retention_expired")
    return {"uri": uri, "retention_rows_deleted": rows_deleted, "skipped": ""}


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


def validate_cleanup_horizon(config: MaintenanceConfig) -> None:
    """Reject cleanup horizons that could race with a concurrent job.

    Args:
        config: Maintenance configuration carrying the cleanup horizon.

    Raises:
        ValueError: If ``cleanup_older_than_seconds`` is set below
            :data:`MIN_CLEANUP_HORIZON_SECONDS`.
    """
    cleanup_horizon: int | None = config.cleanup_older_than_seconds
    if cleanup_horizon is not None and cleanup_horizon < MIN_CLEANUP_HORIZON_SECONDS:
        raise ValueError(
            f"cleanup_older_than_seconds={cleanup_horizon} is below the safe floor of "
            f"{MIN_CLEANUP_HORIZON_SECONDS}; cleanup horizons must exceed the longest concurrent job"
        )


def cleanup_dataset(
    uri: str, config: MaintenanceConfig, telemetry: Telemetry, dataset: lance.LanceDataset | None = None
) -> int:
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
        dataset: An already-open handle to reuse instead of re-opening the URI. Pass only a
            handle known to be at least as fresh as the last commit this task made — the
            post-compaction prune in :func:`commit_one_dataset` deliberately re-opens so cleanup
            sees the version its commit just superseded.

    Returns:
        The number of bytes reclaimed.

    Raises:
        ValueError: If ``cleanup_older_than_seconds`` is set below
            :data:`MIN_CLEANUP_HORIZON_SECONDS`. The horizon must exceed the
            longest-running concurrent job so its rebase can still read old transaction files.
    """
    validate_cleanup_horizon(config)
    older_than: timedelta | None = (
        timedelta(seconds=config.cleanup_older_than_seconds) if config.cleanup_older_than_seconds is not None else None
    )
    if dataset is None:
        dataset = lance.dataset(uri, storage_options=config.storage_options)
    with telemetry.timed("dataset.cleanup_ms"):
        stats: Any = dataset.cleanup_old_versions(
            older_than=older_than,
            retain_versions=config.retain_versions,
            error_if_tagged_old_versions=False,
        )
    telemetry.distribution("dataset.bytes_removed", stats.bytes_removed)
    telemetry.distribution("dataset.old_versions_removed", stats.old_versions)
    telemetry.incr("dataset.cleaned")
    return int(stats.bytes_removed)


def cleanup_hot_dataset(uri: str, config: MaintenanceConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Clean versions created by same-run retention work after compaction conflicts exhaust.

    The cleanup opens the latest dataset version on its executor. It does not reuse any handle
    from a conflicted compaction plan and does not attempt to commit those stale rewrites.

    Args:
        uri: Dataset URI whose compaction was deferred as hot.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A result carrying the dataset URI and reclaimed byte count.
    """
    bytes_removed: int = cleanup_dataset(uri, config, telemetry)
    return {"uri": uri, "bytes_removed": bytes_removed}


def compaction_skip_reason(dataset: lance.LanceDataset) -> str | None:
    """Return a reason string when the dataset provably needs no compaction planning.

    This is a conservative derived-state check using only the already-open dataset handle.
    A dataset with more than one fragment always needs the real planner, since Lance's own
    ``Compaction.plan`` decides fragment grouping and small-file merging. A dataset with at most
    one fragment has nothing to compact ONLY when it also carries no soft-deletions: Lance's
    planner marks a single fragment as a genuine ``CompactItself`` candidate once its deletion
    fraction exceeds :data:`MATERIALIZE_DELETIONS_THRESHOLD`, so a lone fragment
    accumulating retention or merge-insert deletions must still reach ``Compaction.plan`` or its
    reclaimable space never gets recovered. The check reads ``dataset.stats.dataset_stats()``,
    which returns ``num_fragments`` and ``num_deleted_rows`` from the in-memory manifest (the
    latter via ``count_deleted_rows()`` over already-loaded deletion-file metadata), so consuming
    both costs no additional object-store I/O over the previous fragment-count-only check.

    The single-fragment deletion-fraction ratio is intentionally NOT reimplemented here: that
    threshold comparison stays inside ``Compaction.plan`` so this function never drifts from
    Lance's own candidacy logic. When a single fragment carries any deletions, this function
    returns ``None`` and defers to the planner; a below-threshold fragment simply yields an empty
    plan that falls through the existing zero-task cleanup path in :func:`plan_one_dataset`.

    Args:
        dataset: The already-open dataset handle.

    Returns:
        A human-readable skip reason when compaction is unnecessary, or ``None`` when the
        planner must decide.
    """
    stats: dict[str, Any] = dataset.stats.dataset_stats()
    num_fragments: int = int(stats["num_fragments"])
    num_deleted_rows: int = int(stats["num_deleted_rows"])
    if num_fragments > 1:
        return None
    if num_deleted_rows == 0:
        return f"only {num_fragments} fragment(s); nothing to compact"
    return None


def idle_cleanup_bytes(
    uri: str,
    config: MaintenanceConfig,
    telemetry: Telemetry,
    dataset: lance.LanceDataset,
    did_work: bool,
    cleanup_slot: int | None,
) -> int:
    """Clean an idle dataset's old versions unless the rotation defers it to a later run.

    A dataset that did real work this run (``did_work``, currently a retention delete) always cleans
    regardless of rotation, because its own commit just created reclaimable versions. Otherwise
    the deterministic per-dataset rotation slot from :func:`should_clean_idle` decides, and a
    deferred dataset increments ``dataset.cleanup_rotation_skipped`` so the savings are directly
    observable.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        telemetry: Telemetry facade for the current executor process.
        dataset: The already-open dataset handle to reuse for cleanup.
        did_work: Whether this dataset had rows deleted (retention) during this run.
        cleanup_slot: The active rotation slot for this run, or ``None`` to always clean.

    Returns:
        The number of bytes reclaimed, or ``0`` when rotation deferred this dataset.
    """
    if did_work or should_clean_idle(uri, config, cleanup_slot):
        return cleanup_dataset(uri, config, telemetry, dataset)
    telemetry.incr("dataset.cleanup_rotation_skipped")
    return 0


def plan_one_dataset(
    uri: str,
    config: MaintenanceConfig,
    cutoff: datetime | None,
    telemetry: Telemetry,
    cleanup_slot: int | None = None,
) -> dict[str, Any]:
    """Run phase P for one dataset on an executor: retention delete, skip check, and compaction plan.

    Opens the dataset once and reuses that handle for the skip check, the early-exit cleanup, and
    ``Compaction.plan`` — refreshing it exactly once only when the retention step committed a delete
    (``ttl_rows_deleted > 0``), so the plan sees the rows it must materialize. An idle tiny
    dataset therefore costs one object-store open per maintenance run instead of two, and an
    active one costs one instead of three, which is what keeps a mostly-idle million-dataset
    fleet affordable. Any further staleness against concurrent writers is pre-existing and
    absorbed by the stale-plan conflict replan in the commit phase.

    The two early-exit cleanup calls (the derived-state skip path and the empty-plan path) are
    gated by :func:`idle_cleanup_bytes`: a dataset that retention-deleted rows this run
    (``did_work``) is always cleaned, and every other idle dataset is cleaned only once per
    ``cleanup_rotation_slots`` runs via its deterministic rotation slot, which is what removes the
    per-run object-store LIST cost for a fleet of mostly-idle datasets. ``cleanup_slot=None`` (the
    default) always cleans, preserving the exact pre-rotation behavior for direct callers such as
    unit tests that do not thread a fleet-wide rotation slot.

    Failure isolation: a missing, corrupt, or unreadable dataset returns a skip dict and never
    aborts the fleet run. When retention is active and a cutoff is supplied, expired rows are deleted
    first because the delete creates compaction work. The derived-state
    :func:`compaction_skip_reason` check and an empty ``Compaction.plan`` both end the dataset's
    run early with a cleanup pass. Otherwise the plan's rewrite tasks are serialized for the
    fleet-wide execute phase.

    The same function serves every dataset size: a small dataset yields one rewrite task and a
    large one yields many, so no separate in-process compaction path exists.

    Args:
        uri: Dataset URI.
        config: Maintenance configuration.
        cutoff: retention cutoff instant, or ``None`` to skip the retention step (replan rounds pass None so
            retention runs exactly once per fleet run).
        telemetry: Telemetry facade for the current executor process.
        cleanup_slot: The active fleet-wide rotation slot for this run, or ``None`` to always
            clean idle datasets (the pre-rotation behavior direct callers rely on).

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

    if config.retention_active() and cutoff is not None:
        retention_result: dict[str, Any] = run_retention_on_open_dataset(dataset, uri, config, cutoff, telemetry)
        result["retention_rows_deleted"] = retention_result.get("retention_rows_deleted", 0)
        if retention_result.get("skipped"):
            result["retention_skipped"] = retention_result["skipped"]
        if int(result["retention_rows_deleted"]) > 0:
            dataset = lance.dataset(uri, storage_options=config.storage_options)

    did_work: bool = int(result.get("retention_rows_deleted", 0)) > 0

    skip: str | None = compaction_skip_reason(dataset)
    if skip is not None:
        telemetry.incr("dataset.skipped_no_work")
        bytes_removed: int = idle_cleanup_bytes(uri, config, telemetry, dataset, did_work, cleanup_slot)
        result.update({"skipped": skip, "tasks": 0, "bytes_removed": bytes_removed})
        return result

    plan: Any = Compaction.plan(dataset, options=config.execute_options())
    task_jsons: list[str] = [task.json() for task in plan.tasks]
    if not task_jsons:
        bytes_removed = idle_cleanup_bytes(uri, config, telemetry, dataset, did_work, cleanup_slot)
        result.update({"tasks": 0, "fragments_removed": 0, "bytes_removed": bytes_removed})
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
    """Runs retention expiry, unified task-based compaction, and version cleanup over a Lance fleet.

    Also runs the opt-in clustered rewrite (:attr:`MaintenanceConfig.cluster_rewrite`) ahead of
    the plan-execute-commit rounds, subsuming normal compaction for the datasets it rewrites.
    """

    def __init__(self, config: MaintenanceConfig) -> None:
        """Initialize the maintenance job.

        Args:
            config: Maintenance configuration.
        """
        self.config: MaintenanceConfig = config

    def execute_fleet_tasks(
        self, spark: SparkSession, tasks: list[tuple[str, int, str]]
    ) -> tuple[dict[str, list[str]], dict[str, str]]:
        """Run every dataset's rewrite tasks in one flat Spark job (phase E).

        All datasets' tasks share one job, so Spark schedules the fleet's rewrite work across
        the cluster: a large dataset contributes many tasks and a small one contributes one,
        with no per-dataset job submission or driver thread pool.

        Per-dataset failure isolation: each task is executed inside a try/except so a single
        dataset's rewrite failure never aborts the flat job. A failing task is tagged
        ``(FLAT_ERROR, uri, message)`` and the first error per URI is recorded, while successful
        tasks are tagged ``(FLAT_OK, uri, rewrite_json)`` and grouped by URI. The caller excludes
        any URI carrying an error from the commit phase, so a dataset whose rewrite partially
        failed is never committed.

        Args:
            spark: Active Spark session.
            tasks: ``(uri, read_version, task_json)`` triples flattened across the fleet.

        Returns:
            A ``(rewrites_by_uri, errors_by_uri)`` pair: the serialized rewrite results grouped
            by dataset URI, and the first error message per dataset URI whose rewrite failed.
        """
        config: MaintenanceConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options

        def run_one(item: tuple[str, int, str]) -> tuple[str, str, str]:
            """Execute one rewrite task on an executor, tagging success or failure.

            Args:
                item: The ``(uri, read_version, task_json)`` triple.

            Returns:
                ``(FLAT_OK, uri, rewrite_json)`` on success or ``(FLAT_ERROR, uri, message)`` when
                the rewrite raised, so the driver can isolate the failing dataset.
            """
            try:
                uri: Any
                rewrite_json: Any
                uri, rewrite_json = execute_rewrite_task(item[0], item[1], item[2], storage_options)
                return FLAT_OK, uri, rewrite_json
            except Exception as exc:
                return FLAT_ERROR, item[0], str(exc)

        return run_flat_tagged_job(spark, tasks, run_one, derive_partitions(spark, REWRITE_PARTITION_FACTOR))

    def commit_fleet(self, spark: SparkSession, pending: list[tuple[str, list[str]]]) -> list[dict[str, Any]]:
        """Commit every planned dataset's rewrites in a per-dataset executor fan-out (phase C).

        Args:
            spark: Active Spark session.
            pending: ``(uri, rewrite_jsons)`` pairs, one per dataset with executed rewrites.

        Returns:
            One outcome dict per dataset, committed, conflict-marked, or error-marked.
        """
        config: MaintenanceConfig = self.config

        def partition(items: Iterable[tuple[str, list[str]]]) -> Iterator[dict[str, Any]]:
            """Commit the datasets assigned to this executor task.

            A non-conflict commit failure is isolated into an error marker instead of aborting
            the fan-out. ``commit_one_dataset`` returns its own conflict marker and only raises on
            a genuine non-conflict error, so any exception reaching here is terminal for that
            dataset and never re-planned.

            Args:
                items: ``(uri, rewrite_jsons)`` pairs for this partition.

            Yields:
                One outcome dict per dataset, an error marker when the commit raised.
            """
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            uri: Any
            rewrite_jsons: Any
            for uri, rewrite_jsons in items:
                try:
                    yield commit_one_dataset(uri, rewrite_jsons, config, executor_telemetry)
                except Exception as exc:
                    executor_telemetry.incr("dataset.commit_error")
                    logger.warning("compaction commit failed for %s, isolating: %s", uri, exc)
                    yield {"uri": uri, "error": str(exc), "phase": "commit", "bytes_removed": 0}

        batch_partitions_resolved: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        slices: int = max(1, min(batch_partitions_resolved, len(pending)))
        return spark.sparkContext.parallelize(pending, slices).mapPartitions(partition).collect()

    def run_round(
        self,
        spark: SparkSession,
        round_index: int,
        pending_uris: list[str],
        cutoff: datetime | None,
        base_by_uri: dict[str, dict[str, Any]],
        results_by_uri: dict[str, dict[str, Any]],
        driver_telemetry: Telemetry,
        cleanup_slot: int,
    ) -> list[str]:
        """Run one plan-execute-commit round over the pending datasets.

        Phase P fans out per dataset (retention runs only in the first round, so ``cutoff`` is dropped
        after it), phase E runs the round's rewrite tasks in one flat Spark job, and phase C fans
        the commits out per dataset. Terminal outcomes land in ``results_by_uri`` and the first
        round's retention fields are kept in ``base_by_uri`` so later rounds merge onto them.

        Args:
            spark: Active Spark session.
            round_index: Zero-based round number, for logging and the retention first-round gate.
            pending_uris: Datasets to plan and compact this round.
            cutoff: The retention cutoff, applied only when ``round_index`` is zero.
            base_by_uri: First-round retention fields per dataset, populated in round zero.
            results_by_uri: Per-dataset terminal outcomes, mutated in place.
            driver_telemetry: The driver's telemetry facade.
            cleanup_slot: The fleet-wide rotation slot active for this run, threaded into
                ``plan_one_dataset`` so idle datasets outside this slot skip version cleanup.

        Returns:
            The datasets whose commit hit a semantic conflict, for the next round's re-plan.
        """
        config: MaintenanceConfig = self.config
        round_cutoff: datetime | None = cutoff if round_index == 0 else None
        plan_batch_partitions: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
        plans: list[dict[str, Any]] = fan_out_per_dataset(
            spark,
            pending_uris,
            config.telemetry,
            lambda uri, telemetry, cutoff_value=round_cutoff, slot=cleanup_slot: plan_one_dataset(
                uri, config, cutoff_value, telemetry, slot
            ),
            plan_batch_partitions,
            phase="plan",
        )
        planned: list[dict[str, Any]] = []
        plan: Any
        for plan in plans:
            uri: str = plan["uri"]
            if round_index == 0:
                base_by_uri[uri] = {
                    field: plan[field] for field in ("retention_rows_deleted", "retention_skipped") if field in plan
                }
            if plan.get("task_jsons"):
                planned.append(plan)
            else:
                results_by_uri[uri] = {**base_by_uri.get(uri, {}), **plan}
        if not planned:
            return []

        flat_tasks: list[tuple[str, int, str]] = [
            (plan["uri"], plan["read_version"], task_json) for plan in planned for task_json in plan["task_jsons"]
        ]
        logger.info(
            "compaction round %d/%d: %d datasets, %d rewrite tasks",
            round_index + 1,
            REPLAN_BUDGET,
            len(planned),
            len(flat_tasks),
        )
        with driver_telemetry.timed("run.rewrite_ms"):
            rewrites_by_uri: Any
            errors_by_uri: Any
            rewrites_by_uri, errors_by_uri = self.execute_fleet_tasks(spark, flat_tasks)

        commit_pairs: list[tuple[str, list[str]]] = []
        for plan in planned:
            planned_uri: str = plan["uri"]
            if planned_uri in errors_by_uri:
                results_by_uri[planned_uri] = {
                    **base_by_uri.get(planned_uri, {}),
                    "uri": planned_uri,
                    "error": errors_by_uri[planned_uri],
                    "phase": "execute",
                    "bytes_removed": 0,
                }
                continue
            commit_pairs.append((planned_uri, rewrites_by_uri.get(planned_uri, [])))
        outcomes: list[dict[str, Any]] = self.commit_fleet(spark, commit_pairs)
        conflicted: list[str] = []
        outcome: Any
        for outcome in outcomes:
            uri = outcome["uri"]
            if outcome.get("conflict"):
                conflicted.append(uri)
            else:
                results_by_uri[uri] = {**base_by_uri.get(uri, {}), **outcome}
        return conflicted

    def run(self, spark: SparkSession, dataset_uris: Iterable[str]) -> list[dict[str, Any]]:
        """Maintain every dataset through the unified plan-execute-commit rounds.

        When :attr:`MaintenanceConfig.cluster_rewrite` is set, an opt-in clustered rewrite
        (:func:`~lance_etl.maintenance.cluster.run_cluster_rewrites`) runs first over every
        dataset, before round 0. A clustered or cluster-errored dataset's result lands directly in
        the final aggregation and that dataset never enters the plan-execute-commit rounds below;
        only the cluster-ineligible passthrough datasets do, exactly as if clustering were off.

        Round structure: phase P fans out per dataset (retention runs only in the first round),
        phase E runs the whole fleet's rewrite tasks in one flat Spark job, and phase C fans the
        commits out per dataset. Datasets whose commit hit a semantic conflict re-enter the next
        round to be re-planned against the latest version, up to :data:`REPLAN_BUDGET` rounds, after
        which they are deferred to the next scheduled run with a ``dataset.hot_skipped`` metric.

        Per-dataset failure isolation: a plan, execute, or commit failure for one dataset is
        recorded as an ``{"error", "phase"}`` marker on that dataset's result and excluded from the
        remaining phases this run, while every other dataset still completes and commits. Failed
        datasets carry no cursor, so the next scheduled run simply re-plans them from current
        state. Callers detect the failures by scanning the returned dicts for the ``"error"`` key.
        A misconfigured cleanup horizon is the one loud exception: it fails the whole run fast
        before any dataset is touched, because it would otherwise mark every dataset identically.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to maintain, typically those changed recently.

        Returns:
            One statistics dictionary per dataset, in input order. A failed dataset's dictionary
            carries an ``"error"`` message and a ``"phase"`` label.

        Raises:
            ValueError: If ``cleanup_older_than_seconds`` is set below
                :data:`MIN_CLEANUP_HORIZON_SECONDS`, which is a misconfiguration that must fail the
                whole run rather than mark every dataset with the same error.
        """
        config: MaintenanceConfig = self.config
        validate_cleanup_horizon(config)
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.maintenance.run") as run_span:
            uris: list[str] = list(dataset_uris)
            run_span.set_tag("dataset_count", len(uris))
            run_span.set_tag("retention_active", config.retention_active())
            if not uris:
                return []

            cutoff: datetime | None = compute_cutoff(config.retention_seconds) if config.retention_active() else None
            cleanup_slot: int = active_cleanup_slot(config, datetime.now(tz=UTC))
            driver_telemetry.gauge("run.cleanup_slot", cleanup_slot)
            results_by_uri: dict[str, dict[str, Any]] = {}
            base_by_uri: dict[str, dict[str, Any]] = {}
            pending_uris: list[str] = uris

            if config.cluster_rewrite:
                cluster_results: Any
                cluster_results, pending_uris = maintenance_cluster.run_cluster_rewrites(
                    spark, uris, config, cutoff, driver_telemetry, cleanup_slot
                )
                results_by_uri.update(cluster_results)

            with driver_telemetry.timed("run.maintain_ms"):
                round_index: Any
                for round_index in range(REPLAN_BUDGET):
                    pending_uris = self.run_round(
                        spark,
                        round_index,
                        pending_uris,
                        cutoff,
                        base_by_uri,
                        results_by_uri,
                        driver_telemetry,
                        cleanup_slot,
                    )
                    if not pending_uris:
                        break

            did_work_hot_uris: list[str] = [
                uri for uri in pending_uris if int(base_by_uri.get(uri, {}).get("retention_rows_deleted", 0)) > 0
            ]
            cleanup_partitions: int = derive_partitions(spark, FANOUT_PARTITION_FACTOR)
            hot_cleanup_outcomes: list[dict[str, Any]] = fan_out_per_dataset(
                spark,
                did_work_hot_uris,
                config.telemetry,
                lambda uri, telemetry: cleanup_hot_dataset(uri, config, telemetry),
                cleanup_partitions,
                phase="cleanup",
            )
            hot_cleanup_by_uri: dict[str, dict[str, Any]] = {
                str(outcome["uri"]): outcome for outcome in hot_cleanup_outcomes
            }

            uri: Any
            for uri in pending_uris:
                driver_telemetry.incr("dataset.hot_skipped")
                logger.warning(
                    "skipping compaction of hot dataset %s: commit conflicted in all %d rounds",
                    uri,
                    REPLAN_BUDGET,
                )
                cleanup_outcome: dict[str, Any] = hot_cleanup_by_uri.get(uri, {})
                results_by_uri[uri] = {
                    **base_by_uri.get(uri, {}),
                    **cleanup_outcome,
                    "uri": uri,
                    "bytes_removed": int(cleanup_outcome.get("bytes_removed", 0)),
                    "skipped": f"commit conflicted in all {REPLAN_BUDGET} re-plan rounds",
                }

            results: list[dict[str, Any]] = [results_by_uri[uri] for uri in uris]

            if config.retention_active():
                rows_deleted: int = sum(int(item.get("retention_rows_deleted", 0)) for item in results)
                datasets_expired: int = sum(1 for item in results if int(item.get("retention_rows_deleted", 0)) > 0)
                run_span.set_tag("retention_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.retention_rows_deleted", rows_deleted)
                driver_telemetry.gauge("run.retention_datasets_expired", datasets_expired)
                logger.info("retention: %d datasets expired, %d rows deleted", datasets_expired, rows_deleted)

            bytes_removed: int = sum(int(item.get("bytes_removed", 0)) for item in results)
            fragments_removed: int = sum(int(item.get("fragments_removed", 0)) for item in results)
            skipped: int = sum(1 for item in results if item.get("skipped"))
            run_span.set_tag("skipped_datasets", skipped)
            driver_telemetry.gauge("run.datasets", len(results))
            driver_telemetry.gauge("run.datasets_skipped", skipped)
            driver_telemetry.gauge("run.bytes_removed", bytes_removed)
            driver_telemetry.gauge("run.fragments_removed", fragments_removed)

            report_fleet_failures(
                results,
                run_span,
                driver_telemetry,
                "maintenance run",
                lambda item: str(item.get("phase", "unknown")),
                logger,
            )

            logger.info(
                "maintenance run: %d datasets, %d fragments removed, %d bytes reclaimed",
                len(results),
                fragments_removed,
                bytes_removed,
            )
            return results
