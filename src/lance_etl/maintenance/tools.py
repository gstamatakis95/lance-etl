"""Fleet-level manifest-migration, serving-tag, and interval-tag-retention helpers for Lance datasets.

These operations are embarrassingly parallel one-call-per-dataset functions.
Their fleet drivers (:func:`migrate_manifest_paths`, :func:`update_serving_tags`,
:func:`prune_interval_tags_fleet`) are thin adapters over :func:`~lance_etl.fanout.run_fleet_fanout`,
which owns the shared span/tag/timer/gauge/log driver shell around
:func:`~lance_etl.fanout.fan_out_per_dataset` and spreads the per-dataset work across Spark
executors without any additional orchestration.

:func:`migrate_dataset_manifest_paths` and :func:`migrate_manifest_paths` upgrade
existing V1 manifest paths to the V2 naming scheme, which makes every dataset open
cost one object-store request instead of a version-count-proportional LIST.

:func:`update_serving_tag` and :func:`update_serving_tags` flip one or more named serving
tags to a target version for blue-green promotion, opening the dataset exactly once per
call regardless of how many tags are flipped. A tagged version is exempt from
:func:`~lance_etl.maintenance.job.cleanup_dataset` pruning, so the version a serving
layer reads stays readable until the tag moves to a newer one.

:func:`prune_interval_tags` and :func:`prune_interval_tags_fleet` delete old interval
tags whose names are classified by :func:`datetime.strptime` against the
``%Y%m%dT%H%M%SZ`` format, keeping only the newest ``tag_keep_last`` tags.  Tags that
do not match the format (``HEAD`` and other non-interval tags) are never touched.

``dataset.tags.create/update/delete`` are plain object-store put/delete calls on
``_refs/tags/<name>`` files, not optimistic-concurrency manifest commits, so they never
raise the "commit conflict" markers that :func:`~lance_etl.telemetry.commit_with_retries`
looks for. Lost races surface instead as a ``ValueError`` whose message contains
:data:`TAG_EXISTS_MARKER` (a create raced against a create or a move) or
:data:`TAG_MISSING_MARKER` (an update or delete raced against a delete). Both
:func:`update_serving_tag` and :func:`prune_interval_tags` match on those substrings to
resolve the lost race idempotently instead of failing the fleet run.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

import lance
from pyspark.sql import SparkSession

from lance_etl.fanout import TAG_FANOUT_PARTITIONS, run_fleet_fanout
from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

TAG_EXISTS_MARKER: str = "already exists"
TAG_MISSING_MARKER: str = "does not exist"
MAX_TAG_RACE_ATTEMPTS: int = 3
TAG_RACE_BACKOFF_SECONDS: float = 0.05


def migrate_dataset_manifest_paths(
    uri: str, storage_options: dict[str, Any] | None, telemetry: Telemetry
) -> dict[str, Any]:
    """Migrate one existing dataset's manifest paths to the V2 naming scheme in place.

    Datasets bootstrapped by the ETL are always created with V2 manifest paths, which
    makes every open one object-store request instead of a version-count-proportional
    LIST. Datasets created before that default still carry V1 names. This helper calls
    ``LanceDataset.migrate_manifest_paths_v2``, which renames every V1 manifest to the
    V2 inverted-version name. The call is idempotent, so re-running it on an
    already-migrated or freshly-bootstrapped dataset is a cheap no-op. It needs no
    lost-race resolver of its own: a manifest-path rename has no commit-conflict
    surface to lose a race against, so a retried task simply converges instead of
    corrupting state, the same idempotency the fleet-level fan-out in
    :func:`migrate_manifest_paths` already relies on. A single dataset's migration failure is
    isolated by that fan-out into a ``{"uri", "error", "phase": "migrate"}`` marker and does not
    abort the run, so the other datasets still migrate.

    DANGER: this is not transactional. Lance documents that it must not run while other
    operations touch the dataset and must run to completion before any resume. Schedule
    it in a maintenance window with ingestion, compaction, and indexing paused for the
    targeted datasets.

    Args:
        uri: Dataset URI.
        storage_options: Object-store options forwarded to pylance.
        telemetry: Telemetry facade for the current process.

    Returns:
        A statistics dictionary with keys ``uri`` and ``migrated`` set to ``True``.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    with telemetry.timed("dataset.migrate_manifest_ms"):
        dataset.migrate_manifest_paths_v2()
    telemetry.incr("dataset.manifest_migrated")
    return {"uri": uri, "migrated": True}


def migrate_manifest_paths(
    spark: SparkSession,
    dataset_uris: Iterable[str],
    telemetry_config: TelemetryConfig,
    storage_options: dict[str, Any] | None,
    partitions: int = TAG_FANOUT_PARTITIONS,
) -> list[dict[str, Any]]:
    """Migrate a fleet of datasets to V2 manifest paths, one task per executor partition.

    Each dataset is independent, so the migration fans out across executors through the
    shared per-dataset fan-out. The per-dataset call is idempotent, so a retried task
    converges instead of corrupting state. This is a maintenance operation: run it only
    with the targeted datasets quiesced.

    Args:
        spark: Active Spark session.
        dataset_uris: Datasets whose manifest paths should be migrated to V2.
        telemetry_config: Telemetry configuration created per executor process.
        storage_options: Object-store options forwarded to pylance.
        partitions: Maximum Spark partitions for the migration job.

    Returns:
        One statistics dictionary per dataset.
    """

    def per_dataset(uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Migrate one dataset's manifest paths, closing over ``storage_options``."""
        return migrate_dataset_manifest_paths(uri, storage_options, telemetry)

    def log_results(results: list[dict[str, Any]]) -> None:
        """Log the manifest migration summary."""
        logger.info("manifest migration: %d datasets migrated to V2 paths", len(results))

    return run_fleet_fanout(
        spark,
        dataset_uris,
        telemetry_config,
        per_dataset,
        partitions,
        span_name="lance.manifest_migration.run",
        phase="migrate",
        timer_metric="run.migrate_manifest_ms",
        gauge_metric="run.manifests_migrated",
        gauge_value=len,
        log_results=log_results,
    )


def resolve_serving_tag(dataset: lance.LanceDataset, tag: str, version: int, created: bool) -> bool:
    """Create or move one serving tag, with a single-level lost-race fallback.

    ``created`` reflects a caller's earlier ``dataset.tags.list()`` snapshot, which can be stale
    by the time the write lands because another writer (a concurrent ETL stamp DAG, a retried
    Spark task, or the separately scheduled pipeline) moved the tag in between. If ``created`` is
    ``True`` but ``dataset.tags.create`` reports the tag already exists
    (:data:`TAG_EXISTS_MARKER`), the create lost the race, so the tag is moved with
    ``dataset.tags.update`` instead. Symmetrically, if ``created`` is ``False`` but
    ``dataset.tags.update`` reports the tag is missing (:data:`TAG_MISSING_MARKER`), a concurrent
    prune or delete raced ahead, so the tag is recreated with ``dataset.tags.create``. Both
    fallbacks are safe: a tag move is last-writer-wins on the stored version, and every caller
    passes the version it actually intends the tag to point at, so whichever write lands last is
    the correct outcome regardless of which branch produced it.

    Args:
        dataset: The open Lance dataset.
        tag: Serving-tag name to create or move.
        version: Target version for the tag.
        created: Whether the tag was absent in the caller's ``dataset.tags.list()`` snapshot.

    Returns:
        ``True`` if a create ultimately landed, ``False`` if an update did.

    Raises:
        ValueError: The fallback write itself lost the race, which signals a pathological
            double race for the caller's bounded retry loop to resolve, or either write failed
            for a reason unrelated to a lost race.
        OSError: Any other object-store failure.
    """
    if created:
        try:
            dataset.tags.create(tag, version)
            return True
        except ValueError as exc:
            if TAG_EXISTS_MARKER not in str(exc):
                raise
            dataset.tags.update(tag, version)
            return False
    try:
        dataset.tags.update(tag, version)
        return False
    except ValueError as exc:
        if TAG_MISSING_MARKER not in str(exc):
            raise
        dataset.tags.create(tag, version)
        return True


def flip_one_tag(dataset: lance.LanceDataset, tag: str, version: int, telemetry: Telemetry) -> bool:
    """Create or move one serving tag on an already-open dataset, with bounded lost-race retry.

    Wraps :func:`resolve_serving_tag` in the bounded retry loop described on
    :func:`update_serving_tag`: a lost race between this call's ``tags.list()`` snapshot and its
    write is self-healed by falling back to the complementary operation, retried up to
    :data:`MAX_TAG_RACE_ATTEMPTS` times with a short fixed backoff
    (:data:`TAG_RACE_BACKOFF_SECONDS`) between attempts, each re-reading ``tags.list()``. The last
    exception is re-raised only once every attempt is exhausted.

    Args:
        dataset: The already-open Lance dataset. Callers flipping several tags on the same
            dataset must reuse this one open handle so the dataset is opened exactly once.
        tag: Serving-tag name to create or move.
        version: Target version for the tag.
        telemetry: Telemetry facade for the current process.

    Returns:
        ``True`` if a create ultimately landed, ``False`` if an update did.

    Raises:
        ValueError: Every retry attempt lost the race, or a write failed for a reason unrelated
            to a lost race.
        OSError: Any other object-store failure.
    """
    created: bool = tag not in dataset.tags.list()
    actual_created: bool = created
    last_exc: ValueError | None = None
    with telemetry.timed("dataset.tag_update_ms", tags=[f"tag:{tag}"]):
        attempt: Any
        for attempt in range(MAX_TAG_RACE_ATTEMPTS):
            attempt_created: bool = created if attempt == 0 else tag not in dataset.tags.list()
            try:
                actual_created = resolve_serving_tag(dataset, tag, version, attempt_created)
                last_exc = None
                break
            except ValueError as exc:
                message: str = str(exc)
                if TAG_EXISTS_MARKER not in message and TAG_MISSING_MARKER not in message:
                    raise
                last_exc = exc
                if attempt < MAX_TAG_RACE_ATTEMPTS - 1:
                    time.sleep(TAG_RACE_BACKOFF_SECONDS)
        if last_exc is not None:
            raise last_exc
        if actual_created:
            telemetry.incr("dataset.tag_created", tags=[f"tag:{tag}"])
        else:
            telemetry.incr("dataset.tag_updated", tags=[f"tag:{tag}"])
    return actual_created


def update_serving_tag(
    uri: str,
    target_version: int | None,
    storage_options: dict[str, Any] | None,
    telemetry: Telemetry,
    tags: Sequence[str] = ("HEAD",),
) -> dict[str, Any]:
    """Point one or more serving tags at a target dataset version for blue-green promotion.

    Opens the dataset exactly once and flips every tag in ``tags`` against that single open
    handle, so an internal caller moving several tags costs one dataset open instead of one per
    tag. Each tag is created when it does not
    exist yet, otherwise updated in place, through the Lance tags API. A tagged version is exempt
    from version cleanup: :func:`~lance_etl.maintenance.job.cleanup_dataset` passes
    ``error_if_tagged_old_versions=False`` and Lance never prunes a tagged version regardless of
    age, so the version a serving layer reads stays readable across maintenance until the tag is
    flipped to a newer one.

    Every tag is resolved through the same open dataset and the same target ``version``, so
    flipping several tags in one call guarantees they land on the identical version, which two
    separate calls could not guarantee if a writer advanced the dataset in between. The
    create-or-update decision for each tag is resolved idempotently through :func:`flip_one_tag`:
    a lost race between that tag's ``tags.list()`` snapshot and its write is self-healed by
    falling back to the complementary operation, and the returned ``created`` entry reflects
    whichever write actually landed rather than the stale snapshot.

    The safe blue-green operational sequence is logged on every call because a tag move
    alone changes nothing for a running serving process. Build the green version (ETL
    plus index plus compaction), prewarm the serving layer against that explicit version,
    and only then flip the tag(s). The serving layer is never assumed to auto-refresh when
    a tag moves: it must be told to re-resolve the tag, or it keeps serving the
    previous version.

    Args:
        uri: Dataset URI.
        target_version: The dataset version to point every tag at. ``None`` selects the
            dataset's latest version only when the tag set does not contain ``HEAD``.
        storage_options: Object-store options forwarded to pylance.
        telemetry: Telemetry facade for the current process.
        tags: Serving-tag names to create or move, all against the same resolved version.
            Defaults to ``("HEAD",)``. Duplicate names are deduplicated before flipping.

    Returns:
        A statistics dictionary with keys ``uri``, ``tags`` (the deduplicated list of tag names
        flipped), ``version``, and ``created`` (a dict mapping each flipped tag name to whether a
        create or an update actually landed for it).

    Raises:
        ValueError: If ``HEAD`` is requested without an explicit target version.
    """
    unique_tags: list[str] = list(dict.fromkeys(tags))
    if "HEAD" in unique_tags and target_version is None:
        raise ValueError("target_version is required when publishing HEAD")
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    version: int = dataset.version if target_version is None else target_version
    logger.info(
        "blue-green tag flip for %s: 1) build green version %d, 2) prewarm the serving layer against version %d, "
        "3) flip tag(s) %r to version %d. A tag move does not refresh a running serving process: prewarm and "
        "re-resolve the tag explicitly before relying on it.",
        uri,
        version,
        version,
        unique_tags,
        version,
    )
    created_by_tag: dict[str, bool] = {tag: flip_one_tag(dataset, tag, version, telemetry) for tag in unique_tags}
    return {"uri": uri, "tags": unique_tags, "version": version, "created": created_by_tag}


def update_serving_tags(
    spark: SparkSession,
    dataset_uris: Iterable[str],
    telemetry_config: TelemetryConfig,
    storage_options: dict[str, Any] | None,
    tags: Sequence[str] = ("HEAD",),
    target_version: int | None = None,
    partitions: int = TAG_FANOUT_PARTITIONS,
) -> list[dict[str, Any]]:
    """Flip one or more serving tags across a fleet of datasets, one task per executor partition.

    Each dataset's tag flip is an independent cheap metadata commit, so the work fans
    out across executors exactly like the manifest migration. Every tag in ``tags`` is flipped
    against the same single dataset open (see :func:`update_serving_tag`). With
    ``target_version`` set, every dataset is pointed at that same version number, which
    only makes sense when every selected dataset has the intended version number. With
    ``target_version=None`` each dataset's non-HEAD interval tags are moved to its own latest
    version. Publishing ``HEAD`` without an exact version is rejected.

    Args:
        spark: Active Spark session.
        dataset_uris: Datasets whose serving tags should be flipped.
        telemetry_config: Telemetry configuration created per executor process.
        storage_options: Object-store options forwarded to pylance.
        tags: Serving-tag names to create or move. Defaults to ``("HEAD",)``.
        target_version: Target version for every dataset, or ``None`` to use each dataset's latest
            version for non-HEAD interval tags only.
        partitions: Maximum Spark partitions for the tag-flip job.

    Returns:
        One statistics dictionary per dataset, shaped like :func:`update_serving_tag`'s return
        value.
    """

    def per_dataset(uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Flip one dataset's serving tags, closing over ``storage_options``/``tags``/``target_version``."""
        return update_serving_tag(uri, target_version, storage_options, telemetry, tags)

    def log_results(results: list[dict[str, Any]]) -> None:
        """Log the serving-tag flip summary."""
        logger.info("serving-tag flip: tags %r moved on %d datasets", list(tags), len(results))

    return run_fleet_fanout(
        spark,
        dataset_uris,
        telemetry_config,
        per_dataset,
        partitions,
        span_name="lance.serving_tag.run",
        phase="tag",
        timer_metric="run.serving_tag_ms",
        gauge_metric="run.tags_flipped",
        gauge_value=len,
        log_results=log_results,
        span_tags={"tags": ",".join(tags)},
    )


def prune_interval_tags(
    uri: str,
    storage_options: dict[str, Any] | None,
    tag_keep_last: int,
    telemetry: Telemetry,
) -> dict[str, Any]:
    """Delete old interval tags on one dataset, keeping the newest ``tag_keep_last``.

    Interval tags are classified by parsing each tag name with
    ``datetime.strptime(name, "%Y%m%dT%H%M%SZ")`` inside a ``try/except ValueError``.
    Tags that do not match the format (``HEAD`` and any other non-interval names) are
    never considered for deletion.  The matching tags are sorted descending by parsed
    time, the newest ``tag_keep_last`` are kept, and the rest are deleted via
    ``dataset.tags.delete(name)``.

    Deletion is idempotent: if ``dataset.tags.delete`` raises a ``ValueError`` containing
    :data:`TAG_MISSING_MARKER`, another pruner or a retried Spark task already removed the tag,
    so the delete is treated as a no-op success (counted under
    ``dataset.interval_tag_already_pruned`` instead of ``dataset.interval_tag_pruned``) rather
    than failing the fleet run. Any other exception still propagates.

    Args:
        uri: Dataset URI.
        storage_options: Object-store options forwarded to pylance.
        tag_keep_last: Number of newest interval tags to retain.
        telemetry: Telemetry facade for the current process.

    Returns:
        A statistics dictionary with keys ``uri``, ``tags_pruned``, and ``tags_kept``.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    all_tags: list[str] = list(dataset.tags.list())

    interval_tags: list[tuple[datetime, str]] = []
    name: Any
    for name in all_tags:
        try:
            parsed: datetime = datetime.strptime(name, "%Y%m%dT%H%M%SZ")
            interval_tags.append((parsed, name))
        except ValueError:
            pass

    interval_tags.sort(key=lambda pair: pair[0], reverse=True)
    to_keep: list[str] = [name for _, name in interval_tags[:tag_keep_last]]
    to_delete: list[str] = [name for _, name in interval_tags[tag_keep_last:]]

    with telemetry.timed("dataset.prune_tags_ms"):
        for name in to_delete:
            try:
                dataset.tags.delete(name)
            except ValueError as exc:
                if TAG_MISSING_MARKER not in str(exc):
                    raise
                telemetry.incr("dataset.interval_tag_already_pruned")
                continue
            telemetry.incr("dataset.interval_tag_pruned")

    logger.info(
        "prune_interval_tags: %s — kept %d, pruned %d interval tags",
        uri,
        len(to_keep),
        len(to_delete),
    )
    return {"uri": uri, "tags_pruned": len(to_delete), "tags_kept": len(to_keep)}


def prune_interval_tags_fleet(
    spark: SparkSession,
    dataset_uris: Iterable[str],
    telemetry_config: TelemetryConfig,
    storage_options: dict[str, Any] | None,
    tag_keep_last: int,
    partitions: int = TAG_FANOUT_PARTITIONS,
) -> list[dict[str, Any]]:
    """Prune old interval tags across a fleet of datasets, one task per executor partition.

    Each dataset's tag pruning is an independent metadata operation, so the work fans
    out across executors exactly like the manifest migration.  ``tag_keep_last`` is
    broadcast implicitly through the closure captured by the per-dataset callable.

    Args:
        spark: Active Spark session.
        dataset_uris: Datasets whose old interval tags should be pruned.
        telemetry_config: Telemetry configuration created per executor process.
        storage_options: Object-store options forwarded to pylance.
        tag_keep_last: Number of newest interval tags to retain per dataset.
        partitions: Maximum Spark partitions for the prune job.

    Returns:
        One statistics dictionary per dataset.
    """

    def per_dataset(uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Prune one dataset's interval tags, closing over ``storage_options``/``tag_keep_last``."""
        return prune_interval_tags(uri, storage_options, tag_keep_last, telemetry)

    def pruned_total(results: list[dict[str, Any]]) -> int:
        """Sum the pruned-tag counts across every dataset result."""
        return sum(int(r.get("tags_pruned", 0)) for r in results)

    def log_results(results: list[dict[str, Any]]) -> None:
        """Log the interval-tag prune summary."""
        logger.info("interval-tag prune: %d tags pruned across %d datasets", pruned_total(results), len(results))

    return run_fleet_fanout(
        spark,
        dataset_uris,
        telemetry_config,
        per_dataset,
        partitions,
        span_name="lance.interval_tag_prune.run",
        phase="prune",
        timer_metric="run.prune_tags_ms",
        gauge_metric="run.interval_tags_pruned",
        gauge_value=pruned_total,
        log_results=log_results,
        span_tags={"tag_keep_last": tag_keep_last},
    )
