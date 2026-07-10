"""Fleet-level manifest-migration, serving-tag, and interval-tag-retention helpers for Lance datasets.

These operations are embarrassingly parallel one-call-per-dataset functions.
They reuse :func:`fan_out_per_dataset` from :mod:`lance_etl.fanout` to
spread work across Spark executors without any additional orchestration.

:func:`migrate_dataset_manifest_paths` and :func:`migrate_manifest_paths` upgrade
existing V1 manifest paths to the V2 naming scheme, which makes every dataset open
cost one object-store request instead of a version-count-proportional LIST.

:func:`update_serving_tag` and :func:`update_serving_tags` flip a named serving tag
to a target version for blue-green promotion. A tagged version is exempt from
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
from collections.abc import Iterable
from datetime import datetime
from typing import Any

import lance
from pyspark.sql import SparkSession

from lance_etl.fanout import fan_out_per_dataset
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
    partitions: int = 512,
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
    driver_telemetry: Telemetry = Telemetry.create(telemetry_config)
    with driver_telemetry.span("lance.manifest_migration.run") as run_span:
        uris: list[str] = list(dataset_uris)
        run_span.set_tag("dataset_count", len(uris))
        if not uris:
            return []
        with driver_telemetry.timed("run.migrate_manifest_ms"):
            results: list[dict[str, Any]] = fan_out_per_dataset(
                spark,
                uris,
                telemetry_config,
                lambda uri, telemetry: migrate_dataset_manifest_paths(uri, storage_options, telemetry),
                partitions,
                phase="migrate",
            )
        driver_telemetry.gauge("run.manifests_migrated", len(results))
        logger.info("manifest migration: %d datasets migrated to V2 paths", len(results))
        return results


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


def update_serving_tag(
    uri: str,
    target_version: int | None,
    storage_options: dict[str, Any] | None,
    telemetry: Telemetry,
    tag: str = "HEAD",
) -> dict[str, Any]:
    """Point a serving tag at a target dataset version for blue-green promotion.

    Creates the tag when it does not exist yet, otherwise updates it in place, through
    the Lance tags API. A tagged version is exempt from version cleanup:
    :func:`~lance_etl.maintenance.job.cleanup_dataset` passes
    ``error_if_tagged_old_versions=False`` and Lance never prunes a tagged version
    regardless of age, so the version a serving layer reads stays readable across
    maintenance until the tag is flipped to a newer one.

    The create-or-update decision is resolved idempotently through :func:`resolve_serving_tag`:
    a lost race between this call's ``tags.list()`` snapshot and its write is self-healed by
    falling back to the complementary operation, and the returned ``created`` flag reflects
    whichever write actually landed rather than the stale snapshot. This is safe because a tag
    move is last-writer-wins on the target version, which every caller supplies explicitly. The
    fallback itself is wrapped in a small bounded retry loop (:data:`MAX_TAG_RACE_ATTEMPTS`) to
    cover the pathological double race where the fallback also loses (for example create loses to
    an existing tag, the fallback update then loses because the tag was deleted again in between):
    each further attempt re-reads ``tags.list()`` and tries again after a short fixed backoff
    (:data:`TAG_RACE_BACKOFF_SECONDS`), and the last exception is re-raised only once every
    attempt is exhausted.

    The safe blue-green operational sequence is logged on every call because a tag move
    alone changes nothing for a running serving process. Build the green version (ETL
    plus index plus compaction), prewarm the serving layer against that explicit version,
    and only then flip the tag. The serving layer is never assumed to auto-refresh when
    the tag moves: it must be told to re-resolve the tag, or it keeps serving the
    previous version.

    Args:
        uri: Dataset URI.
        target_version: The dataset version to point the tag at. ``None`` selects the
            dataset's latest version.
        storage_options: Object-store options forwarded to pylance.
        telemetry: Telemetry facade for the current process.
        tag: Serving-tag name to create or move. Defaults to ``"HEAD"``.

    Returns:
        A statistics dictionary with keys ``uri``, ``tag``, ``version``, and ``created``, where
        ``created`` reflects the write that actually landed.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    version: int = dataset.version if target_version is None else target_version
    created: bool = tag not in dataset.tags.list()
    logger.info(
        "blue-green tag flip for %s: 1) build green version %d, 2) prewarm the serving layer against version %d, "
        "3) flip tag %r to version %d. A tag move does not refresh a running serving process: prewarm and re-resolve "
        "the tag explicitly before relying on it.",
        uri,
        version,
        version,
        tag,
        version,
    )
    actual_created: bool = created
    last_exc: ValueError | None = None
    with telemetry.timed("dataset.tag_update_ms", tags=[f"tag:{tag}"]):
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
    return {"uri": uri, "tag": tag, "version": version, "created": actual_created}


def update_serving_tags(
    spark: SparkSession,
    dataset_uris: Iterable[str],
    telemetry_config: TelemetryConfig,
    storage_options: dict[str, Any] | None,
    tag: str = "HEAD",
    target_version: int | None = None,
    partitions: int = 512,
) -> list[dict[str, Any]]:
    """Flip a serving tag across a fleet of datasets, one task per executor partition.

    Each dataset's tag flip is an independent cheap metadata commit, so the work fans
    out across executors exactly like the manifest migration. With ``target_version``
    set, every dataset is pointed at that same version number, which only makes sense
    for a single dataset. With ``target_version=None`` (the common fleet case) each
    dataset's tag is moved to its own latest version, promoting the freshly built green
    version of each.

    Args:
        spark: Active Spark session.
        dataset_uris: Datasets whose serving tag should be flipped.
        telemetry_config: Telemetry configuration created per executor process.
        storage_options: Object-store options forwarded to pylance.
        tag: Serving-tag name to create or move. Defaults to ``"HEAD"``.
        target_version: Target version for every dataset, or ``None`` to use each
            dataset's latest version.
        partitions: Maximum Spark partitions for the tag-flip job.

    Returns:
        One statistics dictionary per dataset.
    """
    driver_telemetry: Telemetry = Telemetry.create(telemetry_config)
    with driver_telemetry.span("lance.serving_tag.run") as run_span:
        uris: list[str] = list(dataset_uris)
        run_span.set_tag("dataset_count", len(uris))
        run_span.set_tag("tag", tag)
        if not uris:
            return []
        with driver_telemetry.timed("run.serving_tag_ms"):
            results: list[dict[str, Any]] = fan_out_per_dataset(
                spark,
                uris,
                telemetry_config,
                lambda uri, telemetry: update_serving_tag(uri, target_version, storage_options, telemetry, tag),
                partitions,
                phase="tag",
            )
        driver_telemetry.gauge("run.tags_flipped", len(results))
        logger.info("serving-tag flip: tag %r moved on %d datasets", tag, len(results))
        return results


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
        A statistics dictionary with keys ``uri``, ``tags_pruned``, ``tags_kept``, and
        optionally ``skipped`` when ``tag_keep_last`` is ``None`` (though callers
        checking ``None`` should skip calling this function entirely).
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
    all_tags: list[str] = list(dataset.tags.list())

    interval_tags: list[tuple[datetime, str]] = []
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
    partitions: int = 512,
) -> list[dict[str, Any]]:
    """Prune old interval tags across a fleet of datasets, one task per executor partition.

    Each dataset's tag pruning is an independent metadata operation, so the work fans
    out across executors exactly like the manifest migration.  ``tag_keep_last`` is
    broadcast implicitly through the closure captured by the per-dataset lambda.

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
    driver_telemetry: Telemetry = Telemetry.create(telemetry_config)
    with driver_telemetry.span("lance.interval_tag_prune.run") as run_span:
        uris: list[str] = list(dataset_uris)
        run_span.set_tag("dataset_count", len(uris))
        run_span.set_tag("tag_keep_last", tag_keep_last)
        if not uris:
            return []
        with driver_telemetry.timed("run.prune_tags_ms"):
            results: list[dict[str, Any]] = fan_out_per_dataset(
                spark,
                uris,
                telemetry_config,
                lambda uri, telemetry: prune_interval_tags(uri, storage_options, tag_keep_last, telemetry),
                partitions,
                phase="prune",
            )
        pruned_total: int = sum(int(r.get("tags_pruned", 0)) for r in results)
        driver_telemetry.gauge("run.interval_tags_pruned", pruned_total)
        logger.info("interval-tag prune: %d tags pruned across %d datasets", pruned_total, len(results))
        return results
