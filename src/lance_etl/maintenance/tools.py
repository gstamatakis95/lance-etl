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
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

import lance
from pyspark.sql import SparkSession

from lance_etl.fanout import fan_out_per_dataset
from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)


def migrate_dataset_manifest_paths(
    uri: str, storage_options: dict[str, Any] | None, telemetry: Telemetry
) -> dict[str, Any]:
    """Migrate one existing dataset's manifest paths to the V2 naming scheme in place.

    Datasets bootstrapped by the ETL are always created with V2 manifest paths, which
    makes every open one object-store request instead of a version-count-proportional
    LIST. Datasets created before that default still carry V1 names. This helper calls
    ``LanceDataset.migrate_manifest_paths_v2``, which renames every V1 manifest to the
    V2 inverted-version name. The call is idempotent, so re-running it on an
    already-migrated or freshly-bootstrapped dataset is a cheap no-op.

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

    Each dataset is independent, so the migration fans out across executors exactly like
    the compaction small tier. The per-dataset call is idempotent, so a retried task
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
            )
        driver_telemetry.gauge("run.manifests_migrated", len(results))
        logger.info("manifest migration: %d datasets migrated to V2 paths", len(results))
        return results


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
        A statistics dictionary with keys ``uri``, ``tag``, ``version``, and ``created``.
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
    with telemetry.timed("dataset.tag_update_ms", tags=[f"tag:{tag}"]):
        if created:
            dataset.tags.create(tag, version)
            telemetry.incr("dataset.tag_created", tags=[f"tag:{tag}"])
        else:
            dataset.tags.update(tag, version)
            telemetry.incr("dataset.tag_updated", tags=[f"tag:{tag}"])
    return {"uri": uri, "tag": tag, "version": version, "created": created}


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
            dataset.tags.delete(name)
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
            )
        pruned_total: int = sum(int(r.get("tags_pruned", 0)) for r in results)
        driver_telemetry.gauge("run.interval_tags_pruned", pruned_total)
        logger.info("interval-tag prune: %d tags pruned across %d datasets", pruned_total, len(results))
        return results
