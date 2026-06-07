"""TTL (data-expiration) maintenance job for Lance datasets.

Rows whose source event timestamp is older than ``now - retention`` are expired by writing a deletion vector via
:meth:`lance.LanceDataset.delete`. The canonical clock is the event timestamp column named by
:attr:`TTLConfig.timestamp_column` (default ``"timestamp"``), matching :attr:`lance_etl.etl.ETLConfig.ts_col`.
There is no ingest-time column and receipt-based retention is not supported; see ADR 0016.

Execution follows the two-tier orchestration from :mod:`lance_etl.compaction`:

- **Tier A (small datasets):** Every dataset URI is fanned out across Spark executors in one batch job. Each
  executor task opens its dataset, validates the timestamp column against the schema, and issues a single
  ``LanceDataset.delete`` call. For very small datasets (below ``small_tier_fragment_threshold``) the entire
  operation happens in-process on the executor.
- **Tier B (large datasets):** Datasets above the fragment threshold are classified as large during the tier-A
  probe and returned to the driver. The driver then fans the large datasets out as a second Spark job, using the
  same per-dataset callable but with a higher Spark partition count so each large dataset gets its own task. For
  TTL, the delete write is always a single Lance call regardless of dataset size, so the tier-B executor work is
  identical in shape to tier A -- the split primarily limits how many large datasets share a partition with small
  ones in the first pass.

Delete predicate safety: the timestamp column name is validated against the Lance dataset schema and the
identifier allowlist ``[A-Za-z_][A-Za-z0-9_]*`` before any predicate is constructed. The cutoff timestamp is
formatted as an unambiguous ISO-8601 literal and embedded as a Lance SQL timestamp literal via
``TIMESTAMP 'YYYY-MM-DDTHH:MM:SS.ffffff'``. No user-supplied string is ever interpolated without validation.

When :attr:`TTLConfig.enabled` is ``False``, :meth:`TTLJob.run` is a strict no-op that logs and returns a zero
report without touching any dataset.

When :attr:`TTLConfig.compact_after_delete` is ``True`` (the default), each dataset is compacted with a
single-process :class:`lance.optimize.Compaction` call after the delete so deletion vectors are materialised and
storage is reclaimed. The compaction honours the same ``cleanup_older_than_seconds`` as the main compaction job.

All commits go through :func:`lance_etl.telemetry.commit_with_retries`.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import lance
from lance.optimize import Compaction, CompactionMetrics
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import discover_datasets
from lance_etl.telemetry import (
    DEFAULT_COMMIT_RETRIES,
    Telemetry,
    TelemetryConfig,
    commit_with_retries,
)

logger: logging.Logger = logging.getLogger(__name__)

TIMESTAMP_COLUMN_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""Identifier allowlist for the timestamp column name.

Mirrors the allowlist in :mod:`lance_etl.recall` and the Rust ``filter.rs`` column-validation regexp. Any column name
that does not match is rejected before predicate construction so no unvalidated string reaches the Lance SQL engine.
"""

TIMESTAMP_LITERAL_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%f"
"""strftime pattern for the cutoff timestamp literal in the Lance delete predicate.

Produces ``YYYY-MM-DDTHH:MM:SS.ffffff`` (microsecond resolution), which DataFusion's SQL parser accepts as
``TIMESTAMP 'YYYY-MM-DDTHH:MM:SS.ffffff'``.
"""

DEFAULT_COMPACT_CLEANUP_SECONDS: int = 6 * 3600
"""Default version-cleanup horizon for the post-delete compaction.

Matches :data:`lance_etl.compaction.MIN_CLEANUP_HORIZON_SECONDS` so the TTL job's compaction calls land in the same
safe range as the main compaction job.
"""

DEFAULT_SMALL_TIER_FRAGMENT_THRESHOLD: int = 128
"""Fragment count at or below which a dataset is handled in the tier-A small batch.

Matches the default in :class:`lance_etl.compaction.CompactionConfig` so a dataset the compactor treats as large is
also treated as large by the TTL job.
"""

DEFAULT_BATCH_PARTITIONS: int = 512
"""Default Spark partition count for the tier-A batch job.

Capped at the URI count by :func:`lance_etl.compaction.fan_out_per_dataset`.
"""

DEFAULT_LARGE_TIER_PARTITIONS: int = 64
"""Default Spark partition count for the tier-B large-dataset job.

Kept lower than the small-tier partition count because the tier-B job processes only the few datasets above the
fragment threshold and each is more expensive.
"""


@dataclass
class TTLConfig:
    """Configuration for :class:`TTLJob`.

    Attributes:
        retention: Maximum age of rows to retain. Rows whose event timestamp is older than ``now - retention`` are
            deleted. This field is required when ``enabled`` is ``True``; the job raises ``ValueError`` at run time
            when enabled is True and retention is ``timedelta(0)``.
        telemetry: Telemetry configuration. The only telemetry object pickled into executor closures.
        enabled: When ``False`` (the default), :meth:`TTLJob.run` is a strict no-op. Opt-in by setting
            ``enabled=True``.
        timestamp_column: Name of the event timestamp column in each dataset. Must match
            :attr:`lance_etl.etl.ETLConfig.ts_col`. Validated against the dataset schema and the identifier
            allowlist before predicate construction. Defaults to ``"timestamp"``.
        storage_options: Object-store options forwarded to pylance.
        compact_after_delete: When ``True`` (the default), each dataset is compacted with a single-process
            ``Compaction.execute`` call after the delete. Materialises the deletion vectors and reclaims storage.
            Set to ``False`` to skip the post-delete compaction, for example when a separate compaction job runs
            after the TTL job in the same pipeline.
        compact_cleanup_older_than_seconds: Age threshold for version cleanup after the post-delete compaction.
            Defaults to :data:`DEFAULT_COMPACT_CLEANUP_SECONDS`.
        small_tier_fragment_threshold: Fragment count above which a dataset is treated as tier-B large. Matches
            :data:`DEFAULT_SMALL_TIER_FRAGMENT_THRESHOLD` by default.
        batch_partitions: Spark partition count for the tier-A batch job.
        large_tier_partitions: Spark partition count for the tier-B large-dataset job.
        commit_retries: Retry budget for commit conflicts. Uses the same default as index and compaction commits.
        commit_backoff_seconds: Base backoff in seconds between commit retries.
    """

    retention: timedelta
    telemetry: TelemetryConfig
    enabled: bool = False
    timestamp_column: str = "timestamp"
    storage_options: dict[str, Any] | None = None
    compact_after_delete: bool = True
    compact_cleanup_older_than_seconds: int = DEFAULT_COMPACT_CLEANUP_SECONDS
    small_tier_fragment_threshold: int = DEFAULT_SMALL_TIER_FRAGMENT_THRESHOLD
    batch_partitions: int = DEFAULT_BATCH_PARTITIONS
    large_tier_partitions: int = DEFAULT_LARGE_TIER_PARTITIONS
    commit_retries: int = DEFAULT_COMMIT_RETRIES
    commit_backoff_seconds: float = 0.5
    constant_tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TTLDatasetResult:
    """The outcome of running TTL expiration on one dataset.

    Attributes:
        uri: The dataset URI.
        rows_deleted: Number of rows deleted.
        compacted: Whether a post-delete compaction was performed.
        tier: The execution tier, either ``"small"`` or ``"large"``.
        skipped: Non-empty when the dataset was skipped, with the reason.
    """

    uri: str
    rows_deleted: int
    compacted: bool
    tier: str
    skipped: str = ""


@dataclass(frozen=True)
class TTLReport:
    """The aggregate result of one TTL run.

    Attributes:
        datasets_scanned: Total datasets processed (or scanned when enabled=False).
        datasets_expired: Datasets on which at least one row was deleted.
        total_rows_deleted: Sum of deleted rows across all datasets.
        datasets_compacted: Datasets where a post-delete compaction was performed.
        datasets_skipped: Datasets skipped (column missing, or no match).
        enabled: Whether TTL was active. False means this is a no-op report.
    """

    datasets_scanned: int
    datasets_expired: int
    total_rows_deleted: int
    datasets_compacted: int
    datasets_skipped: int
    enabled: bool


def validate_timestamp_column(column: str, schema: Any) -> None:
    """Validate the timestamp column name against the identifier allowlist and dataset schema.

    The column name must pass the ``[A-Za-z_][A-Za-z0-9_]*`` allowlist and must exist as a field in the dataset
    schema. This two-step check prevents both unvalidated identifier characters from reaching the SQL engine and
    silent misconfigurations where the column name was changed but the config was not updated.

    Args:
        column: The timestamp column name from :attr:`TTLConfig.timestamp_column`.
        schema: The pyarrow schema of the target dataset.

    Raises:
        ValueError: If the column name fails the identifier allowlist.
        KeyError: If the column is not present in the dataset schema.
    """
    if not TIMESTAMP_COLUMN_PATTERN.match(column):
        raise ValueError(
            f"timestamp_column {column!r} fails the identifier allowlist "
            f"[A-Za-z_][A-Za-z0-9_]*. Use a simple column name with no special characters."
        )
    column_names: list[str] = schema.names
    if column not in column_names:
        raise KeyError(
            f"timestamp_column {column!r} is not present in the dataset schema. Available columns: {column_names}"
        )


def build_ttl_predicate(timestamp_column: str, cutoff: datetime) -> str:
    """Build the Lance SQL delete predicate for TTL expiration.

    The predicate is ``{column} < TIMESTAMP '{iso_cutoff}'`` which deletes all rows whose event timestamp is
    strictly before the cutoff. The timestamp column name has already been validated against the identifier
    allowlist and the dataset schema by :func:`validate_timestamp_column` before this function is called.

    The cutoff is formatted in UTC with microsecond resolution as ``YYYY-MM-DDTHH:MM:SS.ffffff``. DataFusion,
    which Lance uses as its query engine, accepts this form as a timestamp literal.

    Args:
        timestamp_column: The validated event timestamp column name.
        cutoff: The UTC cutoff datetime. Rows with a timestamp strictly before this are expired.

    Returns:
        A Lance SQL predicate string safe for passing to :meth:`lance.LanceDataset.delete`.
    """
    utc_cutoff: datetime = cutoff.astimezone(UTC)
    literal: str = utc_cutoff.strftime(TIMESTAMP_LITERAL_FORMAT)
    return f"{timestamp_column} < TIMESTAMP '{literal}'"


def compute_cutoff(retention: timedelta) -> datetime:
    """Compute the expiration cutoff as ``now(UTC) - retention``.

    Args:
        retention: The maximum age of rows to retain.

    Returns:
        The UTC cutoff datetime. Any event timestamp strictly before this is expired.
    """
    return datetime.now(tz=UTC) - retention


def compact_dataset_after_delete(
    uri: str,
    config: TTLConfig,
    telemetry: Telemetry,
) -> int:
    """Compact one dataset after TTL deletion to materialise deletion vectors and reclaim storage.

    Runs a single-process ``Compaction.execute`` call with ``materialize_deletions=True`` so the physical rows that
    received deletion vectors during the TTL delete are actually removed from the fragment files. Then prunes old
    versions. The operation is a best-effort: failure is logged and counted as a metric but does not propagate
    because a failed compaction leaves the data correct (deletion vectors remain valid); only the storage reclaim
    is deferred.

    Args:
        uri: Dataset URI.
        config: TTL configuration.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        The number of bytes reclaimed by version cleanup, or ``0`` when compaction fails.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        with telemetry.timed("ttl.compact_ms", tags=[f"uri:{uri}"]):
            metrics: CompactionMetrics = Compaction.execute(dataset, {"materialize_deletions": True})
        telemetry.distribution("ttl.compact_fragments_removed", metrics.fragments_removed)

        older_than: timedelta = timedelta(seconds=config.compact_cleanup_older_than_seconds)
        cleaned_dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        stats = cleaned_dataset.cleanup_old_versions(older_than=older_than, error_if_tagged_old_versions=False)
        telemetry.distribution("ttl.compact_bytes_removed", float(stats.bytes_removed))
        return int(stats.bytes_removed)
    except Exception:
        telemetry.error(f"post-delete compaction failed for {uri}", tags=[f"uri:{uri}"])
        return 0


def expire_one_dataset(uri: str, config: TTLConfig, cutoff: datetime, telemetry: Telemetry) -> dict[str, Any]:
    """Apply TTL expiration to one dataset, deleting rows older than the cutoff.

    Validates the timestamp column against the dataset schema and the identifier allowlist, builds the delete
    predicate, and issues the delete through :func:`lance_etl.telemetry.commit_with_retries`. When
    :attr:`TTLConfig.compact_after_delete` is ``True`` and at least one row was deleted, a post-delete compaction
    is run to materialise the deletion vectors.

    Args:
        uri: Dataset URI.
        config: TTL configuration.
        cutoff: The precomputed expiration cutoff. Rows with a timestamp strictly before this are deleted.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        A result dictionary with keys ``uri``, ``rows_deleted``, ``compacted``, ``tier``, and optionally
        ``skipped``.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        validate_timestamp_column(config.timestamp_column, dataset.schema)
    except (FileNotFoundError, ValueError, OSError) as exc:
        logger.warning("ttl: cannot open dataset %s, skipping: %s", uri, exc)
        telemetry.incr("ttl.dataset_open_error")
        return {"uri": uri, "rows_deleted": 0, "compacted": False, "tier": "small", "skipped": str(exc)}
    except KeyError as exc:
        logger.warning("ttl: timestamp column missing in %s, skipping: %s", uri, exc)
        telemetry.incr("ttl.dataset_column_missing")
        return {"uri": uri, "rows_deleted": 0, "compacted": False, "tier": "small", "skipped": str(exc)}

    predicate: str = build_ttl_predicate(config.timestamp_column, cutoff)

    def action() -> int:
        """Re-open the dataset and execute the delete on the latest version.

        Returns:
            The number of rows deleted.
        """
        fresh: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        delete_result: dict[str, Any] = fresh.delete(
            predicate,
            conflict_retries=config.commit_retries,
        )
        return int(delete_result.get("num_deleted_rows", 0))

    with telemetry.timed("ttl.delete_ms", tags=[f"uri:{uri}"]):
        rows_deleted: int = commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("ttl.commit_conflict"),
        )

    telemetry.distribution("ttl.rows_deleted", float(rows_deleted))
    if rows_deleted:
        telemetry.incr("ttl.dataset_expired")

    compacted: bool = False
    if config.compact_after_delete and rows_deleted:
        compact_dataset_after_delete(uri, config, telemetry)
        compacted = True

    return {"uri": uri, "rows_deleted": rows_deleted, "compacted": compacted, "tier": "small", "skipped": ""}


def classify_or_expire(uri: str, config: TTLConfig, cutoff: datetime, telemetry: Telemetry) -> dict[str, Any]:
    """Classify a dataset by size and expire small ones in-process; flag large ones for tier B.

    Datasets at or below :attr:`TTLConfig.small_tier_fragment_threshold` fragments are expired in process by
    :func:`expire_one_dataset`. Datasets above the threshold are returned with ``tier="large"`` so the driver
    can schedule them as a dedicated tier-B pass.

    Args:
        uri: Dataset URI.
        config: TTL configuration.
        cutoff: The precomputed expiration cutoff.
        telemetry: Telemetry facade for the current executor process.

    Returns:
        The expiration result dict, or ``{"uri", "tier": "large", "fragments"}`` for large datasets.
    """
    try:
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        fragments: int = int(dataset.stats.dataset_stats()["num_fragments"])
    except Exception as exc:
        logger.warning("ttl: cannot stat dataset %s, skipping: %s", uri, exc)
        telemetry.incr("ttl.dataset_stat_error")
        return {"uri": uri, "rows_deleted": 0, "compacted": False, "tier": "small", "skipped": str(exc)}

    if fragments > config.small_tier_fragment_threshold:
        telemetry.incr("ttl.dataset_deferred_to_large_tier")
        return {"uri": uri, "tier": "large", "fragments": fragments}

    return expire_one_dataset(uri, config, cutoff, telemetry)


def ttl_fan_out(
    spark: SparkSession,
    uris: list[str],
    telemetry_config: TelemetryConfig,
    per_dataset: Callable[[str, Telemetry], dict[str, Any]],
    partitions: int,
) -> list[dict[str, Any]]:
    """Fan a per-dataset TTL operation out across Spark executors, one task per partition.

    Each executor partition creates its own :class:`lance_etl.telemetry.Telemetry` facade and applies
    ``per_dataset`` to every URI assigned to it. The pattern mirrors
    :func:`lance_etl.compaction.fan_out_per_dataset`.

    Args:
        spark: Active Spark session.
        uris: Dataset URIs to process.
        telemetry_config: Telemetry configuration, the only serialised telemetry object.
        per_dataset: Callable applied to one URI with an executor-local telemetry facade.
        partitions: Upper bound on Spark partitions, capped at the URI count.

    Returns:
        One outcome dictionary per dataset.
    """

    def partition(part: Iterable[str]) -> Iterator[dict[str, Any]]:
        """Apply ``per_dataset`` to one partition of dataset URIs on an executor.

        Args:
            part: Dataset URIs assigned to this executor task.

        Yields:
            One outcome dictionary per dataset.
        """
        executor_telemetry: Telemetry = Telemetry.create(telemetry_config)
        for uri in part:
            yield per_dataset(uri, executor_telemetry)

    return spark.sparkContext.parallelize(uris, min(len(uris), partitions)).mapPartitions(partition).collect()


class TTLJob:
    """Expires rows from a fleet of Lance datasets by event-timestamp age."""

    def __init__(self, config: TTLConfig) -> None:
        """Initialize the TTL job.

        Args:
            config: TTL configuration.
        """
        self.config: TTLConfig = config

    def run(
        self,
        spark: SparkSession,
        dataset_uris: Iterable[str] | None = None,
        base_uri: str | None = None,
    ) -> TTLReport:
        """Expire rows from every target dataset whose event timestamp is older than ``now - retention``.

        When :attr:`TTLConfig.enabled` is ``False`` this method logs and returns a zero report without opening any
        dataset. When ``enabled`` is ``True`` and ``retention`` is zero (``timedelta(0)``) the method raises
        ``ValueError`` to prevent accidental bulk deletion.

        Dataset enumeration: supply either ``dataset_uris`` (an explicit list) or ``base_uri`` (discover all
        ``.lance`` datasets under that prefix via :func:`lance_etl.cloud_storage.discover_datasets`). Exactly one
        must be provided when enabled is ``True``.

        Tier-A execution fans out across executors, classifying each dataset by fragment count. Datasets at or
        below :attr:`TTLConfig.small_tier_fragment_threshold` are expired in process on the executor. Datasets above
        the threshold are returned to the driver and fanned out in a dedicated tier-B pass with a separate Spark
        partition budget.

        Args:
            spark: Active Spark session. The driver creates telemetry and dispatches both tiers through the Spark
                context. All Lance I/O runs on executors.
            dataset_uris: Explicit dataset URIs to process, or ``None`` to discover from ``base_uri``.
            base_uri: Root prefix under which Lance datasets are discovered, or ``None`` when ``dataset_uris`` is
                provided.

        Returns:
            A :class:`TTLReport` summarising datasets scanned, rows deleted, and datasets compacted.

        Raises:
            ValueError: If ``enabled`` is ``True`` and ``retention`` is ``timedelta(0)``, or if neither
                ``dataset_uris`` nor ``base_uri`` is provided when enabled.
        """
        config: TTLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)

        if not config.enabled:
            logger.info("ttl: disabled (TTLConfig.enabled=False); returning zero report without touching any dataset")
            driver_telemetry.incr("ttl.run_noop")
            return TTLReport(
                datasets_scanned=0,
                datasets_expired=0,
                total_rows_deleted=0,
                datasets_compacted=0,
                datasets_skipped=0,
                enabled=False,
            )

        if config.retention <= timedelta(0):
            raise ValueError(
                "TTLConfig.retention must be a positive timedelta when enabled=True. "
                "A zero or negative retention would delete all rows."
            )

        uris: list[str]
        if dataset_uris is not None:
            uris = list(dataset_uris)
        elif base_uri is not None:
            uris = discover_datasets(base_uri, config.storage_options)
        else:
            raise ValueError("Provide either dataset_uris or base_uri when TTLConfig.enabled=True.")

        if not uris:
            logger.info("ttl: no datasets to process")
            return TTLReport(
                datasets_scanned=0,
                datasets_expired=0,
                total_rows_deleted=0,
                datasets_compacted=0,
                datasets_skipped=0,
                enabled=True,
            )

        cutoff: datetime = compute_cutoff(config.retention)

        with driver_telemetry.span("lance.ttl.run") as run_span:
            run_span.set_tag("dataset_count", len(uris))
            run_span.set_tag("cutoff", cutoff.isoformat())

            logger.info("ttl: processing %d datasets, cutoff=%s", len(uris), cutoff.isoformat())

            with driver_telemetry.timed("ttl.small_tier_ms"):
                outcomes: list[dict[str, Any]] = ttl_fan_out(
                    spark,
                    uris,
                    config.telemetry,
                    lambda uri, tel: classify_or_expire(uri, config, cutoff, tel),
                    config.batch_partitions,
                )

            small_results: list[dict[str, Any]] = [r for r in outcomes if r.get("tier") != "large"]
            large_uris: list[str] = [r["uri"] for r in outcomes if r.get("tier") == "large"]

            run_span.set_tag("small_datasets", len(small_results))
            run_span.set_tag("large_datasets", len(large_uris))
            logger.info(
                "ttl: small tier processed %d datasets; %d deferred to large tier",
                len(small_results),
                len(large_uris),
            )

            large_results: list[dict[str, Any]] = []
            if large_uris:
                with driver_telemetry.timed("ttl.large_tier_ms"):
                    large_results = ttl_fan_out(
                        spark,
                        large_uris,
                        config.telemetry,
                        lambda uri, tel: expire_one_dataset(uri, config, cutoff, tel),
                        config.large_tier_partitions,
                    )

            all_results: list[dict[str, Any]] = small_results + large_results
            datasets_expired: int = sum(1 for r in all_results if int(r.get("rows_deleted", 0)) > 0)
            total_rows: int = sum(int(r.get("rows_deleted", 0)) for r in all_results)
            datasets_compacted: int = sum(1 for r in all_results if r.get("compacted"))
            datasets_skipped: int = sum(1 for r in all_results if r.get("skipped"))

            driver_telemetry.gauge("ttl.datasets_scanned", len(all_results))
            driver_telemetry.gauge("ttl.datasets_expired", datasets_expired)
            driver_telemetry.gauge("ttl.total_rows_deleted", total_rows)
            driver_telemetry.gauge("ttl.datasets_compacted", datasets_compacted)

            logger.info(
                "ttl run: %d datasets scanned, %d expired, %d rows deleted, %d compacted",
                len(all_results),
                datasets_expired,
                total_rows,
                datasets_compacted,
            )

            return TTLReport(
                datasets_scanned=len(all_results),
                datasets_expired=datasets_expired,
                total_rows_deleted=total_rows,
                datasets_compacted=datasets_compacted,
                datasets_skipped=datasets_skipped,
                enabled=True,
            )
