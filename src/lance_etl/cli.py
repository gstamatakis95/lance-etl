"""Command-line entry point for the Lance vector pipeline jobs.

The CLI is deliberately small and opinionated. It exposes only the arguments that are genuinely
per-deployment: the data and identity contract (which table, which window, where datasets live, which
Datadog service) and what to build (which index types, the distance metric, and the FTS base tokenizer
and language). Every tuning knob — shuffle partitions, retry budgets, commit backoff, compaction fragment
sizing, IVF training parameters, the fine-grained FTS tokenizer toggles (including position storage), and
two-tier thresholds — is set to a sensible opinionated default in the configuration dataclasses
(:class:`lance_etl.etl.ETLConfig`, :class:`lance_etl.indexing.IndexJobConfig`,
:class:`lance_etl.maintenance.MaintenanceConfig`). Those fields stay tunable in code, just not from the
command line.

Provides the following subcommands. ``etl`` reads a time range from an Iceberg table and routes the
changes into per-tenant Lance datasets. Routing always uses the fixed trio ``org_id``, ``tenant_id``,
``namespace`` (see ``ROUTING_COLS`` in :mod:`lance_etl.etl`). The source table's map columns (``vectors``,
``texts``, ``metadata``) are pivoted dynamically: every key present in the data becomes a concrete column
with no per-field declaration required on the command line. When ``--changed-uris-path`` is supplied the
driver writes one changed-dataset URI per line to that object-store path after each run so downstream
maintenance and index tasks can scope their work to only the datasets touched in the window. Backfills are
catch-up replays of this same job over historical windows. ``maintenance`` runs per-dataset maintenance
over a set of datasets: per-row TTL expiration (when a TTL column is named), two-tier distributed
compaction, and version cleanup, in that order. ``index`` builds IVF_RQ vector, btree scalar, bitmap, and
full-text BM25 indices over a set of datasets. ``recall`` replays Datadog-sampled vector queries as exact
brute-force scans against the dataset versions that served them and reports recall@k. ``tag`` flips a
serving tag (default ``HEAD``) to a target dataset version for blue-green promotion. ``migrate-manifests``
migrates dataset manifest paths to the V2 naming scheme. ``migrate-namespace`` copies a whole namespace to
a new namespace name. It is a one-off operator tool and is not scheduled. ``optimize-iceberg`` runs
Iceberg's own source-table maintenance procedures (``rewrite_data_files``, ``rewrite_manifests``,
``expire_snapshots``, and the opt-in ``remove_orphan_files``) on the upstream Iceberg table. It is
distinct from ``maintenance``, which optimizes the Lance datasets.

Dataset-URI sources for ``maintenance``, ``index``, ``tag``, and ``migrate-manifests``: a single run may
combine ``--dataset-uri`` (individual URIs), ``--datasets-file`` (one URI per line), and ``--base-uri``
(recursive fleet discovery under a root). At least one source must be supplied or ``load_dataset_uris``
raises ``ValueError``. A ``--datasets-file`` that exists but is empty contributes no URIs and is not an
error. Idle-window state files written by the ETL are therefore safe to pass: an empty file is a clean
no-op for maintenance and index tasks.

Each subcommand builds a Spark session, runs the job, and exits non-zero on failure so an orchestrator
can retry.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lance_etl.cloud_storage import discover_datasets
from lance_etl.etl import ROUTING_COLS, ETLConfig, IcebergToLanceETL
from lance_etl.iceberg_optimize import (
    DEFAULT_EXPIRE_OLDER_THAN_DAYS,
    DEFAULT_EXPIRE_RETAIN_LAST,
    IcebergOptimizeConfig,
    IcebergOptimizer,
)
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.maintenance import (
    MaintenanceConfig,
    MaintenanceJob,
    migrate_manifest_paths,
    update_serving_tags,
)
from lance_etl.migrate_namespace import MigrateConfig, NamespaceMigrator
from lance_etl.recall import DatadogSpanSource, RecallAuditJob, RecallJobConfig
from lance_etl.telemetry import TelemetryConfig, configure_logging

logger: logging.Logger = logging.getLogger(__name__)

APP_NAME: str = "lance-pipeline"
"""Opinionated Spark application name shared by every subcommand."""


def parse_epoch_ms(value: str) -> int:
    """Parse an ISO 8601 timestamp or epoch milliseconds into epoch ms.

    Args:
        value: An ISO 8601 string or integer milliseconds.

    Returns:
        The instant as epoch milliseconds.
    """
    try:
        return int(value)
    except ValueError:
        parsed: datetime = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp() * 1000)


def parse_key_values(pairs: Sequence[str] | None) -> dict[str, str]:
    """Parse repeated ``key=value`` arguments into a dictionary.

    Args:
        pairs: The raw ``key=value`` strings, or None.

    Returns:
        A dictionary of the parsed pairs.

    Raises:
        ValueError: If an argument is not in ``key=value`` form.
    """
    result: dict[str, str] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def parse_storage_options(args: argparse.Namespace) -> dict[str, str] | None:
    """Parse the repeated ``--storage-option`` arguments shared by every subcommand.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The storage options for pylance, or None when none were given.
    """
    return parse_key_values(args.storage_option) or None


def parse_partition_cols(value: str | None) -> list[str] | None:
    """Parse the comma-separated ``--partition-by`` column list used by ``migrate-namespace``.

    Args:
        value: The raw flag value, or None when the flag is absent.

    Returns:
        The column names in dataset-path order, or None when the flag is absent so the
        configuration default applies.

    Raises:
        ValueError: If the flag is present but lists no columns.
    """
    if value is None:
        return None
    columns: list[str] = [part.strip() for part in value.split(",") if part.strip()]
    if not columns:
        raise ValueError(f"--partition-by must list at least one column, got {value!r}")
    return columns


def build_telemetry_config(args: argparse.Namespace) -> TelemetryConfig:
    """Build a telemetry configuration from the identity arguments.

    The DogStatsD host and port and the metric prefix are not exposed on the CLI: they take the opinionated
    :class:`TelemetryConfig` defaults (``localhost:8125`` and ``lance.pipeline``).

    Args:
        args: Parsed command-line arguments.

    Returns:
        The telemetry configuration.
    """
    kv: dict[str, str] = parse_key_values(args.dd_tag)
    return TelemetryConfig(
        service=args.dd_service,
        env=args.dd_env,
        version=args.dd_version,
        constant_tags=[f"{k}:{v}" for k, v in kv.items()],
    )


def load_dataset_uris(args: argparse.Namespace) -> list[str]:
    """Collect dataset URIs from arguments, an optional file, and base-URI discovery.

    Three sources are combined in order: explicit ``--dataset-uri`` flags, a ``--datasets-file`` (one URI per
    line), and recursive discovery under ``--base-uri``. A ``--datasets-file`` that exists but is empty
    contributes no URIs and is not an error, so an idle-window state file written by the ETL passes
    through cleanly. Callers that receive an empty list should treat it as a no-op rather than raise.

    When ``--base-uri`` is supplied, every ``*.lance`` dataset under it is discovered recursively at any
    depth, so the standard three-level ``org_id/tenant_id/namespace`` layout and any deeper
    ``migrate-namespace`` hierarchies are both picked up.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The list of dataset URIs, which may be empty when all sources are empty.

    Raises:
        ValueError: If neither ``--datasets-file`` nor ``--base-uri`` nor ``--dataset-uri`` was supplied
            at all (configuration error), distinguished from the case where all sources were supplied but
            happened to produce no URIs.
    """
    has_any_source: bool = bool(args.dataset_uri or args.datasets_file or args.base_uri)
    if not has_any_source:
        raise ValueError(
            "no dataset URI source configured: supply at least one of --dataset-uri, --datasets-file, or --base-uri"
        )
    uris: list[str] = list(args.dataset_uri or [])
    if args.datasets_file:
        with open(args.datasets_file, encoding="utf-8") as handle:
            uris.extend(line.strip() for line in handle if line.strip())
    if args.base_uri:
        uris.extend(discover_datasets(args.base_uri, parse_storage_options(args)))
    return uris


def run_etl(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the ETL subcommand.

    Only the data and identity contract is taken from the CLI. Routing uses the fixed trio
    ``org_id``, ``tenant_id``, ``namespace`` encoded in ``ROUTING_COLS``. The source table's
    map columns (``vectors``, ``texts``, ``metadata``) are pivoted dynamically: every key present
    in the data becomes a concrete column with no per-field CLI declaration. The key/timestamp/op
    column names, the delete-op encodings, the map column names, the window column, shuffle
    partition count, conflict-retry budget, and retry timeout take their opinionated
    :class:`ETLConfig` defaults.

    When ``--changed-uris-path`` is supplied the ETL driver writes one changed-dataset URI per line
    to that object-store path after each run. The file is always written (even when no datasets
    changed) so downstream maintenance and index tasks can scope their work to the datasets the ETL
    actually touched in this window.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: ETLConfig = ETLConfig(
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        iceberg_read_options=parse_key_values(args.iceberg_option),
        window_start=args.window_start,
        window_end=args.window_end,
        changed_uris_path=args.changed_uris_path,
    )
    IcebergToLanceETL(config).run(spark, args.table, parse_epoch_ms(args.start), parse_epoch_ms(args.end))


def run_maintenance(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the maintenance subcommand: per-row TTL expiration, compaction, and version cleanup.

    Maintenance has no per-deployment data contract beyond which datasets to process and the optional
    TTL columns. When ``--ttl-column`` names a per-row TTL (``Duration``) column, expired rows are
    deleted before compaction by the predicate ``ts_column + ttl_column < now``. Absent the flag,
    TTL is off and the job is compaction plus cleanup. Fragment sizing, deletion materialization,
    two-tier thresholds, retry budgets, and version-cleanup retention take their opinionated
    :class:`MaintenanceConfig` defaults.

    An empty dataset list (for example from an idle-window state file) is a clean no-op: the job
    logs and returns without touching any datasets.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    uris: list[str] = load_dataset_uris(args)
    if not uris:
        logger.info("maintenance: no datasets in the URI list, nothing to do")
        return
    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        ttl_column=args.ttl_column,
        ts_column=args.ts_column,
    )
    MaintenanceJob(config).run(spark, uris)


def run_index(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the indexing subcommand.

    The CLI selects what to build (which columns get which index type) and the data-shape knobs that cannot be
    defaulted (the distance metric, the FTS base tokenizer, and the FTS language). IVF training parameters, partition
    counts, shard counts, the vector row floor, delta and retrain bounds, retry budgets, and the fine-grained FTS
    tokenizer toggles (lower-case, stemming, stop-word removal, ASCII folding, and position storage) take their
    opinionated :class:`IndexJobConfig` defaults. ``--rebuild`` remains as the operational escape hatch for tokenizer
    or parameter changes that need a full reindex.

    An empty dataset list (for example from an idle-window state file) is a clean no-op: the job logs and returns
    without touching any datasets.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    uris: list[str] = load_dataset_uris(args)
    if not uris:
        logger.info("index: no datasets in the URI list, nothing to do")
        return
    config: IndexJobConfig = IndexJobConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        vector_columns=list(args.vector_column or []),
        metric=args.metric,
        scalar_columns=list(args.scalar_column or []),
        bitmap_columns=list(args.bitmap_column or []),
        text_columns=list(args.text_column or []),
        fts_base_tokenizer=args.fts_base_tokenizer,
        fts_language=args.fts_language,
        rebuild=args.rebuild,
    )
    LanceIndexer(config).run(spark, uris)


def run_recall(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the recall-audit subcommand.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: RecallJobConfig = RecallJobConfig(
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        id_column=args.id_column,
        vector_column=args.vector_column,
        max_samples=args.max_samples,
        batch_size=args.batch_size,
    )
    source: DatadogSpanSource = DatadogSpanSource(site=args.dd_site)
    RecallAuditJob(config).run(spark, source, parse_epoch_ms(args.from_ts), parse_epoch_ms(args.to_ts))


def run_tag(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the serving-tag subcommand for blue-green promotion.

    Flips a serving tag (default ``HEAD``) to a target dataset version across the selected datasets. With no
    ``--tag-version`` the tag is moved to each dataset's latest version. The helper logs the safe operational
    sequence and never assumes the serving layer auto-refreshes on a tag move.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    update_serving_tags(
        spark,
        load_dataset_uris(args),
        build_telemetry_config(args),
        parse_storage_options(args),
        tag=args.tag,
        target_version=args.tag_version,
    )


def run_migrate_manifests(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the manifest-path migration subcommand.

    Migrates every selected dataset's manifest paths to the V2 naming scheme so subsequent opens cost one object-store
    request instead of a version-count-proportional LIST. The migration is not transactional, so run it only with the
    targeted datasets quiesced (no concurrent ingestion, compaction, or indexing).

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    migrate_manifest_paths(
        spark,
        load_dataset_uris(args),
        build_telemetry_config(args),
        parse_storage_options(args),
    )


def run_migrate_namespace(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the namespace-migration subcommand.

    Copies every dataset whose namespace component equals ``--source-namespace`` to the same address
    with the namespace component replaced by ``--target-namespace``. The source datasets are never
    deleted, so an operator can verify the new namespace and flip serving through the blue-green tag
    helpers before removing the source.

    Recompaction and reindexing run in the same pipeline order as production (compact, then index).
    Pass ``--no-recompact`` or ``--no-reindex`` to skip the respective step. If no index column flags
    are provided, ``index`` is left as ``None`` and reindexing is skipped with a warning from the
    migrator. Pass ``--overwrite-target`` to allow clobbering existing target datasets.

    The ``--partition-by`` flag is specific to this subcommand: it overrides the default
    ``org_id,tenant_id,namespace`` routing for fleets where a custom hierarchy was written. The ETL
    itself always uses the fixed ``ROUTING_COLS`` trio and does not expose this flag.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    index_config: IndexJobConfig | None = None
    has_index_columns: bool = bool(args.vector_column or args.scalar_column or args.bitmap_column or args.text_column)
    if has_index_columns:
        index_config = IndexJobConfig(
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            vector_columns=list(args.vector_column or []),
            metric=args.metric,
            scalar_columns=list(args.scalar_column or []),
            bitmap_columns=list(args.bitmap_column or []),
            text_columns=list(args.text_column or []),
            fts_base_tokenizer=args.fts_base_tokenizer,
            fts_language=args.fts_language,
            rebuild=False,
        )
    partition_cols: list[str] | None = parse_partition_cols(args.partition_by)
    config: MigrateConfig = MigrateConfig(
        source_namespace=args.source_namespace,
        target_namespace=args.target_namespace,
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        partition_cols=partition_cols or list(ROUTING_COLS),
        recompact=not args.no_recompact,
        reindex=not args.no_reindex,
        overwrite_target=args.overwrite_target,
        index=index_config,
    )
    report = NamespaceMigrator(config).run(spark)
    logger.info("migrate-namespace report: %s", report)


def run_optimize_iceberg(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the Iceberg source-table optimization subcommand.

    Optimizes the upstream Iceberg source table via Iceberg's own ``CALL <catalog>.system.<procedure>`` maintenance
    procedures: ``rewrite_data_files`` bin-packs small files, ``rewrite_manifests`` realigns manifests,
    ``expire_snapshots`` prunes snapshot history, and the opt-in ``remove_orphan_files`` deletes unreferenced files.
    This is distinct from the ``maintenance`` subcommand, which optimizes the per-tenant Lance datasets. The catalog
    is supplied through the Spark configuration at submit time exactly like the ``etl`` subcommand's reads. Step toggles
    and the bin-pack and orphan-file sizing take their opinionated :class:`IcebergOptimizeConfig` defaults.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(
        table=args.table,
        telemetry=build_telemetry_config(args),
        rewrite_data_files=not args.no_rewrite_data_files,
        rewrite_manifests=not args.no_rewrite_manifests,
        expire_snapshots=not args.no_expire_snapshots,
        remove_orphan_files=args.remove_orphan_files,
        expire_retain_last=args.expire_retain_last,
        expire_older_than_days=args.expire_older_than_days,
    )
    report = IcebergOptimizer(config).run(spark)
    logger.info("optimize-iceberg report: %s", report)


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the identity and storage options shared by all subcommands.

    These are the only cross-cutting per-deployment arguments: the Datadog service/env/version tags, repeatable
    constant tags, and pylance storage options. DogStatsD host/port and metric prefix are not exposed; they take their
    opinionated configuration defaults and can be tuned in code.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dd-service", default="lance-pipeline")
    parser.add_argument("--dd-env", default="prod")
    parser.add_argument("--dd-version", default="")
    parser.add_argument("--dd-tag", action="append", help="Constant tag key=value, repeatable")
    parser.add_argument("--storage-option", action="append", help="pylance storage option key=value, repeatable")


def add_dataset_arguments(parser: argparse.ArgumentParser) -> None:
    """Add dataset-selection options shared by maintenance, index, tag, and migrate-manifests.

    Three sources may be combined: ``--dataset-uri`` for individual URIs, ``--datasets-file`` for a
    newline-delimited URI list, and ``--base-uri`` for recursive fleet discovery. At least one must be
    supplied.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dataset-uri", action="append", help="Dataset URI, repeatable")
    parser.add_argument("--datasets-file", help="File with one dataset URI per line")
    parser.add_argument(
        "--base-uri",
        default=None,
        help=(
            "Discover datasets recursively under this URI: every *.lance path at any depth is included, "
            "covering the standard org_id/tenant_id/namespace three-level layout."
        ),
    )


def add_index_column_arguments(parser: argparse.ArgumentParser) -> None:
    """Add index column-selection flags shared by the ``index`` and ``migrate-namespace`` subcommands.

    These flags select which columns receive which index type. When no flags are given the indexer
    builds no handlers and the step is a no-op (or skipped with a warning in the migrator).

    ``--vector-column`` is repeatable: each use appends one column name to the list of vector columns that
    receive an IVF_RQ index. Multiple vector columns are supported when a dataset carries more than one
    embedding (for example a dense vector and a sparse vector).

    The ``fts_with_position`` field (whether token positions are stored for phrase queries) is a
    tokenizer-schema contract: changing it requires a full ``--rebuild`` and must be set in
    :class:`~lance_etl.indexing.IndexJobConfig` in code rather than toggled per invocation.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--vector-column", action="append", help="Vector column to index with IVF_RQ, repeatable")
    parser.add_argument("--metric", default="L2", help="Vector distance metric: L2, cosine, or dot")
    parser.add_argument("--scalar-column", action="append", help="Scalar column for a btree index")
    parser.add_argument("--bitmap-column", action="append", help="Column for a bitmap index")
    parser.add_argument("--text-column", action="append", help="Text column for a full-text BM25 index")
    parser.add_argument("--fts-base-tokenizer", default=None, help="FTS base tokenizer name")
    parser.add_argument("--fts-language", default=None, help="FTS stemming and stop-word language")


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser.

    Returns:
        The parser with the ``etl``, ``maintenance``, ``index``, ``recall``, ``tag``, ``migrate-manifests``,
        ``migrate-namespace``, and ``optimize-iceberg`` subcommands.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    etl: argparse.ArgumentParser = subparsers.add_parser("etl", help="Run the Iceberg-to-Lance ETL")
    add_common_arguments(etl)
    etl.add_argument("--table", required=True)
    etl.add_argument("--start", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--end", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--base-uri", required=True)
    etl.add_argument("--iceberg-option", action="append", help="Iceberg read option key=value")
    etl.add_argument(
        "--window-start",
        default=None,
        help=(
            "ISO-8601 lower bound (inclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read.  Absent means the lower bound is open (no filter)."
        ),
    )
    etl.add_argument(
        "--window-end",
        default=None,
        help=(
            "ISO-8601 upper bound (exclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read.  Absent means the upper bound is open (no filter)."
        ),
    )
    etl.add_argument(
        "--changed-uris-path",
        default=None,
        help=(
            "Object-store path where the ETL driver writes one changed-dataset URI per line after each run. "
            "The file is always written (even when empty) so downstream maintenance and index tasks can scope "
            "their work to the datasets actually touched in this window. Absent means the list is not written."
        ),
    )

    maintenance: argparse.ArgumentParser = subparsers.add_parser(
        "maintenance",
        help=(
            "Per-dataset maintenance: per-row TTL expiration (when --ttl-column is set), two-tier distributed "
            "compaction, and version cleanup, in that order."
        ),
    )
    add_common_arguments(maintenance)
    add_dataset_arguments(maintenance)
    maintenance.add_argument(
        "--ttl-column",
        default=None,
        help=(
            "Per-row TTL column holding each row's lifetime as an Arrow Duration. When set, rows are expired before "
            "compaction by the predicate ts-column + ttl-column < now. Absent (the default) turns TTL off."
        ),
    )
    maintenance.add_argument(
        "--ts-column",
        default="event_timestamp",
        help=(
            "Event timestamp column used as the TTL clock. Must match ETLConfig.ts_col. Only used when --ttl-column "
            "is set. Default: event_timestamp."
        ),
    )

    index: argparse.ArgumentParser = subparsers.add_parser(
        "index", help="Build IVF_RQ vector, btree scalar, bitmap, and full-text BM25 indices on Lance datasets"
    )
    add_common_arguments(index)
    add_dataset_arguments(index)
    add_index_column_arguments(index)
    index.add_argument(
        "--rebuild",
        action="store_true",
        help="Reindex every fragment instead of only uncovered ones. Use after tokenizer or parameter changes.",
    )

    recall: argparse.ArgumentParser = subparsers.add_parser(
        "recall", help="Audit served recall@k by replaying Datadog-sampled vector queries as exact brute-force scans"
    )
    add_common_arguments(recall)
    recall.add_argument("--from", dest="from_ts", required=True, help="Window start, ISO 8601 or epoch milliseconds")
    recall.add_argument("--to", dest="to_ts", required=True, help="Window end, ISO 8601 or epoch milliseconds")
    recall.add_argument(
        "--base-uri",
        required=True,
        help="Root under which per-tenant datasets live as base/<org>/<tenant>/<namespace>.lance",
    )
    recall.add_argument(
        "--dd-site",
        default="datadoghq.com",
        help="Datadog site domain for the Spans search API. DD_API_KEY and DD_APP_KEY must be in the environment.",
    )
    recall.add_argument("--max-samples", type=int, default=10_000, help="Cap on sampled spans fetched. Default 10000.")
    recall.add_argument("--id-column", default="vector_id", help="Unique id column matched against served result ids")
    recall.add_argument("--vector-column", default="vector", help="Fixed-size-list vector column to scan")
    recall.add_argument("--batch-size", type=int, default=8192, help="Scanner batch size for the brute-force scan")

    tag: argparse.ArgumentParser = subparsers.add_parser(
        "tag",
        help=(
            "Flip a serving tag (default 'HEAD') to a target dataset version for blue-green promotion. Tagged "
            "versions are exempt from version cleanup."
        ),
    )
    add_common_arguments(tag)
    add_dataset_arguments(tag)
    tag.add_argument("--tag", default="HEAD", help="Serving tag name to update. Default: HEAD.")
    tag.add_argument(
        "--tag-version",
        type=int,
        default=None,
        help="Target dataset version for the tag. Omit to point the tag at each dataset's latest version.",
    )

    migrate_manifests: argparse.ArgumentParser = subparsers.add_parser(
        "migrate-manifests",
        help=(
            "Migrate existing datasets' manifest paths to the V2 naming scheme (one object-store request per open). "
            "Not transactional: run only with the targeted datasets quiesced."
        ),
    )
    add_common_arguments(migrate_manifests)
    add_dataset_arguments(migrate_manifests)

    migrate_namespace: argparse.ArgumentParser = subparsers.add_parser(
        "migrate-namespace",
        help=(
            "Copy a whole namespace to a new namespace name. Source datasets are never deleted. "
            "One-off operator tool — not a scheduled task."
        ),
    )
    add_common_arguments(migrate_namespace)
    migrate_namespace.add_argument("--source-namespace", required=True, help="Namespace component value to copy from.")
    migrate_namespace.add_argument("--target-namespace", required=True, help="Namespace component value to copy to.")
    migrate_namespace.add_argument("--base-uri", required=True, help="Root URI under which per-tenant datasets live.")
    migrate_namespace.add_argument(
        "--partition-by",
        default=None,
        help=(
            "Comma-separated columns whose values build each dataset path in order. "
            "Default: org_id,tenant_id,namespace."
        ),
    )
    migrate_namespace.add_argument(
        "--no-recompact",
        action="store_true",
        help="Skip compaction of target datasets after copying.",
    )
    migrate_namespace.add_argument(
        "--no-reindex",
        action="store_true",
        help="Skip index rebuild on target datasets after copying.",
    )
    migrate_namespace.add_argument(
        "--overwrite-target",
        action="store_true",
        help="Allow overwriting target datasets that already exist. Default: fail if any target exists.",
    )
    add_index_column_arguments(migrate_namespace)

    optimize_iceberg: argparse.ArgumentParser = subparsers.add_parser(
        "optimize-iceberg",
        help=(
            "Optimize the upstream Iceberg source table via CALL maintenance procedures (rewrite_data_files, "
            "rewrite_manifests, expire_snapshots, and the opt-in remove_orphan_files). Distinct from the Lance "
            "'maintenance' subcommand which optimizes the Lance datasets."
        ),
    )
    add_common_arguments(optimize_iceberg)
    optimize_iceberg.add_argument(
        "--table", required=True, help="Fully-qualified Iceberg source table: catalog.namespace.table."
    )
    optimize_iceberg.add_argument(
        "--no-rewrite-data-files", action="store_true", help="Skip the bin-pack rewrite of small data files."
    )
    optimize_iceberg.add_argument("--no-rewrite-manifests", action="store_true", help="Skip the manifest rewrite.")
    optimize_iceberg.add_argument(
        "--no-expire-snapshots", action="store_true", help="Skip snapshot-history expiration."
    )
    optimize_iceberg.add_argument(
        "--remove-orphan-files",
        action="store_true",
        help=(
            "Delete files no live snapshot references. Opt-in and destructive. Only files older than Iceberg's safety "
            "horizon are removed."
        ),
    )
    optimize_iceberg.add_argument(
        "--expire-retain-last",
        type=int,
        default=DEFAULT_EXPIRE_RETAIN_LAST,
        help=f"Snapshots always retained regardless of age. Default: {DEFAULT_EXPIRE_RETAIN_LAST}.",
    )
    optimize_iceberg.add_argument(
        "--expire-older-than-days",
        type=int,
        default=DEFAULT_EXPIRE_OLDER_THAN_DAYS,
        help=f"Age horizon in days for snapshot expiration. Default: {DEFAULT_EXPIRE_OLDER_THAN_DAYS}.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, build a Spark session, and dispatch the subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    level: int = logging.getLevelName(args.log_level.upper())
    configure_logging(build_telemetry_config(args), level=level)
    spark: SparkSession = SparkSession.builder.appName(APP_NAME).getOrCreate()
    runners: dict[str, Callable[[argparse.Namespace, SparkSession], None]] = {
        "etl": run_etl,
        "maintenance": run_maintenance,
        "index": run_index,
        "recall": run_recall,
        "tag": run_tag,
        "migrate-manifests": run_migrate_manifests,
        "migrate-namespace": run_migrate_namespace,
        "optimize-iceberg": run_optimize_iceberg,
    }
    try:
        runners[args.command](args, spark)
        return 0
    except Exception:
        logger.exception("job failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
