"""Operator tools CLI: recall audit, namespace migration, and Iceberg source-table optimization.

Uninstalled operator CLI, not registered as a console script in ``pyproject.toml``. Exposes the
``main()`` entry point reachable via ``python -m lance_etl.tools.cli``. Three subcommands are
provided.

``recall`` replays Datadog-sampled vector queries as exact brute-force scans against the dataset
versions that served them and reports recall@k, nDCG@k, and MRR.

``migrate-namespace`` copies a whole namespace to a new namespace name.  Source datasets are never
deleted, so an operator can verify the new namespace and flip serving through the blue-green tag
helpers before removing the source.

``optimize-iceberg`` runs Iceberg's own source-table maintenance procedures
(``rewrite_data_files``, ``rewrite_manifests``, ``expire_snapshots``, and the opt-in
``remove_orphan_files``) on the upstream Iceberg table.  It is distinct from the maintenance
package which optimizes the Lance datasets.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence

from lance_etl.cliutil import (
    add_common_arguments,
    add_index_column_arguments,
    build_spark,
    build_telemetry_config,
    index_config_from_args,
    parse_epoch_ms,
    parse_partition_cols,
    parse_storage_options,
    run_cli_main,
    run_with_spark,
)
from lance_etl.etl import ROUTING_COLS
from lance_etl.iceberg_optimize import (
    DEFAULT_EXPIRE_OLDER_THAN_DAYS,
    DEFAULT_EXPIRE_RETAIN_LAST,
    IcebergOptimizeConfig,
    IcebergOptimizer,
)
from lance_etl.indexing import IndexJobConfig
from lance_etl.migrate_namespace import MigrateConfig, NamespaceMigrator
from lance_etl.recall import DatadogSpanSource, RecallAuditJob, RecallJobConfig

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the tools argument parser with recall, migrate-namespace, and optimize-iceberg subcommands.

    Returns:
        The top-level argument parser for the tools CLI.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=("Operator tools: recall audit, namespace migration, and Iceberg source-table optimization.")
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    recall_parser: argparse.ArgumentParser = subparsers.add_parser(
        "recall",
        help="Audit served recall@k by replaying Datadog-sampled vector queries as exact brute-force scans",
    )
    add_common_arguments(recall_parser)
    recall_parser.add_argument(
        "--from", dest="from_ts", required=True, help="Window start, ISO 8601 or epoch milliseconds"
    )
    recall_parser.add_argument("--to", dest="to_ts", required=True, help="Window end, ISO 8601 or epoch milliseconds")
    recall_parser.add_argument(
        "--base-uri",
        required=True,
        help="Root under which per-tenant datasets live as base/<org>/<tenant>/<namespace>.lance",
    )
    recall_parser.add_argument(
        "--dd-site",
        default="datadoghq.com",
        help="Datadog site domain for the Spans search API. DD_API_KEY and DD_APP_KEY must be in the environment.",
    )
    recall_parser.add_argument(
        "--max-samples", type=int, default=10_000, help="Cap on sampled spans fetched. Default 10000."
    )
    recall_parser.add_argument("--vector-column", default="vector", help="Fixed-size-list vector column to scan")

    migrate_namespace_parser: argparse.ArgumentParser = subparsers.add_parser(
        "migrate-namespace",
        help=(
            "Copy a whole namespace to a new namespace name. Source datasets are never deleted. "
            "One-off operator tool — not a scheduled task."
        ),
    )
    add_common_arguments(migrate_namespace_parser)
    migrate_namespace_parser.add_argument(
        "--source-namespace", required=True, help="Namespace component value to copy from."
    )
    migrate_namespace_parser.add_argument(
        "--target-namespace", required=True, help="Namespace component value to copy to."
    )
    migrate_namespace_parser.add_argument(
        "--base-uri", required=True, help="Root URI under which per-tenant datasets live."
    )
    migrate_namespace_parser.add_argument(
        "--partition-by",
        default=None,
        help=(
            "Comma-separated columns whose values build each dataset path in order. "
            "Default: org_id,tenant_id,namespace."
        ),
    )
    migrate_namespace_parser.add_argument(
        "--no-recompact",
        action="store_true",
        help="Skip compaction of target datasets after copying.",
    )
    migrate_namespace_parser.add_argument(
        "--no-reindex",
        action="store_true",
        help="Skip index rebuild on target datasets after copying.",
    )
    migrate_namespace_parser.add_argument(
        "--overwrite-target",
        action="store_true",
        help="Allow overwriting target datasets that already exist. Default: fail if any target exists.",
    )
    add_index_column_arguments(migrate_namespace_parser)

    optimize_iceberg_parser: argparse.ArgumentParser = subparsers.add_parser(
        "optimize-iceberg",
        help=(
            "Optimize the upstream Iceberg source table via CALL maintenance procedures (rewrite_data_files, "
            "rewrite_manifests, expire_snapshots, and the opt-in remove_orphan_files). Distinct from the Lance "
            "maintenance subcommand which optimizes the Lance datasets."
        ),
    )
    add_common_arguments(optimize_iceberg_parser)
    optimize_iceberg_parser.add_argument(
        "--table", required=True, help="Fully-qualified Iceberg source table: catalog.namespace.table."
    )
    optimize_iceberg_parser.add_argument(
        "--no-rewrite-data-files", action="store_true", help="Skip the bin-pack rewrite of small data files."
    )
    optimize_iceberg_parser.add_argument(
        "--no-rewrite-manifests", action="store_true", help="Skip the manifest rewrite."
    )
    optimize_iceberg_parser.add_argument(
        "--expire-snapshots",
        action="store_true",
        help="Expire snapshot history only after the durable source retention gate authorizes it.",
    )
    optimize_iceberg_parser.add_argument(
        "--remove-orphan-files",
        action="store_true",
        help=(
            "Delete files no live snapshot references. Opt-in and destructive. Only files older than Iceberg's safety "
            "horizon are removed."
        ),
    )
    optimize_iceberg_parser.add_argument(
        "--expire-retain-last",
        type=int,
        default=DEFAULT_EXPIRE_RETAIN_LAST,
        help=f"Snapshots always retained regardless of age. Default: {DEFAULT_EXPIRE_RETAIN_LAST}.",
    )
    optimize_iceberg_parser.add_argument(
        "--expire-older-than-days",
        type=int,
        default=DEFAULT_EXPIRE_OLDER_THAN_DAYS,
        help=f"Age horizon in days for snapshot expiration. Default: {DEFAULT_EXPIRE_OLDER_THAN_DAYS}.",
    )

    return parser


def run_recall(args: argparse.Namespace) -> None:
    """Execute the recall-audit subcommand.

    Builds a Spark session and dispatches to :class:`~lance_etl.recall.RecallAuditJob`.

    Args:
        args: Parsed command-line arguments.
    """
    spark = build_spark()

    def work() -> None:
        """Build the config and run the recall audit."""
        config: RecallJobConfig = RecallJobConfig(
            base_uri=args.base_uri,
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            vector_column=args.vector_column,
            max_samples=args.max_samples,
        )
        source: DatadogSpanSource = DatadogSpanSource(site=args.dd_site)
        RecallAuditJob(config).run(spark, source, parse_epoch_ms(args.from_ts), parse_epoch_ms(args.to_ts))

    run_with_spark(spark, "recall", logger, work)


def run_migrate_namespace(args: argparse.Namespace) -> int:
    """Execute the namespace-migration subcommand.

    Copies every dataset whose namespace component equals ``--source-namespace`` to the same
    address with the namespace component replaced by ``--target-namespace``.  Source datasets are
    never deleted. A target that fails to recompact or reindex in isolation is excluded from the
    report's compacted/indexed counts and tallied instead in the count this function returns, so
    :func:`~lance_etl.cliutil.run_cli_main` maps a partial failure to
    :data:`~lance_etl.cliutil.EXIT_PARTIAL_FAILURE` instead of exit ``0``.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of targets that failed to optimize in isolation, ``0`` when every target
        succeeded.
    """
    index_config: IndexJobConfig | None = None
    has_index_columns: bool = bool(
        args.vector_column or args.scalar_column or args.bitmap_column or args.zonemap_column or args.text_column
    )
    if has_index_columns:
        index_config = index_config_from_args(args, build_telemetry_config(args), parse_storage_options(args))
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
    spark = build_spark()

    def work() -> int:
        """Run the namespace migration, log its report, and return the failed-target count."""
        report = NamespaceMigrator(config).run(spark)
        logger.info("migrate-namespace report: %s", report)
        return report.failed

    return run_with_spark(spark, "migrate-namespace", logger, work)


def run_optimize_iceberg(args: argparse.Namespace) -> None:
    """Execute the Iceberg source-table optimization subcommand.

    Builds a Spark session and dispatches to :class:`~lance_etl.iceberg_optimize.IcebergOptimizer`.

    Args:
        args: Parsed command-line arguments.
    """
    spark = build_spark()

    def work() -> None:
        """Build the config, run the Iceberg optimization, and log its report."""
        config: IcebergOptimizeConfig = IcebergOptimizeConfig(
            table=args.table,
            telemetry=build_telemetry_config(args),
            rewrite_data_files=not args.no_rewrite_data_files,
            rewrite_manifests=not args.no_rewrite_manifests,
            expire_snapshots=args.expire_snapshots,
            remove_orphan_files=args.remove_orphan_files,
            expire_retain_last=args.expire_retain_last,
            expire_older_than_days=args.expire_older_than_days,
        )
        report = IcebergOptimizer(config).run(spark)
        logger.info("optimize-iceberg report: %s", report)

    run_with_spark(spark, "optimize-iceberg", logger, work)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate tools subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    runners: dict[str, Callable[[argparse.Namespace], int | None]] = {
        "recall": run_recall,
        "migrate-namespace": run_migrate_namespace,
        "optimize-iceberg": run_optimize_iceberg,
    }
    return run_cli_main(build_parser(), runners, argv)


if __name__ == "__main__":
    raise SystemExit(main())
