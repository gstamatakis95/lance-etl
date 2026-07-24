"""Operator tools CLI: recall audit and Iceberg source-table optimization.

Uninstalled operator CLI, not registered as a console script in ``pyproject.toml``. Exposes the
``main()`` entry point reachable via ``python -m lance_etl.tools.cli``. Two subcommands are
provided.

``recall`` replays Datadog-sampled vector queries as exact brute-force scans against the dataset
versions that served them and reports recall@k, nDCG@k, and MRR.

``optimize-iceberg`` runs Iceberg's own source-table maintenance procedures
(``rewrite_data_files``, ``rewrite_manifests``, and the opt-in ``remove_orphan_files``) on the
upstream Iceberg table. It is distinct from the maintenance package which optimizes the Lance
datasets. Snapshot expiration is intentionally not an operator option because only the durable
PostgreSQL control plane can prove the oldest source snapshot unfinished work still needs.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence

from lance_etl.cliutil import (
    add_common_arguments,
    build_spark,
    build_telemetry_config,
    parse_epoch_ms,
    parse_storage_options,
    run_cli_main,
    run_with_spark,
)
from lance_etl.iceberg_optimize import IcebergOptimizeConfig, IcebergOptimizer
from lance_etl.recall import DatadogSpanSource, RecallAuditJob, RecallJobConfig

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the tools argument parser with recall and optimize-iceberg subcommands.

    Returns:
        The top-level argument parser for the tools CLI.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Operator tools: recall audit and Iceberg source-table optimization."
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

    optimize_iceberg_parser: argparse.ArgumentParser = subparsers.add_parser(
        "optimize-iceberg",
        help=(
            "Optimize the upstream Iceberg source table via CALL maintenance procedures (rewrite_data_files, "
            "rewrite_manifests and the opt-in remove_orphan_files). Snapshot expiration stays behind the durable "
            "PostgreSQL source-retention gate. Distinct from the Lance "
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
        "--remove-orphan-files",
        action="store_true",
        help=(
            "Delete files no live snapshot references. Opt-in and destructive. Only files older than Iceberg's safety "
            "horizon are removed."
        ),
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
            remove_orphan_files=args.remove_orphan_files,
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
    runners: dict[str, Callable[[argparse.Namespace], None]] = {
        "recall": run_recall,
        "optimize-iceberg": run_optimize_iceberg,
    }
    return run_cli_main(build_parser(), runners, argv)


if __name__ == "__main__":
    raise SystemExit(main())
