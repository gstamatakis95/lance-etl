"""Unified pipeline CLI: interval-tag pruning, compaction, indexing, and interval-tag stamping.

Exposes the ``main()`` entry point consumed by the ``lance-etl-pipeline`` script and
``python -m lance_etl.pipeline``.  One subcommand is provided.

``run`` executes the full four-phase pipeline over a fleet of datasets: prune old interval
tags, TTL expiration and unified compaction, unified index builds, and finally write an
interval tag on every dataset that completed successfully.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from typing import Any

from lance_etl.cliutil import (
    add_common_arguments,
    add_dataset_arguments,
    add_index_column_arguments,
    add_ttl_arguments,
    build_spark,
    build_telemetry_config,
    index_config_from_args,
    load_uris_or_none,
    parse_storage_options,
    parse_window_tag,
    run_cli_main,
    run_with_spark,
)
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.maintenance.job import MaintenanceConfig
from lance_etl.pipeline.job import PipelineConfig, PipelineJob
from lance_etl.telemetry import TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the pipeline argument parser with a ``run`` subcommand.

    Returns:
        The top-level argument parser for the pipeline CLI.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            "Run the unified lance-etl pipeline: prune old interval tags, per-dataset TTL expiration "
            "and compaction, index builds, and interval-tag stamping."
        )
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers: Any = parser.add_subparsers(dest="command", required=True)

    run_parser: argparse.ArgumentParser = subparsers.add_parser(
        "run",
        help=(
            "Execute all pipeline phases: prune interval tags, maintenance (TTL + compaction + cleanup), "
            "indexing, and optional interval-tag stamping."
        ),
    )
    add_common_arguments(run_parser)
    add_dataset_arguments(run_parser)
    add_index_column_arguments(run_parser)
    add_ttl_arguments(run_parser)
    run_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Reindex every fragment instead of only uncovered ones. Use after tokenizer or parameter changes.",
    )
    run_parser.add_argument(
        "--tag-stamp",
        default=None,
        type=parse_window_tag,
        help=(
            "ISO 8601 datetime (e.g. '2026-06-11 12:00:00+00:00') converted to a colon-free UTC interval tag "
            "name (e.g. '20260611T120000Z') and written after a successful run. Omit to skip the stamp phase."
        ),
    )
    run_parser.add_argument(
        "--tag-keep-last",
        type=int,
        default=48,
        help=(
            "Number of newest interval tags to retain when pruning. Default: 48 (two days at hourly cadence). "
            "Pass 0 to disable pruning entirely."
        ),
    )
    return parser


def run_run(args: argparse.Namespace) -> int:
    """Execute the pipeline run subcommand.

    Loads dataset URIs, builds both sub-configurations, constructs a
    :class:`~lance_etl.pipeline.job.PipelineConfig`, and dispatches to
    :class:`~lance_etl.pipeline.job.PipelineJob`. Per-dataset failures are isolated by the fleet
    phases rather than aborting the run, so this returns the pipeline's ``counts["failed"]`` count
    for the operator alert instead of raising.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of datasets that failed in isolation, ``0`` when all succeeded.
    """
    spark: Any = build_spark()

    def work() -> int:
        """Load the fleet, run the four pipeline phases, and return the failed-dataset count."""
        uris: list[str] | None = load_uris_or_none(args, spark, "pipeline run", logger)
        if uris is None:
            return 0
        tag_keep_last: int | None = args.tag_keep_last if args.tag_keep_last != 0 else None
        maintenance_config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            ttl_column=args.ttl_column,
            ts_column=args.ts_column,
        )
        indexing_config: IndexJobConfig = index_config_from_args(args, TelemetryConfig(), rebuild=args.rebuild)
        config: PipelineConfig = PipelineConfig(
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            maintenance=maintenance_config,
            indexing=indexing_config,
            tag_keep_last=tag_keep_last,
            tag_stamp=args.tag_stamp,
        )
        result: dict[str, Any] = PipelineJob(config).run(spark, uris)
        failed: int = int(result["counts"]["failed"])
        if failed:
            logger.warning("pipeline run: %d datasets failed and will be retried next run", failed)
        return failed

    return run_with_spark(spark, "pipeline run", logger, work)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate pipeline subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code: ``0`` when every dataset succeeded, ``1`` when the run itself raised
        an unhandled exception, and :data:`~lance_etl.cliutil.EXIT_PARTIAL_FAILURE` (``3``) when
        the run completed but one or more datasets failed in isolation and will be retried by the
        next scheduled run.
    """
    runners: dict[str, Callable[[argparse.Namespace], int | None]] = {
        "run": run_run,
    }
    return run_cli_main(build_parser(), runners, argv)
