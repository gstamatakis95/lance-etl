"""Maintenance job CLI: per-dataset retention expiry, compaction, version cleanup, and manifest migration.

Uninstalled operator CLI, not registered as a console script in ``pyproject.toml``. Exposes the
``main()`` entry point reachable via ``python -m lance_etl.maintenance.cli``. Three subcommands are
provided.

``run`` applies maintenance to a fleet of datasets: retention expiry (when ``--retention-seconds``
sets a window), unified distributed compaction, and version cleanup in that order.
``tag`` flips ``HEAD`` to an explicit target dataset version for blue-green promotion.

``migrate-manifests`` migrates every selected dataset's manifest paths to the V2 naming scheme
so subsequent opens cost one object-store request instead of a version-count-proportional LIST.
The migration is not transactional: run it only with the targeted datasets quiesced.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence

from lance_etl.cliutil import (
    add_common_arguments,
    add_dataset_arguments,
    add_retention_arguments,
    build_spark,
    build_telemetry_config,
    load_dataset_uris,
    load_uris_or_none,
    parse_storage_options,
    run_cli_main,
    run_with_spark,
)
from lance_etl.fanout import count_failed
from lance_etl.maintenance.job import MaintenanceConfig, MaintenanceJob
from lance_etl.maintenance.tools import migrate_manifest_paths, update_serving_tags

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the maintenance argument parser with run, tag, and migrate-manifests subcommands.

    Returns:
        The top-level argument parser for the maintenance CLI.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            "Run maintenance operations on Lance datasets: retention expiry, "
            "distributed compaction, version cleanup, serving-tag promotion, and V2 manifest migration."
        )
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser: argparse.ArgumentParser = subparsers.add_parser(
        "run",
        help=(
            "Per-dataset maintenance: retention expiry (when --retention-seconds is set), "
            "unified distributed compaction, and version cleanup, in that order."
        ),
    )
    add_common_arguments(run_parser)
    add_dataset_arguments(run_parser)
    add_retention_arguments(run_parser)
    tag_parser: argparse.ArgumentParser = subparsers.add_parser(
        "tag",
        help=(
            "Flip HEAD to an explicit target dataset version for blue-green promotion. "
            "Tagged versions are exempt from version cleanup."
        ),
    )
    add_common_arguments(tag_parser)
    add_dataset_arguments(tag_parser)
    tag_parser.add_argument(
        "--tag-version",
        type=int,
        required=True,
        help="Exact target dataset version for HEAD.",
    )

    migrate_manifests_parser: argparse.ArgumentParser = subparsers.add_parser(
        "migrate-manifests",
        help=(
            "Migrate existing datasets' manifest paths to the V2 naming scheme (one object-store request per open). "
            "Not transactional: run only with the targeted datasets quiesced."
        ),
    )
    add_common_arguments(migrate_manifests_parser)
    add_dataset_arguments(migrate_manifests_parser)

    return parser


def run_run(args: argparse.Namespace) -> int:
    """Execute the maintenance run subcommand.

    Loads dataset URIs, builds the config, and dispatches to
    :class:`~lance_etl.maintenance.job.MaintenanceJob`. A single dataset's failure is isolated
    into an error marker by the fleet phases rather than aborting the run, so this returns the
    count of failed datasets for the operator alert instead of raising.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of datasets that failed in isolation, ``0`` when all succeeded.
    """
    spark = build_spark()

    def work() -> int:
        """Load the fleet, run maintenance, and return the failed-dataset count."""
        uris: list[str] | None = load_uris_or_none(args, spark, "maintenance run", logger)
        if uris is None:
            return 0
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            retention_seconds=args.retention_seconds,
            ts_column=args.ts_column,
        )
        failed: int = count_failed(MaintenanceJob(config).run(spark, uris))
        if failed:
            logger.warning("maintenance run: %d datasets failed and will be retried next run", failed)
        return failed

    return run_with_spark(spark, "maintenance run", logger, work)


def run_tag(args: argparse.Namespace) -> int:
    """Execute the serving-tag subcommand.

    Flips the configured tag across all selected datasets and logs the safe operational sequence.
    A single dataset's tag flip failing is isolated into an error marker by the fleet fan-out.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of datasets whose tag flip failed in isolation, ``0`` when all succeeded.
    """
    spark = build_spark()

    def work() -> int:
        """Flip serving tags across the fleet and return the failed-dataset count."""
        failed: int = count_failed(
            update_serving_tags(
                spark,
                load_dataset_uris(args, spark),
                build_telemetry_config(args),
                parse_storage_options(args),
                tags=["HEAD"],
                target_version=args.tag_version,
            )
        )
        if failed:
            logger.warning("maintenance tag: %d datasets failed the tag flip", failed)
        return failed

    return run_with_spark(spark, "maintenance tag", logger, work)


def run_migrate_manifests(args: argparse.Namespace) -> int:
    """Execute the manifest-migration subcommand.

    Migrates every selected dataset to the V2 manifest naming scheme. A single dataset's migration
    failure is isolated into an error marker by the fleet fan-out.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of datasets whose migration failed in isolation, ``0`` when all succeeded.
    """
    spark = build_spark()

    def work() -> int:
        """Migrate manifest paths across the fleet and return the failed-dataset count."""
        failed: int = count_failed(
            migrate_manifest_paths(
                spark,
                load_dataset_uris(args, spark),
                build_telemetry_config(args),
                parse_storage_options(args),
            )
        )
        if failed:
            logger.warning("maintenance migrate-manifests: %d datasets failed to migrate", failed)
        return failed

    return run_with_spark(spark, "maintenance migrate-manifests", logger, work)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate maintenance subcommand.

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
        "tag": run_tag,
        "migrate-manifests": run_migrate_manifests,
    }
    return run_cli_main(build_parser(), runners, argv)


if __name__ == "__main__":
    raise SystemExit(main())
