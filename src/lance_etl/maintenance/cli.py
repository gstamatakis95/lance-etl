"""Maintenance job CLI: per-dataset TTL expiration, compaction, version cleanup, and manifest migration.

Exposes the ``main()`` entry point consumed by the ``lance-etl-maintenance`` script and
``python -m lance_etl.maintenance``.  Three subcommands are provided.

``run`` applies maintenance to a fleet of datasets: per-row TTL expiration (when ``--ttl-column``
names a per-row TTL column), unified distributed compaction, and version cleanup in that order.
``--cluster-rewrite`` opts a run into an occasional, operator-triggered full rewrite that reorders
same-centroid rows into shared fragments before normal compaction (ADR 0041); it requires the
targeted datasets quiesced (no concurrent ETL writer) and is not exposed on the pipeline CLI.

``tag`` flips a serving tag (default ``HEAD``) to a target dataset version for blue-green
promotion.  With no ``--tag-version`` the tag is moved to each dataset's latest version.

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
    add_ttl_arguments,
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
            "Run maintenance operations on Lance datasets: per-row TTL expiration, "
            "distributed compaction, version cleanup, serving-tag promotion, and V2 manifest migration."
        )
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser: argparse.ArgumentParser = subparsers.add_parser(
        "run",
        help=(
            "Per-dataset maintenance: per-row TTL expiration (when --ttl-column is set), "
            "unified distributed compaction, and version cleanup, in that order."
        ),
    )
    add_common_arguments(run_parser)
    add_dataset_arguments(run_parser)
    add_ttl_arguments(run_parser)
    run_parser.add_argument(
        "--cluster-rewrite",
        action="store_true",
        help=(
            "Opt-in occasional, operator-triggered full rewrite that reorders rows so same-centroid rows share "
            "fragments (ADR 0041). Subsumes normal compaction for the datasets it rewrites. Non-transactional: "
            "requires the targeted datasets quiesced, with no concurrent ETL writer, for the duration of the run."
        ),
    )
    run_parser.add_argument(
        "--cluster-column",
        default=None,
        help=(
            "Explicit vector column to cluster on. Defaults to the single vector-role column stored in the "
            "dataset's column roles; ambiguous or missing roles skip the dataset back into normal compaction."
        ),
    )
    run_parser.add_argument(
        "--cluster-serve-tag",
        action="store_true",
        help=(
            "Advance the HEAD serving tag blue-green after a clustered rewrite's vector index rebuild commits. "
            "Off by default; the pipeline's stamp phase is the normal promotion path."
        ),
    )

    tag_parser: argparse.ArgumentParser = subparsers.add_parser(
        "tag",
        help=(
            "Flip a serving tag (default 'HEAD') to a target dataset version for blue-green promotion. "
            "Tagged versions are exempt from version cleanup."
        ),
    )
    add_common_arguments(tag_parser)
    add_dataset_arguments(tag_parser)
    tag_parser.add_argument("--tag", default="HEAD", help="Serving tag name to update. Default: HEAD.")
    tag_parser.add_argument(
        "--tag-version",
        type=int,
        default=None,
        help="Target dataset version for the tag. Omit to point the tag at each dataset's latest version.",
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
            ttl_column=args.ttl_column,
            ts_column=args.ts_column,
            cluster_rewrite=args.cluster_rewrite,
            cluster_column=args.cluster_column,
            cluster_serve_tag=args.cluster_serve_tag,
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
                tags=[args.tag],
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
