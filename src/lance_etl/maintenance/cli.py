"""Maintenance job CLI: per-dataset TTL expiration, compaction, version cleanup, and manifest migration.

Exposes the ``main()`` entry point consumed by the ``lance-etl-maintenance`` script and
``python -m lance_etl.maintenance``.  Three subcommands are provided.

``run`` applies maintenance to a fleet of datasets: per-row TTL expiration (when ``--ttl-column``
names a per-row TTL column), unified distributed compaction, and version cleanup in that order.

``tag`` flips a serving tag (default ``HEAD``) to a target dataset version for blue-green
promotion.  With no ``--tag-version`` the tag is moved to each dataset's latest version.

``migrate-manifests`` migrates every selected dataset's manifest paths to the V2 naming scheme
so subsequent opens cost one object-store request instead of a version-count-proportional LIST.
The migration is not transactional: run it only with the targeted datasets quiesced.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from lance_etl.cliutil import (
    add_common_arguments,
    add_dataset_arguments,
    build_spark,
    build_telemetry_config,
    configure_logging_from_args,
    load_dataset_uris,
    parse_storage_options,
)
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
    run_parser.add_argument(
        "--ttl-column",
        default=None,
        help=(
            "Per-row TTL column holding each row's lifetime as an Arrow Duration. When set, rows are expired before "
            "compaction by the predicate ts-column + ttl-column < now. Absent (the default) turns TTL off."
        ),
    )
    run_parser.add_argument(
        "--ts-column",
        default="event_timestamp",
        help=(
            "Event timestamp column used as the TTL clock. Must match ETLConfig.ts_col. Only used when --ttl-column "
            "is set. Default: event_timestamp."
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


def run_run(args: argparse.Namespace) -> None:
    """Execute the maintenance run subcommand.

    Loads dataset URIs, builds the config, and dispatches to
    :class:`~lance_etl.maintenance.job.MaintenanceJob`.

    Args:
        args: Parsed command-line arguments.
    """
    spark = build_spark()
    uris: list[str] = load_dataset_uris(args, spark)
    if not uris:
        logger.info("maintenance run: no datasets in the URI list, nothing to do")
        spark.stop()
        return
    try:
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            ttl_column=args.ttl_column,
            ts_column=args.ts_column,
        )
        MaintenanceJob(config).run(spark, uris)
    except Exception:
        logger.exception("maintenance run failed")
        raise
    finally:
        spark.stop()


def run_tag(args: argparse.Namespace) -> None:
    """Execute the serving-tag subcommand.

    Flips the configured tag across all selected datasets and logs the safe operational sequence.

    Args:
        args: Parsed command-line arguments.
    """
    spark = build_spark()
    try:
        update_serving_tags(
            spark,
            load_dataset_uris(args, spark),
            build_telemetry_config(args),
            parse_storage_options(args),
            tag=args.tag,
            target_version=args.tag_version,
        )
    except Exception:
        logger.exception("maintenance tag failed")
        raise
    finally:
        spark.stop()


def run_migrate_manifests(args: argparse.Namespace) -> None:
    """Execute the manifest-migration subcommand.

    Migrates every selected dataset to the V2 manifest naming scheme.

    Args:
        args: Parsed command-line arguments.
    """
    spark = build_spark()
    try:
        migrate_manifest_paths(
            spark,
            load_dataset_uris(args, spark),
            build_telemetry_config(args),
            parse_storage_options(args),
        )
    except Exception:
        logger.exception("maintenance migrate-manifests failed")
        raise
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate maintenance subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    configure_logging_from_args(args)
    runners = {
        "run": run_run,
        "tag": run_tag,
        "migrate-manifests": run_migrate_manifests,
    }
    try:
        runners[args.command](args)
        return 0
    except Exception:
        return 1
