"""ETL job CLI: Iceberg-to-Lance incremental routing.

Exposes the ``etl`` argument parser and the ``main()`` entry point consumed by the
``lance-etl-etl`` script and ``python -m lance_etl.etl``.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from lance_etl.cliutil import (
    APP_NAME,
    add_common_arguments,
    build_spark,
    build_telemetry_config,
    configure_logging_from_args,
    parse_epoch_ms,
    parse_hour_tag,
    parse_key_values,
    parse_storage_options,
)
from lance_etl.etl.job import IcebergToLanceETL
from lance_etl.etl.pivot import ETLConfig

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the ETL argument parser.

    Returns:
        The argument parser for the ETL subcommand.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            "Run the Iceberg-to-Lance ETL: read a time range from an Iceberg table and route the changes "
            "into per-tenant Lance datasets. Routing uses the fixed trio org_id, tenant_id, namespace. "
            "Map columns (vectors, texts, metadata) are pivoted dynamically."
        )
    )
    parser.add_argument("--log-level", default="INFO")
    add_common_arguments(parser)
    parser.add_argument("--table", required=True, help="Fully-qualified Iceberg source table.")
    parser.add_argument("--start", required=True, help="Window start: ISO 8601 or epoch milliseconds.")
    parser.add_argument("--end", required=True, help="Window end: ISO 8601 or epoch milliseconds.")
    parser.add_argument("--base-uri", required=True, help="Root URI under which per-tenant datasets live.")
    parser.add_argument("--iceberg-option", action="append", help="Iceberg read option key=value, repeatable.")
    parser.add_argument(
        "--window-start",
        default=None,
        help=(
            "ISO-8601 lower bound (inclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read. Absent means the lower bound is open (no filter)."
        ),
    )
    parser.add_argument(
        "--window-end",
        default=None,
        help=(
            "ISO-8601 upper bound (exclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read. Absent means the upper bound is open (no filter)."
        ),
    )
    parser.add_argument(
        "--tag-stamp",
        default=None,
        type=parse_hour_tag,
        help=(
            "ISO-8601 datetime whose truncated hour names the interval tag stamped on every dataset this run "
            "writes (format %%Y%%m%%dT%%H%%M%%SZ). A later run in the same hour moves the tag to the newest "
            "version. Tagged versions are exempt from version cleanup until the pipeline prunes old interval "
            "tags. Absent disables stamping."
        ),
    )
    parser.add_argument(
        "--spark-batches",
        type=int,
        default=1,
        help=(
            "Split the increment into this many sequential Spark-level key-hash batches, each processed as its "
            "own Spark job over a fraction of the rows. Raise this for very large increments (tens of millions "
            "of rows per org) so executor memory needs scale with the batch size instead of the increment size. "
            "Default 1 processes the whole increment in a single pass."
        ),
    )
    return parser


def run(args: argparse.Namespace) -> None:
    """Execute the ETL job from parsed arguments.

    Builds a Spark session, constructs the configuration, and dispatches to
    :class:`~lance_etl.etl.job.IcebergToLanceETL`.

    Args:
        args: Parsed command-line arguments.
    """

    spark = build_spark(APP_NAME)
    try:
        config: ETLConfig = ETLConfig(
            base_uri=args.base_uri,
            telemetry=build_telemetry_config(args),
            storage_options=parse_storage_options(args),
            iceberg_read_options=parse_key_values(args.iceberg_option),
            window_start=args.window_start,
            window_end=args.window_end,
            spark_batches=args.spark_batches,
            tag_stamp=args.tag_stamp,
        )
        IcebergToLanceETL(config).run(spark, args.table, parse_epoch_ms(args.start), parse_epoch_ms(args.end))
    except Exception:
        logger.exception("etl job failed")
        raise
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the ETL job.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    configure_logging_from_args(args)
    try:
        run(args)
        return 0
    except Exception:
        return 1
