"""Indexing job CLI: build IVF_RQ, BTREE, BITMAP, and FTS indices on Lance datasets.

Exposes the ``index`` argument parser and the ``main()`` entry point consumed by the
``lance-etl-index`` script and ``python -m lance_etl.indexing``.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from lance_etl.cliutil import (
    APP_NAME,
    add_common_arguments,
    add_dataset_arguments,
    add_index_column_arguments,
    build_spark,
    build_telemetry_config,
    configure_logging_from_args,
    index_config_from_args,
    load_uris_or_none,
    parse_storage_options,
    resolve_exit_code,
    run_with_spark,
)
from lance_etl.fanout import count_failed
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.indexing.runner import LanceIndexer

logger: logging.Logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the indexing argument parser.

    Returns:
        The argument parser for the indexing subcommand.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            "Build IVF_RQ vector, btree scalar, bitmap, and full-text BM25 indices on Lance datasets. "
            "The CLI selects which columns get which index type and the data-shape knobs that cannot be "
            "defaulted. Training parameters, shard counts, the vector row floor, delta and retrain bounds, "
            "and fine-grained FTS tokenizer toggles take their opinionated IndexJobConfig defaults."
        )
    )
    parser.add_argument("--log-level", default="INFO")
    add_common_arguments(parser)
    add_dataset_arguments(parser)
    add_index_column_arguments(parser)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Reindex every fragment instead of only uncovered ones. Use after tokenizer or parameter changes.",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    """Execute the indexing job from parsed arguments.

    Builds a Spark session, constructs the configuration, and dispatches to
    :class:`~lance_etl.indexing.runner.LanceIndexer`. A single dataset's index failure is isolated
    into an error marker by the fleet phases rather than aborting the run, so this returns the
    count of failed datasets for the operator alert instead of raising.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The number of datasets that failed in isolation, ``0`` when all succeeded.
    """
    spark = build_spark(APP_NAME)

    def work() -> int:
        """Load the fleet, run the indexer, and return the failed-dataset count."""
        uris: list[str] | None = load_uris_or_none(args, spark, "index", logger)
        if uris is None:
            return 0
        config: IndexJobConfig = index_config_from_args(
            args,
            build_telemetry_config(args),
            parse_storage_options(args),
            rebuild=args.rebuild,
        )
        failed: int = count_failed(LanceIndexer(config).run(spark, uris))
        if failed:
            logger.warning("indexing job: %d datasets failed and will be retried next run", failed)
        return failed

    return run_with_spark(spark, "indexing job", logger, work)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the indexing job.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code: ``0`` when every dataset succeeded, ``1`` when the run itself raised
        an unhandled exception, and :data:`~lance_etl.cliutil.EXIT_PARTIAL_FAILURE` (``3``) when
        the run completed but one or more datasets failed in isolation and will be retried by the
        next scheduled run.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    configure_logging_from_args(args)
    try:
        return resolve_exit_code(run(args))
    except Exception:
        return 1
