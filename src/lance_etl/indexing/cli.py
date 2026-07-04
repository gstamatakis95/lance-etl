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
    load_dataset_uris,
    parse_storage_options,
)
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


def run(args: argparse.Namespace) -> None:
    """Execute the indexing job from parsed arguments.

    Builds a Spark session, constructs the configuration, and dispatches to
    :class:`~lance_etl.indexing.runner.LanceIndexer`.

    Args:
        args: Parsed command-line arguments.
    """
    uris = load_dataset_uris(args)
    if not uris:
        logger.info("index: no datasets in the URI list, nothing to do")
        return
    spark = build_spark(APP_NAME)
    try:
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
    except Exception:
        logger.exception("indexing job failed")
        raise
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the indexing job.

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
