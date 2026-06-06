"""Command-line entry point for the Lance vector pipeline jobs.

Provides three subcommands. ``etl`` reads a time range from an Iceberg table and
routes the changes into per-tenant Lance datasets; backfills are catch-up
replays of this same job over historical windows. ``compact`` runs distributed
compaction over a set of datasets. ``index`` builds the IVF_RQ vector index and
btree scalar indices over a set of datasets.

Each subcommand builds a Spark session, runs the job, and exits non-zero on
failure so an orchestrator can retry.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from pyspark.sql import SparkSession

from arrow_types import resolve_type_map
from iceberg_lance_etl import ETLConfig, IcebergToLanceETL
from lance_compaction import CompactionConfig, LanceCompactor
from lance_indexing import IndexJobConfig, LanceIndexer
from telemetry import (
    LanceRuntimeConfig,
    TelemetryConfig,
    apply_lance_runtime,
    configure_logging,
)

logger: logging.Logger = logging.getLogger(__name__)


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
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)


def parse_key_values(pairs: Optional[Sequence[str]]) -> Dict[str, str]:
    """Parse repeated ``key=value`` arguments into a dictionary.

    Args:
        pairs: The raw ``key=value`` strings, or None.

    Returns:
        A dictionary of the parsed pairs.

    Raises:
        ValueError: If an argument is not in ``key=value`` form.
    """
    result: Dict[str, str] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def build_telemetry_config(args: argparse.Namespace) -> TelemetryConfig:
    """Build a telemetry configuration from common arguments.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The telemetry configuration.
    """
    return TelemetryConfig(
        service=args.dd_service,
        env=args.dd_env,
        version=args.dd_version,
        statsd_host=args.statsd_host,
        statsd_port=args.statsd_port,
        metric_prefix=args.metric_prefix,
        constant_tags=parse_key_values(args.dd_tag),
    )


def build_lance_runtime_config(args: argparse.Namespace) -> LanceRuntimeConfig:
    """Build Lance runtime tuning from common arguments.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The Lance runtime configuration.
    """
    return LanceRuntimeConfig(
        cpu_threads=args.lance_cpu_threads,
        io_threads=args.lance_io_threads,
        io_buffer_size_bytes=args.lance_io_buffer_size,
        lance_log=args.lance_log,
        lance_tracing=args.lance_tracing,
    )


def load_dataset_uris(args: argparse.Namespace) -> List[str]:
    """Collect dataset URIs from arguments and an optional file.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The list of dataset URIs.

    Raises:
        ValueError: If no dataset URIs are provided.
    """
    uris: List[str] = list(args.dataset_uri or [])
    if args.datasets_file:
        with open(args.datasets_file, encoding="utf-8") as handle:
            uris.extend(line.strip() for line in handle if line.strip())
    if not uris:
        raise ValueError("no dataset URIs provided")
    return uris


def run_etl(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the ETL subcommand.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: ETLConfig = ETLConfig(
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        key_col=args.key_col,
        org_col=args.org_col,
        tenant_col=args.tenant_col,
        namespace_col=args.namespace_col,
        vectors_col=args.vectors_col,
        metadata_col=args.metadata_col,
        ts_col=args.ts_col,
        op_col=args.op_col,
        delete_op_values=list(args.delete_op_value or ["delete", "DELETE", "d"]),
        column_types=resolve_type_map(parse_key_values(args.column_type)),
        storage_options=parse_key_values(args.storage_option) or None,
        num_partitions=args.num_partitions,
        conflict_retries=args.conflict_retries,
        guard_updates_by_ts=args.guard_updates_by_ts,
        iceberg_read_options=parse_key_values(args.iceberg_option),
    )
    IcebergToLanceETL(config).run(
        spark, args.table, parse_epoch_ms(args.start), parse_epoch_ms(args.end)
    )


def run_compact(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the compaction subcommand.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: CompactionConfig = CompactionConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_key_values(args.storage_option) or None,
        target_rows_per_fragment=args.target_rows_per_fragment,
        max_rows_per_group=args.max_rows_per_group,
        max_bytes_per_file=args.max_bytes_per_file,
        materialize_deletions=not args.no_materialize_deletions,
        materialize_deletions_threshold=args.materialize_deletions_threshold,
        defer_index_remap=not args.no_defer_index_remap,
        num_threads=args.num_threads,
        batch_size=args.batch_size,
        compaction_mode=args.compaction_mode,
        max_tasks=args.max_tasks,
        run_cleanup=not args.no_cleanup,
        cleanup_older_than_seconds=args.cleanup_older_than_seconds,
        retain_versions=args.retain_versions,
        commit_retries=args.commit_retries,
        commit_backoff_seconds=args.commit_backoff_seconds,
    )
    LanceCompactor(config).run(spark, load_dataset_uris(args))


def run_index(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the indexing subcommand.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: IndexJobConfig = IndexJobConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_key_values(args.storage_option) or None,
        vector_column=args.vector_column,
        num_partitions=args.num_partitions,
        num_bits=args.num_bits,
        metric=args.metric,
        distance_type=args.distance_type,
        train_sample_rate=args.train_sample_rate,
        train_max_iters=args.train_max_iters,
        vector_index_name=args.vector_index_name,
        scalar_columns=list(args.scalar_column or []),
        bitmap_columns=list(args.bitmap_column or []),
        text_columns=list(args.text_column or []),
        fts_with_position=args.fts_with_position,
        fts_base_tokenizer=args.fts_base_tokenizer,
        fts_language=args.fts_language,
        fts_lower_case=args.fts_lower_case,
        fts_stem=args.fts_stem,
        fts_remove_stop_words=args.fts_remove_stop_words,
        fts_ascii_folding=args.fts_ascii_folding,
        num_shards=args.num_shards,
        rebuild=args.rebuild,
        reuse_artifacts=not args.no_reuse_artifacts,
        commit_retries=args.commit_retries,
        commit_backoff_seconds=args.commit_backoff_seconds,
    )
    LanceIndexer(config).run(spark, load_dataset_uris(args))


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add telemetry and storage options shared by all subcommands.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dd-service", default="lance-pipeline")
    parser.add_argument("--dd-env", default="prod")
    parser.add_argument("--dd-version", default="")
    parser.add_argument("--statsd-host", default="localhost")
    parser.add_argument("--statsd-port", type=int, default=8125)
    parser.add_argument("--metric-prefix", default="lance.pipeline")
    parser.add_argument("--dd-tag", action="append", help="Constant tag, repeatable")
    parser.add_argument(
        "--storage-option", action="append", help="pylance storage option key=value, repeatable"
    )
    parser.add_argument(
        "--lance-cpu-threads",
        type=int,
        default=None,
        help="LANCE_CPU_THREADS; set below executor cores when tasks share an executor",
    )
    parser.add_argument(
        "--lance-io-threads",
        type=int,
        default=None,
        help="LANCE_IO_THREADS; cloud stores often need 128 or 256",
    )
    parser.add_argument(
        "--lance-io-buffer-size",
        type=int,
        default=None,
        help="LANCE_DEFAULT_IO_BUFFER_SIZE in bytes; raise with the I/O thread count",
    )
    parser.add_argument("--lance-log", default=None, help="LANCE_LOG filter, for example info")
    parser.add_argument(
        "--lance-tracing", default=None, help="LANCE_TRACING level the event bridge observes"
    )


def add_dataset_arguments(parser: argparse.ArgumentParser) -> None:
    """Add dataset-selection options shared by compact and index.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dataset-uri", action="append", help="Dataset URI, repeatable")
    parser.add_argument("--datasets-file", help="File with one dataset URI per line")


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser.

    Returns:
        The parser with the ``etl``, ``compact``, and ``index`` subcommands.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-name", default="lance-pipeline")
    parser.add_argument("--log-level", default="INFO")
    subparsers: argparse._SubParsersAction = parser.add_subparsers(dest="command", required=True)

    etl: argparse.ArgumentParser = subparsers.add_parser("etl", help="Run the Iceberg-to-Lance ETL")
    add_common_arguments(etl)
    etl.add_argument("--table", required=True)
    etl.add_argument("--start", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--end", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--base-uri", required=True)
    etl.add_argument("--key-col", default="vector_id")
    etl.add_argument("--org-col", default="org_id")
    etl.add_argument("--tenant-col", default="tenant_id")
    etl.add_argument("--namespace-col", default="namespace")
    etl.add_argument("--vectors-col", default="vectors")
    etl.add_argument("--metadata-col", default="metadata")
    etl.add_argument("--ts-col", default="timestamp")
    etl.add_argument("--op-col", default="op")
    etl.add_argument("--delete-op-value", action="append")
    etl.add_argument("--column-type", action="append", help="Cast column as name=arrow_type")
    etl.add_argument("--iceberg-option", action="append", help="Iceberg read option key=value")
    etl.add_argument("--num-partitions", type=int, default=512)
    etl.add_argument("--conflict-retries", type=int, default=10)
    etl.add_argument("--guard-updates-by-ts", action="store_true")

    compact: argparse.ArgumentParser = subparsers.add_parser(
        "compact", help="Distributed compaction of Lance datasets"
    )
    add_common_arguments(compact)
    add_dataset_arguments(compact)
    compact.add_argument("--target-rows-per-fragment", type=int, default=None)
    compact.add_argument("--max-rows-per-group", type=int, default=None)
    compact.add_argument("--max-bytes-per-file", type=int, default=None)
    compact.add_argument("--no-materialize-deletions", action="store_true")
    compact.add_argument("--materialize-deletions-threshold", type=float, default=None)
    compact.add_argument("--no-defer-index-remap", action="store_true")
    compact.add_argument("--num-threads", type=int, default=None)
    compact.add_argument("--batch-size", type=int, default=None)
    compact.add_argument("--compaction-mode", default=None)
    compact.add_argument("--max-tasks", type=int, default=256)
    compact.add_argument("--no-cleanup", action="store_true")
    compact.add_argument("--cleanup-older-than-seconds", type=int, default=None)
    compact.add_argument("--retain-versions", type=int, default=None)
    compact.add_argument("--commit-retries", type=int, default=20)
    compact.add_argument("--commit-backoff-seconds", type=float, default=0.5)

    index: argparse.ArgumentParser = subparsers.add_parser(
        "index", help="Build IVF_RQ and btree indices on Lance datasets"
    )
    add_common_arguments(index)
    add_dataset_arguments(index)
    index.add_argument("--vector-column", default=None, help="Vector column to index with IVF_RQ")
    index.add_argument("--num-partitions", type=int, default=None)
    index.add_argument("--num-bits", type=int, default=1)
    index.add_argument("--metric", default="L2")
    index.add_argument("--distance-type", default=None)
    index.add_argument("--train-sample-rate", type=int, default=256)
    index.add_argument("--train-max-iters", type=int, default=50)
    index.add_argument("--vector-index-name", default=None)
    index.add_argument("--scalar-column", action="append", help="Scalar column for a btree index")
    index.add_argument("--bitmap-column", action="append", help="Column for a bitmap index")
    index.add_argument(
        "--text-column", action="append", help="Text column for a full-text BM25 index"
    )
    index.add_argument(
        "--fts-with-position", action="store_true", help="Store token positions for phrase queries"
    )
    index.add_argument("--fts-base-tokenizer", default=None, help="FTS base tokenizer name")
    index.add_argument("--fts-language", default=None, help="FTS stemming and stop-word language")
    index.add_argument("--fts-lower-case", action="store_const", const=True, default=None)
    index.add_argument("--fts-stem", action="store_const", const=True, default=None)
    index.add_argument("--fts-remove-stop-words", action="store_const", const=True, default=None)
    index.add_argument("--fts-ascii-folding", action="store_const", const=True, default=None)
    index.add_argument("--num-shards", type=int, default=64)
    index.add_argument("--rebuild", action="store_true")
    index.add_argument("--no-reuse-artifacts", action="store_true")
    index.add_argument("--commit-retries", type=int, default=20)
    index.add_argument("--commit-backoff-seconds", type=float, default=0.5)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Parse arguments, build a Spark session, and dispatch the subcommand.

    Args:
        argv: Optional argument vector; defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    level: int = logging.getLevelName(args.log_level.upper())
    configure_logging(build_telemetry_config(args), level=level)
    lance_env: Dict[str, str] = apply_lance_runtime(build_lance_runtime_config(args))
    builder = SparkSession.builder.appName(args.app_name)
    for variable_name, variable_value in lance_env.items():
        builder = builder.config(f"spark.executorEnv.{variable_name}", variable_value)
    spark: SparkSession = builder.getOrCreate()
    try:
        if args.command == "etl":
            run_etl(args, spark)
        elif args.command == "compact":
            run_compact(args, spark)
        else:
            run_index(args, spark)
        return 0
    except Exception:
        logger.exception("job failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
