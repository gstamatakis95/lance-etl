"""Command-line entry point for the Lance vector pipeline jobs.

Provides three subcommands. ``etl`` reads a time range from an Iceberg table and routes the changes into per-tenant
Lance datasets; backfills are catch-up replays of this same job over historical windows. ``compact`` runs distributed
compaction over a set of datasets. ``index`` builds IVF_RQ vector, btree scalar, bitmap, and full-text BM25 indices over
a set of datasets.

Each subcommand builds a Spark session, runs the job, and exits non-zero on failure so an orchestrator can retry.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from lance_etl.arrow_types import resolve_type_map
from lance_etl.cloud_storage import discover_datasets
from lance_etl.compaction import CompactionConfig, LanceCompactor
from lance_etl.etl import DEFAULT_PARTITION_COLS, ETLConfig, IcebergToLanceETL, PartitionDerivation
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.telemetry import (
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
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp() * 1000)


def parse_key_values(pairs: Sequence[str] | None) -> dict[str, str]:
    """Parse repeated ``key=value`` arguments into a dictionary.

    Args:
        pairs: The raw ``key=value`` strings, or None.

    Returns:
        A dictionary of the parsed pairs.

    Raises:
        ValueError: If an argument is not in ``key=value`` form.
    """
    result: dict[str, str] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def parse_partition_cols(value: str | None) -> list[str] | None:
    """Parse the comma-separated ``--partition-by`` column list.

    Args:
        value: The raw flag value, or None when the flag is absent.

    Returns:
        The column names in dataset-path order, or None when the flag is absent so the configuration default applies.

    Raises:
        ValueError: If the flag is present but lists no columns.
    """
    if value is None:
        return None
    columns: list[str] = [part.strip() for part in value.split(",") if part.strip()]
    if not columns:
        raise ValueError(f"--partition-by must list at least one column, got {value!r}")
    return columns


def parse_partition_derivations(specs: Sequence[str] | None) -> list[PartitionDerivation]:
    """Parse repeated ``--partition-derive NAME=SOURCE:FORMAT`` arguments.

    ``FORMAT`` is a Python strftime pattern (supported directives ``%Y %y %m %d %H %M %S %j %%``) translated to
    Spark's ``date_format`` pattern, for example ``event_date=processing_timestamp:%Y-%m-%d``.

    Args:
        specs: The raw ``NAME=SOURCE:FORMAT`` strings, or None.

    Returns:
        One :class:`PartitionDerivation` per argument.

    Raises:
        ValueError: If an argument is not in ``NAME=SOURCE:FORMAT`` form or any part is empty.
    """
    result: list[PartitionDerivation] = []
    for spec in specs or []:
        if "=" not in spec:
            raise ValueError(f"expected NAME=SOURCE:FORMAT, got {spec!r}")
        name, rest = spec.split("=", 1)
        if ":" not in rest:
            raise ValueError(f"expected NAME=SOURCE:FORMAT, got {spec!r}")
        source_col, strftime_format = rest.split(":", 1)
        if not name or not source_col or not strftime_format:
            raise ValueError(f"expected NAME=SOURCE:FORMAT with non-empty parts, got {spec!r}")
        result.append(PartitionDerivation(name=name, source_col=source_col, strftime_format=strftime_format))
    return result


def build_telemetry_config(args: argparse.Namespace) -> TelemetryConfig:
    """Build a telemetry configuration from common arguments.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The telemetry configuration.
    """
    kv: dict[str, str] = parse_key_values(args.dd_tag)
    return TelemetryConfig(
        service=args.dd_service,
        env=args.dd_env,
        version=args.dd_version,
        statsd_host=args.statsd_host,
        statsd_port=args.statsd_port,
        metric_prefix=args.metric_prefix,
        constant_tags=[f"{k}:{v}" for k, v in kv.items()],
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


def load_dataset_uris(args: argparse.Namespace) -> list[str]:
    """Collect dataset URIs from arguments, an optional file, and base-URI discovery.

    When ``--base-uri`` is supplied, every ``*.lance`` dataset under it is discovered recursively at any depth, so
    deeper partition hierarchies produced by custom ``--partition-by`` layouts are picked up alongside the historical
    three-level layout.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The list of dataset URIs.

    Raises:
        ValueError: If no dataset URIs are provided or discovered.
    """
    uris: list[str] = list(args.dataset_uri or [])
    if args.datasets_file:
        with open(args.datasets_file, encoding="utf-8") as handle:
            uris.extend(line.strip() for line in handle if line.strip())
    if args.base_uri:
        uris.extend(discover_datasets(args.base_uri, parse_key_values(args.storage_option) or None))
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
        partition_cols=parse_partition_cols(args.partition_by) or list(DEFAULT_PARTITION_COLS),
        partition_derivations=parse_partition_derivations(args.partition_derive),
        vectors_col=args.vectors_col,
        metadata_col=args.metadata_col,
        ts_col=args.ts_col,
        op_col=args.op_col,
        delete_op_values=list(args.delete_op_value or ["delete", "DELETE", "d"]),
        column_types=resolve_type_map(parse_key_values(args.column_type)),
        storage_options=parse_key_values(args.storage_option) or None,
        num_partitions=args.num_partitions,
        conflict_retries=args.conflict_retries,
        retry_timeout=timedelta(seconds=args.retry_timeout),
        guard_updates_by_ts=args.guard_updates_by_ts,
        iceberg_read_options=parse_key_values(args.iceberg_option),
        window_start=args.window_start,
        window_end=args.window_end,
        window_column=args.window_column,
    )
    IcebergToLanceETL(config).run(spark, args.table, parse_epoch_ms(args.start), parse_epoch_ms(args.end))


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
        large_dataset_fragment_threshold=args.fragment_count_threshold,
        batch_partitions=args.small_tier_parallelism,
        max_source_fragments=args.max_source_fragments,
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
        small_dataset_fragment_threshold=args.fragment_count_threshold,
        small_tier_slices=args.small_tier_parallelism,
        vector_min_rows=args.vector_index_row_floor,
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
    parser.add_argument("--dd-tag", action="append", help="Constant tag key=value, repeatable")
    parser.add_argument("--storage-option", action="append", help="pylance storage option key=value, repeatable")
    parser.add_argument(
        "--lance-cpu-threads",
        type=int,
        default=None,
        help="LANCE_CPU_THREADS — set below executor cores when tasks share an executor",
    )
    parser.add_argument(
        "--lance-io-threads",
        type=int,
        default=None,
        help="LANCE_IO_THREADS — cloud stores often need 128 or 256",
    )
    parser.add_argument(
        "--lance-io-buffer-size",
        type=int,
        default=None,
        help="LANCE_DEFAULT_IO_BUFFER_SIZE in bytes — raise alongside the I/O thread count",
    )
    parser.add_argument("--lance-log", default=None, help="LANCE_LOG filter, for example info")
    parser.add_argument("--lance-tracing", default=None, help="LANCE_TRACING level the event bridge observes")


def add_dataset_arguments(parser: argparse.ArgumentParser) -> None:
    """Add dataset-selection options shared by compact and index.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dataset-uri", action="append", help="Dataset URI, repeatable")
    parser.add_argument("--datasets-file", help="File with one dataset URI per line")
    parser.add_argument(
        "--base-uri",
        default=None,
        help=(
            "Discover datasets recursively under this URI: every *.lance path at any depth is included, so custom "
            "--partition-by hierarchies are picked up alongside the default three-level layout."
        ),
    )


def add_two_tier_arguments(parser: argparse.ArgumentParser, include_vector_floor: bool = False) -> None:
    """Add two-tier orchestration knobs shared by compact and index.

    The two-tier orchestration routes datasets with fewer than ``--fragment-count-threshold`` fragments through a
    lightweight single-executor path that runs multiple datasets in one Spark job via ``--small-tier-parallelism``
    parallel tasks. Larger datasets keep the existing per-dataset segment fan-out.

    Args:
        parser: The subcommand parser to extend.
        include_vector_floor: When ``True``, also add ``--vector-index-row-floor``.
    """
    parser.add_argument(
        "--fragment-count-threshold",
        type=int,
        default=32,
        help=(
            "Datasets with fewer than this many fragments are processed by the small-tier single-executor path "
            "rather than the per-dataset distributed segment fan-out. Default 32."
        ),
    )
    parser.add_argument(
        "--small-tier-parallelism",
        type=int,
        default=256,
        help=(
            "Number of small-tier datasets batched into one Spark job and processed in parallel across executors. "
            "Default 256."
        ),
    )
    if include_vector_floor:
        parser.add_argument(
            "--vector-index-row-floor",
            type=int,
            default=50_000,
            help=(
                "Datasets with fewer than this many rows skip IVF_RQ vector indexing. Lance flat KNN is adequate "
                "at this scale and training an IVF with too few rows degrades quality. Default 50000."
            ),
        )


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser.

    Returns:
        The parser with the ``etl``, ``compact``, and ``index`` subcommands.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-name", default="lance-pipeline")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    etl: argparse.ArgumentParser = subparsers.add_parser("etl", help="Run the Iceberg-to-Lance ETL")
    add_common_arguments(etl)
    etl.add_argument("--table", required=True)
    etl.add_argument("--start", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--end", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--base-uri", required=True)
    etl.add_argument("--key-col", default="vector_id")
    etl.add_argument(
        "--partition-by",
        default=None,
        help=(
            "Comma-separated columns routing each row to its dataset. The path is base_uri/<val1>/.../<valN>.lance "
            "in this order. Every column must exist in the source table or be produced by --partition-derive. "
            "Default: org_id,tenant_id,namespace. A key whose partition value changes between runs leaves a stale "
            "copy in the previously-routed dataset. Readers deduplicate."
        ),
    )
    etl.add_argument(
        "--partition-derive",
        action="append",
        help=(
            "Derived partition column as NAME=SOURCE:FORMAT, repeatable. FORMAT is a Python strftime pattern "
            "(supported directives: %%Y %%y %%m %%d %%H %%M %%S %%j %%%%) translated to Spark's date_format and "
            "applied to SOURCE before routing, e.g. event_date=processing_timestamp:%%Y-%%m-%%d."
        ),
    )
    etl.add_argument("--vectors-col", default="vectors")
    etl.add_argument("--metadata-col", default="metadata")
    etl.add_argument("--ts-col", default="timestamp")
    etl.add_argument("--op-col", default="op")
    etl.add_argument("--delete-op-value", action="append")
    etl.add_argument("--column-type", action="append", help="Cast column as name=arrow_type")
    etl.add_argument("--iceberg-option", action="append", help="Iceberg read option key=value")
    etl.add_argument("--num-partitions", type=int, default=512)
    etl.add_argument("--conflict-retries", type=int, default=10)
    etl.add_argument(
        "--retry-timeout",
        type=float,
        default=30.0,
        help=(
            "Total seconds the merge-insert conflict-retry loop is allowed to run per dataset "
            "(maps to MergeInsertBuilder.retry_timeout). Default 30.0."
        ),
    )
    etl.add_argument("--guard-updates-by-ts", action="store_true")
    etl.add_argument(
        "--window-start",
        default=None,
        help=(
            "ISO-8601 lower bound (inclusive) for the source timestamp window pushdown filter applied to "
            "--window-column after the Iceberg read.  Absent means the lower bound is open (no filter)."
        ),
    )
    etl.add_argument(
        "--window-end",
        default=None,
        help=(
            "ISO-8601 upper bound (exclusive) for the source timestamp window pushdown filter applied to "
            "--window-column after the Iceberg read.  Absent means the upper bound is open (no filter)."
        ),
    )
    etl.add_argument(
        "--window-column",
        default="updated_at",
        help=(
            "Iceberg column used for the timestamp window pushdown filter.  Must be a timestamp column "
            "present in the source table.  Default: updated_at."
        ),
    )

    compact: argparse.ArgumentParser = subparsers.add_parser("compact", help="Distributed compaction of Lance datasets")
    add_common_arguments(compact)
    add_dataset_arguments(compact)
    add_two_tier_arguments(compact)
    compact.add_argument("--target-rows-per-fragment", type=int, default=None)
    compact.add_argument("--max-rows-per-group", type=int, default=None)
    compact.add_argument("--max-bytes-per-file", type=int, default=None)
    compact.add_argument("--no-materialize-deletions", action="store_true")
    compact.add_argument("--materialize-deletions-threshold", type=float, default=None)
    compact.add_argument("--no-defer-index-remap", action="store_true")
    compact.add_argument("--num-threads", type=int, default=None)
    compact.add_argument("--batch-size", type=int, default=None)
    compact.add_argument(
        "--compaction-mode",
        default=None,
        choices=("reencode", "try_binary_copy", "force_binary_copy"),
    )
    compact.add_argument("--max-tasks", type=int, default=256)
    compact.add_argument("--no-cleanup", action="store_true")
    compact.add_argument("--cleanup-older-than-seconds", type=int, default=None)
    compact.add_argument("--retain-versions", type=int, default=None)
    compact.add_argument("--commit-retries", type=int, default=20)
    compact.add_argument("--commit-backoff-seconds", type=float, default=0.5)
    compact.add_argument(
        "--max-source-fragments",
        type=int,
        default=None,
        help="Hard cap on the number of source fragments per compaction plan task. Default: no limit.",
    )

    index: argparse.ArgumentParser = subparsers.add_parser(
        "index", help="Build IVF_RQ vector, btree scalar, bitmap, and full-text BM25 indices on Lance datasets"
    )
    add_common_arguments(index)
    add_dataset_arguments(index)
    add_two_tier_arguments(index, include_vector_floor=True)
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
    index.add_argument("--text-column", action="append", help="Text column for a full-text BM25 index")
    index.add_argument("--fts-with-position", action="store_true", help="Store token positions for phrase queries")
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


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, build a Spark session, and dispatch the subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    level: int = logging.getLevelName(args.log_level.upper())
    configure_logging(build_telemetry_config(args), level=level)
    lance_env: dict[str, str] = apply_lance_runtime(build_lance_runtime_config(args))
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
