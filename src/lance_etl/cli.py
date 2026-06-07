"""Command-line entry point for the Lance vector pipeline jobs.

The CLI is deliberately small and opinionated. It exposes only the arguments that are genuinely
per-deployment: the data and identity contract (which table, which window, where datasets live, which
Datadog service) and what to build (partition routing, which index types, the distance metric, and the
FTS base tokenizer and language). Every tuning knob — the schema column names, shuffle partitions, retry
budgets, commit backoff, compaction fragment sizing, IVF training parameters, the fine-grained FTS
tokenizer toggles, and two-tier thresholds — is set to a sensible opinionated default in the configuration dataclasses
(:class:`lance_etl.etl.ETLConfig`, :class:`lance_etl.indexing.IndexJobConfig`,
:class:`lance_etl.compaction.CompactionConfig`). Those fields stay tunable in code, just not from the
command line.

Provides the following subcommands. ``etl`` reads a time range from an Iceberg table and routes the
changes into per-tenant Lance datasets. Backfills are catch-up replays of this same job over historical
windows. ``compact`` runs distributed compaction over a set of datasets. ``index`` builds IVF_RQ vector,
btree scalar, bitmap, and full-text BM25 indices over a set of datasets. ``recall`` replays
Datadog-sampled vector queries as exact brute-force scans against the dataset versions that served them
and reports recall@k. ``tag`` flips a serving tag (default ``prod``) to a target dataset version for
blue-green promotion. ``migrate-manifests`` migrates dataset manifest paths to the V2 naming scheme.

Each subcommand builds a Spark session, runs the job, and exits non-zero on failure so an orchestrator
can retry.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lance_etl.arrow_types import resolve_type_map
from lance_etl.cloud_storage import discover_datasets
from lance_etl.compaction import (
    CompactionConfig,
    LanceCompactor,
    migrate_manifest_paths,
    update_serving_tags,
)
from lance_etl.etl import DEFAULT_PARTITION_COLS, ETLConfig, IcebergToLanceETL
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.recall import DatadogSpanSource, RecallAuditJob, RecallJobConfig
from lance_etl.telemetry import TelemetryConfig, configure_logging

logger: logging.Logger = logging.getLogger(__name__)

APP_NAME: str = "lance-pipeline"
"""Opinionated Spark application name shared by every subcommand."""


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


def parse_storage_options(args: argparse.Namespace) -> dict[str, str] | None:
    """Parse the repeated ``--storage-option`` arguments shared by every subcommand.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The storage options for pylance, or None when none were given.
    """
    return parse_key_values(args.storage_option) or None


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


def build_telemetry_config(args: argparse.Namespace) -> TelemetryConfig:
    """Build a telemetry configuration from the identity arguments.

    The DogStatsD host and port and the metric prefix are not exposed on the CLI: they take the opinionated
    :class:`TelemetryConfig` defaults (``localhost:8125`` and ``lance.pipeline``).

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
        constant_tags=[f"{k}:{v}" for k, v in kv.items()],
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
        uris.extend(discover_datasets(args.base_uri, parse_storage_options(args)))
    if not uris:
        raise ValueError("no dataset URIs provided")
    return uris


def run_etl(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the ETL subcommand.

    Only the data and identity contract is taken from the CLI. The key/timestamp/op column names, the delete-op
    encodings, the map column names, the window column, shuffle partition count, conflict-retry budget, retry timeout,
    and the timestamp update guard take their opinionated :class:`ETLConfig` defaults.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: ETLConfig = ETLConfig(
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        partition_cols=parse_partition_cols(args.partition_by) or list(DEFAULT_PARTITION_COLS),
        column_types=resolve_type_map(parse_key_values(args.column_type)),
        storage_options=parse_storage_options(args),
        iceberg_read_options=parse_key_values(args.iceberg_option),
        window_start=args.window_start,
        window_end=args.window_end,
    )
    IcebergToLanceETL(config).run(spark, args.table, parse_epoch_ms(args.start), parse_epoch_ms(args.end))


def run_compact(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the compaction subcommand.

    Compaction has no per-deployment data contract beyond which datasets to compact, so the subcommand exposes
    only dataset selection and identity. Fragment sizing, deletion materialization, two-tier thresholds, retry
    budgets, and version-cleanup retention take their opinionated :class:`CompactionConfig` defaults.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: CompactionConfig = CompactionConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
    )
    LanceCompactor(config).run(spark, load_dataset_uris(args))


def run_index(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the indexing subcommand.

    The CLI selects what to build (which columns get which index type) and the data-shape knobs that cannot be
    defaulted (the distance metric, the FTS base tokenizer, and the FTS language). IVF training parameters, partition
    counts, shard counts, the vector row floor, delta and retrain bounds, retry budgets, and the fine-grained FTS
    tokenizer toggles (lower-case, stemming, stop-word removal, ASCII folding) take their opinionated
    :class:`IndexJobConfig` defaults. ``--rebuild`` remains as the operational escape hatch for tokenizer or
    parameter changes that need a full reindex.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: IndexJobConfig = IndexJobConfig(
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        vector_column=args.vector_column,
        metric=args.metric,
        scalar_columns=list(args.scalar_column or []),
        bitmap_columns=list(args.bitmap_column or []),
        text_columns=list(args.text_column or []),
        fts_with_position=args.fts_with_position,
        fts_base_tokenizer=args.fts_base_tokenizer,
        fts_language=args.fts_language,
        rebuild=args.rebuild,
    )
    LanceIndexer(config).run(spark, load_dataset_uris(args))


def run_recall(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the recall-audit subcommand.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    config: RecallJobConfig = RecallJobConfig(
        base_uri=args.base_uri,
        telemetry=build_telemetry_config(args),
        storage_options=parse_storage_options(args),
        id_column=args.id_column,
        vector_column=args.vector_column,
        max_samples=args.max_samples,
        batch_size=args.batch_size,
    )
    source: DatadogSpanSource = DatadogSpanSource(site=args.dd_site)
    RecallAuditJob(config).run(spark, source, parse_epoch_ms(args.from_ts), parse_epoch_ms(args.to_ts))


def run_tag(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the serving-tag subcommand for blue-green promotion.

    Flips a serving tag (default ``prod``) to a target dataset version across the selected datasets. With no
    ``--tag-version`` the tag is moved to each dataset's latest version. The helper logs the safe operational
    sequence and never assumes the serving layer auto-refreshes on a tag move.

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    update_serving_tags(
        spark,
        load_dataset_uris(args),
        build_telemetry_config(args),
        parse_storage_options(args),
        tag=args.tag,
        target_version=args.tag_version,
    )


def run_migrate_manifests(args: argparse.Namespace, spark: SparkSession) -> None:
    """Run the manifest-path migration subcommand.

    Migrates every selected dataset's manifest paths to the V2 naming scheme so subsequent opens cost one object-store
    request instead of a version-count-proportional LIST. The migration is not transactional, so run it only with the
    targeted datasets quiesced (no concurrent ingestion, compaction, or indexing).

    Args:
        args: Parsed command-line arguments.
        spark: Active Spark session.
    """
    migrate_manifest_paths(
        spark,
        load_dataset_uris(args),
        build_telemetry_config(args),
        parse_storage_options(args),
    )


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the identity and storage options shared by all subcommands.

    These are the only cross-cutting per-deployment arguments: the Datadog service/env/version tags, repeatable
    constant tags, and pylance storage options. DogStatsD host/port and metric prefix are not exposed; they take their
    opinionated configuration defaults and can be tuned in code.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dd-service", default="lance-pipeline")
    parser.add_argument("--dd-env", default="prod")
    parser.add_argument("--dd-version", default="")
    parser.add_argument("--dd-tag", action="append", help="Constant tag key=value, repeatable")
    parser.add_argument("--storage-option", action="append", help="pylance storage option key=value, repeatable")


def add_dataset_arguments(parser: argparse.ArgumentParser) -> None:
    """Add dataset-selection options shared by compact, index, tag, and migrate-manifests.

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


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser.

    Returns:
        The parser with the ``etl``, ``compact``, ``index``, ``recall``, ``tag``, and ``migrate-manifests``
        subcommands.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    etl: argparse.ArgumentParser = subparsers.add_parser("etl", help="Run the Iceberg-to-Lance ETL")
    add_common_arguments(etl)
    etl.add_argument("--table", required=True)
    etl.add_argument("--start", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--end", required=True, help="ISO 8601 or epoch milliseconds")
    etl.add_argument("--base-uri", required=True)
    etl.add_argument(
        "--partition-by",
        default=None,
        help=(
            "Comma-separated columns routing each row to its dataset. The path is base_uri/<val1>/.../<valN>.lance "
            "in this order. Every column must exist in the source table. Default: org_id,tenant_id,namespace. Each key "
            "lives in exactly one dataset, so the per-dataset merge_insert is the sole dedup."
        ),
    )
    etl.add_argument("--column-type", action="append", help="Cast column as name=arrow_type")
    etl.add_argument("--iceberg-option", action="append", help="Iceberg read option key=value")
    etl.add_argument(
        "--window-start",
        default=None,
        help=(
            "ISO-8601 lower bound (inclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read.  Absent means the lower bound is open (no filter)."
        ),
    )
    etl.add_argument(
        "--window-end",
        default=None,
        help=(
            "ISO-8601 upper bound (exclusive) for the source timestamp window pushdown filter applied to the "
            "configured window column after the Iceberg read.  Absent means the upper bound is open (no filter)."
        ),
    )

    compact: argparse.ArgumentParser = subparsers.add_parser("compact", help="Distributed compaction of Lance datasets")
    add_common_arguments(compact)
    add_dataset_arguments(compact)

    index: argparse.ArgumentParser = subparsers.add_parser(
        "index", help="Build IVF_RQ vector, btree scalar, bitmap, and full-text BM25 indices on Lance datasets"
    )
    add_common_arguments(index)
    add_dataset_arguments(index)
    index.add_argument("--vector-column", default=None, help="Vector column to index with IVF_RQ")
    index.add_argument("--metric", default="L2", help="Vector distance metric: L2, cosine, or dot")
    index.add_argument("--scalar-column", action="append", help="Scalar column for a btree index")
    index.add_argument("--bitmap-column", action="append", help="Column for a bitmap index")
    index.add_argument("--text-column", action="append", help="Text column for a full-text BM25 index")
    index.add_argument("--fts-with-position", action="store_true", help="Store token positions for phrase queries")
    index.add_argument("--fts-base-tokenizer", default=None, help="FTS base tokenizer name")
    index.add_argument("--fts-language", default=None, help="FTS stemming and stop-word language")
    index.add_argument(
        "--rebuild",
        action="store_true",
        help="Reindex every fragment instead of only uncovered ones. Use after tokenizer or parameter changes.",
    )

    recall: argparse.ArgumentParser = subparsers.add_parser(
        "recall", help="Audit served recall@k by replaying Datadog-sampled vector queries as exact brute-force scans"
    )
    add_common_arguments(recall)
    recall.add_argument("--from", dest="from_ts", required=True, help="Window start, ISO 8601 or epoch milliseconds")
    recall.add_argument("--to", dest="to_ts", required=True, help="Window end, ISO 8601 or epoch milliseconds")
    recall.add_argument(
        "--base-uri",
        required=True,
        help="Root under which per-tenant datasets live as base/<org>/<tenant>/<namespace>.lance",
    )
    recall.add_argument(
        "--dd-site",
        default="datadoghq.com",
        help="Datadog site domain for the Spans search API. DD_API_KEY and DD_APP_KEY must be in the environment.",
    )
    recall.add_argument("--max-samples", type=int, default=10_000, help="Cap on sampled spans fetched. Default 10000.")
    recall.add_argument("--id-column", default="vector_id", help="Unique id column matched against served result ids")
    recall.add_argument("--vector-column", default="vector", help="Fixed-size-list vector column to scan")
    recall.add_argument("--batch-size", type=int, default=8192, help="Scanner batch size for the brute-force scan")

    tag: argparse.ArgumentParser = subparsers.add_parser(
        "tag",
        help=(
            "Flip a serving tag (default 'prod') to a target dataset version for blue-green promotion. Tagged "
            "versions are exempt from version cleanup."
        ),
    )
    add_common_arguments(tag)
    add_dataset_arguments(tag)
    tag.add_argument("--tag", default="prod", help="Serving tag name to update. Default: prod.")
    tag.add_argument(
        "--tag-version",
        type=int,
        default=None,
        help="Target dataset version for the tag. Omit to point the tag at each dataset's latest version.",
    )

    migrate_manifests: argparse.ArgumentParser = subparsers.add_parser(
        "migrate-manifests",
        help=(
            "Migrate existing datasets' manifest paths to the V2 naming scheme (one object-store request per open). "
            "Not transactional: run only with the targeted datasets quiesced."
        ),
    )
    add_common_arguments(migrate_manifests)
    add_dataset_arguments(migrate_manifests)
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
    spark: SparkSession = SparkSession.builder.appName(APP_NAME).getOrCreate()
    runners: dict[str, Callable[[argparse.Namespace, SparkSession], None]] = {
        "etl": run_etl,
        "compact": run_compact,
        "index": run_index,
        "recall": run_recall,
        "tag": run_tag,
        "migrate-manifests": run_migrate_manifests,
    }
    try:
        runners[args.command](args, spark)
        return 0
    except Exception:
        logger.exception("job failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
