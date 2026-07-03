"""Shared CLI helpers for every lance-etl job package.

Provides argument parsing utilities, telemetry-config construction, dataset-URI loading, and the
argument groups that are reused across the etl, indexing, maintenance, and tools CLIs.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lance_etl.cloud_storage import discover_datasets
from lance_etl.telemetry import TelemetryConfig, configure_logging

APP_NAME: str = "lance-pipeline"
"""Opinionated Spark application name shared by every subcommand."""

SPARK_CONF_DEFAULTS: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "4096",
}
"""Runtime Spark SQL defaults applied by :func:`build_spark` when the operator did not set them.

Adaptive query execution right-sizes shuffle partitions from actual data volumes instead of the
static partition count. The Arrow batch cap bounds the per-slice memory of every ``mapInArrow``
stage for wide vector rows (a 4096-row batch of 512-byte rows stays around 2 MiB), which is the
first line of defense against executor OOM on large increments.
"""


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
    """Parse the comma-separated ``--partition-by`` column list used by ``migrate-namespace``.

    Args:
        value: The raw flag value, or None when the flag is absent.

    Returns:
        The column names in dataset-path order, or None when the flag is absent so the
        configuration default applies.

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

    Three sources are combined in order: explicit ``--dataset-uri`` flags, a ``--datasets-file`` (one URI per
    line), and recursive discovery under ``--base-uri``. A ``--datasets-file`` that exists but is empty
    contributes no URIs and is not an error, so an idle-window state file written by the ETL passes
    through cleanly. Callers that receive an empty list should treat it as a no-op rather than raise.

    When ``--base-uri`` is supplied, every ``*.lance`` dataset under it is discovered recursively at any
    depth, so the standard three-level ``org_id/tenant_id/namespace`` layout and any deeper
    ``migrate-namespace`` hierarchies are both picked up.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The list of dataset URIs, which may be empty when all sources are empty.

    Raises:
        ValueError: If neither ``--datasets-file`` nor ``--base-uri`` nor ``--dataset-uri`` was supplied
            at all (configuration error), distinguished from the case where all sources were supplied but
            happened to produce no URIs.
    """
    has_any_source: bool = bool(args.dataset_uri or args.datasets_file or args.base_uri)
    if not has_any_source:
        raise ValueError(
            "no dataset URI source configured: supply at least one of --dataset-uri, --datasets-file, or --base-uri"
        )
    uris: list[str] = list(args.dataset_uri or [])
    if args.datasets_file:
        with open(args.datasets_file, encoding="utf-8") as handle:
            uris.extend(line.strip() for line in handle if line.strip())
    if args.base_uri:
        uris.extend(discover_datasets(args.base_uri, parse_storage_options(args)))
    return uris


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
    """Add dataset-selection options shared by maintenance, index, tag, and migrate-manifests.

    Three sources may be combined: ``--dataset-uri`` for individual URIs, ``--datasets-file`` for a
    newline-delimited URI list, and ``--base-uri`` for recursive fleet discovery. At least one must be
    supplied.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dataset-uri", action="append", help="Dataset URI, repeatable")
    parser.add_argument("--datasets-file", help="File with one dataset URI per line")
    parser.add_argument(
        "--base-uri",
        default=None,
        help=(
            "Discover datasets recursively under this URI: every *.lance path at any depth is included, "
            "covering the standard org_id/tenant_id/namespace three-level layout."
        ),
    )


def add_index_column_arguments(parser: argparse.ArgumentParser) -> None:
    """Add index column-selection flags shared by the ``index`` and ``migrate-namespace`` subcommands.

    These flags select which columns receive which index type. When no flags are given the indexer
    builds no handlers and the step is a no-op (or skipped with a warning in the migrator).

    ``--vector-column`` is repeatable: each use appends one column name to the list of vector columns that
    receive an IVF_RQ index. Multiple vector columns are supported when a dataset carries more than one
    embedding (for example a dense vector and a sparse vector).

    The ``fts_with_position`` field (whether token positions are stored for phrase queries) is a
    tokenizer-schema contract: changing it requires a full ``--rebuild`` and must be set in
    :class:`~lance_etl.indexing.IndexJobConfig` in code rather than toggled per invocation.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--vector-column", action="append", help="Vector column to index with IVF_RQ, repeatable")
    parser.add_argument("--metric", default="L2", help="Vector distance metric: L2, cosine, or dot")
    parser.add_argument("--scalar-column", action="append", help="Scalar column for a btree index")
    parser.add_argument("--bitmap-column", action="append", help="Column for a bitmap index")
    parser.add_argument("--text-column", action="append", help="Text column for a full-text BM25 index")
    parser.add_argument("--fts-base-tokenizer", default=None, help="FTS base tokenizer name")
    parser.add_argument("--fts-language", default=None, help="FTS stemming and stop-word language")


def build_spark(app_name: str | None = None) -> SparkSession:
    """Build and return a Spark session for a lance-etl job with memory-safe SQL defaults.

    Each entry of :data:`SPARK_CONF_DEFAULTS` is applied only when the key was not set
    explicitly through spark-submit, spark-defaults, or the session builder, so operator
    configuration always wins. Explicitly-set keys are detected through the SparkContext's
    ``SparkConf``, which carries only explicit settings and not Spark's built-in defaults.

    Args:
        app_name: Spark application name. Defaults to :data:`APP_NAME`.

    Returns:
        An active SparkSession.
    """
    session: SparkSession = SparkSession.builder.appName(app_name or APP_NAME).getOrCreate()
    explicit = session.sparkContext.getConf()
    for conf_key, conf_value in SPARK_CONF_DEFAULTS.items():
        if not explicit.contains(conf_key):
            session.conf.set(conf_key, conf_value)
    return session


def configure_logging_from_args(args: argparse.Namespace) -> None:
    """Configure stdlib and ddtrace logging from parsed CLI arguments.

    Args:
        args: Parsed command-line arguments carrying ``log_level``.
    """

    level: int = logging.getLevelName(args.log_level.upper())
    configure_logging(build_telemetry_config(args), level=level)


def parse_tag_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime string to a UTC instant for interval-tag formatting.

    Accepts both the space-separated form (``"2026-06-11 12:00:00+00:00"``) and the T-separated
    form, with or without timezone info. Naive datetimes are treated as UTC.

    Args:
        value: An ISO 8601 datetime string, as templated by Airflow's
            ``{{ data_interval_end | string }}`` or supplied by an operator.

    Returns:
        The parsed instant converted to UTC.

    Raises:
        ValueError: If ``value`` cannot be parsed by :func:`datetime.fromisoformat`.
    """
    try:
        parsed: datetime = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"cannot parse {value!r} as an ISO 8601 datetime: {exc}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_window_tag(value: str) -> str:
    """Convert an Airflow-rendered datetime string to a colon-free UTC interval tag.

    The result is formatted as ``%Y%m%dT%H%M%SZ`` (e.g. ``"20260611T120000Z"``), the colon-free
    stamp used as the interval tag name throughout the pipeline.

    Args:
        value: An ISO 8601 datetime string, as templated by Airflow's
            ``{{ data_interval_end | string }}``.

    Returns:
        The tag name in ``%Y%m%dT%H%M%SZ`` format.

    Raises:
        ValueError: If ``value`` cannot be parsed by :func:`datetime.fromisoformat`.
    """
    return parse_tag_datetime(value).strftime("%Y%m%dT%H%M%SZ")


def parse_hour_tag(value: str) -> str:
    """Convert an ISO 8601 datetime string to the UTC interval tag of its truncated hour.

    Like :func:`parse_window_tag` but the minutes, seconds, and microseconds are zeroed before
    formatting, so any instant within an hour maps to that hour's tag name (for example
    ``"2026-06-11 12:34:56+00:00"`` becomes ``"20260611T120000Z"``). The ETL uses this to stamp
    every written dataset with the hour it was produced. The result parses under the same
    ``%Y%m%dT%H%M%SZ`` format the interval-tag pruning recognizes.

    Args:
        value: An ISO 8601 datetime string, as templated by Airflow's
            ``{{ data_interval_end | string }}`` or supplied by an operator.

    Returns:
        The truncated-hour tag name in ``%Y%m%dT%H%M%SZ`` format.

    Raises:
        ValueError: If ``value`` cannot be parsed by :func:`datetime.fromisoformat`.
    """
    truncated: datetime = parse_tag_datetime(value).replace(minute=0, second=0, microsecond=0)
    return truncated.strftime("%Y%m%dT%H%M%SZ")
