"""Shared CLI helpers for every lance-etl job package.

Provides argument parsing utilities, telemetry-config construction, dataset-URI loading, the
argument groups that are reused across the etl, indexing, maintenance, and tools CLIs, and the
shared subcommand-runner harness (Spark lifecycle, dataset-URI no-op guard, and the partial-failure
exit-code convention) every per-job CLI's ``run*`` functions build on.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.cloud_storage import discover_datasets
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.maintenance.job import MaintenanceConfig
from lance_etl.telemetry import TelemetryConfig, configure_logging

APP_NAME: str = "lance-pipeline"
"""Opinionated Spark application name shared by every subcommand."""

EXIT_PARTIAL_FAILURE: int = 3
"""Process exit code when a fleet run completed but one or more datasets failed in isolation."""

SPARK_CORE_CONF_PINS: dict[str, str] = {
    "spark.speculation": "false",
}
"""Correctness pins applied on the session builder, before the SparkContext starts.

``spark.speculation`` must stay off: a speculative or zombie duplicate task attempt would run the
same routing keys' ``merge_insert`` commits concurrently with the original attempt, breaking the
key-disjointness guarantee that makes concurrent merge writers safe (ADR 0034) and silently
duplicating rows. Unlike :data:`SPARK_CONF_DEFAULTS` these are correctness invariants rather than
tunable defaults, and core scheduler configs cannot be modified on a running session, so they are
pinned at builder time.
"""

SPARK_CONF_DEFAULTS: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "8192",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "4096",
}
"""Runtime Spark SQL defaults applied by :func:`build_spark` when the operator did not set them.

Adaptive query execution right-sizes shuffle partitions from actual data volumes instead of the
static partition count. AQE only coalesces DOWN from the initial partition count, so the high
``initialPartitionNum`` lets a long-tail increment (up to ~1M tiny routing keys) start wide and
shrink to the byte-sized advisory target instead of being capped at ``spark.sql.shuffle.partitions``.
The Arrow batch cap bounds the per-slice memory of every ``mapInArrow`` stage for wide vector
rows (a 4096-row batch of 512-byte rows stays around 2 MiB), which is the first line of defense
against executor OOM on large increments.
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
    pair: Any
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"expected key=value, got {pair!r}")
        key: Any
        value: Any
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


def load_dataset_uris(args: argparse.Namespace, spark: SparkSession | None = None) -> list[str]:
    """Collect dataset URIs from arguments, an optional file, and base-URI discovery.

    Three sources are combined in order: explicit ``--dataset-uri`` flags, a ``--datasets-file`` (one URI per
    line), and recursive discovery under ``--base-uri``. A ``--datasets-file`` that exists but is empty
    contributes no URIs and is not an error, so an idle-window state file written by the ETL passes
    through cleanly. Callers that receive an empty list should treat it as a no-op rather than raise.

    When ``--base-uri`` is supplied, every ``*.lance`` dataset under it is discovered recursively at any
    depth, so the standard three-level ``org_id/tenant_id/namespace`` layout and any deeper
    ``migrate-namespace`` hierarchies are both picked up. Passing the job's Spark session fans the
    per-prefix listings out across executors, which large fleets need for tolerable discovery time.

    Duplicate URIs are removed before returning, preserving first-occurrence order, so a dataset named through more
    than one source (for example a repeated ``--dataset-uri`` flag, or a URI present in both ``--dataset-uri`` and
    the ``--base-uri`` discovery) contributes exactly one commit entry rather than two that would conflict with each
    other in the same job.

    Args:
        args: Parsed command-line arguments.
        spark: Active session forwarded to :func:`~lance_etl.cloud_storage.discover_datasets` for
            executor-fanned discovery, or ``None`` for the pure-driver walk.

    Returns:
        The deduplicated list of dataset URIs, which may be empty when all sources are empty.

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
        uris.extend(
            discover_datasets(
                args.base_uri,
                parse_storage_options(args),
                spark=spark,
                partitions=getattr(args, "discover_partitions", 64),
            )
        )
    return list(dict.fromkeys(uris))


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the identity and storage options shared by all subcommands.

    These are the only cross-cutting per-deployment arguments: the Datadog service/env/version tags, repeatable
    constant tags, and pylance storage options. DogStatsD host/port and metric prefix are not exposed; they take their
    opinionated configuration defaults and can be tuned in code.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dd-service", default=TelemetryConfig.service)
    parser.add_argument("--dd-env", default=TelemetryConfig.env)
    parser.add_argument("--dd-version", default=TelemetryConfig.version)
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
    parser.add_argument(
        "--discover-partitions",
        type=int,
        default=64,
        help="Executor partitions for base-URI dataset discovery",
    )


def add_ttl_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the per-row TTL flags shared by the maintenance and pipeline ``run`` subcommands.

    Both subcommands expire rows the same way: ``--ttl-column`` turns per-row TTL on and names the
    Arrow ``Duration`` column holding each row's lifetime, and ``--ts-column`` names the event
    timestamp column used as the TTL clock. The ``--ts-column`` default mirrors
    :attr:`~lance_etl.maintenance.job.MaintenanceConfig.ts_column` directly rather than
    duplicating the literal, so the two can never drift apart.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument(
        "--ttl-column",
        default=None,
        help=(
            "Per-row TTL column holding each row's lifetime as an Arrow Duration. When set, rows are expired before "
            "compaction by the predicate ts-column + ttl-column < now. Absent (the default) turns TTL off."
        ),
    )
    parser.add_argument(
        "--ts-column",
        default=MaintenanceConfig.ts_column,
        help=(
            "Event timestamp column used as the TTL clock. Must match ETLConfig.ts_col. Only used when --ttl-column "
            "is set. Default: event_timestamp."
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
    parser.add_argument("--metric", default=IndexJobConfig.metric, help="Vector distance metric: L2, cosine, or dot")
    parser.add_argument("--scalar-column", action="append", help="Scalar column for a btree index")
    parser.add_argument("--bitmap-column", action="append", help="Column for a bitmap index")
    parser.add_argument("--zonemap-column", action="append", help="Column for a zonemap index")
    parser.add_argument("--text-column", action="append", help="Text column for a full-text BM25 index")
    parser.add_argument("--fts-base-tokenizer", default=None, help="FTS base tokenizer name")
    parser.add_argument("--fts-language", default=None, help="FTS stemming and stop-word language")


def index_config_from_args(
    args: argparse.Namespace,
    telemetry: TelemetryConfig,
    storage_options: dict[str, Any] | None = None,
    rebuild: bool = False,
) -> IndexJobConfig:
    """Build an :class:`~lance_etl.indexing.config.IndexJobConfig` from the shared index-column flags.

    Every caller that exposes :func:`add_index_column_arguments` (the indexing CLI, the
    ``migrate-namespace`` subcommand, and the pipeline CLI) constructs the same column-selection
    fields from the same flags; only the identity/storage wiring and the rebuild flag differ per
    caller, so those three are left to the caller instead of defaulted here. The pipeline CLI
    passes a placeholder ``telemetry`` and omits ``storage_options`` because
    :meth:`~lance_etl.pipeline.job.PipelineConfig.__post_init__` overwrites both cross-cutting
    fields on the composed sub-config unconditionally once the ``PipelineConfig`` is built, so
    resolving the "real" values here would be wasted work.

    Args:
        args: Parsed command-line arguments carrying the :func:`add_index_column_arguments` flags.
        telemetry: Telemetry configuration for the constructed config.
        storage_options: Object-store options for the constructed config. Defaults to ``None``,
            matching :class:`~lance_etl.indexing.config.IndexJobConfig`'s own default.
        rebuild: Whether to force a full index rebuild. Defaults to ``False`` for callers with no
            ``--rebuild`` flag, such as ``migrate-namespace``.

    Returns:
        The constructed indexing configuration.
    """
    return IndexJobConfig(
        telemetry=telemetry,
        storage_options=storage_options,
        vector_columns=list(args.vector_column or []),
        metric=args.metric,
        scalar_columns=list(args.scalar_column or []),
        bitmap_columns=list(args.bitmap_column or []),
        zonemap_columns=list(args.zonemap_column or []),
        text_columns=list(args.text_column or []),
        fts_base_tokenizer=args.fts_base_tokenizer,
        fts_language=args.fts_language,
        rebuild=rebuild,
    )


def build_spark(app_name: str | None = None) -> SparkSession:
    """Build and return a Spark session for a lance-etl job with memory-safe SQL defaults.

    Each entry of :data:`SPARK_CONF_DEFAULTS` is applied only when the key was not set
    explicitly through spark-submit, spark-defaults, or the session builder, so operator
    configuration always wins. Explicitly-set keys are detected through the SparkContext's
    ``SparkConf``, which carries only explicit settings and not Spark's built-in defaults.

    The :data:`SPARK_CORE_CONF_PINS` entries (``spark.speculation=false``) are set on the builder
    instead, because core scheduler configs raise ``CANNOT_MODIFY_CONFIG`` when set on a running
    session. They are correctness invariants, not defaults: speculative or zombie duplicate task
    attempts would run the same routing keys' merges concurrently and break the merge-insert
    key-disjointness guarantee (ADR 0034), so they take effect whenever this call creates the
    SparkContext. A pre-existing context keeps its own values, which spark-submit must then pin.

    Args:
        app_name: Spark application name. Defaults to :data:`APP_NAME`.

    Returns:
        An active SparkSession.
    """
    builder: SparkSession.Builder = SparkSession.builder.appName(app_name or APP_NAME)
    pin_key: Any
    pin_value: Any
    for pin_key, pin_value in SPARK_CORE_CONF_PINS.items():
        builder = builder.config(pin_key, pin_value)
    session: SparkSession = builder.getOrCreate()
    explicit: Any = session.sparkContext.getConf()
    conf_key: Any
    conf_value: Any
    for conf_key, conf_value in SPARK_CONF_DEFAULTS.items():
        if not explicit.contains(conf_key):
            session.conf.set(conf_key, conf_value)
    return session


def run_with_spark[JobResult](
    spark: SparkSession,
    job_label: str,
    logger: logging.Logger,
    work: Callable[[], JobResult],
) -> JobResult:
    """Run a CLI job body inside the shared Spark-lifecycle try/except/finally harness.

    Every subcommand runner builds a Spark session, executes its job body, logs and re-raises any
    exception under a job-specific label, and stops the session in a ``finally`` block regardless
    of outcome. This function owns that shape so each CLI module supplies only the label, its own
    logger, and the job body; ``spark`` itself is still built by the caller (not by this helper),
    so a test can monkeypatch a CLI module's own ``build_spark`` reference and have it take effect.

    Args:
        spark: An already-built Spark session that this call owns and stops.
        job_label: Human-readable label for the ``"<label> failed"`` exception log line.
        logger: The calling module's logger, so the log record's logger name matches the module.
        work: Zero-argument callable that performs the job and returns its result.

    Returns:
        Whatever ``work`` returns.

    Raises:
        Exception: Re-raises any exception ``work`` raises, after logging it.
    """
    try:
        return work()
    except Exception:
        logger.exception("%s failed", job_label)
        raise
    finally:
        spark.stop()


def load_uris_or_none(
    args: argparse.Namespace, spark: SparkSession, job_label: str, logger: logging.Logger
) -> list[str] | None:
    """Load dataset URIs for a fleet job, logging and returning ``None`` when there is nothing to do.

    Shared by every fleet subcommand runner that gates its work on a non-empty dataset list. The
    caller invokes this inside its :func:`run_with_spark` job body and returns early when this
    returns ``None``, so run_with_spark owns the Spark teardown on every exit path, including the
    empty-fleet one.

    Args:
        args: Parsed command-line arguments carrying dataset-selection flags.
        spark: Active Spark session forwarded to :func:`load_dataset_uris` for executor-fanned
            discovery.
        job_label: Human-readable label for the ``"<label>: no datasets..."`` log line.
        logger: The calling module's logger, so the log record's logger name matches the module.

    Returns:
        The resolved, deduplicated dataset URIs, or ``None`` when the list is empty.
    """
    uris: list[str] = load_dataset_uris(args, spark)
    if not uris:
        logger.info("%s: no datasets in the URI list, nothing to do", job_label)
        return None
    return uris


def run_cli_main(
    parser: argparse.ArgumentParser,
    runners: dict[str, Callable[[argparse.Namespace], int | None]] | Callable[[argparse.Namespace], int | None],
    argv: Sequence[str] | None,
) -> int:
    """Run one per-job CLI main: parse, configure logging, dispatch, map the exit code.

    Owns the shape shared by all five per-job CLIs. ``runners`` is either a subcommand dispatch
    dict keyed by ``args.command`` (maintenance, pipeline, tools) or the single runner for a
    subcommand-free CLI (etl, indexing). A runner returns the isolated failed-dataset count
    (``None`` is treated as zero), which :func:`resolve_exit_code` maps to ``0`` or
    :data:`EXIT_PARTIAL_FAILURE`. Any exception escaping the runner yields exit code ``1``.
    ``SystemExit`` from argparse (``--help``, bad flags) propagates before the try block,
    preserving argparse's own exit codes.

    Args:
        parser: The subcommand's fully-built argument parser.
        runners: Either a single runner callable, or a dispatch dict mapping ``args.command``
            values to their runner callables.
        argv: Optional argument vector forwarded to ``parser.parse_args``. Defaults to ``sys.argv``.

    Returns:
        A process exit code: ``0`` when every dataset succeeded, ``1`` when the run raised an
        unhandled exception, and :data:`EXIT_PARTIAL_FAILURE` (``3``) when the run completed but
        one or more datasets failed in isolation.
    """
    args: argparse.Namespace = parser.parse_args(argv)
    configure_logging_from_args(args)
    try:
        runner: Callable[[argparse.Namespace], int | None] = (
            runners[args.command] if isinstance(runners, dict) else runners
        )
        failed: int | None = runner(args)
        return resolve_exit_code(failed if failed is not None else 0)
    except Exception:
        logging.getLogger(__name__).exception("unexpected top-level CLI failure")
        return 1


def resolve_exit_code(failed: int) -> int:
    """Map a failed-dataset count to the shared partial-failure exit code convention.

    Args:
        failed: The number of datasets that failed in isolation during the run, ``0`` when every
            dataset succeeded.

    Returns:
        ``0`` when ``failed`` is ``0``, otherwise :data:`EXIT_PARTIAL_FAILURE`.
    """
    return EXIT_PARTIAL_FAILURE if failed > 0 else 0


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
        value: An ISO 8601 datetime string supplied by a local caller.

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
    """Convert an ISO 8601 datetime string to a colon-free UTC interval tag.

    The result is formatted as ``%Y%m%dT%H%M%SZ`` (e.g. ``"20260611T120000Z"``), the colon-free
    stamp used as the interval tag name throughout the pipeline.

    Args:
        value: An ISO 8601 datetime string supplied by a local caller.

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
        value: An ISO 8601 datetime string supplied by a local caller.

    Returns:
        The truncated-hour tag name in ``%Y%m%dT%H%M%SZ`` format.

    Raises:
        ValueError: If ``value`` cannot be parsed by :func:`datetime.fromisoformat`.
    """
    truncated: datetime = parse_tag_datetime(value).replace(minute=0, second=0, microsecond=0)
    return truncated.strftime("%Y%m%dT%H%M%SZ")
