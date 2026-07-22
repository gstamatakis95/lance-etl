"""Shared parsing, telemetry, Spark lifecycle, and command runner helpers for local tools."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.spark_process import SPARK_CORE_CONF_PINS, ensure_spark_process_safety
from lance_etl.telemetry import TelemetryConfig, configure_logging

logger: logging.Logger = logging.getLogger(__name__)

APP_NAME: str = "lance-pipeline"
"""Opinionated Spark application name shared by every subcommand."""

SPARK_CONF_DEFAULTS: dict[str, str] = {
    "spark.sql.session.timeZone": "UTC",
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
The UTC session timezone makes timestamp projection and destructive age cutoffs independent of the
machine's local timezone.
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


def build_spark(app_name: str | None = None) -> SparkSession:
    """Build and return a Spark session for a lance-etl job with memory-safe SQL defaults.

    Each entry of :data:`SPARK_CONF_DEFAULTS` is applied only when the key was not set
    explicitly through spark-submit, spark-defaults, or the session builder, so operator
    configuration always wins. Explicitly-set keys are detected through the SparkContext's
    ``SparkConf``, which carries only explicit settings and not Spark's built-in defaults.

    The :data:`SPARK_CORE_CONF_PINS` entries are set on the builder instead, because process and
    core scheduler configs cannot be repaired after a context starts. They disable speculation and
    the fork-based Python daemon, and enable worker fault handlers. A pre-existing context keeps
    its own core values. This function verifies the effective speculation value and fails closed
    when that reused context is unsafe.

    Args:
        app_name: Spark application name. Defaults to :data:`APP_NAME`.

    Returns:
        An active SparkSession.

    Raises:
        RuntimeError: If a reused SparkContext has speculation enabled.
    """
    builder: SparkSession.Builder = SparkSession.builder.appName(app_name or APP_NAME)
    pin_key: Any
    pin_value: Any
    for pin_key, pin_value in SPARK_CORE_CONF_PINS.items():
        builder = builder.config(pin_key, pin_value)
    session: SparkSession = builder.getOrCreate()
    ensure_spark_process_safety(session, "running commit-producing jobs")
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
    of outcome. A shutdown failure is surfaced after a successful job but logged and suppressed
    after a failed job so it cannot replace the actionable primary exception. This function owns
    that shape so each CLI module supplies only the label, its own logger, and the job body.
    ``spark`` itself is still built by the caller, so a test can monkeypatch a CLI module's own
    ``build_spark`` reference and have it take effect.

    Args:
        spark: An already-built Spark session that this call owns and stops.
        job_label: Human-readable label for the ``"<label> failed"`` exception log line.
        logger: The calling module's logger, so the log record's logger name matches the module.
        work: Zero-argument callable that performs the job and returns its result.

    Returns:
        Whatever ``work`` returns.

    Raises:
        BaseException: Re-raises any failure from ``work`` after logging it, or a shutdown failure
            when the job itself succeeded.
    """
    work_error: BaseException | None = None
    try:
        return work()
    except BaseException as error:
        work_error = error
        logger.exception("%s failed", job_label)
        raise
    finally:
        try:
            spark.stop()
        except BaseException:
            if work_error is None:
                raise
            logger.exception("%s Spark shutdown also failed", job_label)


def run_cli_main(
    parser: argparse.ArgumentParser,
    runners: dict[str, Callable[[argparse.Namespace], None]] | Callable[[argparse.Namespace], None],
    argv: Sequence[str] | None,
) -> int:
    """Parse, configure logging, dispatch one local tool, and map unhandled failures.

    ``runners`` is either a subcommand dispatch dict keyed by ``args.command`` or a single runner.
    Any exception escaping the runner yields exit code ``1``. ``SystemExit`` from argparse
    (``--help``, bad flags) propagates before the try block, preserving argparse's own exit codes.

    Args:
        parser: The subcommand's fully-built argument parser.
        runners: Either a single runner callable, or a dispatch dict mapping ``args.command``
            values to their runner callables.
        argv: Optional argument vector forwarded to ``parser.parse_args``. Defaults to ``sys.argv``.

    Returns:
        ``0`` after successful dispatch or ``1`` when the runner raises.
    """
    args: argparse.Namespace = parser.parse_args(argv)
    configure_logging_from_args(args)
    try:
        runner: Callable[[argparse.Namespace], None] = runners[args.command] if isinstance(runners, dict) else runners
        runner(args)
        return 0
    except Exception:
        logging.getLogger(__name__).exception("unexpected top-level CLI failure")
        return 1


def configure_logging_from_args(args: argparse.Namespace) -> None:
    """Configure stdlib and ddtrace logging from parsed CLI arguments.

    Args:
        args: Parsed command-line arguments carrying ``log_level``.
    """
    level: int = logging.getLevelName(args.log_level.upper())
    configure_logging(build_telemetry_config(args), level=level)
