"""Source-table maintenance for the Iceberg table the ETL reads.

This job optimizes the upstream Iceberg source table, which is distinct from the Lance maintenance job in
:mod:`lance_etl.maintenance` that optimizes the per-tenant Lance datasets. It runs Iceberg's own table maintenance
through the Spark SQL stored procedures exposed by the Iceberg Spark session extensions, issued as
``CALL <catalog>.system.<procedure>(...)`` statements.

Four maintenance steps run in a safe order. ``rewrite_data_files`` bin-packs many small data files into fewer larger
ones. ``rewrite_manifests`` rewrites the manifest list so manifests align with the new file layout. ``expire_snapshots``
prunes snapshot history beyond a retention horizon, the Iceberg analog of Lance version cleanup. ``remove_orphan_files``
deletes files that no live snapshot references and stays opt-in because it is the only destructive step that can delete
data files outright. Each step is wrapped with telemetry timing and a metric.

All heavy work runs distributed inside Spark executors: each ``CALL`` plans and executes as a normal Spark job. The
driver only issues the validated statements, so this job satisfies the executor rule the same way the ETL does.

The catalog is supplied to Spark exactly like the ETL's Iceberg reads: through the cluster's Spark configuration
(``spark.sql.catalog.<catalog>`` and the Iceberg session extensions) at submit time, not through any per-job flag. The
job only needs the fully-qualified table name to address the procedures.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

IDENTIFIER_COMPONENT_PATTERN: re.Pattern[str] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
"""Allowlist for a single dotted component of an Iceberg table identifier."""

DEFAULT_TARGET_FILE_SIZE_BYTES: int = 512 * 1024 * 1024
"""Target output file size for ``rewrite_data_files`` bin-packing, matching Iceberg's own 512 MiB write default."""

DEFAULT_MIN_INPUT_FILES: int = 5
"""Minimum number of files in a bin-pack group before ``rewrite_data_files`` rewrites it, matching Iceberg's default."""

DEFAULT_EXPIRE_RETAIN_LAST: int = 5
"""Snapshots always kept by ``expire_snapshots`` regardless of age, so recent rollback targets survive."""

DEFAULT_EXPIRE_OLDER_THAN_DAYS: int = 7
"""Age horizon for ``expire_snapshots``: snapshots older than this and beyond the retained count are pruned."""

DEFAULT_ORPHAN_OLDER_THAN_DAYS: int = 3
"""Age horizon for ``remove_orphan_files``, matching Iceberg's own three-day safety default."""

TIMESTAMP_LITERAL_FORMAT: str = "%Y-%m-%d %H:%M:%S"
"""Format for the typed ``TIMESTAMP`` literal passed to the age-bounded procedures."""


def validate_table_identifier(table: str) -> tuple[str, str]:
    """Validate a fully-qualified Iceberg table name and split it into catalog and in-catalog identifier.

    The identifier must be ``catalog.namespace.table`` (or more deeply nested), with at least a catalog and a table
    component. Every dotted component is checked against :data:`IDENTIFIER_COMPONENT_PATTERN` so nothing but a
    well-formed identifier ever reaches a ``CALL`` statement.

    Args:
        table: The fully-qualified table name, for example ``prod.vectors.events``.

    Returns:
        A ``(catalog, in_catalog_identifier)`` pair where the second element is the namespace-qualified name the
        procedures take as their ``table`` argument, for example ``("prod", "vectors.events")``.

    Raises:
        ValueError: If the identifier has fewer than two components or any component is not a bare identifier.
    """
    components: list[str] = table.split(".")
    if len(components) < 2:
        raise ValueError(f"table must be a qualified catalog.namespace.table identifier, got {table!r}")
    for component in components:
        if not IDENTIFIER_COMPONENT_PATTERN.fullmatch(component):
            raise ValueError(f"invalid Iceberg identifier component {component!r} in {table!r}")
    return components[0], ".".join(components[1:])


@dataclass
class IcebergOptimizeConfig:
    """Configuration for the Iceberg source-table optimization job.

    Attributes:
        table: Fully-qualified Iceberg table name ``catalog.namespace.table``.
        telemetry: Telemetry configuration created once per process inside :meth:`IcebergOptimizer.run`.
        rewrite_data_files: Bin-pack small data files into larger ones.
        rewrite_manifests: Rewrite manifests to align with the current file layout.
        expire_snapshots: Prune snapshot history beyond the retention horizon.
        remove_orphan_files: Delete files no live snapshot references. Opt-in because it is the only step that can
            delete data outright. Off by default.
        target_file_size_bytes: Target output file size for ``rewrite_data_files``.
        min_input_files: Minimum files in a bin-pack group before ``rewrite_data_files`` rewrites it.
        expire_retain_last: Snapshots always retained by ``expire_snapshots`` regardless of age.
        expire_older_than_days: Age horizon in days for ``expire_snapshots``.
        orphan_older_than_days: Age horizon in days for ``remove_orphan_files``, matching Iceberg's safety default.
    """

    table: str
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    rewrite_data_files: bool = True
    rewrite_manifests: bool = True
    expire_snapshots: bool = True
    remove_orphan_files: bool = False
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES
    min_input_files: int = DEFAULT_MIN_INPUT_FILES
    expire_retain_last: int = DEFAULT_EXPIRE_RETAIN_LAST
    expire_older_than_days: int = DEFAULT_EXPIRE_OLDER_THAN_DAYS
    orphan_older_than_days: int = DEFAULT_ORPHAN_OLDER_THAN_DAYS


@dataclass
class IcebergStepResult:
    """Outcome of one Iceberg maintenance step.

    Attributes:
        step: The procedure name, for example ``rewrite_data_files``.
        ran: Whether the step was enabled and executed.
        duration_seconds: Wall time of the ``CALL``, or 0.0 when the step did not run.
        metrics: Integer result columns reported by the procedure, for example ``rewritten_data_files_count``.
    """

    step: str
    ran: bool
    duration_seconds: float
    metrics: dict[str, int] = field(default_factory=dict)


@dataclass
class IcebergOptimizeReport:
    """Aggregate report of an Iceberg source-table optimization run.

    Attributes:
        table: The fully-qualified table that was optimized.
        steps: The per-step results in execution order.
    """

    table: str
    steps: list[IcebergStepResult] = field(default_factory=list)


def timestamp_literal(days_ago: int) -> str:
    """Build a UTC ``TIMESTAMP`` literal for ``now - days_ago``.

    Args:
        days_ago: Whole days before the current instant. ``0`` yields the current instant.

    Returns:
        A wall-clock timestamp string in :data:`TIMESTAMP_LITERAL_FORMAT`, safe to embed in a typed ``TIMESTAMP``
        literal.
    """
    moment: datetime = datetime.now(UTC) - timedelta(days=days_ago)
    return moment.strftime(TIMESTAMP_LITERAL_FORMAT)


class IcebergOptimizer:
    """Run the enabled Iceberg maintenance procedures over the source table."""

    def __init__(self, config: IcebergOptimizeConfig) -> None:
        """Initialize the optimizer and validate the table identifier.

        Args:
            config: The optimization configuration.

        Raises:
            ValueError: If the configured table is not a well-formed qualified identifier.
        """
        self.config: IcebergOptimizeConfig = config
        self.catalog, self.table_argument = validate_table_identifier(config.table)

    def run(self, spark: SparkSession) -> IcebergOptimizeReport:
        """Run every enabled maintenance step in a safe order and return the per-step report.

        The order is fixed: bin-pack data files, then rewrite manifests so they align with the new layout, then expire
        snapshots beyond the retention horizon, then remove orphan files when explicitly enabled. Each step is timed and
        emits a metric. The driver only issues the validated ``CALL`` statements. The procedures plan and execute
        distributed across Spark executors.

        Args:
            spark: Active Spark session whose configuration carries the Iceberg catalog and session extensions.

        Returns:
            The aggregate report with one entry per executed step.
        """
        telemetry: Telemetry = Telemetry.create(self.config.telemetry)
        report: IcebergOptimizeReport = IcebergOptimizeReport(table=self.config.table)
        with telemetry.span("iceberg.optimize.run", resource=self.config.table):
            if self.config.rewrite_data_files:
                report.steps.append(self.rewrite_data_files(spark, telemetry))
            if self.config.rewrite_manifests:
                report.steps.append(self.rewrite_manifests(spark, telemetry))
            if self.config.expire_snapshots:
                report.steps.append(self.expire_snapshots(spark, telemetry))
            if self.config.remove_orphan_files:
                report.steps.append(self.remove_orphan_files(spark, telemetry))
        logger.info("iceberg optimize complete for %s: %d steps", self.config.table, len(report.steps))
        return report

    def call(self, spark: SparkSession, step: str, statement: str, telemetry: Telemetry) -> IcebergStepResult:
        """Execute one ``CALL`` statement, timed and metered, and parse its integer result columns.

        Args:
            spark: Active Spark session.
            step: The procedure name used for metrics and the step result.
            statement: The fully-formed ``CALL`` statement.
            telemetry: Telemetry facade for the current process.

        Returns:
            The step result carrying the duration and the integer result columns the procedure reported.
        """
        logger.info("iceberg optimize step %s on %s", step, self.config.table)
        started: float = time.perf_counter()
        with telemetry.timed(f"iceberg.optimize.{step}_ms", tags=[f"table:{self.config.table}"]):
            rows = spark.sql(statement).collect()
        duration: float = time.perf_counter() - started
        metrics: dict[str, int] = {}
        if step == "remove_orphan_files":
            metrics["orphan_files_removed"] = len(rows)
        elif rows:
            for key, value in rows[0].asDict().items():
                if isinstance(value, int):
                    metrics[key] = int(value)
        telemetry.incr(f"iceberg.optimize.{step}", tags=[f"table:{self.config.table}"])
        for key, value in metrics.items():
            telemetry.gauge(f"iceberg.optimize.{step}.{key}", float(value), tags=[f"table:{self.config.table}"])
        return IcebergStepResult(step=step, ran=True, duration_seconds=duration, metrics=metrics)

    def rewrite_data_files(self, spark: SparkSession, telemetry: Telemetry) -> IcebergStepResult:
        """Bin-pack small data files into larger ones via ``rewrite_data_files``.

        Args:
            spark: Active Spark session.
            telemetry: Telemetry facade for the current process.

        Returns:
            The step result.
        """
        options: str = (
            f"map('min-input-files', '{int(self.config.min_input_files)}', "
            f"'target-file-size-bytes', '{int(self.config.target_file_size_bytes)}')"
        )
        statement: str = (
            f"CALL {self.catalog}.system.rewrite_data_files(table => '{self.table_argument}', options => {options})"
        )
        return self.call(spark, "rewrite_data_files", statement, telemetry)

    def rewrite_manifests(self, spark: SparkSession, telemetry: Telemetry) -> IcebergStepResult:
        """Rewrite manifests to align with the current file layout via ``rewrite_manifests``.

        Args:
            spark: Active Spark session.
            telemetry: Telemetry facade for the current process.

        Returns:
            The step result.
        """
        statement: str = f"CALL {self.catalog}.system.rewrite_manifests(table => '{self.table_argument}')"
        return self.call(spark, "rewrite_manifests", statement, telemetry)

    def expire_snapshots(self, spark: SparkSession, telemetry: Telemetry) -> IcebergStepResult:
        """Prune snapshot history beyond the retention horizon via ``expire_snapshots``.

        Retains at least :attr:`IcebergOptimizeConfig.expire_retain_last` snapshots regardless of age and expires
        snapshots older than ``now - expire_older_than_days`` beyond that count.

        Args:
            spark: Active Spark session.
            telemetry: Telemetry facade for the current process.

        Returns:
            The step result.
        """
        older_than: str = timestamp_literal(self.config.expire_older_than_days)
        statement: str = (
            f"CALL {self.catalog}.system.expire_snapshots("
            f"table => '{self.table_argument}', "
            f"older_than => TIMESTAMP '{older_than}', "
            f"retain_last => {int(self.config.expire_retain_last)})"
        )
        return self.call(spark, "expire_snapshots", statement, telemetry)

    def remove_orphan_files(self, spark: SparkSession, telemetry: Telemetry) -> IcebergStepResult:
        """Delete files no live snapshot references via ``remove_orphan_files``.

        Only files older than ``now - orphan_older_than_days`` are removed, matching Iceberg's own safety default so an
        in-flight write is never mistaken for an orphan.

        Args:
            spark: Active Spark session.
            telemetry: Telemetry facade for the current process.

        Returns:
            The step result, whose ``orphan_files_removed`` metric counts the removed files.
        """
        older_than: str = timestamp_literal(self.config.orphan_older_than_days)
        statement: str = (
            f"CALL {self.catalog}.system.remove_orphan_files("
            f"table => '{self.table_argument}', "
            f"older_than => TIMESTAMP '{older_than}')"
        )
        return self.call(spark, "remove_orphan_files", statement, telemetry)
