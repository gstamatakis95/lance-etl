"""Iceberg-to-Lance ETL routing changes into per-tenant datasets.

Reads a time range of changes from an Iceberg table that carries an operation column (insert, update, delete), flattens
the two map columns into struct-free parallel arrays, collapses to the last-write-wins terminal state per vector id,
casts columns to caller-supplied Arrow types, and applies each routing key's rows to exactly one Lance dataset
identified by the configured ``partition_cols`` (default ``(org_id, tenant_id, namespace)``) with a ``merge_insert``
upsert plus a ``when_matched_delete``.

Routing is dynamic: ``ETLConfig.partition_cols`` lists the columns whose values build the dataset path
``base_uri/<val1>/<val2>/.../<valN>.lance`` in order, and the collapse window, the routing repartition, and the
per-partition Arrow ``group_by`` all derive from that one list. Partition columns may also be derived from source
columns with ``ETLConfig.partition_derivations`` (for example an ``event_date`` day partition derived from a
``processing_timestamp`` column); derivations are materialized before the collapse and repartition so derived columns
are usable in ``partition_cols``.

Duplicate semantics across partitions are deliberately ALLOW: ``merge_insert`` stays keyed on ``key_col`` per dataset,
so a key whose partition value changes between runs leaves a stale copy in the previously-routed dataset, and deletes
only reach the currently-routed dataset. Readers and serving layers are responsible for deduplicating across datasets.

Backfills are catch-up replays: rerun this same incremental job over the historical windows with the orchestrator (for
example Airflow). Because every window is an idempotent merge keyed by vector id, a replayed or retried window converges
instead of duplicating, so no separate bulk path is needed.

An optional timestamp window filter (``window_start`` / ``window_end`` / ``window_column`` on :class:`ETLConfig`) can
narrow the rows that reach the collapse and merge steps to those whose ``window_column`` value falls within
``[window_start, window_end)``.  Both bounds are ISO-8601 strings; absent means the bound is open (full-table).  The
filter is applied as a Spark ``DataFrame.filter`` call immediately after the Iceberg read so Spark can push it down into
the Iceberg scan for partition pruning.

Cross-contamination is prevented structurally: the dataset URI is a validated pure function of the routing columns and
rows are shuffled by routing key, so a row can only reach its own dataset. Lance has no map type and structs are not
used downstream, so ``vectors`` and ``metadata`` are flattened with ``map_keys``/``map_values`` into ``{col}_keys`` and
``{col}_values`` parallel list columns associated by index. ``conflict_retries`` makes concurrent runs on the same
dataset safe; any other failure propagates so the job fails fast.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_PARTITION_COLS: tuple[str, str, str] = ("org_id", "tenant_id", "namespace")

STRFTIME_TO_SPARK: dict[str, str] = {
    "Y": "yyyy",
    "y": "yy",
    "m": "MM",
    "d": "dd",
    "H": "HH",
    "M": "mm",
    "S": "ss",
    "j": "DDD",
    "%": "%",
}


def strftime_to_spark_format(strftime_format: str) -> str:
    """Translate a Python strftime pattern into a Spark ``date_format`` pattern.

    Supported directives are ``%Y %y %m %d %H %M %S %j %%``; any other directive raises so a typo cannot silently
    produce wrong partition values. Literal letters are single-quoted because Spark treats bare letters as pattern
    symbols; other literal characters pass through unchanged.

    Args:
        strftime_format: The strftime pattern, for example ``%Y-%m-%d``.

    Returns:
        The equivalent Spark datetime pattern, for example ``yyyy-MM-dd``.

    Raises:
        ValueError: If the pattern ends with a bare ``%`` or uses an unsupported directive.
    """
    parts: list[str] = []
    index: int = 0
    while index < len(strftime_format):
        char: str = strftime_format[index]
        if char == "%":
            if index + 1 >= len(strftime_format):
                raise ValueError(f"strftime format {strftime_format!r} ends with a bare '%'")
            directive: str = strftime_format[index + 1]
            if directive not in STRFTIME_TO_SPARK:
                raise ValueError(f"unsupported strftime directive %{directive} in {strftime_format!r}")
            parts.append(STRFTIME_TO_SPARK[directive])
            index += 2
        elif char.isalpha():
            parts.append(f"'{char}'")
            index += 1
        else:
            parts.append(char)
            index += 1
    return "".join(parts)


@dataclass(frozen=True)
class PartitionDerivation:
    """A partition column derived from a source column before routing.

    The derivation is applied as ``F.date_format(F.col(source_col), spark_format).alias(name)`` before the collapse
    and the routing repartition, so the derived column can appear in ``ETLConfig.partition_cols`` and also lands in
    the written dataset as a regular payload column. The canonical use case is a ``processing_timestamp`` source
    column deriving an ``event_date`` day partition.

    Attributes:
        name: Name of the derived column added to the DataFrame.
        source_col: Source timestamp column the derivation reads.
        strftime_format: Python strftime pattern (for example ``%Y-%m-%d``) translated to Spark's ``date_format``
            pattern via :func:`strftime_to_spark_format`; only ``%Y %y %m %d %H %M %S %j %%`` are supported.
    """

    name: str
    source_col: str
    strftime_format: str

    def spark_format(self) -> str:
        """Return the Spark ``date_format`` pattern for this derivation.

        Returns:
            The translated Spark datetime pattern.

        Raises:
            ValueError: If ``strftime_format`` uses an unsupported directive.
        """
        return strftime_to_spark_format(self.strftime_format)


def stats_schema(routing_cols: list[str]) -> pa.Schema:
    """Build the per-dataset stats schema for a routing-column list.

    Args:
        routing_cols: The routing columns in dataset-path order.

    Returns:
        A schema with one string column per routing column plus ``upserted`` and ``deleted`` counters.
    """
    fields: list[tuple[str, pa.DataType]] = [(column, pa.string()) for column in routing_cols]
    fields.extend([("upserted", pa.int64()), ("deleted", pa.int64())])
    return pa.schema(fields)


def stats_spark_ddl(routing_cols: list[str]) -> str:
    """Build the Spark DDL string matching :func:`stats_schema`.

    Args:
        routing_cols: The routing columns in dataset-path order.

    Returns:
        A DDL string usable as the ``mapInArrow`` output schema.
    """
    columns: str = ", ".join(f"`{column}` string" for column in routing_cols)
    return f"{columns}, `upserted` bigint, `deleted` bigint"


STATS_SCHEMA: pa.Schema = stats_schema(list(DEFAULT_PARTITION_COLS))


@dataclass
class ETLConfig:
    """Configuration for :class:`IcebergToLanceETL`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live.
        telemetry: Telemetry configuration.
        key_col: Unique vector id column and per-dataset merge key.
        org_col: Legacy organisation routing column; feeds the default ``partition_cols``.
        tenant_col: Legacy tenant routing column; feeds the default ``partition_cols``.
        namespace_col: Legacy namespace routing column; feeds the default ``partition_cols``.
        partition_cols: Columns whose values route each row to its dataset and build the dataset path
            ``base_uri/<val1>/<val2>/.../<valN>.lance`` in list order; each path component is validated against
            ``path_component_pattern``. Every entry must exist in the source or be produced by
            ``partition_derivations``. Defaults to ``["org_id", "tenant_id", "namespace"]`` (or the legacy
            ``org_col`` / ``tenant_col`` / ``namespace_col`` overrides), which is byte-identical to the historical
            fixed routing. Duplicate semantics across partitions are deliberately ALLOW: ``merge_insert`` stays
            keyed on ``key_col`` per dataset, so a key whose partition value changes between runs leaves a stale
            copy in the previously-routed dataset, and deletes only reach the currently-routed dataset; readers and
            serving layers handle deduplication.
        partition_derivations: Derived partition columns materialized with ``F.date_format`` before the collapse and
            the routing repartition, so derived names are usable in ``partition_cols``; formats are Python strftime
            patterns translated via :func:`strftime_to_spark_format`.
        vectors_col: Map column of vectors flattened into parallel arrays.
        metadata_col: Map column of metadata flattened into parallel arrays.
        ts_col: Event timestamp column used for last-write-wins collapse.
        op_col: Operation column carrying insert, update, or delete.
        delete_op_values: Operation values treated as deletes; others upsert.
        column_types: Map of column name to target Arrow type, for types Spark cannot express such as float16.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Shuffle partitions for routing co-location.
        conflict_retries: Retry budget for concurrent merge commits.
        retry_timeout: Total time budget for conflict retries; raised above the 30-second Lance default to give headroom
            on hot multi-tenant datasets.
        guard_updates_by_ts: Enable the strict timestamp guard on updates.
        iceberg_read_options: Extra Iceberg reader options merged into the read.
        path_component_pattern: Allowed pattern for each routing component.
        window_start: ISO-8601 lower bound (inclusive) for the source timestamp window filter; absent means open.
        window_end: ISO-8601 upper bound (exclusive) for the source timestamp window filter; absent means open.
        window_column: Column used for the timestamp window pushdown filter; defaults to ``updated_at``.
    """

    base_uri: str
    telemetry: TelemetryConfig
    key_col: str = "vector_id"
    org_col: str = "org_id"
    tenant_col: str = "tenant_id"
    namespace_col: str = "namespace"
    partition_cols: list[str] = field(default_factory=lambda: list(DEFAULT_PARTITION_COLS))
    partition_derivations: list[PartitionDerivation] = field(default_factory=list)
    vectors_col: str = "vectors"
    metadata_col: str = "metadata"
    ts_col: str = "timestamp"
    op_col: str = "op"
    delete_op_values: list[str] = field(default_factory=lambda: ["delete", "DELETE", "d"])
    column_types: dict[str, pa.DataType] = field(default_factory=dict)
    storage_options: dict[str, Any] | None = None
    num_partitions: int = 512
    conflict_retries: int = 10
    retry_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=120))
    guard_updates_by_ts: bool = False
    iceberg_read_options: dict[str, str] = field(default_factory=dict)
    path_component_pattern: str = r"^[A-Za-z0-9._-]+$"
    window_start: str | None = None
    window_end: str | None = None
    window_column: str = "updated_at"

    def routing_cols(self) -> list[str]:
        """Return the routing columns in path order.

        Returns:
            The organisation, tenant, and namespace column names.
        """
        return [self.org_col, self.tenant_col, self.namespace_col]


def dataset_uri(config: ETLConfig, org_id: str, tenant_id: str, namespace: str) -> str:
    """Build the validated dataset URI for one routing key.

    Args:
        config: ETL configuration.
        org_id: Organisation routing value.
        tenant_id: Tenant routing value.
        namespace: Namespace routing value.

    Returns:
        The dataset URI confined to the routing-key prefix.

    Raises:
        ValueError: If any routing component is null or fails validation, which prevents path traversal and routing
            collisions.
    """
    pattern: re.Pattern[str] = re.compile(config.path_component_pattern)
    for component in (org_id, tenant_id, namespace):
        if component is None or not pattern.match(component):
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = config.base_uri.rstrip("/")
    return f"{base}/{org_id}/{tenant_id}/{namespace}.lance"


def cast_table(table: pa.Table, column_types: dict[str, pa.DataType]) -> pa.Table:
    """Cast named columns of a table to target Arrow types.

    Args:
        table: The table to cast.
        column_types: Map of column name to target Arrow type.

    Returns:
        The table with the requested columns cast; others untouched.
    """
    if not column_types:
        return table
    result: pa.Table = table
    for name, target in column_types.items():
        if name in result.column_names:
            index: int = result.schema.get_field_index(name)
            result = result.set_column(index, name, result.column(name).cast(target))
    return result


def group_by_routing(table: pa.Table, routing_cols: list[str]) -> Iterator[tuple[tuple[Any, ...], pa.Table]]:
    """Yield each routing key's rows from a partition table.

    Args:
        table: The materialized partition table.
        routing_cols: The routing key columns.

    Yields:
        ``(key_values, sub_table)`` for each distinct routing key.
    """
    combos: pa.Table = table.group_by(routing_cols).aggregate([])
    for index in range(combos.num_rows):
        key: tuple[Any, ...] = tuple(combos[c][index].as_py() for c in routing_cols)
        mask: pa.Array | None = None
        for position, column_name in enumerate(routing_cols):
            equals: pa.Array = pc.equal(table[column_name], key[position])
            mask = equals if mask is None else pc.and_(mask, equals)
        yield key, table.filter(mask)


def build_delete_predicate(key_col: str, keys: pa.Array) -> str:
    """Build a SQL predicate string that matches any of the given key values.

    Lance's ``LanceDataset.delete`` accepts a SQL string predicate.  This helper renders the key column membership test
    so the delete path does not need to materialise a source table and run a merge.

    Args:
        key_col: Name of the key column in the target dataset.
        keys: Arrow array of key values to delete.

    Returns:
        A SQL ``IN (...)`` predicate string.
    """
    quoted: list[str] = [f"'{v}'" for v in keys.to_pylist()]
    joined: str = ", ".join(quoted)
    return f"{key_col} IN ({joined})"


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, str, str], group: pa.Table) -> tuple[int, int]:
    """Apply one dataset's terminal rows with merge upsert and physical delete.

    Bootstrap strategy: when the dataset does not exist yet, an empty table is written with ``lance.write_dataset(...,
    mode='append')``, which creates the dataset if absent and is race-free for concurrent first writers on the same
    routing key. The rows themselves always flow through ``merge_insert`` so a re-upsert of an existing key updates it
    in place instead of duplicating it.

    The merge ``execute()`` return dict provides authoritative row counts (``num_inserted_rows``, ``num_updated_rows``,
    ``num_deleted_rows``); we report those rather than recomputing from the source table.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: The ``(org_id, tenant_id, namespace)`` routing key.
        group: Rows for this routing key carrying the op column.

    Returns:
        The counts of upserted (inserted + updated) and deleted rows.
    """
    uri: str = dataset_uri(config, key[0], key[1], key[2])
    is_delete: pa.Array = pc.is_in(group[config.op_col], value_set=pa.array(config.delete_op_values))
    payload_cols: list[str] = [c for c in group.column_names if c != config.op_col]
    upserts: pa.Table = cast_table(group.filter(pc.invert(is_delete)).select(payload_cols), config.column_types)
    deletes: pa.Table = cast_table(group.filter(is_delete).select([config.key_col]), config.column_types)

    upserted: int = 0
    deleted: int = 0

    if upserts.num_rows:
        with telemetry.timed("dataset.merge_ms"):
            try:
                dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            except (FileNotFoundError, ValueError):
                dataset = lance.write_dataset(
                    upserts.schema.empty_table(), uri, mode="append", storage_options=config.storage_options
                )
            try:
                builder = dataset.merge_insert(on=[config.key_col])
                if config.guard_updates_by_ts:
                    builder = builder.when_matched_update_all(
                        condition=f"source.{config.ts_col} > target.{config.ts_col}"
                    )
                else:
                    builder = builder.when_matched_update_all()
                stats: dict[str, Any] = (
                    builder.when_not_matched_insert_all()
                    .conflict_retries(config.conflict_retries)
                    .retry_timeout(config.retry_timeout)
                    .execute(upserts)
                )
                upserted = stats.get("num_inserted_rows", 0) + stats.get("num_updated_rows", 0)
                telemetry.incr("dataset.merged")
            except Exception:
                telemetry.incr("dataset.merge_error")
                raise

    if deletes.num_rows:
        try:
            dataset = lance.dataset(uri, storage_options=config.storage_options)
        except (FileNotFoundError, ValueError):
            dataset = None
        if dataset is not None:
            with telemetry.timed("dataset.delete_ms"):
                predicate: str = build_delete_predicate(config.key_col, deletes[config.key_col])
                delete_stats: dict[str, Any] = dataset.delete(
                    predicate,
                    conflict_retries=config.conflict_retries,
                    retry_timeout=config.retry_timeout,
                )
                deleted = delete_stats.get("num_deleted_rows", 0)

    telemetry.distribution("dataset.upserted", upserted)
    telemetry.distribution("dataset.deleted", deleted)
    return upserted, deleted


def build_stats_batch(rows: list[tuple[str, str, str, int, int]]) -> pa.RecordBatch:
    """Build the per-partition stats record batch.

    Args:
        rows: One ``(org, tenant, namespace, upserted, deleted)`` per dataset.

    Returns:
        A record batch conforming to ``STATS_SCHEMA``.
    """
    return pa.RecordBatch.from_arrays(
        [
            pa.array([r[0] for r in rows], pa.string()),
            pa.array([r[1] for r in rows], pa.string()),
            pa.array([r[2] for r in rows], pa.string()),
            pa.array([r[3] for r in rows], pa.int64()),
            pa.array([r[4] for r in rows], pa.int64()),
        ],
        schema=STATS_SCHEMA,
    )


class IcebergToLanceETL:
    """Routes an Iceberg increment into per-tenant Lance datasets."""

    def __init__(self, config: ETLConfig) -> None:
        """Initialize the ETL.

        Args:
            config: ETL configuration.
        """
        self.config: ETLConfig = config

    def read_increment(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> DataFrame:
        """Read appended rows in a time range using Iceberg incremental options.

        Uses ``start-timestamp`` and ``end-timestamp`` in epoch milliseconds. The exact option names depend on the
        Iceberg version; override or extend via ``iceberg_read_options`` if the deployment differs.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.

        Returns:
            The incremental rows as a DataFrame.
        """
        reader = (
            spark.read.format("iceberg").option("start-timestamp", str(start_ms)).option("end-timestamp", str(end_ms))
        )
        for option_key, option_value in self.config.iceberg_read_options.items():
            reader = reader.option(option_key, option_value)
        return reader.load(table)

    def apply_window_filter(self, source: DataFrame) -> DataFrame:
        """Apply the optional timestamp window pushdown filter.

        Filters ``source`` to rows where ``config.window_column`` falls within ``[window_start, window_end)``.  Both
        bounds are ISO-8601 strings and are optional; an absent bound leaves that side of the interval open.  The filter
        is applied as a ``DataFrame.filter`` SQL-string predicate before any shuffle so Spark can push it down into the
        Iceberg scan for partition pruning.  When neither bound is set the DataFrame is returned unchanged
        (full-table behaviour).

        Args:
            source: The incremental source DataFrame produced by :meth:`read_increment`.

        Returns:
            The filtered DataFrame, or the original if no window bounds are configured.
        """
        config: ETLConfig = self.config
        if config.window_start is None and config.window_end is None:
            return source
        filtered: DataFrame = source
        if config.window_start is not None:
            filtered = filtered.filter(f"`{config.window_column}` >= TIMESTAMP '{config.window_start}'")
        if config.window_end is not None:
            filtered = filtered.filter(f"`{config.window_column}` < TIMESTAMP '{config.window_end}'")
        return filtered

    def validate_schema(self, source: DataFrame) -> None:
        """Validate that the source carries every required column.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If a required column is missing.
        """
        config: ETLConfig = self.config
        required: list[str] = [
            config.key_col,
            config.ts_col,
            config.op_col,
            config.vectors_col,
            config.metadata_col,
            *config.routing_cols(),
        ]
        missing: list[str] = [c for c in required if c not in source.columns]
        if missing:
            raise ValueError(f"source is missing required columns: {missing}")

    def flatten_maps(self, source: DataFrame) -> DataFrame:
        """Flatten the vector and metadata maps into struct-free parallel arrays.

        Args:
            source: A DataFrame containing the two map columns.

        Returns:
            The DataFrame with both map columns replaced by parallel arrays.
        """
        config: ETLConfig = self.config
        return (
            source.withColumn(f"{config.vectors_col}_keys", F.map_keys(F.col(config.vectors_col)))
            .withColumn(f"{config.vectors_col}_values", F.map_values(F.col(config.vectors_col)))
            .drop(config.vectors_col)
            .withColumn(f"{config.metadata_col}_keys", F.map_keys(F.col(config.metadata_col)))
            .withColumn(f"{config.metadata_col}_values", F.map_values(F.col(config.metadata_col)))
            .drop(config.metadata_col)
        )

    def collapse(self, source: DataFrame) -> DataFrame:
        """Reduce to the last-write-wins terminal event per id within a tenant.

        Args:
            source: The flattened source DataFrame.

        Returns:
            One row per routing key and vector id, carrying the terminal op.
        """
        config: ETLConfig = self.config
        partition_by: list[Column] = [F.col(c) for c in config.routing_cols()]
        partition_by.append(F.col(config.key_col))
        window: WindowSpec = Window.partitionBy(*partition_by).orderBy(F.col(config.ts_col).desc())
        return source.withColumn("row_num", F.row_number().over(window)).where(F.col("row_num") == 1).drop("row_num")

    def run(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> None:
        """Read, transform, and route one time range of Iceberg changes.

        After the Iceberg incremental read an optional timestamp window filter is applied via
        :meth:`apply_window_filter` before the collapse and routing shuffle.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.apply_window_filter(self.read_increment(spark, table, start_ms, end_ms)))

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Transform and route a pre-read increment.

        Args:
            source: A source DataFrame carrying the operation column.
        """
        config: ETLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.etl.run") as run_span:
            self.validate_schema(source)
            collapsed: DataFrame = self.collapse(self.flatten_maps(source))
            routed: DataFrame = collapsed.repartition(config.num_partitions, *[F.col(c) for c in config.routing_cols()])
            etl_config: ETLConfig = config

            def merge_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                """Merge one Spark partition's datasets on an executor.

                Args:
                    batches: Arrow batches for this task.

                Yields:
                    One stats record batch when the partition wrote any dataset.
                """
                collected: list[pa.RecordBatch] = [b for b in batches if b.num_rows]
                if not collected:
                    return
                telemetry: Telemetry = Telemetry.create(etl_config.telemetry)
                table: pa.Table = pa.Table.from_batches(collected)
                results: list[tuple[str, str, str, int, int]] = []
                with telemetry.span("lance.etl.partition"):
                    try:
                        for key, group in group_by_routing(table, etl_config.routing_cols()):
                            upserted, deleted = apply_merge(etl_config, telemetry, key, group)
                            results.append((key[0], key[1], key[2], upserted, deleted))
                    except Exception:
                        telemetry.error("etl partition failed")
                        raise
                if results:
                    yield build_stats_batch(results)

            try:
                with driver_telemetry.timed("run.execute_ms"):
                    stats: DataFrame = routed.mapInArrow(merge_partition, schema=STATS_SCHEMA)
                    totals = stats.agg(
                        F.count(F.lit(1)).alias("datasets"),
                        F.coalesce(F.sum("upserted"), F.lit(0)).alias("upserted"),
                        F.coalesce(F.sum("deleted"), F.lit(0)).alias("deleted"),
                    ).collect()[0]
            except Exception:
                driver_telemetry.error("etl run failed")
                raise

            run_span.set_tag("datasets", totals["datasets"])
            driver_telemetry.gauge("run.datasets", totals["datasets"])
            driver_telemetry.gauge("run.upserted", totals["upserted"])
            driver_telemetry.gauge("run.deleted", totals["deleted"])
            logger.info(
                "incremental run: %s datasets, %s upserts, %s deletes",
                totals["datasets"],
                totals["upserted"],
                totals["deleted"],
            )
