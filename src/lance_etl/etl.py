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
``processing_timestamp`` column). Derivations are materialized before the collapse and repartition so derived columns
are usable in ``partition_cols``.

Duplicate semantics across partitions are deliberately ALLOW: ``merge_insert`` stays keyed on ``key_col`` per dataset,
so a key whose partition value changes between runs leaves a stale copy in the previously-routed dataset, and deletes
only reach the currently-routed dataset. Readers and serving layers are responsible for deduplicating across datasets.

Backfills are catch-up replays: rerun this same incremental job over the historical windows with the orchestrator (for
example Airflow). Because every window is an idempotent merge keyed by vector id, a replayed or retried window converges
instead of duplicating, so no separate bulk path is needed.

An optional timestamp window filter (``window_start`` / ``window_end`` / ``window_column`` on :class:`ETLConfig`) can
narrow the rows that reach the collapse and merge steps to those whose ``window_column`` value falls within
``[window_start, window_end)``.  Both bounds are ISO-8601 strings. An absent bound means the bound is open
(no filter on that side). The
filter is applied as a Spark ``DataFrame.filter`` call immediately after the Iceberg read so Spark can push it down into
the Iceberg scan for partition pruning.

Cross-contamination is prevented structurally: the dataset URI is a validated pure function of the routing columns and
rows are shuffled by routing key, so a row can only reach its own dataset. Lance has no map type and structs are not
used downstream, so ``vectors`` and ``metadata`` are flattened with ``map_keys``/``map_values`` into ``{col}_keys`` and
``{col}_values`` parallel list columns associated by index. ``conflict_retries`` makes concurrent runs on the same
dataset safe. Any other failure propagates so the job fails fast.

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

    Supported directives are ``%Y %y %m %d %H %M %S %j %%``. Any other directive raises so a typo cannot silently
    produce wrong partition values. Literal letters are single-quoted because Spark treats bare letters as pattern
    symbols. Other literal characters pass through unchanged.

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
            pattern via :func:`strftime_to_spark_format`. Only ``%Y %y %m %d %H %M %S %j %%`` are supported.
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
        partition_cols: Columns whose values route each row to its dataset and build the dataset path
            ``base_uri/<val1>/<val2>/.../<valN>.lance`` in list order. Each path component is validated against
            ``path_component_pattern``. Every entry must exist in the source or be produced by
            ``partition_derivations``. Defaults to ``["org_id", "tenant_id", "namespace"]``, which is byte-identical
            to the historical fixed routing. Duplicate semantics across partitions are deliberately ALLOW:
            ``merge_insert`` stays keyed on ``key_col`` per dataset, so a key whose partition value changes between
            runs leaves a stale copy in the previously-routed dataset, and deletes only reach the currently-routed
            dataset. Readers and serving layers handle deduplication.
        partition_derivations: Derived partition columns materialized with ``F.date_format`` before the collapse and
            the routing repartition, so derived names are usable in ``partition_cols``. Formats are Python strftime
            patterns translated via :func:`strftime_to_spark_format`.
        vectors_col: Map column of vectors flattened into parallel arrays.
        metadata_col: Map column of metadata flattened into parallel arrays.
        ts_col: Event timestamp column used for last-write-wins collapse.
        op_col: Operation column carrying insert, update, or delete.
        delete_op_values: Operation values treated as deletes. Others upsert.
        column_types: Map of column name to target Arrow type, for types Spark cannot express such as float16.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Shuffle partitions for routing co-location.
        conflict_retries: Retry budget for concurrent merge commits.
        retry_timeout: Total time budget for conflict retries. Raised above the 30-second Lance default to give
            headroom on hot multi-tenant datasets.
        guard_updates_by_ts: Enable the strict timestamp guard on updates.
        iceberg_read_options: Extra Iceberg reader options merged into the read.
        path_component_pattern: Allowed pattern for each routing component.
        window_start: ISO-8601 lower bound (inclusive) for the source timestamp window filter. Absent means open.
        window_end: ISO-8601 upper bound (exclusive) for the source timestamp window filter. Absent means open.
        window_column: Column used for the timestamp window pushdown filter. Defaults to ``updated_at``.
    """

    base_uri: str
    telemetry: TelemetryConfig
    key_col: str = "vector_id"
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
        """Return the routing columns in dataset-path order.

        Returns:
            The partition columns routing each row to its dataset, in path order.
        """
        return list(self.partition_cols)


def validate_partition_spec(partition_cols: list[str], derivations: list[PartitionDerivation]) -> None:
    """Validate the partition routing specification at configuration-build time.

    Checks everything that does not require the source schema: at least one partition column, no duplicate partition
    columns, unique derivation names, and translatable derivation formats. Whether every partition column actually
    exists in the source (or is produced by a derivation) is checked against the real DataFrame by
    :meth:`IcebergToLanceETL.validate_schema` before any work runs.

    Args:
        partition_cols: The partition columns in dataset-path order.
        derivations: The configured derived partition columns.

    Raises:
        ValueError: If the partition column list is empty or carries duplicates, a derivation name is duplicated, or a
            derivation format uses an unsupported strftime directive.
    """
    if not partition_cols:
        raise ValueError("partition_cols must list at least one column")
    duplicate_cols: list[str] = sorted({column for column in partition_cols if partition_cols.count(column) > 1})
    if duplicate_cols:
        raise ValueError(f"partition_cols carries duplicate columns: {duplicate_cols}")
    names: list[str] = [derivation.name for derivation in derivations]
    duplicate_names: list[str] = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        raise ValueError(f"partition_derivations carries duplicate names: {duplicate_names}")
    for derivation in derivations:
        derivation.spark_format()


def dataset_uri(config: ETLConfig, *components: str) -> str:
    """Build the validated dataset URI for one routing key.

    The path is ``base_uri/<val1>/<val2>/.../<valN>.lance`` over ``config.routing_cols()`` in order, so the default
    configuration yields the historical ``{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`` layout byte-identically.

    Args:
        config: ETL configuration.
        *components: One routing value per configured partition column, in path order.

    Returns:
        The dataset URI confined to the routing-key prefix.

    Raises:
        ValueError: If the component count does not match the configured partition columns, or any component is null
            or fails validation, which prevents path traversal and routing collisions.
    """
    routing: list[str] = config.routing_cols()
    if len(components) != len(routing):
        raise ValueError(f"expected {len(routing)} routing components for {routing}, got {len(components)}")
    pattern: re.Pattern[str] = re.compile(config.path_component_pattern)
    for component in components:
        if not isinstance(component, str) or not pattern.match(component):
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = config.base_uri.rstrip("/")
    return f"{base}/{'/'.join(components)}.lance"


def cast_table(table: pa.Table, column_types: dict[str, pa.DataType]) -> pa.Table:
    """Cast named columns of a table to target Arrow types.

    Args:
        table: The table to cast.
        column_types: Map of column name to target Arrow type.

    Returns:
        The table with the requested columns cast. Others are untouched.
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


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Apply one dataset's terminal rows with merge upsert and physical delete.

    Bootstrap strategy: when the dataset does not exist yet, an empty table is written with ``lance.write_dataset(...,
    mode='append')``, which creates the dataset if absent and is race-free for concurrent first writers on the same
    routing key. The rows themselves always flow through ``merge_insert`` so a re-upsert of an existing key updates it
    in place instead of duplicating it.

    The merge ``execute()`` return dict provides authoritative row counts (``num_inserted_rows``, ``num_updated_rows``,
    ``num_deleted_rows``). We report those rather than recomputing from the source table.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: The routing key, one value per configured partition column in path order.
        group: Rows for this routing key carrying the op column.

    Returns:
        The counts of upserted (inserted + updated) and deleted rows.
    """
    uri: str = dataset_uri(config, *key)
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


def snapshot_id_bounds(
    spark: SparkSession, table: str, start_ms: int, end_ms: int
) -> tuple[int | None, int | None, bool]:
    """Resolve a wall-clock window to Iceberg snapshot-id bounds via the snapshots metadata table.

    Queries ``{table}.snapshots`` and walks the snapshots in ``committed_at`` order. The start bound is the last
    snapshot committed strictly before ``start_ms`` — the state the previous window already processed, used as the
    exclusive ``start-snapshot-id`` of an incremental append scan. The end bound is the last snapshot committed at or
    before ``end_ms`` — the inclusive ``end-snapshot-id``. Either bound is None when no snapshot satisfies it. The
    third element reports whether any snapshot was committed inside the window itself (``start_ms <= committed_at <=
    end_ms``). Callers must gate the empty-window short circuit on that flag rather than on ``start_id == end_id``,
    which conflates a genuinely empty window with bound ids that merely resolve to the same historical snapshot.

    Args:
        spark: Active Spark session.
        table: Fully qualified Iceberg table name.
        start_ms: Window start in epoch milliseconds.
        end_ms: Window end in epoch milliseconds.

    Returns:
        ``(start_id, end_id, has_new_snapshots)`` where the ids are None when no snapshot satisfies the bound and
        ``has_new_snapshots`` is True when at least one snapshot was committed within the window.
    """
    snapshots: DataFrame = spark.read.format("iceberg").load(f"{table}.snapshots")
    committed: list[tuple[int, int]] = sorted(
        (int(row["committed_at"].timestamp() * 1000), int(row["snapshot_id"]))
        for row in snapshots.select("committed_at", "snapshot_id").collect()
    )
    start_id: int | None = None
    end_id: int | None = None
    has_new_snapshots: bool = False
    for committed_ms, snapshot_id in committed:
        if committed_ms < start_ms:
            start_id = snapshot_id
        if committed_ms <= end_ms:
            end_id = snapshot_id
            if committed_ms >= start_ms:
                has_new_snapshots = True
    return start_id, end_id, has_new_snapshots


def build_stats_batch(rows: list[tuple[Any, ...]], schema: pa.Schema) -> pa.RecordBatch:
    """Build the per-partition stats record batch.

    Args:
        rows: One ``(*routing_values, upserted, deleted)`` per dataset, matching the schema's column order.
        schema: The stats schema produced by :func:`stats_schema` for the configured routing columns.

    Returns:
        A record batch conforming to the given schema.
    """
    arrays: list[pa.Array] = [pa.array([row[index] for row in rows], field.type) for index, field in enumerate(schema)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class IcebergToLanceETL:
    """Routes an Iceberg increment into per-tenant Lance datasets."""

    def __init__(self, config: ETLConfig) -> None:
        """Initialize the ETL, validating the partition specification early.

        Args:
            config: ETL configuration.

        Raises:
            ValueError: If the partition columns or derivations fail :func:`validate_partition_spec`.
        """
        validate_partition_spec(config.routing_cols(), config.partition_derivations)
        self.config: ETLConfig = config

    def read_increment(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> DataFrame:
        """Read the rows committed to an Iceberg table within a wall-clock window.

        Iceberg 1.10 rejects the ``start-timestamp`` / ``end-timestamp`` read options outside changelog scans
        (``SparkScanBuilder``: "Cannot set start-timestamp or end-timestamp for incremental scans and batch scan.
        They are only valid for changelog scans."), so the window is first resolved to snapshot ids through
        :func:`snapshot_id_bounds` over the ``{table}.snapshots`` metadata table. When a snapshot exists strictly
        before the window start, the read is an incremental append scan bounded by ``start-snapshot-id`` (exclusive)
        and ``end-snapshot-id`` (inclusive). When the table has no snapshot before the window start (first run), the
        read falls back to a full batch scan pinned to the window's last snapshot via ``snapshot-id``. When no
        snapshot at all resolves the end bound, or when no snapshot was committed inside the window, an empty
        DataFrame with the current table schema is returned. ``iceberg_read_options`` are merged into every
        non-empty read.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.

        Returns:
            The incremental rows as a DataFrame.
        """
        start_id, end_id, has_new_snapshots = snapshot_id_bounds(spark, table, start_ms, end_ms)
        if end_id is None or not has_new_snapshots:
            return spark.read.format("iceberg").load(table).limit(0)
        reader = spark.read.format("iceberg")
        if start_id is None:
            reader = reader.option("snapshot-id", str(end_id))
        else:
            reader = reader.option("start-snapshot-id", str(start_id)).option("end-snapshot-id", str(end_id))
        for option_key, option_value in self.config.iceberg_read_options.items():
            reader = reader.option(option_key, option_value)
        return reader.load(table)

    def apply_window_filter(self, source: DataFrame) -> DataFrame:
        """Apply the optional timestamp window pushdown filter.

        Filters ``source`` to rows where ``config.window_column`` falls within ``[window_start, window_end)``.  Both
        bounds are ISO-8601 strings and are optional. An absent bound leaves that side of the interval open.  The filter
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

        Every partition column must exist in the source or be produced by a configured derivation, and every
        derivation's source column must exist in the source.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If a required column is missing.
        """
        config: ETLConfig = self.config
        derived: set[str] = {derivation.name for derivation in config.partition_derivations}
        required: list[str] = [
            config.key_col,
            config.ts_col,
            config.op_col,
            config.vectors_col,
            config.metadata_col,
            *(derivation.source_col for derivation in config.partition_derivations),
            *(column for column in config.routing_cols() if column not in derived),
        ]
        missing: list[str] = [c for c in required if c not in source.columns]
        if missing:
            raise ValueError(
                f"source is missing required columns: {missing} "
                "(every partition column must exist in the source or be produced by partition_derivations)"
            )

    def derive_partition_columns(self, source: DataFrame) -> DataFrame:
        """Materialize the configured derived partition columns.

        Each derivation is applied as ``F.date_format`` over its source column before the collapse and the routing
        repartition, so derived names are usable in ``partition_cols`` and land in the written datasets as regular
        payload columns.

        Args:
            source: The incremental source DataFrame.

        Returns:
            The DataFrame with one extra string column per derivation, or the original when none are configured.
        """
        result: DataFrame = source
        for derivation in self.config.partition_derivations:
            result = result.withColumn(
                derivation.name, F.date_format(F.col(derivation.source_col), derivation.spark_format())
            )
        return result

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
            prepared: DataFrame = self.derive_partition_columns(source)
            collapsed: DataFrame = self.collapse(self.flatten_maps(prepared))
            routing: list[str] = config.routing_cols()
            routed: DataFrame = collapsed.repartition(config.num_partitions, *[F.col(c) for c in routing])
            etl_config: ETLConfig = config
            partition_stats_schema: pa.Schema = stats_schema(routing)

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
                results: list[tuple[Any, ...]] = []
                with telemetry.span("lance.etl.partition"):
                    try:
                        for key, group in group_by_routing(table, etl_config.routing_cols()):
                            upserted, deleted = apply_merge(etl_config, telemetry, key, group)
                            results.append((*key, upserted, deleted))
                    except Exception:
                        telemetry.error("etl partition failed")
                        raise
                if results:
                    yield build_stats_batch(results, partition_stats_schema)

            try:
                with driver_telemetry.timed("run.execute_ms"):
                    stats: DataFrame = routed.mapInArrow(merge_partition, schema=stats_spark_ddl(routing))
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
