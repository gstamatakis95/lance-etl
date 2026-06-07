"""Iceberg-to-Lance ETL routing changes into per-tenant datasets.

Reads a time range of changes from an Iceberg table that carries an operation column (insert, update, delete), pivots
the declared named vectors and texts out of their map columns into concrete indexable columns, flattens the metadata
map into struct-free parallel arrays, collapses to the last-write-wins terminal state per vector id, casts columns to
caller-supplied Arrow types, and applies each routing key's rows to exactly one Lance dataset identified by the
configured ``partition_cols`` (default ``(org_id, tenant_id, namespace)``) with a ``merge_insert`` upsert plus a
``when_matched_delete``.

Routing is dynamic: ``ETLConfig.partition_cols`` lists the columns whose values build the dataset path
``base_uri/<val1>/<val2>/.../<valN>.lance`` in order, and the collapse window, the routing repartition, and the
per-partition Arrow ``group_by`` all derive from that one list. The default routing is the stable
``(org_id, tenant_id, namespace)`` identity trio.

Each key lives in exactly one dataset, so the ``merge_insert`` keyed on ``key_col`` is the sole dedup mechanism: a
re-upsert of an existing key updates it in place and a delete reaches the one dataset that holds it. No cross-dataset
reader deduplication is required.

Backfills are catch-up replays: rerun this same incremental job over the historical windows with the orchestrator (for
example Airflow). Because every window is an idempotent merge keyed by vector id, a replayed or retried window converges
instead of duplicating, so no separate bulk path is needed.

An optional timestamp window filter (``window_start`` / ``window_end`` / ``window_column`` on :class:`ETLConfig`) can
narrow the rows that reach the collapse and merge steps to those whose ``window_column`` value falls within
``[window_start, window_end)``.  Both bounds are ISO-8601 strings. An absent bound means the bound is open
(no filter on that side). The filter is applied as a Spark ``DataFrame.filter`` call immediately after the Iceberg read
so Spark can push it down into the Iceberg scan for partition pruning.

The single canonical time clock is the source event timestamp column named by ``ETLConfig.ts_col`` (default
``"timestamp"``). Date-range queries are expressed as scalar range filters on that column, which can be pruned by a
BTREE scalar index. There is no derived date column and no ingest-time column: the event timestamp is authoritative for
ordering, collapse, and time-bounded serving. See ADR 0016 for the rationale and tradeoffs.

Cross-contamination is prevented structurally: the dataset URI is a validated pure function of the routing columns and
rows are shuffled by routing key, so a row can only reach its own dataset. Lance has no map type and structs are not
used downstream, so the maps are unpacked before write. The ``vectors`` and ``texts`` maps are pivoted: each name in
``vector_fields`` becomes a concrete column holding ``vectors[name]`` (cast to a fixed-size-list the IVF_RQ index can
target) and each name in ``text_fields`` becomes a concrete string column holding ``texts[name]`` (the INVERTED/FTS
index can target it). Undeclared map keys are dropped and a declared key absent from a row yields NULL for that column.
The ``metadata`` map stays payload, flattened with ``map_keys``/``map_values`` into ``{col}_keys`` and ``{col}_values``
parallel list columns associated by index. ``conflict_retries`` makes concurrent runs on the same dataset safe. Any
other failure propagates so the job fails fast.

Small-and-big efficiency: the per-tenant population is power-law shaped (tens of thousands of orgs, most tiny, a few
huge), so the ETL never does per-row work on the driver. The driver only resolves the Iceberg snapshot bounds from
table metadata, short-circuits to an empty read when no snapshot landed in the window, and broadcasts the routing
plan. All collapse, routing, and merge work runs in executors: rows shuffle by routing key into ``num_partitions``
co-located partitions, and ``merge_partition`` groups each partition's rows by routing key and applies one keyed,
idempotent ``merge_insert`` per dataset. A tiny org's increment is a small group merged in process on one executor at
near-zero cost, a huge org's increment co-locates to its partition and merges there, and an org with no rows in the
window produces no group and touches no dataset. Bootstrapping a brand-new tiny dataset is a single empty append plus
merge, never a cluster-wide fan-out.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType
from pyspark.sql.window import Window, WindowSpec

from lance_etl.telemetry import (
    DEFAULT_CONFLICT_RETRIES,
    DEFAULT_RETRY_TIMEOUT,
    Telemetry,
    TelemetryConfig,
    commit_with_retries,
)

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_PARTITION_COLS: tuple[str, str, str] = ("org_id", "tenant_id", "namespace")

PATH_COMPONENT_PATTERN: str = r"^[A-Za-z0-9._-]+$"
"""Allowed pattern for each routing-key path component.

This is a security invariant baked as a constant rather than exposed as a config field so a caller cannot silently
weaken the allowlist that confines every dataset URI to its routing-key prefix and prevents path traversal and routing
collisions.
"""


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


@dataclass
class ETLConfig:
    """Configuration for :class:`IcebergToLanceETL`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live.
        telemetry: Telemetry configuration.
        key_col: Unique vector id column and per-dataset merge key.
        partition_cols: Columns whose values route each row to its dataset and build the dataset path
            ``base_uri/<val1>/<val2>/.../<valN>.lance`` in list order. Each path component is validated against
            :data:`PATH_COMPONENT_PATTERN`. Every entry must exist in the source. Defaults to
            ``["org_id", "tenant_id", "namespace"]``, the stable identity trio. Each key lives in exactly one dataset,
            so the per-dataset ``merge_insert`` keyed on ``key_col`` is the sole dedup mechanism: a re-upsert updates a
            key in place and a delete reaches the one dataset that holds it. No cross-dataset reader dedup is needed.
        vectors_col: Map column of named vectors pivoted into concrete columns, one per :attr:`vector_fields` entry.
            Dropped after the pivot. Skipped when absent from the source. Lance has no map type, so the map itself is
            never written.
        texts_col: Map column of named text fields pivoted into concrete string columns, one per :attr:`text_fields`
            entry. Dropped after the pivot. Skipped when absent from the source.
        metadata_col: Map column of metadata flattened into ``{metadata_col}_keys`` / ``{metadata_col}_values`` parallel
            arrays. Stays stored-only payload. Skipped when absent from the source.
        vector_fields: Keys of :attr:`vectors_col` to pivot into concrete columns. Each name becomes a column holding
            that key's value, cast to a fixed-size-list by :attr:`column_types` so the IVF_RQ index can target it. A key
            absent from a row yields NULL for that column in that row. Undeclared keys are dropped. Each name is
            validated against :data:`PATH_COMPONENT_PATTERN` and must not collide with another column.
        text_fields: Keys of :attr:`texts_col` to pivot into concrete string columns. Each name becomes a string column
            the INVERTED/FTS index can target. A key absent from a row yields NULL. Undeclared keys are dropped. Each
            name is validated like :attr:`vector_fields`.
        ts_col: Source event timestamp column. Used for last-write-wins collapse and written into every dataset as the
            single canonical time clock. Date-range queries on the written datasets are expressed as scalar range
            filters on this column, pruned by a BTREE scalar index when one is configured.
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
        window_start: ISO-8601 lower bound (inclusive) for the source timestamp window filter. Absent means open.
        window_end: ISO-8601 upper bound (exclusive) for the source timestamp window filter. Absent means open.
        window_column: Column used for the timestamp window pushdown filter. Defaults to ``updated_at``.
        retry_backoff_seconds: Base backoff in seconds for the Python-side commit-conflict retry loop that observes the
            merge conflict count. Tests set this to ``0.0`` to avoid sleeping.
    """

    base_uri: str
    telemetry: TelemetryConfig
    key_col: str = "vector_id"
    partition_cols: list[str] = field(default_factory=lambda: list(DEFAULT_PARTITION_COLS))
    vectors_col: str = "vectors"
    texts_col: str = "texts"
    metadata_col: str = "metadata"
    vector_fields: list[str] = field(default_factory=list)
    text_fields: list[str] = field(default_factory=list)
    ts_col: str = "timestamp"
    op_col: str = "op"
    delete_op_values: list[str] = field(default_factory=lambda: ["delete", "DELETE", "d"])
    column_types: dict[str, pa.DataType] = field(default_factory=dict)
    storage_options: dict[str, Any] | None = None
    num_partitions: int = 512
    conflict_retries: int = DEFAULT_CONFLICT_RETRIES
    retry_timeout: timedelta = DEFAULT_RETRY_TIMEOUT
    guard_updates_by_ts: bool = False
    iceberg_read_options: dict[str, str] = field(default_factory=dict)
    window_start: str | None = None
    window_end: str | None = None
    window_column: str = "updated_at"
    retry_backoff_seconds: float = 0.5

    def routing_cols(self) -> list[str]:
        """Return the routing columns in dataset-path order.

        Returns:
            The partition columns routing each row to its dataset, in path order.
        """
        return list(self.partition_cols)


def validate_partition_spec(partition_cols: list[str]) -> None:
    """Validate the partition routing specification at configuration-build time.

    Checks everything that does not require the source schema: at least one partition column and no duplicate partition
    columns. Whether every partition column actually exists in the source is checked against the real DataFrame by
    :meth:`IcebergToLanceETL.validate_schema` before any work runs.

    Args:
        partition_cols: The partition columns in dataset-path order.

    Raises:
        ValueError: If the partition column list is empty or carries duplicates.
    """
    if not partition_cols:
        raise ValueError("partition_cols must list at least one column")
    duplicate_cols: list[str] = sorted({column for column in partition_cols if partition_cols.count(column) > 1})
    if duplicate_cols:
        raise ValueError(f"partition_cols carries duplicate columns: {duplicate_cols}")


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
    pattern: re.Pattern[str] = re.compile(PATH_COMPONENT_PATTERN)
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
    quoted: list[str] = []
    for value in keys.to_pylist():
        escaped: str = str(value).replace("'", "''")
        quoted.append(f"'{escaped}'")
    joined: str = ", ".join(quoted)
    return f"{key_col} IN ({joined})"


def conflict_bucket(conflicts: int) -> str:
    """Bucket an observed commit-conflict count into a low-cardinality metric tag value.

    Args:
        conflicts: The number of retryable commit conflicts observed during one merge.

    Returns:
        ``"0"`` for no conflict, ``"1"`` for a single conflict, ``"2+"`` for two or more.
    """
    if conflicts <= 0:
        return "0"
    if conflicts == 1:
        return "1"
    return "2+"


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Apply one dataset's terminal rows with merge upsert and physical delete.

    Bootstrap strategy: when the dataset does not exist yet, an empty table is written with ``lance.write_dataset(...,
    mode='append')``, which creates the dataset if absent and is race-free for concurrent first writers on the same
    routing key. ``enable_v2_manifest_paths=True`` is always passed on this bootstrap write because V2 manifest paths
    are a creation-time naming choice, so bootstrapping with V2 names makes every later open of the dataset a single
    object-store request instead of a version-count-proportional LIST. The rows themselves always flow through
    ``merge_insert`` so a re-upsert of an existing key updates it in place instead of duplicating it.

    The merge ``execute()`` return dict provides authoritative row counts (``num_inserted_rows``, ``num_updated_rows``,
    ``num_deleted_rows``). We report those rather than recomputing from the source table.

    Conflict visibility: Lance does not surface its internal ``num_attempts`` through the pylance merge stats dict, so
    the retry count is captured by wrapping the merge in :func:`commit_with_retries`, whose ``on_conflict`` callback
    counts each retryable commit conflict observed in Python. The builder keeps its own ``conflict_retries`` so Lance's
    internal handling of write contention (``Error::TooMuchWriteContention``, which is intentionally not a retryable
    marker for the Python loop) is unchanged; the Python wrapper is a strictly-additive outer layer for commit-conflict
    markers and never reduces the existing retry budget. The ``dataset.merge_ms`` timing is tagged with a ``conflicts:``
    bucket (``"0"`` / ``"1"`` / ``"2+"``), and a ``dataset.merge_conflict_retries`` counter is emitted when any
    conflict was observed.

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
        conflicts: list[int] = []

        def run_merge() -> dict[str, Any]:
            """Open or bootstrap the dataset, evolve its schema, and execute the merge upsert once.

            Returns:
                The merge statistics dictionary with the authoritative row counts.
            """
            try:
                dataset_local: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            except (FileNotFoundError, ValueError):
                dataset_local = lance.write_dataset(
                    upserts.schema.empty_table(),
                    uri,
                    mode="append",
                    storage_options=config.storage_options,
                    enable_v2_manifest_paths=True,
                )
            builder = dataset_local.merge_insert(on=[config.key_col])
            if config.guard_updates_by_ts:
                builder = builder.when_matched_update_all(condition=f"source.{config.ts_col} > target.{config.ts_col}")
            else:
                builder = builder.when_matched_update_all()
            return (
                builder.when_not_matched_insert_all()
                .conflict_retries(config.conflict_retries)
                .retry_timeout(config.retry_timeout)
                .execute(upserts)
            )

        def count_conflict() -> None:
            """Record one observed retryable commit conflict for the merge bucket tag."""
            conflicts.append(1)

        started: float = time.perf_counter()
        try:
            stats: dict[str, Any] = commit_with_retries(
                run_merge,
                retries=config.conflict_retries,
                backoff_seconds=config.retry_backoff_seconds,
                on_conflict=count_conflict,
            )
            upserted = stats.get("num_inserted_rows", 0) + stats.get("num_updated_rows", 0)
            telemetry.incr("dataset.merged")
        except Exception:
            telemetry.incr("dataset.merge_error")
            raise
        finally:
            bucket: str = conflict_bucket(len(conflicts))
            merge_ms: float = (time.perf_counter() - started) * 1000.0
            telemetry.distribution("dataset.merge_ms", merge_ms, tags=[f"conflicts:{bucket}"])
            if conflicts:
                telemetry.incr("dataset.merge_conflict_retries", value=len(conflicts), tags=[f"conflicts:{bucket}"])

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
            ValueError: If the partition columns fail :func:`validate_partition_spec`.
        """
        validate_partition_spec(config.routing_cols())
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
        """Validate that the source carries every required column and that the pivot specification is safe.

        Every partition column, the key, timestamp, and op columns must exist in the source. When
        :attr:`ETLConfig.vector_fields` is non-empty the :attr:`ETLConfig.vectors_col` map column must exist and be a
        ``MapType``, and likewise for :attr:`ETLConfig.text_fields` and :attr:`ETLConfig.texts_col`. Every declared
        pivot field name, and every map column name in use, is validated against :data:`PATH_COMPONENT_PATTERN`, and no
        pivot field name may collide with an existing column or a reserved column (the routing, key, op, timestamp,
        window, or flattened metadata key/value columns). The map columns themselves are optional payload sources that
        are skipped gracefully when absent.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If a required column is missing, a declared map column is absent or not a map, a pivot field
                name fails the identifier allowlist, or a pivot field name collides with another column.
        """
        config: ETLConfig = self.config
        required: list[str] = [config.key_col, config.ts_col, config.op_col, *config.routing_cols()]
        missing: list[str] = [c for c in required if c not in source.columns]
        if missing:
            raise ValueError(
                f"source is missing required columns: {missing} (every partition column must exist in the source)"
            )
        self.validate_pivot(source)

    def validate_pivot(self, source: DataFrame) -> None:
        """Validate the named-vector and named-text pivot specification against the source schema.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If a declared map column is missing or not a ``MapType``, a map column name or pivot field name
                fails :data:`PATH_COMPONENT_PATTERN`, or a pivot field name collides with another column.
        """
        config: ETLConfig = self.config
        pattern: re.Pattern[str] = re.compile(PATH_COMPONENT_PATTERN)
        field_types: dict[str, Any] = {field_def.name: field_def.dataType for field_def in source.schema.fields}
        reserved: set[str] = {
            config.key_col,
            config.op_col,
            config.ts_col,
            config.window_column,
            f"{config.metadata_col}_keys",
            f"{config.metadata_col}_values",
            *config.routing_cols(),
        }
        for map_col, pivot_fields, label in (
            (config.vectors_col, config.vector_fields, "vector_fields"),
            (config.texts_col, config.text_fields, "text_fields"),
        ):
            if not pivot_fields:
                continue
            if not pattern.match(map_col):
                raise ValueError(f"map column {map_col!r} fails the identifier allowlist {PATH_COMPONENT_PATTERN}")
            if map_col not in field_types:
                raise ValueError(f"{label} declared but map column {map_col!r} is missing from the source")
            if not isinstance(field_types[map_col], MapType):
                raise ValueError(f"map column {map_col!r} must be a MapType to pivot {label}")
            for name in pivot_fields:
                if not pattern.match(name):
                    raise ValueError(f"{label} name {name!r} fails the identifier allowlist {PATH_COMPONENT_PATTERN}")
                if name in reserved or name in source.columns:
                    raise ValueError(f"{label} name {name!r} collides with an existing or reserved column")
                reserved.add(name)

    def materialize_maps(self, source: DataFrame) -> DataFrame:
        """Pivot the named vectors and texts into concrete columns and flatten the metadata map.

        For each name in :attr:`ETLConfig.vector_fields` a concrete column ``name`` is created holding
        ``vectors_col[name]``, after which the ``vectors_col`` map is dropped so the map never reaches Lance. The same
        pivot turns each name in :attr:`ETLConfig.text_fields` into a concrete string column from ``texts_col``. A key
        absent from a given row yields NULL for that column in that row, which is acceptable for optional fields.
        Undeclared map keys are not materialized and are dropped with the map. The ``metadata_col`` map stays payload
        and is flattened into ``{metadata_col}_keys`` / ``{metadata_col}_values`` parallel arrays. Each map column is
        unpacked only when it is present in the source, so an absent map column is skipped gracefully.

        Args:
            source: A DataFrame containing the map columns.

        Returns:
            The DataFrame with the vector and text maps pivoted into concrete columns and the metadata map flattened.
        """
        config: ETLConfig = self.config
        result: DataFrame = source
        for map_col, pivot_fields in (
            (config.vectors_col, config.vector_fields),
            (config.texts_col, config.text_fields),
        ):
            if map_col not in result.columns:
                continue
            for name in pivot_fields:
                result = result.withColumn(name, F.col(map_col).getItem(name))
            result = result.drop(map_col)
        if config.metadata_col in result.columns:
            result = (
                result.withColumn(f"{config.metadata_col}_keys", F.map_keys(F.col(config.metadata_col)))
                .withColumn(f"{config.metadata_col}_values", F.map_values(F.col(config.metadata_col)))
                .drop(config.metadata_col)
            )
        return result

    def collapse(self, source: DataFrame) -> DataFrame:
        """Reduce to the last-write-wins terminal event per id within a tenant.

        Args:
            source: The map-materialized source DataFrame.

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

        Resolves the Iceberg snapshot bounds, reads the incremental rows, applies the optional timestamp window filter
        via :meth:`apply_window_filter`, and delegates to :meth:`run_on_dataframe` for collapse and routing. The event
        timestamp column (``config.ts_col``) is the single canonical clock for ordering and collapse.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.apply_window_filter(self.read_increment(spark, table, start_ms, end_ms)))

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Transform and route a pre-read increment.

        Validates the source schema, pivots the named vectors and texts into concrete columns and flattens the metadata
        map, collapses to the last-write-wins terminal row per routing key and vector id using the event timestamp, and
        routes each routing key's rows to its Lance dataset via ``merge_insert``. The pivoted vector columns are cast to
        their fixed-size-list target by ``cast_table`` inside :func:`apply_merge`, so the order is pivot, then collapse,
        then cast, then merge. The event timestamp column (``config.ts_col``) is the single canonical clock: it drives
        the collapse order and is available for scalar range filters on the written datasets. No ingest-time column is
        added.

        Args:
            source: A source DataFrame carrying the operation column.
        """
        config: ETLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.etl.run") as run_span:
            self.validate_schema(source)
            collapsed: DataFrame = self.collapse(self.materialize_maps(source))
            routing: list[str] = config.routing_cols()
            routed: DataFrame = collapsed.repartition(config.num_partitions, *[F.col(c) for c in routing])
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
                telemetry: Telemetry = Telemetry.create(config.telemetry)
                table: pa.Table = pa.Table.from_batches(collected)
                results: list[tuple[Any, ...]] = []
                with telemetry.span("lance.etl.partition"):
                    try:
                        for key, group in group_by_routing(table, routing):
                            upserted, deleted = apply_merge(config, telemetry, key, group)
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
