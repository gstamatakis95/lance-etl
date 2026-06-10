"""Iceberg-to-Lance ETL routing changes into per-tenant datasets.

Reads a time range of changes from an Iceberg table, pivots all map-column keys into concrete
indexable columns per dataset group, collapses to last-write-wins per vector id, and routes each
``(org_id, tenant_id, namespace)`` group to its own Lance dataset via ``merge_insert`` upsert plus
``when_matched_delete`` for physical deletes.

The source schema contract is in ``docs/iceberg-source-table.sql``. ``validate_schema`` enforces
it: routing columns, key, op, and timestamp columns are required; map and TTL columns are optional
but must carry contracted types when present.

Routing is fixed: the dataset path is ``base_uri/org_id/tenant_id/namespace.lance``. The trio
``(org_id, tenant_id, namespace)`` maps each row to exactly one dataset URI with no cross-org
sharing.

The per-dataset Lance schema is grow-only. Columns are never removed; new keys are absorbed via
``add_columns`` schema evolution on first appearance.

Every window is an idempotent merge keyed by vector id: replayed or retried windows converge
instead of duplicating. No separate bulk path is needed for backfills.

Heavy work (dataset reads, writes, pivot, merge) runs exclusively in Spark executors.
The driver only resolves snapshot bounds, short-circuits on empty windows, and collects stats.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.window import Window, WindowSpec

from lance_etl.telemetry import (
    DEFAULT_CONFLICT_RETRIES,
    DEFAULT_RETRY_TIMEOUT,
    Telemetry,
    TelemetryConfig,
    commit_with_retries,
)

logger: logging.Logger = logging.getLogger(__name__)

ROUTING_COLS: tuple[str, str, str] = ("org_id", "tenant_id", "namespace")
"""Fixed routing columns per the source contract in docs/iceberg-source-table.sql."""


def stats_schema() -> pa.Schema:
    """Build the per-dataset stats schema (routing columns + upserted/deleted counters).

    Returns:
        A schema with one string column per routing column plus ``upserted`` and ``deleted``.
    """
    fields: list[tuple[str, pa.DataType]] = [(column, pa.string()) for column in ROUTING_COLS]
    fields.extend([("upserted", pa.int64()), ("deleted", pa.int64())])
    return pa.schema(fields)


def stats_spark_ddl() -> str:
    """Return the Spark DDL string matching :func:`stats_schema` for use as ``mapInArrow`` output schema.

    Returns:
        A DDL string with routing columns as string plus upserted/deleted as bigint.
    """
    columns: str = ", ".join(f"`{column}` string" for column in ROUTING_COLS)
    return f"{columns}, `upserted` bigint, `deleted` bigint"


@dataclass
class ETLConfig:
    """Configuration for :class:`IcebergToLanceETL`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live.
        telemetry: Telemetry configuration.
        key_col: Unique vector id column and per-dataset merge key.
        ts_col: Source event timestamp — single canonical clock for collapse and range queries.
        op_col: Operation column carrying insert, update, or delete.
        delete_op_values: Operation values treated as deletes.
        ttl_col: Optional per-row lifetime column (BIGINT seconds). Cast to ``pa.duration("s")`` when present.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Shuffle partitions for routing co-location.
        conflict_retries: Retry budget for concurrent merge commits.
        retry_timeout: Total time budget for conflict retries.
        iceberg_read_options: Extra Iceberg reader options merged into the read.
        window_start: ISO-8601 inclusive lower bound for the window pushdown filter. Open when absent.
        window_end: ISO-8601 exclusive upper bound for the window pushdown filter. Open when absent.
        window_column: Column for the timestamp window filter.
        retry_backoff_seconds: Base backoff (seconds) for the commit-conflict retry loop.
    """

    base_uri: str
    telemetry: TelemetryConfig
    key_col: str = "vector_id"
    ts_col: str = "event_timestamp"
    op_col: str = "op"
    delete_op_values: list[str] = field(default_factory=lambda: ["delete", "DELETE", "d"])
    ttl_col: str = "ttl"
    storage_options: dict[str, Any] | None = None
    num_partitions: int = 512
    conflict_retries: int = DEFAULT_CONFLICT_RETRIES
    retry_timeout: timedelta = DEFAULT_RETRY_TIMEOUT
    iceberg_read_options: dict[str, str] = field(default_factory=dict)
    window_start: str | None = None
    window_end: str | None = None
    window_column: str = "processing_timestamp"
    retry_backoff_seconds: float = 0.5


def dataset_uri(config: ETLConfig, *components: str) -> str:
    """Build the validated dataset URI ``base_uri/org_id/tenant_id/namespace.lance``.

    Args:
        config: ETL configuration.
        *components: One routing value per column in :data:`ROUTING_COLS`, in path order.

    Returns:
        The dataset URI for the given routing key.

    Raises:
        ValueError: If the component count is not three or any component is an empty string.
    """
    if len(components) != len(ROUTING_COLS):
        raise ValueError(
            f"expected {len(ROUTING_COLS)} routing components for {list(ROUTING_COLS)}, got {len(components)}"
        )
    for component in components:
        if not isinstance(component, str) or not component:
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = config.base_uri.rstrip("/")
    return f"{base}/{'/'.join(components)}.lance"


def apply_fsl_cast(
    table: pa.Table,
    col_name: str,
    invalid_counts: dict[str, int],
) -> pa.Table:
    """Cast a vector column to ``fixed_size_list<float32, dim>``, inferring dim from first non-null value.

    Rows whose length differs from the inferred dimension are nulled out and counted into
    ``invalid_counts``. A fully-null column is returned unchanged (no dimension to infer).

    Args:
        table: Table containing the column.
        col_name: Name of the column to cast.
        invalid_counts: Mutable accumulator for wrong-dimension row counts, updated in place.

    Returns:
        Table with the column cast, or unchanged when no dimension can be inferred.
    """
    column: pa.ChunkedArray = table.column(col_name)
    lengths: pa.ChunkedArray = pc.list_value_length(column)
    observed: pa.ChunkedArray = lengths.drop_null()
    if len(observed) == 0:
        return table
    dim: int = int(observed[0].as_py())
    matches: pa.ChunkedArray = pc.equal(lengths, dim)
    mismatches: int = int(pc.sum(pc.invert(matches)).as_py() or 0)
    if mismatches:
        invalid_counts[col_name] = invalid_counts.get(col_name, 0) + mismatches
        column = pc.if_else(matches, column, pa.scalar(None, column.type))
    col_idx: int = table.schema.get_field_index(col_name)
    return table.set_column(col_idx, col_name, column.cast(pa.list_(pa.float32(), dim)))


def pivot_map_columns(table: pa.Table, config: ETLConfig) -> tuple[pa.Table, dict[str, int]]:
    """Expand every map column into concrete per-key columns for this dataset group.

    Processes ``vectors``, ``texts``, and ``metadata`` in order. For each map column, all distinct
    keys in this group become new columns via ``map_lookup(occurrence="last")``. Keys colliding with
    an existing or reserved column are skipped. Vector columns are passed through
    :func:`apply_fsl_cast`. The map column is dropped after its keys are extracted.

    Args:
        table: The upsert table for one dataset group, after Spark serialisation.
        config: ETL configuration providing the set of reserved column names.

    Returns:
        ``(result_table, counts)`` where ``counts`` carries ``"invalid_map_keys"`` and
        ``"invalid_vector_rows"`` when non-zero.
    """
    routing_reserved: set[str] = {
        config.key_col,
        config.op_col,
        config.ts_col,
        config.window_column,
        *ROUTING_COLS,
    }
    result: pa.Table = table
    collision_key_count: int = 0
    fsl_invalid_counts: dict[str, int] = {}

    for map_col, is_vectors in (
        ("vectors", True),
        ("texts", False),
        ("metadata", False),
    ):
        if map_col not in result.schema.names:
            continue
        field_type: pa.DataType = result.schema.field(map_col).type
        if not pa.types.is_map(field_type):
            continue

        map_column: pa.ChunkedArray = result.column(map_col)
        seen_names: set[str] = set(result.schema.names)

        raw_keys: set[str] = set()
        for chunk in map_column.chunks:
            for key_val in pc.unique(chunk.keys).to_pylist():
                if key_val is not None:
                    raw_keys.add(str(key_val))

        for key in sorted(raw_keys):
            if key in seen_names or key in routing_reserved:
                collision_key_count += 1
                continue
            extracted: pa.ChunkedArray = pc.map_lookup(map_column, query_key=key, occurrence="last")
            result = result.append_column(key, extracted)
            seen_names.add(key)

            if is_vectors:
                result = apply_fsl_cast(result, key, fsl_invalid_counts)

        col_idx: int = result.schema.get_field_index(map_col)
        result = result.remove_column(col_idx)

    counts: dict[str, int] = {}
    if collision_key_count:
        counts["invalid_map_keys"] = collision_key_count
    invalid_rows: int = sum(fsl_invalid_counts.values())
    if invalid_rows:
        counts["invalid_vector_rows"] = invalid_rows
    return result, counts


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


def apply_ttl_cast(table: pa.Table, ttl_col: str) -> pa.Table:
    """Cast the integer TTL column to ``pa.duration("s")`` so Arrow time arithmetic works natively.

    Args:
        table: The upsert table after pivot.
        ttl_col: Name of the TTL column per ``ETLConfig.ttl_col``.

    Returns:
        Table with the TTL column cast to ``pa.duration("s")``, or unchanged when absent or
        non-integer.
    """
    if ttl_col not in table.schema.names or not pa.types.is_integer(table.schema.field(ttl_col).type):
        return table
    idx: int = table.schema.get_field_index(ttl_col)
    return table.set_column(idx, ttl_col, table.column(ttl_col).cast(pa.duration("s")))


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Pivot, cast, and merge one dataset group via upsert and physical delete.

    Pivots map columns, casts TTL, bootstraps a new dataset with V2 manifest paths when absent
    (concurrent-bootstrap race caught with OSError fallback), evolves schema via ``add_columns``
    when new keys appear (idempotent on retry), then runs ``merge_insert`` with
    ``when_matched_update_all()``. Physical deletes use ``when_matched_delete()`` on a key-only
    table. Both paths go through :func:`commit_with_retries` with ``on_conflict`` incrementing
    ``dataset.merge_conflict_retries``.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: Routing key values in :data:`ROUTING_COLS` order.
        group: Rows for this routing key carrying the op column.

    Returns:
        Counts of upserted (inserted + updated) and deleted rows.
    """
    uri: str = dataset_uri(config, *key)
    is_delete: pa.Array = pc.is_in(group[config.op_col], value_set=pa.array(config.delete_op_values))
    payload_cols: list[str] = [c for c in group.column_names if c != config.op_col]

    upserts_pre_pivot: pa.Table = group.filter(pc.invert(is_delete)).select(payload_cols)
    upserts_pivoted, pivot_counts = pivot_map_columns(upserts_pre_pivot, config)
    if pivot_counts.get("invalid_map_keys", 0) > 0:
        telemetry.incr("dataset.invalid_map_keys", value=pivot_counts["invalid_map_keys"])
        logger.warning(
            "dataset %s: %d map keys skipped (collision with an existing or reserved column)",
            uri,
            pivot_counts["invalid_map_keys"],
        )
    if pivot_counts.get("invalid_vector_rows", 0) > 0:
        telemetry.incr("dataset.invalid_vector_rows", value=pivot_counts["invalid_vector_rows"])
        logger.warning(
            "dataset %s: %d vector rows nulled out (length did not match the inferred dimension)",
            uri,
            pivot_counts["invalid_vector_rows"],
        )

    upserts: pa.Table = apply_ttl_cast(upserts_pivoted, config.ttl_col)

    deletes: pa.Table = group.filter(is_delete).select([config.key_col])

    upserted: int = 0
    deleted: int = 0

    if upserts.num_rows:

        def run_merge() -> dict[str, Any]:
            """Open (or bootstrap) the dataset, evolve schema for new columns, and execute the merge upsert.

            Re-opens the dataset on every call so retries see the latest version. ``add_columns``
            schema evolution is idempotent: a retry that re-enters after evolution finds no missing
            fields.

            Returns:
                Merge statistics dictionary with authoritative row counts.
            """
            try:
                dataset_local: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            except (FileNotFoundError, ValueError):
                try:
                    dataset_local = lance.write_dataset(
                        upserts.schema.empty_table(),
                        uri,
                        mode="append",
                        storage_options=config.storage_options,
                        enable_v2_manifest_paths=True,
                    )
                except OSError:
                    dataset_local = lance.dataset(uri, storage_options=config.storage_options)
            missing_fields: list[pa.Field] = [
                upserts.schema.field(name) for name in upserts.schema.names if name not in dataset_local.schema.names
            ]
            if missing_fields:
                dataset_local.add_columns(pa.schema(missing_fields))
                dataset_local = lance.dataset(uri, storage_options=config.storage_options)
            builder = dataset_local.merge_insert(on=[config.key_col])
            builder = builder.when_matched_update_all()
            return (
                builder.when_not_matched_insert_all()
                .conflict_retries(config.conflict_retries)
                .retry_timeout(config.retry_timeout)
                .execute(upserts)
            )

        try:
            with telemetry.timed("dataset.merge_ms"):
                stats: dict[str, Any] = commit_with_retries(
                    run_merge,
                    retries=config.conflict_retries,
                    backoff_seconds=config.retry_backoff_seconds,
                    on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
                )
            upserted = stats.get("num_inserted_rows", 0) + stats.get("num_updated_rows", 0)
            telemetry.incr("dataset.merged")
        except Exception:
            telemetry.incr("dataset.merge_error")
            raise

    if deletes.num_rows:

        def run_delete() -> dict[str, Any]:
            """Re-open the dataset and execute ``when_matched_delete`` on the key-only deletes table.

            Returns:
                Merge statistics dictionary, or an empty dict when the dataset does not exist.
            """
            try:
                delete_dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            except (FileNotFoundError, ValueError):
                return {}
            return (
                delete_dataset.merge_insert(on=[config.key_col])
                .when_matched_delete()
                .conflict_retries(config.conflict_retries)
                .retry_timeout(config.retry_timeout)
                .execute(deletes)
            )

        with telemetry.timed("dataset.delete_ms"):
            delete_stats: dict[str, Any] = commit_with_retries(
                run_delete,
                retries=config.conflict_retries,
                backoff_seconds=config.retry_backoff_seconds,
                on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
            )
        deleted = delete_stats.get("num_deleted_rows", 0)

    telemetry.distribution("dataset.upserted", upserted)
    telemetry.distribution("dataset.deleted", deleted)
    return upserted, deleted


def snapshot_id_bounds(
    spark: SparkSession, table: str, start_ms: int, end_ms: int
) -> tuple[int | None, int | None, bool]:
    """Resolve a wall-clock window to Iceberg snapshot-id bounds via ``{table}.snapshots``.

    Gate the empty-window short circuit on ``has_new_snapshots``, not on ``start_id == end_id``:
    equal ids can mean a genuinely empty window or two bounds resolving to the same snapshot.

    Args:
        spark: Active Spark session.
        table: Fully qualified Iceberg table name.
        start_ms: Window start in epoch milliseconds.
        end_ms: Window end in epoch milliseconds.

    Returns:
        ``(start_id, end_id, has_new_snapshots)`` — ids are None when no snapshot satisfies the bound.
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
        rows: One ``(*routing_values, upserted, deleted)`` per dataset, matching the schema's
            column order.
        schema: The stats schema produced by :func:`stats_schema`.

    Returns:
        A record batch conforming to the given schema.
    """
    arrays: list[pa.Array] = [pa.array([row[index] for row in rows], field.type) for index, field in enumerate(schema)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class IcebergToLanceETL:
    """Routes an Iceberg increment into per-tenant Lance datasets."""

    def __init__(self, config: ETLConfig) -> None:
        """Initialize the ETL with the given configuration.

        Args:
            config: ETL configuration.
        """
        self.config: ETLConfig = config

    def read_increment(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> DataFrame:
        """Read the rows committed within a wall-clock window using snapshot-id bounds.

        Resolves the window to snapshot ids via :func:`snapshot_id_bounds` (Iceberg 1.10 rejects
        ``start-timestamp``/``end-timestamp`` outside changelog scans). Falls back to a full scan
        pinned to ``snapshot-id`` when no prior snapshot exists. Returns an empty DataFrame when
        no snapshot landed in the window.

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
        """Filter source rows to ``[window_start, window_end)`` on ``config.window_column``.

        Each bound is validated with ``datetime.fromisoformat`` before any Spark work runs.
        Returns source unchanged when neither bound is set.

        Args:
            source: The incremental source DataFrame.

        Returns:
            Filtered DataFrame, or the original when no bounds are configured.

        Raises:
            ValueError: If a bound string is not a valid ISO-8601 datetime.
        """
        config: ETLConfig = self.config
        if config.window_start is None and config.window_end is None:
            return source

        def parse_bound(value: str) -> str:
            """Validate and return an ISO-8601 bound string.

            Args:
                value: The bound string to validate.

            Returns:
                The original string, validated.

            Raises:
                ValueError: If the string is not a valid ISO-8601 datetime.
            """
            normalised: str = value.replace("Z", "+00:00")
            try:
                datetime.fromisoformat(normalised)
            except ValueError as exc:
                raise ValueError(f"window bound {value!r} is not a valid ISO-8601 datetime: {exc}") from exc
            return value

        filtered: DataFrame = source
        if config.window_start is not None:
            validated_start: str = parse_bound(config.window_start)
            filtered = filtered.filter(f"`{config.window_column}` >= TIMESTAMP '{validated_start}'")
        if config.window_end is not None:
            validated_end: str = parse_bound(config.window_end)
            filtered = filtered.filter(f"`{config.window_column}` < TIMESTAMP '{validated_end}'")
        return filtered

    def validate_schema(self, source: DataFrame) -> None:
        """Verify the source schema against the contract in ``docs/iceberg-source-table.sql``.

        All violations are collected and reported together. Extra payload columns are allowed.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If any required column is missing or any column violates its contracted
                type. The message lists every violation and references the SQL contract.
        """
        config: ETLConfig = self.config
        ft: dict[str, Any] = {f.name: f.dataType for f in source.schema.fields}
        violations: list[str] = []

        def is_string(t: Any) -> bool:
            """Return True when t is a StringType."""
            return isinstance(t, StringType)

        def is_timestamp(t: Any) -> bool:
            """Return True when t is a TimestampType or TimestampNTZType."""
            return isinstance(t, (TimestampType, TimestampNTZType))

        def is_vector_map(t: Any) -> bool:
            """Return True when t is MapType(StringType, ArrayType(FloatType|DoubleType))."""
            return (
                isinstance(t, MapType)
                and isinstance(t.keyType, StringType)
                and isinstance(t.valueType, ArrayType)
                and isinstance(t.valueType.elementType, (FloatType, DoubleType))
            )

        def is_string_map(t: Any) -> bool:
            """Return True when t is MapType(StringType, StringType)."""
            return isinstance(t, MapType) and isinstance(t.keyType, StringType) and isinstance(t.valueType, StringType)

        def is_integer(t: Any) -> bool:
            """Return True when t is LongType or IntegerType."""
            return isinstance(t, (LongType, IntegerType))

        required_checks: list[tuple[str, Any, str]] = [
            *[(col, is_string, "StringType") for col in [*ROUTING_COLS, config.key_col, config.op_col]],
            *[
                (col, is_timestamp, "TimestampType or TimestampNTZType")
                for col in [config.ts_col, config.window_column]
            ],
        ]
        for col, predicate, expected in required_checks:
            if col not in ft:
                violations.append(f"  missing required column {col!r} (expected {expected})")
            elif not predicate(ft[col]):
                violations.append(f"  column {col!r}: expected {expected}, got {type(ft[col]).__name__}")

        optional_checks: list[tuple[str, Any, str]] = [
            ("vectors", is_vector_map, "MapType(StringType, ArrayType(FloatType|DoubleType))"),
            ("texts", is_string_map, "MapType(StringType, StringType)"),
            ("metadata", is_string_map, "MapType(StringType, StringType)"),
            (config.ttl_col, is_integer, "LongType or IntegerType"),
        ]
        for col, predicate, expected in optional_checks:
            if col in ft and not predicate(ft[col]):
                violations.append(f"  column {col!r} must be a {expected}, got {ft[col]}")

        if violations:
            detail: str = "\n".join(violations)
            raise ValueError(f"Source schema violates the contract in docs/iceberg-source-table.sql:\n{detail}")

    def collapse(self, source: DataFrame) -> DataFrame:
        """Reduce to the last-write-wins terminal event per (routing key, vector id).

        Orders by ``ts_col`` descending NULLS LAST; ties broken by ``xxhash64`` over all
        non-MapType columns ascending (MapType columns cannot be hashed by Spark).

        Args:
            source: The source DataFrame with map columns still intact.

        Returns:
            One row per routing key and vector id, carrying the terminal op.
        """
        config: ETLConfig = self.config
        partition_by: list[Column] = [F.col(c) for c in ROUTING_COLS]
        partition_by.append(F.col(config.key_col))
        non_map_cols: list[str] = [f.name for f in source.schema.fields if not isinstance(f.dataType, MapType)]
        window: WindowSpec = Window.partitionBy(*partition_by).orderBy(
            F.col(config.ts_col).desc_nulls_last(),
            F.xxhash64(*[F.col(c) for c in non_map_cols]).asc(),
        )
        return source.withColumn("row_num", F.row_number().over(window)).where(F.col("row_num") == 1).drop("row_num")

    def run(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> None:
        """Read one Iceberg window and route it via :meth:`run_on_dataframe`.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.apply_window_filter(self.read_increment(spark, table, start_ms, end_ms)))

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Validate, collapse, shuffle, and merge a pre-read increment into per-tenant Lance datasets.

        Order: validate schema, collapse LWW, repartition by routing key, pivot+cast+merge on
        executors. Null routing rows are dropped and counted as ``dataset.null_routing_rows``.

        Args:
            source: A source DataFrame carrying the operation column.
        """
        config: ETLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.etl.run") as run_span:
            self.validate_schema(source)
            collapsed: DataFrame = self.collapse(source)
            routing: list[str] = list(ROUTING_COLS)
            routed: DataFrame = collapsed.repartition(config.num_partitions, *[F.col(c) for c in routing])
            partition_stats_schema: pa.Schema = stats_schema()

            def merge_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                """Merge all routing-key groups in one Spark partition.

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
                valid_mask: pa.Array | None = None
                for routing_col in routing:
                    col_valid: pa.Array = pc.is_valid(table[routing_col])
                    valid_mask = col_valid if valid_mask is None else pc.and_(valid_mask, col_valid)
                if valid_mask is not None:
                    null_count: int = int(pc.sum(pc.invert(valid_mask)).as_py() or 0)
                    if null_count:
                        telemetry.incr("dataset.null_routing_rows", value=null_count)
                        logger.warning("dropped %d rows with null routing key(s) in this partition", null_count)
                        table = table.filter(valid_mask)
                if not table.num_rows:
                    return
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
                    stats: DataFrame = routed.mapInArrow(merge_partition, schema=stats_spark_ddl())
                    rows: list[Any] = stats.collect()
            except Exception:
                driver_telemetry.error("etl run failed")
                raise

            datasets: int = len(rows)
            upserted: int = sum(int(row["upserted"] or 0) for row in rows)
            deleted: int = sum(int(row["deleted"] or 0) for row in rows)

            run_span.set_tag("datasets", datasets)
            driver_telemetry.gauge("run.datasets", datasets)
            driver_telemetry.gauge("run.upserted", upserted)
            driver_telemetry.gauge("run.deleted", deleted)
            logger.info(
                "incremental run: %s datasets, %s upserts, %s deletes",
                datasets,
                upserted,
                deleted,
            )
