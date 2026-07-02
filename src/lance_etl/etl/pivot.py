"""Pure pivot and cast helpers for the Iceberg-to-Lance ETL.

Provides the per-group pivot, FSL cast, TTL cast, and stats-batch helpers used by the executor
closure in :mod:`lance_etl.etl.job`. All functions operate on PyArrow tables and have no Spark
dependency, so they can be unit-tested without a Spark context.

Also owns :data:`ROUTING_COLS` and :class:`ETLConfig`, which are the two names that
:mod:`lance_etl.etl.job` imports to avoid a circular dependency.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from lance_etl.column_roles import SCALAR_ROLE, TEXT_ROLE, VECTOR_ROLE
from lance_etl.telemetry import DEFAULT_CONFLICT_RETRIES, DEFAULT_RETRY_TIMEOUT, TelemetryConfig

ROUTING_COLS: tuple[str, str, str] = ("org_id", "tenant_id", "namespace")
"""Fixed routing columns per the source contract in docs/iceberg-source-table.sql."""


@dataclass
class ETLConfig:
    """Configuration for :class:`lance_etl.etl.job.IcebergToLanceETL`.

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
        merge_batch_bytes: Byte budget per merge_insert commit, derived from the source table's
            actual row width to keep the DataFusion hash-join build side inside the default ~100 MB
            pool regardless of vector dtype. At 64 MiB source budget, a 128-dim float32 row (512
            bytes) yields ~131 K rows/chunk whose hash-join build side peaks around 67 MB — well
            under the 100 MB default pool that the sift1m repro exhausted at 97.7 MB. None disables
            chunking.
        data_storage_version: Lance file format version for newly created datasets. The default
            ``"2.1"`` adopts the latest stable format with structural encodings. Existing
            datasets keep the format they were created with, and lance reads both transparently.
        spark_batches: Number of sequential Spark-level batches the increment is split into before
            collapse. Each batch keeps the rows whose ``pmod(xxhash64(key_col), spark_batches)``
            equals the batch index, so every event for a vector id lands in exactly one batch and
            per-batch collapse equals global collapse restricted to that batch — last-write-wins
            is preserved. Each batch runs the full collapse-shuffle-merge flow as its own Spark job
            over roughly ``1/spark_batches`` of the increment, so executor memory needs scale with
            the batch size instead of the increment size. Raise this to absorb increments of tens
            of millions of rows per org without raising executor memory limits. 1 (the default)
            processes the whole increment in a single pass.
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
    merge_batch_bytes: int | None = 64 * 1024 * 1024
    data_storage_version: str = "2.1"
    spark_batches: int = 1


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


def pivot_map_columns(table: pa.Table, config: ETLConfig) -> tuple[pa.Table, dict[str, int], dict[str, str]]:
    """Expand every map column into concrete per-key columns for this dataset group.

    Processes ``vectors``, ``texts``, and ``metadata`` in order. For each map column, all distinct
    keys in this group become new columns via ``map_lookup(occurrence="last")``. Keys colliding with
    an existing or reserved column are skipped. Vector columns are passed through
    :func:`apply_fsl_cast`. The map column is dropped after its keys are extracted.

    Each created column's role is its source map: keys from ``vectors`` are ``"vector"`` columns,
    keys from ``texts`` are ``"text"`` columns, and keys from ``metadata`` are ``"scalar"``
    columns. The sink persists these roles into the dataset's config KV so the indexer can later
    choose which index each column gets.

    Args:
        table: The upsert table for one dataset group, after Spark serialisation.
        config: ETL configuration providing the set of reserved column names.

    Returns:
        ``(result_table, counts, roles)`` where ``counts`` carries ``"invalid_map_keys"`` and
        ``"invalid_vector_rows"`` when non-zero, and ``roles`` maps each created column to its
        role string.
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
    roles: dict[str, str] = {}

    for map_col, role in (
        ("vectors", VECTOR_ROLE),
        ("texts", TEXT_ROLE),
        ("metadata", SCALAR_ROLE),
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
            roles[key] = role

            if role == VECTOR_ROLE:
                result = apply_fsl_cast(result, key, fsl_invalid_counts)

        col_idx: int = result.schema.get_field_index(map_col)
        result = result.remove_column(col_idx)

    counts: dict[str, int] = {}
    if collision_key_count:
        counts["invalid_map_keys"] = collision_key_count
    invalid_rows: int = sum(fsl_invalid_counts.values())
    if invalid_rows:
        counts["invalid_vector_rows"] = invalid_rows
    return result, counts, roles


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
