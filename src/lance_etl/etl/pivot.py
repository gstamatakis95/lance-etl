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
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from lance_etl.column_roles import SCALAR_ROLE, TEXT_ROLE, VECTOR_ROLE
from lance_etl.telemetry import DEFAULT_CONFLICT_RETRIES, TelemetryConfig

ROUTING_COLS: tuple[str, str, str] = ("org_id", "tenant_id", "namespace")
"""Fixed routing columns per the source contract in docs/iceberg-source-table.sql."""

KEY_COL: str = "vector_id"
"""Unique vector id column and per-dataset merge key, per the source contract."""

OP_COL: str = "op"
"""Operation column carrying insert, update, or delete, per the source contract."""

DELETE_OP_VALUES: list[str] = ["delete", "DELETE", "d"]
"""Operation values treated as deletes, per the source contract.

Kept as a ``list`` rather than a ``tuple`` because :meth:`pyspark.sql.Column.isin` only unpacks a
single ``list`` or ``set`` argument, not a tuple.
"""

TTL_COL: str = "ttl"
"""Optional per-row lifetime column (BIGINT seconds). Cast to ``pa.duration("s")`` when present."""


@dataclass
class ETLConfig:
    """Configuration for :class:`lance_etl.etl.job.IcebergToLanceETL`.

    The schema-contract columns (:data:`KEY_COL`, :data:`OP_COL`, :data:`DELETE_OP_VALUES`,
    :data:`TTL_COL`) and the operational constants (``MAX_SHUFFLE_PARTITIONS`` in
    :mod:`lance_etl.etl.plan` and ``DATA_STORAGE_VERSION`` in :mod:`lance_etl.etl.sink`) are fixed
    module-level constants, not fields, because they are never varied.

    Attributes:
        base_uri: Root location under which per-tenant datasets live.
        telemetry: Telemetry configuration.
        ts_col: Source event timestamp — single canonical clock for collapse and range queries.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Manual override of the adaptive routing-shuffle width. ``None`` (the
            default) lets :func:`lance_etl.etl.plan.compute_routing_plan` size the shuffle by both
            rows and trio count. An explicit integer pins a fixed-width shuffle.
        bucket_rows: Target rows per merge-writer sub-bucket and per shuffle task; the unit sizing
            both K (buckets per big dataset) and N (shuffle partitions).
        max_buckets_per_dataset: Cap on concurrent merge writers per dataset, bounding
            commit-conflict retries and BTREE-delta contention risk.
        datasets_per_task: Partition-floor divisor keeping per-dataset commit fixed cost (~1-2s)
            to roughly 1-2 minutes per task at fleet scale.
        conflict_retries: Retry budget for concurrent merge commits.
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
        bulk_append: Enable the parallel ``write_fragments`` + single ``commit_batch`` fast path
            for big NEW or empty datasets (operational kill switch).
        tag_stamp: Pre-formatted interval tag name (``%Y%m%dT%H%M%SZ``, typically the run's
            truncated hour via ``cliutil.parse_hour_tag``) stamped on every dataset the run
            wrote, after all batches commit. Create-or-move semantics: a later run in the same
            hour advances that hour's tag to the newest version, so the tag always marks the
            latest version produced within its hour. ``None`` (the default) disables stamping.
    """

    base_uri: str
    telemetry: TelemetryConfig
    ts_col: str = "event_timestamp"
    storage_options: dict[str, Any] | None = None
    num_partitions: int | None = None
    bucket_rows: int = 2_000_000
    max_buckets_per_dataset: int = 32
    datasets_per_task: int = 64
    conflict_retries: int = DEFAULT_CONFLICT_RETRIES
    iceberg_read_options: dict[str, str] = field(default_factory=dict)
    window_start: str | None = None
    window_end: str | None = None
    window_column: str = "processing_timestamp"
    retry_backoff_seconds: float = 0.5
    merge_batch_bytes: int | None = 64 * 1024 * 1024
    bulk_append: bool = True
    tag_stamp: str | None = None


def routing_stats_schema(extra_fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    """Build a per-dataset stats schema: one string column per routing column plus extras.

    Shared builder behind :func:`stats_schema` and :func:`lance_etl.etl.bulk.bulk_stats_schema`,
    so both the merge and bulk-append fast-path stats schemas agree on the routing-column prefix.

    Args:
        extra_fields: Additional ``(name, type)`` fields appended after the routing columns.

    Returns:
        A schema with one string column per :data:`ROUTING_COLS` entry, followed by ``extra_fields``
        in order.
    """
    fields: list[tuple[str, pa.DataType]] = [(column, pa.string()) for column in ROUTING_COLS]
    fields.extend(extra_fields)
    return pa.schema(fields)


def routing_stats_ddl(extra_columns: list[tuple[str, str]]) -> str:
    """Return the Spark DDL string matching :func:`routing_stats_schema`.

    Shared builder behind :func:`stats_spark_ddl` and
    :func:`lance_etl.etl.bulk.bulk_stats_spark_ddl`.

    Args:
        extra_columns: Additional ``(name, spark_type)`` columns appended after the routing columns.

    Returns:
        A DDL string with routing columns as string, followed by ``extra_columns`` in order.
    """
    columns: str = ", ".join(f"`{column}` string" for column in ROUTING_COLS)
    extras: str = ", ".join(f"`{name}` {spark_type}" for name, spark_type in extra_columns)
    return f"{columns}, {extras}" if extras else columns


def stats_schema() -> pa.Schema:
    """Build the per-dataset stats schema (routing columns + upserted/deleted counters).

    Returns:
        A schema with one string column per routing column plus ``upserted`` and ``deleted``.
    """
    return routing_stats_schema([("upserted", pa.int64()), ("deleted", pa.int64())])


def stats_spark_ddl() -> str:
    """Return the Spark DDL string matching :func:`stats_schema` for use as ``mapInArrow`` output schema.

    Returns:
        A DDL string with routing columns as string plus upserted/deleted as bigint.
    """
    return routing_stats_ddl([("upserted", "bigint"), ("deleted", "bigint")])


def apply_fsl_cast(
    table: pa.Table,
    col_name: str,
    invalid_counts: dict[str, int],
    dim: int | None = None,
) -> pa.Table:
    """Cast a vector column to ``fixed_size_list<float32, dim>``.

    When ``dim`` is ``None`` the dimension is inferred from the first non-null value and a
    fully-null column is returned unchanged (no dimension to infer). When ``dim`` is given the
    inference is skipped and that dimension is used, so a fully-null column is still cast to the
    requested fixed size. In both modes rows whose length differs from the target dimension are
    nulled out and counted into ``invalid_counts``.

    The explicit ``dim`` path exists for the bulk-append fast path, where every parallel task must
    cast a vector key to the one canonical dimension derived on the driver rather than to whatever
    length happens to arrive first in that task's slice.

    Args:
        table: Table containing the column.
        col_name: Name of the column to cast.
        invalid_counts: Mutable accumulator for wrong-dimension row counts, updated in place.
        dim: Target dimension. ``None`` infers it from the first non-null value.

    Returns:
        Table with the column cast, or unchanged when no dimension can be inferred.
    """
    column: pa.ChunkedArray = table.column(col_name)
    lengths: pa.ChunkedArray = pc.list_value_length(column)
    if dim is None:
        observed: pa.ChunkedArray = lengths.drop_null()
        if len(observed) == 0:
            return table
        dim = int(observed[0].as_py())
    matches: pa.ChunkedArray = pc.equal(lengths, dim)
    mismatches: int = int(pc.sum(pc.invert(matches)).as_py() or 0)
    if mismatches:
        invalid_counts[col_name] = invalid_counts.get(col_name, 0) + mismatches
        column = pc.if_else(matches, column, pa.scalar(None, column.type))
    col_idx: int = table.schema.get_field_index(col_name)
    return table.set_column(col_idx, col_name, column.cast(pa.list_(pa.float32(), dim)))


def pivot_map_columns(
    table: pa.Table, config: ETLConfig, vector_dims: dict[str, int] | None = None
) -> tuple[pa.Table, dict[str, int], dict[str, str]]:
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
        vector_dims: Optional map of vector key to canonical dimension. When a vector key is
            present, its dimension is forwarded to :func:`apply_fsl_cast` instead of being
            inferred from this slice, so every parallel bulk-append task casts to the same
            driver-derived dimension. Keys absent from the map fall back to per-slice inference.

    Returns:
        ``(result_table, counts, roles)`` where ``counts`` carries ``"invalid_map_keys"`` and
        ``"invalid_vector_rows"`` when non-zero, and ``roles`` maps each created column to its
        role string.
    """
    dims: dict[str, int] = vector_dims or {}
    routing_reserved: set[str] = {
        KEY_COL,
        OP_COL,
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
                result = apply_fsl_cast(result, key, fsl_invalid_counts, dims.get(key))

        col_idx: int = result.schema.get_field_index(map_col)
        result = result.remove_column(col_idx)

    counts: dict[str, int] = {}
    if collision_key_count:
        counts["invalid_map_keys"] = collision_key_count
    invalid_rows: int = sum(fsl_invalid_counts.values())
    if invalid_rows:
        counts["invalid_vector_rows"] = invalid_rows
    return result, counts, roles


def group_run_starts(table: pa.Table, routing_cols: list[str]) -> list[int]:
    """Find the start offset of every contiguous routing-key run in a table.

    A single vectorized pass: each routing column is compared against itself shifted by one row
    (``pc.not_equal`` over zero-copy slices), the per-column change masks are OR'd, and the
    nonzero indices become run boundaries. Cost is ``O(rows)`` regardless of how many distinct
    keys the table holds. Assumes the routing columns are non-null, which the ETL guarantees by
    dropping null-routing rows before the shuffle.

    Args:
        table: The partition table, expected sorted by the routing columns.
        routing_cols: The routing key columns.

    Returns:
        Sorted run-start row offsets, beginning with ``0``. Empty for an empty table.
    """
    if table.num_rows == 0:
        return []
    if table.num_rows == 1:
        return [0]
    routing: pa.Table = table.select(routing_cols).combine_chunks()
    changed: pa.Array | None = None
    for column_name in routing_cols:
        column: pa.Array = routing.column(column_name).chunk(0)
        differs: pa.Array = pc.not_equal(column.slice(1), column.slice(0, len(column) - 1))
        changed = differs if changed is None else pc.or_(changed, differs)
    boundaries: list[int] = [int(i.as_py()) + 1 for i in pc.indices_nonzero(changed)]
    return [0, *boundaries]


def stream_routing_groups(
    batches: Iterator[pa.RecordBatch],
    routing_cols: list[str],
    flush_bytes: int | None,
    counters: dict[str, int] | None = None,
) -> Iterator[tuple[tuple[Any, ...], pa.Table]]:
    """Stream routing-key groups from an iterator of batches without materializing the partition.

    Streaming counterpart of the non-streaming, whole-partition grouping this module used to
    expose: consumes an iterator of Arrow batches sorted by ``routing_cols`` and yields ``(key,
    sub_table)`` groups while holding at most one group (or one flush's worth) in memory, so
    executor memory scales with one dataset group rather than the whole partition. Cost is
    ``O(rows)``.

    Because collapse runs before this stage, each key is already exactly one atomic row, so a
    byte-budget flush mid-run never splits a key across upsert commits. When one key does span
    multiple flushes, it yields multiple groups over disjoint rows, and the downstream idempotent
    :func:`~lance_etl.etl.sink.apply_merge` calls converge — the same key-split-across-runs
    degradation contract as the test-only equivalence oracle in ``tests/conftest.py``.

    Byte accounting uses each batch's mean row width (``nbytes // num_rows``) rather than a
    per-slice ``.nbytes``, which over-counts zero-copy slices.

    Args:
        batches: Iterator of Arrow batches, sorted by the routing columns.
        routing_cols: The routing key columns.
        flush_bytes: Approximate byte budget that forces a mid-run flush, or ``None`` to disable.
        counters: Optional accumulator mutated in place. ``"flushes"`` counts yielded groups and
            ``"peak_buffered_bytes"`` records the high-water buffered byte estimate.

    Yields:
        ``(key_values, sub_table)`` for each contiguous routing-key run, split further whenever the
        buffered byte estimate reaches ``flush_bytes``.
    """
    current_key: tuple[Any, ...] | None = None
    buffer: list[pa.RecordBatch] = []
    buffered_bytes: int = 0

    def flush() -> Iterator[tuple[tuple[Any, ...], pa.Table]]:
        """Yield the buffered rows as one group, then reset the buffer and byte counter.

        Increments ``counters["flushes"]`` on each emitted group so the flush count equals the
        number of yielded groups by construction. Does not reset ``current_key``.

        Yields:
            One ``(current_key, buffered_table)`` group when the buffer is non-empty.
        """
        nonlocal buffered_bytes
        if not buffer:
            return
        if counters is not None:
            counters["flushes"] = counters.get("flushes", 0) + 1
        yield current_key, pa.Table.from_batches(buffer)  # type: ignore[misc]
        buffer.clear()
        buffered_bytes = 0

    for batch in batches:
        if batch.num_rows == 0:
            continue
        width: int = max(1, batch.nbytes // batch.num_rows)
        single: pa.Table = pa.Table.from_batches([batch])
        starts: list[int] = group_run_starts(single, routing_cols)
        for position, start in enumerate(starts):
            stop: int = starts[position + 1] if position + 1 < len(starts) else batch.num_rows
            run_key: tuple[Any, ...] = tuple(batch.column(c)[start].as_py() for c in routing_cols)
            if current_key is not None and run_key != current_key:
                yield from flush()
            current_key = run_key
            run_rows: int = stop - start
            buffer.append(batch.slice(start, run_rows))
            buffered_bytes += run_rows * width
            if counters is not None:
                counters["peak_buffered_bytes"] = max(counters.get("peak_buffered_bytes", 0), buffered_bytes)
            if flush_bytes is not None and buffered_bytes >= flush_bytes:
                yield from flush()
    yield from flush()


def apply_ttl_cast(table: pa.Table, ttl_col: str) -> pa.Table:
    """Cast the integer TTL column to ``pa.duration("s")`` so Arrow time arithmetic works natively.

    Args:
        table: The upsert table after pivot.
        ttl_col: Name of the TTL column, per :data:`TTL_COL`.

    Returns:
        Table with the TTL column cast to ``pa.duration("s")``, or unchanged when absent or
        non-integer.
    """
    if ttl_col not in table.schema.names or not pa.types.is_integer(table.schema.field(ttl_col).type):
        return table
    idx: int = table.schema.get_field_index(ttl_col)
    return table.set_column(idx, ttl_col, table.column(ttl_col).cast(pa.duration("s")))


def align_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Conform a pivoted per-bucket slice to a driver-derived canonical schema.

    Returns a table whose columns exactly match ``schema`` in name, order, and type. For each
    field in ``schema`` the table's column is reused (cast to the field type when it differs and
    the cast is safe), or a full-null column of the field type is synthesised when the table lacks
    it. Columns present in the table but absent from ``schema`` are dropped.

    This makes each parallel bulk-append task's pivoted slice union-compatible with every other
    task's slice, so their ``write_fragments`` outputs share one schema and commit as a single
    ``commit_batch`` append. Different slices see different subsets of map keys, so without this
    alignment they would produce structurally incompatible fragments.

    Args:
        table: The pivoted-and-cast slice for one bucket of one dataset.
        schema: The canonical schema for that dataset, derived on the driver.

    Returns:
        A table matching ``schema`` exactly.
    """
    present: set[str] = set(table.schema.names)
    columns: list[pa.Array | pa.ChunkedArray] = []
    for target in schema:
        if target.name in present:
            column: pa.ChunkedArray = table.column(target.name)
            if not column.type.equals(target.type):
                column = column.cast(target.type)
            columns.append(column)
        else:
            columns.append(pa.nulls(table.num_rows, target.type))
    return pa.table(columns, schema=schema)


def build_stats_batch(rows: list[tuple[Any, ...]], schema: pa.Schema) -> pa.RecordBatch:
    """Build the per-partition stats record batch.

    Args:
        rows: One tuple of values per dataset, matching the schema's column order — the routing
            column values followed by the extra stat values in schema order.
        schema: The stats schema the rows conform to, e.g. from :func:`stats_schema` or
            :func:`lance_etl.etl.bulk.bulk_stats_schema`.

    Returns:
        A record batch conforming to the given schema.
    """
    arrays: list[pa.Array] = [pa.array([row[index] for row in rows], field.type) for index, field in enumerate(schema)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
