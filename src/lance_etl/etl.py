"""Iceberg-to-Lance ETL routing changes into per-tenant datasets.

Reads a time range of changes from an Iceberg table that carries an operation column (insert,
update, delete), dynamically pivots all keys of every map column into concrete indexable columns,
collapses to the last-write-wins terminal state per vector id, and applies each routing key's rows
to exactly one Lance dataset identified by the fixed routing trio ``(org_id, tenant_id, namespace)``
with a ``merge_insert`` upsert plus a ``when_matched_delete`` merge-delete for the physical-delete
path.

The source schema is the contract defined in ``docs/iceberg-source-table.sql``. Input types
carry no uncertainty: vectors are ``MAP<STRING, ARRAY<FLOAT>>``, texts and metadata are
``MAP<STRING, STRING>``, and ttl is ``BIGINT`` (seconds). There are no caller-supplied type maps.
All casts are contract-driven and automatic: vector columns are cast to inferred fixed-size lists
and the TTL column is cast to ``pa.duration("s")`` when present.

Map pivot (executor-side, per dataset group)
--------------------------------------------
The source Iceberg table carries three map columns: ``vectors`` (``MAP<STRING, ARRAY<FLOAT>>``,
named float-array embeddings), ``texts`` (``MAP<STRING, STRING>``, named text fields), and
``metadata`` (``MAP<STRING, STRING>``, arbitrary string metadata). Lance has no map type, so all
three maps are fully unpacked before write.

The pivot is performed by :func:`pivot_map_columns` on the executor, applied to each routing
key's upsert table immediately before the cast and merge steps. For each map column present in
the Arrow table schema, :func:`pivot_map_columns` discovers all distinct keys present in that
table (within this dataset group), skips any key that collides with an already-present column or
reserved name (counted and metered), and calls ``pyarrow.compute.map_lookup`` with
``occurrence="last"`` to extract the per-row value for each valid key as a new column. The map
column itself is then dropped. Because the discovery and pivot happen per executor per routing key
group, different org datasets can have completely different pivot column sets: an org that only
uses ``embedding_v2`` in its ``vectors`` map gets exactly one pivoted vector column, and another
org's ``embedding_v1`` column never appears in the first org's dataset.

Vector columns extracted from the ``vectors`` map arrive as ``list<float32>`` (the Arrow
representation of Spark ``ARRAY<FLOAT>``). :func:`pivot_map_columns` infers the fixed-size-list
dimension from the first non-null entry in each extracted column. A vector column whose every
value is null is kept as a nullable ``list<float32>`` column (no FSL cast is attempted, since no
dimension can be inferred). If the inner value type is not already float32, it is cast to float32
as defensive normalization (Spark may widen ARRAY<FLOAT> to float64 in some environments).

Metadata keys become real, filter-eligible, scalar-index-ready columns: a row with
``metadata["region"] = "eu-west"`` produces a ``region STRING`` column in that org's dataset,
which downstream BTREE or BITMAP scalar indexes can cover.

Grow-only dataset schema guarantee: the per-dataset Lance schema is strictly additive. Keys that
stop appearing in the source leave their column in place with NULL values for new rows, so existing
readers and indexes are never broken. New keys that appear in a later ETL window are absorbed by
the existing ``add_columns`` schema evolution in :func:`apply_merge` without any operator
intervention: the first batch that carries the new key adds a nullable column to the dataset schema
and subsequent writes fill it normally. No code path ever removes a dataset column.

Colliding keys are silently skipped: no key error is ever fatal, they are counted in
the return value of :func:`pivot_map_columns`, and :func:`apply_merge` emits a
``dataset.invalid_map_keys`` metric and a WARNING log line when the count is non-zero.

Routing is fixed: the dataset path is ``base_uri/org_id/tenant_id/namespace.lance`` per the
source contract in ``docs/iceberg-source-table.sql``. The collapse window, the routing
repartition, and the per-partition Arrow ``group_by`` all derive from :data:`ROUTING_COLS`.

Each key lives in exactly one dataset, so the ``merge_insert`` keyed on ``key_col`` is the sole
dedup mechanism: a re-upsert of an existing key updates it in place and a delete reaches the one
dataset that holds it. No cross-dataset reader deduplication is required.

Backfills are catch-up replays: rerun this same incremental job over the historical windows with
the orchestrator (for example Airflow). Because every window is an idempotent merge keyed by
vector id, a replayed or retried window converges instead of duplicating, so no separate bulk path
is needed.

An optional timestamp window filter (``window_start`` / ``window_end`` / ``window_column`` on
:class:`ETLConfig`) can narrow the rows that reach the collapse and merge steps to those whose
``window_column`` value falls within ``[window_start, window_end)``. Both bounds are ISO-8601
strings validated with ``datetime.fromisoformat``. An absent bound means the bound is open (no
filter on that side). The filter is applied as a Spark ``DataFrame.filter`` call immediately after
the Iceberg read so Spark can push it down into the Iceberg scan for partition pruning.

The single canonical time clock is the source event timestamp column named by
``ETLConfig.ts_col`` (default ``"event_timestamp"``). Date-range queries are expressed as scalar
range filters on that column, which can be pruned by a BTREE scalar index. There is no derived
date column and no ingest-time column: the event timestamp is authoritative for ordering, collapse,
and time-bounded serving. See ADR 0016 for the rationale and tradeoffs.

Cross-contamination is prevented structurally: the dataset URI is a validated pure function of the
routing columns and rows are shuffled by routing key, so a row can only reach its own dataset. The
map columns are unpacked before write, and because the pivot is per-dataset the schemas stay
minimal with no cross-org column pollution.

Physical deletes use ``merge_insert(...).when_matched_delete().execute(deletes)`` with the same
:func:`commit_with_retries` wrapper as the upsert path, so retries re-read the dataset at the
latest version and ``when_matched_delete`` touches only rows whose key matches the source
key-only table.

The optional ``changed_uris_path`` on :class:`ETLConfig` accepts an object-store path. When set,
the driver writes one dataset URI per line (sorted, UTF-8) to that path after every run, listing
exactly the datasets touched in the window, for downstream maintenance and index jobs to consume.
The file is always written, even when empty (zero datasets touched), so consumers can distinguish
an idle window from a missing run.

Small-and-big efficiency: the per-tenant population is power-law shaped (tens of thousands of orgs,
most tiny, a few huge), so the ETL never does per-row work on the driver. The driver only resolves
the Iceberg snapshot bounds from table metadata, short-circuits to an empty read when no snapshot
landed in the window, and broadcasts the routing plan. All collapse, routing, and merge work runs
in executors: rows shuffle by routing key into ``num_partitions`` co-located partitions, and
``merge_partition`` groups each partition's rows by routing key and applies one keyed, idempotent
``merge_insert`` per dataset. A tiny org's increment is a small group merged in process on one
executor at near-zero cost, a huge org's increment co-locates to its partition and merges there,
and an org with no rows in the window produces no group and touches no dataset. Bootstrapping a
brand-new tiny dataset is a single empty append plus merge, never a cluster-wide fan-out.

Requires pylance and the Datadog Agent on the executors.
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

from lance_etl.cloud_storage import resolve_filesystem, write_object
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
    """Build the per-dataset stats schema for the fixed routing columns.

    Returns:
        A schema with one string column per routing column plus ``upserted`` and ``deleted``
        counters.
    """
    fields: list[tuple[str, pa.DataType]] = [(column, pa.string()) for column in ROUTING_COLS]
    fields.extend([("upserted", pa.int64()), ("deleted", pa.int64())])
    return pa.schema(fields)


def stats_spark_ddl() -> str:
    """Build the Spark DDL string matching :func:`stats_schema`.

    Returns:
        A DDL string usable as the ``mapInArrow`` output schema.
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
        ts_col: Source event timestamp column. Used for last-write-wins collapse and written into
            every dataset as the single canonical time clock. Date-range queries on the written
            datasets are expressed as scalar range filters on this column, pruned by a BTREE scalar
            index when one is configured. Defaults to ``"event_timestamp"`` matching the SQL
            contract.
        op_col: Operation column carrying insert, update, or delete.
        delete_op_values: Operation values treated as deletes. Others upsert.
        ttl_col: Name of the optional per-row lifetime column in the source (``BIGINT`` seconds
            per the SQL contract). When present in the upsert table with an integer type, it is
            cast automatically to ``pa.duration("s")`` so the maintenance TTL predicate
            ``event_timestamp + ttl < now`` evaluates natively. An absent column means no cast is
            performed.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Shuffle partitions for routing co-location.
        conflict_retries: Retry budget for concurrent merge commits.
        retry_timeout: Total time budget for conflict retries. Raised above the 30-second Lance
            default to give headroom on hot multi-tenant datasets.
        iceberg_read_options: Extra Iceberg reader options merged into the read.
        window_start: ISO-8601 lower bound (inclusive) for the source timestamp window filter.
            Absent means open. Validated with ``datetime.fromisoformat`` at filter-application
            time; a malformed value raises ``ValueError`` before any Spark work runs.
        window_end: ISO-8601 upper bound (exclusive) for the source timestamp window filter.
            Absent means open. Validated with ``datetime.fromisoformat`` at filter-application
            time.
        window_column: Column used for the timestamp window pushdown filter. Defaults to
            ``"processing_timestamp"`` matching the SQL contract.
        retry_backoff_seconds: Base backoff in seconds for the Python-side commit-conflict retry
            loop that observes the merge conflict count. Tests set this to ``0.0`` to avoid
            sleeping.
        changed_uris_path: When set, the driver writes one dataset URI per line (UTF-8, sorted) to
            this object-store path after the run, listing exactly the datasets touched in the
            window. The file is always written, even when no datasets were touched (empty file), so
            consumers can distinguish an idle window from a missing run. Downstream maintenance and
            index jobs consume this list to limit their scope to touched datasets.
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
    changed_uris_path: str | None = None


def dataset_uri(config: ETLConfig, *components: str) -> str:
    """Build the validated dataset URI for one routing key.

    The path is ``base_uri/org_id/tenant_id/namespace.lance`` per :data:`ROUTING_COLS`, which is
    byte-identical to the historical layout.

    Args:
        config: ETL configuration.
        *components: One routing value per column in :data:`ROUTING_COLS`, in path order.

    Returns:
        The dataset URI confined to the routing-key prefix.

    Raises:
        ValueError: If the component count does not match three (the fixed routing depth), or any
            component is not a non-empty string. NULL routing rows are filtered upstream in
            ``merge_partition`` before this function is called.
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
    """Cast one extracted vector column to a fixed-size list, inferring dimension from data.

    The dimension is the length of the first non-null value. A column whose every value is null is
    left unchanged, since no dimension can be inferred. Rows whose length differs from the inferred
    dimension are replaced with null and counted into ``invalid_counts``, then the whole column is
    cast to ``fixed_size_list<float32, dim>`` in one step (Arrow casts the inner values to float32
    as part of the same cast, covering sources widened to float64).

    Args:
        table: The table containing the column to cast.
        col_name: Name of the column to cast.
        invalid_counts: Mutable accumulator for wrong-dimension row counts, updated in place.

    Returns:
        The table with the column cast to a fixed-size list type, or unchanged when no dimension
        is available.
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
    """Expand every map column into concrete per-key columns on the executor.

    For each of the contract map columns ``vectors``, ``texts``, and ``metadata`` that is present
    in ``table`` as a ``pa.MapType`` column, this function:

    1. Discovers all distinct keys present in this table's data (within this dataset group) by
       scanning the map key arrays of each chunk and collecting unique non-null values.
    2. Checks that the key does not collide with an already-present column or a reserved name
       (routing columns, the key, op, timestamp, and window columns). Colliding keys are skipped.
    3. Calls ``pyarrow.compute.map_lookup(column, query_key=key, occurrence="last")`` to extract
       the per-row value for that key as a new ``ChunkedArray``. Rows where the key is absent
       yield null.
    4. Appends the extracted column to the table under the key's name.
    5. After all keys of that map column are processed, drops the map column from the table.

    For columns derived from the ``vectors`` map, the extracted value type is
    ``list<float32>`` (contract: ``ARRAY<FLOAT>``). Each such column is then routed through
    :func:`apply_fsl_cast`: the dimension is inferred from the first non-null entry and the inner
    type is normalized to float32 if needed. A vectors-map column whose every value is null is
    kept as a nullable ``list<float32>`` column with no FSL cast applied.

    Colliding-key counts are returned under ``"invalid_map_keys"`` and wrong-dimension vector row
    counts under ``"invalid_vector_rows"``. Neither is fatal. A key that collides with an existing
    or reserved column is skipped and counted. Any key string that does not collide is accepted as
    a column name as-is.

    Args:
        table: The upsert table for one dataset group, after Spark serialisation.
        config: ETL configuration providing the set of reserved column names.

    Returns:
        A ``(result_table, counts)`` pair where ``result_table`` has all map columns replaced by
        their per-key concrete columns and ``counts`` carries ``"invalid_map_keys"`` (skipped
        colliding keys) and ``"invalid_vector_rows"`` (null-outs from wrong-dimension vectors)
        when non-zero.
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


def emit_changed_uris(config: ETLConfig, uris: list[str]) -> None:
    """Write the sorted unique list of touched dataset URIs to ``config.changed_uris_path``.

    Resolves the target filesystem via :func:`~lance_etl.cloud_storage.resolve_filesystem` and
    writes one URI per line (UTF-8) to the configured path. The file is always written, even when
    ``uris`` is empty, so consumers can distinguish an idle window from a missing run. This
    function is a no-op when ``config.changed_uris_path`` is None.

    Args:
        config: ETL configuration; ``changed_uris_path`` must be set.
        uris: The dataset URIs touched during the run, in any order.
    """
    if config.changed_uris_path is None:
        return
    content: bytes = "\n".join(sorted(set(uris))).encode("utf-8")
    filesystem, path = resolve_filesystem(config.changed_uris_path, config.storage_options)
    write_object(filesystem, path, content)


def apply_ttl_cast(table: pa.Table, ttl_col: str) -> pa.Table:
    """Cast the TTL column from an integer type to ``pa.duration("s")`` when present.

    The source contract defines ``ttl`` as a ``BIGINT`` (seconds). This function casts it to
    ``pa.duration("s")`` so the maintenance TTL predicate ``event_timestamp + ttl < now``
    evaluates natively in Arrow. If the column is absent or already a duration type, the table is
    returned unchanged. Only integer types (int32, int64, and their unsigned counterparts) trigger
    the cast; other types are left untouched.

    Args:
        table: The upsert table after pivot, potentially carrying the TTL column.
        ttl_col: Name of the TTL column per ``ETLConfig.ttl_col``.

    Returns:
        The table with the TTL column cast to ``pa.duration("s")``, or unchanged when the column
        is absent or already a duration type.
    """
    if ttl_col not in table.schema.names:
        return table
    col_type: pa.DataType = table.schema.field(ttl_col).type
    if pa.types.is_duration(col_type):
        return table
    if not pa.types.is_integer(col_type):
        return table
    col_idx: int = table.schema.get_field_index(ttl_col)
    return table.set_column(col_idx, ttl_col, table.column(ttl_col).cast(pa.duration("s")))


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Apply one dataset's terminal rows with merge upsert and merge-delete.

    The per-dataset Lance schema is grow-only: columns are only ever added, never removed. Keys
    that stop appearing in the source leave their column in place with NULL values for new rows,
    so existing readers and indexes are never broken. New keys appearing in a later ETL window are
    absorbed by ``add_columns`` schema evolution without operator intervention.

    Before merging, the upsert table is passed through :func:`pivot_map_columns` to expand all map
    columns (``vectors``, ``texts``, ``metadata``) into concrete per-key columns. Every distinct
    key present in this routing key's data becomes a column: key equals column name, value equals
    column value, absent keys yield null. Keys that collide with an existing or reserved column are
    silently skipped and counted; the total is emitted as ``dataset.invalid_map_keys`` with a
    WARNING log line naming the URI when non-zero.

    After pivot, vector columns (from the vectors map) carry wrong-dimension null-outs counted via
    the ``dataset.invalid_vector_rows`` metric (accumulated by :func:`apply_fsl_cast` inside
    :func:`pivot_map_columns`). The TTL column, when present with an integer type, is cast to
    ``pa.duration("s")`` by :func:`apply_ttl_cast` so the maintenance predicate evaluates natively.
    The deletes path is key-only and needs no cast.

    Bootstrap strategy: when the dataset does not exist yet, an empty table is written with
    ``lance.write_dataset(..., mode='append')``, which creates the dataset if absent. If a
    concurrent first writer wins the creation race, the bootstrap write may raise ``OSError``; in
    that case the loser falls back to re-opening the existing dataset with ``lance.dataset(...)``.
    ``enable_v2_manifest_paths=True`` is always passed on this bootstrap write because V2 manifest
    paths are a creation-time naming choice, so bootstrapping with V2 names makes every later open
    of the dataset a single object-store request instead of a version-count-proportional LIST. The
    rows themselves always flow through ``merge_insert`` so a re-upsert of an existing key updates
    it in place instead of duplicating it.

    Schema evolution: before executing the upsert, the columns present in ``upserts`` but absent
    from the existing dataset schema are added as all-NULL columns via
    ``LanceDataset.add_columns(pa.schema(missing_fields))``. This is a metadata-only operation
    and is idempotent: if a retry re-enters this path after the schema was already evolved, the
    missing-field check finds nothing to add. Schema evolution runs only on the non-bootstrap path;
    for a fresh dataset the schema is inferred directly from ``upserts``.

    Physical deletes use ``merge_insert(on=[key_col]).when_matched_delete().execute(deletes)``
    wrapped in :func:`commit_with_retries` with the same retry budget and backoff as the upsert
    path. The ``deletes`` table is a key-only subschema, which is a valid source for
    ``merge_insert``. The action re-reads the dataset at the latest version on every retry so each
    attempt observes the current state.

    The merge ``execute()`` return dict provides authoritative row counts
    (``num_inserted_rows``, ``num_updated_rows``, ``num_deleted_rows``). We report those rather
    than recomputing from the source table.

    Conflict visibility: Lance does not surface its internal ``num_attempts`` through the pylance
    merge stats dict, so each retryable commit conflict observed by :func:`commit_with_retries`
    increments the ``dataset.merge_conflict_retries`` counter directly via the ``on_conflict``
    callback, on both the upsert and the merge-delete path. The builder keeps its own
    ``conflict_retries`` so Lance's internal handling of write contention
    (``Error::TooMuchWriteContention``, which is intentionally not a retryable marker for the
    Python loop) is unchanged; the Python wrapper is a strictly-additive outer layer for
    commit-conflict markers and never reduces the existing retry budget.

    The merge builder always uses ``when_matched_update_all()`` with no condition because the
    last-write-wins collapse already orders by event timestamp upstream, so every row in the upsert
    table is already the correct terminal state.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: The routing key, one value per column in :data:`ROUTING_COLS` in path order.
        group: Rows for this routing key carrying the op column.

    Returns:
        The counts of upserted (inserted + updated) and deleted rows.
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
            """Open or bootstrap the dataset, evolve its schema if needed, and execute the merge upsert once.

            The dataset is opened fresh on every call so retries always see the latest committed
            version. When the dataset does not exist yet, an empty bootstrap write creates it; if
            a concurrent writer wins that race the resulting ``OSError`` is caught and the dataset
            is opened instead. After opening, any columns present in ``upserts`` but absent from
            the dataset schema are added as all-NULL columns via
            ``add_columns(pa.schema(missing_fields))`` before executing the merge. This
            schema-evolution step is idempotent: a retry that re-enters after the schema was
            already evolved finds no missing fields.

            The merge builder uses ``when_matched_update_all()`` with no condition: the
            last-write-wins collapse upstream already produces the correct terminal state for every
            key, so no additional timestamp guard is needed.

            Returns:
                The merge statistics dictionary with the authoritative row counts.
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
            """Re-open the dataset at the latest version and execute the merge-delete once.

            Uses ``merge_insert(on=[key_col]).when_matched_delete().execute(deletes)`` so only
            rows whose key matches the source key-only table are removed. Skips when the dataset
            does not exist.

            Returns:
                The merge statistics dictionary, or an empty dict when the dataset was absent.
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
    """Resolve a wall-clock window to Iceberg snapshot-id bounds via the snapshots metadata table.

    Queries ``{table}.snapshots`` and walks the snapshots in ``committed_at`` order. The start
    bound is the last snapshot committed strictly before ``start_ms`` — the state the previous
    window already processed, used as the exclusive ``start-snapshot-id`` of an incremental append
    scan. The end bound is the last snapshot committed at or before ``end_ms`` — the inclusive
    ``end-snapshot-id``. Either bound is None when no snapshot satisfies it. The third element
    reports whether any snapshot was committed inside the window itself (``start_ms <=
    committed_at <= end_ms``). Callers must gate the empty-window short circuit on that flag
    rather than on ``start_id == end_id``, which conflates a genuinely empty window with bound ids
    that merely resolve to the same historical snapshot.

    Args:
        spark: Active Spark session.
        table: Fully qualified Iceberg table name.
        start_ms: Window start in epoch milliseconds.
        end_ms: Window end in epoch milliseconds.

    Returns:
        ``(start_id, end_id, has_new_snapshots)`` where the ids are None when no snapshot
        satisfies the bound and ``has_new_snapshots`` is True when at least one snapshot was
        committed within the window.
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
        """Read the rows committed to an Iceberg table within a wall-clock window.

        Iceberg 1.10 rejects the ``start-timestamp`` / ``end-timestamp`` read options outside
        changelog scans (``SparkScanBuilder``: "Cannot set start-timestamp or end-timestamp for
        incremental scans and batch scan. They are only valid for changelog scans."), so the window
        is first resolved to snapshot ids through :func:`snapshot_id_bounds` over the
        ``{table}.snapshots`` metadata table. When a snapshot exists strictly before the window
        start, the read is an incremental append scan bounded by ``start-snapshot-id`` (exclusive)
        and ``end-snapshot-id`` (inclusive). When the table has no snapshot before the window start
        (first run), the read falls back to a full batch scan pinned to the window's last snapshot
        via ``snapshot-id``. When no snapshot at all resolves the end bound, or when no snapshot
        was committed inside the window, an empty DataFrame with the current table schema is
        returned. ``iceberg_read_options`` are merged into every non-empty read.

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

        Filters ``source`` to rows where ``config.window_column`` falls within
        ``[window_start, window_end)``. Both bounds are ISO-8601 strings. Each configured bound is
        validated by ``datetime.fromisoformat`` before interpolation; the 'Z' UTC designator (e.g.
        ``"2024-01-01T00:00:00Z"``) is accepted natively on Python 3.11+. A bound string that does
        not parse raises ``ValueError`` with a clear message before any Spark work runs. An absent
        bound leaves that side of the interval open. The filter is applied as a ``DataFrame.filter``
        SQL-string predicate before any shuffle so Spark can push it down into the Iceberg scan for
        partition pruning. When neither bound is set the DataFrame is returned unchanged (full-table
        behaviour).

        Args:
            source: The incremental source DataFrame produced by :meth:`read_increment`.

        Returns:
            The filtered DataFrame, or the original if no window bounds are configured.

        Raises:
            ValueError: If a configured bound string is not a valid ISO-8601 datetime.
        """
        config: ETLConfig = self.config
        if config.window_start is None and config.window_end is None:
            return source

        def parse_bound(value: str) -> str:
            """Validate an ISO-8601 bound string and return it unchanged for interpolation.

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
        """Verify the source schema against the SQL contract in ``docs/iceberg-source-table.sql``.

        Checks that every required column exists with the expected Spark type and that optional
        columns, when present, carry their contracted types. Extra payload columns not listed here
        are allowed and pass through unchanged. Key-level validation (identifier allowlist,
        collision checks) happens at merge time in :func:`pivot_map_columns` per routing-key
        group, since different orgs use different map keys.

        Required column presence and types:
          - Routing columns (``org_id``, ``tenant_id``, ``namespace``), ``key_col``, and
            ``op_col``: ``StringType``.
          - ``ts_col`` and ``window_column``: ``TimestampType`` or ``TimestampNTZType``.

        Optional columns — when present, must be MapType (wrong type raises):
          - ``vectors``: must be a MapType with ``ArrayType(FloatType|DoubleType)`` values.
          - ``texts`` and ``metadata``: must be a MapType with ``StringType`` values.
          - ``ttl_col``: ``LongType`` or ``IntegerType``.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If any required column is missing, a required column has the wrong type,
                or an optional column is present with a type that violates the contract. All
                violations are collected and reported together in one message that references
                ``docs/iceberg-source-table.sql`` as the authoritative contract.
        """
        config: ETLConfig = self.config
        field_types: dict[str, Any] = {f.name: f.dataType for f in source.schema.fields}
        violations: list[str] = []

        string_cols: list[str] = [*ROUTING_COLS, config.key_col, config.op_col]
        for col in string_cols:
            if col not in field_types:
                violations.append(f"  missing required column {col!r} (expected StringType)")
            elif not isinstance(field_types[col], StringType):
                violations.append(f"  column {col!r}: expected StringType, got {type(field_types[col]).__name__}")

        ts_cols: list[str] = [config.ts_col, config.window_column]
        for col in ts_cols:
            if col not in field_types:
                violations.append(f"  missing required column {col!r} (expected TimestampType or TimestampNTZType)")
            elif not isinstance(field_types[col], (TimestampType, TimestampNTZType)):
                violations.append(
                    f"  column {col!r}: expected TimestampType or TimestampNTZType,"
                    f" got {type(field_types[col]).__name__}"
                )

        if "vectors" in field_types:
            vt = field_types["vectors"]
            if not (
                isinstance(vt, MapType)
                and isinstance(vt.keyType, StringType)
                and isinstance(vt.valueType, ArrayType)
                and isinstance(vt.valueType.elementType, (FloatType, DoubleType))
            ):
                violations.append(
                    f"  column 'vectors' must be a MapType(StringType, ArrayType(FloatType|DoubleType)), got {vt}"
                )

        for col in ("texts", "metadata"):
            if col in field_types:
                ct = field_types[col]
                if not (
                    isinstance(ct, MapType)
                    and isinstance(ct.keyType, StringType)
                    and isinstance(ct.valueType, StringType)
                ):
                    violations.append(f"  column {col!r} must be a MapType(StringType, StringType), got {ct}")

        if config.ttl_col in field_types and not isinstance(field_types[config.ttl_col], (LongType, IntegerType)):
            violations.append(
                f"  column {config.ttl_col!r}: expected LongType or IntegerType,"
                f" got {type(field_types[config.ttl_col]).__name__}"
            )

        if violations:
            detail: str = "\n".join(violations)
            raise ValueError(f"Source schema violates the contract in docs/iceberg-source-table.sql:\n{detail}")

    def collapse(self, source: DataFrame) -> DataFrame:
        """Reduce to the last-write-wins terminal event per id within a tenant.

        The window orders by ``config.ts_col`` descending with nulls last, so NULL-timestamp rows
        lose to any timestamped row. Within equal timestamps, a deterministic tiebreaker is
        applied: ``xxhash64`` over every non-MapType column of the DataFrame, ascending, so the
        winner is stable across retries and the result is reproducible given the same input rows.
        MapType columns are excluded because Spark's ``xxhash64`` cannot hash map values. Map
        columns that remain in the Spark DataFrame at this stage (they are not pivoted until merge
        time on the executor) are silently excluded from the tiebreaker hash for that reason.

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
        """Read, transform, and route one time range of Iceberg changes.

        Resolves the Iceberg snapshot bounds, reads the incremental rows, applies the optional
        timestamp window filter via :meth:`apply_window_filter`, and delegates to
        :meth:`run_on_dataframe` for collapse and routing. The event timestamp column
        (``config.ts_col``) is the single canonical clock for ordering and collapse.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.apply_window_filter(self.read_increment(spark, table, start_ms, end_ms)))

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Transform and route a pre-read increment.

        Validates the source schema against the contract in ``docs/iceberg-source-table.sql``,
        collapses to the last-write-wins terminal row per routing key and vector id using the event
        timestamp, and routes each routing key's rows to its Lance dataset via ``merge_insert``.
        Map columns (``vectors``, ``texts``, ``metadata``) ride through the Spark shuffle intact
        and are pivoted on the executor inside :func:`apply_merge` via :func:`pivot_map_columns`.
        Vector columns are cast to inferred fixed-size lists and the TTL column is cast to
        ``pa.duration("s")`` automatically. The order is: validate, collapse, shuffle, pivot+cast,
        merge. The event timestamp column (``config.ts_col``) is the single canonical clock: it
        drives the collapse order and is available for scalar range filters on the written datasets.
        No ingest-time column is added.

        Routing rows with a null value in any routing column are silently dropped before the
        group-by: rows that cannot be routed to a dataset URI are invalid and routing them would
        raise ``ValueError`` from :func:`dataset_uri`. The count of dropped rows is emitted as
        ``dataset.null_routing_rows`` and logged at WARNING level.

        Stats collection uses a single Spark action (``collect()``) over the ``mapInArrow``
        output. Each row in the collected result represents one touched dataset. Totals are
        aggregated in Python on the driver over the bounded result set (bounded by fleet size).
        When ``config.changed_uris_path`` is set, the sorted list of touched dataset URIs is
        written to that path via :func:`emit_changed_uris` after totals are computed.

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
                """Merge one Spark partition's datasets on an executor.

                Before grouping by routing key, rows with a null value in any routing column are
                filtered out. Such rows cannot be routed to a valid dataset URI and routing them
                would raise ``ValueError``. The count of dropped rows is emitted as
                ``dataset.null_routing_rows`` and logged at WARNING.

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

            if config.changed_uris_path is not None:
                touched_uris: list[str] = [dataset_uri(config, *[str(row[c]) for c in routing]) for row in rows]
                emit_changed_uris(config, touched_uris)
