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
from datetime import datetime
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

from lance_etl.etl.pivot import (
    ROUTING_COLS,
    ETLConfig,
    apply_ttl_cast,
    build_stats_batch,
    group_by_routing,
    pivot_map_columns,
    stats_schema,
    stats_spark_ddl,
)
from lance_etl.telemetry import (
    Telemetry,
    commit_with_retries,
)

logger: logging.Logger = logging.getLogger(__name__)


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


def table_chunks(table: pa.Table, batch_bytes: int | None) -> list[pa.Table]:
    """Slice a PyArrow table into zero-copy chunks whose source byte budget does not exceed ``batch_bytes``.

    The rows-per-chunk is derived from the table's actual mean row width so that any dtype — uint8,
    float32, float64 — stays within the budget without requiring manual tuning. When ``batch_bytes``
    is None or the whole table is already within the budget, returns a single-element list containing
    the original table so the caller can use the same loop for both cases.

    Args:
        table: The table to slice.
        batch_bytes: Source byte budget per chunk. None means no chunking.

    Returns:
        Ordered list of table slices covering all rows.
    """
    if batch_bytes is None or table.nbytes <= batch_bytes:
        return [table]
    bytes_per_row: int = max(1, table.nbytes // table.num_rows)
    rows_per_chunk: int = max(1, batch_bytes // bytes_per_row)
    offsets: list[int] = list(range(0, table.num_rows, rows_per_chunk))
    return [table.slice(offset, min(rows_per_chunk, table.num_rows - offset)) for offset in offsets]


def build_update_condition(ts_col: str) -> str:
    """Build the SQL condition that guards cross-window out-of-order updates.

    The condition ``source.{ts_col} >= target.{ts_col}`` ensures that a source row only overwrites
    a target row when the source timestamp is greater than or equal to the target timestamp,
    enforcing last-write-wins semantics across ETL windows.

    Tie semantics: ties (source.ts == target.ts) apply the update. This preserves idempotency
    when the same ETL window is replayed — applying the same window twice must converge to the
    same result, so equal timestamps must not block the update.

    NULL semantics for target: SQL ``x >= NULL`` evaluates to NULL, treated as FALSE by the Lance
    executor. A target row that carries a NULL ts cannot be updated by any source row. This is an
    accepted constraint: rows written before the ts column was added to the schema retain NULL and
    are skipped by the guard. Such rows can only be overwritten by a schema migration or a direct
    delete-and-reinsert. The alternative (COALESCE with an epoch literal) is not supported in the
    current lance version because the ``target.`` table-qualifier cannot appear inside function
    arguments in the DataFusion condition planner.

    NULL semantics for source: ``NULL >= target.ts`` evaluates to NULL (FALSE), so a source row
    carrying a NULL ts will never overwrite an existing target row. ``collapse`` orders NULL
    timestamps NULLS LAST within a window, treating them as the oldest event — consistent with the
    cross-window guard rejecting NULL-ts source rows. Callers must ensure ``ts_col`` is non-NULL in
    all source rows when the guard is active.

    Delete semantics: ``when_matched_delete`` does not accept a condition parameter in the Lance
    7.x API, so cross-window stale deletes (delete ts older than stored row ts) cannot be blocked
    at the Lance level. Within a window, ``collapse`` selects the terminal op per key, which
    mitigates in-window stale deletes. Cross-window stale deletes remain a known gap pending a
    future Lance API extension.

    Args:
        ts_col: Name of the timestamp column in both source and target.

    Returns:
        An SQL condition string suitable for ``when_matched_update_all(condition=...)``.
    """
    return f"source.{ts_col} >= target.{ts_col}"


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Pivot, cast, and merge one dataset group via upsert and physical delete.

    Pivots map columns, casts TTL, bootstraps a new dataset with V2 manifest paths when absent
    (concurrent-bootstrap race caught with OSError fallback), evolves schema via ``add_columns``
    when new keys appear (idempotent on retry), then runs ``merge_insert`` with
    ``when_matched_update_all(condition)``. Physical deletes use ``when_matched_delete()`` on a
    key-only table. Both paths go through :func:`commit_with_retries` with ``on_conflict``
    incrementing ``dataset.merge_conflict_retries``.

    Cross-window last-write-wins guard: when ``config.ts_col`` is present in the upsert table, the
    update condition ``source.{ts_col} >= target.{ts_col}`` prevents a later ETL batch carrying an
    older timestamp from silently overwriting a newer stored value. Without this guard, ``collapse``
    enforces last-write-wins only within a single window; across windows, arrival order would
    determine the winner instead of timestamp order.

    Ties (equal timestamps) still apply the update, preserving idempotency: replaying the same
    window twice converges to the same result. NULL source or target timestamps are never updated
    (SQL ``NULL >= x`` and ``x >= NULL`` both evaluate to NULL, treated as FALSE). See
    :func:`build_update_condition` for the full NULL and delete semantics.

    Delete guard: ``when_matched_delete`` does not accept a condition parameter in the Lance API,
    so cross-window stale deletes (delete ts older than stored row ts) are not blocked at the Lance
    level. Within a window, ``collapse`` selects the terminal op per key, which mitigates in-window
    stale deletes. Cross-window stale deletes remain a known gap.

    When ``config.merge_batch_bytes`` is set, the upsert and delete tables are sliced into
    zero-copy chunks via :func:`table_chunks` using the table's actual mean row width to derive a
    rows-per-chunk value, and each chunk is committed independently. Chunking is order-safe because
    :meth:`IcebergToLanceETL.collapse` guarantees at most one row per vector id reaches this
    function, so no key appears in more than one chunk.

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
    update_condition: str | None = (
        build_update_condition(config.ts_col) if config.ts_col in upserts_pivoted.schema.names else None
    )
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
        upsert_chunks: list[pa.Table] = table_chunks(upserts, config.merge_batch_bytes)
        num_chunks: int = len(upsert_chunks)

        def make_run_merge(chunk: pa.Table, chunk_index: int) -> Any:
            """Return a closure that merges one upsert chunk into the dataset.

            The closure captures ``chunk`` and ``chunk_index`` so each call operates on a
            fixed, immutable slice. Re-opens the dataset on every call so retries and sequential
            chunk commits see the latest version. ``add_columns`` schema evolution is idempotent.

            Args:
                chunk: The upsert slice to commit.
                chunk_index: Zero-based position in the chunk sequence, used for logging.

            Returns:
                A zero-argument callable returning merge statistics.
            """

            def run() -> dict[str, Any]:
                """Open (or bootstrap) the dataset, evolve schema, and execute the merge upsert."""
                logger.debug(
                    "dataset %s: upsert chunk %d/%d (%d rows)",
                    uri,
                    chunk_index + 1,
                    num_chunks,
                    chunk.num_rows,
                )
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
                    upserts.schema.field(name)
                    for name in upserts.schema.names
                    if name not in dataset_local.schema.names
                ]
                if missing_fields:
                    dataset_local.add_columns(pa.schema(missing_fields))
                    dataset_local = lance.dataset(uri, storage_options=config.storage_options)
                builder = dataset_local.merge_insert(on=[config.key_col])
                builder = builder.when_matched_update_all(condition=update_condition)
                return (
                    builder.when_not_matched_insert_all()
                    .conflict_retries(config.conflict_retries)
                    .retry_timeout(config.retry_timeout)
                    .execute(chunk)
                )

            return run

        try:
            with telemetry.timed("dataset.merge_ms"):
                for chunk_idx, upsert_chunk in enumerate(upsert_chunks):
                    chunk_stats: dict[str, Any] = commit_with_retries(
                        make_run_merge(upsert_chunk, chunk_idx),
                        retries=config.conflict_retries,
                        backoff_seconds=config.retry_backoff_seconds,
                        on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
                    )
                    upserted += chunk_stats.get("num_inserted_rows", 0) + chunk_stats.get("num_updated_rows", 0)
            telemetry.incr("dataset.merged")
        except Exception:
            telemetry.incr("dataset.merge_error")
            raise

    if deletes.num_rows:
        delete_chunks: list[pa.Table] = table_chunks(deletes, config.merge_batch_bytes)
        num_delete_chunks: int = len(delete_chunks)

        def make_run_delete(chunk: pa.Table, chunk_index: int) -> Any:
            """Return a closure that deletes one chunk of key rows from the dataset.

            The closure captures ``chunk`` and ``chunk_index`` so each call operates on a fixed
            slice. Re-opens the dataset on every call so retries and sequential chunk commits see
            the latest version.

            Args:
                chunk: Key-only delete slice.
                chunk_index: Zero-based position in the chunk sequence, used for logging.

            Returns:
                A zero-argument callable returning delete statistics.
            """

            def run() -> dict[str, Any]:
                """Re-open the dataset and execute ``when_matched_delete`` on one delete chunk."""
                logger.debug(
                    "dataset %s: delete chunk %d/%d (%d rows)",
                    uri,
                    chunk_index + 1,
                    num_delete_chunks,
                    chunk.num_rows,
                )
                try:
                    delete_dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
                except (FileNotFoundError, ValueError):
                    return {}
                return (
                    delete_dataset.merge_insert(on=[config.key_col])
                    .when_matched_delete()
                    .conflict_retries(config.conflict_retries)
                    .retry_timeout(config.retry_timeout)
                    .execute(chunk)
                )

            return run

        with telemetry.timed("dataset.delete_ms"):
            for del_chunk_idx, delete_chunk in enumerate(delete_chunks):
                delete_stats: dict[str, Any] = commit_with_retries(
                    make_run_delete(delete_chunk, del_chunk_idx),
                    retries=config.conflict_retries,
                    backoff_seconds=config.retry_backoff_seconds,
                    on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
                )
                deleted += delete_stats.get("num_deleted_rows", 0)

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
                executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
                table: pa.Table = pa.Table.from_batches(collected)
                valid_mask: pa.Array | None = None
                for routing_col in routing:
                    col_valid: pa.Array = pc.is_valid(table[routing_col])
                    valid_mask = col_valid if valid_mask is None else pc.and_(valid_mask, col_valid)
                if valid_mask is not None:
                    null_count: int = int(pc.sum(pc.invert(valid_mask)).as_py() or 0)
                    if null_count:
                        executor_telemetry.incr("dataset.null_routing_rows", value=null_count)
                        logger.warning("dropped %d rows with null routing key(s) in this partition", null_count)
                        table = table.filter(valid_mask)
                if not table.num_rows:
                    return
                results: list[tuple[Any, ...]] = []
                with executor_telemetry.span("lance.etl.partition"):
                    try:
                        for key, group in group_by_routing(table, routing):
                            upserted, deleted = apply_merge(config, executor_telemetry, key, group)
                            results.append((*key, upserted, deleted))
                    except Exception:
                        executor_telemetry.error("etl partition failed")
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
