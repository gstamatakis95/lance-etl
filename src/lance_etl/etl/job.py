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
instead of duplicating.

Heavy work (dataset reads, writes, pivot, merge) runs exclusively in Spark executors.
The driver only resolves snapshot bounds, short-circuits on empty windows, and collects stats.
Everything that maps cleanly onto Spark stays in Spark: the null-routing-row filter (whose
dropped-row count is folded into the routing plan's single ``groupBy`` scan) and the
last-write-wins collapse are all native DataFrame operations. The Arrow closure carries only what
Spark cannot express — the dynamic per-dataset map pivot and the Lance merge commits.

The increment is sized by an adaptive routing plan (:func:`lance_etl.etl.plan.compute_routing_plan`)
that salts big datasets across key-hash sub-buckets so no single partition holds a whole billion-row
org and no task serializes the entire long tail of tiny datasets. Each shuffle partition is consumed
as a stream (:func:`lance_etl.etl.pivot.stream_routing_groups`), so executor memory scales with one
dataset group rather than the whole partition.

Big datasets that are new or empty take a bulk-append fast path (:mod:`lance_etl.etl.bulk`): parallel
``write_fragments`` across their key-hash sub-buckets plus one ``commit_batch`` per dataset, instead
of thousands of per-key merge commits. Those datasets are excluded from the merge input, so every
other trio still flows through the idempotent merge path unchanged.

The I/O seams are explicit: :meth:`IcebergToLanceETL.read_increment` is the Iceberg source and
:mod:`lance_etl.etl.sink` is the Lance sink (idempotent LWW merge, chunked commits, grow-only
schema with column-role metadata, storage_options pass-through).
"""

from __future__ import annotations

import itertools
import logging
import math
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
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

from lance_etl.etl.bulk import (
    bootstrap_bulk_datasets,
    commit_bulk_transactions,
    derive_bulk_schemas,
    plan_bulk_append,
    run_bulk_append,
)
from lance_etl.etl.pivot import (
    KEY_COL,
    OP_COL,
    ROUTING_COLS,
    TTL_COL,
    ETLConfig,
    build_stats_batch,
    stats_schema,
    stats_spark_ddl,
    stream_routing_groups,
)
from lance_etl.etl.plan import (
    RoutingPlan,
    apply_salted_shuffle,
    collapse,
    compute_routing_plan,
    routing_null_predicate,
)
from lance_etl.etl.sink import apply_merge, dataset_uri
from lance_etl.fanout import TAG_FANOUT_PARTITIONS
from lance_etl.maintenance.tools import update_serving_tags
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)


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
            *[(col, is_string, "StringType") for col in [*ROUTING_COLS, KEY_COL, OP_COL]],
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
            (TTL_COL, is_integer, "LongType or IntegerType"),
        ]
        for col, predicate, expected in optional_checks:
            if col in ft and not predicate(ft[col]):
                violations.append(f"  column {col!r} must be a {expected}, got {ft[col]}")

        if violations:
            detail: str = "\n".join(violations)
            raise ValueError(f"Source schema violates the contract in docs/iceberg-source-table.sql:\n{detail}")

    def collapse(self, source: DataFrame) -> DataFrame:
        """Reduce to the last-write-wins terminal event per (routing key, vector id).

        Delegates to the shared :func:`lance_etl.etl.plan.collapse` free function so the merge and
        bulk-append paths share one collapse implementation and can never disagree on which row
        survives per key.

        Args:
            source: The source DataFrame with map columns still intact.

        Returns:
            One row per routing key and vector id, carrying the terminal op.
        """
        return collapse(source, self.config)

    def run(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> None:
        """Read one Iceberg window and route it via :meth:`run_on_dataframe`.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.apply_window_filter(self.read_increment(spark, table, start_ms, end_ms)))

    def drop_null_routing_rows(self, source: DataFrame) -> DataFrame:
        """Drop rows carrying a NULL routing value with a native Spark filter.

        Filtering before the shuffle removes the dead rows from the wire instead of carrying them
        into the executors' Arrow path. Uses the shared :func:`~lance_etl.etl.plan.routing_null_predicate`
        so the filter and the plan's null-routing count can never disagree. The dropped-row count
        itself is produced by :func:`~lance_etl.etl.plan.compute_routing_plan` in its single
        ``groupBy`` scan, not here.

        Args:
            source: The increment DataFrame, before collapse.

        Returns:
            The source with every null-routing row removed.
        """
        return source.where(~routing_null_predicate())

    def route_increment(self, filtered: DataFrame, plan: RoutingPlan) -> DataFrame:
        """Collapse the increment, salt-shuffle it by routing key, and sort each partition by the key.

        Delegates to :func:`~lance_etl.etl.plan.apply_salted_shuffle` after the last-write-wins
        collapse: big datasets are fanned across key-hash sub-buckets so no single partition holds a
        whole large org, and ``sortWithinPartitions`` runs in Spark's spill-aware shuffle sort so
        each partition reaches the Arrow closure with every dataset's rows as one contiguous run.

        Args:
            filtered: The null-routing-filtered increment DataFrame.
            plan: The adaptive routing plan for this increment.

        Returns:
            The collapsed, salt-partitioned, partition-sorted DataFrame.
        """
        return apply_salted_shuffle(self.collapse(filtered), plan)

    def merge_dataframe(self, filtered: DataFrame, plan: RoutingPlan) -> list[Any]:
        """Collapse, salt-shuffle, sort, and merge the increment, returning per-dataset stats rows.

        The salted shuffle hash-partitions by routing key (fanning big datasets across sub-buckets)
        and ``sortWithinPartitions`` orders each partition by the routing columns in Spark's
        spill-aware shuffle sort, so the Arrow closure receives each dataset's rows as one
        contiguous run streamed a group at a time. Only work that cannot be expressed in native
        Spark runs inside the ``mapInArrow`` closure: the dynamic per-dataset map pivot and the
        Lance ``merge_insert`` commits. Per-dataset stats are pre-aggregated natively before
        collection, so a big dataset flushed as several groups across partitions still returns one
        summed stats row.

        Args:
            filtered: The null-routing-filtered increment DataFrame.
            plan: The adaptive routing plan for this increment.

        Returns:
            The collected per-dataset stats rows, one per dataset this increment touched.
        """
        config: ETLConfig = self.config
        routing: list[str] = list(ROUTING_COLS)
        routed: DataFrame = self.route_increment(filtered, plan)
        partition_stats_schema: pa.Schema = stats_schema()

        def merge_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Stream and merge all routing-key groups in one Spark partition.

            Args:
                batches: Arrow batches for this task.

            Yields:
                One stats record batch when the partition wrote any dataset.
            """
            first: pa.RecordBatch | None = next(batches, None)
            if first is None:
                return
            executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
            chained: Iterator[pa.RecordBatch] = itertools.chain([first], batches)
            counters: dict[str, int] = {}
            results: list[tuple[Any, ...]] = []
            with executor_telemetry.span("lance.etl.partition"):
                try:
                    for key, group in stream_routing_groups(chained, routing, config.merge_batch_bytes, counters):
                        upserted, deleted = apply_merge(config, executor_telemetry, key, group)
                        results.append((*key, upserted, deleted))
                except Exception:
                    executor_telemetry.error("etl partition failed")
                    raise
            if counters:
                executor_telemetry.distribution("partition.flushes", counters.get("flushes", 0))
                executor_telemetry.distribution("partition.peak_buffered_bytes", counters.get("peak_buffered_bytes", 0))
            if results:
                yield build_stats_batch(results, partition_stats_schema)

        stats: DataFrame = routed.mapInArrow(merge_partition, schema=stats_spark_ddl())
        return (
            stats.groupBy(*ROUTING_COLS)
            .agg(F.sum("upserted").alias("upserted"), F.sum("deleted").alias("deleted"))
            .collect()
        )

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Validate, plan, collapse, salt-shuffle, and merge a pre-read increment into Lance datasets.

        Order: validate schema, drop null-routing rows with a native Spark filter, compute the
        adaptive routing plan (which sizes the shuffle by rows and trio count and identifies the
        big datasets to salt), run the bulk-append fast path for the big datasets that are new or
        empty (:meth:`run_bulk_phase`), then collapse LWW, salt-shuffle by routing key, and
        pivot+cast+merge the remaining trios on executors. Big new or empty datasets take the fast
        path — parallel ``write_fragments`` plus one ``commit_batch`` per dataset instead of
        thousands of per-key merge commits — and are excluded from the merge input so no row is
        written twice. Executor memory scales with one dataset group, not the increment, because
        each partition is streamed group by group. Null routing rows are counted as
        ``dataset.null_routing_rows``. An empty increment short-circuits before any merge. After
        all writes commit, every written dataset is stamped with the configured interval tag
        (:meth:`stamp_interval_tags`).

        Args:
            source: A source DataFrame carrying the operation column.
        """
        config: ETLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.etl.run") as run_span:
            try:
                self.validate_schema(source)
                filtered: DataFrame = self.drop_null_routing_rows(source)
                plan: RoutingPlan = compute_routing_plan(source, config)
                null_routing_rows: int = plan.null_routing_rows

                driver_telemetry.gauge("run.plan_datasets", plan.trio_count)
                driver_telemetry.gauge("run.plan_big_datasets", len(plan.big_trios))
                driver_telemetry.gauge("run.plan_buckets", plan.total_buckets)
                driver_telemetry.gauge("run.plan_partitions", plan.num_partitions)
                driver_telemetry.gauge("run.plan_rows", plan.total_rows)

                if null_routing_rows:
                    driver_telemetry.incr("dataset.null_routing_rows", value=null_routing_rows)
                    logger.warning("dropped %d rows with null routing key(s)", null_routing_rows)

                if plan.total_rows == 0:
                    run_span.set_tag("datasets", 0)
                    driver_telemetry.gauge("run.datasets", 0)
                    driver_telemetry.gauge("run.upserted", 0)
                    driver_telemetry.gauge("run.deleted", 0)
                    logger.info("empty increment: no datasets written")
                    return

                seen_datasets: set[tuple[str, ...]] = set()
                upserted: int = 0
                deleted: int = 0
                with driver_telemetry.timed("run.execute_ms"):
                    bulk_seen, bulk_appended, bulk_trios = self.run_bulk_phase(filtered, plan, driver_telemetry)
                    seen_datasets |= bulk_seen
                    upserted += bulk_appended
                    merge_input: DataFrame = self.exclude_bulk_trios(filtered, bulk_trios)
                    rows: list[Any] = self.merge_dataframe(merge_input, plan)
                for row in rows:
                    seen_datasets.add(tuple(row[column] for column in ROUTING_COLS))
                    upserted += int(row["upserted"] or 0)
                    deleted += int(row["deleted"] or 0)
            except Exception:
                driver_telemetry.error("etl run failed")
                raise

            datasets: int = len(seen_datasets)
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
            self.stamp_interval_tags(source.sparkSession, seen_datasets, driver_telemetry)

    def run_bulk_phase(
        self, filtered: DataFrame, plan: RoutingPlan, telemetry: Telemetry
    ) -> tuple[set[tuple[str, ...]], int, list[tuple[str, str, str, int]]]:
        """Run the bulk-append fast path for big new or empty datasets, before the merge.

        Selects the bulk-eligible big trios, derives one canonical schema per trio, bootstraps each
        empty dataset, fans parallel ``write_fragments`` appends across their key-hash sub-buckets,
        and commits each trio's fragments as one ``commit_batch`` append
        (:mod:`lance_etl.etl.bulk`). Every bootstrapped trio is excluded from the subsequent merge
        so no row is written twice. Returns nothing eligible when the fast path is disabled or every
        big trio already carries rows.

        Args:
            filtered: The null-routing-filtered increment, before collapse.
            plan: The adaptive routing plan carrying the big trios.
            telemetry: The driver telemetry facade.

        Returns:
            ``(seen_datasets, appended_rows, eligible_trios)`` — the trios written by the fast path,
            the total rows appended, and the eligible trios with sub-bucket counts to exclude from
            the merge.
        """
        config: ETLConfig = self.config
        bulk_trios: list[tuple[str, str, str, int]] = plan_bulk_append(plan, config)
        if not bulk_trios:
            return set(), 0, []
        schemas = derive_bulk_schemas(filtered, bulk_trios, config)
        eligible: list[tuple[str, str, str]] = bootstrap_bulk_datasets(schemas, config, telemetry)
        if not eligible:
            return set(), 0, []
        sub_buckets: dict[tuple[str, str, str], int] = {(o, t, n): k for o, t, n, k in bulk_trios}
        eligible_with_buckets: list[tuple[str, str, str, int]] = [
            (o, t, n, sub_buckets[(o, t, n)]) for o, t, n in eligible
        ]
        collected = run_bulk_append(filtered, eligible_with_buckets, schemas, config, telemetry)
        transactions_by_trio: dict[tuple[str, str, str], list[Any]] = {}
        for org, tenant, namespace, _, transaction in collected:
            transactions_by_trio.setdefault((org, tenant, namespace), []).append(transaction)
        appended_total: int = 0
        for trio, transactions in transactions_by_trio.items():
            _, roles, _ = schemas[trio]
            appended_total += commit_bulk_transactions(
                config, telemetry, dataset_uri(config, *trio), transactions, roles
            )
        telemetry.gauge("run.bulk_appended", appended_total)
        seen: set[tuple[str, ...]] = {trio for trio in eligible}
        return seen, appended_total, eligible_with_buckets

    def exclude_bulk_trios(self, filtered: DataFrame, bulk_trios: list[tuple[str, str, str, int]]) -> DataFrame:
        """Remove every bulk-appended trio's rows from the merge input via a broadcast left-anti join.

        The bulk fast path has already written these trios' rows, so the merge job must handle only
        the remainder. Returns the frame unchanged when no trio was bulk-appended.

        Args:
            filtered: The null-routing-filtered increment.
            bulk_trios: The trios (with sub-bucket counts) the fast path handled.

        Returns:
            The increment with every bulk-appended trio's rows removed.
        """
        if not bulk_trios:
            return filtered
        trios_df: DataFrame = filtered.sparkSession.createDataFrame(
            [(o, t, n) for o, t, n, _ in bulk_trios],
            schema="org_id string, tenant_id string, namespace string",
        )
        return filtered.join(F.broadcast(trios_df), on=list(ROUTING_COLS), how="left_anti")

    def stamp_interval_tags(
        self, spark: SparkSession, seen_datasets: set[tuple[str, ...]], telemetry: Telemetry
    ) -> None:
        """Stamp the configured interval tag on every dataset this run wrote.

        Runs once on the driver after the merge commits (never inside the executor-side
        merges, which touch one dataset from multiple partitions) and fans the create-or-move
        tag update out per dataset via :func:`~lance_etl.maintenance.tools.update_serving_tags`
        at each dataset's latest version. The fan-out width follows the same dataset-per-task
        floor as the routing shuffle, clamped below by :data:`~lance_etl.fanout.TAG_FANOUT_PARTITIONS`. A
        second run within the same hour moves that hour's tag forward, so the tag always marks the
        hour's newest version. Skipped when ``config.tag_stamp`` is unset or the run wrote nothing.

        Args:
            spark: Active Spark session.
            seen_datasets: Routing keys of every dataset the run wrote.
            telemetry: The driver telemetry facade.
        """
        config: ETLConfig = self.config
        if config.tag_stamp is None or not seen_datasets:
            return
        uris: list[str] = sorted(dataset_uri(config, *key) for key in seen_datasets)
        logger.info("stamping interval tag %r on %d datasets", config.tag_stamp, len(uris))
        with telemetry.timed("run.tag_stamp_ms"):
            results: list[dict[str, Any]] = update_serving_tags(
                spark,
                uris,
                config.telemetry,
                config.storage_options,
                tags=[config.tag_stamp],
                partitions=max(TAG_FANOUT_PARTITIONS, math.ceil(len(uris) / config.datasets_per_task)),
            )
        telemetry.gauge("run.tags_stamped", len(results))
