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
Everything that maps cleanly onto Spark stays in Spark: the key-hash batch split, the
null-routing-row filter (with an ``Observation``-based dropped-row count), and the
last-write-wins collapse are all native DataFrame operations. The Arrow closure carries only
what Spark cannot express — the dynamic per-dataset map pivot and the Lance merge commits.

Large increments are absorbed at the Spark level: ``ETLConfig.spark_batches`` splits the
increment into sequential key-hash batches, each its own Spark job over a fraction of the rows,
so a single org increment of 50M+ rows runs without raising executor memory limits.

The I/O seams are explicit: :meth:`IcebergToLanceETL.read_increment` is the Iceberg source and
:mod:`lance_etl.etl.sink` is the Lance sink (idempotent LWW merge, chunked commits, grow-only
schema with column-role metadata, storage_options pass-through).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pyarrow as pa
from pyspark.sql import Column, DataFrame, Observation, SparkSession
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
    build_stats_batch,
    group_by_routing,
    stats_schema,
    stats_spark_ddl,
)
from lance_etl.etl.sink import apply_merge, dataset_uri
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

    def select_batch(self, source: DataFrame, batch_index: int, batch_count: int) -> DataFrame:
        """Filter the increment to one Spark-level key-hash batch.

        Buckets rows by ``pmod(xxhash64(key_col), batch_count)`` natively in Spark, so the split
        is a pushed-down filter rather than driver-side work. Every event for a vector id lands in
        exactly one batch, so per-batch collapse equals global collapse restricted to that batch
        and last-write-wins is preserved across the split.

        Args:
            source: The full incremental source DataFrame.
            batch_index: Zero-based index of the batch to keep.
            batch_count: Total number of batches.

        Returns:
            The batch's rows, or the source unchanged when ``batch_count`` is one.
        """
        if batch_count <= 1:
            return source
        bucket: Column = F.pmod(F.xxhash64(F.col(self.config.key_col)), F.lit(batch_count))
        return source.where(bucket == F.lit(batch_index))

    def drop_null_routing_rows(self, source: DataFrame) -> tuple[DataFrame, Observation]:
        """Drop rows carrying a NULL routing value with a native Spark filter.

        Filtering before the shuffle removes the dead rows from the wire instead of carrying them
        into the executors' Arrow path. The dropped-row count is captured through a Spark
        ``Observation`` in the same pass, so no extra job runs for the metric.

        Args:
            source: The batch DataFrame, before collapse.

        Returns:
            ``(filtered, observation)`` where ``observation`` exposes ``null_routing_rows`` after
            an action has run on the filtered plan.
        """
        any_null: Column | None = None
        for routing_col in ROUTING_COLS:
            is_null: Column = F.col(routing_col).isNull()
            any_null = is_null if any_null is None else (any_null | is_null)
        observation: Observation = Observation()
        observed: DataFrame = source.observe(observation, F.count(F.when(any_null, True)).alias("null_routing_rows"))
        return observed.where(~any_null), observation

    def merge_dataframe(self, batch: DataFrame) -> list[Any]:
        """Collapse, shuffle, and merge one batch, returning the collected per-dataset stats rows.

        Only the work that cannot be expressed in native Spark runs inside the ``mapInArrow``
        closure: the dynamic per-dataset map pivot and the Lance ``merge_insert`` commits. The
        closure materializes its partition, groups by routing key, and merges each group into its
        dataset.

        Args:
            batch: The null-routing-filtered batch DataFrame.

        Returns:
            The collected stats rows, one per dataset this batch touched.
        """
        config: ETLConfig = self.config
        routing: list[str] = list(ROUTING_COLS)
        routed: DataFrame = self.collapse(batch).repartition(config.num_partitions, *[F.col(c) for c in routing])
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

        return routed.mapInArrow(merge_partition, schema=stats_spark_ddl()).collect()

    def run_on_dataframe(self, source: DataFrame) -> None:
        """Validate, batch, collapse, shuffle, and merge a pre-read increment into per-tenant Lance datasets.

        Order: validate schema, split into ``config.spark_batches`` key-hash batches, then per
        batch: drop null-routing rows with a native Spark filter, collapse LWW, repartition by
        routing key, pivot+cast+merge on executors. Batches run sequentially as separate Spark
        jobs, so executor memory needs scale with the batch size instead of the increment size —
        an org increment of 50M+ rows is absorbed by raising ``spark_batches``, not memory limits.
        Null routing rows are counted as ``dataset.null_routing_rows``. After all batches commit,
        every written dataset is stamped with the configured interval tag
        (:meth:`stamp_interval_tags`).

        Args:
            source: A source DataFrame carrying the operation column.
        """
        config: ETLConfig = self.config
        driver_telemetry: Telemetry = Telemetry.create(config.telemetry)
        with driver_telemetry.span("lance.etl.run") as run_span:
            self.validate_schema(source)
            batch_count: int = max(1, config.spark_batches)
            seen_datasets: set[tuple[str, ...]] = set()
            upserted: int = 0
            deleted: int = 0
            null_routing_rows: int = 0
            try:
                with driver_telemetry.timed("run.execute_ms"):
                    for batch_index in range(batch_count):
                        batch, observation = self.drop_null_routing_rows(
                            self.select_batch(source, batch_index, batch_count)
                        )
                        rows: list[Any] = self.merge_dataframe(batch)
                        null_routing_rows += int(observation.get["null_routing_rows"] or 0)
                        for row in rows:
                            seen_datasets.add(tuple(row[column] for column in ROUTING_COLS))
                            upserted += int(row["upserted"] or 0)
                            deleted += int(row["deleted"] or 0)
                        logger.info(
                            "spark batch %d/%d merged %d dataset groups",
                            batch_index + 1,
                            batch_count,
                            len(rows),
                        )
            except Exception:
                driver_telemetry.error("etl run failed")
                raise

            if null_routing_rows:
                driver_telemetry.incr("dataset.null_routing_rows", value=null_routing_rows)
                logger.warning("dropped %d rows with null routing key(s)", null_routing_rows)

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

    def stamp_interval_tags(
        self, spark: SparkSession, seen_datasets: set[tuple[str, ...]], telemetry: Telemetry
    ) -> None:
        """Stamp the configured interval tag on every dataset this run wrote.

        Runs once on the driver after all batches commit (never inside the executor-side
        merges, which touch one dataset from multiple partitions) and fans the create-or-move
        tag update out per dataset via :func:`~lance_etl.maintenance.tools.update_serving_tags`
        at each dataset's latest version. A second run within the same hour moves that hour's
        tag forward, so the tag always marks the hour's newest version. Skipped when
        ``config.tag_stamp`` is unset or the run wrote nothing.

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
                tag=config.tag_stamp,
                partitions=config.num_partitions,
            )
        telemetry.gauge("run.tags_stamped", len(results))
