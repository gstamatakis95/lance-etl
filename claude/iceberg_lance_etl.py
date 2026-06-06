"""Hourly Iceberg-to-Lance ETL routing changes into per-tenant datasets.

Reads a time range of changes from an Iceberg table that carries an operation
column (insert, update, delete), flattens the two map columns into struct-free
parallel arrays, collapses to the last-write-wins terminal state per vector id,
casts columns to caller-supplied Arrow types, and applies each routing key's
rows to exactly one Lance dataset identified by ``(org_id, tenant_id,
namespace)`` with a ``merge_insert`` upsert plus a ``when_matched_delete``.

Backfills are catch-up replays: rerun this same incremental job over the
historical windows with the orchestrator (for example Airflow). Because every
window is an idempotent merge keyed by vector id, a replayed or retried window
converges instead of duplicating, so no separate bulk path is needed.

Cross-contamination is prevented structurally: the dataset URI is a validated
pure function of the routing columns and rows are shuffled by routing key, so a
row can only reach its own dataset. Lance has no map type and structs are not
used downstream, so ``vectors`` and ``metadata`` are flattened with
``map_keys``/``map_values`` into ``{col}_keys`` and ``{col}_values`` parallel
list columns associated by index. ``conflict_retries`` makes concurrent runs on
the same dataset safe; any other failure propagates so the job fails fast.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

STATS_SCHEMA: pa.Schema = pa.schema(
    [
        ("org_id", pa.string()),
        ("tenant_id", pa.string()),
        ("namespace", pa.string()),
        ("upserted", pa.int64()),
        ("deleted", pa.int64()),
    ]
)


@dataclass
class ETLConfig:
    """Configuration for :class:`IcebergToLanceETL`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live.
        telemetry: Telemetry configuration.
        key_col: Unique vector id column and per-dataset merge key.
        org_col: Organisation routing column.
        tenant_col: Tenant routing column.
        namespace_col: Namespace routing column.
        vectors_col: Map column of vectors flattened into parallel arrays.
        metadata_col: Map column of metadata flattened into parallel arrays.
        ts_col: Event timestamp column used for last-write-wins collapse.
        op_col: Operation column carrying insert, update, or delete.
        delete_op_values: Operation values treated as deletes; others upsert.
        column_types: Map of column name to target Arrow type, for types Spark
            cannot express such as float16.
        storage_options: Object-store options forwarded to pylance.
        num_partitions: Shuffle partitions for routing co-location.
        conflict_retries: Retry budget for concurrent merge commits.
        guard_updates_by_ts: Enable the strict timestamp guard on updates.
        iceberg_read_options: Extra Iceberg reader options merged into the read.
        path_component_pattern: Allowed pattern for each routing component.
    """

    base_uri: str
    telemetry: TelemetryConfig
    key_col: str = "vector_id"
    org_col: str = "org_id"
    tenant_col: str = "tenant_id"
    namespace_col: str = "namespace"
    vectors_col: str = "vectors"
    metadata_col: str = "metadata"
    ts_col: str = "timestamp"
    op_col: str = "op"
    delete_op_values: List[str] = field(default_factory=lambda: ["delete", "DELETE", "d"])
    column_types: Dict[str, pa.DataType] = field(default_factory=dict)
    storage_options: Optional[Dict[str, Any]] = None
    num_partitions: int = 512
    conflict_retries: int = 10
    guard_updates_by_ts: bool = False
    iceberg_read_options: Dict[str, str] = field(default_factory=dict)
    path_component_pattern: str = r"^[A-Za-z0-9._-]+$"

    def routing_cols(self) -> List[str]:
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
        ValueError: If any routing component is null or fails validation, which
            prevents path traversal and routing collisions.
    """
    pattern: "re.Pattern[str]" = re.compile(config.path_component_pattern)
    for component in (org_id, tenant_id, namespace):
        if component is None or not pattern.match(component):
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = config.base_uri.rstrip("/")
    return f"{base}/{org_id}/{tenant_id}/{namespace}.lance"


def cast_table(table: pa.Table, column_types: Dict[str, pa.DataType]) -> pa.Table:
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


def group_by_routing(
    table: pa.Table, routing_cols: List[str]
) -> Iterator[Tuple[Tuple[Any, ...], pa.Table]]:
    """Yield each routing key's rows from a partition table.

    Args:
        table: The materialized partition table.
        routing_cols: The routing key columns.

    Yields:
        ``(key_values, sub_table)`` for each distinct routing key.
    """
    combos: pa.Table = table.group_by(routing_cols).aggregate([])
    for index in range(combos.num_rows):
        key: Tuple[Any, ...] = tuple(combos[c][index].as_py() for c in routing_cols)
        mask: Optional[pa.Array] = None
        for position, column_name in enumerate(routing_cols):
            equals: pa.Array = pc.equal(table[column_name], key[position])
            mask = equals if mask is None else pc.and_(mask, equals)
        yield key, table.filter(mask)


def apply_merge(
    config: ETLConfig, telemetry: Telemetry, key: Tuple[str, str, str], group: pa.Table
) -> Tuple[int, int]:
    """Apply one dataset's terminal rows with merge upsert and physical delete.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: The ``(org_id, tenant_id, namespace)`` routing key.
        group: Rows for this routing key carrying the op column.

    Returns:
        The counts of upserted and deleted rows.
    """
    uri: str = dataset_uri(config, key[0], key[1], key[2])
    is_delete: pa.Array = pc.is_in(group[config.op_col], value_set=pa.array(config.delete_op_values))
    payload_cols: List[str] = [c for c in group.column_names if c != config.op_col]
    upserts: pa.Table = cast_table(
        group.filter(pc.invert(is_delete)).select(payload_cols), config.column_types
    )
    deletes: pa.Table = cast_table(
        group.filter(is_delete).select([config.key_col]), config.column_types
    )

    upserted: int = 0
    deleted: int = 0
    if upserts.num_rows:
        with telemetry.timed("dataset.merge_ms"):
            try:
                dataset: "lance.LanceDataset" = lance.dataset(
                    uri, storage_options=config.storage_options
                )
                builder = dataset.merge_insert(on=[config.key_col])
                if config.guard_updates_by_ts:
                    builder = builder.when_matched_update_all(
                        condition=f"source.{config.ts_col} > target.{config.ts_col}"
                    )
                else:
                    builder = builder.when_matched_update_all()
                builder.when_not_matched_insert_all().conflict_retries(
                    config.conflict_retries
                ).execute(upserts)
                telemetry.incr("dataset.merged")
            except (FileNotFoundError, ValueError):
                lance.write_dataset(
                    upserts, uri, mode="create", storage_options=config.storage_options
                )
                telemetry.incr("dataset.created")
        upserted = upserts.num_rows

    if deletes.num_rows:
        try:
            dataset = lance.dataset(uri, storage_options=config.storage_options)
            with telemetry.timed("dataset.delete_ms"):
                dataset.merge_insert(on=[config.key_col]).when_matched_delete().conflict_retries(
                    config.conflict_retries
                ).execute(deletes)
            deleted = deletes.num_rows
        except (FileNotFoundError, ValueError):
            deleted = 0

    telemetry.distribution("dataset.upserted", upserted)
    telemetry.distribution("dataset.deleted", deleted)
    return upserted, deleted


def build_stats_batch(rows: List[Tuple[str, str, str, int, int]]) -> pa.RecordBatch:
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

    def read_increment(
        self, spark: SparkSession, table: str, start_ms: int, end_ms: int
    ) -> DataFrame:
        """Read appended rows in a time range using Iceberg incremental options.

        Uses ``start-timestamp`` and ``end-timestamp`` in epoch milliseconds. The
        exact option names depend on the Iceberg version; override or extend via
        ``iceberg_read_options`` if the deployment differs.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.

        Returns:
            The incremental rows as a DataFrame.
        """
        reader = (
            spark.read.format("iceberg")
            .option("start-timestamp", str(start_ms))
            .option("end-timestamp", str(end_ms))
        )
        for option_key, option_value in self.config.iceberg_read_options.items():
            reader = reader.option(option_key, option_value)
        return reader.load(table)

    def validate_schema(self, source: DataFrame) -> None:
        """Validate that the source carries every required column.

        Args:
            source: The incremental source DataFrame.

        Raises:
            ValueError: If a required column is missing.
        """
        config: ETLConfig = self.config
        required: List[str] = [
            config.key_col,
            config.ts_col,
            config.op_col,
            config.vectors_col,
            config.metadata_col,
            *config.routing_cols(),
        ]
        missing: List[str] = [c for c in required if c not in source.columns]
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
        partition_by: List[Column] = [F.col(c) for c in config.routing_cols()]
        partition_by.append(F.col(config.key_col))
        window: WindowSpec = Window.partitionBy(*partition_by).orderBy(F.col(config.ts_col).desc())
        return (
            source.withColumn("row_num", F.row_number().over(window))
            .where(F.col("row_num") == 1)
            .drop("row_num")
        )

    def run(self, spark: SparkSession, table: str, start_ms: int, end_ms: int) -> None:
        """Read, transform, and route one time range of Iceberg changes.

        Args:
            spark: Active Spark session.
            table: Fully qualified Iceberg table name.
            start_ms: Range start in epoch milliseconds.
            end_ms: Range end in epoch milliseconds.
        """
        self.run_on_dataframe(self.read_increment(spark, table, start_ms, end_ms))

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
            routed: DataFrame = collapsed.repartition(
                config.num_partitions, *[F.col(c) for c in config.routing_cols()]
            )
            etl_config: ETLConfig = config

            def merge_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
                """Merge one Spark partition's datasets on an executor.

                Args:
                    batches: Arrow batches for this task.

                Yields:
                    One stats record batch when the partition wrote any dataset.
                """
                collected: List[pa.RecordBatch] = [b for b in batches if b.num_rows]
                if not collected:
                    return
                telemetry: Telemetry = Telemetry.create(etl_config.telemetry)
                table: pa.Table = pa.Table.from_batches(collected)
                results: List[Tuple[str, str, str, int, int]] = []
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
