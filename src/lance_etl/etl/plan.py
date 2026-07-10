"""Adaptive routing plan and salted shuffle for the Iceberg-to-Lance ETL.

The plan sizes the routing shuffle by both bytes (total rows) and cardinality (distinct dataset
trios), so a fleet of tens of thousands of tiny datasets is not serialized into one task and a
single billion-row org is not crammed into one shuffle partition. Big datasets are salted across
``K`` key-hash sub-buckets so their rows fan out over multiple merge writers instead of one.

The plan is computed on the driver with memory bounded by the number of big trios: only the
per-trio counts and the trios that exceed the bucket threshold are collected, never the rows
themselves. The salt is applied through a tiny broadcast join carrying one integer bucket count
per big trio, whose helper column is dropped before the merge so it is never written into any
dataset.

Imports only from :mod:`lance_etl.etl.pivot` (``KEY_COL``, ``ROUTING_COLS``, and ``ETLConfig``), so
:mod:`lance_etl.etl.job` can import this module without a cycle.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType
from pyspark.sql.window import Window, WindowSpec

from lance_etl.etl.pivot import KEY_COL, ROUTING_COLS, ETLConfig

MAX_SHUFFLE_PARTITIONS: int = 32_768
"""Upper clamp on the planned routing-shuffle width, never varied in practice."""

SALT_BUCKETS_COL: str = "lance_etl_bucket_count"
"""Transient helper column broadcast-joined onto the collapsed frame and dropped before the merge.

Carries the per-big-trio sub-bucket count ``K`` so the salt expression can fan a big dataset's
rows across ``K`` key-hash buckets. Never written into any dataset: the salted shuffle re-selects
the input schema after the join.
"""


def routing_null_predicate() -> Column:
    """Build the predicate matching rows with a NULL in any routing column.

    Shared by the null-routing filter (:meth:`lance_etl.etl.job.IcebergToLanceETL.drop_null_routing_rows`)
    and the plan's null-routing count, so the two can never disagree on what "null routing" means.
    ``isNull()`` is always true or false, so the predicate and its negation are exhaustive and
    complementary.

    Returns:
        The OR of ``isNull()`` over every column in :data:`~lance_etl.etl.pivot.ROUTING_COLS`.
    """
    predicate: Column | None = None
    for column_name in ROUTING_COLS:
        is_null: Column = F.col(column_name).isNull()
        predicate = is_null if predicate is None else (predicate | is_null)
    return predicate


def collapse(source: DataFrame, config: ETLConfig) -> DataFrame:
    """Reduce to the last-write-wins terminal event per (routing key, vector id).

    Orders by ``config.ts_col`` descending NULLS LAST; ties broken by ``xxhash64`` over all
    non-MapType columns ascending (MapType columns cannot be hashed by Spark). Shared by the
    merge path (:meth:`lance_etl.etl.job.IcebergToLanceETL.collapse`) and the bulk-append path
    (:func:`lance_etl.etl.bulk.run_bulk_append`) so the two can never disagree on which row
    survives per key, which is the invariant that keeps bulk output identical to merge output.

    Args:
        source: The source DataFrame with map columns still intact.
        config: ETL configuration providing the key and timestamp columns.

    Returns:
        One row per routing key and vector id, carrying the terminal op.
    """
    partition_by: list[Column] = [F.col(c) for c in ROUTING_COLS]
    partition_by.append(F.col(KEY_COL))
    non_map_cols: list[str] = [f.name for f in source.schema.fields if not isinstance(f.dataType, MapType)]
    window: WindowSpec = Window.partitionBy(*partition_by).orderBy(
        F.col(config.ts_col).desc_nulls_last(),
        F.xxhash64(*[F.col(c) for c in non_map_cols]).asc(),
    )
    return source.withColumn("row_num", F.row_number().over(window)).where(F.col("row_num") == 1).drop("row_num")


def bucket_count(rows: int, bucket_rows: int, max_buckets: int) -> int:
    """Compute the number of key-hash sub-buckets for a dataset of ``rows`` rows.

    Args:
        rows: Row count of the dataset in this increment.
        bucket_rows: Target rows per sub-bucket. Values below one are treated as one.
        max_buckets: Upper cap on sub-buckets per dataset.

    Returns:
        ``min(max_buckets, max(1, ceil(rows / bucket_rows)))``.
    """
    safe_bucket_rows: int = max(1, bucket_rows)
    return min(max_buckets, max(1, math.ceil(rows / safe_bucket_rows)))


def shuffle_partition_count(total_rows: int, trio_count: int, config: ETLConfig) -> int:
    """Size the routing shuffle by both total rows and distinct-trio count.

    Takes the larger of a row floor (so no partition holds far more than ``bucket_rows`` rows) and
    a dataset floor (so no task serializes far more than ``datasets_per_task`` datasets), clamped
    to :data:`MAX_SHUFFLE_PARTITIONS`. A manual ``config.num_partitions`` overrides the whole
    computation.

    Args:
        total_rows: Total collapsed rows in the increment.
        trio_count: Number of distinct routing trios in the increment.
        config: ETL configuration carrying the sizing tunables.

    Returns:
        The planned shuffle-partition width, at least one.
    """
    if config.num_partitions is not None:
        return config.num_partitions
    safe_bucket_rows: int = max(1, config.bucket_rows)
    safe_datasets_per_task: int = max(1, config.datasets_per_task)
    row_floor: int = math.ceil(total_rows / safe_bucket_rows)
    dataset_floor: int = math.ceil(trio_count / safe_datasets_per_task)
    return max(1, min(MAX_SHUFFLE_PARTITIONS, max(row_floor, dataset_floor)))


@dataclass(frozen=True)
class RoutingPlan:
    """An adaptive routing plan for one increment.

    Attributes:
        total_rows: Total pre-collapse rows across all non-null-routing trios.
        trio_count: Number of distinct routing trios.
        big_trios: One ``(org_id, tenant_id, namespace, K)`` per trio whose row count exceeds
            ``bucket_rows``, where ``K > 1`` is the sub-bucket count. Trios with ``K == 1`` are
            excluded (no salting needed).
        num_partitions: The planned routing-shuffle width.
        null_routing_rows: Rows dropped because a routing column was NULL, counted in the same
            single ``groupBy`` scan that sizes the plan (carried here to avoid a second scan or a
            fragile ``Observation``).
    """

    total_rows: int
    trio_count: int
    big_trios: list[tuple[str, str, str, int]]
    num_partitions: int
    null_routing_rows: int

    @property
    def total_buckets(self) -> int:
        """Return the total number of merge-writer sub-buckets across the whole increment.

        Each big trio contributes its ``K`` sub-buckets; every other trio contributes one.

        Returns:
            ``sum(K for big trios) + (trio_count - number of big trios)``.
        """
        return sum(k for *_, k in self.big_trios) + (self.trio_count - len(self.big_trios))


def compute_routing_plan(source: DataFrame, config: ETLConfig) -> RoutingPlan:
    """Compute an adaptive routing plan in one ``groupBy`` scan of the source.

    Aggregates the per-trio counts once over the raw source (persisted so the null count, the
    trio aggregate, and the big-trio scan all reuse a single scan), splitting null-routing groups
    from real trios with the shared :func:`routing_null_predicate`. Only the trios whose count
    exceeds ``config.bucket_rows`` are collected to the driver, so driver memory stays ``O(big
    trios)``. The full row set is never collected. Folding the null-routing count into this pass
    avoids both a second scan and a fragile ``Observation`` whose metric row fails to materialize
    under a ``groupBy``-derived action.

    Args:
        source: The raw increment, before the null-routing filter and before collapse.
        config: ETL configuration carrying the sizing tunables.

    Returns:
        A :class:`RoutingPlan` sized for this increment, carrying the null-routing drop count.
    """
    any_null: Column = routing_null_predicate()
    counts: DataFrame = source.groupBy(*ROUTING_COLS).count()
    counts = counts.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        null_routing_rows: int = int(
            counts.where(any_null).agg(F.sum("count").alias("null_routing_rows")).collect()[0]["null_routing_rows"] or 0
        )
        valid: DataFrame = counts.where(~any_null)
        aggregates: Any = valid.agg(
            F.count(F.lit(1)).alias("trio_count"),
            F.sum("count").alias("total_rows"),
        ).collect()[0]
        trio_count: int = int(aggregates["trio_count"] or 0)
        total_rows: int = int(aggregates["total_rows"] or 0)
        big_rows: list[Any] = valid.where(F.col("count") > config.bucket_rows).collect()
        big_trios: list[tuple[str, str, str, int]] = [
            (
                row["org_id"],
                row["tenant_id"],
                row["namespace"],
                bucket_count(int(row["count"]), config.bucket_rows, config.max_buckets_per_dataset),
            )
            for row in big_rows
        ]
        big_trios = [entry for entry in big_trios if entry[3] > 1]
    finally:
        counts.unpersist()
    return RoutingPlan(
        total_rows=total_rows,
        trio_count=trio_count,
        big_trios=big_trios,
        num_partitions=shuffle_partition_count(total_rows, trio_count, config),
        null_routing_rows=null_routing_rows,
    )


def apply_salted_shuffle(collapsed: DataFrame, plan: RoutingPlan) -> DataFrame:
    """Repartition a collapsed frame by routing key, salting big trios across key-hash sub-buckets.

    With no big trios, this is a plain routing-key repartition to ``plan.num_partitions`` followed
    by a within-partition routing sort. With big trios, a tiny broadcast frame carrying one
    ``(org_id, tenant_id, namespace, K)`` per big trio is left-joined on, and the repartition key
    gains a salt ``pmod(xxhash64(key_col), coalesce(K, 1))``.

    Invariants:
        - The salt is a pure function of :data:`~lance_etl.etl.pivot.KEY_COL`, so every row of one
          key gets the same salt and lands in exactly one partition. Concurrent merge writers are
          therefore key-disjoint by construction. This is mandatory because the dataset declares no
          enforced primary key, so Lance has no insert-side conflict detection and two writers
          inserting the same key would silently duplicate it.
        - Two sub-buckets of one trio colliding into one partition is harmless: it only lengthens a
          contiguous routing run.
        - Collapse runs before this join, so each key is already exactly one row.
        - The helper column is dropped by re-selecting the input schema after the join, so it is
          never written into any dataset and the output schema equals the input schema exactly.

    Args:
        collapsed: The collapsed, null-routing-filtered frame.
        plan: The routing plan for this increment.

    Returns:
        The salted, routing-partitioned, partition-sorted DataFrame with the input schema.
    """
    routing: list[Any] = [F.col(c) for c in ROUTING_COLS]
    if not plan.big_trios:
        return collapsed.repartition(plan.num_partitions, *routing).sortWithinPartitions(*ROUTING_COLS)
    input_columns: list[str] = collapsed.columns
    spark = collapsed.sparkSession
    rows: list[tuple[str, str, str, int]] = [(o, t, n, k) for (o, t, n, k) in plan.big_trios]
    bucket_df: DataFrame = spark.createDataFrame(
        rows,
        schema=f"org_id string, tenant_id string, namespace string, {SALT_BUCKETS_COL} int",
    )
    joined: DataFrame = collapsed.join(F.broadcast(bucket_df), on=list(ROUTING_COLS), how="left")
    salt: Any = F.pmod(F.xxhash64(F.col(KEY_COL)), F.coalesce(F.col(SALT_BUCKETS_COL), F.lit(1)))
    shuffled: DataFrame = joined.repartition(plan.num_partitions, *routing, salt)
    return shuffled.select(*input_columns).sortWithinPartitions(*ROUTING_COLS)
