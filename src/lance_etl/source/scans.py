"""Spark scan plans pinned to exact Iceberg source windows."""

from __future__ import annotations

from typing import Any

from pyspark.sql.functions import col, lit

from lance_etl.source.errors import SourcePlanningError
from lance_etl.source.models import SnapshotRecord, SparkScanPlan, TargetKey, WindowKind


def build_spark_scan(table: str, snapshot: SnapshotRecord, kind: WindowKind, target: TargetKey) -> SparkScanPlan:
    """Build a deterministic target scan for one exact baseline or parent-to-snapshot increment.

    Args:
        table: Catalog-qualified Iceberg table name.
        snapshot: Immutable source snapshot.
        kind: Accepted source window kind.
        target: Target filter applied after Iceberg manifest pruning.

    Returns:
        Frozen Spark read options and typed target filter.

    Raises:
        SourcePlanningError: If an append lacks a direct parent or maintenance is requested as data.
    """
    if kind is WindowKind.BASELINE:
        options = (("snapshot-id", str(snapshot.snapshot_id)),)
    elif kind is WindowKind.APPEND:
        if snapshot.parent_snapshot_id is None:
            raise SourcePlanningError("append source window requires a direct parent snapshot")
        options = (
            ("start-snapshot-id", str(snapshot.parent_snapshot_id)),
            ("end-snapshot-id", str(snapshot.snapshot_id)),
        )
    else:
        raise SourcePlanningError("trusted maintenance windows do not produce Spark data scans")
    return SparkScanPlan(table, options, target, snapshot.sequence_number)


def execute_spark_scan(spark: Any, plan: SparkScanPlan) -> Any:
    """Execute a prebuilt exact Iceberg scan with typed equality filters.

    Args:
        spark: Spark session.
        plan: Frozen scan plan.

    Returns:
        Filtered Spark DataFrame carrying the window's internal source sequence.
    """
    reader = spark.read.format("iceberg")
    for key, value in plan.options:
        reader = reader.option(key, value)
    frame = reader.load(plan.table)
    target_filter = (
        (col("tenant_id") == lit(plan.target.tenant_id))
        & (col("namespace") == lit(plan.target.namespace))
        & (col("org_id") == lit(plan.target.org_id))
    )
    return frame.where(target_filter).withColumn("lance_etl_source_sequence", lit(plan.source_sequence))
