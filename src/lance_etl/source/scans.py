"""Spark scan plans pinned to exact Iceberg source windows."""

from __future__ import annotations

from typing import Any

from pyspark.sql.functions import col, lit

from lance_etl.source.errors import SourcePlanningError
from lance_etl.source.models import SnapshotRecord, SparkScanPlan, TargetKey, WindowKind


def build_spark_scan(
    table: str,
    snapshot: SnapshotRecord,
    kind: WindowKind,
    target: TargetKey,
    route_columns: tuple[str, str, str] = ("tenant_id", "namespace", "org_id"),
) -> SparkScanPlan:
    """Build a deterministic target scan for one exact baseline or parent-to-snapshot increment.

    Args:
        table: Catalog-qualified Iceberg table name.
        snapshot: Immutable source snapshot.
        kind: Accepted source window kind.
        target: Target filter applied after Iceberg manifest pruning.
        route_columns: Physical tenant, namespace, and organization column names.

    Returns:
        Frozen Spark read options and typed target filter.

    Raises:
        SourcePlanningError: If an append lacks a direct parent or maintenance is requested as data.
    """
    if kind is WindowKind.BASELINE:
        options: tuple[tuple[str, str], ...] = (("snapshot-id", str(snapshot.snapshot_id)),)
    elif kind is WindowKind.APPEND:
        if snapshot.parent_snapshot_id is None:
            raise SourcePlanningError("append source window requires a direct parent snapshot")
        options = (
            ("start-snapshot-id", str(snapshot.parent_snapshot_id)),
            ("end-snapshot-id", str(snapshot.snapshot_id)),
        )
    else:
        raise SourcePlanningError("trusted maintenance windows do not produce Spark data scans")
    return SparkScanPlan(table, options, target, snapshot.sequence_number, route_columns)


def execute_spark_scan(spark: Any, plan: SparkScanPlan) -> Any:
    """Execute a prebuilt exact Iceberg scan with typed equality filters.

    Args:
        spark: Spark session.
        plan: Frozen scan plan.

    Returns:
        Filtered Spark DataFrame carrying the window's internal source sequence.
    """
    reader: Any = spark.read.format("iceberg")
    key: Any
    value: Any
    for key, value in plan.options:
        reader = reader.option(key, value)
    frame: Any = reader.load(plan.table)
    tenant_column: str
    namespace_column: str
    org_column: str
    tenant_column, namespace_column, org_column = plan.route_columns
    target_filter: Any = (
        (col(tenant_column) == lit(plan.target.tenant_id))
        & (col(namespace_column) == lit(plan.target.namespace))
        & (col(org_column) == lit(plan.target.org_id))
    )
    return frame.where(target_filter).withColumn("lance_etl_source_sequence", lit(plan.source_sequence))
