"""Spark scan plans pinned to exact Iceberg source windows."""

from __future__ import annotations

from typing import Any

from pyspark.sql.functions import col, lit

from lance_etl.source.errors import SourcePlanningError
from lance_etl.source.models import SnapshotRecord, TargetKey, WindowKind


def snapshot_scan_options(
    snapshot: SnapshotRecord,
    kind: WindowKind,
) -> tuple[tuple[str, str], ...]:
    """Return exact Iceberg read options for one accepted source window.

    Args:
        snapshot: Immutable source snapshot.
        kind: Accepted source window kind.

    Returns:
        Frozen snapshot options for a baseline or direct append window.

    Raises:
        SourcePlanningError: If an append lacks a direct parent or maintenance is requested as data.
    """
    if kind is WindowKind.BASELINE:
        return (("snapshot-id", str(snapshot.snapshot_id)),)
    if kind is WindowKind.APPEND:
        if snapshot.parent_snapshot_id is None:
            raise SourcePlanningError("append source window requires a direct parent snapshot")
        return (
            ("start-snapshot-id", str(snapshot.parent_snapshot_id)),
            ("end-snapshot-id", str(snapshot.snapshot_id)),
        )
    raise SourcePlanningError("trusted maintenance windows do not produce Spark data scans")


def execute_spark_scan(
    spark: Any,
    table: str,
    snapshot: SnapshotRecord,
    kind: WindowKind,
    target: TargetKey,
) -> Any:
    """Execute one exact Iceberg source-window scan with canonical routing filters.

    Args:
        spark: Spark session.
        table: Catalog-qualified Iceberg table name.
        snapshot: Immutable source snapshot.
        kind: Accepted source window kind.
        target: Canonical target filter applied after Iceberg manifest pruning.

    Returns:
        Filtered Spark DataFrame carrying the window's internal source sequence.
    """
    reader: Any = spark.read.format("iceberg")
    key: Any
    value: Any
    for key, value in snapshot_scan_options(snapshot, kind):
        reader = reader.option(key, value)
    frame: Any = reader.load(table)
    target_filter: Any = (
        (col("tenant_id") == lit(target.tenant_id))
        & (col("namespace") == lit(target.namespace))
        & (col("org_id") == lit(target.org_id))
    )
    return frame.where(target_filter).withColumn("lance_etl_source_sequence", lit(snapshot.sequence_number))
