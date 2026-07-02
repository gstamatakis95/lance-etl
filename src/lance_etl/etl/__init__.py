"""ETL job package: Iceberg-to-Lance incremental routing.

Re-exports the public API from the sub-modules so callers can write
``from lance_etl.etl import IcebergToLanceETL, ETLConfig, ROUTING_COLS``.
"""

from __future__ import annotations

from lance_etl.etl.job import (
    IcebergToLanceETL,
    snapshot_id_bounds,
)
from lance_etl.etl.pivot import (
    ROUTING_COLS,
    ETLConfig,
    apply_fsl_cast,
    apply_ttl_cast,
    build_stats_batch,
    group_by_routing,
    pivot_map_columns,
    stats_schema,
    stats_spark_ddl,
)
from lance_etl.etl.sink import (
    apply_merge,
    build_update_condition,
    dataset_uri,
    table_chunks,
)

__all__ = [
    "ROUTING_COLS",
    "ETLConfig",
    "IcebergToLanceETL",
    "apply_merge",
    "build_update_condition",
    "dataset_uri",
    "snapshot_id_bounds",
    "table_chunks",
    "apply_fsl_cast",
    "apply_ttl_cast",
    "build_stats_batch",
    "group_by_routing",
    "pivot_map_columns",
    "stats_schema",
    "stats_spark_ddl",
]
