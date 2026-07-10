"""ETL job package: Iceberg-to-Lance incremental routing.

Re-exports the consumer API from the sub-modules so callers can write
``from lance_etl.etl import IcebergToLanceETL, ETLConfig, ROUTING_COLS``.
"""

from __future__ import annotations

from lance_etl.etl.bulk import derive_bulk_schemas, plan_bulk_append
from lance_etl.etl.job import IcebergToLanceETL, snapshot_id_bounds
from lance_etl.etl.pivot import ROUTING_COLS, ETLConfig, apply_ttl_cast, pivot_map_columns
from lance_etl.etl.sink import apply_merge, dataset_uri

__all__ = [
    "ROUTING_COLS",
    "ETLConfig",
    "IcebergToLanceETL",
    "apply_merge",
    "apply_ttl_cast",
    "dataset_uri",
    "derive_bulk_schemas",
    "pivot_map_columns",
    "plan_bulk_append",
    "snapshot_id_bounds",
]
