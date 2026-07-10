"""Maintenance job package: per-dataset TTL expiration, unified compaction, version cleanup, and fleet tools.

Re-exports the consumer API from the sub-modules so callers can write
``from lance_etl.maintenance import MaintenanceJob, MaintenanceConfig, plan_one_dataset``.
"""

from __future__ import annotations

from lance_etl.fanout import fan_out_per_dataset
from lance_etl.maintenance.cluster import plan_cluster_rewrite
from lance_etl.maintenance.job import (
    MaintenanceConfig,
    MaintenanceJob,
    build_ttl_predicate,
    cleanup_dataset,
    commit_one_dataset,
    compaction_metrics_dict,
    compute_cutoff,
    plan_one_dataset,
    run_ttl_on_open_dataset,
    validate_column_name,
)
from lance_etl.maintenance.tools import migrate_dataset_manifest_paths, update_serving_tag

__all__ = [
    "MaintenanceConfig",
    "MaintenanceJob",
    "build_ttl_predicate",
    "cleanup_dataset",
    "commit_one_dataset",
    "compaction_metrics_dict",
    "compute_cutoff",
    "fan_out_per_dataset",
    "migrate_dataset_manifest_paths",
    "plan_cluster_rewrite",
    "plan_one_dataset",
    "run_ttl_on_open_dataset",
    "update_serving_tag",
    "validate_column_name",
]
