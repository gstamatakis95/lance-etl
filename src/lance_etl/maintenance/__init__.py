"""Maintenance job package: per-dataset TTL expiration, unified compaction, version cleanup, and fleet tools.

Re-exports the public API from the sub-modules so callers can write
``from lance_etl.maintenance import MaintenanceJob, MaintenanceConfig, plan_one_dataset``.
"""

from __future__ import annotations

from lance_etl.fanout import fan_out_per_dataset
from lance_etl.maintenance.job import (
    MaintenanceConfig,
    MaintenanceJob,
    build_ttl_predicate,
    cleanup_dataset,
    commit_one_dataset,
    compaction_metrics_dict,
    compaction_skip_reason,
    compute_cutoff,
    execute_rewrite_task,
    plan_one_dataset,
    run_ttl_on_open_dataset,
    validate_column_name,
)
from lance_etl.maintenance.tools import (
    migrate_dataset_manifest_paths,
    migrate_manifest_paths,
    prune_interval_tags,
    prune_interval_tags_fleet,
    update_serving_tag,
    update_serving_tags,
)

__all__ = [
    "MaintenanceConfig",
    "MaintenanceJob",
    "build_ttl_predicate",
    "cleanup_dataset",
    "commit_one_dataset",
    "compaction_metrics_dict",
    "compaction_skip_reason",
    "compute_cutoff",
    "execute_rewrite_task",
    "fan_out_per_dataset",
    "migrate_dataset_manifest_paths",
    "migrate_manifest_paths",
    "plan_one_dataset",
    "prune_interval_tags",
    "prune_interval_tags_fleet",
    "run_ttl_on_open_dataset",
    "update_serving_tag",
    "update_serving_tags",
    "validate_column_name",
]
