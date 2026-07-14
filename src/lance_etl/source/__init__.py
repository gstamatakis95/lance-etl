"""Exact Iceberg snapshot source discovery and deterministic replay planning."""

from __future__ import annotations

from lance_etl.source.contract import (
    REQUIRED_PARTITION_FIELDS as REQUIRED_PARTITION_FIELDS,
)
from lance_etl.source.contract import (
    validate_partition_contract as validate_partition_contract,
)
from lance_etl.source.contract import (
    validate_table_contract as validate_table_contract,
)
from lance_etl.source.errors import (
    SourceBaselineError as SourceBaselineError,
)
from lance_etl.source.errors import (
    SourceContractError as SourceContractError,
)
from lance_etl.source.errors import (
    SourceLineageError as SourceLineageError,
)
from lance_etl.source.errors import (
    SourcePlanningError as SourcePlanningError,
)
from lance_etl.source.errors import (
    SourceSnapshotBlockedError as SourceSnapshotBlockedError,
)
from lance_etl.source.lineage import walk_snapshot_lineage as walk_snapshot_lineage
from lance_etl.source.manifests import (
    classify_snapshot as classify_snapshot,
)
from lance_etl.source.manifests import (
    discover_added_targets as discover_added_targets,
)
from lance_etl.source.manifests import (
    discover_baseline_targets as discover_baseline_targets,
)
from lance_etl.source.models import (
    BaselineProof as BaselineProof,
)
from lance_etl.source.models import (
    MaintenanceTrust as MaintenanceTrust,
)
from lance_etl.source.models import (
    ManifestContent as ManifestContent,
)
from lance_etl.source.models import (
    ManifestEntry as ManifestEntry,
)
from lance_etl.source.models import (
    ManifestStatus as ManifestStatus,
)
from lance_etl.source.models import (
    PartitionField as PartitionField,
)
from lance_etl.source.models import (
    PartitionSpec as PartitionSpec,
)
from lance_etl.source.models import (
    PartitionValues as PartitionValues,
)
from lance_etl.source.models import (
    SnapshotRecord as SnapshotRecord,
)
from lance_etl.source.models import (
    SourceCheckpoint as SourceCheckpoint,
)
from lance_etl.source.models import (
    SourcePlan as SourcePlan,
)
from lance_etl.source.models import (
    SparkScanPlan as SparkScanPlan,
)
from lance_etl.source.models import (
    TableMetadata as TableMetadata,
)
from lance_etl.source.models import (
    TargetKey as TargetKey,
)
from lance_etl.source.models import (
    TouchedTarget as TouchedTarget,
)
from lance_etl.source.models import (
    WindowKind as WindowKind,
)
from lance_etl.source.models import (
    WindowPlan as WindowPlan,
)
from lance_etl.source.planner import (
    SourceCatalog as SourceCatalog,
)
from lance_etl.source.planner import (
    SourcePlanner as SourcePlanner,
)
from lance_etl.source.scans import build_spark_scan as build_spark_scan
from lance_etl.source.scans import execute_spark_scan as execute_spark_scan
