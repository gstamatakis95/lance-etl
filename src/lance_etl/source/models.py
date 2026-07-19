"""Typed immutable records used by Iceberg source discovery and replay planning."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class WindowKind(StrEnum):
    """Kinds of source window accepted by the durable control plane."""

    BASELINE = "BASELINE"
    APPEND = "APPEND"
    TRUSTED_MAINTENANCE = "TRUSTED_MAINTENANCE"


class ManifestStatus(StrEnum):
    """Iceberg manifest entry status values relevant to source planning."""

    ADDED = "ADDED"
    EXISTING = "EXISTING"
    DELETED = "DELETED"


class ManifestContent(StrEnum):
    """Iceberg manifest content types relevant to physical-change validation."""

    DATA = "DATA"
    POSITION_DELETES = "POSITION_DELETES"
    EQUALITY_DELETES = "EQUALITY_DELETES"


@dataclass(frozen=True, slots=True)
class PartitionField:
    """One named field in an Iceberg partition specification."""

    field_name: str
    source_column: str
    transform: str


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    """An active Iceberg partition specification."""

    spec_id: int
    fields: tuple[PartitionField, ...]


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Pinned table identity and active metadata needed by a planner run."""

    table_uuid: str
    current_snapshot_id: int | None
    active_partition_spec: PartitionSpec


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    """Snapshot metadata sufficient for lineage and operation classification."""

    table_uuid: str
    snapshot_id: int
    parent_snapshot_id: int | None
    sequence_number: int
    committed_at_ms: int
    operation: str
    partition_spec_id: int
    summary: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class TargetKey:
    """Logical Lance target encoded by the first three Iceberg partition fields."""

    tenant_id: str
    namespace: str
    org_id: str


@dataclass(frozen=True, slots=True)
class PartitionValues:
    """Typed values from the required Iceberg partition layout."""

    tenant_id: str
    namespace: str
    org_id: str
    processing_timestamp_hour: int

    def target_key(self) -> TargetKey:
        """Return the target identity without the pruning-only hour.

        Returns:
            Logical target key.
        """
        return TargetKey(self.tenant_id, self.namespace, self.org_id)


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """A normalized Iceberg manifest entry for one data or delete file."""

    snapshot_id: int
    partition_spec_id: int
    status: ManifestStatus
    content: ManifestContent
    partition: PartitionValues


@dataclass(frozen=True, slots=True)
class TouchedTarget:
    """A target touched by one snapshot and its manifest-derived pruning hours."""

    target: TargetKey
    hours: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MaintenanceTrust:
    """Catalog-authenticated evidence for a logical maintenance rewrite."""

    writer_identity: str | None
    writer_authenticated: bool
    writer_allowlisted: bool
    logical_change: bool | None
    manifest_invariants_match: bool


@dataclass(frozen=True, slots=True)
class BaselineProof:
    """Evidence that a pinned snapshot is a canonical initial source baseline."""

    table_uuid: str
    snapshot_id: int
    partition_spec_id: int
    canonical: bool
    distinct_mutation_conflicts: int


@dataclass(frozen=True, slots=True)
class SourceCheckpoint:
    """The newest source window already recorded by the durable state repository."""

    table_uuid: str
    snapshot_id: int
    sequence_number: int
    partition_spec_id: int


@dataclass(frozen=True, slots=True)
class SparkScanPlan:
    """A deterministic Spark read pinned to one source window and target."""

    table: str
    options: tuple[tuple[str, str], ...]
    target: TargetKey
    source_sequence: int
    route_columns: tuple[str, str, str] = ("tenant_id", "namespace", "org_id")


@dataclass(frozen=True, slots=True)
class WindowPlan:
    """One source window and all target work derived from its immutable manifests."""

    snapshot: SnapshotRecord
    kind: WindowKind
    touched_targets: tuple[TouchedTarget, ...]
    scans: tuple[SparkScanPlan, ...]


@dataclass(frozen=True, slots=True)
class SourcePlan:
    """Deterministic planner output for one pinned Iceberg head."""

    table_uuid: str
    pinned_head_snapshot_id: int | None
    partition_spec_id: int
    windows: tuple[WindowPlan, ...]

    def retention_snapshot_id(self) -> int | None:
        """Return the oldest snapshot needed to execute this plan.

        Returns:
            Parent of the first append window, its baseline snapshot, or ``None`` for an empty plan.
        """
        if not self.windows:
            return None
        first: Any = self.windows[0]
        if first.kind is WindowKind.BASELINE:
            return first.snapshot.snapshot_id
        return first.snapshot.parent_snapshot_id
