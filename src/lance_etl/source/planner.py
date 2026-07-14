"""Side-effect-free source planner abstractions for a future durable state repository."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from lance_etl.source.contract import validate_table_contract
from lance_etl.source.errors import SourceBaselineError, SourceLineageError
from lance_etl.source.lineage import index_snapshots, validate_snapshot_identity, walk_snapshot_lineage
from lance_etl.source.manifests import (
    classify_snapshot,
    discover_added_targets,
    discover_baseline_targets,
)
from lance_etl.source.models import (
    BaselineProof,
    MaintenanceTrust,
    ManifestEntry,
    SnapshotRecord,
    SourceCheckpoint,
    SourcePlan,
    SparkScanPlan,
    TableMetadata,
    TouchedTarget,
    WindowKind,
    WindowPlan,
)
from lance_etl.source.scans import build_spark_scan


class SourceCatalog(Protocol):
    """Bounded read-only Iceberg metadata access required by source planning."""

    def table_metadata(self, table: str) -> TableMetadata:
        """Return table metadata with one current snapshot pinned for this call.

        Args:
            table: Catalog-qualified table name.

        Returns:
            Pinned table metadata.
        """
        ...

    def snapshots_through(
        self, table: str, pinned_head_snapshot_id: int, stop_snapshot_id: int
    ) -> tuple[SnapshotRecord, ...]:
        """Return only ancestry needed to reach an inclusive stopping snapshot.

        Args:
            table: Catalog-qualified table name.
            pinned_head_snapshot_id: Head captured by ``table_metadata``.
            stop_snapshot_id: Oldest snapshot the result must include.

        Returns:
            Bounded snapshot metadata.
        """
        ...

    def manifest_entries(self, table: str, snapshot_id: int) -> tuple[ManifestEntry, ...]:
        """Return normalized manifest entries pinned to one exact snapshot.

        Args:
            table: Catalog-qualified table name.
            snapshot_id: Exact snapshot identifier.

        Returns:
            Immutable manifest entry records.
        """
        ...

    def maintenance_trust(self, table: str, snapshot_id: int) -> MaintenanceTrust | None:
        """Return authenticated maintenance evidence when the selected catalog supplies it.

        Args:
            table: Catalog-qualified table name.
            snapshot_id: Rewrite snapshot identifier.

        Returns:
            Trust evidence or ``None`` when it cannot be proven.
        """
        ...


@dataclass(frozen=True, slots=True)
class SourcePlanner:
    """Plan immutable source windows without writing control-plane or data state."""

    catalog: SourceCatalog

    def plan(
        self,
        table: str,
        checkpoint: SourceCheckpoint | None,
        baseline: BaselineProof | None = None,
    ) -> SourcePlan:
        """Plan all accepted windows from a recorded tip or validated pinned baseline.

        Args:
            table: Catalog-qualified Iceberg table name.
            checkpoint: Newest durable source window, when incremental planning has begun.
            baseline: Canonical proof required only for the first planner run.

        Returns:
            Deterministic plan pinned to the head observed at method entry.

        Raises:
            SourceBaselineError: If initial planning lacks a canonical pinned baseline.
            SourceContractError: If table identity or partition layout changed.
            SourceLineageError: If the pinned head does not descend from the recorded tip.
        """
        metadata = self.catalog.table_metadata(table)
        validate_table_contract(metadata, checkpoint)
        if metadata.current_snapshot_id is None:
            return SourcePlan(metadata.table_uuid, None, metadata.active_partition_spec.spec_id, ())
        if checkpoint is None:
            return self.plan_from_baseline(table, metadata, baseline)
        snapshots = self.catalog.snapshots_through(table, metadata.current_snapshot_id, checkpoint.snapshot_id)
        lineage = walk_snapshot_lineage(
            snapshots,
            metadata.current_snapshot_id,
            checkpoint.snapshot_id,
            metadata.table_uuid,
            metadata.active_partition_spec.spec_id,
        )
        if lineage and lineage[0].sequence_number <= checkpoint.sequence_number:
            raise SourceLineageError("first descendant sequence number does not follow the recorded checkpoint")
        windows = tuple(self.plan_snapshot(table, snapshot) for snapshot in lineage)
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            metadata.active_partition_spec.spec_id,
            windows,
        )

    def plan_from_baseline(
        self,
        table: str,
        metadata: TableMetadata,
        baseline: BaselineProof | None,
    ) -> SourcePlan:
        """Validate and include a canonical baseline followed by its pinned descendants.

        Args:
            table: Catalog-qualified Iceberg table name.
            metadata: Metadata pinned at planner start.
            baseline: Separately generated canonical-baseline proof.

        Returns:
            Baseline and descendant window plans.

        Raises:
            SourceBaselineError: If proof identity, spec, or canonicality is invalid.
        """
        validate_baseline_proof(metadata, baseline)
        if baseline is None or metadata.current_snapshot_id is None:
            raise SourceBaselineError("canonical baseline proof is required")
        snapshots = self.catalog.snapshots_through(table, metadata.current_snapshot_id, baseline.snapshot_id)
        indexed = index_snapshots(snapshots)
        baseline_snapshot = indexed.get(baseline.snapshot_id)
        if baseline_snapshot is None:
            raise SourceLineageError("pinned baseline is not retained in the requested ancestry")
        validate_snapshot_identity(
            baseline_snapshot,
            metadata.table_uuid,
            metadata.active_partition_spec.spec_id,
        )
        descendants = walk_snapshot_lineage(
            snapshots,
            metadata.current_snapshot_id,
            baseline.snapshot_id,
            metadata.table_uuid,
            metadata.active_partition_spec.spec_id,
        )
        entries = self.catalog.manifest_entries(table, baseline.snapshot_id)
        touched = discover_baseline_targets(baseline_snapshot, entries)
        baseline_window = build_window(table, baseline_snapshot, WindowKind.BASELINE, touched)
        descendant_windows = tuple(self.plan_snapshot(table, snapshot) for snapshot in descendants)
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            metadata.active_partition_spec.spec_id,
            (baseline_window, *descendant_windows),
        )

    def plan_snapshot(self, table: str, snapshot: SnapshotRecord) -> WindowPlan:
        """Classify and plan one next snapshot before considering its descendants.

        Args:
            table: Catalog-qualified Iceberg table name.
            snapshot: Next snapshot in direct ancestry order.

        Returns:
            Accepted immutable window plan.
        """
        entries = self.catalog.manifest_entries(table, snapshot.snapshot_id)
        trust = self.catalog.maintenance_trust(table, snapshot.snapshot_id)
        kind = classify_snapshot(snapshot, entries, trust)
        touched = discover_added_targets(snapshot, entries) if kind is WindowKind.APPEND else ()
        return build_window(table, snapshot, kind, touched)


def validate_baseline_proof(metadata: TableMetadata, baseline: BaselineProof | None) -> None:
    """Require a separately validated canonical baseline tied to exact table metadata.

    Args:
        metadata: Pinned table metadata.
        baseline: Candidate proof.

    Raises:
        SourceBaselineError: If any proof condition fails.
    """
    accepted = (
        baseline is not None
        and baseline.table_uuid == metadata.table_uuid
        and baseline.partition_spec_id == metadata.active_partition_spec.spec_id
        and baseline.canonical
        and baseline.distinct_mutation_conflicts == 0
    )
    if not accepted:
        raise SourceBaselineError(
            "initial source snapshot must have a matching canonical proof with no distinct per-key mutations"
        )


def build_window(
    table: str,
    snapshot: SnapshotRecord,
    kind: WindowKind,
    touched: tuple[TouchedTarget, ...],
) -> WindowPlan:
    """Build target scans and freeze one classified source window.

    Args:
        table: Catalog-qualified Iceberg table name.
        snapshot: Window snapshot.
        kind: Accepted window kind.
        touched: Manifest-derived target partitions.

    Returns:
        Frozen window plan.
    """
    scans: tuple[SparkScanPlan, ...]
    if kind is WindowKind.TRUSTED_MAINTENANCE:
        scans = ()
    else:
        scans = tuple(build_spark_scan(table, snapshot, kind, item.target) for item in touched)
    return WindowPlan(snapshot, kind, touched, scans)
