"""Mapping from side-effect-free Iceberg source plans into durable PostgreSQL work."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from lance_etl.reconciler.config import ReconcilerSettings
from lance_etl.source import SourcePlan, SourceSnapshotRejection, TargetKey, WindowKind, WindowPlan
from lance_etl.state import DatasetPlan, IcebergSource, RoutingIdentity, SourceSnapshotKind, SourceSnapshotPlan


class SourceSnapshotRepository(Protocol):
    """Atomic state operation required by the source-plan adapter."""

    def enqueue_source_snapshot(self, plan: SourceSnapshotPlan, dataset_plans: list[DatasetPlan]) -> int:
        """Atomically enqueue a source snapshot and its immutable dataset set.

        Args:
            plan: Durable source-snapshot metadata.
            dataset_plans: Logical datasets touched by the snapshot.

        Returns:
            Existing or newly created window sequence.
        """
        ...

    def enqueue_blocked_source_snapshot(self, plan: SourceSnapshotPlan, error_code: str) -> int:
        """Persist one exact rejected snapshot after its accepted parent prefix.

        Args:
            plan: Rejected immutable source snapshot.
            error_code: Bounded rejection classification.

        Returns:
            Existing or newly created source snapshot sequence.
        """
        ...


@dataclass(frozen=True, slots=True)
class EnqueueSummary:
    """Bounded durable result of mapping one pinned source plan."""

    pinned_head_snapshot_id: int | None
    planned_snapshots: int
    enqueued_snapshots: int
    source_snapshot_sequences: tuple[int, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class SourcePlanEnqueuer:
    """Atomically enqueue accepted source snapshots without inventing routing state."""

    repository: SourceSnapshotRepository
    settings: ReconcilerSettings
    source: IcebergSource

    def enqueue(self, source_plan: SourcePlan) -> EnqueueSummary:
        """Map a bounded prefix of one pinned source plan into idempotent repository calls.

        Args:
            source_plan: Pinned plan emitted by the durable Iceberg provider.

        Returns:
            Durable window identities and whether another planner pass is needed.

        Raises:
            ValueError: If the Iceberg table UUID is not canonical or a target is duplicated.
        """
        table_uuid: uuid.UUID = uuid.UUID(source_plan.table_uuid)
        if table_uuid != self.source.table_uuid:
            raise ValueError("source plan table UUID differs from the registered Iceberg source")
        selected: tuple[WindowPlan, ...] = source_plan.windows[: self.settings.max_snapshots_per_plan]
        sequences: list[int] = []
        window: WindowPlan
        for window in selected:
            state_plan: SourceSnapshotPlan = map_snapshot(
                self.source.source_id,
                window,
                source_plan.planning_epoch,
            )
            datasets: list[DatasetPlan] = map_datasets(window)
            sequences.append(self.repository.enqueue_source_snapshot(state_plan, datasets))
        if source_plan.rejection is not None and len(selected) == len(source_plan.windows):
            rejection_plan: SourceSnapshotPlan = map_rejection(
                self.source.source_id,
                source_plan.rejection,
                source_plan.planning_epoch,
            )
            self.repository.enqueue_blocked_source_snapshot(rejection_plan, source_plan.rejection.error_code)
        return EnqueueSummary(
            pinned_head_snapshot_id=source_plan.pinned_head_snapshot_id,
            planned_snapshots=len(source_plan.windows),
            enqueued_snapshots=len(selected),
            source_snapshot_sequences=tuple(sequences),
            truncated=len(selected) < len(source_plan.windows),
        )


def map_rejection(
    source_id: uuid.UUID,
    rejection: SourceSnapshotRejection,
    source_planning_epoch: int | None = None,
) -> SourceSnapshotPlan:
    """Convert one deferred rejection into durable source evidence.

    Args:
        source_id: PostgreSQL-owned source identity.
        rejection: Exact unsupported snapshot and bounded code.
        source_planning_epoch: Optional durable planner fence.

    Returns:
        Rejected source snapshot plan.
    """
    snapshot = rejection.snapshot
    return SourceSnapshotPlan(
        source_id=source_id,
        snapshot_id=snapshot.snapshot_id,
        parent_snapshot_id=snapshot.parent_snapshot_id,
        iceberg_sequence_number=snapshot.sequence_number,
        partition_spec_id=snapshot.partition_spec_id,
        committed_at=datetime.fromtimestamp(snapshot.committed_at_ms / 1000, UTC),
        iceberg_operation=snapshot.operation,
        kind=SourceSnapshotKind.REJECTED,
        source_planning_epoch=source_planning_epoch,
    )


def map_snapshot(
    source_id: uuid.UUID,
    window: WindowPlan,
    source_planning_epoch: int | None = None,
) -> SourceSnapshotPlan:
    """Convert one source window into the durable source-snapshot representation.

    Args:
        source_id: PostgreSQL-owned Iceberg source identity.
        window: Accepted source window.
        source_planning_epoch: Optional durable planner fence observed before reading Iceberg.

    Returns:
        Durable source-snapshot plan.
    """
    snapshot_kind: SourceSnapshotKind = SourceSnapshotKind(window.kind.value)
    committed_at: datetime = datetime.fromtimestamp(window.snapshot.committed_at_ms / 1000, UTC)
    return SourceSnapshotPlan(
        source_id=source_id,
        snapshot_id=window.snapshot.snapshot_id,
        parent_snapshot_id=window.snapshot.parent_snapshot_id,
        iceberg_sequence_number=window.snapshot.sequence_number,
        partition_spec_id=window.snapshot.partition_spec_id,
        committed_at=committed_at,
        iceberg_operation=window.snapshot.operation,
        kind=snapshot_kind,
        source_planning_epoch=source_planning_epoch,
    )


def map_datasets(window: WindowPlan) -> list[DatasetPlan]:
    """Convert manifest-derived routing identities into logical dataset plans.

    Args:
        window: Accepted source window.

    Returns:
        Deterministically ordered durable dataset plans.

    Raises:
        ValueError: If one logical target appears more than once.
    """
    if window.kind is WindowKind.TRUSTED_MAINTENANCE:
        return []
    seen: set[TargetKey] = set()
    plans: list[DatasetPlan] = []
    target: TargetKey
    for target in window.targets:
        if target in seen:
            raise ValueError("source window contains a duplicate target identity")
        seen.add(target)
        plans.append(
            DatasetPlan(
                identity=RoutingIdentity(
                    tenant_id=target.tenant_id,
                    namespace=target.namespace,
                    org_id=target.org_id,
                )
            )
        )
    return plans
