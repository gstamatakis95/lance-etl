"""Mapping from side-effect-free Iceberg source plans into durable PostgreSQL work."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from lance_etl.reconciler.config import ReconcilerSettings
from lance_etl.source import SourcePlan, TargetKey, TouchedTarget, WindowKind, WindowPlan
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
            source_plan: Side-effect-free plan emitted by ``SourcePlanner``.

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
            state_plan: SourceSnapshotPlan = map_snapshot(self.source.source_id, window)
            datasets: list[DatasetPlan] = map_datasets(window)
            sequences.append(self.repository.enqueue_source_snapshot(state_plan, datasets))
        return EnqueueSummary(
            pinned_head_snapshot_id=source_plan.pinned_head_snapshot_id,
            planned_snapshots=len(source_plan.windows),
            enqueued_snapshots=len(selected),
            source_snapshot_sequences=tuple(sequences),
            truncated=len(selected) < len(source_plan.windows),
        )


def map_snapshot(source_id: uuid.UUID, window: WindowPlan) -> SourceSnapshotPlan:
    """Convert one source window into the durable source-snapshot representation.

    Args:
        source_id: PostgreSQL-owned Iceberg source identity.
        window: Accepted source window.

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
    touched: TouchedTarget
    for touched in window.touched_targets:
        if touched.target in seen:
            raise ValueError("source window contains a duplicate target identity")
        seen.add(touched.target)
        plans.append(
            DatasetPlan(
                identity=RoutingIdentity(
                    tenant_id=touched.target.tenant_id,
                    namespace=touched.target.namespace,
                    org_id=touched.target.org_id,
                )
            )
        )
    return plans
