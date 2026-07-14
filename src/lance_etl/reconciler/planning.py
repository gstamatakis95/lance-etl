"""Mapping from side-effect-free Iceberg source plans into durable PostgreSQL work."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol

from lance_etl.reconciler.config import DeploymentProfile
from lance_etl.source import SourcePlan, TargetKey, WindowKind, WindowPlan
from lance_etl.state import RoutingIdentity, SourceWindowKind, SourceWindowPlan, TargetPlan


class SourceWindowRepository(Protocol):
    """Atomic state operation required by the source-plan adapter."""

    def enqueue_source_window(self, plan: SourceWindowPlan, target_plans: list[TargetPlan]) -> int:
        """Atomically enqueue a source window and its immutable target set.

        Args:
            plan: Durable source-window metadata.
            target_plans: Release-profile targets touched by the snapshot.

        Returns:
            Existing or newly created window sequence.
        """
        ...


@dataclass(frozen=True, slots=True)
class EnqueueSummary:
    """Bounded durable result of mapping one pinned source plan."""

    pinned_head_snapshot_id: int | None
    planned_windows: int
    enqueued_windows: int
    window_sequences: tuple[int, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class SourcePlanEnqueuer:
    """Atomically enqueue accepted source windows without inventing routing state."""

    repository: SourceWindowRepository
    profile: DeploymentProfile

    def enqueue(self, source_plan: SourcePlan) -> EnqueueSummary:
        """Map a bounded prefix of one pinned source plan into idempotent repository calls.

        Args:
            source_plan: Side-effect-free plan emitted by ``SourcePlanner``.

        Returns:
            Durable window identities and whether another planner pass is needed.

        Raises:
            ValueError: If the Iceberg table UUID is not canonical or a target is duplicated.
        """
        table_uuid = uuid.UUID(source_plan.table_uuid)
        selected = source_plan.windows[: self.profile.max_windows_per_plan]
        sequences: list[int] = []
        for window in selected:
            state_plan = map_window(table_uuid, window)
            targets = map_targets(window, self.profile.profile_id)
            sequences.append(self.repository.enqueue_source_window(state_plan, targets))
        return EnqueueSummary(
            pinned_head_snapshot_id=source_plan.pinned_head_snapshot_id,
            planned_windows=len(source_plan.windows),
            enqueued_windows=len(selected),
            window_sequences=tuple(sequences),
            truncated=len(selected) < len(source_plan.windows),
        )


def map_window(table_uuid: uuid.UUID, window: WindowPlan) -> SourceWindowPlan:
    """Convert one source window into the durable state representation.

    Args:
        table_uuid: Parsed canonical Iceberg table UUID.
        window: Accepted source window.

    Returns:
        Durable source-window plan.
    """
    kind = SourceWindowKind(window.kind.value)
    return SourceWindowPlan(
        table_uuid=table_uuid,
        snapshot_id=window.snapshot.snapshot_id,
        parent_snapshot_id=window.snapshot.parent_snapshot_id,
        iceberg_sequence_number=window.snapshot.sequence_number,
        kind=kind,
    )


def map_targets(window: WindowPlan, profile_id: str) -> list[TargetPlan]:
    """Convert manifest-derived target identities using one release-owned profile.

    Args:
        window: Accepted source window.
        profile_id: Versioned deployment policy identifier.

    Returns:
        Deterministically ordered durable target plans.

    Raises:
        ValueError: If one logical target appears more than once.
    """
    if window.kind is WindowKind.TRUSTED_MAINTENANCE:
        return []
    seen: set[TargetKey] = set()
    plans: list[TargetPlan] = []
    for touched in window.touched_targets:
        if touched.target in seen:
            raise ValueError("source window contains a duplicate target identity")
        seen.add(touched.target)
        plans.append(
            TargetPlan(
                identity=RoutingIdentity(
                    tenant_id=touched.target.tenant_id,
                    namespace=touched.target.namespace,
                    org_id=touched.target.org_id,
                ),
                profile_id=profile_id,
            )
        )
    return plans
