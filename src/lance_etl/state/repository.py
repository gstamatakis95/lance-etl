"""SQL-oriented PostgreSQL repository for durable source and target work."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine, RowMapping

from lance_etl.state.tables import source_windows, target_work, targets
from lance_etl.state.types import (
    ControlPlaneStatus,
    RoutingIdentity,
    ServingTarget,
    SourceWindowKind,
    SourceWindowPlan,
    SourceWindowState,
    TargetPlan,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
    WorkState,
    deterministic_ingest_work_id,
    deterministic_target_id,
    ingest_uri,
)

ERROR_CODE_LIMIT: int = 128
"""Maximum persisted error-code length."""

ERROR_MESSAGE_LIMIT: int = 2000
"""Maximum persisted diagnostic length."""

PHASE_ORDER: dict[WorkPhase, int] = {
    WorkPhase.INGEST: 0,
    WorkPhase.MAINTAIN: 1,
    WorkPhase.INDEX: 2,
    WorkPhase.VALIDATE: 3,
    WorkPhase.PREWARM: 4,
    WorkPhase.PUBLISH: 5,
}
"""Forward-only phase order for durable checkpoints."""


class StateTransitionError(RuntimeError):
    """Raised when durable state no longer matches a requested transition."""


def utc_now() -> datetime:
    """Return the current timezone-aware UTC instant.

    Returns:
        Current UTC time.
    """
    return datetime.now(UTC)


def bounded_error(value: str | None, limit: int) -> str | None:
    """Truncate an optional diagnostic to its durable schema bound.

    Args:
        value: Diagnostic string or ``None``.
        limit: Maximum persisted character count.

    Returns:
        The original or truncated diagnostic.
    """
    return value[:limit] if value is not None else None


def completed_publication_matches(
    work_row: RowMapping,
    current_catalog: tuple[str | None, int | None],
    desired_catalog: tuple[str, int],
    indexed_lance_version: int,
    artifact_manifest_uri: str,
    artifact_digest: bytes,
) -> bool:
    """Validate an idempotent replay after publication already succeeded.

    Args:
        work_row: Locked durable work row.
        current_catalog: Current exact serving catalog tuple.
        desired_catalog: Requested exact serving catalog tuple.
        indexed_lance_version: Requested validated Lance version.
        artifact_manifest_uri: Requested immutable manifest URI.
        artifact_digest: Requested immutable artifact digest.

    Returns:
        True when work was already completed with the exact requested output.

    Raises:
        StateTransitionError: If completed durable state differs from the replayed request.
    """
    if work_row["state"] != WorkState.SUCCEEDED.value:
        return False
    stored_digest = work_row["artifact_digest"]
    actual_outputs = (
        work_row["indexed_lance_version"],
        work_row["artifact_manifest_uri"],
        bytes(stored_digest) if stored_digest is not None else None,
    )
    expected_outputs = indexed_lance_version, artifact_manifest_uri, artifact_digest
    if current_catalog != desired_catalog or actual_outputs != expected_outputs:
        raise StateTransitionError("completed publication differs from the requested durable result")
    return True


def publication_lease_matches(
    work_row: RowMapping,
    target_row: RowMapping,
    claim: WorkClaim,
    current: datetime,
) -> bool:
    """Check whether a locked publication still belongs to its fenced worker.

    Args:
        work_row: Locked durable work row.
        target_row: Locked target catalog row.
        claim: Worker claim presented for publication.
        current: Transaction clock.

    Returns:
        True only while state, lease, fence, and expiry all match.
    """
    lease_expires_at = work_row["lease_expires_at"]
    return (
        work_row["state"] == WorkState.RUNNING.value
        and work_row["lease_token"] == claim.lease_token
        and int(target_row["fence_epoch"]) == claim.fence_epoch
        and lease_expires_at is not None
        and lease_expires_at > current
    )


def publication_target_values(
    claim: WorkClaim,
    candidate_lance_uri: str,
    indexed_lance_version: int,
    current: datetime,
) -> dict[str, Any]:
    """Build the atomic target-catalog update for a publication.

    Args:
        claim: Current SERVE or REBUILD claim.
        candidate_lance_uri: Validated candidate dataset URI.
        indexed_lance_version: Exact validated version.
        current: Transaction clock.

    Returns:
        Catalog values for the target update.
    """
    values: dict[str, Any] = {
        "served_lance_uri": candidate_lance_uri,
        "served_lance_version": indexed_lance_version,
        "updated_at": current,
    }
    if claim.kind == WorkKind.REBUILD:
        values.update(
            ingest_lance_uri=candidate_lance_uri,
            last_applied_lance_version=indexed_lance_version,
        )
    return values


def build_control_plane_engine(database_url: str) -> Engine:
    """Build the direct psycopg-backed SQLAlchemy engine.

    Args:
        database_url: PostgreSQL URL using the psycopg driver.

    Returns:
        Pool-pre-ping SQLAlchemy engine.

    Raises:
        ValueError: If the URL does not select PostgreSQL with psycopg 3.
    """
    engine: Engine = sa.create_engine(database_url, pool_pre_ping=True)
    if engine.dialect.name != "postgresql" or engine.dialect.driver != "psycopg":
        engine.dispose()
        raise ValueError("control plane requires a postgresql+psycopg database URL")
    return engine


@dataclass(frozen=True)
class ControlPlaneRepository:
    """Owns visible transactions and fenced target-work transitions."""

    engine: Engine
    lance_base_uri: str

    def enqueue_source_window(self, plan: SourceWindowPlan, target_plans: Sequence[TargetPlan]) -> int:
        """Atomically seal one source window and enqueue its target INGEST work.

        Args:
            plan: Exact immutable Iceberg snapshot plan.
            target_plans: Validated targets touched by the snapshot.

        Returns:
            Existing or newly allocated source-window sequence.

        Raises:
            StateTransitionError: If an existing idempotency key carries different metadata.
        """
        plan.validate()
        validated_targets: list[TargetPlan] = [target_plan.validate() for target_plan in target_plans]
        with self.engine.begin() as connection:
            window_seq, inserted = self.insert_or_validate_window(connection, plan)
            if not inserted:
                expected_target_ids: set[uuid.UUID] = {
                    deterministic_target_id(target_plan.identity) for target_plan in validated_targets
                }
                existing_target_ids: set[uuid.UUID] = set(
                    connection.execute(
                        sa.select(target_work.c.target_id).where(
                            target_work.c.source_window_seq == window_seq,
                            target_work.c.kind == WorkKind.INGEST.value,
                        )
                    ).scalars()
                )
                if not existing_target_ids:
                    window_state: str = connection.execute(
                        sa.select(source_windows.c.state).where(source_windows.c.window_seq == window_seq)
                    ).scalar_one()
                    if window_state == SourceWindowState.COMPLETE.value:
                        return window_seq
                if existing_target_ids != expected_target_ids:
                    raise StateTransitionError("existing source window carries a different immutable target set")
            for target_plan in validated_targets:
                target_row: RowMapping = self.insert_or_validate_target(connection, target_plan)
                self.insert_ingest_work(connection, target_row, window_seq)
            if not validated_targets:
                connection.execute(
                    source_windows.update()
                    .where(source_windows.c.window_seq == window_seq)
                    .where(source_windows.c.state == SourceWindowState.SEALED.value)
                    .values(state=SourceWindowState.COMPLETE.value, updated_at=utc_now())
                )
            return window_seq

    def work_execution_context(
        self,
        claim: WorkClaim,
        now: datetime | None = None,
    ) -> WorkExecutionContext | None:
        """Resolve immutable target and exact source metadata for a live fenced claim.

        Args:
            claim: Current worker claim.
            now: Deterministic clock override for tests.

        Returns:
            Execution context while the claim lease and target fence remain current, otherwise null.
        """
        current = now or utc_now()
        joined = target_work.join(targets, target_work.c.target_id == targets.c.target_id).outerjoin(
            source_windows,
            target_work.c.source_window_seq == source_windows.c.window_seq,
        )
        context_query = sa.select(
            target_work.c.state,
            target_work.c.lease_token,
            target_work.c.lease_expires_at,
            target_work.c.candidate_lance_uri,
            target_work.c.indexed_lance_version,
            target_work.c.artifact_manifest_uri,
            target_work.c.artifact_digest,
            targets.c.fence_epoch,
            targets.c.tenant_id,
            targets.c.namespace,
            targets.c.org_id,
            targets.c.profile_id,
            source_windows.c.snapshot_id,
            source_windows.c.parent_snapshot_id,
            source_windows.c.iceberg_sequence_number,
            source_windows.c.kind.label("source_window_kind"),
        ).select_from(joined)
        with self.engine.connect() as connection:
            row = (
                connection.execute(context_query.where(target_work.c.work_id == claim.work_id)).mappings().one_or_none()
            )
        if row is None:
            return None
        live = (
            row["state"] == WorkState.RUNNING.value
            and row["lease_token"] == claim.lease_token
            and int(row["fence_epoch"]) == claim.fence_epoch
            and row["lease_expires_at"] is not None
            and row["lease_expires_at"] > current
        )
        if not live:
            return None
        identity = RoutingIdentity(
            tenant_id=str(row["tenant_id"]),
            namespace=str(row["namespace"]),
            org_id=str(row["org_id"]),
        ).validate()
        snapshot_id = int(row["snapshot_id"]) if row["snapshot_id"] is not None else None
        parent_snapshot_id = int(row["parent_snapshot_id"]) if row["parent_snapshot_id"] is not None else None
        sequence = int(row["iceberg_sequence_number"]) if row["iceberg_sequence_number"] is not None else None
        source_kind = row["source_window_kind"]
        kind = SourceWindowKind(source_kind) if source_kind is not None else None
        return WorkExecutionContext(
            claim=claim,
            identity=identity,
            profile_id=str(row["profile_id"]),
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_snapshot_id,
            iceberg_sequence_number=sequence,
            source_window_kind=kind,
            candidate_lance_uri=row["candidate_lance_uri"],
            indexed_lance_version=(
                int(row["indexed_lance_version"]) if row["indexed_lance_version"] is not None else None
            ),
            artifact_manifest_uri=row["artifact_manifest_uri"],
            artifact_digest=bytes(row["artifact_digest"]) if row["artifact_digest"] is not None else None,
        )

    def insert_or_validate_window(self, connection: Connection, plan: SourceWindowPlan) -> tuple[int, bool]:
        """Insert one source window or validate an idempotent replay.

        Args:
            connection: Active transaction connection.
            plan: Validated immutable source plan.

        Returns:
            Durable source-window sequence and whether this transaction inserted it.
        """
        insert = (
            postgresql.insert(source_windows)
            .values(
                table_uuid=plan.table_uuid,
                snapshot_id=plan.snapshot_id,
                parent_snapshot_id=plan.parent_snapshot_id,
                iceberg_sequence_number=plan.iceberg_sequence_number,
                kind=plan.kind.value,
                state=SourceWindowState.SEALED.value,
            )
            .on_conflict_do_nothing(index_elements=[source_windows.c.table_uuid, source_windows.c.snapshot_id])
            .returning(source_windows.c.window_seq)
        )
        inserted: int | None = connection.execute(insert).scalar_one_or_none()
        if inserted is not None:
            return int(inserted), True
        row: RowMapping = (
            connection.execute(
                source_windows.select()
                .where(source_windows.c.table_uuid == plan.table_uuid)
                .where(source_windows.c.snapshot_id == plan.snapshot_id)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        expected: tuple[Any, ...] = (
            plan.parent_snapshot_id,
            plan.iceberg_sequence_number,
            plan.kind.value,
        )
        actual: tuple[Any, ...] = (
            row["parent_snapshot_id"],
            row["iceberg_sequence_number"],
            row["kind"],
        )
        if actual != expected:
            raise StateTransitionError("existing source window differs from the replayed immutable snapshot plan")
        return int(row["window_seq"]), False

    def insert_or_validate_target(self, connection: Connection, plan: TargetPlan) -> RowMapping:
        """Insert a missing target or validate its code-owned immutable fields.

        Args:
            connection: Active transaction connection.
            plan: Validated target plan.

        Returns:
            Durable target row.
        """
        identity: RoutingIdentity = plan.identity
        target_id: uuid.UUID = deterministic_target_id(identity)
        uri: str = ingest_uri(self.lance_base_uri, target_id)
        insert = (
            postgresql.insert(targets)
            .values(
                target_id=target_id,
                tenant_id=identity.tenant_id,
                namespace=identity.namespace,
                org_id=identity.org_id,
                ingest_lance_uri=uri,
                profile_id=plan.profile_id,
            )
            .on_conflict_do_nothing(index_elements=[targets.c.tenant_id, targets.c.namespace, targets.c.org_id])
        )
        connection.execute(insert)
        row: RowMapping = (
            connection.execute(
                targets.select()
                .where(targets.c.tenant_id == identity.tenant_id)
                .where(targets.c.namespace == identity.namespace)
                .where(targets.c.org_id == identity.org_id)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        if row["target_id"] != target_id or row["ingest_lance_uri"] != uri or row["profile_id"] != plan.profile_id:
            raise StateTransitionError("existing target differs from the release-owned target plan")
        return row

    def insert_ingest_work(self, connection: Connection, target_row: RowMapping, window_seq: int) -> None:
        """Insert deterministic INGEST work if it is not already durable.

        Args:
            connection: Active transaction connection.
            target_row: Durable target row.
            window_seq: Source-window sequence.
        """
        work_id: uuid.UUID = deterministic_ingest_work_id(target_row["target_id"], window_seq)
        insert = (
            postgresql.insert(target_work)
            .values(
                work_id=work_id,
                target_id=target_row["target_id"],
                source_window_seq=window_seq,
                kind=WorkKind.INGEST.value,
                state=WorkState.PENDING.value,
                phase=WorkPhase.INGEST.value,
                expected_ingest_lance_uri=target_row["ingest_lance_uri"],
            )
            .on_conflict_do_nothing(index_elements=[target_work.c.work_id])
        )
        connection.execute(insert)

    def claim_due_work(
        self,
        limit: int,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> list[WorkClaim]:
        """Claim at most one eligible work item per target with fencing.

        Args:
            limit: Maximum number of target lanes to claim.
            lease_duration: New lease duration.
            now: Deterministic clock override for tests.

        Returns:
            Fenced work claims owned by fresh lease tokens.

        Raises:
            ValueError: If the limit or lease duration is not positive.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current: datetime = now or utc_now()
        claims: list[WorkClaim] = []
        with self.engine.begin() as connection:
            candidate_target_ids: list[uuid.UUID] = list(
                connection.execute(
                    sa.select(target_work.c.target_id).where(self.due_predicate(current)).distinct().limit(limit * 4)
                ).scalars()
            )
            for target_id in candidate_target_ids:
                if len(claims) >= limit:
                    break
                target_row: RowMapping | None = (
                    connection.execute(
                        targets.select().where(targets.c.target_id == target_id).with_for_update(skip_locked=True)
                    )
                    .mappings()
                    .one_or_none()
                )
                if target_row is None:
                    continue
                work_row: RowMapping | None = (
                    connection.execute(
                        target_work.select()
                        .where(target_work.c.target_id == target_id)
                        .where(self.due_predicate(current))
                        .where(self.ordered_ingest_predicate())
                        .order_by(
                            sa.case((target_work.c.kind == WorkKind.INGEST.value, 0), else_=1),
                            target_work.c.source_window_seq.asc().nulls_last(),
                            target_work.c.created_at,
                        )
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    .mappings()
                    .one_or_none()
                )
                if work_row is None:
                    continue
                fence_epoch: int = int(target_row["fence_epoch"]) + 1
                lease_token: uuid.UUID = uuid.uuid4()
                connection.execute(
                    targets.update()
                    .where(targets.c.target_id == target_id)
                    .values(fence_epoch=fence_epoch, updated_at=current)
                )
                updated: RowMapping = (
                    connection.execute(
                        target_work.update()
                        .where(target_work.c.work_id == work_row["work_id"])
                        .values(
                            state=WorkState.RUNNING.value,
                            lease_token=lease_token,
                            lease_expires_at=current + lease_duration,
                            attempt_count=target_work.c.attempt_count + 1,
                            updated_at=current,
                        )
                        .returning(target_work)
                    )
                    .mappings()
                    .one()
                )
                claims.append(self.claim_from_row(updated, fence_epoch))
        return claims

    def due_predicate(self, now: datetime) -> sa.ColumnElement[bool]:
        """Build eligibility for pending, retrying, or expired-running work.

        Args:
            now: Claim transaction time.

        Returns:
            SQL predicate for due work.
        """
        return sa.or_(
            sa.and_(
                target_work.c.state.in_((WorkState.PENDING.value, WorkState.RETRY_WAIT.value)),
                target_work.c.next_attempt_at <= now,
            ),
            sa.and_(
                target_work.c.state == WorkState.RUNNING.value,
                target_work.c.lease_expires_at <= now,
            ),
        )

    def ordered_ingest_predicate(self) -> sa.ColumnElement[bool]:
        """Require INGEST claims to be the smallest unfinished sequence for a target.

        Returns:
            SQL predicate enforcing target-local source order.
        """
        predecessor: sa.Alias = target_work.alias("predecessor_work")
        smaller_unfinished = sa.exists(
            sa.select(sa.literal(1)).where(
                predecessor.c.target_id == target_work.c.target_id,
                predecessor.c.kind == WorkKind.INGEST.value,
                predecessor.c.state != WorkState.SUCCEEDED.value,
                predecessor.c.source_window_seq < target_work.c.source_window_seq,
            )
        )
        return sa.or_(target_work.c.kind != WorkKind.INGEST.value, ~smaller_unfinished)

    def claim_from_row(self, row: RowMapping, fence_epoch: int) -> WorkClaim:
        """Convert one claimed database row into a typed lease.

        Args:
            row: Claimed target-work row.
            fence_epoch: Target fence installed by the claim transaction.

        Returns:
            Typed work claim.
        """
        return WorkClaim(
            work_id=row["work_id"],
            target_id=row["target_id"],
            kind=WorkKind(row["kind"]),
            phase=WorkPhase(row["phase"]),
            lease_token=row["lease_token"],
            fence_epoch=fence_epoch,
            attempt_count=int(row["attempt_count"]),
            source_window_seq=row["source_window_seq"],
            expected_ingest_lance_uri=row["expected_ingest_lance_uri"],
            data_lance_version=row["data_lance_version"],
        )

    def lease_is_current(self, claim: WorkClaim) -> sa.ColumnElement[bool]:
        """Build the database fence predicate for a claimed worker.

        Args:
            claim: Work claim presented by a worker.

        Returns:
            SQL predicate requiring the current target fence.
        """
        return sa.exists(
            sa.select(sa.literal(1)).where(
                targets.c.target_id == target_work.c.target_id,
                targets.c.fence_epoch == claim.fence_epoch,
            )
        )

    def renew_lease(
        self,
        claim: WorkClaim,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> bool:
        """Renew a lease only while its token and target fence are current.

        Args:
            claim: Current fenced claim.
            lease_duration: New duration from the renewal instant.
            now: Deterministic clock override for tests.

        Returns:
            True when renewed, false for a stale worker.
        """
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                target_work.update()
                .where(target_work.c.work_id == claim.work_id)
                .where(target_work.c.state == WorkState.RUNNING.value)
                .where(target_work.c.lease_token == claim.lease_token)
                .where(target_work.c.lease_expires_at > current)
                .where(self.lease_is_current(claim))
                .values(lease_expires_at=current + lease_duration, updated_at=current)
            )
            return result.rowcount == 1

    def advance_phase(
        self,
        claim: WorkClaim,
        phase: WorkPhase,
        data_lance_version: int | None = None,
        indexed_lance_version: int | None = None,
        candidate_lance_uri: str | None = None,
        artifact_manifest_uri: str | None = None,
        artifact_digest: bytes | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Checkpoint a forward phase transition under the current lease and fence.

        Args:
            claim: Current fenced claim.
            phase: Later phase reached by the worker.
            data_lance_version: Optional exact data input or output version.
            indexed_lance_version: Optional exact indexed version.
            candidate_lance_uri: Optional frozen publication candidate.
            artifact_manifest_uri: Optional immutable artifact manifest.
            artifact_digest: Optional 32-byte artifact digest.
            now: Deterministic clock override for tests.

        Returns:
            True when checkpointed, false for a stale worker.

        Raises:
            ValueError: If the transition moves backward or carries invalid output.
        """
        if artifact_digest is not None and len(artifact_digest) != 32:
            raise ValueError("artifact_digest must contain exactly 32 bytes")
        current: datetime = now or utc_now()
        values: dict[str, Any] = {"phase": phase.value, "updated_at": current}
        optional_values: dict[str, Any] = {
            "data_lance_version": data_lance_version,
            "indexed_lance_version": indexed_lance_version,
            "candidate_lance_uri": candidate_lance_uri,
            "artifact_manifest_uri": artifact_manifest_uri,
            "artifact_digest": artifact_digest,
        }
        values.update({key: value for key, value in optional_values.items() if value is not None})
        with self.engine.begin() as connection:
            row: RowMapping | None = (
                connection.execute(
                    target_work.select()
                    .where(target_work.c.work_id == claim.work_id)
                    .where(target_work.c.state == WorkState.RUNNING.value)
                    .where(target_work.c.lease_token == claim.lease_token)
                    .where(target_work.c.lease_expires_at > current)
                    .where(self.lease_is_current(claim))
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            if PHASE_ORDER[phase] <= PHASE_ORDER[WorkPhase(row["phase"])]:
                raise ValueError("phase transitions must move forward")
            result = connection.execute(
                target_work.update().where(target_work.c.work_id == claim.work_id).values(**values)
            )
            return result.rowcount == 1

    def retry_work(
        self,
        claim: WorkClaim,
        delay: timedelta,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        """Persist a transient failure without imposing an attempt ceiling.

        Args:
            claim: Current fenced claim.
            delay: Code-owned retry delay.
            error_code: Bounded stable error classification.
            error_message: Bounded operator diagnostic.
            now: Deterministic clock override for tests.

        Returns:
            True when transitioned, false for a stale worker.
        """
        if delay < timedelta(0):
            raise ValueError("delay must be non-negative")
        current: datetime = now or utc_now()
        return self.finish_running_transition(
            claim,
            {
                "state": WorkState.RETRY_WAIT.value,
                "next_attempt_at": current + delay,
                "error_code": bounded_error(error_code, ERROR_CODE_LIMIT),
                "error_message": bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                "updated_at": current,
            },
            current,
        )

    def block_work(
        self,
        claim: WorkClaim,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        """Block contract-invalid work while preserving its durable identity.

        Args:
            claim: Current fenced claim.
            error_code: Stable contract-failure code.
            error_message: Bounded operator diagnostic.
            now: Deterministic clock override for tests.

        Returns:
            True when transitioned, false for a stale worker.
        """
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                target_work.update()
                .where(target_work.c.work_id == claim.work_id)
                .where(target_work.c.state == WorkState.RUNNING.value)
                .where(target_work.c.lease_token == claim.lease_token)
                .where(target_work.c.lease_expires_at > current)
                .where(self.lease_is_current(claim))
                .values(
                    state=WorkState.BLOCKED.value,
                    lease_token=None,
                    lease_expires_at=None,
                    error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                    error_message=bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def block_source_window(
        self,
        window_seq: int,
        error_code: str,
        now: datetime | None = None,
    ) -> bool:
        """Block a source-level lineage or snapshot-contract failure.

        Args:
            window_seq: Sealed source window to block.
            error_code: Stable bounded source-contract code.
            now: Deterministic clock override for tests.

        Returns:
            True when the sealed window transitioned, false otherwise.
        """
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                source_windows.update()
                .where(source_windows.c.window_seq == window_seq)
                .where(source_windows.c.state == SourceWindowState.SEALED.value)
                .values(
                    state=SourceWindowState.BLOCKED.value,
                    error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def retry_blocked_work(self, work_id: uuid.UUID, now: datetime | None = None) -> bool:
        """Return blocked target work to its same durable identity for operator retry.

        Args:
            work_id: Existing blocked work identity.
            now: Deterministic clock override for tests.

        Returns:
            True when returned to pending, false when the work was not blocked.
        """
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                target_work.update()
                .where(target_work.c.work_id == work_id)
                .where(target_work.c.state == WorkState.BLOCKED.value)
                .values(
                    state=WorkState.PENDING.value,
                    next_attempt_at=current,
                    error_code=None,
                    error_message=None,
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def finish_running_transition(self, claim: WorkClaim, values: dict[str, Any], current: datetime) -> bool:
        """Apply a lease-clearing transition guarded by token and target fence.

        Args:
            claim: Current fenced claim.
            values: Transition values excluding cleared lease fields.
            current: Transaction clock used to reject an expired lease.

        Returns:
            True when transitioned, false for a stale worker.
        """
        with self.engine.begin() as connection:
            result = connection.execute(
                target_work.update()
                .where(target_work.c.work_id == claim.work_id)
                .where(target_work.c.state == WorkState.RUNNING.value)
                .where(target_work.c.lease_token == claim.lease_token)
                .where(target_work.c.lease_expires_at > current)
                .where(self.lease_is_current(claim))
                .values(**values, lease_token=None, lease_expires_at=None)
            )
            return result.rowcount == 1

    def complete_ingest(
        self,
        claim: WorkClaim,
        data_lance_version: int,
        source_row_count: int,
        source_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Complete source-applied INGEST and coalesce serving work atomically.

        Args:
            claim: Current fenced INGEST claim.
            data_lance_version: Exact Lance version carrying the durable completion marker.
            source_row_count: Terminal mutation count for the target and source snapshot.
            source_digest: Frozen 32-byte source digest.
            now: Deterministic clock override for tests.

        Returns:
            True when completed or already completed with identical outputs.

        Raises:
            ValueError: If completion values or claim kind are invalid.
            StateTransitionError: If an already-completed identity carries different outputs.
        """
        if claim.kind != WorkKind.INGEST or claim.source_window_seq is None:
            raise ValueError("complete_ingest requires an INGEST claim with a source window")
        if data_lance_version < 1 or source_row_count < 0 or len(source_digest) != 32:
            raise ValueError("invalid INGEST completion values")
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            row: RowMapping = (
                connection.execute(target_work.select().where(target_work.c.work_id == claim.work_id).with_for_update())
                .mappings()
                .one()
            )
            if row["state"] == WorkState.SUCCEEDED.value:
                expected: tuple[Any, ...] = (data_lance_version, source_row_count, source_digest)
                actual: tuple[Any, ...] = (
                    row["data_lance_version"],
                    row["source_row_count"],
                    bytes(row["source_digest"]),
                )
                if actual != expected:
                    raise StateTransitionError("INGEST completion identity already carries different outputs")
                return True
            result = connection.execute(
                target_work.update()
                .where(target_work.c.work_id == claim.work_id)
                .where(target_work.c.state == WorkState.RUNNING.value)
                .where(target_work.c.lease_token == claim.lease_token)
                .where(target_work.c.lease_expires_at > current)
                .where(self.lease_is_current(claim))
                .values(
                    state=WorkState.SUCCEEDED.value,
                    lease_token=None,
                    lease_expires_at=None,
                    source_applied_at=current,
                    source_row_count=source_row_count,
                    source_digest=source_digest,
                    data_lance_version=data_lance_version,
                    error_code=None,
                    error_message=None,
                    updated_at=current,
                )
            )
            if result.rowcount != 1:
                return False
            connection.execute(
                targets.update()
                .where(targets.c.target_id == claim.target_id)
                .where(
                    sa.or_(
                        targets.c.last_applied_window_seq.is_(None),
                        targets.c.last_applied_window_seq < claim.source_window_seq,
                    )
                )
                .values(
                    last_applied_window_seq=claim.source_window_seq,
                    last_applied_lance_version=data_lance_version,
                    updated_at=current,
                )
            )
            self.complete_window_if_applied(connection, claim.source_window_seq, current)
            self.enqueue_or_advance_serve(connection, claim.target_id, data_lance_version, current)
            return True

    def complete_window_if_applied(self, connection: Connection, window_seq: int, now: datetime) -> None:
        """Mark a window complete when every INGEST child is source-applied.

        Args:
            connection: Active completion transaction.
            window_seq: Source window whose children changed.
            now: Transaction timestamp.
        """
        unfinished = sa.exists(
            sa.select(sa.literal(1)).where(
                target_work.c.source_window_seq == window_seq,
                target_work.c.kind == WorkKind.INGEST.value,
                target_work.c.source_applied_at.is_(None),
            )
        )
        connection.execute(
            source_windows.update()
            .where(source_windows.c.window_seq == window_seq)
            .where(~unfinished)
            .values(state=SourceWindowState.COMPLETE.value, error_code=None, updated_at=now)
        )

    def enqueue_or_advance_serve(
        self,
        connection: Connection,
        target_id: uuid.UUID,
        data_lance_version: int,
        now: datetime,
    ) -> None:
        """Coalesce serving work without mutating a RUNNING generation.

        Args:
            connection: Active completion transaction.
            target_id: Dirty target identifier.
            data_lance_version: Newest applied Lance version.
            now: Transaction timestamp.
        """
        target_row: RowMapping = (
            connection.execute(targets.select().where(targets.c.target_id == target_id).with_for_update())
            .mappings()
            .one()
        )
        open_serve: RowMapping | None = (
            connection.execute(
                target_work.select()
                .where(target_work.c.target_id == target_id)
                .where(target_work.c.kind == WorkKind.SERVE.value)
                .where(
                    target_work.c.state.in_(
                        (WorkState.PENDING.value, WorkState.RUNNING.value, WorkState.RETRY_WAIT.value)
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if open_serve is None:
            connection.execute(
                target_work.insert().values(
                    work_id=uuid.uuid4(),
                    target_id=target_id,
                    kind=WorkKind.SERVE.value,
                    state=WorkState.PENDING.value,
                    phase=WorkPhase.MAINTAIN.value,
                    data_lance_version=data_lance_version,
                    expected_ingest_lance_uri=target_row["ingest_lance_uri"],
                    expected_served_lance_uri=target_row["served_lance_uri"],
                    expected_served_lance_version=target_row["served_lance_version"],
                    next_attempt_at=now,
                )
            )
            return
        if open_serve["state"] == WorkState.RUNNING.value:
            return
        connection.execute(
            target_work.update()
            .where(target_work.c.work_id == open_serve["work_id"])
            .values(
                state=WorkState.PENDING.value,
                phase=WorkPhase.MAINTAIN.value,
                next_attempt_at=now,
                data_lance_version=data_lance_version,
                indexed_lance_version=None,
                candidate_lance_uri=None,
                artifact_manifest_uri=None,
                artifact_digest=None,
                error_code=None,
                error_message=None,
                updated_at=now,
            )
        )

    def complete_serve(
        self,
        claim: WorkClaim,
        indexed_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Complete a SERVE generation without changing source retention.

        Args:
            claim: Current fenced SERVE claim.
            indexed_lance_version: Exact validated and published version.
            artifact_manifest_uri: Immutable artifact manifest URI.
            artifact_digest: Frozen 32-byte artifact digest.
            now: Deterministic clock override for tests.

        Returns:
            True when completed, false for a stale worker.
        """
        if claim.kind not in (WorkKind.SERVE, WorkKind.REBUILD):
            raise ValueError("complete_serve requires a SERVE or REBUILD claim")
        if indexed_lance_version < 1 or not artifact_manifest_uri or len(artifact_digest) != 32:
            raise ValueError("invalid SERVE completion values")
        current: datetime = now or utc_now()
        return self.finish_running_transition(
            claim,
            {
                "state": WorkState.SUCCEEDED.value,
                "phase": WorkPhase.PUBLISH.value,
                "indexed_lance_version": indexed_lance_version,
                "artifact_manifest_uri": artifact_manifest_uri,
                "artifact_digest": artifact_digest,
                "error_code": None,
                "error_message": None,
                "updated_at": current,
            },
            current,
        )

    def resolve_serving_target(self, identity: RoutingIdentity) -> ServingTarget | None:
        """Resolve one validated logical target to an exact published dataset version.

        Args:
            identity: Logical target identity validated before database access.

        Returns:
            Exact serving target or null before first publication or for an unknown target.
        """
        identity.validate()
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    targets.select()
                    .where(targets.c.tenant_id == identity.tenant_id)
                    .where(targets.c.namespace == identity.namespace)
                    .where(targets.c.org_id == identity.org_id)
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["served_lance_uri"] is None or row["served_lance_version"] is None:
            return None
        return ServingTarget(
            target_id=row["target_id"],
            identity=identity,
            lance_uri=str(row["served_lance_uri"]),
            lance_version=int(row["served_lance_version"]),
            profile_id=str(row["profile_id"]),
        )

    def enqueue_rollback(self, identity: RoutingIdentity, successful_work_id: uuid.UUID) -> uuid.UUID:
        """Enqueue a fenced rollback to one retained validated publication.

        Args:
            identity: Validated logical target selected by an operator.
            successful_work_id: Retained successful SERVE or REBUILD publication.

        Returns:
            New rollback work identity requiring exact prewarm before catalog publication.

        Raises:
            StateTransitionError: If the target, retained result, or target lane is unsuitable.
        """
        identity.validate()
        current = utc_now()
        with self.engine.begin() as connection:
            target_row = (
                connection.execute(
                    targets.select()
                    .where(targets.c.tenant_id == identity.tenant_id)
                    .where(targets.c.namespace == identity.namespace)
                    .where(targets.c.org_id == identity.org_id)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if target_row is None:
                raise StateTransitionError("rollback target does not exist")
            retained = (
                connection.execute(
                    target_work.select()
                    .where(target_work.c.work_id == successful_work_id)
                    .where(target_work.c.target_id == target_row["target_id"])
                    .where(target_work.c.kind.in_((WorkKind.SERVE.value, WorkKind.REBUILD.value)))
                    .where(target_work.c.state == WorkState.SUCCEEDED.value)
                )
                .mappings()
                .one_or_none()
            )
            required = (
                retained is not None
                and retained["candidate_lance_uri"] is not None
                and retained["indexed_lance_version"] is not None
                and retained["artifact_manifest_uri"] is not None
                and retained["artifact_digest"] is not None
            )
            if not required or retained is None:
                raise StateTransitionError("rollback result lacks retained validated publication evidence")
            open_lane = connection.scalar(
                sa.select(sa.literal(True))
                .select_from(target_work)
                .where(target_work.c.target_id == target_row["target_id"])
                .where(
                    target_work.c.state.in_(
                        (WorkState.PENDING.value, WorkState.RUNNING.value, WorkState.RETRY_WAIT.value)
                    )
                )
                .limit(1)
            )
            if open_lane:
                raise StateTransitionError("rollback requires an idle target lane")
            rollback_work_id = uuid.uuid4()
            connection.execute(
                target_work.insert().values(
                    work_id=rollback_work_id,
                    target_id=target_row["target_id"],
                    kind=WorkKind.SERVE.value,
                    state=WorkState.PENDING.value,
                    phase=WorkPhase.PREWARM.value,
                    data_lance_version=target_row["last_applied_lance_version"],
                    indexed_lance_version=retained["indexed_lance_version"],
                    candidate_lance_uri=retained["candidate_lance_uri"],
                    expected_ingest_lance_uri=target_row["ingest_lance_uri"],
                    expected_served_lance_uri=target_row["served_lance_uri"],
                    expected_served_lance_version=target_row["served_lance_version"],
                    artifact_manifest_uri=retained["artifact_manifest_uri"],
                    artifact_digest=retained["artifact_digest"],
                    next_attempt_at=current,
                )
            )
            return rollback_work_id

    def publish_serve(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        indexed_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Atomically publish an exact version and complete its fenced work generation.

        Args:
            claim: Current fenced SERVE or REBUILD claim.
            candidate_lance_uri: Persisted validated candidate dataset URI.
            indexed_lance_version: Exact validated candidate version.
            artifact_manifest_uri: Immutable artifact manifest URI.
            artifact_digest: Frozen 32-byte artifact digest.
            now: Deterministic clock override for tests.

        Returns:
            True when the desired catalog and work state are durable, false for a stale worker.

        Raises:
            StateTransitionError: If catalog state differs from both expected and desired tuples.
            ValueError: If inputs or work kind are invalid.
        """
        if claim.kind not in (WorkKind.SERVE, WorkKind.REBUILD):
            raise ValueError("publish_serve requires a SERVE or REBUILD claim")
        if indexed_lance_version < 1 or not candidate_lance_uri or not artifact_manifest_uri:
            raise ValueError("invalid publication values")
        if len(artifact_digest) != 32:
            raise ValueError("artifact digest must contain 32 bytes")
        current = now or utc_now()
        with self.engine.begin() as connection:
            work_row = (
                connection.execute(target_work.select().where(target_work.c.work_id == claim.work_id).with_for_update())
                .mappings()
                .one()
            )
            target_row = (
                connection.execute(targets.select().where(targets.c.target_id == claim.target_id).with_for_update())
                .mappings()
                .one()
            )
            desired_tuple = candidate_lance_uri, indexed_lance_version
            current_tuple = target_row["served_lance_uri"], target_row["served_lance_version"]
            if completed_publication_matches(
                work_row,
                current_tuple,
                desired_tuple,
                indexed_lance_version,
                artifact_manifest_uri,
                artifact_digest,
            ):
                return True
            if work_row["phase"] != WorkPhase.PREWARM.value:
                raise StateTransitionError("publication requires a completed PREWARM phase")
            if not publication_lease_matches(work_row, target_row, claim, current):
                return False
            expected_tuple = work_row["expected_served_lance_uri"], work_row["expected_served_lance_version"]
            if current_tuple != expected_tuple:
                if current_tuple == desired_tuple:
                    raise StateTransitionError("catalog is desired but work completion is missing")
                raise StateTransitionError("serving catalog differs from the persisted expected tuple")
            if target_row["ingest_lance_uri"] != work_row["expected_ingest_lance_uri"]:
                raise StateTransitionError("ingest catalog differs from the persisted expected URI")
            target_values = publication_target_values(claim, candidate_lance_uri, indexed_lance_version, current)
            connection.execute(targets.update().where(targets.c.target_id == claim.target_id).values(**target_values))
            connection.execute(
                target_work.update()
                .where(target_work.c.work_id == claim.work_id)
                .values(
                    state=WorkState.SUCCEEDED.value,
                    phase=WorkPhase.PUBLISH.value,
                    lease_token=None,
                    lease_expires_at=None,
                    candidate_lance_uri=candidate_lance_uri,
                    indexed_lance_version=indexed_lance_version,
                    artifact_manifest_uri=artifact_manifest_uri,
                    artifact_digest=artifact_digest,
                    error_code=None,
                    error_message=None,
                    updated_at=current,
                )
            )
            newest_applied = target_row["last_applied_lance_version"]
            planned_data_version = work_row["data_lance_version"]
            if (
                claim.kind == WorkKind.SERVE
                and newest_applied is not None
                and planned_data_version is not None
                and int(newest_applied) > int(planned_data_version)
            ):
                self.enqueue_or_advance_serve(connection, claim.target_id, int(newest_applied), current)
            return True

    def retention_floor(self) -> RowMapping | None:
        """Return the oldest source window still holding Iceberg retention.

        Returns:
            Oldest non-complete source row, or ``None`` when all windows are complete.
        """
        with self.engine.connect() as connection:
            return (
                connection.execute(
                    source_windows.select()
                    .where(source_windows.c.state != SourceWindowState.COMPLETE.value)
                    .order_by(source_windows.c.window_seq)
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )

    def latest_source_window(self, table_uuid: uuid.UUID) -> RowMapping | None:
        """Return the newest durable audit-tip row for one Iceberg table.

        Args:
            table_uuid: Stable Iceberg table identity.

        Returns:
            Newest source-window row or ``None`` before initial planning.
        """
        with self.engine.connect() as connection:
            return (
                connection.execute(
                    source_windows.select()
                    .where(source_windows.c.table_uuid == table_uuid)
                    .order_by(source_windows.c.window_seq.desc())
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )

    def control_plane_status(self, now: datetime | None = None) -> ControlPlaneStatus:
        """Read bounded queue and retention state for reconciliation SLOs.

        Args:
            now: Deterministic clock override for tests.

        Returns:
            Constant-size aggregate status with no target identifiers.
        """
        current = now or utc_now()
        open_states = (WorkState.PENDING.value, WorkState.RUNNING.value, WorkState.RETRY_WAIT.value)
        with self.engine.connect() as connection:
            grouped_rows = connection.execute(
                sa.select(target_work.c.state, sa.func.count()).group_by(target_work.c.state)
            ).all()
            counts: dict[str, int] = {str(state): int(count) for state, count in grouped_rows}
            due_work = int(
                connection.scalar(
                    sa.select(sa.func.count()).select_from(target_work).where(self.due_predicate(current))
                )
                or 0
            )
            blocked_source_windows = int(
                connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_windows)
                    .where(source_windows.c.state == SourceWindowState.BLOCKED.value)
                )
                or 0
            )
            oldest_open_work_at = connection.scalar(
                sa.select(sa.func.min(target_work.c.created_at)).where(target_work.c.state.in_(open_states))
            )
            floor = (
                connection.execute(
                    source_windows.select()
                    .where(source_windows.c.state != SourceWindowState.COMPLETE.value)
                    .order_by(source_windows.c.window_seq)
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
        return ControlPlaneStatus(
            pending_work=counts.get(WorkState.PENDING.value, 0),
            running_work=counts.get(WorkState.RUNNING.value, 0),
            retry_wait_work=counts.get(WorkState.RETRY_WAIT.value, 0),
            blocked_work=counts.get(WorkState.BLOCKED.value, 0),
            due_work=due_work,
            blocked_source_windows=blocked_source_windows,
            oldest_open_work_at=oldest_open_work_at,
            retention_window_seq=int(floor["window_seq"]) if floor is not None else None,
            retention_snapshot_id=int(floor["snapshot_id"]) if floor is not None else None,
            retention_parent_snapshot_id=(
                int(floor["parent_snapshot_id"])
                if floor is not None and floor["parent_snapshot_id"] is not None
                else None
            ),
            retention_state=SourceWindowState(floor["state"]) if floor is not None else None,
            retention_created_at=floor["created_at"] if floor is not None else None,
        )

    def delete_completed_work(self, completed_before: datetime, limit: int) -> int:
        """Delete a bounded batch of completed work after external horizons permit it.

        Args:
            completed_before: Oldest timestamp still retained for audit and rollback.
            limit: Maximum rows to delete.

        Returns:
            Number of deleted rows.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        with self.engine.begin() as connection:
            candidates = (
                sa.select(target_work.c.work_id)
                .where(target_work.c.state == WorkState.SUCCEEDED.value)
                .where(target_work.c.updated_at < completed_before)
                .order_by(target_work.c.updated_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            result = connection.execute(target_work.delete().where(target_work.c.work_id.in_(candidates)))
            return int(result.rowcount or 0)
