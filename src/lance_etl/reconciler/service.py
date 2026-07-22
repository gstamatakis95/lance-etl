"""Bounded durable reconciler services used by the local process."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from lance_etl.reconciler.config import ReconcilerSettings
from lance_etl.reconciler.planning import EnqueueSummary, SourcePlanEnqueuer
from lance_etl.reconciler.results import DispatchSummary, ReconcileSummary, ResultKind, WorkResult
from lance_etl.source import SourcePlan
from lance_etl.state import (
    ControlPlaneStatus,
    PublicationEvidence,
    RoutingIdentity,
    SourceSnapshotState,
    StateTransitionError,
    WorkClaim,
    WorkPhase,
)

logger: logging.Logger = logging.getLogger(__name__)


class WorkRepository(Protocol):
    """Fenced state transitions required by dataset-work reconciliation."""

    def claim_due_work(
        self,
        limit: int,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> list[WorkClaim]:
        """Claim a bounded dataset-disjoint batch.

        Args:
            limit: Maximum claims.
            lease_duration: PostgreSQL-backed lease duration.
            now: Optional deterministic clock.

        Returns:
            Fenced claims.
        """
        ...

    def complete_ingest(
        self,
        claim: WorkClaim,
        data_lance_version: int,
        source_row_count: int,
        source_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Complete one exact INGEST result.

        Args:
            claim: Fenced work claim.
            data_lance_version: Exact committed Lance version.
            source_row_count: Terminal source mutation count.
            source_digest: Frozen source digest.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

    def publish_dataset(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        indexed_lance_version: int,
        manifest_uri: str,
        manifest_digest: bytes,
        evidence: PublicationEvidence,
        now: datetime | None = None,
    ) -> bool:
        """Atomically publish and complete one exact dataset generation.

        Args:
            claim: Fenced work claim.
            candidate_lance_uri: Validated immutable candidate dataset URI.
            indexed_lance_version: Exact published version.
            manifest_uri: Immutable artifact manifest.
            manifest_digest: Frozen artifact digest.
            evidence: Exact schema, cardinality, fragment, and index evidence.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

    def advance_phase(
        self,
        claim: WorkClaim,
        phase: WorkPhase,
        candidate_lance_uri: str | None = None,
        candidate_lance_version: int | None = None,
        artifact_manifest_uri: str | None = None,
        artifact_digest: bytes | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Advance a current claim to a later durable phase.

        Args:
            claim: Fenced work claim.
            phase: Later phase.
            candidate_lance_uri: Optional immutable candidate URI.
            candidate_lance_version: Optional exact candidate version.
            artifact_manifest_uri: Optional immutable artifact manifest.
            artifact_digest: Optional artifact digest.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

    def retry_work(
        self,
        claim: WorkClaim,
        delay: timedelta,
        error_code: str,
        error_message: str,
        max_attempts: int,
        now: datetime | None = None,
    ) -> bool:
        """Persist a transient failure for unlimited durable retry.

        Args:
            claim: Fenced work claim.
            delay: Code-owned retry delay.
            error_code: Bounded failure classification.
            error_message: Bounded diagnostic.
            max_attempts: Bootstrap-configured maximum durable attempts before blocking.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

    def block_work(
        self,
        claim: WorkClaim,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        """Persist a contract failure without retrying automatically.

        Args:
            claim: Fenced work claim.
            error_code: Bounded failure classification.
            error_message: Bounded diagnostic.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

    def retry_blocked_work(self, work_id: uuid.UUID, now: datetime | None = None) -> bool:
        """Return one explicitly selected blocked identity to pending.

        Args:
            work_id: Existing durable work identity.
            now: Optional deterministic clock.

        Returns:
            Whether a blocked row was retried.
        """
        ...

    def enqueue_rebuild(self, identity: RoutingIdentity, request_id: uuid.UUID) -> uuid.UUID:
        """Enqueue one idempotent canonical duplicate-recovery generation.

        Args:
            identity: Validated dataset routing identity.
            request_id: Stable operator idempotency identity.

        Returns:
            Deterministic rebuild work identity.
        """
        ...

    def control_plane_status(self, now: datetime | None = None) -> ControlPlaneStatus:
        """Read bounded queue and retention status.

        Args:
            now: Optional deterministic clock.

        Returns:
            Constant-size control-plane status.
        """
        ...


class SourcePlanProvider(Protocol):
    """Read-only provider of one plan pinned to an Iceberg head."""

    def plan(self) -> SourcePlan:
        """Build the next side-effect-free source plan.

        Returns:
            Pinned source plan.
        """
        ...


class WorkExecutor(Protocol):
    """Execute one dataset-scoped fenced claim without owning state transitions."""

    def execute(self, claim: WorkClaim) -> WorkResult:
        """Execute one exact claim and return durable evidence.

        Args:
            claim: Fenced dataset work.

        Returns:
            Typed result for repository reconciliation.
        """
        ...


class ExternalResultSweep(Protocol):
    """Reconcile ambiguous outcomes from an external execution substrate."""

    def reconcile(self) -> ReconcileSummary:
        """Reconcile a bounded batch of already submitted outcomes.

        Returns:
            Bounded sweep summary.
        """
        ...


class SloEmitter(Protocol):
    """Emit low-cardinality reconciler health without dataset identifiers."""

    def emit(self, status: SloStatus) -> None:
        """Emit one evaluated status.

        Args:
            status: Low-cardinality SLO evaluation.
        """
        ...


@dataclass(frozen=True, slots=True)
class ResultReconciler:
    """Apply typed worker outcomes through lease-token and dataset-fence checks."""

    repository: WorkRepository
    settings: ReconcilerSettings

    def reconcile(self, result: WorkResult, now: datetime | None = None) -> bool:
        """Persist one result idempotently or reject its stale fence.

        Args:
            result: Validated worker outcome.
            now: Optional deterministic clock.

        Returns:
            Whether the current durable fence accepted the transition.
        """
        result.validate()
        if result.kind is ResultKind.INGEST_SUCCEEDED:
            return self.repository.complete_ingest(
                result.claim,
                required_int(result.data_lance_version),
                required_int(result.source_row_count),
                required_bytes(result.source_digest),
                now,
            )
        if result.kind is ResultKind.PUBLISH_SUCCEEDED:
            if not self.advance_publish_phases(result, now):
                return False
            return self.repository.publish_dataset(
                result.claim,
                required_str(result.candidate_lance_uri),
                required_int(result.indexed_lance_version),
                required_str(result.manifest_uri),
                required_bytes(result.manifest_digest),
                required_publication_evidence(result.publication_evidence),
                now,
            )
        if result.kind is ResultKind.RETRY:
            return self.repository.retry_work(
                result.claim,
                self.settings.retry_delay(result.claim.attempt_count, result.claim.work_id),
                required_str(result.error_code),
                result.error_message or "",
                self.settings.max_attempts,
                now,
            )
        return self.repository.block_work(
            result.claim,
            required_str(result.error_code),
            result.error_message or "",
            now,
        )

    def advance_publish_phases(self, result: WorkResult, now: datetime | None) -> bool:
        """Checkpoint completed fixed phases before atomic publication.

        Args:
            result: Fully qualified serving result.
            now: Optional deterministic transaction clock.

        Returns:
            Whether every required phase checkpoint retained the live fence.
        """
        phases: tuple[WorkPhase, ...] = (
            WorkPhase.COMPACT,
            WorkPhase.INDEX,
            WorkPhase.VALIDATE,
            WorkPhase.PREWARM,
        )
        current_index: int = phases.index(result.claim.phase)
        phase: WorkPhase
        for phase in phases[current_index + 1 :]:
            accepted: bool = self.repository.advance_phase(
                result.claim,
                phase,
                candidate_lance_uri=result.candidate_lance_uri,
                candidate_lance_version=result.indexed_lance_version,
                artifact_manifest_uri=result.manifest_uri,
                artifact_digest=result.manifest_digest,
                now=now,
            )
            if not accepted:
                return False
        return True


@dataclass(frozen=True, slots=True)
class BoundedDispatcher:
    """Drain a bounded number of dataset-disjoint claims with failure isolation."""

    repository: WorkRepository
    executor: WorkExecutor
    results: ResultReconciler
    settings: ReconcilerSettings

    def run(self) -> DispatchSummary:
        """Claim, execute, and reconcile a bounded amount of due work.

        Result reconciliation runs inside the same per-claim failure-isolation boundary as
        execution: a divergent replay (``StateTransitionError`` from the repository) or any other
        unexpected reconciliation exception blocks the offending claim on a best-effort basis and
        moves on to the next claim, rather than escaping and abandoning the rest of the batch.

        Returns:
            Constant-size outcome counts.
        """
        counts: dict[ResultKind, int] = {kind: 0 for kind in ResultKind}
        stale: int = 0
        diverged: int = 0
        claimed: int = 0
        batch_number: int
        for batch_number in range(self.settings.max_drain_batches):
            del batch_number
            claims: list[WorkClaim] = self.repository.claim_due_work(
                self.settings.claim_batch_size,
                self.settings.lease_duration,
            )
            if not claims:
                break
            claimed += len(claims)
            claim: WorkClaim
            for claim in claims:
                result: WorkResult = execute_isolated(self.executor, claim)
                exc: StateTransitionError | Exception
                try:
                    accepted: bool = self.results.reconcile(result)
                except StateTransitionError as exc:
                    diverged += 1
                    self.block_diverged_claim(claim, exc)
                    continue
                except Exception as exc:
                    diverged += 1
                    logger.warning(
                        "reconciler: result reconciliation raised for claim %s, isolating: %s",
                        claim.work_id,
                        exc,
                    )
                    continue
                counts[result.kind] += 1
                if not accepted:
                    stale += 1
            if len(claims) < self.settings.claim_batch_size:
                break
        return DispatchSummary(
            claimed=claimed,
            succeeded=counts[ResultKind.INGEST_SUCCEEDED] + counts[ResultKind.PUBLISH_SUCCEEDED],
            advanced=0,
            retried=counts[ResultKind.RETRY],
            blocked=counts[ResultKind.BLOCKED] + diverged,
            stale=stale,
        )

    def block_diverged_claim(self, claim: WorkClaim, exc: Exception) -> None:
        """Best-effort block one claim whose result reconciliation found a state divergence.

        The block attempt itself may return ``False`` if the fence has already moved on (for
        example a concurrent reclaim after lease expiry); that is tolerated silently since the
        durable row is no longer this dispatcher's concern either way.

        Args:
            claim: The fenced claim whose reconciliation raised.
            exc: The divergence exception the repository raised.
        """
        try:
            self.repository.block_work(claim, "STATE_DIVERGENCE", str(exc))
        except Exception as block_exc:
            logger.warning(
                "reconciler: best-effort block failed for diverged claim %s: %s",
                claim.work_id,
                block_exc,
            )


def execute_isolated(executor: WorkExecutor, claim: WorkClaim) -> WorkResult:
    """Convert an unexpected dataset-local exception into durable retry state.

    Args:
        executor: Target work executor.
        claim: Fenced claim.

    Returns:
        Worker result or a retry result for an unexpected exception.
    """
    try:
        return executor.execute(claim)
    except Exception as exc:
        return WorkResult(
            claim=claim,
            kind=ResultKind.RETRY,
            error_code="UNEXPECTED_WORKER_FAILURE",
            error_message=str(exc),
        )


@dataclass(frozen=True, slots=True)
class RetentionDecision:
    """Exact Iceberg snapshot floor that expiration must preserve."""

    retention_held: bool
    source_snapshot_seq: int | None
    retain_snapshot_id: int | None
    state: str | None


@dataclass(frozen=True, slots=True)
class SloStatus:
    """Low-cardinality reconciler health evaluation."""

    healthy: bool
    reasons: tuple[str, ...]
    due_work: int
    blocked_work: int
    blocked_source_snapshots: int
    oldest_open_age_seconds: float
    retention_age_seconds: float


@dataclass(frozen=True, slots=True)
class RunOnceSummary:
    """Typed result of one complete local reconciliation cycle."""

    planning: EnqueueSummary
    dispatch: DispatchSummary
    reconciliation: ReconcileSummary
    retention: RetentionDecision
    slo: SloStatus


def no_shutdown() -> None:
    """Provide a no-op shutdown callback for injected test applications."""


def retention_decision(status: ControlPlaneStatus) -> RetentionDecision:
    """Derive the exact snapshot floor held by unfinished source application.

    Args:
        status: Bounded control-plane snapshot.

    Returns:
        Floor decision that never treats publication work as a source dependency.
    """
    if status.retention_source_snapshot_seq is None:
        return RetentionDecision(False, None, None, None)
    retain_snapshot_id: int | None = status.retention_parent_snapshot_id or status.retention_snapshot_id
    return RetentionDecision(
        True,
        status.retention_source_snapshot_seq,
        retain_snapshot_id,
        status.retention_state.value if status.retention_state is not None else None,
    )


def evaluate_slo(
    status: ControlPlaneStatus,
    settings: ReconcilerSettings,
    now: datetime | None = None,
) -> SloStatus:
    """Evaluate fixed queue and retention thresholds.

    Args:
        status: Bounded control-plane snapshot.
        settings: PostgreSQL-backed operational thresholds.
        now: Optional deterministic clock.

    Returns:
        Low-cardinality health result.
    """
    current: datetime = now or datetime.now(UTC)
    oldest_age: float = age_seconds(status.oldest_open_work_at, current)
    retention_age: float = age_seconds(status.retention_created_at, current)
    reasons: list[str] = []
    if status.blocked_work > 0:
        reasons.append("blocked_work")
    if status.blocked_source_snapshots > 0:
        reasons.append("blocked_source_snapshot")
    if status.due_work > settings.max_due_work:
        reasons.append("due_queue_over_budget")
    if oldest_age > settings.max_open_work_age.total_seconds():
        reasons.append("open_work_age_over_budget")
    if (
        status.retention_state != SourceSnapshotState.COMPLETE
        and retention_age > settings.max_retention_age.total_seconds()
    ):
        reasons.append("source_retention_age_over_budget")
    return SloStatus(
        healthy=not reasons,
        reasons=tuple(reasons),
        due_work=status.due_work,
        blocked_work=status.blocked_work,
        blocked_source_snapshots=status.blocked_source_snapshots,
        oldest_open_age_seconds=oldest_age,
        retention_age_seconds=retention_age,
    )


def age_seconds(value: datetime | None, now: datetime) -> float:
    """Return a non-negative age for an optional durable timestamp.

    Args:
        value: Stored timestamp or ``None``.
        now: Evaluation time.

    Returns:
        Age in seconds, with absent and future values clamped to zero.
    """
    if value is None:
        return 0.0
    return max(0.0, (now - value).total_seconds())


@dataclass(frozen=True, slots=True)
class ReconcilerApplication:
    """One-process application over durable source and dataset state."""

    plan_provider: SourcePlanProvider
    plan_enqueuer: SourcePlanEnqueuer
    dispatcher: BoundedDispatcher
    result_sweep: ExternalResultSweep
    repository: WorkRepository
    slo_emitter: SloEmitter
    settings: ReconcilerSettings
    shutdown: Callable[[], None] = no_shutdown

    def close(self) -> None:
        """Release process-owned Spark and PostgreSQL resources."""
        self.shutdown()

    def plan_and_enqueue_snapshots(self) -> EnqueueSummary:
        """Classify and enqueue a bounded source backlog one snapshot at a time.

        Returns:
            Durable enqueue summary.
        """
        pinned_head: int | None = None
        planned: int = 0
        enqueued: int = 0
        sequences: list[int] = []
        truncated: bool = False
        planner_pass: int
        for planner_pass in range(self.settings.max_snapshots_per_plan):
            del planner_pass
            summary: EnqueueSummary = self.plan_enqueuer.enqueue(self.plan_provider.plan())
            pinned_head = summary.pinned_head_snapshot_id
            planned += summary.planned_snapshots
            enqueued += summary.enqueued_snapshots
            sequences.extend(summary.source_snapshot_sequences)
            truncated = truncated or summary.truncated
            if summary.enqueued_snapshots == 0 or summary.truncated:
                break
        else:
            truncated = True
        return EnqueueSummary(pinned_head, planned, enqueued, tuple(sequences), truncated)

    def run_due_dataset_work(self) -> DispatchSummary:
        """Drain a bounded amount of due work.

        Returns:
            Dataset-isolated dispatch summary.
        """
        return self.dispatcher.run()

    def reconcile_results(self) -> ReconcileSummary:
        """Reconcile ambiguous externally submitted outcomes.

        Returns:
            Bounded sweep summary.
        """
        return self.result_sweep.reconcile()

    def gate_source_retention(self) -> RetentionDecision:
        """Return the exact unfinished source floor that expiration must preserve.

        Returns:
            Retention decision.
        """
        return retention_decision(self.repository.control_plane_status())

    def emit_slo_status(self) -> SloStatus:
        """Evaluate and emit low-cardinality reconciler health.

        Returns:
            Emitted SLO status.
        """
        status: SloStatus = evaluate_slo(self.repository.control_plane_status(), self.settings)
        self.slo_emitter.emit(status)
        return status

    def run_once(self) -> RunOnceSummary:
        """Execute one complete reconciliation cycle in dependency order.

        Returns:
            Planning, work, reconciliation, retention, and SLO results.
        """
        planning: EnqueueSummary = self.plan_and_enqueue_snapshots()
        dispatch: DispatchSummary = self.run_due_dataset_work()
        reconciliation: ReconcileSummary = self.reconcile_results()
        retention: RetentionDecision = self.gate_source_retention()
        slo: SloStatus = self.emit_slo_status()
        return RunOnceSummary(planning, dispatch, reconciliation, retention, slo)

    def repair_blocked_work(self, work_id: uuid.UUID, dry_run: bool) -> bool:
        """Retry one explicit blocked work identity without changing source ownership.

        Args:
            work_id: Existing durable work id selected by an operator.
            dry_run: Validate without mutating state.

        Returns:
            True for a dry run or when the blocked row transitioned.
        """
        return True if dry_run else self.repository.retry_blocked_work(work_id)

    def repair_rebuild(
        self,
        identity: RoutingIdentity,
        request_id: uuid.UUID,
        dry_run: bool,
    ) -> uuid.UUID | None:
        """Enqueue canonical duplicate recovery without directly mutating a catalog.

        Args:
            identity: Fully specified validated dataset identity.
            request_id: Stable operator-issued idempotency identity.
            dry_run: Validate input without enqueuing work.

        Returns:
            Deterministic rebuild work id, or ``None`` for a dry run.
        """
        identity.validate()
        return None if dry_run else self.repository.enqueue_rebuild(identity, request_id)


@dataclass(frozen=True, slots=True)
class ReconcilerOperator:
    """PostgreSQL-only status and repair surface that never starts Spark."""

    repository: WorkRepository
    slo_emitter: SloEmitter
    settings: ReconcilerSettings
    shutdown: Callable[[], None] = no_shutdown

    def close(self) -> None:
        """Release process-owned PostgreSQL resources."""
        self.shutdown()

    def emit_slo_status(self) -> SloStatus:
        """Evaluate and emit low-cardinality control-plane health.

        Returns:
            Emitted SLO status.
        """
        status: SloStatus = evaluate_slo(self.repository.control_plane_status(), self.settings)
        self.slo_emitter.emit(status)
        return status

    def repair_blocked_work(self, work_id: uuid.UUID, dry_run: bool) -> bool:
        """Retry one explicit blocked identity without starting Spark.

        Args:
            work_id: Existing durable work identity.
            dry_run: Validate without mutating state.

        Returns:
            True for a dry run or when the blocked row transitioned.
        """
        return True if dry_run else self.repository.retry_blocked_work(work_id)

    def repair_rebuild(
        self,
        identity: RoutingIdentity,
        request_id: uuid.UUID,
        dry_run: bool,
    ) -> uuid.UUID | None:
        """Enqueue canonical duplicate recovery without starting Spark.

        Args:
            identity: Fully specified validated dataset identity.
            request_id: Stable operator idempotency identity.
            dry_run: Validate input without enqueuing work.

        Returns:
            Deterministic rebuild work identity, or ``None`` for a dry run.
        """
        identity.validate()
        return None if dry_run else self.repository.enqueue_rebuild(identity, request_id)


def required_int(value: int | None) -> int:
    """Narrow a result field already validated as an integer.

    Args:
        value: Optional integer.

    Returns:
        Required integer.
    """
    if value is None:
        raise ValueError("required integer result field is absent")
    return value


def required_bytes(value: bytes | None) -> bytes:
    """Narrow a result field already validated as bytes.

    Args:
        value: Optional bytes.

    Returns:
        Required bytes.
    """
    if value is None:
        raise ValueError("required bytes result field is absent")
    return value


def required_str(value: str | None) -> str:
    """Narrow a result field already validated as a non-empty string.

    Args:
        value: Optional string.

    Returns:
        Required string.
    """
    if not value:
        raise ValueError("required string result field is absent")
    return value


def required_publication_evidence(value: PublicationEvidence | None) -> PublicationEvidence:
    """Narrow a result field already validated as publication evidence.

    Args:
        value: Optional publication evidence.

    Returns:
        Required publication evidence.
    """
    if value is None:
        raise ValueError("required publication evidence result field is absent")
    return value
