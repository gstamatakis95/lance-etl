"""Bounded durable reconciler services shared by the CLI and scheduler."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from lance_etl.reconciler.config import DeploymentProfile
from lance_etl.reconciler.planning import EnqueueSummary, SourcePlanEnqueuer
from lance_etl.reconciler.results import DispatchSummary, ReconcileSummary, ResultKind, WorkResult
from lance_etl.source import SourcePlan
from lance_etl.state import ControlPlaneStatus, RoutingIdentity, WorkClaim, WorkPhase


class WorkRepository(Protocol):
    """Fenced state transitions required by target-work reconciliation."""

    def claim_due_work(self, limit: int, lease_duration: timedelta, now: datetime | None = None) -> list[WorkClaim]:
        """Claim a bounded target-disjoint batch.

        Args:
            limit: Maximum claims.
            lease_duration: Code-owned lease duration.
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

    def publish_serve(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        indexed_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Atomically publish and complete one exact SERVE or REBUILD result.

        Args:
            claim: Fenced work claim.
            candidate_lance_uri: Validated immutable candidate dataset URI.
            indexed_lance_version: Exact published version.
            artifact_manifest_uri: Immutable artifact manifest.
            artifact_digest: Frozen artifact digest.
            now: Optional deterministic clock.

        Returns:
            Whether the current fence accepted the result.
        """
        ...

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
        """Advance a current claim to a later durable phase.

        Args:
            claim: Fenced work claim.
            phase: Later phase.
            data_lance_version: Optional exact data version.
            indexed_lance_version: Optional exact index version.
            candidate_lance_uri: Optional immutable candidate URI.
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
        now: datetime | None = None,
    ) -> bool:
        """Persist a transient failure for unlimited durable retry.

        Args:
            claim: Fenced work claim.
            delay: Code-owned retry delay.
            error_code: Bounded failure classification.
            error_message: Bounded diagnostic.
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

    def enqueue_rollback(self, identity: RoutingIdentity, successful_work_id: uuid.UUID) -> uuid.UUID:
        """Enqueue exact retained publication evidence for fenced PREWARM.

        Args:
            identity: Validated target routing identity.
            successful_work_id: Retained successful publication identity.

        Returns:
            New rollback work identity.
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
    """Execute one target-scoped fenced claim without owning state transitions."""

    def execute(self, claim: WorkClaim) -> WorkResult:
        """Execute one exact claim and return durable evidence.

        Args:
            claim: Fenced target work.

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
    """Emit low-cardinality reconciler health without target identifiers."""

    def emit(self, status: SloStatus) -> None:
        """Emit one evaluated status.

        Args:
            status: Low-cardinality SLO evaluation.
        """
        ...


@dataclass(frozen=True, slots=True)
class ResultReconciler:
    """Apply typed worker outcomes through lease-token and target-fence checks."""

    repository: WorkRepository
    profile: DeploymentProfile

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
        if result.kind is ResultKind.SERVE_SUCCEEDED:
            if not self.advance_serve_phases(result, now):
                return False
            return self.repository.publish_serve(
                result.claim,
                required_str(result.candidate_lance_uri),
                required_int(result.indexed_lance_version),
                required_str(result.artifact_manifest_uri),
                required_bytes(result.artifact_digest),
                now,
            )
        if result.kind is ResultKind.PHASE_ADVANCED:
            advanced = self.repository.advance_phase(
                result.claim,
                required_phase(result.next_phase),
                indexed_lance_version=result.indexed_lance_version,
                candidate_lance_uri=result.candidate_lance_uri,
                artifact_manifest_uri=result.artifact_manifest_uri,
                artifact_digest=result.artifact_digest,
                now=now,
            )
            if not advanced:
                return False
            return self.repository.retry_work(result.claim, timedelta(0), "PHASE_CHECKPOINTED", "", now)
        if result.kind is ResultKind.RETRY:
            return self.repository.retry_work(
                result.claim,
                self.profile.retry_delay(result.claim.attempt_count),
                required_str(result.error_code),
                result.error_message or "",
                now,
            )
        return self.repository.block_work(
            result.claim,
            required_str(result.error_code),
            result.error_message or "",
            now,
        )

    def advance_serve_phases(self, result: WorkResult, now: datetime | None) -> bool:
        """Checkpoint completed fixed phases before atomic publication.

        Args:
            result: Fully qualified serving result.
            now: Optional deterministic transaction clock.

        Returns:
            Whether every required phase checkpoint retained the live fence.
        """
        phases = (WorkPhase.MAINTAIN, WorkPhase.INDEX, WorkPhase.VALIDATE, WorkPhase.PREWARM)
        current_index = phases.index(result.claim.phase)
        for phase in phases[current_index + 1 :]:
            accepted = self.repository.advance_phase(
                result.claim,
                phase,
                indexed_lance_version=result.indexed_lance_version,
                candidate_lance_uri=result.candidate_lance_uri,
                artifact_manifest_uri=result.artifact_manifest_uri,
                artifact_digest=result.artifact_digest,
                now=now,
            )
            if not accepted:
                return False
        return True


@dataclass(frozen=True, slots=True)
class BoundedDispatcher:
    """Drain a code-owned number of target-disjoint claims with failure isolation."""

    repository: WorkRepository
    executor: WorkExecutor
    results: ResultReconciler
    profile: DeploymentProfile

    def run(self) -> DispatchSummary:
        """Claim, execute, and reconcile a bounded amount of due work.

        Returns:
            Constant-size outcome counts.
        """
        counts = {kind: 0 for kind in ResultKind}
        stale = 0
        claimed = 0
        for batch_number in range(self.profile.max_drain_batches):
            del batch_number
            claims = self.repository.claim_due_work(self.profile.claim_batch_size, self.profile.lease_duration)
            if not claims:
                break
            claimed += len(claims)
            for claim in claims:
                result = execute_isolated(self.executor, claim)
                counts[result.kind] += 1
                if not self.results.reconcile(result):
                    stale += 1
            if len(claims) < self.profile.claim_batch_size:
                break
        return DispatchSummary(
            claimed=claimed,
            succeeded=counts[ResultKind.INGEST_SUCCEEDED] + counts[ResultKind.SERVE_SUCCEEDED],
            advanced=counts[ResultKind.PHASE_ADVANCED],
            retried=counts[ResultKind.RETRY],
            blocked=counts[ResultKind.BLOCKED],
            stale=stale,
        )


def execute_isolated(executor: WorkExecutor, claim: WorkClaim) -> WorkResult:
    """Convert an unexpected target-local exception into durable retry state.

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
    window_seq: int | None
    retain_snapshot_id: int | None
    state: str | None


@dataclass(frozen=True, slots=True)
class SloStatus:
    """Low-cardinality reconciler health evaluation."""

    healthy: bool
    reasons: tuple[str, ...]
    due_work: int
    blocked_work: int
    blocked_source_windows: int
    oldest_open_age_seconds: float
    retention_age_seconds: float


def retention_decision(status: ControlPlaneStatus) -> RetentionDecision:
    """Derive the exact snapshot floor held by unfinished source application.

    Args:
        status: Bounded control-plane snapshot.

    Returns:
        Floor decision that never treats SERVE work as a source dependency.
    """
    if status.retention_window_seq is None:
        return RetentionDecision(False, None, None, None)
    retain_snapshot_id = status.retention_parent_snapshot_id or status.retention_snapshot_id
    return RetentionDecision(
        True,
        status.retention_window_seq,
        retain_snapshot_id,
        status.retention_state.value if status.retention_state is not None else None,
    )


def evaluate_slo(status: ControlPlaneStatus, profile: DeploymentProfile, now: datetime | None = None) -> SloStatus:
    """Evaluate fixed queue and retention thresholds.

    Args:
        status: Bounded control-plane snapshot.
        profile: Release-owned thresholds.
        now: Optional deterministic clock.

    Returns:
        Low-cardinality health result.
    """
    current = now or datetime.now(UTC)
    oldest_age = age_seconds(status.oldest_open_work_at, current)
    retention_age = age_seconds(status.retention_created_at, current)
    reasons: list[str] = []
    if status.blocked_work > 0:
        reasons.append("blocked_work")
    if status.blocked_source_windows > 0:
        reasons.append("blocked_source_window")
    if status.due_work > profile.max_due_work:
        reasons.append("due_queue_over_budget")
    if oldest_age > profile.max_open_work_age.total_seconds():
        reasons.append("open_work_age_over_budget")
    if retention_age > profile.max_retention_age.total_seconds():
        reasons.append("source_retention_age_over_budget")
    return SloStatus(
        healthy=not reasons,
        reasons=tuple(reasons),
        due_work=status.due_work,
        blocked_work=status.blocked_work,
        blocked_source_windows=status.blocked_source_windows,
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
    """Five idempotent scheduler actions over durable source and target state."""

    plan_provider: SourcePlanProvider
    plan_enqueuer: SourcePlanEnqueuer
    dispatcher: BoundedDispatcher
    result_sweep: ExternalResultSweep
    repository: WorkRepository
    slo_emitter: SloEmitter
    profile: DeploymentProfile

    def plan_and_enqueue_window(self) -> EnqueueSummary:
        """Plan a pinned source prefix and enqueue each window atomically.

        Returns:
            Durable enqueue summary.
        """
        return self.plan_enqueuer.enqueue(self.plan_provider.plan())

    def run_due_target_work(self) -> DispatchSummary:
        """Drain a bounded amount of due work.

        Returns:
            Target-isolated dispatch summary.
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
        status = evaluate_slo(self.repository.control_plane_status(), self.profile)
        self.slo_emitter.emit(status)
        return status

    def repair_blocked_work(self, work_id: uuid.UUID, dry_run: bool) -> bool:
        """Retry one explicit blocked work identity without changing source ownership.

        Args:
            work_id: Existing durable work id selected by an operator.
            dry_run: Validate without mutating state.

        Returns:
            True for a dry run or when the blocked row transitioned.
        """
        return True if dry_run else self.repository.retry_blocked_work(work_id)

    def repair_rollback(
        self,
        identity: RoutingIdentity,
        retained_work_id: uuid.UUID,
        dry_run: bool,
    ) -> uuid.UUID | None:
        """Enqueue one exact retained publication without directly mutating the catalog.

        Args:
            identity: Fully specified validated target identity.
            retained_work_id: Successful serving work carrying retained evidence.
            dry_run: Validate input without enqueuing work.

        Returns:
            New rollback work id, or ``None`` for a dry run.
        """
        identity.validate()
        return None if dry_run else self.repository.enqueue_rollback(identity, retained_work_id)


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


def required_phase(value: WorkPhase | None) -> WorkPhase:
    """Narrow a result field already validated as a phase.

    Args:
        value: Optional phase.

    Returns:
        Required phase.
    """
    if value is None:
        raise ValueError("required phase result field is absent")
    return value
