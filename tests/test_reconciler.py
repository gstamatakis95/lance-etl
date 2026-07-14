"""Unit tests for durable bounded reconciliation and minimal configuration."""

from __future__ import annotations

import argparse
import time
import tomllib
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from lance_etl.reconciler import (
    BoundedDispatcher,
    DeploymentProfile,
    ReconcilerApplication,
    ResultKind,
    ResultReconciler,
    SourcePlanEnqueuer,
    WorkResult,
    evaluate_slo,
    retention_decision,
)
from lance_etl.reconciler.cli import SCHEDULED_PHASES, build_parser, execute_command, main
from lance_etl.reconciler.config import RuntimeSettings
from lance_etl.reconciler.iceberg import DurableSourcePlanProvider
from lance_etl.reconciler.results import ReconcileSummary
from lance_etl.reconciler.workers import FencedWorkExecutor, ProfiledServeRunner, required_indexes
from lance_etl.source import (
    BaselineProof,
    PartitionSpec,
    SnapshotRecord,
    SourcePlan,
    TableMetadata,
    TargetKey,
    TouchedTarget,
    WindowKind,
    WindowPlan,
)
from lance_etl.state import (
    ControlPlaneStatus,
    RoutingIdentity,
    SourceWindowKind,
    SourceWindowState,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
)
from lance_etl.telemetry import TelemetryConfig


def work_claim(
    kind: WorkKind = WorkKind.INGEST,
    phase: WorkPhase = WorkPhase.INGEST,
    attempt_count: int = 1,
) -> WorkClaim:
    """Return one fenced target-work claim.

    Args:
        kind: Work kind.
        phase: Current durable phase.
        attempt_count: Durable claim attempt.

    Returns:
        Claim fixture.
    """
    return WorkClaim(
        work_id=uuid.uuid4(),
        target_id=uuid.uuid4(),
        kind=kind,
        phase=phase,
        lease_token=uuid.uuid4(),
        fence_epoch=4,
        attempt_count=attempt_count,
        source_window_seq=3 if kind is WorkKind.INGEST else None,
        expected_ingest_lance_uri="s3://lance/target.lance",
        data_lance_version=7 if kind is not WorkKind.INGEST else None,
    )


def control_status(
    *,
    pending: int = 0,
    running: int = 0,
    retry_wait: int = 0,
    blocked: int = 0,
    due: int = 0,
    blocked_sources: int = 0,
    oldest_open: datetime | None = None,
    retention_window: int | None = None,
    retention_snapshot: int | None = None,
    retention_parent: int | None = None,
    retention_state: SourceWindowState | None = None,
    retention_created: datetime | None = None,
) -> ControlPlaneStatus:
    """Return one bounded control-plane status fixture.

    Args:
        pending: Pending work count.
        running: Running work count.
        retry_wait: Retrying work count.
        blocked: Blocked work count.
        due: Currently due count.
        blocked_sources: Blocked source-window count.
        oldest_open: Oldest open work timestamp.
        retention_window: Retention-floor window sequence.
        retention_snapshot: Retention-floor snapshot.
        retention_parent: Parent required by exact incremental scan.
        retention_state: Durable floor state.
        retention_created: Floor creation timestamp.

    Returns:
        Status fixture.
    """
    return ControlPlaneStatus(
        pending_work=pending,
        running_work=running,
        retry_wait_work=retry_wait,
        blocked_work=blocked,
        due_work=due,
        blocked_source_windows=blocked_sources,
        oldest_open_work_at=oldest_open,
        retention_window_seq=retention_window,
        retention_snapshot_id=retention_snapshot,
        retention_parent_snapshot_id=retention_parent,
        retention_state=retention_state,
        retention_created_at=retention_created,
    )


def source_plan(window_count: int = 2) -> SourcePlan:
    """Build a pinned source plan with deterministic distinct targets.

    Args:
        window_count: Number of append windows.

    Returns:
        Source plan fixture.
    """
    table_uuid = str(uuid.uuid4())
    windows: list[WindowPlan] = []
    for index in range(window_count):
        snapshot_id = index + 2
        snapshot = SnapshotRecord(table_uuid, snapshot_id, snapshot_id - 1, snapshot_id, 1000, "append", 7)
        target = TargetKey(f"tenant{index}", "vectors", "org1")
        windows.append(
            WindowPlan(
                snapshot=snapshot,
                kind=WindowKind.APPEND,
                touched_targets=(TouchedTarget(target, (1, 9)),),
                scans=(),
            )
        )
    return SourcePlan(table_uuid, window_count + 1, 7, tuple(windows))


def test_source_plan_enqueuer_maps_windows_and_release_profile() -> None:
    """Source windows and manifest targets reach one atomic repository call per snapshot."""
    repository = MagicMock()
    repository.enqueue_source_window.side_effect = [11, 12]
    profile = DeploymentProfile(profile_id="prod_2026_07")
    summary = SourcePlanEnqueuer(repository, profile).enqueue(source_plan())
    assert summary.window_sequences == (11, 12)
    assert summary.enqueued_windows == 2
    first_state_plan, first_targets = repository.enqueue_source_window.call_args_list[0].args
    assert first_state_plan.snapshot_id == 2
    assert first_state_plan.parent_snapshot_id == 1
    assert first_state_plan.iceberg_sequence_number == 2
    assert first_targets[0].profile_id == "prod_2026_07"
    assert first_targets[0].identity.tenant_id == "tenant0"


def test_source_plan_enqueuer_has_code_owned_window_bound() -> None:
    """One planner task cannot enqueue an unbounded lineage catch-up."""
    repository = MagicMock()
    repository.enqueue_source_window.side_effect = range(100)
    profile = replace(DeploymentProfile(), max_windows_per_plan=2)
    summary = SourcePlanEnqueuer(repository, profile).enqueue(source_plan(5))
    assert summary.enqueued_windows == 2
    assert summary.planned_windows == 5
    assert summary.truncated
    assert repository.enqueue_source_window.call_count == 2


def test_initial_source_plan_uses_executor_qualified_baseline_proof(monkeypatch: pytest.MonkeyPatch) -> None:
    """An environment snapshot ID is never treated as canonical without a qualifier scan.

    Args:
        monkeypatch: Scoped planner-constructor replacement.
    """
    table_uuid = str(uuid.uuid4())
    metadata = TableMetadata(table_uuid, 9, PartitionSpec(7, ()))
    proof = BaselineProof(table_uuid, 9, 7, True, 0)
    catalog = MagicMock()
    catalog.table_metadata.return_value = metadata
    repository = MagicMock()
    repository.latest_source_window.return_value = None
    qualifier = MagicMock()
    qualifier.qualify.return_value = proof
    planner = MagicMock()
    expected = SourcePlan(table_uuid, 9, 7, ())
    planner.plan.return_value = expected
    monkeypatch.setattr("lance_etl.reconciler.iceberg.SourcePlanner", MagicMock(return_value=planner))
    provider = DurableSourcePlanProvider("prod.vectors.events", catalog, repository, 9, qualifier)
    assert provider.plan() == expected
    qualifier.qualify.assert_called_once_with("prod.vectors.events", metadata, 9)
    planner.plan.assert_called_once_with("prod.vectors.events", None, proof)


def test_ingest_success_reconciles_exact_completion_evidence() -> None:
    """INGEST completion carries exact version, count, and digest through the fence."""
    repository = MagicMock()
    repository.complete_ingest.return_value = True
    claim = work_claim()
    result = WorkResult(
        claim=claim,
        kind=ResultKind.INGEST_SUCCEEDED,
        data_lance_version=8,
        source_row_count=42,
        source_digest=b"a" * 32,
    )
    assert ResultReconciler(repository, DeploymentProfile()).reconcile(result)
    repository.complete_ingest.assert_called_once_with(claim, 8, 42, b"a" * 32, None)


def test_serve_success_requires_candidate_uri_and_uses_atomic_publish() -> None:
    """SERVE completion can only use the catalog-CAS publication transaction."""
    repository = MagicMock()
    repository.publish_serve.return_value = True
    claim = work_claim(WorkKind.SERVE, WorkPhase.PREWARM)
    result = WorkResult(
        claim=claim,
        kind=ResultKind.SERVE_SUCCEEDED,
        candidate_lance_uri="s3://candidates/target-generation.lance",
        indexed_lance_version=9,
        artifact_manifest_uri="s3://artifacts/manifest.json",
        artifact_digest=b"b" * 32,
    )
    assert ResultReconciler(repository, DeploymentProfile()).reconcile(result)
    repository.publish_serve.assert_called_once_with(
        claim,
        "s3://candidates/target-generation.lance",
        9,
        "s3://artifacts/manifest.json",
        b"b" * 32,
        None,
    )
    assert not repository.complete_serve.called


def test_serve_success_checkpoints_every_fixed_phase_before_publication() -> None:
    """A complete worker result reaches PREWARM under one fence before catalog publication."""
    repository = MagicMock()
    repository.advance_phase.return_value = True
    repository.publish_serve.return_value = True
    claim = work_claim(WorkKind.SERVE, WorkPhase.MAINTAIN)
    result = WorkResult(
        claim=claim,
        kind=ResultKind.SERVE_SUCCEEDED,
        candidate_lance_uri="s3://candidates/target-generation.lance",
        indexed_lance_version=9,
        artifact_manifest_uri="s3://artifacts/manifest.json",
        artifact_digest=b"b" * 32,
    )
    assert ResultReconciler(repository, DeploymentProfile()).reconcile(result)
    assert [call.args[1] for call in repository.advance_phase.call_args_list] == [
        WorkPhase.INDEX,
        WorkPhase.VALIDATE,
        WorkPhase.PREWARM,
    ]
    assert repository.publish_serve.call_count == 1


def test_release_profile_declares_and_qualifies_every_required_index(monkeypatch: pytest.MonkeyPatch) -> None:
    """Publication evidence isolates IVF, text, tombstone, and time-index coverage.

    Args:
        monkeypatch: Scoped Lance dataset replacement.
    """
    profile = DeploymentProfile()
    declarations = required_indexes(profile)
    assert {kind for kind, column, name in declarations} == {"IVF_RQ", "INVERTED", "BITMAP", "BTREE", "ZONEMAP"}
    dataset = MagicMock()
    dataset.version = 11
    dataset.schema = pa.schema(
        [
            pa.field("vector_id", pa.string()),
            pa.field("event_timestamp", pa.timestamp("us")),
            pa.field("vector", pa.list_(pa.float32(), 128)),
            pa.field("text", pa.string()),
            pa.field("cluster", pa.string()),
            pa.field("ttl", pa.int64()),
            pa.field("lance_etl_window_seq", pa.int64()),
            pa.field("lance_etl_source_sequence", pa.int64()),
            pa.field("lance_etl_event_digest", pa.binary(32)),
            pa.field("is_deleted", pa.bool_()),
        ]
    )
    dataset.describe_indices.return_value = [MagicMock(name=name) for kind, column, name in declarations]
    for description, declaration in zip(dataset.describe_indices.return_value, declarations, strict=True):
        description.name = declaration[2]
        description.index_type = declaration[0]
        description.field_names = [declaration[1]]
    dataset.stats.index_stats.return_value = {"num_unindexed_fragments": 0, "num_indexed_fragments": 7}
    dataset.get_fragments.return_value = [MagicMock()] * 7
    dataset.count_rows.return_value = 0
    monkeypatch.setattr("lance_etl.reconciler.workers.lance.dataset", lambda uri: dataset if uri else dataset)
    monkeypatch.setattr(
        "lance_etl.reconciler.workers.load_vector_config",
        lambda opened, column: {"column": column, "generation": 1} if opened is dataset else None,
    )
    spark = MagicMock()
    rdd = MagicMock()
    spark.sparkContext.parallelize.return_value = rdd
    rdd.map.side_effect = lambda function: MagicMock(collect=lambda: [function(rdd.payload)])

    def capture_payload(payload: list[object], partitions: int) -> MagicMock:
        """Capture the single executor payload for the synchronous test RDD.

        Args:
            payload: Single qualification payload.
            partitions: Requested executor partition count.

        Returns:
            Test RDD carrying the payload.
        """
        assert partitions == 1
        rdd.payload = payload[0]
        return rdd

    spark.sparkContext.parallelize.side_effect = capture_payload
    runner = ProfiledServeRunner(spark, profile, TelemetryConfig())
    evidence = runner.qualify_candidate("s3://candidate/target.lance")
    assert evidence["lance_version"] == 11
    assert len(evidence["indexes"]) == len(declarations)
    assert all(item["fully_covered"] for item in evidence["indexes"])


def test_candidate_qualification_blocks_one_uncovered_required_index(monkeypatch: pytest.MonkeyPatch) -> None:
    """One uncovered required index blocks the entire serving generation.

    Args:
        monkeypatch: Scoped Lance dataset replacement.
    """
    profile = DeploymentProfile()
    declarations = required_indexes(profile)
    dataset = MagicMock()
    dataset.version = 11
    dataset.schema.names = [
        "vector_id",
        "event_timestamp",
        "vector",
        "text",
        "cluster",
        "ttl",
        "lance_etl_window_seq",
        "lance_etl_source_sequence",
        "lance_etl_event_digest",
        "is_deleted",
    ]
    dataset.describe_indices.return_value = []
    monkeypatch.setattr("lance_etl.reconciler.workers.lance.dataset", lambda uri: dataset if uri else dataset)
    spark = MagicMock()
    rdd = MagicMock()
    spark.sparkContext.parallelize.return_value = rdd
    rdd.map.side_effect = lambda function: MagicMock(collect=lambda: [function(rdd.payload)])

    def capture_payload(payload: list[object], partitions: int) -> MagicMock:
        """Capture one qualification payload for local execution.

        Args:
            payload: Single qualification payload.
            partitions: Requested executor partition count.

        Returns:
            Test RDD carrying the payload.
        """
        assert partitions == 1
        rdd.payload = payload[0]
        return rdd

    spark.sparkContext.parallelize.side_effect = capture_payload
    evidence = ProfiledServeRunner(spark, profile, TelemetryConfig()).qualify_candidate("s3://candidate/target.lance")
    assert evidence["error_code"] == "INCOMPLETE_INDEX_COVERAGE"
    assert len(evidence["indexes"]) == len(declarations)


def test_serve_success_without_candidate_uri_is_rejected() -> None:
    """A result cannot advance work while leaving the serving catalog undefined."""
    result = WorkResult(
        claim=work_claim(WorkKind.SERVE, WorkPhase.PREWARM),
        kind=ResultKind.SERVE_SUCCEEDED,
        indexed_lance_version=9,
        artifact_manifest_uri="s3://artifacts/manifest.json",
        artifact_digest=b"b" * 32,
    )
    with pytest.raises(ValueError, match="candidate URI"):
        result.validate()


def test_transient_retry_delay_is_code_owned_and_unbounded_by_attempt_count() -> None:
    """A high-attempt failure returns to durable RETRY_WAIT using the capped release delay."""
    repository = MagicMock()
    repository.retry_work.return_value = True
    claim = work_claim(attempt_count=10_000)
    result = WorkResult(claim, ResultKind.RETRY, error_code="OBJECT_STORE_TIMEOUT", error_message="timeout")
    reconciler = ResultReconciler(repository, DeploymentProfile())
    assert reconciler.reconcile(result)
    repository.retry_work.assert_called_once_with(
        claim,
        DeploymentProfile().retry_max_delay,
        "OBJECT_STORE_TIMEOUT",
        "timeout",
        None,
    )


@dataclass(slots=True)
class ScriptedExecutor:
    """Executor returning exact outcomes or raising for selected claims."""

    outcomes: dict[uuid.UUID, WorkResult]
    failures: set[uuid.UUID]

    def execute(self, claim: WorkClaim) -> WorkResult:
        """Return or raise the configured target-local result.

        Args:
            claim: Fenced target claim.

        Returns:
            Configured result.

        Raises:
            RuntimeError: For a configured transient target failure.
        """
        if claim.work_id in self.failures:
            raise RuntimeError("isolated target failure")
        return self.outcomes[claim.work_id]


def test_dispatcher_bounds_claims_and_isolates_target_failures() -> None:
    """One crashing target retries while another succeeds in the same bounded drain."""
    first = work_claim()
    second = work_claim()
    repository = MagicMock()
    repository.claim_due_work.side_effect = [[first, second], []]
    repository.complete_ingest.return_value = True
    repository.retry_work.return_value = True
    success = WorkResult(
        first,
        ResultKind.INGEST_SUCCEEDED,
        data_lance_version=3,
        source_row_count=1,
        source_digest=b"c" * 32,
    )
    executor = ScriptedExecutor({first.work_id: success}, {second.work_id})
    profile = replace(DeploymentProfile(), claim_batch_size=2, max_drain_batches=3)
    reconciler = ResultReconciler(repository, profile)
    summary = BoundedDispatcher(repository, executor, reconciler, profile).run()
    assert summary.claimed == 2
    assert summary.succeeded == 1
    assert summary.retried == 1
    assert summary.stale == 0
    repository.complete_ingest.assert_called_once()
    repository.retry_work.assert_called_once()


def test_dispatcher_stops_at_release_owned_drain_limit() -> None:
    """Continuously due work cannot turn one scheduled task into an unbounded loop."""
    claims = [work_claim(), work_claim(), work_claim()]
    repository = MagicMock()
    repository.claim_due_work.side_effect = [[claim] for claim in claims]
    repository.block_work.return_value = True
    outcomes = {claim.work_id: WorkResult(claim, ResultKind.BLOCKED, error_code="SOURCE_CONTRACT") for claim in claims}
    profile = replace(DeploymentProfile(), claim_batch_size=1, max_drain_batches=2)
    summary = BoundedDispatcher(
        repository,
        ScriptedExecutor(outcomes, set()),
        ResultReconciler(repository, profile),
        profile,
    ).run()
    assert summary.claimed == 2
    assert repository.claim_due_work.call_count == 2


def test_fenced_executor_discards_result_when_heartbeat_loses_lease() -> None:
    """Long Spark work cannot reconcile output after a background renewal loses the fence."""
    claim = work_claim()
    context = WorkExecutionContext(
        claim,
        RoutingIdentity("tenant1", "namespace1", "org1"),
        "production-v1",
        10,
        None,
        10,
        SourceWindowKind.BASELINE,
        None,
        None,
        None,
        None,
    )
    repository = MagicMock()
    repository.work_execution_context.return_value = context
    repository.renew_lease.return_value = False
    ingest = MagicMock()

    def slow_ingest(work_context: WorkExecutionContext) -> WorkResult:
        """Let the renewal thread observe a lost claim.

        Args:
            work_context: Exact work context.

        Returns:
            An otherwise valid completion result.
        """
        assert work_context == context
        time.sleep(0.04)
        return WorkResult(
            claim,
            ResultKind.INGEST_SUCCEEDED,
            data_lance_version=1,
            source_row_count=0,
            source_digest=b"a" * 32,
        )

    ingest.run.side_effect = slow_ingest
    profile = replace(
        DeploymentProfile(),
        lease_duration=timedelta(seconds=1),
        lease_heartbeat_interval=timedelta(milliseconds=5),
    )
    result = FencedWorkExecutor(repository, ingest, MagicMock(), profile).execute(claim)
    assert result.kind is ResultKind.RETRY
    assert result.error_code == "LEASE_LOST"
    assert repository.renew_lease.call_count >= 1


def test_retention_gate_preserves_exact_append_parent() -> None:
    """Unapplied or blocked INGEST state holds the parent needed by its exact incremental scan."""
    status = control_status(
        blocked_sources=1,
        retention_window=8,
        retention_snapshot=101,
        retention_parent=100,
        retention_state=SourceWindowState.BLOCKED,
    )
    decision = retention_decision(status)
    assert decision.retention_held
    assert decision.window_seq == 8
    assert decision.retain_snapshot_id == 100
    assert decision.state == "BLOCKED"


def test_serve_failure_does_not_hold_source_retention() -> None:
    """Retrying serving work alerts independently after source application completes."""
    status = control_status(retry_wait=1, due=1)
    decision = retention_decision(status)
    assert not decision.retention_held
    assert decision.retain_snapshot_id is None


def test_slo_evaluation_is_low_cardinality_and_fixed() -> None:
    """Queue, block, and retention age breaches emit bounded reason names only."""
    now = datetime(2026, 7, 14, 12, tzinfo=UTC)
    status = control_status(
        blocked=2,
        due=20_000,
        blocked_sources=1,
        oldest_open=now - timedelta(hours=2),
        retention_window=5,
        retention_snapshot=9,
        retention_created=now - timedelta(days=2),
    )
    evaluated = evaluate_slo(status, DeploymentProfile(), now)
    assert not evaluated.healthy
    assert evaluated.reasons == (
        "blocked_work",
        "blocked_source_window",
        "due_queue_over_budget",
        "open_work_age_over_budget",
        "source_retention_age_over_budget",
    )


def test_cli_exposes_only_closed_phases_and_restricted_repair() -> None:
    """The reconciler parser has no range, dataset, engine, index, TTL, or cache flags."""
    parser = build_parser()
    for phase in SCHEDULED_PHASES:
        assert parser.parse_args([phase]).command == phase
    repair = parser.parse_args(["repair", "--work-id", str(uuid.uuid4()), "--action", "retry-blocked", "--dry-run"])
    assert repair.command == "repair"
    assert repair.dry_run
    rollback = parser.parse_args(
        [
            "repair",
            "--action",
            "rollback",
            "--tenant-id",
            "tenant1",
            "--namespace",
            "namespace1",
            "--org-id",
            "org1",
            "--retained-work-id",
            str(uuid.uuid4()),
            "--dry-run",
        ]
    )
    assert rollback.action == "rollback"
    with pytest.raises(SystemExit):
        parser.parse_args(["run_due_target_work", "--datasets-file", "/tmp/targets"])
    with pytest.raises(SystemExit):
        parser.parse_args(["plan_and_enqueue_window", "--window-start", "2026-01-01"])


def test_only_fenced_production_and_restricted_tool_scripts_are_installed() -> None:
    """Legacy direct job entry points cannot bypass durable work from an installed wheel."""
    project = tomllib.loads((Path(__file__).parent.parent / "pyproject.toml").read_text())
    assert project["project"]["scripts"] == {
        "lance-etl-reconcile": "lance_etl.reconciler.cli:main",
        "lance-etl-tools": "lance_etl.tools.cli:main",
    }


def test_runtime_settings_are_deployment_owned_environment_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runtime identity loads from deployment environment instead of Airflow parameters.

    Args:
        monkeypatch: Scoped environment fixture.
    """
    monkeypatch.setenv("LANCE_ETL_DATABASE_URL", "postgresql+psycopg://db/control")
    monkeypatch.setenv("LANCE_ETL_LANCE_BASE_URI", "s3://bucket/lance")
    monkeypatch.setenv("LANCE_ETL_SOURCE_TABLE", "prod.vectors.events")
    settings = RuntimeSettings.from_environment()
    assert settings.database_url == "postgresql+psycopg://db/control"
    assert settings.lance_base_uri == "s3://bucket/lance"
    assert settings.source_table == "prod.vectors.events"


def test_application_dispatches_five_actions_and_restricted_repair() -> None:
    """The application delegates without carrying transient scheduler state."""
    plan_provider = MagicMock()
    plan_provider.plan.return_value = source_plan(0)
    plan_enqueuer = MagicMock()
    plan_enqueuer.enqueue.return_value = "enqueued"
    dispatcher = MagicMock()
    dispatcher.run.return_value = "dispatched"
    sweep = MagicMock()
    sweep.reconcile.return_value = ReconcileSummary(0, 0, 0)
    repository = MagicMock()
    repository.control_plane_status.return_value = control_status()
    repository.retry_blocked_work.return_value = True
    emitter = MagicMock()
    application = ReconcilerApplication(
        plan_provider,
        plan_enqueuer,
        dispatcher,
        sweep,
        repository,
        emitter,
        DeploymentProfile(),
    )
    assert application.plan_and_enqueue_window() == "enqueued"
    assert application.run_due_target_work() == "dispatched"
    assert application.reconcile_results() == ReconcileSummary(0, 0, 0)
    assert not application.gate_source_retention().retention_held
    assert application.emit_slo_status().healthy
    work_id = uuid.uuid4()
    assert application.repair_blocked_work(work_id, True)
    repository.retry_blocked_work.assert_not_called()
    assert application.repair_blocked_work(work_id, False)
    repository.retry_blocked_work.assert_called_once_with(work_id)
    identity = RoutingIdentity("tenant1", "namespace1", "org1")
    retained_work_id = uuid.uuid4()
    rollback_work_id = uuid.uuid4()
    repository.enqueue_rollback.return_value = rollback_work_id
    assert application.repair_rollback(identity, retained_work_id, True) is None
    repository.enqueue_rollback.assert_not_called()
    assert application.repair_rollback(identity, retained_work_id, False) == rollback_work_id
    repository.enqueue_rollback.assert_called_once_with(identity, retained_work_id)


def test_execute_command_never_dispatches_arbitrary_action() -> None:
    """Parsed commands map to the closed application methods."""
    application = MagicMock()
    application.emit_slo_status.return_value = "status"
    args = argparse.Namespace(command="emit_slo_status")
    assert execute_command(application, args) == "status"
    application.emit_slo_status.assert_called_once_with()


def test_main_bootstraps_a_fresh_process_without_installation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scheduled command creates its runtime when no test application is injected.

    Args:
        monkeypatch: Scoped runtime factory replacement.
    """
    application = MagicMock()
    application.reconcile_results.return_value = ReconcileSummary(0, 0, 0)
    factory = MagicMock(return_value=application)
    monkeypatch.setattr("lance_etl.reconciler.cli.build_runtime_application", factory)
    assert main(["reconcile_results"]) == 0
    factory.assert_called_once_with()
    application.reconcile_results.assert_called_once_with()
