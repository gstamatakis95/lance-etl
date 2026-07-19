"""Unit tests for the local PostgreSQL-backed reconciler."""

from __future__ import annotations

import argparse
import uuid
from dataclasses import fields as dataclass_fields
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from lance_etl.reconciler import (
    BoundedDispatcher,
    DispatchSummary,
    EnqueueSummary,
    ReconcilerApplication,
    ReconcilerSettings,
    ReconcileSummary,
    ResultKind,
    ResultReconciler,
    RetentionDecision,
    RunOnceSummary,
    SloStatus,
    SourcePlanEnqueuer,
    WorkResult,
    default_reconciler_settings,
    evaluate_slo,
    retention_decision,
)
from lance_etl.reconciler.cli import build_parser, execute_command
from lance_etl.reconciler.config import RuntimeSettings
from lance_etl.reconciler.iceberg import DurableSourcePlanProvider
from lance_etl.reconciler.workers import FencedWorkExecutor, persisted_arrow_schema, required_indexes
from lance_etl.source import (
    BaselineProof,
    PartitionField,
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
    DatasetPlan,
    DatasetSpecRevision,
    IcebergSource,
    IndexType,
    PublicationEvidence,
    PublicationIndexEvidence,
    RoutingIdentity,
    SourceLifecycleState,
    SourceSnapshotKind,
    SourceSnapshotPlan,
    SourceSnapshotState,
    WorkClaim,
    WorkKind,
    WorkPhase,
    production_default_spec_revision,
)


def source_registration(baseline_snapshot_id: int | None = 9) -> IcebergSource:
    """Return one valid database-owned source registration.

    Args:
        baseline_snapshot_id: Optional canonical first snapshot.

    Returns:
        Source registration fixture.
    """
    return IcebergSource(
        source_id=uuid.uuid4(),
        source_name="local",
        spark_catalog="local",
        table_namespace="db",
        table_name="events",
        table_uuid=uuid.uuid4(),
        lance_base_uri="/tmp/lance",
        lifecycle_state=SourceLifecycleState.ACTIVE,
        default_spec_id=production_default_spec_revision().spec_id,
        canonical_baseline_snapshot_id=baseline_snapshot_id,
        replay_horizon=timedelta(days=7),
    ).validate()


def reconciler_settings(**changes: object) -> ReconcilerSettings:
    """Return validated settings with selected fixture overrides.

    Args:
        changes: Dataclass fields to replace.

    Returns:
        Validated reconciler settings.
    """
    return replace(default_reconciler_settings(), **changes).validate()


def work_claim(
    kind: WorkKind = WorkKind.INGEST,
    phase: WorkPhase = WorkPhase.INGEST,
    attempt_count: int = 1,
) -> WorkClaim:
    """Return one exact fenced dataset-work claim.

    Args:
        kind: Durable work kind.
        phase: Current checkpointed phase.
        attempt_count: Durable attempt count.

    Returns:
        Claim fixture.
    """
    spec: DatasetSpecRevision = production_default_spec_revision()
    return WorkClaim(
        work_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        kind=kind,
        phase=phase,
        lease_token=uuid.uuid4(),
        fence_epoch=4,
        attempt_count=attempt_count,
        source_snapshot_seq=3 if kind is WorkKind.INGEST else 2,
        spec_revision_id=spec.spec_revision_id,
        ingest_lance_uri="/tmp/lance/dataset.lance",
        ingest_lance_version=None if kind is WorkKind.INGEST else 7,
    )


def publication_evidence() -> PublicationEvidence:
    """Return complete evidence for the bundled immutable specification.

    Returns:
        Validated publication evidence.
    """
    spec: DatasetSpecRevision = production_default_spec_revision()
    indexes: tuple[PublicationIndexEvidence, ...] = tuple(
        PublicationIndexEvidence(
            index_definition_id=definition.index_definition_id,
            actual_index_type=definition.index_type,
            indexed_fragment_count=2,
            unindexed_fragment_count=0,
            artifact_generation_digest=b"v" * 32 if definition.index_type is IndexType.IVF_RQ else None,
        )
        for definition in spec.index_definitions
    )
    return PublicationEvidence(b"s" * 32, 8, 8, 7, 7, 2, indexes).validate()


def control_status(
    *,
    due: int = 0,
    blocked: int = 0,
    blocked_sources: int = 0,
    oldest_open: datetime | None = None,
    retention_sequence: int | None = None,
    retention_snapshot: int | None = None,
    retention_parent: int | None = None,
    retention_state: SourceSnapshotState | None = None,
    retention_created: datetime | None = None,
) -> ControlPlaneStatus:
    """Return a bounded control-plane status fixture.

    Args:
        due: Currently due work.
        blocked: Blocked work.
        blocked_sources: Blocked source snapshots.
        oldest_open: Oldest open work timestamp.
        retention_sequence: Oldest unfinished source sequence.
        retention_snapshot: Snapshot at the retention floor.
        retention_parent: Parent required for incremental scanning.
        retention_state: State of the floor snapshot.
        retention_created: Creation time of the floor snapshot.

    Returns:
        Status fixture.
    """
    return ControlPlaneStatus(
        pending_work=due,
        running_work=0,
        retry_wait_work=0,
        blocked_work=blocked,
        due_work=due,
        blocked_source_snapshots=blocked_sources,
        oldest_open_work_at=oldest_open,
        retention_source_snapshot_seq=retention_sequence,
        retention_snapshot_id=retention_snapshot,
        retention_parent_snapshot_id=retention_parent,
        retention_state=retention_state,
        retention_created_at=retention_created,
    )


def valid_partition_spec(spec_id: int = 7) -> PartitionSpec:
    """Return the required Iceberg routing partition layout.

    Args:
        spec_id: Numeric Iceberg partition specification identity.

    Returns:
        Valid partition specification.
    """
    return PartitionSpec(
        spec_id,
        (
            PartitionField("tenant_id", "tenant_id", "identity"),
            PartitionField("namespace", "namespace", "identity"),
            PartitionField("org_id", "org_id", "identity"),
            PartitionField("processing_timestamp_hour", "processing_timestamp", "hour"),
        ),
    )


def source_plan(source: IcebergSource, window_count: int = 2) -> SourcePlan:
    """Build a pinned source plan with distinct logical datasets.

    Args:
        source: Registered source carrying the table UUID.
        window_count: Number of append snapshots.

    Returns:
        Source plan fixture.
    """
    windows: list[WindowPlan] = []
    index: int
    for index in range(window_count):
        snapshot_id: int = index + 2
        snapshot: SnapshotRecord = SnapshotRecord(
            str(source.table_uuid), snapshot_id, snapshot_id - 1, snapshot_id, 1000, "append", 7
        )
        target: TargetKey = TargetKey(f"tenant{index}", "vectors", "org1")
        windows.append(WindowPlan(snapshot, WindowKind.APPEND, (TouchedTarget(target, (1, 9)),), ()))
    return SourcePlan(str(source.table_uuid), window_count + 1, 7, tuple(windows))


def test_source_plan_enqueuer_maps_snapshots_and_database_spec() -> None:
    """Manifest datasets and source metadata reach one atomic call per snapshot."""
    source: IcebergSource = source_registration()
    repository: MagicMock = MagicMock()
    repository.enqueue_source_snapshot.side_effect = [11, 12]
    summary: EnqueueSummary = SourcePlanEnqueuer(repository, reconciler_settings(), source).enqueue(source_plan(source))
    assert summary.source_snapshot_sequences == (11, 12)
    assert summary.enqueued_snapshots == 2
    first_snapshot: SourceSnapshotPlan
    first_datasets: list[DatasetPlan]
    first_snapshot, first_datasets = repository.enqueue_source_snapshot.call_args_list[0].args
    assert first_snapshot.source_id == source.source_id
    assert first_snapshot.snapshot_id == 2
    assert first_snapshot.kind is SourceSnapshotKind.APPEND
    assert first_datasets[0].identity == RoutingIdentity("tenant0", "vectors", "org1")


def test_source_plan_enqueuer_applies_postgres_planning_bound() -> None:
    """One planner task cannot enqueue an unbounded Iceberg lineage catch-up."""
    source: IcebergSource = source_registration()
    repository: MagicMock = MagicMock()
    repository.enqueue_source_snapshot.side_effect = range(100)
    settings: ReconcilerSettings = reconciler_settings(max_snapshots_per_plan=2)
    summary: EnqueueSummary = SourcePlanEnqueuer(repository, settings, source).enqueue(source_plan(source, 5))
    assert summary.enqueued_snapshots == 2
    assert summary.planned_snapshots == 5
    assert summary.truncated
    assert repository.enqueue_source_snapshot.call_count == 2


def test_source_plan_enqueuer_rejects_another_registered_table() -> None:
    """A source plan cannot cross its PostgreSQL source identity."""
    source: IcebergSource = source_registration()
    plan: SourcePlan = replace(source_plan(source), table_uuid=str(uuid.uuid4()))
    with pytest.raises(ValueError, match="table UUID"):
        SourcePlanEnqueuer(MagicMock(), reconciler_settings(), source).enqueue(plan)


def test_initial_source_plan_uses_qualified_database_baseline() -> None:
    """The PostgreSQL baseline is accepted only after an executor qualification."""
    source: IcebergSource = source_registration(9)
    metadata: TableMetadata = TableMetadata(str(source.table_uuid), 9, valid_partition_spec())
    baseline: SnapshotRecord = SnapshotRecord(str(source.table_uuid), 9, None, 9, 1000, "append", 7)
    catalog: MagicMock = MagicMock()
    catalog.table_metadata.return_value = metadata
    catalog.snapshots_through.return_value = (baseline,)
    catalog.manifest_entries.return_value = ()
    repository: MagicMock = MagicMock()
    repository.latest_source_snapshot.return_value = None
    qualifier: MagicMock = MagicMock()
    qualifier.qualify.return_value = BaselineProof(str(source.table_uuid), 9, 7, True, 0)
    plan: SourcePlan = DurableSourcePlanProvider(source, catalog, repository, qualifier).plan()
    assert len(plan.windows) == 1
    assert plan.windows[0].kind is WindowKind.BASELINE
    qualifier.qualify.assert_called_once_with(source, metadata, 9)


def test_invalid_initial_baseline_is_a_durable_rejected_snapshot() -> None:
    """A failed root qualification is retained as a blocked REJECTED source row."""
    source: IcebergSource = source_registration(9)
    metadata: TableMetadata = TableMetadata(str(source.table_uuid), 9, valid_partition_spec())
    baseline: SnapshotRecord = SnapshotRecord(str(source.table_uuid), 9, None, 9, 1000, "append", 7)
    catalog: MagicMock = MagicMock()
    catalog.table_metadata.return_value = metadata
    catalog.snapshots_through.return_value = (baseline,)
    repository: MagicMock = MagicMock()
    repository.latest_source_snapshot.return_value = None
    qualifier: MagicMock = MagicMock()
    qualifier.qualify.return_value = BaselineProof(str(source.table_uuid), 9, 7, False, 1)
    plan: SourcePlan = DurableSourcePlanProvider(source, catalog, repository, qualifier).plan()
    assert plan.windows == ()
    blocked_plan: SourceSnapshotPlan
    error_code: str
    blocked_plan, error_code = repository.enqueue_blocked_source_snapshot.call_args.args
    assert blocked_plan.kind is SourceSnapshotKind.REJECTED
    assert blocked_plan.parent_snapshot_id is None
    assert error_code == "BASELINE_NOT_CANONICAL"


def test_source_contract_drift_blocks_the_existing_audit_tip() -> None:
    """Changed table metadata stops planning before any incremental scan."""
    source: IcebergSource = source_registration()
    catalog: MagicMock = MagicMock()
    catalog.table_metadata.return_value = TableMetadata(str(source.table_uuid), 10, valid_partition_spec(8))
    repository: MagicMock = MagicMock()
    repository.latest_source_snapshot.return_value = {
        "source_snapshot_seq": 11,
        "table_uuid": source.table_uuid,
        "snapshot_id": 9,
        "iceberg_sequence_number": 9,
        "partition_spec_id": 7,
        "state": SourceSnapshotState.COMPLETE.value,
    }
    plan: SourcePlan = DurableSourcePlanProvider(source, catalog, repository, MagicMock()).plan()
    assert plan.windows == ()
    repository.block_source_snapshot.assert_called_once_with(11, "SOURCE_TABLE_CONTRACT")
    catalog.snapshots_through.assert_not_called()


def test_work_result_requires_exact_ingest_and_publication_evidence() -> None:
    """Successful results cannot omit replay or publication evidence."""
    claim: WorkClaim = work_claim()
    with pytest.raises(ValueError, match="INGEST success"):
        WorkResult(claim, ResultKind.INGEST_SUCCEEDED, data_lance_version=2).validate()
    publish: WorkClaim = work_claim(WorkKind.PUBLISH, WorkPhase.COMPACT)
    with pytest.raises(ValueError, match="PUBLISH success"):
        WorkResult(publish, ResultKind.PUBLISH_SUCCEEDED, indexed_lance_version=3).validate()


def test_result_reconciler_completes_ingest_with_exact_evidence() -> None:
    """An ingest completion preserves the claim fence and exact source evidence."""
    repository: MagicMock = MagicMock()
    repository.complete_ingest.return_value = True
    claim: WorkClaim = work_claim()
    result: WorkResult = WorkResult(
        claim, ResultKind.INGEST_SUCCEEDED, data_lance_version=8, source_row_count=4, source_digest=b"d" * 32
    )
    now: datetime = datetime(2026, 7, 18, tzinfo=UTC)
    assert ResultReconciler(repository, reconciler_settings()).reconcile(result, now)
    repository.complete_ingest.assert_called_once_with(claim, 8, 4, b"d" * 32, now)


def test_result_reconciler_checkpoints_fixed_publication_phases() -> None:
    """Publication records every completed phase before the atomic pointer swap."""
    repository: MagicMock = MagicMock()
    repository.advance_phase.return_value = True
    repository.publish_dataset.return_value = True
    claim: WorkClaim = work_claim(WorkKind.PUBLISH, WorkPhase.COMPACT)
    evidence: PublicationEvidence = publication_evidence()
    result: WorkResult = WorkResult(
        claim,
        ResultKind.PUBLISH_SUCCEEDED,
        indexed_lance_version=11,
        candidate_lance_uri=claim.ingest_lance_uri,
        manifest_uri="/tmp/lance/manifest.json",
        manifest_digest=b"m" * 32,
        publication_evidence=evidence,
    )
    assert ResultReconciler(repository, reconciler_settings()).reconcile(result)
    assert [call.args[1] for call in repository.advance_phase.call_args_list] == [
        WorkPhase.INDEX,
        WorkPhase.VALIDATE,
        WorkPhase.PREWARM,
    ]
    repository.publish_dataset.assert_called_once_with(
        claim,
        claim.ingest_lance_uri,
        11,
        "/tmp/lance/manifest.json",
        b"m" * 32,
        evidence,
        None,
    )


def test_retry_delay_is_reproducible_capped_and_used_by_reconciler() -> None:
    """PostgreSQL attempt counts yield deterministic bounded retry scheduling."""
    claim: WorkClaim = work_claim(attempt_count=8)
    settings: ReconcilerSettings = reconciler_settings(
        retry_base_delay=timedelta(seconds=10), retry_max_delay=timedelta(seconds=30)
    )
    first: timedelta = settings.retry_delay(claim.attempt_count, claim.work_id)
    second: timedelta = settings.retry_delay(claim.attempt_count, claim.work_id)
    assert first == second
    assert timedelta(0) <= first <= timedelta(seconds=30)
    repository: MagicMock = MagicMock()
    repository.retry_work.return_value = True
    result: WorkResult = WorkResult(claim, ResultKind.RETRY, error_code="TRANSIENT", error_message="try again")
    assert ResultReconciler(repository, settings).reconcile(result)
    repository.retry_work.assert_called_once_with(claim, first, "TRANSIENT", "try again", None)


def test_bounded_dispatcher_isolates_worker_failures_and_stale_results() -> None:
    """One dataset exception becomes retry work while another stale fence is counted."""
    repository: MagicMock = MagicMock()
    claims: list[WorkClaim] = [work_claim(), work_claim()]
    repository.claim_due_work.side_effect = [claims, []]
    executor: MagicMock = MagicMock()
    executor.execute.side_effect = [RuntimeError("boom"), WorkResult(claims[1], ResultKind.BLOCKED, error_code="BAD")]
    results: MagicMock = MagicMock()
    results.reconcile.side_effect = [True, False]
    summary: DispatchSummary = BoundedDispatcher(
        repository, executor, results, reconciler_settings(claim_batch_size=2)
    ).run()
    assert summary == DispatchSummary(2, 0, 0, 1, 1, 1)
    first_result: WorkResult = results.reconcile.call_args_list[0].args[0]
    assert first_result.kind is ResultKind.RETRY
    assert first_result.error_code == "UNEXPECTED_WORKER_FAILURE"


def test_fenced_executor_rejects_stale_context_without_external_work() -> None:
    """A lost database fence prevents any Spark or Lance operation."""
    repository: MagicMock = MagicMock()
    repository.work_execution_context.return_value = None
    ingest: MagicMock = MagicMock()
    publisher: MagicMock = MagicMock()
    result: WorkResult = FencedWorkExecutor(repository, ingest, publisher, reconciler_settings()).execute(work_claim())
    assert result.kind is ResultKind.RETRY
    assert result.error_code == "STALE_CLAIM"
    ingest.run.assert_not_called()
    publisher.run.assert_not_called()


def test_required_indexes_are_entirely_specification_driven() -> None:
    """Index requirements come from normalized definitions without process profiles."""
    spec: DatasetSpecRevision = production_default_spec_revision()
    fields_by_id: dict[uuid.UUID, str] = {field.field_id: field.target_name for field in spec.fields}
    assert required_indexes(spec) == tuple(
        (definition.index_type.value, fields_by_id[definition.field_id], definition.index_name)
        for definition in spec.index_definitions
    )


def test_persisted_schema_exactly_matches_the_normalized_field_graph() -> None:
    """The writer and publication qualifier share exact order, physical types, and nullability."""
    spec: DatasetSpecRevision = production_default_spec_revision()
    schema: pa.Schema = persisted_arrow_schema(spec)

    assert schema.names == [field.target_name for field in sorted(spec.fields, key=lambda item: item.ordinal)]
    assert schema.field("vector_id") == pa.field("vector_id", pa.string(), nullable=False)
    assert schema.field("vector") == pa.field("vector", pa.list_(pa.float32(), 128))
    assert schema.field("ttl") == pa.field("ttl", pa.duration("s"))
    assert schema.field("lance_etl_event_digest") == pa.field(
        "lance_etl_event_digest",
        pa.binary(32),
        nullable=False,
    )
    assert schema.field("is_deleted") == pa.field("is_deleted", pa.bool_(), nullable=False)


def test_retention_floor_preserves_incremental_parent() -> None:
    """An unfinished increment retains its parent snapshot for an exact changelog scan."""
    decision: RetentionDecision = retention_decision(
        control_status(
            retention_sequence=4,
            retention_snapshot=104,
            retention_parent=103,
            retention_state=SourceSnapshotState.SEALED,
        )
    )
    assert decision.retention_held
    assert decision.source_snapshot_seq == 4
    assert decision.retain_snapshot_id == 103
    assert retention_decision(control_status()).retention_held is False


def test_slo_reports_queue_blockage_and_retention_age() -> None:
    """Database thresholds produce only low-cardinality health reasons."""
    now: datetime = datetime(2026, 7, 18, 12, tzinfo=UTC)
    settings: ReconcilerSettings = reconciler_settings(
        max_due_work=2,
        max_open_work_age=timedelta(minutes=5),
        max_retention_age=timedelta(minutes=10),
    )
    status: ControlPlaneStatus = control_status(
        due=3,
        blocked=1,
        blocked_sources=1,
        oldest_open=now - timedelta(minutes=6),
        retention_sequence=2,
        retention_snapshot=10,
        retention_state=SourceSnapshotState.SEALED,
        retention_created=now - timedelta(minutes=11),
    )
    result: SloStatus = evaluate_slo(status, settings, now)
    assert not result.healthy
    assert result.reasons == (
        "blocked_work",
        "blocked_source_snapshot",
        "due_queue_over_budget",
        "open_work_age_over_budget",
        "source_retention_age_over_budget",
    )


def test_application_runs_local_cycle_in_dependency_order() -> None:
    """The one-process application plans, drains, sweeps, gates, then emits."""
    provider: MagicMock = MagicMock()
    provider.plan.return_value = SourcePlan(str(uuid.uuid4()), None, 7, ())
    enqueuer: MagicMock = MagicMock()
    planning: EnqueueSummary = EnqueueSummary(None, 0, 0, (), False)
    enqueuer.enqueue.return_value = planning
    dispatcher: MagicMock = MagicMock()
    dispatch: DispatchSummary = DispatchSummary(0, 0, 0, 0, 0, 0)
    dispatcher.run.return_value = dispatch
    sweep: MagicMock = MagicMock()
    reconciliation: ReconcileSummary = ReconcileSummary(0, 0, 0)
    sweep.reconcile.return_value = reconciliation
    repository: MagicMock = MagicMock()
    repository.control_plane_status.return_value = control_status()
    emitter: MagicMock = MagicMock()
    application: ReconcilerApplication = ReconcilerApplication(
        provider,
        enqueuer,
        dispatcher,
        sweep,
        repository,
        emitter,
        reconciler_settings(),
    )
    result: RunOnceSummary = application.run_once()
    assert result.planning == planning
    assert result.dispatch == dispatch
    assert result.reconciliation == reconciliation
    assert not result.retention.retention_held
    assert result.slo.healthy
    emitter.emit.assert_called_once_with(result.slo)


def test_rebuild_repair_uses_a_stable_operator_request() -> None:
    """A rebuild request enqueues a database-owned generation and supports dry run."""
    application: ReconcilerApplication = ReconcilerApplication(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        reconciler_settings(),
    )
    identity: RoutingIdentity = RoutingIdentity("tenant1", "vectors", "org1")
    request_id: uuid.UUID = uuid.uuid4()
    application.repository.enqueue_rebuild.return_value = uuid.uuid4()
    assert application.repair_rebuild(identity, request_id, True) is None
    result: uuid.UUID | None = application.repair_rebuild(identity, request_id, False)
    assert result == application.repository.enqueue_rebuild.return_value
    application.repository.enqueue_rebuild.assert_called_once_with(identity, request_id)


def test_runtime_settings_are_local_and_have_no_remote_execution_options(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Environment bootstrap contains only PostgreSQL, local Spark, and local paths.

    Args:
        monkeypatch: Isolated environment fixture.
        tmp_path: Local runtime root.
    """
    monkeypatch.setenv("LANCE_ETL_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("LANCE_ETL_DATABASE_URL", "postgresql+psycopg://localhost/lance_etl")
    monkeypatch.setenv("LANCE_ETL_SPARK_MASTER", "local[2]")
    settings: RuntimeSettings = RuntimeSettings.from_environment()
    assert settings.spark_master == "local[2]"
    assert settings.lance_base_uri == str(tmp_path / "lance")
    assert settings.spark_warehouse_path == (tmp_path / "iceberg").resolve()
    assert {field.name for field in dataclass_fields(settings)} == {
        "database_url",
        "lance_base_uri",
        "source_table",
        "datadog_service",
        "datadog_env",
        "canonical_baseline_snapshot_id",
        "spark_master",
        "spark_catalog",
        "spark_warehouse_path",
        "spark_iceberg_package",
    }


def test_cli_exposes_only_local_control_plane_actions() -> None:
    """The command surface is migrate, local run, status, and restricted repair."""
    parser: argparse.ArgumentParser = build_parser()
    assert parser.parse_args(["run-once"]).command == "run-once"
    assert parser.parse_args(["run", "--poll-seconds", "1"]).poll_seconds == 1.0
    migrator: MagicMock = MagicMock()
    assert execute_command(None, parser.parse_args(["migrate"]), migrator) == {"migrated": True}
    migrator.migrate.assert_called_once_with()
    with pytest.raises(SystemExit):
        parser.parse_args(["rollback"])
