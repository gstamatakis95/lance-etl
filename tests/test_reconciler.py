"""Unit tests for the local PostgreSQL-backed reconciler."""

from __future__ import annotations

import argparse
import uuid
from dataclasses import fields as dataclass_fields
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark

from lance_etl.etl.completion import CompletionConflict, CompletionMarker, finalize_completion_marker
from lance_etl.reconciler import (
    BoundedDispatcher,
    DispatchSummary,
    EnqueueSummary,
    ReconcilerApplication,
    ReconcilerOperator,
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
from lance_etl.reconciler.cli import EXIT_UNHEALTHY_STATUS, build_parser, execute_command, main
from lance_etl.reconciler.config import RuntimeSettings
from lance_etl.reconciler.iceberg import DurableSourcePlanProvider
from lance_etl.reconciler.service import execute_isolated
from lance_etl.reconciler.workers import (
    ConfiguredPublicationRunner,
    DistributedIngestRunner,
    FencedWorkExecutor,
    index_kind_matches,
    lance_major_version,
    observed_index_type,
    persisted_arrow_schema,
    required_indexes,
    required_unindexed_fragments,
    resolved_actual_index_kind,
)
from lance_etl.reconciler.workers import (
    publication_evidence as build_publication_evidence,
)
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
    StateTransitionError,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
    production_default_spec_revision,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


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
            PartitionField("ts_hour", "ts", "hour"),
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
    repository.retry_work.assert_called_once_with(claim, first, "TRANSIENT", "try again", settings.max_attempts, None)


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


def test_bounded_dispatcher_isolates_divergent_reconciliation() -> None:
    """A StateTransitionError from reconciling one claim never aborts the rest of the batch."""
    repository: MagicMock = MagicMock()
    claims: list[WorkClaim] = [work_claim(), work_claim()]
    repository.claim_due_work.side_effect = [claims, []]
    repository.block_work.return_value = True
    executor: MagicMock = MagicMock()
    executor.execute.side_effect = [
        WorkResult(
            claims[0], ResultKind.INGEST_SUCCEEDED, data_lance_version=1, source_row_count=0, source_digest=b"a" * 32
        ),
        WorkResult(
            claims[1], ResultKind.INGEST_SUCCEEDED, data_lance_version=2, source_row_count=0, source_digest=b"b" * 32
        ),
    ]
    results: MagicMock = MagicMock()
    results.reconcile.side_effect = [StateTransitionError("digest mismatch on replay"), True]
    summary: DispatchSummary = BoundedDispatcher(
        repository, executor, results, reconciler_settings(claim_batch_size=2)
    ).run()
    assert summary == DispatchSummary(claimed=2, succeeded=1, advanced=0, retried=0, blocked=1, stale=0)
    assert results.reconcile.call_count == 2
    repository.block_work.assert_called_once_with(claims[0], "STATE_DIVERGENCE", "digest mismatch on replay")


def test_dispatcher_isolates_a_real_reconciler_state_divergence_on_complete_ingest() -> None:
    """The PR-01 acceptance scenario: a real reconciler isolates one divergent claim.

    A real ``ResultReconciler`` whose ``repository.complete_ingest`` raises
    ``StateTransitionError`` for claim 1 of a 2-claim batch never aborts claim 2. Unlike the
    stubbed-``ResultReconciler`` test above, this drives the actual
    ``ResultReconciler.reconcile`` implementation so a regression that swallowed
    ``StateTransitionError`` inside ``reconcile`` itself (rather than the dispatcher's wrapper)
    would also be caught.
    """
    repository: MagicMock = MagicMock()
    claims: list[WorkClaim] = [work_claim(), work_claim()]
    repository.claim_due_work.side_effect = [claims, []]
    repository.complete_ingest.side_effect = [
        StateTransitionError("replayed digest disagrees with persisted evidence"),
        True,
    ]
    repository.block_work.return_value = True
    executor: MagicMock = MagicMock()
    executor.execute.side_effect = [
        WorkResult(
            claims[0], ResultKind.INGEST_SUCCEEDED, data_lance_version=1, source_row_count=0, source_digest=b"a" * 32
        ),
        WorkResult(
            claims[1], ResultKind.INGEST_SUCCEEDED, data_lance_version=2, source_row_count=0, source_digest=b"b" * 32
        ),
    ]
    settings: ReconcilerSettings = reconciler_settings(claim_batch_size=2)
    dispatcher: BoundedDispatcher = BoundedDispatcher(
        repository, executor, ResultReconciler(repository, settings), settings
    )

    summary: DispatchSummary = dispatcher.run()

    assert summary == DispatchSummary(claimed=2, succeeded=1, advanced=0, retried=0, blocked=1, stale=0)
    assert repository.complete_ingest.call_count == 2
    repository.block_work.assert_called_once_with(
        claims[0], "STATE_DIVERGENCE", "replayed digest disagrees with persisted evidence"
    )


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


def test_index_kind_gate_accepts_pylance8_unknown_inverted(monkeypatch: pytest.MonkeyPatch) -> None:
    """On pylance 8 an INVERTED index reported as Unknown resolves through its stats type.

    pylance 8.0.0 reports a segment-committed inverted index's ``describe_indices`` type as
    ``Unknown`` while ``stats.index_stats`` still reports the true ``Inverted`` type. The gate must
    recover the true type from stats and still reject genuinely wrong types.
    """
    monkeypatch.setattr(lance, "__version__", "8.0.0")
    assert resolved_actual_index_kind("UNKNOWN", "Inverted") == "INVERTED"
    assert index_kind_matches("INVERTED", resolved_actual_index_kind("UNKNOWN", "Inverted"))
    assert not index_kind_matches("INVERTED", resolved_actual_index_kind("UNKNOWN", "BTree"))
    assert resolved_actual_index_kind("IVF_RQ", "IVF_RQ") == "IVF_RQ"
    assert resolved_actual_index_kind("UNKNOWN", None) == "UNKNOWN"


def test_index_kind_gate_falls_back_on_pylance9(monkeypatch: pytest.MonkeyPatch) -> None:
    """On pylance 9 the Unknown fallback still fires because index_stats reports the true kind.

    The 9.x checkout derives ``index_stats``'s ``index_type`` from the index's own plugin
    statistics, so a segment-committed inverted index reported as ``Unknown`` by
    ``describe_indices`` still resolves to ``Inverted``. The resolution is data-driven and
    version-independent, and it still rejects a genuinely wrong stats type.
    """
    monkeypatch.setattr(lance, "__version__", "9.0.0-beta.17")
    assert resolved_actual_index_kind("UNKNOWN", "Inverted") == "INVERTED"
    assert index_kind_matches("INVERTED", resolved_actual_index_kind("UNKNOWN", "Inverted"))
    assert not index_kind_matches("INVERTED", resolved_actual_index_kind("UNKNOWN", "BTree"))
    assert resolved_actual_index_kind("UNKNOWN", None) == "UNKNOWN"


def test_lance_major_version_tolerates_malformed_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed pylance version string reports an unknown major instead of raising.

    The major is used only for an operator diagnostic, never to gate correctness, so a version
    string that cannot be parsed must not fail a publish. It is reported as ``0`` (unknown).
    """
    monkeypatch.setattr(lance, "__version__", "8.0.0")
    assert lance_major_version() == 8
    monkeypatch.setattr(lance, "__version__", "not-a-version")
    assert lance_major_version() == 0


def test_required_unindexed_fragments_rejects_missing_key() -> None:
    """A missing coverage key is a hard failure, while a zero count is a valid full-coverage result."""
    assert required_unindexed_fragments({"num_unindexed_fragments": 0}, "idx") == 0
    assert required_unindexed_fragments({"num_unindexed_fragments": 3}, "idx") == 3
    with pytest.raises(RuntimeError, match="num_unindexed_fragments"):
        required_unindexed_fragments({"num_indexed_fragments": 2}, "idx")
    with pytest.raises(RuntimeError, match="num_unindexed_fragments"):
        required_unindexed_fragments({"num_unindexed_fragments": None}, "idx")


def test_observed_index_type_normalizes_describe_aliases() -> None:
    """The observed describe alias resolves to the canonical control-plane index type."""
    assert observed_index_type("IVF") is IndexType.IVF_RQ
    assert observed_index_type("IVF_RQ") is IndexType.IVF_RQ
    assert observed_index_type("inverted") is IndexType.INVERTED
    assert observed_index_type("BTREE") is IndexType.BTREE
    with pytest.raises(ValueError, match="no supported index type"):
        observed_index_type("HNSW")


def qualification_evidence(spec: DatasetSpecRevision, observed: dict[IndexType, str]) -> dict[str, object]:
    """Build a successful qualification dictionary with per-index observed kinds.

    Args:
        spec: Frozen dataset specification.
        observed: Observed describe or stats kind per configured index type.

    Returns:
        Qualification evidence shaped like the executor gate output.
    """
    fields_by_id: dict[uuid.UUID, str] = {field.field_id: field.target_name for field in spec.fields}
    indexes: list[dict[str, object]] = [
        {
            "kind": definition.index_type.value,
            "column": fields_by_id[definition.field_id],
            "name": definition.index_name,
            "present": True,
            "actual_kind": observed[definition.index_type],
            "kind_matches": True,
            "columns_match": True,
            "fully_covered": True,
            "unindexed_fragments": 0,
            "index_fragments": 2,
            "artifact_generation_digest": ("aa" * 32) if definition.index_type is IndexType.IVF_RQ else None,
        }
        for definition in spec.index_definitions
    ]
    return {
        "schema_fingerprint": "ab" * 32,
        "total_rows": 8,
        "distinct_record_ids": 8,
        "live_rows": 7,
        "distinct_live_record_ids": 7,
        "fragment_count": 2,
        "indexes": indexes,
    }


def test_publication_evidence_records_observed_kind_not_configured() -> None:
    """Evidence persists the resolved observed kind so the publish gate is a real cross-check.

    Every configured index built correctly resolves back to its configured type through the
    describe alias, and a mismatched observation is recorded verbatim rather than silently copying
    the spec, which is what makes ``validate_publication_indexes`` able to fail.
    """
    spec: DatasetSpecRevision = production_default_spec_revision()
    describe_alias: dict[IndexType, str] = {
        IndexType.IVF_RQ: "IVF",
        IndexType.BTREE: "BTREE",
        IndexType.BITMAP: "BITMAP",
        IndexType.ZONEMAP: "ZONEMAP",
        IndexType.INVERTED: "INVERTED",
    }
    evidence: PublicationEvidence = build_publication_evidence(spec, qualification_evidence(spec, describe_alias))
    resolved: dict[uuid.UUID, IndexType] = {
        item.index_definition_id: item.actual_index_type for item in evidence.indexes
    }
    configured: dict[uuid.UUID, IndexType] = {
        definition.index_definition_id: definition.index_type for definition in spec.index_definitions
    }
    assert resolved == configured

    wrong: dict[IndexType, str] = dict(describe_alias)
    wrong[IndexType.INVERTED] = "BTREE"
    mismatched: PublicationEvidence = build_publication_evidence(spec, qualification_evidence(spec, wrong))
    inverted_definition_id: uuid.UUID = next(
        definition.index_definition_id
        for definition in spec.index_definitions
        if definition.index_type is IndexType.INVERTED
    )
    observed_kind: IndexType = next(
        item.actual_index_type for item in mismatched.indexes if item.index_definition_id == inverted_definition_id
    )
    assert observed_kind is IndexType.BTREE


def ingest_probe_context(uri: str, window_seq: int) -> SimpleNamespace:
    """Build a minimal context exposing only the fields the marker probe reads.

    Args:
        uri: Ingest dataset URI.
        window_seq: Source window sequence for the claim.

    Returns:
        A stand-in context whose claim carries the ingest URI and window sequence.
    """
    return SimpleNamespace(claim=SimpleNamespace(ingest_lance_uri=uri, source_snapshot_seq=window_seq))


def test_applied_completion_marker_short_circuits_a_completed_window(tmp_path: Path) -> None:
    """The ingest runner consults the completion marker before merging.

    A window at or below the durable marker short-circuits with the marker, an unapplied later
    window does not, an absent dataset resolves to no marker, and a same-window different-digest
    surfaces the durable conflict instead of silently re-merging.
    """
    uri: str = str(tmp_path / "ingest.lance")
    lance.write_dataset(pa.table({"record_id": pa.array(["a"], pa.string())}), uri)
    telemetry: Telemetry = Telemetry.create(TelemetryConfig())
    digest: bytes = b"d" * 32
    finalize_completion_marker(uri, 3, digest, telemetry, retry_backoff_seconds=0.0)
    runner: DistributedIngestRunner = DistributedIngestRunner(spark=FakeSpark(), telemetry_config=TelemetryConfig())

    applied: CompletionMarker | None = runner.applied_completion_marker(ingest_probe_context(uri, 3), digest)
    assert applied is not None
    assert applied.window_seq == 3

    superseded: CompletionMarker | None = runner.applied_completion_marker(ingest_probe_context(uri, 2), b"e" * 32)
    assert superseded is not None

    unapplied: CompletionMarker | None = runner.applied_completion_marker(ingest_probe_context(uri, 4), digest)
    assert unapplied is None

    missing_uri: str = str(tmp_path / "missing.lance")
    assert runner.applied_completion_marker(ingest_probe_context(missing_uri, 3), digest) is None

    with pytest.raises(CompletionConflict):
        runner.applied_completion_marker(ingest_probe_context(uri, 3), b"x" * 32)


class FakeColumn:
    """Minimal stand-in for a pyspark ``Column``, inert outside of an active SparkContext."""

    def isNull(self) -> FakeColumn:
        """Return self, standing in for a real null-check expression.

        Returns:
            This column.
        """
        return self

    def alias(self, name: str) -> FakeColumn:
        """Return self, ignoring the requested alias.

        Args:
            name: Ignored alias name.

        Returns:
            This column.
        """
        del name
        return self

    def __gt__(self, other: object) -> FakeColumn:
        """Return self, standing in for a real greater-than comparison.

        Args:
            other: Ignored comparison operand.

        Returns:
            This column.
        """
        del other
        return self


def fake_column(name: object) -> FakeColumn:
    """Return an inert column stand-in regardless of the requested name.

    Args:
        name: Ignored column or expression name.

    Returns:
        A fresh fake column.
    """
    del name
    return FakeColumn()


def patch_inert_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch ``col`` and ``countDistinct`` in the workers module to context-free stand-ins.

    The real pyspark ``col``/``countDistinct`` require an active ``SparkContext`` even to
    construct an inert ``Column``, which a ``FakeSpark``-driven unit test never starts. The
    null-record_id and same-snapshot conflict checks in ``DistributedIngestRunner.run`` only ever
    feed their result into :class:`FakeTerminalFrame`'s own inert chain, so a context-free stand-in
    is behaviorally exact for these tests.

    Args:
        monkeypatch: Active pytest monkeypatch fixture.
    """
    monkeypatch.setattr("lance_etl.reconciler.workers.col", fake_column)
    monkeypatch.setattr("lance_etl.reconciler.workers.countDistinct", fake_column)


class FakeTerminalFrame:
    """Minimal stand-in for the persisted terminal DataFrame consumed by run()'s contract checks."""

    def persist(self, storage_level: object) -> FakeTerminalFrame:
        """Return self, ignoring the requested storage level.

        Args:
            storage_level: Ignored persistence level.

        Returns:
            This frame.
        """
        del storage_level
        return self

    def where(self, condition: object) -> FakeTerminalFrame:
        """Return self, ignoring the filter condition.

        Args:
            condition: Ignored filter expression.

        Returns:
            This frame.
        """
        del condition
        return self

    def limit(self, count: int) -> FakeTerminalFrame:
        """Return self, ignoring the requested row limit.

        Args:
            count: Ignored row limit.

        Returns:
            This frame.
        """
        del count
        return self

    def count(self) -> int:
        """Return zero so neither the null-record_id nor the conflict check ever trips.

        Returns:
            Zero.
        """
        return 0

    def groupBy(self, *columns: object) -> FakeTerminalFrame:
        """Return self, ignoring the grouping columns.

        Args:
            columns: Ignored grouping columns.

        Returns:
            This frame.
        """
        del columns
        return self

    def agg(self, *aggregations: object) -> FakeTerminalFrame:
        """Return self, ignoring the requested aggregations.

        Args:
            aggregations: Ignored aggregation expressions.

        Returns:
            This frame.
        """
        del aggregations
        return self

    def dropDuplicates(self, columns: object) -> FakeTerminalFrame:
        """Return self, ignoring the deduplication columns.

        Args:
            columns: Ignored deduplication columns.

        Returns:
            This frame.
        """
        del columns
        return self

    def unpersist(self) -> None:
        """Record no-op release of the fake persisted frame."""


def fake_execute_spark_scan(spark: object, plan: object) -> object:
    """Return an opaque scan result without touching a real Spark session.

    Args:
        spark: Ignored Spark session.
        plan: Ignored scan plan.

    Returns:
        An opaque scan-result stand-in, forwarded unchanged by the stubbed phase methods.
    """
    del spark, plan
    return object()


def ingest_run_context(claim: WorkClaim) -> WorkExecutionContext:
    """Build a minimal live INGEST execution context for one claim.

    Args:
        claim: Fenced INGEST claim carrying its own source_snapshot_seq.

    Returns:
        Execution context satisfying every guard `DistributedIngestRunner.run` checks before
        dispatching to the overridable phase methods.
    """
    return WorkExecutionContext(
        claim=claim,
        identity=RoutingIdentity("tenant", "ns", "org").validate(),
        source_table="db.events",
        source=source_registration(),
        spec_revision=production_default_spec_revision(),
        snapshot_id=100,
        parent_snapshot_id=None,
        iceberg_sequence_number=10,
        partition_spec_id=0,
        source_snapshot_kind=SourceSnapshotKind.BASELINE,
        candidate_lance_uri=None,
        candidate_lance_version=None,
        artifact_manifest_uri=None,
        artifact_digest=None,
    )


class StubbedPhasesIngestRunner(DistributedIngestRunner):
    """Ingest runner whose scan/validate/select/normalize phases are trivial stand-ins.

    Only the phases after the contract-violation boundary (`compute_source_digest`,
    `applied_completion_marker`, `write_terminal`, `finalize_marker`) are left to each test to
    override or fail, isolating exactly the boundary PR-03 narrows.
    """

    def canonical_source(self, source: object, context: object) -> object:
        """Return the scan output unchanged.

        Args:
            source: Fake upstream scan result.
            context: Ignored execution context.

        Returns:
            The unchanged source.
        """
        del context
        return source

    def validate_source_profile(self, source: object, spec: object) -> None:
        """Accept any source without inspection.

        Args:
            source: Ignored source.
            spec: Ignored dataset specification.
        """
        del source, spec

    def select_profile_fields(self, source: object, spec: object) -> object:
        """Return the source unchanged.

        Args:
            source: Ignored source.
            spec: Ignored dataset specification.

        Returns:
            The unchanged source.
        """
        del spec
        return source

    def normalize_terminal(self, source: object, context: object) -> FakeTerminalFrame:
        """Return a fake terminal frame that never trips the null or conflict checks.

        Args:
            source: Ignored source.
            context: Ignored execution context.

        Returns:
            A fresh fake terminal frame.
        """
        del source, context
        return FakeTerminalFrame()


def test_ingest_runner_write_phase_failure_propagates_as_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A driver-side failure in the write phase becomes a RETRY, not a terminal contract BLOCKED.

    Only the scan/validation phases through the null-record_id and same-snapshot conflict checks
    are genuine contract violations. A ``ValueError`` raised later, from the write phase, must
    propagate out of ``run()`` (proving the narrowed try) so ``execute_isolated`` converts it into
    an ``UNEXPECTED_WORKER_FAILURE`` retry instead of a misleading terminal
    ``SOURCE_PROFILE_VIOLATION`` block.
    """
    monkeypatch.setattr("lance_etl.reconciler.workers.execute_spark_scan", fake_execute_spark_scan)
    patch_inert_columns(monkeypatch)

    class FailingWriteIngestRunner(StubbedPhasesIngestRunner):
        """Ingest runner whose write phase always fails with a non-contract ``ValueError``."""

        def compute_source_digest(self, terminal: object) -> tuple[bytes, int]:
            """Return a fixed digest and row count.

            Args:
                terminal: Ignored terminal frame.

            Returns:
                A fixed digest and row count.
            """
            del terminal
            return b"d" * 32, 3

        def applied_completion_marker(self, context: object, source_digest: bytes) -> CompletionMarker | None:
            """Report the window as not yet applied.

            Args:
                context: Ignored execution context.
                source_digest: Ignored digest.

            Returns:
                ``None``.
            """
            del context, source_digest
            return None

        def write_terminal(self, terminal: object, context: object) -> list[int]:
            """Simulate a driver-side write failure unrelated to the source contract.

            Args:
                terminal: Ignored terminal frame.
                context: Ignored execution context.

            Raises:
                ValueError: Always, simulating an executor write-task failure.
            """
            del terminal, context
            raise ValueError("executor write task failed")

    claim: WorkClaim = work_claim()
    context: WorkExecutionContext = ingest_run_context(claim)
    runner: FailingWriteIngestRunner = FailingWriteIngestRunner(spark=FakeSpark(), telemetry_config=TelemetryConfig())
    with pytest.raises(ValueError, match="executor write task failed"):
        runner.run(context)

    class SingleContextExecutor:
        """Adapter satisfying `WorkExecutor` by running one fixed context regardless of claim."""

        def execute(self, claim: WorkClaim) -> WorkResult:
            """Run the fixed context, ignoring the passed claim identity.

            Args:
                claim: Ignored claim (the fixed context already carries one).

            Returns:
                The runner's result.
            """
            del claim
            return runner.run(context)

    result: WorkResult = execute_isolated(SingleContextExecutor(), claim)
    assert result.kind is ResultKind.RETRY
    assert result.error_code == "UNEXPECTED_WORKER_FAILURE"


def test_ingest_runner_validate_phase_failure_still_blocks_as_contract_violation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `ValueError` from the validation phase still becomes a terminal contract block.

    Regression guard for the narrowed try: the scan/validate/select/normalize phases must stay
    inside the `SOURCE_PROFILE_VIOLATION` boundary even after PR-03 narrows what comes after them.
    """
    monkeypatch.setattr("lance_etl.reconciler.workers.execute_spark_scan", fake_execute_spark_scan)
    patch_inert_columns(monkeypatch)

    class InvalidProfileIngestRunner(StubbedPhasesIngestRunner):
        """Ingest runner whose validation phase always raises a contract `ValueError`."""

        def validate_source_profile(self, source: object, spec: object) -> None:
            """Reject every source as a contract violation.

            Args:
                source: Ignored source.
                spec: Ignored dataset specification.

            Raises:
                ValueError: Always, simulating a genuine contract violation.
            """
            del source, spec
            raise ValueError("source contains an unsupported mutation operation")

    claim: WorkClaim = work_claim()
    context: WorkExecutionContext = ingest_run_context(claim)
    runner: InvalidProfileIngestRunner = InvalidProfileIngestRunner(
        spark=FakeSpark(), telemetry_config=TelemetryConfig()
    )
    result: WorkResult = runner.run(context)
    assert result.kind is ResultKind.BLOCKED
    assert result.error_code == "SOURCE_PROFILE_VIOLATION"


class StopAfterQualify(RuntimeError):
    """Sentinel raised from the mocked gate to end a publish run after ordering is recorded."""


def test_publication_runner_compacts_then_indexes_then_qualifies(monkeypatch: pytest.MonkeyPatch) -> None:
    """A compaction-enabled first publish runs compaction, then indexing, then qualification.

    Regression guard for the suspected ordering defect: the publish worker must maintain the
    candidate and build every index before the qualification gate reads it, so the gate never
    inspects a pre-index compaction output.
    """
    order: list[str] = []

    def record_compaction(*args: object, **kwargs: object) -> list[dict[str, object]]:
        del args, kwargs
        order.append("compaction")
        return [{}]

    maintenance_job: MagicMock = MagicMock()
    maintenance_job.return_value.run.side_effect = record_compaction
    monkeypatch.setattr("lance_etl.reconciler.workers.MaintenanceJob", maintenance_job)

    class RecordingRunner(ConfiguredPublicationRunner):
        """Publication runner whose side-effecting steps only record their invocation order."""

        def candidate_pin_version(self, candidate_uri: str, pin: str) -> int | None:
            """Report the candidate as unpinned so the first-publish branch runs."""
            del candidate_uri, pin
            return None

        def run_indexing(self, candidate_uri: str, spec: DatasetSpecRevision) -> list[dict[str, object]]:
            """Record the indexing step without building any index."""
            del candidate_uri, spec
            order.append("indexing")
            return []

        def candidate_version(self, candidate_uri: str) -> int:
            """Return a fixed post-index head version."""
            del candidate_uri
            return 5

        def pin_candidate(self, candidate_uri: str, candidate_version: int, pin: str) -> None:
            """Skip immutable pin persistence."""
            del candidate_uri, candidate_version, pin

        def candidate_counts(
            self, candidate_uri: str, lance_version: int, spec: DatasetSpecRevision
        ) -> tuple[int, int, int, int]:
            """Return fixed distributed count evidence."""
            del candidate_uri, lance_version, spec
            return (16, 16, 16, 16)

        def qualify_candidate(
            self,
            candidate_uri: str,
            spec: DatasetSpecRevision,
            lance_version: int | None,
            counts: tuple[int, int, int, int] | None,
        ) -> dict[str, object]:
            """Record the qualification step and stop the run before persistence."""
            del candidate_uri, spec, lance_version, counts
            order.append("qualify")
            raise StopAfterQualify

    runner: RecordingRunner = RecordingRunner(MagicMock(), MagicMock(), MagicMock())

    claim: MagicMock = MagicMock()
    claim.kind = WorkKind.PUBLISH
    claim.work_id = uuid.uuid4()
    claim.ingest_lance_uri = "memory://candidate"
    context: MagicMock = MagicMock()
    context.claim = claim
    context.candidate_lance_uri = None
    context.spec_revision = production_default_spec_revision()

    with pytest.raises(StopAfterQualify):
        runner.run(context)
    assert order == ["compaction", "indexing", "qualify"]


def test_persisted_schema_exactly_matches_the_normalized_field_graph() -> None:
    """The writer and publication qualifier share exact order, physical types, and nullability."""
    spec: DatasetSpecRevision = production_default_spec_revision()
    schema: pa.Schema = persisted_arrow_schema(spec)

    assert schema.names == [field.target_name for field in sorted(spec.fields, key=lambda item: item.ordinal)]
    assert schema.field("record_id") == pa.field("record_id", pa.string(), nullable=False)
    assert schema.field("ts") == pa.field("ts", pa.timestamp("us", "UTC"))
    assert schema.field("vector") == pa.field("vector", pa.list_(pa.float32(), 128))
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


def slo_status(healthy: bool) -> SloStatus:
    """Return a minimal SLO status fixture with the requested health.

    Args:
        healthy: Whether the fixture reports a healthy control plane.

    Returns:
        Low-cardinality SLO status fixture.
    """
    return SloStatus(
        healthy=healthy,
        reasons=() if healthy else ("blocked_work",),
        due_work=0,
        blocked_work=0 if healthy else 3,
        blocked_source_snapshots=0,
        oldest_open_age_seconds=0.0,
        retention_age_seconds=0.0,
    )


def test_cli_main_status_exits_zero_when_healthy() -> None:
    """`lance-etl-reconcile status` exits 0 when the evaluated control plane is healthy."""
    application: MagicMock = MagicMock(spec=ReconcilerOperator)
    application.emit_slo_status.return_value = slo_status(healthy=True)
    assert main(["status"], application=application) == 0


def test_cli_main_status_exits_nonzero_when_unhealthy() -> None:
    """`lance-etl-reconcile status` exits nonzero when the evaluated control plane is unhealthy.

    A shell-level health check or cron wrapper must not see success (`0`) on a `status` invocation
    that itself reports `healthy: false`.
    """
    application: MagicMock = MagicMock(spec=ReconcilerOperator)
    application.emit_slo_status.return_value = slo_status(healthy=False)
    assert main(["status"], application=application) == EXIT_UNHEALTHY_STATUS


def test_cli_main_run_once_exits_zero_regardless_of_blocked_work() -> None:
    """`run-once` semantics are unchanged: retries and blocks are normal operation, not a CLI failure."""
    application: MagicMock = MagicMock(spec=ReconcilerApplication)
    application.run_once.return_value = RunOnceSummary(
        planning=EnqueueSummary(None, 0, 0, (), False),
        dispatch=DispatchSummary(claimed=0, succeeded=0, advanced=0, retried=0, blocked=1, stale=0),
        reconciliation=ReconcileSummary(inspected=0, reconciled=0, deferred=0),
        retention=RetentionDecision(False, None, None, None),
        slo=slo_status(healthy=False),
    )
    assert main(["run-once"], application=application) == 0
