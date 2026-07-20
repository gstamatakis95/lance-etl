"""Real-PostgreSQL tests guarding the REBUILD-versus-open-PUBLISH wedge.

A REBUILD freezes the current serving pointer at enqueue time. Enqueuing one while a
PUBLISH is still open used to leave the REBUILD permanently unclaimable once the publish
advanced the pointer, while its still-pending row kept gating every future ingest for the
dataset. These tests pin the three defensive layers: the enqueue guard, the deferred
spec-rollout, and the claim-time sweep plus lane-order exclusion that recover the lane.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine, RowMapping
from test_state_postgres import (
    claim_one,
    complete_and_publish_snapshot,
    dataset_plan,
    draft_revision,
    evidence_for_context,
    postgres_repository,
    register_source,
    source_plan,
)

from lance_etl.state.repository import ControlPlaneRepository, StateTransitionError
from lance_etl.state.specs import DatasetSpecRevision
from lance_etl.state.tables import dataset_work, datasets
from lance_etl.state.types import (
    IcebergSource,
    RoutingIdentity,
    ServingDataset,
    SourceSnapshotKind,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
    WorkState,
)

__all__ = ["postgres_repository"]


def drive_publication(repository: ControlPlaneRepository, claim: WorkClaim, lance_version: int) -> bool:
    """Drive one claimed PUBLISH or REBUILD generation through to a served pointer.

    Args:
        repository: Migrated repository.
        claim: Live PUBLISH or REBUILD claim.
        lance_version: Exact materialized version for the generation.

    Returns:
        Whether the publication was accepted.
    """
    context: WorkExecutionContext | None = repository.work_execution_context(claim)
    assert context is not None
    candidate_uri: str = context.candidate_lance_uri or claim.ingest_lance_uri
    manifest_uri: str = f"file:///tmp/manifests/{claim.work_id}.json"
    manifest_digest: bytes = bytes([(lance_version + 32) % 256]) * 32
    assert repository.advance_phase(
        claim,
        WorkPhase.PREWARM,
        candidate_lance_uri=candidate_uri,
        candidate_lance_version=lance_version,
        artifact_manifest_uri=manifest_uri,
        artifact_digest=manifest_digest,
    )
    return repository.publish_dataset(
        claim,
        candidate_uri,
        lance_version,
        manifest_uri,
        manifest_digest,
        evidence_for_context(context),
    )


def work_rows(engine: Engine) -> list[RowMapping]:
    """Read every work row ordered by lane and creation.

    Args:
        engine: Control-plane engine.

    Returns:
        Ordered work rows.
    """
    with engine.connect() as connection:
        return list(
            connection.execute(
                sa.select(dataset_work).order_by(
                    dataset_work.c.source_snapshot_seq,
                    dataset_work.c.created_at,
                )
            ).mappings()
        )


def test_enqueue_rebuild_rejected_while_publication_open(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """An explicit operator REBUILD is rejected while a PUBLISH generation is open.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    identity: RoutingIdentity = dataset_plan().identity
    complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 1, None, SourceSnapshotKind.BASELINE),
        1,
    )
    repository.enqueue_source_snapshot(source_plan(source.source_id, 101, 2, 100), [dataset_plan()])
    ingest_claim: WorkClaim = claim_one(repository)
    assert ingest_claim.kind is WorkKind.INGEST
    assert repository.complete_ingest(ingest_claim, 2, 10, bytes([2]) * 32)
    with pytest.raises(StateTransitionError, match="publication generation is open"):
        repository.enqueue_rebuild(identity, uuid.uuid4())
    rebuild_rows: list[RowMapping] = [row for row in work_rows(engine) if row["kind"] == WorkKind.REBUILD.value]
    assert rebuild_rows == []


def test_assign_revision_defers_rebuild_while_publication_open(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Spec rollout records the desired revision but defers the REBUILD behind an open publish.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    served: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 1, None, SourceSnapshotKind.BASELINE),
        1,
    )
    draft: DatasetSpecRevision = repository.create_draft_spec_revision(draft_revision(uuid.uuid4()), "tenant-vectors")
    active: DatasetSpecRevision = repository.activate_spec_revision(draft.spec_revision_id)
    repository.enqueue_source_snapshot(source_plan(source.source_id, 101, 2, 100), [dataset_plan()])
    ingest_claim: WorkClaim = claim_one(repository)
    assert ingest_claim.kind is WorkKind.INGEST
    assert repository.complete_ingest(ingest_claim, 2, 10, bytes([2]) * 32)
    deferred: uuid.UUID | None = repository.assign_dataset_spec_revision(served.dataset_id, active.spec_revision_id)
    assert deferred is None
    rebuild_rows: list[RowMapping] = [row for row in work_rows(engine) if row["kind"] == WorkKind.REBUILD.value]
    assert rebuild_rows == []
    with engine.connect() as connection:
        desired: uuid.UUID | None = connection.scalar(
            sa.select(datasets.c.desired_spec_revision_id).where(datasets.c.dataset_id == served.dataset_id)
        )
    assert desired == active.spec_revision_id


def test_stale_rebuild_is_swept_and_lane_recovers(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """A REBUILD stranded past the enqueue guard is swept to BLOCKED and never wedges the lane.

    The REBUILD is enqueued while the next INGEST is still in flight, so no PUBLISH row yet
    exists and the enqueue guard cannot fire. Completing that INGEST and publishing advances
    the serving pointer, stranding the REBUILD. The claim-time sweep must block it with error
    evidence, and a later snapshot INGEST must remain claimable.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    identity: RoutingIdentity = dataset_plan().identity
    complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 1, None, SourceSnapshotKind.BASELINE),
        1,
    )
    repository.enqueue_source_snapshot(source_plan(source.source_id, 101, 2, 100), [dataset_plan()])
    ingest_claim: WorkClaim = claim_one(repository)
    assert ingest_claim.kind is WorkKind.INGEST
    rebuild_id: uuid.UUID = repository.enqueue_rebuild(identity, uuid.uuid4())
    assert repository.complete_ingest(ingest_claim, 2, 10, bytes([2]) * 32)
    publication: WorkClaim = claim_one(repository)
    assert publication.kind is WorkKind.PUBLISH
    assert drive_publication(repository, publication, 2)
    blocked: RowMapping = next(row for row in work_rows(engine) if row["work_id"] == rebuild_id)
    assert blocked["state"] == WorkState.BLOCKED.value
    assert blocked["error_code"] == "EXPECTATION_STALE"
    assert blocked["error_message"] is not None
    repository.enqueue_source_snapshot(source_plan(source.source_id, 102, 3, 101), [dataset_plan()])
    later: list[WorkClaim] = repository.claim_due_work(5, timedelta(minutes=5))
    assert any(claim.kind is WorkKind.INGEST and claim.source_snapshot_seq == 3 for claim in later)
    assert repository.retry_blocked_work(rebuild_id)
