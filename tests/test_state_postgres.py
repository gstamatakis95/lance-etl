"""Real-PostgreSQL integration tests for durable control-plane transitions."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from lance_etl.state.repository import ControlPlaneRepository, StateTransitionError, build_control_plane_engine
from lance_etl.state.tables import source_windows, target_work, targets
from lance_etl.state.types import (
    RoutingIdentity,
    SourceWindowKind,
    SourceWindowPlan,
    SourceWindowState,
    TargetPlan,
    WorkKind,
    WorkPhase,
)

POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"
"""Explicit database URL required to enable destructive isolated-schema tests."""

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent
"""Repository root used to locate the Alembic configuration."""


def psycopg_url(raw_url: str) -> URL:
    """Normalize a PostgreSQL test URL onto the required psycopg 3 driver.

    Args:
        raw_url: Explicit integration database URL.

    Returns:
        SQLAlchemy URL selecting ``postgresql+psycopg``.
    """
    url: URL = make_url(raw_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError(f"{POSTGRES_URL_ENV} must select PostgreSQL")
    return url.set(drivername="postgresql+psycopg")


def schema_url(base_url: URL, schema_name: str) -> URL:
    """Build a URL whose sessions use one isolated test schema.

    Args:
        base_url: Base PostgreSQL URL.
        schema_name: Newly created schema name.

    Returns:
        URL carrying a PostgreSQL startup search-path option.
    """
    return base_url.update_query_dict({"options": f"-csearch_path={schema_name}"})


@pytest.fixture
def postgres_repository() -> Iterator[tuple[ControlPlaneRepository, Engine]]:
    """Migrate an isolated schema and yield a repository plus its engine.

    Yields:
        Repository and engine backed by a freshly migrated PostgreSQL schema.
    """
    raw_url: str | None = os.environ.get(POSTGRES_URL_ENV)
    if raw_url is None:
        pytest.skip(f"set {POSTGRES_URL_ENV} to run real-PostgreSQL control-plane tests")
    base_url: URL = psycopg_url(raw_url)
    schema_name: str = f"lance_etl_test_{uuid.uuid4().hex}"
    admin_engine: Engine = sa.create_engine(base_url)
    with admin_engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    isolated_url: URL = schema_url(base_url, schema_name)
    isolated_url_string: str = isolated_url.render_as_string(hide_password=False)
    engine: Engine | None = None
    try:
        alembic_config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
        alembic_config.set_main_option("script_location", str(REPOSITORY_ROOT / "migrations"))
        alembic_config.set_main_option("sqlalchemy.url", isolated_url_string.replace("%", "%%"))
        command.upgrade(alembic_config, "head")
        engine = build_control_plane_engine(isolated_url_string)
        yield ControlPlaneRepository(engine, "s3://test-bucket/lance"), engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        admin_engine.dispose()


def target_plan() -> TargetPlan:
    """Return one valid release-owned target plan.

    Returns:
        Stable target plan used across repository tests.
    """
    return TargetPlan(
        identity=RoutingIdentity(tenant_id="tenant1", namespace="namespace1", org_id="org1"),
        profile_id="production_v1",
    )


def source_plan(
    table_uuid: uuid.UUID,
    snapshot_id: int,
    sequence_number: int,
    parent_snapshot_id: int | None,
    kind: SourceWindowKind = SourceWindowKind.APPEND,
) -> SourceWindowPlan:
    """Build a source-window plan.

    Args:
        table_uuid: Iceberg table identity.
        snapshot_id: Exact snapshot identifier.
        sequence_number: Iceberg sequence number.
        parent_snapshot_id: Direct parent snapshot.
        kind: Source classification.

    Returns:
        Immutable source-window plan.
    """
    return SourceWindowPlan(
        table_uuid=table_uuid,
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent_snapshot_id,
        iceberg_sequence_number=sequence_number,
        kind=kind,
    )


def table_counts(engine: Engine) -> tuple[int, int, int]:
    """Count durable source, target, and work rows.

    Args:
        engine: Migrated PostgreSQL engine.

    Returns:
        Counts in source, target, work order.
    """
    with engine.connect() as connection:
        return (
            int(connection.scalar(sa.select(sa.func.count()).select_from(source_windows)) or 0),
            int(connection.scalar(sa.select(sa.func.count()).select_from(targets)) or 0),
            int(connection.scalar(sa.select(sa.func.count()).select_from(target_work)) or 0),
        )


def test_migration_creates_exactly_three_application_tables(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Alembic creates the approved tables and only its own metadata beside them.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del repository
    names: set[str] = set(sa.inspect(engine).get_table_names())
    assert names == {"alembic_version", "source_windows", "targets", "target_work"}


def test_duplicate_plan_ordered_ingest_and_serve_coalescing(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Planning and completion are idempotent, ordered, and separate from serving.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    table_uuid: uuid.UUID = uuid.uuid4()
    first_plan = source_plan(table_uuid, 100, 10, None, SourceWindowKind.BASELINE)
    second_plan = source_plan(table_uuid, 101, 11, 100)
    first_seq: int = repository.enqueue_source_window(first_plan, [target_plan()])
    assert repository.enqueue_source_window(first_plan, [target_plan()]) == first_seq
    second_seq: int = repository.enqueue_source_window(second_plan, [target_plan()])
    assert second_seq > first_seq
    assert table_counts(engine) == (2, 1, 2)

    first_claims = repository.claim_due_work(10, timedelta(minutes=5))
    assert len(first_claims) == 1
    first_claim = first_claims[0]
    assert first_claim.source_window_seq == first_seq
    assert repository.complete_ingest(first_claim, 1, 3, b"a" * 32)
    assert repository.complete_ingest(first_claim, 1, 3, b"a" * 32)

    restarted = ControlPlaneRepository(engine, "s3://test-bucket/lance")
    second_claims = restarted.claim_due_work(10, timedelta(minutes=5))
    assert len(second_claims) == 1
    second_claim = second_claims[0]
    assert second_claim.kind == WorkKind.INGEST
    assert second_claim.source_window_seq == second_seq
    assert restarted.complete_ingest(second_claim, 2, 4, b"b" * 32)

    serve_claims = repository.claim_due_work(10, timedelta(minutes=5))
    assert len(serve_claims) == 1
    serve_claim = serve_claims[0]
    assert serve_claim.kind == WorkKind.SERVE
    assert serve_claim.data_lance_version == 2
    assert repository.advance_phase(serve_claim, WorkPhase.INDEX, indexed_lance_version=2)
    assert repository.advance_phase(
        serve_claim,
        WorkPhase.VALIDATE,
        artifact_manifest_uri="s3://artifacts/manifest.json",
        artifact_digest=b"c" * 32,
    )
    assert repository.complete_serve(serve_claim, 2, "s3://artifacts/manifest.json", b"c" * 32)

    with engine.connect() as connection:
        window_states: list[str] = list(
            connection.execute(sa.select(source_windows.c.state).order_by(source_windows.c.window_seq)).scalars()
        )
        serve_count: int = int(
            connection.scalar(
                sa.select(sa.func.count()).select_from(target_work).where(target_work.c.kind == WorkKind.SERVE.value)
            )
            or 0
        )
        target_row = connection.execute(targets.select()).mappings().one()
    assert window_states == [SourceWindowState.COMPLETE.value, SourceWindowState.COMPLETE.value]
    assert serve_count == 1
    assert target_row["last_applied_window_seq"] == second_seq
    assert target_row["last_applied_lance_version"] == 2
    assert repository.retention_floor() is None


def test_fenced_publication_updates_exact_serving_catalog_atomically(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """PREWARM completion, catalog CAS, and work success form one idempotent transaction.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    plan = target_plan()
    window_seq = repository.enqueue_source_window(
        source_plan(uuid.uuid4(), 120, 12, None, SourceWindowKind.BASELINE),
        [plan],
    )
    ingest_claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert ingest_claim.source_window_seq == window_seq
    assert repository.complete_ingest(ingest_claim, 3, 10, b"a" * 32)
    assert repository.resolve_serving_target(plan.identity) is None
    serve_claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert repository.advance_phase(serve_claim, WorkPhase.INDEX, indexed_lance_version=4)
    assert repository.advance_phase(
        serve_claim,
        WorkPhase.VALIDATE,
        artifact_manifest_uri="s3://artifacts/work.json",
        artifact_digest=b"b" * 32,
    )
    assert repository.advance_phase(serve_claim, WorkPhase.PREWARM)
    candidate_uri = serve_claim.expected_ingest_lance_uri
    assert repository.publish_serve(
        serve_claim,
        candidate_uri,
        4,
        "s3://artifacts/work.json",
        b"b" * 32,
    )
    assert repository.publish_serve(
        serve_claim,
        candidate_uri,
        4,
        "s3://artifacts/work.json",
        b"b" * 32,
    )
    served = repository.resolve_serving_target(plan.identity)
    assert served is not None
    assert served.lance_uri == candidate_uri
    assert served.lance_version == 4
    assert served.profile_id == plan.profile_id
    with engine.connect() as connection:
        work_row = (
            connection.execute(target_work.select().where(target_work.c.work_id == serve_claim.work_id))
            .mappings()
            .one()
        )
    assert work_row["state"] == "SUCCEEDED"
    assert work_row["phase"] == "PUBLISH"


def test_duplicate_window_rejects_a_changed_target_set(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """A replay cannot mutate the target set sealed atomically with a source window.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    plan = source_plan(uuid.uuid4(), 150, 15, None, SourceWindowKind.BASELINE)
    repository.enqueue_source_window(plan, [target_plan()])
    with pytest.raises(StateTransitionError, match="target set"):
        repository.enqueue_source_window(plan, [])


def test_expired_lease_reclaims_and_fences_stale_worker(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """An expired lease is reclaimed and its former owner cannot transition state.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    table_uuid: uuid.UUID = uuid.uuid4()
    repository.enqueue_source_window(
        source_plan(table_uuid, 200, 20, None, SourceWindowKind.BASELINE),
        [target_plan()],
    )
    start: datetime = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    old_claim = repository.claim_due_work(1, timedelta(seconds=30), start)[0]
    new_claim = repository.claim_due_work(1, timedelta(seconds=30), start + timedelta(seconds=31))[0]
    assert new_claim.work_id == old_claim.work_id
    assert new_claim.lease_token != old_claim.lease_token
    assert new_claim.fence_epoch == old_claim.fence_epoch + 1
    assert new_claim.attempt_count == 2
    assert not repository.renew_lease(old_claim, timedelta(minutes=1), start + timedelta(seconds=32))
    assert not repository.retry_work(old_claim, timedelta(0), "TRANSIENT", "stale")
    assert not repository.complete_ingest(old_claim, 1, 1, b"a" * 32)
    assert repository.retry_work(new_claim, timedelta(minutes=2), "X" * 200, "Y" * 3000, start)


def test_expired_claim_cannot_transition_before_another_worker_reclaims(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Lease expiry itself fences renew, phase, retry, block, and completion transitions.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    repository.enqueue_source_window(
        source_plan(uuid.uuid4(), 210, 21, None, SourceWindowKind.BASELINE),
        [target_plan()],
    )
    start = datetime(2026, 7, 14, 13, 0, tzinfo=UTC)
    claim = repository.claim_due_work(1, timedelta(seconds=30), start)[0]
    expired = start + timedelta(seconds=31)
    assert repository.work_execution_context(claim, expired) is None
    assert not repository.renew_lease(claim, timedelta(minutes=1), expired)
    assert not repository.advance_phase(claim, WorkPhase.INDEX, now=expired)
    assert not repository.retry_work(claim, timedelta(0), "TRANSIENT", "expired", expired)
    assert not repository.block_work(claim, "CONTRACT", "expired", expired)
    assert not repository.complete_ingest(claim, 1, 1, b"a" * 32, expired)


def test_work_execution_context_is_exact_only_while_claim_is_live(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """A live claim resolves exact routing and source metadata, while stale tokens resolve nothing.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    plan = target_plan()
    repository.enqueue_source_window(
        source_plan(uuid.uuid4(), 220, 22, None, SourceWindowKind.BASELINE),
        [plan],
    )
    start = datetime(2026, 7, 14, 14, 0, tzinfo=UTC)
    claim = repository.claim_due_work(1, timedelta(minutes=5), start)[0]
    context = repository.work_execution_context(claim, start + timedelta(minutes=1))
    assert context is not None
    assert context.claim == claim
    assert context.identity == plan.identity
    assert context.profile_id == plan.profile_id
    assert context.snapshot_id == 220
    assert context.parent_snapshot_id is None
    assert context.iceberg_sequence_number == 22
    assert context.source_window_kind == SourceWindowKind.BASELINE
    assert context.candidate_lance_uri is None
    assert repository.work_execution_context(claim, start + timedelta(minutes=6)) is None


def test_concurrent_claimers_cannot_own_the_same_target(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Target-row locks and SKIP LOCKED yield one owner under concurrent claimers.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    repository.enqueue_source_window(
        source_plan(uuid.uuid4(), 300, 30, None, SourceWindowKind.BASELINE),
        [target_plan()],
    )

    def claim_once() -> list[Any]:
        """Claim from a separately constructed repository instance."""
        contender = ControlPlaneRepository(engine, "s3://test-bucket/lance")
        return contender.claim_due_work(1, timedelta(minutes=5))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(claim_once), executor.submit(claim_once)]
        results: list[list[Any]] = [future.result() for future in futures]
    claims: list[Any] = [claim for result in results for claim in result]
    assert len(claims) == 1


def test_blocked_ingest_holds_retention_and_prevents_later_ingest(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """A blocked source contract holds its exact retention floor and target lane.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    table_uuid: uuid.UUID = uuid.uuid4()
    first_seq: int = repository.enqueue_source_window(
        source_plan(table_uuid, 400, 40, None, SourceWindowKind.BASELINE),
        [target_plan()],
    )
    repository.enqueue_source_window(source_plan(table_uuid, 401, 41, 400), [target_plan()])
    claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert repository.block_work(claim, "SOURCE_CONFLICT", "two distinct mutations")
    assert repository.claim_due_work(10, timedelta(minutes=5)) == []
    floor = repository.retention_floor()
    assert floor is not None
    assert floor["window_seq"] == first_seq
    assert floor["state"] == SourceWindowState.SEALED.value
    assert repository.retry_blocked_work(claim.work_id)
    retried = repository.claim_due_work(1, timedelta(minutes=5))
    assert len(retried) == 1
    assert retried[0].work_id == claim.work_id


def test_control_plane_status_aggregates_queue_and_exact_retention_floor(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """The reconciler status API remains constant-size while preserving the exact source floor.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    del engine
    plan = source_plan(uuid.uuid4(), 500, 50, None, SourceWindowKind.BASELINE)
    window_seq = repository.enqueue_source_window(plan, [target_plan()])
    before_claim = repository.control_plane_status()
    assert before_claim.pending_work == 1
    assert before_claim.due_work == 1
    assert before_claim.retention_window_seq == window_seq
    assert before_claim.retention_snapshot_id == 500
    assert before_claim.retention_parent_snapshot_id is None
    assert before_claim.retention_state == SourceWindowState.SEALED

    claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    running = repository.control_plane_status()
    assert running.pending_work == 0
    assert running.running_work == 1
    assert running.retention_window_seq == window_seq

    assert repository.complete_ingest(claim, 1, 1, b"z" * 32)
    applied = repository.control_plane_status()
    assert applied.retention_window_seq is None
    assert applied.pending_work == 1
    assert applied.blocked_source_windows == 0


def test_rollback_reuses_retained_evidence_and_publishes_with_catalog_cas(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Rollback PREWARM republishes a retained candidate while leaving the ingest URI unchanged.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository, engine = postgres_repository
    plan = target_plan()
    table_uuid = uuid.uuid4()
    repository.enqueue_source_window(
        source_plan(table_uuid, 600, 60, None, SourceWindowKind.BASELINE),
        [plan],
    )
    ingest_claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    ingest_uri = ingest_claim.expected_ingest_lance_uri
    assert repository.complete_ingest(ingest_claim, 3, 10, b"a" * 32)
    first = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert repository.advance_phase(first, WorkPhase.INDEX, indexed_lance_version=4)
    assert repository.advance_phase(first, WorkPhase.VALIDATE)
    assert repository.advance_phase(first, WorkPhase.PREWARM)
    first_uri = "s3://test-bucket/candidates/generation-one.lance"
    assert repository.publish_serve(first, first_uri, 4, "s3://artifacts/one.json", b"b" * 32)

    repository.enqueue_source_window(source_plan(table_uuid, 601, 61, 600), [plan])
    second_ingest = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert repository.complete_ingest(second_ingest, 5, 11, b"c" * 32)
    second = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert repository.advance_phase(second, WorkPhase.INDEX, indexed_lance_version=6)
    assert repository.advance_phase(second, WorkPhase.VALIDATE)
    assert repository.advance_phase(second, WorkPhase.PREWARM)
    second_uri = "s3://test-bucket/candidates/generation-two.lance"
    assert repository.publish_serve(second, second_uri, 6, "s3://artifacts/two.json", b"d" * 32)
    currently_served = repository.resolve_serving_target(plan.identity)
    assert currently_served is not None
    assert currently_served.lance_uri == second_uri
    assert currently_served.lance_version == 6

    rollback_id = repository.enqueue_rollback(plan.identity, first.work_id)
    with pytest.raises(StateTransitionError, match="idle target lane"):
        repository.enqueue_rollback(plan.identity, first.work_id)
    rollback_claim = repository.claim_due_work(1, timedelta(minutes=5))[0]
    assert rollback_claim.work_id == rollback_id
    assert rollback_claim.phase == WorkPhase.PREWARM
    context = repository.work_execution_context(rollback_claim)
    assert context is not None
    assert context.candidate_lance_uri == first_uri
    assert context.indexed_lance_version == 4
    assert context.artifact_manifest_uri == "s3://artifacts/one.json"
    assert context.artifact_digest == b"b" * 32
    assert repository.publish_serve(
        rollback_claim,
        context.candidate_lance_uri,
        context.indexed_lance_version,
        context.artifact_manifest_uri,
        context.artifact_digest,
    )
    served = repository.resolve_serving_target(plan.identity)
    assert served is not None
    assert served.lance_uri == first_uri
    assert served.lance_version == 4
    with engine.connect() as connection:
        target_row = connection.execute(targets.select()).mappings().one()
    assert target_row["ingest_lance_uri"] == ingest_uri
