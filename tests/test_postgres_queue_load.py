"""Bounded real-PostgreSQL queue contention qualification."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from lance_etl.state import (
    DatasetPlan,
    IcebergSource,
    RoutingIdentity,
    SourceSnapshotKind,
    SourceSnapshotPlan,
    WorkClaim,
)
from lance_etl.state.repository import ControlPlaneRepository, build_control_plane_engine
from lance_etl.state.tables import dataset_work

pytestmark = pytest.mark.integration

POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"
"""Explicit database URL enabling destructive isolated-schema queue tests."""

QUEUE_DATASETS_ENV: str = "LANCE_ETL_QUEUE_LOAD_DATASETS"
"""Optional bounded queue dataset override for larger external qualification."""

MAX_QUEUE_DATASETS: int = 50_000
"""Hard ceiling preventing an accidental unbounded developer-host load test."""

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent
"""Repository root used to locate Alembic migrations."""


def isolated_url(base_url: URL, schema_name: str) -> URL:
    """Select a fresh PostgreSQL schema through the session search path.

    Args:
        base_url: PostgreSQL URL provided by the operator.
        schema_name: Unique test schema.

    Returns:
        Psycopg URL scoped to the test schema.
    """
    if base_url.get_backend_name() != "postgresql":
        raise ValueError(f"{POSTGRES_URL_ENV} must select PostgreSQL")
    return base_url.set(drivername="postgresql+psycopg").update_query_dict({"options": f"-csearch_path={schema_name}"})


@pytest.fixture
def queue_repository() -> Iterator[tuple[ControlPlaneRepository, Engine]]:
    """Migrate an isolated schema and yield a real control-plane repository.

    Yields:
        Repository and engine scoped to a disposable PostgreSQL schema.
    """
    raw_url: str | None = os.environ.get(POSTGRES_URL_ENV)
    if raw_url is None:
        pytest.skip(f"set {POSTGRES_URL_ENV} to run real-PostgreSQL queue load qualification")
    base_url: URL = make_url(raw_url).set(drivername="postgresql+psycopg")
    schema_name: str = f"lance_etl_queue_{uuid.uuid4().hex}"
    admin_engine: Engine = sa.create_engine(base_url)
    with admin_engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    test_url: URL = isolated_url(base_url, schema_name)
    rendered_url: str = test_url.render_as_string(hide_password=False)
    engine: Engine | None = None
    try:
        alembic_config: Config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
        alembic_config.set_main_option("script_location", str(REPOSITORY_ROOT / "migrations"))
        alembic_config.set_main_option("sqlalchemy.url", rendered_url.replace("%", "%%"))
        command.upgrade(alembic_config, "head")
        engine = build_control_plane_engine(rendered_url)
        yield ControlPlaneRepository(engine), engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        admin_engine.dispose()


def queue_dataset_count() -> int:
    """Resolve and bound the operator-selected dataset count.

    Returns:
        Positive dataset count no greater than :data:`MAX_QUEUE_DATASETS`.

    Raises:
        ValueError: If the override falls outside the safe range.
    """
    count: int = int(os.environ.get(QUEUE_DATASETS_ENV, "256"))
    if count < 1 or count > MAX_QUEUE_DATASETS:
        raise ValueError(f"{QUEUE_DATASETS_ENV} must be between 1 and {MAX_QUEUE_DATASETS}")
    return count


def drain_worker(repository: ControlPlaneRepository) -> list[uuid.UUID]:
    """Claim and terminally block disjoint batches until the queue is empty.

    Args:
        repository: Shared thread-safe repository.

    Returns:
        Work identities claimed by this worker.
    """
    claimed: list[uuid.UUID] = []
    while True:
        batch: list[WorkClaim] = repository.claim_due_work(16, timedelta(minutes=5))
        if not batch:
            return claimed
        claim: WorkClaim
        for claim in batch:
            assert repository.block_work(claim, "QUALIFIED", "bounded queue load terminal state")
            claimed.append(claim.work_id)


def test_skip_locked_queue_load_is_complete_disjoint_and_bounded(
    queue_repository: tuple[ControlPlaneRepository, Engine], record_property: Any
) -> None:
    """Concurrent claimers drain every dataset once without duplicates.

    Args:
        queue_repository: Fresh real-PostgreSQL repository and engine.
        record_property: Pytest result-property recorder for throughput evidence.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = queue_repository
    source: IcebergSource = repository.ensure_source_registration(
        source_name="queue_vectors",
        source_table="local.lake.raw_vectors",
        table_uuid=uuid.uuid4(),
        canonical_baseline_snapshot_id=1,
        lance_base_uri="file:///tmp/lance-etl-queue",
    )
    dataset_count: int = queue_dataset_count()
    plans: list[DatasetPlan] = [
        DatasetPlan(identity=RoutingIdentity(tenant_id="tenant0", namespace="vectors", org_id=f"org-{index:05d}"))
        for index in range(dataset_count)
    ]
    repository.enqueue_source_snapshot(
        SourceSnapshotPlan(
            source_id=source.source_id,
            snapshot_id=1,
            parent_snapshot_id=None,
            iceberg_sequence_number=1,
            partition_spec_id=7,
            committed_at=datetime(2026, 1, 1, tzinfo=UTC),
            iceberg_operation="append",
            kind=SourceSnapshotKind.BASELINE,
        ),
        plans,
    )
    started: float = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as executor:
        batches: list[list[uuid.UUID]] = list(executor.map(drain_worker, repeat(repository, 8)))
    elapsed_seconds: float = time.perf_counter() - started
    claimed: list[uuid.UUID] = [work_id for batch in batches for work_id in batch]
    with engine.connect() as connection:
        states: list[tuple[str, int]] = list(
            connection.execute(sa.select(dataset_work.c.state, dataset_work.c.attempt_count)).tuples()
        )
    record_property("queue_datasets", dataset_count)
    record_property("queue_elapsed_seconds", elapsed_seconds)
    record_property("queue_claims_per_second", dataset_count / elapsed_seconds)
    assert len(claimed) == dataset_count
    assert len(set(claimed)) == dataset_count
    assert states == [("BLOCKED", 1)] * dataset_count
