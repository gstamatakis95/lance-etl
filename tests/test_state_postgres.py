"""Real-PostgreSQL tests for the normalized dataset control plane."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, Engine, RowMapping, make_url
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateSchema, DropSchema

from lance_etl.state.repository import ControlPlaneRepository, StateTransitionError, build_control_plane_engine
from lance_etl.state.settings import ReconcilerSettings
from lance_etl.state.specs import (
    DEFAULT_SPEC_REVISION_ID,
    DatasetField,
    DatasetSpecRevision,
    IndexDefinition,
    SpecRevisionState,
    production_default_spec_revision,
)
from lance_etl.state.tables import (
    dataset_fields,
    dataset_publications,
    dataset_spec_revisions,
    dataset_specs,
    dataset_state,
    dataset_work,
    datasets,
    fts_index_options,
    iceberg_sources,
    index_definitions,
    metadata,
    publication_indexes,
    reconciler_settings,
    source_snapshots,
    vector_index_options,
)
from lance_etl.state.types import (
    ControlPlaneStatus,
    DatasetPlan,
    IcebergSource,
    PublicationCleanup,
    PublicationEvidence,
    PublicationIndexEvidence,
    RoutingIdentity,
    ServingDataset,
    SourceSnapshotKind,
    SourceSnapshotPlan,
    SourceSnapshotState,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkLauncherKind,
    WorkPhase,
    WorkProvenance,
    WorkState,
    deterministic_dataset_id,
)

POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"
"""Explicit database URL required to enable isolated-schema tests."""

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent
"""Repository root used to locate Alembic configuration."""

APPLICATION_TABLES: set[str] = {
    "reconciler_settings",
    "dataset_specs",
    "dataset_spec_revisions",
    "dataset_fields",
    "index_definitions",
    "vector_index_options",
    "fts_index_options",
    "iceberg_sources",
    "datasets",
    "source_snapshots",
    "dataset_work",
    "dataset_publications",
    "publication_indexes",
    "dataset_state",
}
"""Exact application tables installed by the single Alembic baseline."""


def psycopg_url(raw_url: str) -> URL:
    """Normalize a PostgreSQL test URL onto the psycopg 3 driver.

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
        alembic_config: Config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
        alembic_config.set_main_option("script_location", str(REPOSITORY_ROOT / "migrations"))
        alembic_config.set_main_option("sqlalchemy.url", isolated_url_string.replace("%", "%%"))
        command.upgrade(alembic_config, "head")
        engine = build_control_plane_engine(isolated_url_string)
        yield ControlPlaneRepository(engine), engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        admin_engine.dispose()


def register_source(
    repository: ControlPlaneRepository,
    table_uuid: uuid.UUID | None = None,
    baseline_snapshot_id: int | None = 100,
) -> IcebergSource:
    """Register and return one active local Iceberg source.

    Args:
        repository: Migrated repository.
        table_uuid: Optional fixed table UUID.
        baseline_snapshot_id: Optional immutable baseline pin.

    Returns:
        Database-owned source registration.
    """
    return repository.ensure_source_registration(
        source_name="vectors",
        source_table="local.lake.raw_vectors",
        table_uuid=table_uuid or uuid.uuid4(),
        canonical_baseline_snapshot_id=baseline_snapshot_id,
        lance_base_uri="file:///tmp/lance-etl-test",
    )


def dataset_plan(org_id: str = "org1") -> DatasetPlan:
    """Build one valid logical dataset plan.

    Args:
        org_id: Route suffix allowing multiple test datasets.

    Returns:
        Valid route plan.
    """
    return DatasetPlan(identity=RoutingIdentity(tenant_id="tenant1", namespace="vectors", org_id=org_id))


def draft_revision(
    spec_id: uuid.UUID,
    revision_number: int = 1,
    supersedes_revision_id: uuid.UUID | None = None,
) -> DatasetSpecRevision:
    """Clone the bundled graph under fresh normalized identities as a DRAFT.

    Args:
        spec_id: Parent named specification identity.
        revision_number: Positive revision sequence.
        supersedes_revision_id: Optional prior revision in the same specification.

    Returns:
        Complete draft whose deliberately empty digest must be recomputed by the repository.
    """
    base: DatasetSpecRevision = production_default_spec_revision()
    revision_id: uuid.UUID = uuid.uuid4()
    field_ids: dict[uuid.UUID, uuid.UUID] = {field.field_id: uuid.uuid4() for field in base.fields}
    fields: tuple[DatasetField, ...] = tuple(
        replace(field, field_id=field_ids[field.field_id], spec_revision_id=revision_id) for field in base.fields
    )
    indexes: tuple[IndexDefinition, ...] = tuple(
        replace(
            index,
            index_definition_id=uuid.uuid4(),
            spec_revision_id=revision_id,
            field_id=field_ids[index.field_id],
        )
        for index in base.indexes
    )
    return replace(
        base,
        spec_id=spec_id,
        spec_revision_id=revision_id,
        revision_number=revision_number,
        state=SpecRevisionState.DRAFT,
        supersedes_revision_id=supersedes_revision_id,
        configuration_digest=b"",
        fields=fields,
        indexes=indexes,
    )


def source_plan(
    source_id: uuid.UUID,
    snapshot_id: int,
    sequence_number: int,
    parent_snapshot_id: int | None,
    kind: SourceSnapshotKind = SourceSnapshotKind.APPEND,
) -> SourceSnapshotPlan:
    """Build an immutable source-snapshot plan.

    Args:
        source_id: Registered source identity.
        snapshot_id: Exact Iceberg snapshot identifier.
        sequence_number: Exact Iceberg sequence number.
        parent_snapshot_id: Direct parent snapshot identifier.
        kind: Accepted or rejected source classification.

    Returns:
        Valid source-snapshot plan.
    """
    return SourceSnapshotPlan(
        source_id=source_id,
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent_snapshot_id,
        iceberg_sequence_number=sequence_number,
        partition_spec_id=7,
        committed_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=sequence_number),
        iceberg_operation="append",
        kind=kind,
    )


def claim_one(
    repository: ControlPlaneRepository,
    now: datetime | None = None,
    lease_duration: timedelta = timedelta(minutes=5),
) -> WorkClaim:
    """Claim exactly one due work item.

    Args:
        repository: Repository containing due work.
        now: Optional deterministic claim clock.
        lease_duration: Requested positive lease duration.

    Returns:
        Sole claimed work item.
    """
    claims: list[WorkClaim] = repository.claim_due_work(1, lease_duration, now=now)
    assert len(claims) == 1
    return claims[0]


def evidence_for_context(context: WorkExecutionContext, fragments: int = 2) -> PublicationEvidence:
    """Build complete publication evidence for a frozen specification.

    Args:
        context: Live work execution context.
        fragments: Candidate fragment count.

    Returns:
        Evidence covering every configured index exactly once.
    """
    return PublicationEvidence(
        schema_digest=b"s" * 32,
        total_row_count=10,
        distinct_row_count=10,
        live_row_count=9,
        distinct_live_row_count=9,
        fragment_count=fragments,
        indexes=tuple(
            PublicationIndexEvidence(
                index_definition_id=index.index_definition_id,
                actual_index_type=index.index_type,
                indexed_fragment_count=fragments,
                unindexed_fragment_count=0,
                artifact_generation_digest=b"i" * 32,
            )
            for index in context.spec_revision.indexes
        ),
    )


def complete_and_publish_snapshot(
    repository: ControlPlaneRepository,
    plan: SourceSnapshotPlan,
    lance_version: int,
) -> ServingDataset:
    """Execute repository transitions for one single-dataset source snapshot.

    Args:
        repository: Migrated repository.
        plan: Accepted source snapshot.
        lance_version: Exact materialized version for the generation.

    Returns:
        Newly active serving dataset.
    """
    repository.enqueue_source_snapshot(plan, [dataset_plan()])
    ingest_claim: WorkClaim = claim_one(repository)
    assert ingest_claim.kind is WorkKind.INGEST
    assert repository.complete_ingest(ingest_claim, lance_version, 10, bytes([lance_version]) * 32)
    publish_claim: WorkClaim = claim_one(repository)
    assert publish_claim.kind is WorkKind.PUBLISH
    context: WorkExecutionContext | None = repository.work_execution_context(publish_claim)
    assert context is not None
    manifest_uri: str = f"file:///tmp/manifests/{publish_claim.work_id}.json"
    manifest_digest: bytes = bytes([lance_version + 32]) * 32
    assert repository.advance_phase(
        publish_claim,
        WorkPhase.PREWARM,
        candidate_lance_uri=publish_claim.ingest_lance_uri,
        candidate_lance_version=lance_version,
        artifact_manifest_uri=manifest_uri,
        artifact_digest=manifest_digest,
    )
    assert repository.publish_dataset(
        publish_claim,
        publish_claim.ingest_lance_uri,
        lance_version,
        manifest_uri,
        manifest_digest,
        evidence_for_context(context),
    )
    served: ServingDataset | None = repository.resolve_serving_dataset(dataset_plan().identity)
    assert served is not None
    return served


def test_migration_creates_exact_entities_and_seeded_typed_configuration(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Alembic creates fourteen entities and one normalized default revision.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    assert set(sa.inspect(engine).get_table_names()) == APPLICATION_TABLES | {"alembic_version"}
    inspector: Inspector = sa.inspect(engine)
    expected_field_checks: set[str] = {
        "ck_dataset_fields_flags_contract",
        "ck_dataset_fields_map_key_target",
        "ck_dataset_fields_source_contract",
        "ck_dataset_fields_type_contract",
        "ck_dataset_fields_vector_shape",
    }
    database_field_checks: set[str] = {
        str(constraint["name"]) for constraint in inspector.get_check_constraints("dataset_fields")
    }
    metadata_field_checks: set[str] = {
        str(constraint.name) for constraint in dataset_fields.constraints if isinstance(constraint, sa.CheckConstraint)
    }
    assert expected_field_checks <= database_field_checks
    assert expected_field_checks <= metadata_field_checks
    field_indexes: dict[str, dict[str, Any]] = {
        str(index["name"]): index for index in inspector.get_indexes("dataset_fields")
    }
    assert field_indexes["uq_dataset_fields_singleton_role"]["unique"] is True
    work_unique_constraints: list[dict[str, object]] = inspector.get_unique_constraints("dataset_work")
    work_publication_identity: dict[str, object] = next(
        constraint
        for constraint in work_unique_constraints
        if constraint["name"] == "uq_dataset_work_publication_identity"
    )
    assert work_publication_identity["column_names"] == [
        "dataset_id",
        "work_id",
        "spec_revision_id",
        "source_snapshot_seq",
    ]
    work_columns: dict[str, dict[str, object]] = {
        str(column["name"]): column for column in inspector.get_columns("dataset_work")
    }
    assert list(work_columns) == [column.name for column in metadata.tables["dataset_work"].columns]
    assert work_columns["source_id"]["nullable"] is False
    assert work_columns["source_snapshot_seq"]["nullable"] is False
    work_foreign_keys: dict[str, dict[str, object]] = {
        str(constraint["name"]): constraint for constraint in inspector.get_foreign_keys("dataset_work")
    }
    assert work_foreign_keys["fk_dataset_work_dataset_source"]["constrained_columns"] == ["dataset_id", "source_id"]
    assert work_foreign_keys["fk_dataset_work_snapshot_source"]["constrained_columns"] == [
        "source_snapshot_seq",
        "source_id",
    ]
    publication_foreign_keys: list[dict[str, object]] = inspector.get_foreign_keys("dataset_publications")
    publication_work_identity: dict[str, object] = next(
        constraint
        for constraint in publication_foreign_keys
        if constraint["name"] == "fk_dataset_publications_dataset_work_identity"
    )
    assert publication_work_identity["constrained_columns"] == [
        "dataset_id",
        "work_id",
        "spec_revision_id",
        "source_snapshot_seq",
    ]
    publication_index_foreign_keys: list[dict[str, object]] = inspector.get_foreign_keys("publication_indexes")
    publication_index_type: dict[str, object] = next(
        constraint
        for constraint in publication_index_foreign_keys
        if constraint["name"] == "fk_publication_indexes_definition_type"
    )
    assert publication_index_type["constrained_columns"] == [
        "spec_revision_id",
        "index_definition_id",
        "actual_index_type",
    ]
    settings: ReconcilerSettings = repository.reconciler_settings()
    assert settings.claim_batch_size > 0
    with engine.connect() as connection:
        assert connection.scalar(sa.select(sa.func.count()).select_from(reconciler_settings)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_specs)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_spec_revisions)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_fields)) == 10
        assert connection.scalar(sa.select(sa.func.count()).select_from(index_definitions)) == 6
        assert connection.scalar(sa.select(sa.func.count()).select_from(vector_index_options)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(fts_index_options)) == 1
        revision: DatasetSpecRevision = repository.load_spec_revision(connection, DEFAULT_SPEC_REVISION_ID)
    assert revision.expected_configuration_digest() == revision.configuration_digest
    assert revision.ingest_shuffle_partitions > 0
    assert revision.compaction_enabled
    assert revision.materialize_deletions
    assert len(revision.indexes) == 6


def test_specification_lifecycle_freezes_history_and_enqueues_revision_convergence(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Lifecycle APIs author drafts, freeze active history, and converge materialized datasets.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository = postgres_repository[0]
    engine: Engine = postgres_repository[1]
    with pytest.raises(ValueError, match="specification name must match"):
        repository.create_spec("Étl")
    requested_spec_id: uuid.UUID = uuid.uuid4()
    spec_id: uuid.UUID = repository.create_spec("tenant-vectors", "Tenant vector policy", requested_spec_id)
    assert spec_id == requested_spec_id
    assert repository.create_spec("tenant-vectors", "Tenant vector policy", requested_spec_id) == spec_id
    with pytest.raises(StateTransitionError, match="different immutable values"):
        repository.create_spec("tenant-vectors", "Changed description", requested_spec_id)

    first_input: DatasetSpecRevision = draft_revision(spec_id)
    with pytest.raises(ValueError, match="at most four decimal places"):
        repository.create_draft_spec_revision(replace(first_input, materialize_deletions_threshold=0.12345))
    first: DatasetSpecRevision = repository.create_draft_spec_revision(first_input)
    assert first.state is SpecRevisionState.DRAFT
    assert first.configuration_digest == first.expected_configuration_digest()
    assert repository.create_draft_spec_revision(first_input) == first
    with pytest.raises(StateTransitionError, match="different content"):
        repository.create_draft_spec_revision(replace(first_input, ingest_shuffle_partitions=17))
    first_active: DatasetSpecRevision = repository.activate_spec_revision(first.spec_revision_id)
    assert first_active.state is SpecRevisionState.ACTIVE
    assert repository.activate_spec_revision(first.spec_revision_id) == first_active

    source: IcebergSource = register_source(repository)
    source = repository.set_source_default_spec(source.source_id, spec_id)
    serving: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        1,
    )
    second_input: DatasetSpecRevision = replace(
        draft_revision(spec_id, 2, first.spec_revision_id),
        ingest_shuffle_partitions=first.ingest_shuffle_partitions + 1,
    )
    second: DatasetSpecRevision = repository.create_draft_spec_revision(second_input)
    with (
        pytest.raises(sa.exc.IntegrityError, match="DRAFT revisions may transition only to ACTIVE"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(dataset_spec_revisions)
            .where(dataset_spec_revisions.c.spec_revision_id == second.spec_revision_id)
            .values(state=SpecRevisionState.RETIRED.value)
        )
    with (
        pytest.raises(sa.exc.IntegrityError, match="desired specification revision must be ACTIVE"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(datasets)
            .where(datasets.c.dataset_id == serving.dataset_id)
            .values(desired_spec_revision_id=second.spec_revision_id)
        )

    second_active: DatasetSpecRevision = repository.activate_spec_revision(second.spec_revision_id)
    assert second_active.state is SpecRevisionState.ACTIVE
    with engine.connect() as connection:
        retired_state: str | None = connection.scalar(
            sa.select(dataset_spec_revisions.c.state).where(
                dataset_spec_revisions.c.spec_revision_id == first.spec_revision_id
            )
        )
        historical_work_count: int = int(
            connection.scalar(
                sa.select(sa.func.count())
                .select_from(dataset_work)
                .where(dataset_work.c.spec_revision_id == first.spec_revision_id)
            )
            or 0
        )
        historical_publication_count: int = int(
            connection.scalar(
                sa.select(sa.func.count())
                .select_from(dataset_publications)
                .where(dataset_publications.c.spec_revision_id == first.spec_revision_id)
            )
            or 0
        )
    assert retired_state == SpecRevisionState.RETIRED.value
    assert historical_work_count == 2
    assert historical_publication_count == 1

    with (
        pytest.raises(sa.exc.IntegrityError, match="active and retired specification revisions are immutable"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(dataset_spec_revisions)
            .where(dataset_spec_revisions.c.spec_revision_id == first.spec_revision_id)
            .values(ingest_shuffle_partitions=19)
        )
    retired_field_id: uuid.UUID = first.fields[0].field_id
    with (
        pytest.raises(sa.exc.IntegrityError, match="children may change only while their revision is DRAFT"),
        engine.begin() as connection,
    ):
        connection.execute(sa.delete(dataset_fields).where(dataset_fields.c.field_id == retired_field_id))
    vector_index: IndexDefinition = next(index for index in second.indexes if index.index_type.value == "IVF_RQ")
    with (
        pytest.raises(sa.exc.IntegrityError, match="children may change only while their revision is DRAFT"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(vector_index_options)
            .where(vector_index_options.c.index_definition_id == vector_index.index_definition_id)
            .values(num_partitions=3)
        )
    with (
        pytest.raises(sa.exc.IntegrityError, match="desired specification revision must be ACTIVE"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(datasets)
            .where(datasets.c.dataset_id == serving.dataset_id)
            .values(desired_spec_revision_id=first.spec_revision_id)
        )

    rebuild_work_id: uuid.UUID | None = repository.assign_dataset_spec_revision(
        serving.dataset_id,
        second.spec_revision_id,
    )
    assert rebuild_work_id is not None
    assert repository.assign_dataset_spec_revision(serving.dataset_id, second.spec_revision_id) == rebuild_work_id
    with engine.connect() as connection:
        rebuild_rows: list[RowMapping] = list(
            connection.execute(
                sa.select(dataset_work).where(
                    dataset_work.c.dataset_id == serving.dataset_id,
                    dataset_work.c.kind == WorkKind.REBUILD.value,
                )
            ).mappings()
        )
        desired_revision_id: uuid.UUID | None = connection.scalar(
            sa.select(datasets.c.desired_spec_revision_id).where(datasets.c.dataset_id == serving.dataset_id)
        )
    assert len(rebuild_rows) == 1
    assert rebuild_rows[0]["work_id"] == rebuild_work_id
    assert rebuild_rows[0]["spec_revision_id"] == second.spec_revision_id
    assert desired_revision_id == second.spec_revision_id

    empty_spec_id: uuid.UUID = repository.create_spec("empty-policy")
    with (
        pytest.raises(sa.exc.IntegrityError, match="source default specification must have an ACTIVE revision"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(iceberg_sources)
            .where(iceberg_sources.c.source_id == source.source_id)
            .values(default_spec_id=empty_spec_id)
        )


def test_airflow_claim_provenance_is_audit_only_and_persisted(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Optional Airflow context is recorded on a normal PostgreSQL work claim.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository = postgres_repository[0]
    engine: Engine = postgres_repository[1]
    source: IcebergSource = register_source(repository)
    repository.enqueue_source_snapshot(
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        [dataset_plan()],
    )
    provenance: WorkProvenance = WorkProvenance(
        launcher_kind=WorkLauncherKind.AIRFLOW,
        airflow_ctx_dag_id="local-reconcile",
        airflow_ctx_dag_run_id="manual__2026-07-18",
        airflow_ctx_task_id="reconcile",
        airflow_ctx_map_index=-1,
        airflow_ctx_try_number=2,
    )
    claims: list[WorkClaim] = repository.claim_due_work(1, timedelta(minutes=5), provenance=provenance)
    assert len(claims) == 1
    with engine.connect() as connection:
        row: RowMapping = (
            connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == claims[0].work_id))
            .mappings()
            .one()
        )
    assert row["launcher_kind"] == WorkLauncherKind.AIRFLOW.value
    assert row["airflow_ctx_dag_id"] == "local-reconcile"
    assert row["airflow_ctx_dag_run_id"] == "manual__2026-07-18"
    assert row["airflow_ctx_task_id"] == "reconcile"
    assert row["airflow_ctx_map_index"] == -1
    assert row["airflow_ctx_try_number"] == 2


def test_postgres_rejects_fields_outside_the_runtime_contract(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Database checks reject field contracts that the local data path cannot implement.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository = postgres_repository[0]
    engine: Engine = postgres_repository[1]
    spec_id: uuid.UUID = repository.create_spec("field-contract-test")
    draft: DatasetSpecRevision = repository.create_draft_spec_revision(draft_revision(spec_id))
    base_row: dict[str, object] = {
        "field_id": uuid.uuid4(),
        "spec_revision_id": draft.spec_revision_id,
        "ordinal": 100,
        "target_name": "embedding",
        "role": "VECTOR",
        "source_kind": "MAP_KEY",
        "source_column": "vectors",
        "source_key": "embedding",
        "data_type": "fixed_size_list<float32,128>",
        "nullable": True,
        "required_on_upsert": True,
        "vector_dimension": 128,
    }
    invalid_cases: tuple[tuple[str, dict[str, object]], ...] = (
        ("ck_dataset_fields_map_key_target", {"source_key": "other_embedding"}),
        ("ck_dataset_fields_source_contract", {"source_column": "metadata"}),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "id",
                "role": "KEY",
                "source_kind": "DIRECT",
                "source_column": "id",
                "source_key": None,
                "data_type": "string",
                "nullable": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "event_time",
                "role": "EVENT_TIME",
                "source_kind": "DIRECT",
                "source_column": "event_time",
                "source_key": None,
                "data_type": "timestamp[us,UTC]",
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "expires",
                "role": "TTL",
                "source_kind": "DIRECT",
                "source_column": "expires",
                "source_key": None,
                "data_type": "duration[s]",
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "deleted",
                "role": "TOMBSTONE",
                "source_kind": "DERIVED",
                "source_column": "deleted",
                "source_key": None,
                "data_type": "bool",
                "nullable": False,
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "lineage_sequence",
                "role": "LINEAGE",
                "source_kind": "DERIVED",
                "source_column": "lineage_sequence",
                "source_key": None,
                "data_type": "int64",
                "nullable": False,
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_source_contract",
            {
                "target_name": "lance_etl_window_seq",
                "role": "LINEAGE",
                "source_kind": "DERIVED",
                "source_column": "other_sequence",
                "source_key": None,
                "data_type": "int64",
                "nullable": False,
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_type_contract",
            {
                "target_name": "lance_etl_event_digest",
                "role": "LINEAGE",
                "source_kind": "DERIVED",
                "source_column": "lance_etl_event_digest",
                "source_key": None,
                "data_type": "int64",
                "nullable": False,
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        (
            "ck_dataset_fields_type_contract",
            {
                "target_name": "summary",
                "role": "TEXT",
                "source_column": "texts",
                "source_key": "summary",
                "data_type": "int64",
                "required_on_upsert": False,
                "vector_dimension": None,
            },
        ),
        ("ck_dataset_fields_flags_contract", {"required_on_upsert": False}),
        (
            "ck_dataset_fields_vector_shape",
            {"data_type": "fixed_size_list<float32,7>", "vector_dimension": 7},
        ),
        ("ck_dataset_fields_vector_shape", {"vector_dimension": 256}),
        (
            "ck_dataset_fields_vector_shape",
            {
                "target_name": "category",
                "role": "METADATA",
                "source_column": "metadata",
                "source_key": "category",
                "data_type": "string",
                "required_on_upsert": False,
            },
        ),
    )
    with engine.connect() as connection:
        transaction: sa.engine.Transaction = connection.begin()
        constraint_name: str
        changes: dict[str, object]
        for constraint_name, changes in invalid_cases:
            row: dict[str, object] = base_row | changes | {"field_id": uuid.uuid4()}
            savepoint: sa.engine.NestedTransaction = connection.begin_nested()
            with pytest.raises(sa.exc.IntegrityError, match=constraint_name):
                connection.execute(dataset_fields.insert().values(**row))
            savepoint.rollback()
        transaction.rollback()


def test_source_bootstrap_is_idempotent_and_postgres_owned(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """First-run source bootstrap persists typed configuration and rejects drift.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    table_uuid: uuid.UUID = uuid.uuid4()
    first: IcebergSource = register_source(repository, table_uuid)
    assert register_source(repository, table_uuid) == first
    assert repository.source_by_name("vectors") == first
    assert first.spark_table == "local.lake.raw_vectors"
    assert first.lance_base_uri == "file:///tmp/lance-etl-test"
    with pytest.raises(StateTransitionError, match="differ from PostgreSQL truth"):
        register_source(repository, uuid.uuid4())
    with engine.connect() as connection:
        row: RowMapping = connection.execute(sa.select(iceberg_sources)).mappings().one()
    assert row["source_id"] == first.source_id
    assert row["vectors_column"] == "vectors"
    assert row["metadata_column"] == "metadata"


def test_snapshot_enqueue_is_idempotent_and_fenced_claims_expire_safely(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Planning replays exactly while expired claims lose their token and fence.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    plan: SourceSnapshotPlan = source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE)
    snapshot_seq: int = repository.enqueue_source_snapshot(plan, [dataset_plan()])
    assert repository.enqueue_source_snapshot(plan, [dataset_plan()]) == snapshot_seq
    identity: RoutingIdentity = dataset_plan().identity
    dataset_id: uuid.UUID = deterministic_dataset_id(source.source_id, identity)
    with engine.connect() as connection:
        assert connection.scalar(sa.select(sa.func.count()).select_from(datasets)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_state)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(source_snapshots)) == 1
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_work)) == 1
    claimed_at: datetime = datetime.now(UTC) + timedelta(seconds=1)
    first_claim: WorkClaim = claim_one(repository, claimed_at, timedelta(seconds=5))
    assert first_claim.dataset_id == dataset_id
    assert first_claim.source_snapshot_seq == snapshot_seq
    assert first_claim.attempt_count == 1
    assert first_claim.lease_expires_at == claimed_at + timedelta(seconds=5)
    with engine.connect() as connection:
        running_work: RowMapping = connection.execute(sa.select(dataset_work)).mappings().one()
    assert running_work["state"] == WorkState.RUNNING.value
    assert running_work["lease_token"] == first_claim.lease_token
    context: WorkExecutionContext | None = repository.work_execution_context(
        first_claim,
        claimed_at + timedelta(seconds=1),
    )
    assert context is not None
    assert context.source == source
    assert context.identity == identity
    assert context.spec_revision.spec_revision_id == DEFAULT_SPEC_REVISION_ID
    assert repository.claim_due_work(1, timedelta(minutes=1), now=claimed_at + timedelta(seconds=2)) == []
    second_claim: WorkClaim = claim_one(repository, claimed_at + timedelta(seconds=6), timedelta(minutes=1))
    assert second_claim.work_id == first_claim.work_id
    assert second_claim.lease_token != first_claim.lease_token
    assert second_claim.fence_epoch > first_claim.fence_epoch
    assert second_claim.attempt_count == 2
    assert not repository.renew_lease(first_claim, timedelta(minutes=1), claimed_at + timedelta(seconds=7))
    assert not repository.complete_ingest(first_claim, 1, 10, b"a" * 32, claimed_at + timedelta(seconds=7))
    assert repository.complete_ingest(second_claim, 1, 10, b"a" * 32, claimed_at + timedelta(seconds=7))
    with engine.connect() as connection:
        state_row: RowMapping = connection.execute(sa.select(dataset_state)).mappings().one()
        snapshot_row: RowMapping = connection.execute(sa.select(source_snapshots)).mappings().one()
        work_row: RowMapping = (
            connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == second_claim.work_id))
            .mappings()
            .one()
        )
    assert state_row["fence_epoch"] == second_claim.fence_epoch
    assert state_row["ingest_lance_version"] == 1
    assert snapshot_row["state"] == SourceSnapshotState.COMPLETE.value
    assert work_row["state"] == WorkState.SUCCEEDED.value
    assert work_row["attempt_count"] == 2
    assert work_row["lease_token"] is None


def test_ingest_to_publish_commits_normalized_evidence_and_pointer_atomically(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Publication requires exact index evidence before its serving pointer changes.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    repository.enqueue_source_snapshot(
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        [dataset_plan()],
    )
    ingest_claim: WorkClaim = claim_one(repository)
    assert repository.complete_ingest(ingest_claim, 3, 10, b"a" * 32)
    assert repository.complete_ingest(ingest_claim, 3, 10, b"a" * 32)
    assert repository.resolve_serving_dataset(dataset_plan().identity) is None
    publish_claim: WorkClaim = claim_one(repository)
    assert publish_claim.kind is WorkKind.PUBLISH
    assert publish_claim.phase is WorkPhase.COMPACT
    context: WorkExecutionContext | None = repository.work_execution_context(publish_claim)
    assert context is not None
    manifest_uri: str = f"file:///tmp/manifests/{publish_claim.work_id}.json"
    artifact_digest: bytes = b"m" * 32
    assert repository.advance_phase(
        publish_claim,
        WorkPhase.PREWARM,
        candidate_lance_uri=publish_claim.ingest_lance_uri,
        candidate_lance_version=3,
        artifact_manifest_uri=manifest_uri,
        artifact_digest=artifact_digest,
    )
    with engine.connect() as connection:
        running_publish_work: RowMapping = (
            connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == publish_claim.work_id))
            .mappings()
            .one()
        )
    assert running_publish_work["state"] == WorkState.RUNNING.value
    assert running_publish_work["phase"] == WorkPhase.PREWARM.value
    full_evidence: PublicationEvidence = evidence_for_context(context)
    incomplete_evidence: PublicationEvidence = replace(full_evidence, indexes=full_evidence.indexes[:-1])
    with pytest.raises(StateTransitionError, match="index evidence differs"):
        repository.publish_dataset(
            publish_claim,
            publish_claim.ingest_lance_uri,
            3,
            manifest_uri,
            artifact_digest,
            incomplete_evidence,
        )
    with engine.connect() as connection:
        assert connection.scalar(sa.select(sa.func.count()).select_from(dataset_publications)) == 0
        assert connection.scalar(sa.select(dataset_state.c.active_publication_id)) is None
    assert repository.publish_dataset(
        publish_claim,
        publish_claim.ingest_lance_uri,
        3,
        manifest_uri,
        artifact_digest,
        full_evidence,
    )
    assert repository.publish_dataset(
        publish_claim,
        publish_claim.ingest_lance_uri,
        3,
        manifest_uri,
        artifact_digest,
        full_evidence,
    )
    served: ServingDataset | None = repository.resolve_serving_dataset(dataset_plan().identity)
    assert served is not None
    assert served.lance_uri == publish_claim.ingest_lance_uri
    assert served.lance_version == 3
    with engine.connect() as connection:
        publication_row: RowMapping = connection.execute(sa.select(dataset_publications)).mappings().one()
        state_row: RowMapping = connection.execute(sa.select(dataset_state)).mappings().one()
        evidence_rows: list[RowMapping] = list(connection.execute(sa.select(publication_indexes)).mappings())
        publish_work: RowMapping = (
            connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == publish_claim.work_id))
            .mappings()
            .one()
        )
    assert state_row["active_publication_id"] == publication_row["publication_id"] == served.publication_id
    assert len(evidence_rows) == len(context.spec_revision.indexes) == 6
    assert {row["actual_index_type"] for row in evidence_rows} == {
        index.index_type.value for index in context.spec_revision.indexes
    }
    assert publish_work["state"] == WorkState.SUCCEEDED.value
    assert publish_work["phase"] == WorkPhase.PUBLISH.value


def test_database_rejects_inconsistent_publication_evidence_and_lineage(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """PostgreSQL rejects non-distinct counts, partial indexes, and mismatched frozen lineage.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    baseline: SourceSnapshotPlan = source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE)
    serving: ServingDataset = complete_and_publish_snapshot(repository, baseline, 3)
    next_snapshot_seq: int = repository.enqueue_source_snapshot(
        source_plan(source.source_id, 101, 11, 100),
        [dataset_plan()],
    )
    with (
        pytest.raises(sa.exc.IntegrityError, match="ck_dataset_publications_distinct_rows"),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(dataset_publications)
            .where(dataset_publications.c.publication_id == serving.publication_id)
            .values(distinct_row_count=9)
        )
    with (
        pytest.raises(
            sa.exc.IntegrityError,
            match="ck_dataset_publications_distinct_live_rows",
        ),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(dataset_publications)
            .where(dataset_publications.c.publication_id == serving.publication_id)
            .values(distinct_live_row_count=8)
        )
    with (
        pytest.raises(
            sa.exc.IntegrityError,
            match="fk_dataset_publications_dataset_work_identity",
        ),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(dataset_publications)
            .where(dataset_publications.c.publication_id == serving.publication_id)
            .values(source_snapshot_seq=next_snapshot_seq)
        )
    with engine.connect() as connection:
        index_definition_id: uuid.UUID = connection.scalar(
            sa.select(publication_indexes.c.index_definition_id)
            .where(publication_indexes.c.publication_id == serving.publication_id)
            .limit(1)
        )
        actual_index_type: str = connection.scalar(
            sa.select(publication_indexes.c.actual_index_type).where(
                publication_indexes.c.publication_id == serving.publication_id,
                publication_indexes.c.index_definition_id == index_definition_id,
            )
        )
    mismatched_index_type: str = "BTREE" if actual_index_type != "BTREE" else "BITMAP"
    with (
        pytest.raises(
            sa.exc.IntegrityError,
            match="fk_publication_indexes_definition_type",
        ),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(publication_indexes)
            .where(
                publication_indexes.c.publication_id == serving.publication_id,
                publication_indexes.c.index_definition_id == index_definition_id,
            )
            .values(actual_index_type=mismatched_index_type)
        )
    with (
        pytest.raises(
            sa.exc.IntegrityError,
            match="ck_publication_indexes_complete_coverage",
        ),
        engine.begin() as connection,
    ):
        connection.execute(
            sa.update(publication_indexes)
            .where(
                publication_indexes.c.publication_id == serving.publication_id,
                publication_indexes.c.index_definition_id == index_definition_id,
            )
            .values(unindexed_fragment_count=1)
        )


def test_retry_bound_blocks_work_and_explicit_retry_reopens_it(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Transient retries honor the database-owned attempt bound and stale fences.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    repository.enqueue_source_snapshot(
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        [dataset_plan()],
    )
    with engine.begin() as connection:
        connection.execute(sa.update(reconciler_settings).values(max_attempts=2))
    first_claim: WorkClaim = claim_one(repository)
    assert repository.retry_work(first_claim, timedelta(0), "TRANSIENT", "try again")
    assert not repository.retry_work(first_claim, timedelta(0), "STALE", "lost fence")
    second_claim: WorkClaim = claim_one(repository)
    assert second_claim.attempt_count == 2
    assert repository.retry_work(second_claim, timedelta(0), "TRANSIENT", "still failing")
    status: ControlPlaneStatus = repository.control_plane_status()
    assert status.blocked_work == 1
    assert status.retry_wait_work == 0
    with engine.connect() as connection:
        row: RowMapping = connection.execute(sa.select(dataset_work)).mappings().one()
    assert row["state"] == WorkState.BLOCKED.value
    assert row["error_code"] == "MAX_ATTEMPTS_EXHAUSTED"
    assert row["attempt_count"] == 2
    assert repository.retry_blocked_work(second_claim.work_id)
    assert not repository.retry_blocked_work(second_claim.work_id)
    with engine.connect() as connection:
        assert connection.scalar(sa.select(dataset_work.c.state)) == WorkState.PENDING.value


def test_rejected_snapshot_and_retention_floor_remain_durable(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Rejected source evidence remains the replay floor after old safe history ages out.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    baseline: SourceSnapshotPlan = source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE)
    baseline_seq: int = repository.enqueue_source_snapshot(baseline, [])
    rejected: SourceSnapshotPlan = source_plan(source.source_id, 101, 11, 100, SourceSnapshotKind.REJECTED)
    rejection_message: str = "delete files " * 200
    rejected_seq: int = repository.enqueue_blocked_source_snapshot(
        rejected,
        "UNTRUSTED_REWRITE",
        rejection_message,
    )
    assert repository.enqueue_blocked_source_snapshot(rejected, "UNTRUSTED_REWRITE", rejection_message) == rejected_seq
    with pytest.raises(StateTransitionError, match="rejected snapshot replay differs"):
        repository.enqueue_blocked_source_snapshot(rejected, "UNTRUSTED_REWRITE")
    with pytest.raises(StateTransitionError, match="rejected snapshot replay differs"):
        repository.enqueue_blocked_source_snapshot(rejected, "DIFFERENT_CLASSIFICATION")
    future: datetime = datetime.now(UTC) + timedelta(days=31)
    floor: RowMapping | None = repository.retention_floor(future)
    assert floor is not None
    assert floor["source_snapshot_seq"] == rejected_seq
    assert floor["state"] == SourceSnapshotState.BLOCKED.value
    status: ControlPlaneStatus = repository.control_plane_status(future)
    assert status.blocked_source_snapshots == 1
    assert status.retention_source_snapshot_seq == rejected_seq
    with engine.connect() as connection:
        baseline_state: str | None = connection.scalar(
            sa.select(source_snapshots.c.state).where(source_snapshots.c.source_snapshot_seq == baseline_seq)
        )
        persisted_message: str | None = connection.scalar(
            sa.select(source_snapshots.c.error_message).where(source_snapshots.c.source_snapshot_seq == rejected_seq)
        )
    assert baseline_state == SourceSnapshotState.COMPLETE.value
    assert persisted_message == rejection_message[:2000]


def test_rebuild_uses_isolated_uri_and_atomically_replaces_active_generation(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """A rebuild freezes current state then publishes an isolated candidate generation.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    original: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        1,
    )
    request_id: uuid.UUID = uuid.uuid4()
    work_id: uuid.UUID = repository.enqueue_rebuild(dataset_plan().identity, request_id)
    assert repository.enqueue_rebuild(dataset_plan().identity, request_id) == work_id
    rebuild_claim: WorkClaim = claim_one(repository)
    assert rebuild_claim.kind is WorkKind.REBUILD
    context: WorkExecutionContext | None = repository.work_execution_context(rebuild_claim)
    assert context is not None
    assert context.source_snapshot_kind is SourceSnapshotKind.BASELINE
    assert context.candidate_lance_uri is not None
    assert f"/rebuild/{original.dataset_id}/{work_id}.lance" in context.candidate_lance_uri
    candidate_uri: str = context.candidate_lance_uri
    manifest_uri: str = f"file:///tmp/manifests/{work_id}.json"
    artifact_digest: bytes = b"r" * 32
    assert repository.advance_phase(
        rebuild_claim,
        WorkPhase.PREWARM,
        candidate_lance_uri=candidate_uri,
        candidate_lance_version=2,
        artifact_manifest_uri=manifest_uri,
        artifact_digest=artifact_digest,
    )
    assert repository.publish_dataset(
        rebuild_claim,
        candidate_uri,
        2,
        manifest_uri,
        artifact_digest,
        evidence_for_context(context),
    )
    rebuilt: ServingDataset | None = repository.resolve_serving_dataset(dataset_plan().identity)
    assert rebuilt is not None
    assert rebuilt.publication_id != original.publication_id
    assert rebuilt.lance_uri == candidate_uri
    assert rebuilt.lance_version == 2
    with engine.connect() as connection:
        original_row: RowMapping = (
            connection.execute(
                sa.select(dataset_publications).where(dataset_publications.c.publication_id == original.publication_id)
            )
            .mappings()
            .one()
        )
        state_row: RowMapping = connection.execute(sa.select(dataset_state)).mappings().one()
    assert original_row["retired_at"] is not None
    assert state_row["ingest_lance_uri"] == candidate_uri
    assert state_row["ingest_lance_version"] == 2


def test_retired_publications_respect_per_spec_retention_before_cleanup(
    postgres_repository: tuple[ControlPlaneRepository, Engine],
) -> None:
    """Frozen publication count and artifact horizon both gate cleanup candidates.

    Args:
        postgres_repository: Fresh repository and engine fixture.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_repository
    source: IcebergSource = register_source(repository)
    first: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 100, 10, None, SourceSnapshotKind.BASELINE),
        1,
    )
    second: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 101, 11, 100),
        2,
    )
    third: ServingDataset = complete_and_publish_snapshot(
        repository,
        source_plan(source.source_id, 102, 12, 101),
        3,
    )
    current: datetime = datetime.now(UTC)
    assert repository.claim_publication_cleanup(current + timedelta(days=29), 10) == []
    cleanups: list[PublicationCleanup] = repository.claim_publication_cleanup(current + timedelta(days=31), 10)
    assert [cleanup.publication_id for cleanup in cleanups] == [first.publication_id]
    assert repository.finalize_publication_cleanup(cleanups[0])
    assert repository.finalize_publication_cleanup(cleanups[0])
    served: ServingDataset | None = repository.resolve_serving_dataset(dataset_plan().identity)
    assert served == third
    with engine.connect() as connection:
        publication_ids: set[uuid.UUID] = set(connection.scalars(sa.select(dataset_publications.c.publication_id)))
        evidence_count: int | None = connection.scalar(sa.select(sa.func.count()).select_from(publication_indexes))
    assert publication_ids == {second.publication_id, third.publication_id}
    assert evidence_count == 12
