"""Real local Iceberg-to-Lance reconciliation through PostgreSQL."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import lance
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from lance_etl.reconciler.config import DEFAULT_DATABASE_URL, DEFAULT_ICEBERG_PACKAGE, RuntimeSettings
from lance_etl.reconciler.iceberg import (
    BaselineQualifier,
    DurableSourcePlanProvider,
    SparkIcebergCatalog,
    snapshot_from_row,
)
from lance_etl.reconciler.planning import SourcePlanEnqueuer
from lance_etl.reconciler.prewarm import LocalExactVersionPrewarmer
from lance_etl.reconciler.retention import PublicationRetentionSweep
from lance_etl.reconciler.runtime import build_runtime_spark
from lance_etl.reconciler.service import BoundedDispatcher, ReconcilerApplication, ResultReconciler
from lance_etl.reconciler.workers import ConfiguredPublicationRunner, DistributedIngestRunner, FencedWorkExecutor
from lance_etl.source.models import TableMetadata
from lance_etl.state import (
    ControlPlaneRepository,
    DatasetField,
    DatasetSpecRevision,
    FieldRole,
    IcebergSource,
    IndexDefinition,
    IndexType,
    RoutingIdentity,
    ServingDataset,
    SourceSnapshotState,
    SpecRevisionState,
    WorkState,
    build_control_plane_engine,
    production_default_spec_revision,
)
from lance_etl.state.settings import ReconcilerSettings
from lance_etl.state.tables import (
    dataset_publications,
    dataset_work,
    datasets,
    source_snapshots,
)
from lance_etl.telemetry import TelemetryConfig

POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"
"""Explicit database URL required for the isolated PostgreSQL schema."""

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent
"""Repository root containing the Alembic configuration."""

VECTOR_DIMENSION: int = 8
"""Small Lance-compatible vector dimension used by the local workflow."""

LOCAL_SPEC_ID: uuid.UUID = uuid.UUID("68e684aa-63c8-5a4a-9f68-b61df8d0fe79")
"""Named specification identity isolated from the seeded production policy."""

LOCAL_SPEC_REVISION_ID: uuid.UUID = uuid.UUID("725f98d4-e4de-5a22-9635-07af77d11431")
"""Draft revision identity installed through the lifecycle API."""

SOURCE_SCHEMA: StructType = StructType(
    [
        StructField("tenant_id", StringType(), False),
        StructField("namespace", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("vector_id", StringType(), False),
        StructField("op", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("processing_timestamp", TimestampType(), False),
        StructField("vectors", MapType(StringType(), ArrayType(FloatType(), False), False), False),
        StructField("texts", MapType(StringType(), StringType(), False), False),
        StructField("metadata", MapType(StringType(), StringType(), False), False),
        StructField("ttl", LongType(), True),
    ]
)
"""Exact source shape accepted by the database-configured ingestion worker."""


def psycopg_url(raw_url: str) -> URL:
    """Normalize an integration URL onto the psycopg 3 driver.

    Args:
        raw_url: Explicit PostgreSQL integration database URL.

    Returns:
        SQLAlchemy URL selecting ``postgresql+psycopg``.
    """
    url: URL = make_url(raw_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError(f"{POSTGRES_URL_ENV} must select PostgreSQL")
    return url.set(drivername="postgresql+psycopg")


def schema_url(base_url: URL, schema_name: str) -> URL:
    """Bind PostgreSQL sessions to one isolated migrated schema.

    Args:
        base_url: Shared integration database URL.
        schema_name: Unique schema created for this test.

    Returns:
        URL carrying the PostgreSQL startup search path.
    """
    return base_url.update_query_dict({"options": f"-csearch_path={schema_name}"})


@pytest.fixture
def postgres_control_plane(tmp_path: Path) -> Iterator[tuple[ControlPlaneRepository, Engine]]:
    """Migrate and expose an isolated local PostgreSQL control plane.

    Args:
        tmp_path: Isolated filesystem root used by the test.

    Yields:
        Migrated repository and SQLAlchemy engine.
    """
    del tmp_path
    raw_url: str | None = os.environ.get(POSTGRES_URL_ENV)
    if raw_url is None:
        pytest.skip(f"set {POSTGRES_URL_ENV} to run the local end-to-end integration test")
    base_url: URL = psycopg_url(raw_url)
    schema_name: str = f"lance_etl_e2e_{uuid.uuid4().hex}"
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
        repository: ControlPlaneRepository = ControlPlaneRepository(engine)
        configure_local_spec(repository)
        yield repository, engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        admin_engine.dispose()


@pytest.fixture
def local_spark(tmp_path: Path) -> Iterator[SparkSession]:
    """Create the local filesystem-backed Iceberg Spark runtime.

    Args:
        tmp_path: Isolated Iceberg warehouse root.

    Yields:
        Local two-core Spark session with Iceberg extensions enabled.
    """
    settings: RuntimeSettings = RuntimeSettings(
        database_url=DEFAULT_DATABASE_URL,
        lance_base_uri=str(tmp_path / "lance"),
        source_table="local.db.events",
        datadog_service="lance-etl-local-e2e",
        datadog_env="test",
        canonical_baseline_snapshot_id=None,
        spark_master="local[2]",
        spark_catalog="local",
        spark_warehouse_path=tmp_path / "iceberg",
        spark_iceberg_package=DEFAULT_ICEBERG_PACKAGE,
    )
    session: SparkSession = build_runtime_spark(settings)
    yield session
    session.stop()


def local_spec() -> DatasetSpecRevision:
    """Return the small immutable specification installed for the E2E test.

    Returns:
        One vector index plus all terminal ingestion fields.
    """
    base: DatasetSpecRevision = production_default_spec_revision()
    field_ids: dict[uuid.UUID, uuid.UUID] = {
        field.field_id: uuid.uuid5(LOCAL_SPEC_REVISION_ID, f"field:{field.target_name}") for field in base.fields
    }
    fields: tuple[DatasetField, ...] = tuple(
        replace(
            field,
            field_id=field_ids[field.field_id],
            spec_revision_id=LOCAL_SPEC_REVISION_ID,
            data_type=f"fixed_size_list<float32,{VECTOR_DIMENSION}>"
            if field.role is FieldRole.VECTOR
            else field.data_type,
            vector_dimension=VECTOR_DIMENSION if field.role is FieldRole.VECTOR else field.vector_dimension,
        )
        for field in base.fields
    )
    base_vector_definition: IndexDefinition = next(
        definition for definition in base.index_definitions if definition.index_type is IndexType.IVF_RQ
    )
    if base_vector_definition.vector_options is None:
        raise RuntimeError("bundled vector index lacks typed options")
    vector_definition: IndexDefinition = replace(
        base_vector_definition,
        index_definition_id=uuid.uuid5(LOCAL_SPEC_REVISION_ID, "index:vector_idx"),
        spec_revision_id=LOCAL_SPEC_REVISION_ID,
        field_id=field_ids[base_vector_definition.field_id],
        vector_options=replace(
            base_vector_definition.vector_options,
            num_partitions=2,
            minimum_partitions=1,
            maximum_partitions=16,
            target_rows_per_partition=4,
        ),
    )
    candidate: DatasetSpecRevision = replace(
        base,
        spec_id=LOCAL_SPEC_ID,
        spec_revision_id=LOCAL_SPEC_REVISION_ID,
        revision_number=1,
        state=SpecRevisionState.DRAFT,
        supersedes_revision_id=None,
        fields=fields,
        indexes=(vector_definition,),
        ingest_shuffle_partitions=2,
        compaction_enabled=False,
        configuration_digest=b"",
    )
    return candidate


def configure_local_spec(repository: ControlPlaneRepository) -> None:
    """Install and activate a small normalized revision through lifecycle APIs.

    Args:
        repository: Fresh migrated PostgreSQL repository.
    """
    spec_id: uuid.UUID = repository.create_spec("local-e2e", "Small local end-to-end policy", LOCAL_SPEC_ID)
    if spec_id != LOCAL_SPEC_ID:
        raise RuntimeError("local specification identity drifted")
    draft: DatasetSpecRevision = repository.create_draft_spec_revision(local_spec())
    repository.activate_spec_revision(draft.spec_revision_id)


def create_source_table(spark: SparkSession, table: str) -> None:
    """Create the exact partitioned Iceberg source contract.

    Args:
        spark: Local Iceberg-enabled Spark session.
        table: Three-part source table identifier.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(
        f"""
        CREATE TABLE {table} (
            tenant_id STRING NOT NULL,
            namespace STRING NOT NULL,
            org_id STRING NOT NULL,
            vector_id STRING NOT NULL,
            op STRING NOT NULL,
            event_timestamp TIMESTAMP NOT NULL,
            processing_timestamp TIMESTAMP NOT NULL,
            vectors MAP<STRING, ARRAY<FLOAT>> NOT NULL,
            texts MAP<STRING, STRING> NOT NULL,
            metadata MAP<STRING, STRING> NOT NULL,
            ttl BIGINT
        ) USING iceberg
        PARTITIONED BY (tenant_id, namespace, org_id, hours(processing_timestamp))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def source_rows(first: int, last: int) -> list[tuple[object, ...]]:
    """Build deterministic source upserts for one logical dataset.

    Args:
        first: Inclusive vector ordinal.
        last: Exclusive vector ordinal.

    Returns:
        Rows matching ``SOURCE_SCHEMA``.
    """
    timestamp: datetime = datetime(2026, 7, 18, 10, tzinfo=UTC)
    rows: list[tuple[object, ...]] = []
    ordinal: int
    for ordinal in range(first, last):
        vector: list[float] = [float((ordinal + offset) % VECTOR_DIMENSION) for offset in range(VECTOR_DIMENSION)]
        rows.append(
            (
                "tenant1",
                "namespace1",
                "org1",
                f"vector-{ordinal}",
                "upsert",
                timestamp,
                timestamp,
                {"vector": vector},
                {"text": f"payload-{ordinal}"},
                {"cluster": "cluster-a"},
                0,
            )
        )
    return rows


def append_source_rows(spark: SparkSession, table: str, first: int, last: int) -> int:
    """Append source rows and return the resulting Iceberg snapshot ID.

    Args:
        spark: Local Iceberg-enabled Spark session.
        table: Source table identifier.
        first: Inclusive vector ordinal.
        last: Exclusive vector ordinal.

    Returns:
        Current exact Iceberg snapshot ID after the append.
    """
    spark.createDataFrame(source_rows(first, last), SOURCE_SCHEMA).writeTo(table).append()
    row: Row | None = (
        spark.read.format("iceberg").load(f"{table}.snapshots").orderBy("committed_at", ascending=False).first()
    )
    if row is None:
        raise RuntimeError("source append produced no Iceberg snapshot")
    return int(row["snapshot_id"])


def build_application(
    spark: SparkSession,
    repository: ControlPlaneRepository,
    table: str,
    baseline_snapshot_id: int,
    lance_base_uri: str,
) -> ReconcilerApplication:
    """Wire the real local application around migrated PostgreSQL state.

    Args:
        spark: Local Iceberg and worker execution session.
        repository: Migrated PostgreSQL repository.
        table: Source Iceberg table identifier.
        baseline_snapshot_id: Explicit canonical startup snapshot.
        lance_base_uri: Isolated local Lance root.

    Returns:
        Fully wired one-process reconciler application.
    """
    telemetry: TelemetryConfig = TelemetryConfig(service="lance-etl-local-e2e", env="test")
    bootstrap_catalog: SparkIcebergCatalog = SparkIcebergCatalog(spark)
    table_metadata: TableMetadata = bootstrap_catalog.table_metadata(table)
    source: IcebergSource = repository.ensure_source_registration(
        source_name="local",
        source_table=table,
        table_uuid=uuid.UUID(table_metadata.table_uuid),
        canonical_baseline_snapshot_id=baseline_snapshot_id,
        lance_base_uri=lance_base_uri,
    )
    source = repository.set_source_default_spec(source.source_id, LOCAL_SPEC_ID)
    settings: ReconcilerSettings = repository.reconciler_settings()
    catalog: SparkIcebergCatalog = SparkIcebergCatalog(
        spark,
        source.canonical_baseline_snapshot_id,
        source.tenant_column,
        source.namespace_column,
        source.org_column,
    )
    provider: DurableSourcePlanProvider = DurableSourcePlanProvider(
        source,
        catalog,
        repository,
        BaselineQualifier(spark),
    )
    enqueuer: SourcePlanEnqueuer = SourcePlanEnqueuer(repository, settings, source)
    ingest: DistributedIngestRunner = DistributedIngestRunner(spark, telemetry)
    publisher: ConfiguredPublicationRunner = ConfiguredPublicationRunner(
        spark,
        telemetry,
        LocalExactVersionPrewarmer(spark),
    )
    executor: FencedWorkExecutor = FencedWorkExecutor(repository, ingest, publisher, settings)
    reconciler: ResultReconciler = ResultReconciler(repository, settings)
    dispatcher: BoundedDispatcher = BoundedDispatcher(repository, executor, reconciler, settings)
    retention: PublicationRetentionSweep = PublicationRetentionSweep(repository, spark, settings, telemetry)
    return ReconcilerApplication(provider, enqueuer, dispatcher, retention, repository, MagicMock(), settings)


def durable_counts(engine: Engine) -> tuple[int, int, int, int]:
    """Return source, dataset, work, and publication counts.

    Args:
        engine: Isolated migrated PostgreSQL engine.

    Returns:
        Durable row counts in dependency order.
    """
    with engine.connect() as connection:
        return (
            int(connection.scalar(sa.select(sa.func.count()).select_from(source_snapshots)) or 0),
            int(connection.scalar(sa.select(sa.func.count()).select_from(datasets)) or 0),
            int(connection.scalar(sa.select(sa.func.count()).select_from(dataset_work)) or 0),
            int(connection.scalar(sa.select(sa.func.count()).select_from(dataset_publications)) or 0),
        )


def test_snapshot_normalization_uses_metadata_sequence_number() -> None:
    """Spark snapshot rows use the sequence number read from Iceberg metadata JSON."""
    record: Any = snapshot_from_row(
        str(uuid.uuid4()),
        7,
        23,
        Row(
            committed_at=datetime(2026, 7, 18, 10),
            snapshot_id=101,
            parent_id=100,
            operation="append",
            summary={},
        ),
    )
    assert record.snapshot_id == 101
    assert record.sequence_number == 23


@pytest.mark.integration
def test_local_reconciler_runs_baseline_increment_and_noop(
    postgres_control_plane: tuple[ControlPlaneRepository, Engine],
    local_spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Local Spark, PostgreSQL, Iceberg, and Lance converge through replay.

    Args:
        postgres_control_plane: Fresh migrated repository and engine.
        local_spark: Local Iceberg-enabled Spark session.
        tmp_path: Isolated local Lance root.
    """
    repository: ControlPlaneRepository
    engine: Engine
    repository, engine = postgres_control_plane
    table: str = "local.db.events"
    create_source_table(local_spark, table)
    baseline_snapshot_id: int = append_source_rows(local_spark, table, 0, 8)
    application: ReconcilerApplication = build_application(
        local_spark,
        repository,
        table,
        baseline_snapshot_id,
        str(tmp_path / "lance"),
    )

    baseline: Any = application.run_once()
    assert baseline.planning.enqueued_snapshots == 1
    assert baseline.dispatch.claimed == 2
    assert baseline.dispatch.succeeded == 2
    assert baseline.dispatch.retried == 0
    assert baseline.dispatch.blocked == 0
    assert durable_counts(engine) == (1, 1, 2, 1)

    identity: RoutingIdentity = RoutingIdentity("tenant1", "namespace1", "org1")
    first_serving: ServingDataset | None = repository.resolve_serving_dataset(identity)
    assert first_serving is not None
    first_dataset: Any = lance.dataset(first_serving.lance_uri, version=first_serving.lance_version)
    assert first_dataset.count_rows() == 8
    assert {description.name for description in first_dataset.describe_indices()} == {"vector_idx"}

    incremental_snapshot_id: int = append_source_rows(local_spark, table, 8, 9)
    assert incremental_snapshot_id != baseline_snapshot_id
    incremental: Any = application.run_once()
    assert incremental.planning.enqueued_snapshots == 1
    assert incremental.dispatch.claimed == 2
    assert incremental.dispatch.succeeded == 2
    assert incremental.dispatch.retried == 0
    assert incremental.dispatch.blocked == 0
    assert durable_counts(engine) == (2, 1, 4, 2)

    second_serving: ServingDataset | None = repository.resolve_serving_dataset(identity)
    assert second_serving is not None
    assert second_serving.lance_uri == first_serving.lance_uri
    assert second_serving.lance_version > first_serving.lance_version
    dataset: Any = lance.dataset(second_serving.lance_uri, version=second_serving.lance_version)
    rows: list[dict[str, object]] = dataset.to_table(columns=["vector_id", "is_deleted"]).to_pylist()
    assert {row["vector_id"] for row in rows} == {f"vector-{ordinal}" for ordinal in range(9)}
    assert all(row["is_deleted"] is False for row in rows)
    assert {description.name for description in dataset.describe_indices()} == {"vector_idx"}
    assert int(dataset.stats.index_stats("vector_idx")["num_unindexed_fragments"]) == 0

    replay_counts: tuple[int, int, int, int] = durable_counts(engine)
    replay: Any = application.run_once()
    assert replay.planning.enqueued_snapshots == 0
    assert replay.dispatch.claimed == 0
    assert durable_counts(engine) == replay_counts
    assert repository.resolve_serving_dataset(identity) == second_serving
    with engine.connect() as connection:
        assert set(connection.scalars(sa.select(dataset_work.c.state))) == {WorkState.SUCCEEDED.value}
        assert set(connection.scalars(sa.select(source_snapshots.c.state))) == {SourceSnapshotState.COMPLETE.value}
        manifests: tuple[str, ...] = tuple(connection.scalars(sa.select(dataset_publications.c.manifest_uri)))
    assert len(manifests) == 2
    assert all(Path(uri).is_file() for uri in manifests)
