"""Drive the production PostgreSQL reconciler over a benchmark Iceberg source.

This module replaces the retired ``lance_etl.pipeline.PipelineJob`` path in the end-to-end
benchmark. Instead of constructing an ETL and pipeline job directly, it stands up the exact
production control plane the local runbook uses: an isolated migrated PostgreSQL schema, a
partitioned Iceberg source table matching the fixed source contract, source registration through
:class:`~lance_etl.state.ControlPlaneRepository`, and a fully wired
:class:`~lance_etl.reconciler.service.ReconcilerApplication`. Reconciliation cycles carry every
appended snapshot through ingest, compaction, indexing, validation, prewarm, and publication, and
the benchmark reads the resulting publications back through
:meth:`~lance_etl.state.ControlPlaneRepository.resolve_serving_dataset`.

Row generation runs inside Spark executors through ``mapInArrow`` so the driver never materializes
the corpus. The source rows carry the production contract columns (the single ``ts`` timestamp and
the routing columns) and always include a deterministic ``texts`` entry so the bundled active
specification's INVERTED index builds regardless of the corpus text setting. Recall is measured only
through the standalone authenticated search command, so the benchmark never depends on the generated
text content.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pyarrow as pa
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy.engine import URL, Engine, make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from bench.config import NAMESPACE, REPO_ROOT, TENANT_ID, BenchConfig
from bench.datasets import DatasetAdapter, adapter_for
from bench.prepare import BASE_DAY_EPOCH_US, MICROS_PER_MINUTE, MINUTES_PER_DAY, single_entry_map
from bench.spark_session import bench_telemetry_config, build_spark
from lance_etl.reconciler.iceberg import (
    BaselineQualifier,
    DurableSourcePlanProvider,
    SparkIcebergCatalog,
)
from lance_etl.reconciler.planning import SourcePlanEnqueuer
from lance_etl.reconciler.prewarm import LocalExactVersionPrewarmer
from lance_etl.reconciler.retention import PublicationRetentionSweep
from lance_etl.reconciler.service import (
    BoundedDispatcher,
    ReconcilerApplication,
    ResultReconciler,
    RunOnceSummary,
    SloStatus,
)
from lance_etl.reconciler.workers import (
    ConfiguredPublicationRunner,
    DistributedIngestRunner,
    FencedWorkExecutor,
)
from lance_etl.source.models import TableMetadata
from lance_etl.state import (
    ControlPlaneRepository,
    DatasetSpecRevision,
    FieldRole,
    IcebergSource,
    IndexDefinition,
    RoutingIdentity,
    ServingDataset,
    SpecRevisionState,
    build_control_plane_engine,
    production_default_spec_revision,
)
from lance_etl.state.settings import ReconcilerSettings
from lance_etl.telemetry import TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL: str = "postgresql+psycopg://lance_etl:lance_etl@localhost:5432/lance_etl"
"""Local Compose PostgreSQL control plane used when no environment override exists."""

SOURCE_TABLE_NAMESPACE: str = "db"
"""Iceberg namespace holding the benchmark production-contract source table."""

SOURCE_TABLE_NAME: str = "events"
"""Bare name of the production-contract Iceberg source table under the bench catalog."""

MAX_DRAIN_CYCLES: int = 500
"""Upper bound on reconciliation cycles per drain to fail fast on a stuck queue."""

BENCH_SPEC_NAMESPACE: uuid.UUID = uuid.UUID("2f7a2f3c-8d1e-5a4b-9c6d-0f1e2a3b4c5d")
"""Deterministic namespace for the benchmark specification identities, isolated from the seed."""

BENCH_DEFAULT_IVF_PARTITIONS: int = 4
"""Small default IVF partition count when the benchmark does not pin one explicitly."""

BENCH_TARGET_ROWS_PER_PARTITION: int = 256
"""Bench-scale IVF target rows per partition keeping the size-aware policy small."""

PRODUCTION_ROW_DDL: str = (
    "tenant_id string, namespace string, org_id string, record_id string, op string, "
    "ts_us long, vectors map<string,array<float>>, "
    "texts map<string,string>, metadata map<string,string>"
)
"""Spark schema of the executor-generated production source batches before the timestamp cast."""


@dataclass(frozen=True, slots=True)
class NullSloEmitter:
    """Discard reconciler SLO evaluations during the offline benchmark."""

    def emit(self, status: SloStatus) -> None:
        """Ignore one evaluated SLO status.

        Args:
            status: Low-cardinality reconciler health evaluation.
        """
        del status


def resolve_database_url() -> str:
    """Resolve the PostgreSQL control-plane URL for the benchmark.

    Returns:
        A ``postgresql+psycopg`` URL from ``LANCE_ETL_TEST_DATABASE_URL`` or
        ``LANCE_ETL_DATABASE_URL`` when set, otherwise the local Compose default.
    """
    raw: str = (
        os.environ.get("LANCE_ETL_TEST_DATABASE_URL")
        or os.environ.get("LANCE_ETL_DATABASE_URL")
        or DEFAULT_DATABASE_URL
    ).strip()
    url: URL = make_url(raw)
    if url.get_backend_name() != "postgresql":
        raise ValueError("benchmark control plane requires a PostgreSQL database URL")
    return url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)


def source_table_identifier(config: BenchConfig) -> str:
    """Return the fully qualified production-contract source table identifier.

    Args:
        config: Benchmark configuration.

    Returns:
        The ``catalog.db.events`` identifier used by the reconciler.
    """
    return f"{config.catalog}.{SOURCE_TABLE_NAMESPACE}.{SOURCE_TABLE_NAME}"


@contextmanager
def isolated_control_plane(database_url: str) -> Iterator[tuple[ControlPlaneRepository, Engine]]:
    """Create, migrate, and drop an isolated PostgreSQL schema for one benchmark run.

    The schema is created on the shared database, migrated to head through Alembic, and dropped
    with all objects when the benchmark finishes, mirroring the isolated-schema pattern the
    integration tests use so a benchmark run never collides with other control-plane state.

    Args:
        database_url: A ``postgresql+psycopg`` control-plane URL.

    Yields:
        A migrated repository and its SQLAlchemy engine.
    """
    base_url: URL = make_url(database_url)
    schema_name: str = f"lance_bench_e2e_{uuid.uuid4().hex}"
    admin_engine: Engine = sa.create_engine(base_url)
    with admin_engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    isolated_url: URL = base_url.update_query_dict({"options": f"-csearch_path={schema_name}"})
    isolated_url_string: str = isolated_url.render_as_string(hide_password=False)
    engine: Engine | None = None
    try:
        alembic_config: Config = Config(str(REPO_ROOT / "alembic.ini"))
        alembic_config.set_main_option("script_location", str(REPO_ROOT / "migrations"))
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


def create_production_source_table(spark: SparkSession, table: str) -> None:
    """Create the fixed partitioned Iceberg source contract for the benchmark.

    Args:
        spark: Local Iceberg-enabled Spark session.
        table: Three-part source table identifier.
    """
    namespace: str = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"""
        CREATE TABLE {table} (
            tenant_id STRING NOT NULL,
            namespace STRING NOT NULL,
            org_id STRING NOT NULL,
            record_id STRING NOT NULL,
            op STRING NOT NULL,
            ts TIMESTAMP NOT NULL,
            vectors MAP<STRING, ARRAY<FLOAT>> NOT NULL,
            texts MAP<STRING, STRING> NOT NULL,
            metadata MAP<STRING, STRING> NOT NULL
        ) USING iceberg
        PARTITIONED BY (tenant_id, namespace, org_id, hours(ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def production_arrow_schema() -> pa.Schema:
    """Return the Arrow schema of the executor-generated production source batches.

    Returns:
        The schema matching :data:`PRODUCTION_ROW_DDL`.
    """
    return pa.schema(
        [
            ("tenant_id", pa.string()),
            ("namespace", pa.string()),
            ("org_id", pa.string()),
            ("record_id", pa.string()),
            ("op", pa.string()),
            ("ts_us", pa.int64()),
            ("vectors", pa.map_(pa.string(), pa.list_(pa.float32()))),
            ("texts", pa.map_(pa.string(), pa.string())),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )


def production_slice_batch(
    start: int,
    count: int,
    adapter: DatasetAdapter,
    corpus_root: Path,
    tenants: int,
    num_clusters: int,
) -> pa.RecordBatch:
    """Build one production-contract source batch for a contiguous vector slice.

    Runs inside a Spark executor task: reads its own base-vector slice through the pickled dataset
    adapter and emits the fixed source columns. The vector rides in the ``vectors`` map under the
    ``"vector"`` key, a deterministic document rides in the ``texts`` map under ``"text"``, and a
    deterministic cluster bucket rides in the ``metadata`` map under ``"cluster"``.

    Args:
        start: First global vector ordinal of the slice.
        count: Vectors in the slice.
        adapter: Dataset adapter pickled into the task closure.
        corpus_root: Shared corpus cache directory readable from the executor.
        tenants: Round-robin organization count.
        num_clusters: Deterministic cluster-bucket cardinality.

    Returns:
        One record batch conforming to :func:`production_arrow_schema`.
    """
    vectors: np.ndarray = adapter.base_vector_slice(corpus_root, start, count)
    indices: np.ndarray = np.arange(start, start + count, dtype=np.int64)
    minutes: np.ndarray = (indices % MINUTES_PER_DAY).astype(np.int64)
    timestamps_us: np.ndarray = BASE_DAY_EPOCH_US + minutes * MICROS_PER_MINUTE
    clusters: list[int] = [int(i) % num_clusters for i in indices]
    flat_offsets: pa.Array = pa.array(np.arange(count + 1, dtype=np.int32) * vectors.shape[1])
    vector_items: pa.ListArray = pa.ListArray.from_arrays(flat_offsets, pa.array(vectors.ravel(), pa.float32()))
    arrays: list[pa.Array] = [
        pa.array([TENANT_ID] * count, pa.string()),
        pa.array([NAMESPACE] * count, pa.string()),
        pa.array([f"org{int(i) % tenants}" for i in indices], pa.string()),
        pa.array([str(int(i)) for i in indices], pa.string()),
        pa.array(["upsert"] * count, pa.string()),
        pa.array(timestamps_us),
        single_entry_map(["vector"] * count, vector_items),
        single_entry_map(
            ["text"] * count,
            pa.array([f"cluster {cluster} document {int(i)}" for cluster, i in zip(clusters, indices, strict=True)]),
        ),
        single_entry_map(["cluster"] * count, pa.array([str(cluster) for cluster in clusters], pa.string())),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=production_arrow_schema())


def append_production_batch(
    spark: SparkSession,
    config: BenchConfig,
    table: str,
    adapter: DatasetAdapter,
    window: tuple[int, int],
) -> int:
    """Append one ordinal window of production source rows and return its snapshot ID.

    Args:
        spark: Local Iceberg-enabled Spark session.
        config: Benchmark configuration.
        table: Source table identifier.
        adapter: Dataset adapter providing executor-side base-vector slices.
        window: Inclusive-exclusive ``(first, last)`` vector ordinals for this batch.

    Returns:
        The Iceberg snapshot ID committed by the append.
    """
    first: int
    last: int
    first, last = window
    corpus_root: Path = config.corpus_root
    tenants: int = config.tenants
    num_clusters: int = config.num_clusters
    slices: list[tuple[int, int]] = [
        (start, min(config.rows_per_slice, last - start)) for start in range(first, last, config.rows_per_slice)
    ]

    def generate(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Generate production source batches for the slice specs assigned to this task.

        Args:
            batches: Arrow batches of ``(start, count)`` slice specs.

        Yields:
            One production source batch per slice spec.
        """
        for batch in batches:
            starts: list[int] = batch.column("start").to_pylist()
            counts: list[int] = batch.column("count").to_pylist()
            for start, count in zip(starts, counts, strict=True):
                yield production_slice_batch(int(start), int(count), adapter, corpus_root, tenants, num_clusters)

    specs = spark.createDataFrame(slices, "start long, count long").repartition(len(slices))
    rows = specs.mapInArrow(generate, schema=PRODUCTION_ROW_DDL)
    rows = rows.withColumn("ts", F.timestamp_micros(F.col("ts_us"))).drop("ts_us")
    rows.writeTo(table).append()
    snapshot = spark.read.format("iceberg").load(f"{table}.snapshots").orderBy("committed_at", ascending=False).first()
    if snapshot is None:
        raise RuntimeError("production source append produced no Iceberg snapshot")
    return int(snapshot["snapshot_id"])


def bench_spec_revision(config: BenchConfig) -> DatasetSpecRevision:
    """Build a bench-scale DRAFT specification the reconciler qualifies at small row counts.

    The specification clones the bundled production default (every field role and every index
    family) and re-identifies the graph so a benchmark run never collides with the seeded default.
    The only production surfaces it narrows are scale knobs: the IVF_RQ index is sized for a small
    local corpus and the ingest shuffle width is set from ``config.etl_partitions``. Compaction
    stays enabled, all six production indexes are kept including the INVERTED full-text index, and
    ``record_retention_seconds`` stays ``None`` so no retention sweep expires benchmark rows. The
    benchmark therefore exercises the exact production publish path: compaction before indexing and
    the full index-coverage and index-kind publish gates over every index family.

    Args:
        config: Benchmark configuration.

    Returns:
        A validated small DRAFT specification revision carrying the corpus vector dimension.
    """
    base: DatasetSpecRevision = production_default_spec_revision()
    dimension: int = adapter_for(config).dimension
    revision_id: uuid.UUID = uuid.uuid5(BENCH_SPEC_NAMESPACE, "revision")
    spec_id: uuid.UUID = uuid.uuid5(BENCH_SPEC_NAMESPACE, "spec")
    field_ids: dict[uuid.UUID, uuid.UUID] = {
        field.field_id: uuid.uuid5(revision_id, f"field:{field.target_name}") for field in base.fields
    }
    fields: tuple = tuple(
        replace(
            field,
            field_id=field_ids[field.field_id],
            spec_revision_id=revision_id,
            data_type=f"fixed_size_list<float32,{dimension}>" if field.role is FieldRole.VECTOR else field.data_type,
            vector_dimension=dimension if field.role is FieldRole.VECTOR else field.vector_dimension,
        )
        for field in base.fields
    )
    partitions: int = config.ivf_partitions or BENCH_DEFAULT_IVF_PARTITIONS
    indexes: list[IndexDefinition] = []
    definition: IndexDefinition
    for definition in base.index_definitions:
        vector_options = definition.vector_options
        if vector_options is not None:
            vector_options = replace(
                vector_options,
                num_partitions=partitions,
                minimum_partitions=1,
                maximum_partitions=max(partitions, vector_options.maximum_partitions),
                target_rows_per_partition=BENCH_TARGET_ROWS_PER_PARTITION,
                minimum_rows=1,
            )
        indexes.append(
            replace(
                definition,
                index_definition_id=uuid.uuid5(revision_id, f"index:{definition.index_name}"),
                spec_revision_id=revision_id,
                field_id=field_ids[definition.field_id],
                vector_options=vector_options,
            )
        )
    candidate: DatasetSpecRevision = replace(
        base,
        spec_id=spec_id,
        spec_revision_id=revision_id,
        revision_number=1,
        state=SpecRevisionState.DRAFT,
        supersedes_revision_id=None,
        fields=fields,
        indexes=tuple(indexes),
        ingest_shuffle_partitions=config.etl_partitions,
        compaction_enabled=True,
        record_retention_seconds=None,
        configuration_digest=b"",
    )
    return candidate


def bench_spec_index_names(config: BenchConfig) -> frozenset[str]:
    """Return every index name the bench specification declares.

    Re-identifying the graph preserves each index's stable ``index_name``, so these are exactly the
    index names a converged benchmark publication must carry. Used by the publication verification
    to assert full index coverage against the installed spec rather than a hardcoded list.

    Args:
        config: Benchmark configuration.

    Returns:
        The declared index names of the installed bench specification.
    """
    return frozenset(definition.index_name for definition in bench_spec_revision(config).indexes)


def install_bench_spec(
    repository: ControlPlaneRepository,
    source: IcebergSource,
    config: BenchConfig,
    spec_revision: DatasetSpecRevision | None = None,
) -> IcebergSource:
    """Install, activate, and select the bench-scale specification for the source.

    Args:
        repository: Migrated PostgreSQL repository.
        source: Registered Iceberg source pointing at the seeded default specification.
        config: Benchmark configuration.
        spec_revision: Explicit DRAFT revision to install. ``None`` uses the default bench spec so
            existing callers are unaffected.

    Returns:
        The source repointed at the activated bench specification.
    """
    candidate: DatasetSpecRevision = spec_revision if spec_revision is not None else bench_spec_revision(config)
    draft: DatasetSpecRevision = repository.create_draft_spec_revision(
        candidate,
        "bench-e2e",
        "Benchmark reconciler-driven end-to-end policy",
    )
    repository.activate_spec_revision(draft.spec_revision_id)
    return repository.set_source_default_spec(source.source_id, draft.spec_id)


def build_reconciler_application(
    spark: SparkSession,
    repository: ControlPlaneRepository,
    table: str,
    baseline_snapshot_id: int,
    config: BenchConfig,
    spec_revision: DatasetSpecRevision | None = None,
) -> ReconcilerApplication:
    """Register the source, install the bench spec, and wire the one-process reconciler.

    Source registration first points every discovered dataset at the seeded default specification,
    then the bench-scale specification is installed and selected as the source default before any
    planning occurs, so every enqueued dataset materializes the small bench policy. The wiring
    mirrors the local runbook and the integration test: an Iceberg plan provider, a source-plan
    enqueuer, a distributed ingest runner, a publication runner with a local exact-version
    prewarmer, a fenced executor, a bounded dispatcher, and a publication retention sweep.

    Args:
        spark: Local Iceberg and worker execution session.
        repository: Migrated PostgreSQL repository.
        table: Source Iceberg table identifier.
        baseline_snapshot_id: Canonical startup snapshot committed by the first append.
        config: Benchmark configuration owning the spec sizing and the Lance root.
        spec_revision: Explicit DRAFT revision to install. ``None`` installs the default bench spec,
            preserving the exact end-to-end wiring for existing callers.

    Returns:
        A fully wired reconciler application.
    """
    telemetry: TelemetryConfig = bench_telemetry_config()
    bootstrap_catalog: SparkIcebergCatalog = SparkIcebergCatalog(spark)
    table_metadata: TableMetadata = bootstrap_catalog.table_metadata(table)
    source: IcebergSource = repository.ensure_source_registration(
        source_name="bench",
        source_table=table,
        table_uuid=uuid.UUID(table_metadata.table_uuid),
        canonical_baseline_snapshot_id=baseline_snapshot_id,
        lance_base_uri=str(config.lance_root()),
    )
    source = install_bench_spec(repository, source, config, spec_revision)
    settings: ReconcilerSettings = ReconcilerSettings.from_environment()
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
    return ReconcilerApplication(
        provider,
        enqueuer,
        dispatcher,
        retention,
        repository,
        NullSloEmitter(),
        settings,
    )


def drain_reconciler(application: ReconcilerApplication, raise_on_blocked: bool = True) -> dict[str, int]:
    """Run reconciliation cycles until the source and work queues are quiescent.

    Args:
        application: Wired reconciler application.
        raise_on_blocked: When true (the default, preserving existing behavior) any blocked cycle
            raises immediately. When false, blocked cycles accumulate into the totals and the drain
            proceeds to quiescence so a caller can inspect the blocked work row.

    Returns:
        Aggregate counts across every cycle: ``cycles``, ``enqueued``, ``claimed``, ``succeeded``,
        ``advanced``, ``retried``, and ``blocked``.

    Raises:
        RuntimeError: If the queue does not reach quiescence within the cycle bound, or, when
            ``raise_on_blocked`` is true, if any work was blocked.
    """
    totals: dict[str, int] = {
        "cycles": 0,
        "enqueued": 0,
        "claimed": 0,
        "succeeded": 0,
        "advanced": 0,
        "retried": 0,
        "blocked": 0,
    }
    while totals["cycles"] < MAX_DRAIN_CYCLES:
        summary: RunOnceSummary = application.run_once()
        totals["cycles"] += 1
        totals["enqueued"] += summary.planning.enqueued_snapshots
        totals["claimed"] += summary.dispatch.claimed
        totals["succeeded"] += summary.dispatch.succeeded
        totals["advanced"] += summary.dispatch.advanced
        totals["retried"] += summary.dispatch.retried
        totals["blocked"] += summary.dispatch.blocked
        if summary.dispatch.blocked and raise_on_blocked:
            raise RuntimeError("reconciler blocked benchmark work; inspect dataset_work error evidence")
        if summary.planning.enqueued_snapshots == 0 and summary.dispatch.claimed == 0:
            return totals
    raise RuntimeError("reconciler did not reach quiescence within the benchmark cycle bound")


def expected_org_rows(config: BenchConfig) -> dict[str, int]:
    """Compute the expected published row count per organization.

    Every vector ordinal is emitted exactly once under a unique ``record_id``, so the terminal
    published cardinality of one organization equals the number of ordinals routed to it.

    Args:
        config: Benchmark configuration.

    Returns:
        Expected row count keyed by organization identifier.
    """
    counts: dict[str, int] = {org: 0 for org in config.org_ids()}
    for ordinal in range(config.limit):
        counts[f"org{ordinal % config.tenants}"] += 1
    return counts


def resolve_org_serving(repository: ControlPlaneRepository, org: str) -> ServingDataset | None:
    """Resolve the active publication for one benchmark organization.

    Args:
        repository: Migrated PostgreSQL repository.
        org: Organization identifier.

    Returns:
        The active serving dataset, or ``None`` when no publication exists.
    """
    identity: RoutingIdentity = RoutingIdentity(TENANT_ID, NAMESPACE, org)
    return repository.resolve_serving_dataset(identity)


def bench_workspace_spark(config: BenchConfig) -> SparkSession:
    """Build the shared local Spark session for the reconciler benchmark.

    Args:
        config: Benchmark configuration.

    Returns:
        The active local Iceberg-enabled Spark session.
    """
    return build_spark(config, "bench-e2e-reconcile")


def batch_windows_by_ordinal(config: BenchConfig) -> list[tuple[int, int]]:
    """Split the corpus ordinals into consecutive append windows.

    Args:
        config: Benchmark configuration.

    Returns:
        One inclusive-exclusive ``(first, last)`` ordinal window per batch, covering the corpus.

    Raises:
        ValueError: If ``config.batches`` is not positive, or exceeds ``config.limit`` so at least
            one window would be empty and drive a ``.repartition(0)`` on the append task.
    """
    if config.batches < 1:
        raise ValueError(f"--batches must be positive, got {config.batches}")
    if config.batches > config.limit:
        raise ValueError(
            f"--batches ({config.batches}) exceeds the row budget --limit ({config.limit}); "
            f"each batch needs at least one row. Reduce --batches to at most {config.limit}."
        )
    boundaries: list[int] = [batch * config.limit // config.batches for batch in range(config.batches)]
    boundaries.append(config.limit)
    return [(boundaries[index], boundaries[index + 1]) for index in range(config.batches)]


def benchmark_adapter(config: BenchConfig) -> DatasetAdapter:
    """Resolve the dataset adapter for the configured corpus.

    Args:
        config: Benchmark configuration.

    Returns:
        The dataset adapter driving the benchmark corpus.
    """
    return adapter_for(config)
