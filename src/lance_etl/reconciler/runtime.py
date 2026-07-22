"""Fresh-process local dependency wiring for every reconciler command."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from sqlalchemy.engine import Engine

from lance_etl.reconciler.config import DEFAULT_LOCAL_SHUFFLE_PARTITIONS, ReconcilerSettings, RuntimeSettings
from lance_etl.reconciler.iceberg import BaselineQualifier, DurableSourcePlanProvider, SparkIcebergCatalog
from lance_etl.reconciler.planning import SourcePlanEnqueuer
from lance_etl.reconciler.prewarm import ExactPrewarmer, LocalExactVersionPrewarmer
from lance_etl.reconciler.retention import PublicationRetentionSweep
from lance_etl.reconciler.service import BoundedDispatcher, ReconcilerApplication, ReconcilerOperator, ResultReconciler
from lance_etl.reconciler.telemetry import TelemetrySloEmitter
from lance_etl.reconciler.workers import ConfiguredPublicationRunner, DistributedIngestRunner, FencedWorkExecutor
from lance_etl.source import TableMetadata
from lance_etl.state import (
    ControlPlaneRepository,
    IcebergSource,
    SourceLifecycleState,
    WorkProvenance,
    build_control_plane_engine,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


def build_runtime_spark(settings: RuntimeSettings) -> SparkSession:
    """Build a local Spark session with a filesystem-backed Iceberg catalog.

    Args:
        settings: Validated local-first runtime settings.

    Returns:
        Spark session ready to create and read local Iceberg tables.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    settings.spark_warehouse_path.mkdir(parents=True, exist_ok=True)
    spark_ivy_path: Path = settings.spark_warehouse_path.parent / "ivy"
    spark_ivy_path.mkdir(parents=True, exist_ok=True)
    warehouse_uri: str = settings.spark_warehouse_path.as_uri()
    catalog_prefix: str = f"spark.sql.catalog.{settings.spark_catalog}"
    builder: SparkSession.Builder = (
        SparkSession.builder.appName("lance-etl-reconciler")
        .master(settings.spark_master)
        .config("spark.jars.packages", settings.spark_iceberg_package)
        .config("spark.jars.ivy", str(spark_ivy_path))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog_prefix}.type", "hadoop")
        .config(f"{catalog_prefix}.warehouse", warehouse_uri)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(DEFAULT_LOCAL_SHUFFLE_PARTITIONS))
        .config("spark.speculation", "false")
        .config("spark.ui.enabled", "false")
    )
    return builder.getOrCreate()


def build_runtime_prewarmer(spark: SparkSession) -> ExactPrewarmer:
    """Build the local exact-version verifier.

    Args:
        spark: Active local Spark session used for executor-owned verification.

    Returns:
        Local executor-owned exact-version verifier.
    """
    return LocalExactVersionPrewarmer(spark)


def build_runtime_application() -> ReconcilerApplication:
    """Build the complete application from local defaults and environment overrides.

    Returns:
        Fully wired application suitable for a local CLI process.
    """
    runtime_settings: RuntimeSettings = RuntimeSettings.from_environment()
    spark: SparkSession = build_runtime_spark(runtime_settings)
    engine: Engine | None = None
    try:
        telemetry_config: TelemetryConfig = TelemetryConfig(
            service=runtime_settings.datadog_service,
            env=runtime_settings.datadog_env,
            metric_prefix="lance.pipeline",
        )
        telemetry: Telemetry = Telemetry.create(telemetry_config)
        engine = build_control_plane_engine(runtime_settings.database_url)
        repository: ControlPlaneRepository = ControlPlaneRepository(engine)
        registered_source: IcebergSource | None = repository.source_by_name("local")
        if registered_source is not None:
            explicit_values: dict[str, tuple[str, str]] = {
                "LANCE_ETL_SOURCE_TABLE": (runtime_settings.source_table, registered_source.spark_table),
                "LANCE_ETL_LANCE_BASE_URI": (runtime_settings.lance_base_uri, registered_source.lance_base_uri),
            }
            environment_name: str
            environment_value: str
            database_value: str
            for environment_name, (environment_value, database_value) in explicit_values.items():
                if environment_name in os.environ and environment_value.rstrip("/") != database_value.rstrip("/"):
                    raise RuntimeError(f"{environment_name} differs from the registered PostgreSQL source")
            if (
                "LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID" in os.environ
                and runtime_settings.canonical_baseline_snapshot_id != registered_source.canonical_baseline_snapshot_id
            ):
                raise RuntimeError(
                    "LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID differs from the registered PostgreSQL source"
                )
        source_table: str = (
            registered_source.spark_table if registered_source is not None else runtime_settings.source_table
        )
        lance_base_uri: str = (
            registered_source.lance_base_uri if registered_source is not None else runtime_settings.lance_base_uri
        )
        baseline_snapshot_id: int | None = (
            registered_source.canonical_baseline_snapshot_id
            if registered_source is not None
            else runtime_settings.canonical_baseline_snapshot_id
        )
        bootstrap_catalog: SparkIcebergCatalog = SparkIcebergCatalog(spark)
        metadata: TableMetadata = bootstrap_catalog.table_metadata(source_table)
        source: IcebergSource = repository.ensure_source_registration(
            source_name="local",
            source_table=source_table,
            table_uuid=uuid.UUID(metadata.table_uuid),
            canonical_baseline_snapshot_id=baseline_snapshot_id,
            lance_base_uri=lance_base_uri,
        )
        if source.lifecycle_state is not SourceLifecycleState.ACTIVE:
            raise RuntimeError("the local Iceberg source registration is not active")
        reconciler_settings: ReconcilerSettings = ReconcilerSettings.from_environment()
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
        enqueuer: SourcePlanEnqueuer = SourcePlanEnqueuer(repository, reconciler_settings, source)
        ingest: DistributedIngestRunner = DistributedIngestRunner(spark, telemetry_config)
        prewarmer: ExactPrewarmer = build_runtime_prewarmer(spark)
        publisher: ConfiguredPublicationRunner = ConfiguredPublicationRunner(spark, telemetry_config, prewarmer)
        executor: FencedWorkExecutor = FencedWorkExecutor(repository, ingest, publisher, reconciler_settings)
        result_reconciler: ResultReconciler = ResultReconciler(repository, reconciler_settings)
        work_provenance: WorkProvenance = WorkProvenance()
        dispatcher: BoundedDispatcher = BoundedDispatcher(
            repository,
            executor,
            result_reconciler,
            reconciler_settings,
            work_provenance,
        )
        retention: PublicationRetentionSweep = PublicationRetentionSweep(
            repository, spark, reconciler_settings, telemetry_config
        )
    except Exception:
        spark.stop()
        if engine is not None:
            engine.dispose()
        raise

    def shutdown_runtime() -> None:
        """Stop local Spark and release pooled PostgreSQL connections."""
        spark.stop()
        engine.dispose()

    return ReconcilerApplication(
        provider,
        enqueuer,
        dispatcher,
        retention,
        repository,
        TelemetrySloEmitter(telemetry),
        reconciler_settings,
        shutdown_runtime,
    )


def build_runtime_operator() -> ReconcilerOperator:
    """Build the PostgreSQL-only status and repair surface.

    Returns:
        Local control-plane operator that does not create a Spark session.
    """
    settings: RuntimeSettings = RuntimeSettings.from_environment()
    telemetry: Telemetry = Telemetry.create(
        TelemetryConfig(
            service=settings.datadog_service,
            env=settings.datadog_env,
            metric_prefix="lance.pipeline",
        )
    )
    engine: Engine = build_control_plane_engine(settings.database_url)
    repository: ControlPlaneRepository = ControlPlaneRepository(engine)
    reconciler_settings: ReconcilerSettings = ReconcilerSettings.from_environment()
    return ReconcilerOperator(repository, TelemetrySloEmitter(telemetry), reconciler_settings, engine.dispose)
