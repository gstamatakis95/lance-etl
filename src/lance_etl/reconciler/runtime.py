"""Fresh-process production dependency wiring for every reconciler command."""

from __future__ import annotations

from dataclasses import dataclass

from lance_etl.cliutil import build_spark
from lance_etl.reconciler.config import RuntimeSettings, production_profile
from lance_etl.reconciler.iceberg import BaselineQualifier, DurableSourcePlanProvider, SparkIcebergCatalog
from lance_etl.reconciler.planning import SourcePlanEnqueuer
from lance_etl.reconciler.results import ReconcileSummary
from lance_etl.reconciler.service import BoundedDispatcher, ReconcilerApplication, ResultReconciler
from lance_etl.reconciler.telemetry import TelemetrySloEmitter
from lance_etl.reconciler.workers import DistributedIngestRunner, FencedWorkExecutor, ProfiledServeRunner
from lance_etl.state import ControlPlaneRepository, build_control_plane_engine
from lance_etl.telemetry import Telemetry, TelemetryConfig


@dataclass(frozen=True, slots=True)
class InlineResultSweep:
    """Report that synchronous fenced execution leaves no external result queue."""

    def reconcile(self) -> ReconcileSummary:
        """Return an empty bounded sweep because dispatch reconciles inline.

        Returns:
            Empty external-result summary.
        """
        return ReconcileSummary(inspected=0, reconciled=0, deferred=0)


def build_runtime_application() -> ReconcilerApplication:
    """Build the complete production application from deployment environment values.

    Returns:
        Fully wired application suitable for a fresh ``spark-submit`` process.
    """
    settings = RuntimeSettings.from_environment()
    profile = production_profile()
    spark = build_spark("lance-etl-reconciler")
    telemetry_config = TelemetryConfig(
        service=settings.datadog_service,
        env=settings.datadog_env,
        metric_prefix="lance.pipeline",
    )
    telemetry = Telemetry.create(telemetry_config)
    engine = build_control_plane_engine(settings.database_url)
    repository = ControlPlaneRepository(engine, settings.lance_base_uri)
    catalog = SparkIcebergCatalog(spark, settings.canonical_baseline_snapshot_id)
    provider = DurableSourcePlanProvider(
        settings.source_table,
        catalog,
        repository,
        settings.canonical_baseline_snapshot_id,
        BaselineQualifier(spark, profile),
    )
    enqueuer = SourcePlanEnqueuer(repository, profile)
    ingest = DistributedIngestRunner(spark, settings.source_table, profile, telemetry_config)
    serve = ProfiledServeRunner(spark, profile, telemetry_config)
    executor = FencedWorkExecutor(repository, ingest, serve, profile)
    result_reconciler = ResultReconciler(repository, profile)
    dispatcher = BoundedDispatcher(repository, executor, result_reconciler, profile)
    return ReconcilerApplication(
        provider,
        enqueuer,
        dispatcher,
        InlineResultSweep(),
        repository,
        TelemetrySloEmitter(telemetry),
        profile,
    )
