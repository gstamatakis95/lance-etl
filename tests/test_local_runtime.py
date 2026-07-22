"""Tests for the local-first reconciler runtime slice."""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest

import lance_etl.reconciler.runtime as reconciler_runtime
from lance_etl.publication.workflow import PrewarmResult
from lance_etl.reconciler import ReconcilerApplication
from lance_etl.reconciler.cli import build_parser, execute_command
from lance_etl.reconciler.config import (
    DEFAULT_DATABASE_URL,
    DEFAULT_ICEBERG_PACKAGE,
    ReconcilerSettings,
    RuntimeSettings,
)
from lance_etl.reconciler.prewarm import LocalExactVersionPrewarmer
from lance_etl.state import IcebergSource, RoutingIdentity, SourceLifecycleState

RUNTIME_ENVIRONMENT_NAMES: tuple[str, ...] = (
    "LANCE_ETL_LOCAL_ROOT",
    "LANCE_ETL_DATABASE_URL",
    "LANCE_ETL_LANCE_BASE_URI",
    "LANCE_ETL_SOURCE_TABLE",
    "LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID",
    "LANCE_ETL_SPARK_MASTER",
    "LANCE_ETL_SPARK_CATALOG",
    "LANCE_ETL_SPARK_WAREHOUSE",
    "LANCE_ETL_SPARK_ICEBERG_PACKAGE",
    "DD_SERVICE",
    "DD_ENV",
)
"""Environment inputs cleared by local-default tests."""


def clear_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove every runtime override from one test environment.

    Args:
        monkeypatch: Scoped environment fixture.
    """
    name: str
    for name in RUNTIME_ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)


def local_settings(tmp_path: Path) -> RuntimeSettings:
    """Build direct local settings for runtime wiring tests.

    Args:
        tmp_path: Isolated local workspace.

    Returns:
        Complete local runtime settings.
    """
    return RuntimeSettings(
        database_url=DEFAULT_DATABASE_URL,
        lance_base_uri=str(tmp_path / "lance"),
        source_table="local.db.events",
        datadog_service="lance-etl-local",
        datadog_env="local",
        canonical_baseline_snapshot_id=None,
        spark_master="local[2]",
        spark_catalog="local",
        spark_warehouse_path=tmp_path / "iceberg",
        spark_iceberg_package=DEFAULT_ICEBERG_PACKAGE,
    )


def test_runtime_settings_default_to_local_workspace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An empty environment yields a complete localhost configuration.

    Args:
        monkeypatch: Scoped environment fixture.
        tmp_path: Isolated working directory.
    """
    clear_runtime_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    settings: RuntimeSettings = RuntimeSettings.from_environment()
    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.lance_base_uri == str(tmp_path / ".lance-etl" / "lance")
    assert settings.source_table == "local.db.events"
    assert settings.spark_master == "local[*]"
    assert settings.spark_catalog == "local"
    assert settings.spark_warehouse_path == tmp_path / ".lance-etl" / "iceberg"
    assert settings.spark_iceberg_package == DEFAULT_ICEBERG_PACKAGE


def test_runtime_settings_allow_local_postgres(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Local PostgreSQL needs no TLS while remote PostgreSQL remains verified.

    Args:
        monkeypatch: Scoped environment fixture.
        tmp_path: Isolated working directory.
    """
    clear_runtime_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LANCE_ETL_DATABASE_URL", "postgresql+psycopg://127.0.0.1/test")
    assert RuntimeSettings.from_environment().database_url.endswith("/test")
    monkeypatch.setenv("LANCE_ETL_DATABASE_URL", "postgresql+psycopg://database.example.com/test")
    with pytest.raises(ValueError, match="remote PostgreSQL"):
        RuntimeSettings.from_environment()


def test_runtime_settings_reject_remote_spark_master(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The local-only runtime rejects remote Spark cluster masters.

    Args:
        monkeypatch: Scoped environment fixture.
        tmp_path: Isolated working directory.
    """
    clear_runtime_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LANCE_ETL_SPARK_MASTER", "spark://remote.example:7077")
    with pytest.raises(ValueError, match="Spark master must be local"):
        RuntimeSettings.from_environment()


def test_runtime_spark_builds_local_iceberg_catalog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Runtime Spark wiring supplies the complete local Iceberg catalog.

    Args:
        monkeypatch: Scoped Spark builder fixture.
        tmp_path: Isolated local warehouse.
    """
    settings: RuntimeSettings = local_settings(tmp_path)
    builder: MagicMock = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    session: MagicMock = MagicMock()
    builder.getOrCreate.return_value = session
    spark_type: SimpleNamespace = SimpleNamespace(builder=builder)
    monkeypatch.setattr(reconciler_runtime, "SparkSession", spark_type)
    assert reconciler_runtime.build_runtime_spark(settings) is session
    assert settings.spark_warehouse_path.is_dir()
    builder.master.assert_called_once_with("local[2]")
    configuration: dict[str, object] = {call.args[0]: call.args[1] for call in builder.config.call_args_list}
    assert configuration["spark.jars.packages"] == DEFAULT_ICEBERG_PACKAGE
    assert configuration["spark.jars.ivy"] == str(settings.spark_warehouse_path.parent / "ivy")
    assert (settings.spark_warehouse_path.parent / "ivy").is_dir()
    assert configuration["spark.sql.extensions"] == "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    assert configuration["spark.sql.catalog.local"] == "org.apache.iceberg.spark.SparkCatalog"
    assert configuration["spark.sql.catalog.local.type"] == "hadoop"
    assert configuration["spark.sql.catalog.local.warehouse"] == settings.spark_warehouse_path.as_uri()
    assert configuration["spark.sql.session.timeZone"] == "UTC"
    assert configuration["spark.sql.shuffle.partitions"] == "8"


def test_local_prewarmer_opens_exact_lance_version(tmp_path: Path) -> None:
    """The local fallback verifies the requested version inside its executor seam.

    Args:
        tmp_path: Isolated candidate dataset root.
    """
    candidate: lance.LanceDataset = lance.write_dataset(pa.table({"id": [1]}), str(tmp_path / "candidate.lance"))
    spark: MagicMock = MagicMock()

    def parallelize(values: list[tuple[str, int]], partitions: int) -> MagicMock:
        """Build a direct in-process RDD fixture.

        Args:
            values: Candidate tasks.
            partitions: Requested local task count.

        Returns:
            RDD fixture executing its mapping callable immediately.
        """
        assert partitions == 1
        rdd: MagicMock = MagicMock()

        def map_values(mapper: Callable[[tuple[str, int]], PrewarmResult]) -> MagicMock:
            """Apply one executor mapper to every fixture task.

            Args:
                mapper: Exact candidate inspection callable.

            Returns:
                RDD fixture carrying collected results.
            """
            rdd.collect.return_value = [mapper(value) for value in values]
            return rdd

        rdd.map.side_effect = map_values
        return rdd

    spark.sparkContext.parallelize.side_effect = parallelize
    prewarmer: LocalExactVersionPrewarmer = LocalExactVersionPrewarmer(spark)
    results: tuple[PrewarmResult, ...] = prewarmer.prewarm(
        RoutingIdentity("tenant1", "namespace1", "org1"),
        candidate.uri,
        candidate.version,
    )
    assert results == (PrewarmResult("local", candidate.uri, candidate.version),)
    with pytest.raises(RuntimeError, match="local exact-version"):
        prewarmer.prewarm(
            RoutingIdentity("tenant1", "namespace1", "org1"),
            candidate.uri,
            candidate.version + 100,
        )


def test_runtime_selects_local_prewarmer(tmp_path: Path) -> None:
    """Publication prewarm always uses local exact-version verification.

    Args:
        tmp_path: Isolated local runtime root.
    """
    spark: MagicMock = MagicMock()
    local_settings(tmp_path)
    assert isinstance(reconciler_runtime.build_runtime_prewarmer(spark), LocalExactVersionPrewarmer)


def test_runtime_bootstraps_source_then_uses_postgres_truth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The full local runtime freezes source bootstrap values and loads queue settings from PostgreSQL.

    Args:
        monkeypatch: Scoped dependency fixture.
        tmp_path: Isolated local storage root.
    """
    runtime_settings: RuntimeSettings = replace(local_settings(tmp_path), canonical_baseline_snapshot_id=41)
    table_uuid: uuid.UUID = uuid.uuid4()
    source: IcebergSource = IcebergSource(
        source_id=uuid.uuid4(),
        source_name="local",
        spark_catalog="local",
        table_namespace="db",
        table_name="events",
        table_uuid=table_uuid,
        lance_base_uri=runtime_settings.lance_base_uri,
        lifecycle_state=SourceLifecycleState.ACTIVE,
        default_spec_id=uuid.uuid4(),
        canonical_baseline_snapshot_id=41,
        replay_horizon=timedelta(days=30),
    )
    reconciler_settings: ReconcilerSettings = replace(
        ReconcilerSettings(), poll_interval=timedelta(seconds=19)
    ).validate()
    spark: MagicMock = MagicMock()
    repository: MagicMock = MagicMock()
    repository.source_by_name.return_value = None
    repository.ensure_source_registration.return_value = source
    bootstrap_catalog: MagicMock = MagicMock()
    bootstrap_catalog.table_metadata.return_value = SimpleNamespace(table_uuid=str(table_uuid))
    runtime_catalog: MagicMock = MagicMock()
    catalog_factory: MagicMock = MagicMock(side_effect=(bootstrap_catalog, runtime_catalog))
    repository_factory: MagicMock = MagicMock(return_value=repository)
    telemetry: MagicMock = MagicMock()
    prewarmer: MagicMock = MagicMock()
    engine: MagicMock = MagicMock()
    monkeypatch.setattr(
        reconciler_runtime.RuntimeSettings, "from_environment", MagicMock(return_value=runtime_settings)
    )
    monkeypatch.setattr(
        reconciler_runtime.ReconcilerSettings, "from_environment", MagicMock(return_value=reconciler_settings)
    )
    monkeypatch.setattr(reconciler_runtime, "build_runtime_spark", MagicMock(return_value=spark))
    monkeypatch.setattr(reconciler_runtime.Telemetry, "create", MagicMock(return_value=telemetry))
    monkeypatch.setattr(reconciler_runtime, "build_control_plane_engine", MagicMock(return_value=engine))
    monkeypatch.setattr(reconciler_runtime, "ControlPlaneRepository", repository_factory)
    monkeypatch.setattr(reconciler_runtime, "SparkIcebergCatalog", catalog_factory)
    monkeypatch.setattr(reconciler_runtime, "build_runtime_prewarmer", MagicMock(return_value=prewarmer))

    application: ReconcilerApplication = reconciler_runtime.build_runtime_application()

    repository_factory.assert_called_once_with(engine)
    repository.ensure_source_registration.assert_called_once_with(
        source_name="local",
        source_table="local.db.events",
        table_uuid=table_uuid,
        canonical_baseline_snapshot_id=41,
        lance_base_uri=runtime_settings.lance_base_uri,
    )
    assert catalog_factory.call_args_list[1].args == (spark, 41, "tenant_id", "namespace", "org_id")
    assert application.settings is reconciler_settings
    assert application.plan_provider.source is source
    assert application.plan_enqueuer.source is source
    application.close()
    spark.stop.assert_called_once_with()
    engine.dispose.assert_called_once_with()


def test_runtime_bootstrap_failure_stops_spark_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A bootstrap failure after Spark and the PostgreSQL engine exist releases both before raising.

    Args:
        monkeypatch: Scoped dependency fixture.
        tmp_path: Isolated local storage root.
    """
    runtime_settings: RuntimeSettings = replace(local_settings(tmp_path), canonical_baseline_snapshot_id=41)
    table_uuid: uuid.UUID = uuid.uuid4()
    paused_source: IcebergSource = IcebergSource(
        source_id=uuid.uuid4(),
        source_name="local",
        spark_catalog="local",
        table_namespace="db",
        table_name="events",
        table_uuid=table_uuid,
        lance_base_uri=runtime_settings.lance_base_uri,
        lifecycle_state=SourceLifecycleState.PAUSED,
        default_spec_id=uuid.uuid4(),
        canonical_baseline_snapshot_id=41,
        replay_horizon=timedelta(days=30),
    )
    reconciler_settings: ReconcilerSettings = replace(
        ReconcilerSettings(), poll_interval=timedelta(seconds=19)
    ).validate()
    spark: MagicMock = MagicMock()
    repository: MagicMock = MagicMock()
    repository.source_by_name.return_value = None
    repository.ensure_source_registration.return_value = paused_source
    bootstrap_catalog: MagicMock = MagicMock()
    bootstrap_catalog.table_metadata.return_value = SimpleNamespace(table_uuid=str(table_uuid))
    catalog_factory: MagicMock = MagicMock(return_value=bootstrap_catalog)
    repository_factory: MagicMock = MagicMock(return_value=repository)
    telemetry: MagicMock = MagicMock()
    engine: MagicMock = MagicMock()
    monkeypatch.setattr(
        reconciler_runtime.RuntimeSettings, "from_environment", MagicMock(return_value=runtime_settings)
    )
    monkeypatch.setattr(
        reconciler_runtime.ReconcilerSettings, "from_environment", MagicMock(return_value=reconciler_settings)
    )
    monkeypatch.setattr(reconciler_runtime, "build_runtime_spark", MagicMock(return_value=spark))
    monkeypatch.setattr(reconciler_runtime.Telemetry, "create", MagicMock(return_value=telemetry))
    monkeypatch.setattr(reconciler_runtime, "build_control_plane_engine", MagicMock(return_value=engine))
    monkeypatch.setattr(reconciler_runtime, "ControlPlaneRepository", repository_factory)
    monkeypatch.setattr(reconciler_runtime, "SparkIcebergCatalog", catalog_factory)

    with pytest.raises(RuntimeError, match="not active"):
        reconciler_runtime.build_runtime_application()

    spark.stop.assert_called_once_with()
    engine.dispose.assert_called_once_with()


def test_runtime_bootstrap_failure_before_engine_exists_still_stops_spark(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failure before the PostgreSQL engine is built still stops the already-created Spark session.

    Args:
        monkeypatch: Scoped dependency fixture.
        tmp_path: Isolated local storage root.
    """
    runtime_settings: RuntimeSettings = local_settings(tmp_path)
    spark: MagicMock = MagicMock()
    monkeypatch.setattr(
        reconciler_runtime.RuntimeSettings, "from_environment", MagicMock(return_value=runtime_settings)
    )
    monkeypatch.setattr(reconciler_runtime, "build_runtime_spark", MagicMock(return_value=spark))
    monkeypatch.setattr(reconciler_runtime.Telemetry, "create", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(
        reconciler_runtime,
        "build_control_plane_engine",
        MagicMock(side_effect=RuntimeError("cannot reach PostgreSQL")),
    )

    with pytest.raises(RuntimeError, match="cannot reach PostgreSQL"):
        reconciler_runtime.build_runtime_application()

    spark.stop.assert_called_once_with()


def test_run_command_uses_postgres_poll_interval_by_default() -> None:
    """Continuous local execution inherits its default cadence from PostgreSQL."""
    application: MagicMock = MagicMock()
    application.settings.poll_interval = timedelta(seconds=17)
    slept: list[float] = []

    def interrupt(seconds: float) -> None:
        """Record the selected cadence and stop the loop.

        Args:
            seconds: Selected polling delay.

        Raises:
            KeyboardInterrupt: Always, after recording the first delay.
        """
        slept.append(seconds)
        raise KeyboardInterrupt

    result: object = execute_command(application, build_parser().parse_args(["run"]), sleep=interrupt)
    assert result is application.run_once.return_value
    assert slept == [17.0]


def test_cli_excludes_retired_rollback_action() -> None:
    """The local repair surface exposes rebuild but no retired rollback work kind."""
    parser: argparse.ArgumentParser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["repair", "--action", "rollback"])
