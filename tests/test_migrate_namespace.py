"""Tests for the namespace copy/migrate utility.

Covers the pure planning helpers (config validation, path-component parsing, target-URI swapping, source discovery)
without Spark, and an end-to-end migration on a local Spark session asserting that target datasets are written with the
same row data, the source datasets are kept intact, an existing target blocks the run unless ``overwrite_target`` is
set, the distributed large-tier copy produces the same data, and the recompact and reindex flags take effect on the
targets.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import lance
import pytest
from conftest import FakeSpark, make_vector_table
from pyspark.sql import SparkSession

import lance_etl.migrate_namespace as migrate_namespace_module
import lance_etl.tools.cli as tools_cli
from lance_etl.cliutil import EXIT_PARTIAL_FAILURE
from lance_etl.indexing import IndexJobConfig, bitmap_index_name
from lance_etl.migrate_namespace import (
    MigrateConfig,
    MigrateReport,
    NamespaceMigrator,
    build_dataset_uri,
    source_dataset_uris,
    target_uri_for,
    validate_config,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session pinned to the test interpreter.

    Yields:
        A two-core local session with four shuffle partitions and the UI disabled.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-migrate-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_source_dataset(base_uri: str, components: list[str], rows: int, max_rows_per_file: int = 1024) -> str:
    """Write a tiny source dataset at a routing path and return its URI.

    Args:
        base_uri: Root location for the dataset fleet.
        components: Routing values in path order, the last being a namespace component.
        rows: Number of rows to generate.
        max_rows_per_file: Row cap per fragment file, controlling fragment count.

    Returns:
        The written dataset URI.
    """
    uri: str = f"{base_uri.rstrip('/')}/{'/'.join(components)}.lance"
    lance.write_dataset(make_vector_table(rows, dim=8), uri, max_rows_per_file=max_rows_per_file)
    return uri


def base_config(base_uri: str, telemetry_config: TelemetryConfig, **overrides: object) -> MigrateConfig:
    """Build a migrate config rooted at a base URI with copy-only defaults.

    Args:
        base_uri: Root location for the dataset fleet.
        telemetry_config: The test telemetry configuration.
        **overrides: Fields to override on the configuration.

    Returns:
        A migrate configuration with recompact and reindex disabled unless overridden.
    """
    fields: dict[str, object] = {
        "source_namespace": "nsA",
        "target_namespace": "nsB",
        "base_uri": base_uri,
        "telemetry": telemetry_config,
        "recompact": False,
        "reindex": False,
    }
    fields.update(overrides)
    return MigrateConfig(**fields)


class TestValidateConfig:
    """Configuration validation rejects unsafe or contradictory inputs."""

    def test_equal_namespaces_rejected(self, telemetry_config: TelemetryConfig) -> None:
        """A copy onto the same namespace would clobber its own source."""
        with pytest.raises(ValueError, match="must differ"):
            validate_config(base_config("/tmp/x", telemetry_config, source_namespace="ns", target_namespace="ns"))

    def test_namespace_col_must_be_partition_col(self, telemetry_config: TelemetryConfig) -> None:
        """The fixed "namespace" partition column must be present in partition_cols."""
        with pytest.raises(ValueError, match="not in partition_cols"):
            validate_config(base_config("/tmp/x", telemetry_config, partition_cols=["org_id", "tenant_id"]))

    @pytest.mark.parametrize("traversal", [".", ".."])
    def test_target_namespace_rejects_traversal(self, telemetry_config: TelemetryConfig, traversal: str) -> None:
        """A ``.`` or ``..`` target_namespace is rejected instead of escaping the base_uri tree."""
        with pytest.raises(ValueError, match="non-traversal"):
            validate_config(base_config("/tmp/x", telemetry_config, source_namespace="ns", target_namespace=traversal))

    @pytest.mark.parametrize("traversal", [".", ".."])
    def test_source_namespace_rejects_traversal(self, telemetry_config: TelemetryConfig, traversal: str) -> None:
        """A ``.`` or ``..`` source_namespace is rejected instead of escaping the base_uri tree."""
        with pytest.raises(ValueError, match="non-traversal"):
            validate_config(base_config("/tmp/x", telemetry_config, source_namespace=traversal, target_namespace="ns"))


class TestBuildDatasetUri:
    """build_dataset_uri confines every routing component to a real path segment."""

    def test_rejects_empty_component(self) -> None:
        """An empty routing component is rejected."""
        with pytest.raises(ValueError, match="invalid routing component"):
            build_dataset_uri("/data", ["org1", "", "ns1"])

    @pytest.mark.parametrize("traversal", [".", ".."])
    def test_rejects_traversal_component(self, traversal: str) -> None:
        """A ``.`` or ``..`` routing component is rejected instead of escaping the base_uri prefix."""
        with pytest.raises(ValueError, match="invalid routing component"):
            build_dataset_uri("/data", ["org1", traversal, "ns1"])

    def test_accepts_normal_components(self) -> None:
        """Ordinary routing components build the expected dataset URI."""
        assert build_dataset_uri("/data", ["org1", "tenant1", "ns1"]) == "/data/org1/tenant1/ns1.lance"


class TestPathHelpers:
    """Path parsing and target-URI swapping operate on the namespace component."""

    def test_target_uri_swaps_only_namespace(self, telemetry_config: TelemetryConfig) -> None:
        """Only the namespace component changes; org and tenant are preserved."""
        config: MigrateConfig = base_config("/data", telemetry_config)
        source: str = "/data/org1/tenant1/nsA.lance"
        assert target_uri_for(config, source) == "/data/org1/tenant1/nsB.lance"

    def test_source_discovery_filters_by_namespace(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """Only datasets whose namespace component matches are discovered."""
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=10)
        write_source_dataset(base, ["org2", "tenant1", "nsA"], rows=10)
        write_source_dataset(base, ["org1", "tenant1", "nsOther"], rows=10)
        config: MigrateConfig = base_config(base, telemetry_config)
        found: list[str] = source_dataset_uris(config)
        assert sorted(found) == sorted([f"{base}/org1/tenant1/nsA.lance", f"{base}/org2/tenant1/nsA.lance"])


class TestEndToEnd:
    """A real migration copies data, keeps the source, and optimizes the targets."""

    def test_copy_keeps_source_and_matches_rows(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Targets exist with the same rows and the source datasets are left intact."""
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=30)
        write_source_dataset(base, ["org2", "tenant1", "nsA"], rows=20)
        config: MigrateConfig = base_config(base, telemetry_config)

        report = NamespaceMigrator(config).run(spark)

        assert report.datasets_found == 2
        assert len(report.copied) == 2
        for org, rows in (("org1", 30), ("org2", 20)):
            source_uri: str = f"{base}/{org}/tenant1/nsA.lance"
            target_uri: str = f"{base}/{org}/tenant1/nsB.lance"
            assert Path(source_uri).exists()
            source_ids: list[int] = sorted(lance.dataset(source_uri).to_table()["id"].to_pylist())
            target_ids: list[int] = sorted(lance.dataset(target_uri).to_table()["id"].to_pylist())
            assert source_ids == target_ids == list(range(rows))

    def test_existing_target_blocks_without_overwrite(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An existing target fails the run unless overwrite_target is set, then succeeds with it."""
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=15)
        write_source_dataset(base, ["org1", "tenant1", "nsB"], rows=99)
        config: MigrateConfig = base_config(base, telemetry_config)

        with pytest.raises(ValueError, match="already exist"):
            NamespaceMigrator(config).run(spark)

        overwrite: MigrateConfig = base_config(base, telemetry_config, overwrite_target=True)
        NamespaceMigrator(overwrite).run(spark)
        target_ids: list[int] = sorted(lance.dataset(f"{base}/org1/tenant1/nsB.lance").to_table()["id"].to_pylist())
        assert target_ids == list(range(15))

    def test_large_tier_distributed_copy_matches_rows(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A multi-fragment dataset copied through the distributed large tier keeps all rows."""
        monkeypatch.setattr(migrate_namespace_module, "LARGE_DATASET_FRAGMENT_THRESHOLD", 1)
        monkeypatch.setattr(migrate_namespace_module, "NUM_SHARDS", 3)
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=40, max_rows_per_file=8)
        config: MigrateConfig = base_config(base, telemetry_config)

        NamespaceMigrator(config).run(spark)

        target_ids: list[int] = sorted(lance.dataset(f"{base}/org1/tenant1/nsB.lance").to_table()["id"].to_pylist())
        assert target_ids == list(range(40))

    def test_recompact_reduces_fragments(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With recompact on, a target copied through the large tier as many fragments ends compacted to one."""
        monkeypatch.setattr(migrate_namespace_module, "LARGE_DATASET_FRAGMENT_THRESHOLD", 1)
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=40, max_rows_per_file=5)
        config: MigrateConfig = base_config(base, telemetry_config, recompact=True)

        report = NamespaceMigrator(config).run(spark)

        assert report.compacted == 1
        target = lance.dataset(f"{base}/org1/tenant1/nsB.lance")
        assert len(target.get_fragments()) == 1

    def test_reindex_builds_configured_index(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """With reindex on and a bitmap column configured, the target carries that index."""
        base: str = str(tmp_path)
        write_source_dataset(base, ["org1", "tenant1", "nsA"], rows=60)
        index: IndexJobConfig = IndexJobConfig(telemetry=telemetry_config, bitmap_columns=["category"])
        config: MigrateConfig = base_config(base, telemetry_config, reindex=True, index=index)

        report = NamespaceMigrator(config).run(spark)

        assert report.indexed == 1
        target = lance.dataset(f"{base}/org1/tenant1/nsB.lance")
        names: set[str] = {description.name for description in target.describe_indices()}
        assert bitmap_index_name("category") in names

    def test_optimize_excludes_isolated_failures_from_compacted_and_indexed_counts(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A target that fails to open during optimize is counted as failed, not compacted/indexed.

        Regression guard for PR-02: `NamespaceMigrator.optimize` previously used `len(results)` for
        both the compacted and indexed counts, so an isolated per-dataset failure (an unopenable
        target) was reported as a success. `report.failed` must surface it instead.
        """
        base: str = str(tmp_path)
        index: IndexJobConfig = IndexJobConfig(telemetry=telemetry_config, bitmap_columns=["category"])
        config: MigrateConfig = base_config(base, telemetry_config, recompact=True, reindex=True, index=index)
        missing_target: str = f"{base}/org1/tenant1/does_not_exist.lance"

        migrator: NamespaceMigrator = NamespaceMigrator(config)
        telemetry: Telemetry = Telemetry.create(telemetry_config)
        compacted, indexed, failed = migrator.optimize(spark, [missing_target], telemetry)

        assert (compacted, indexed, failed) == (0, 0, 2)


def fake_build_spark(*args: object, **kwargs: object) -> FakeSpark:
    """Return a fake session regardless of the requested Spark configuration.

    Args:
        args: Ignored positional arguments.
        kwargs: Ignored keyword arguments.

    Returns:
        A fresh fake Spark session.
    """
    del args, kwargs
    return FakeSpark()


class TestToolsCliExitCode:
    """The uninstalled operator tools CLI maps a migrate-namespace report's failed count to exit 3."""

    def test_run_migrate_namespace_returns_report_failed_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run_migrate_namespace` returns `report.failed` so `run_cli_main` maps it to exit 3.

        Regression guard for PR-02: the tools CLI previously returned `None` unconditionally,
        so `run_cli_main` always mapped a migrate-namespace run to exit 0 even when targets failed
        to optimize. Stubs `NamespaceMigrator.run` so this test does not need a real Spark session
        or dataset fleet, isolating the exit-code wiring from the migration logic already covered
        above.
        """
        stub_report: MigrateReport = MigrateReport(
            source_namespace="old-ns", target_namespace="new-ns", datasets_found=1, compacted=0, indexed=0, failed=2
        )

        def fake_run(migrator: NamespaceMigrator, spark: object) -> MigrateReport:
            """Return the fixed stub report regardless of the migrator or session.

            Args:
                migrator: Ignored migrator instance.
                spark: Ignored Spark session.

            Returns:
                The fixed stub report.
            """
            del migrator, spark
            return stub_report

        monkeypatch.setattr(tools_cli, "build_spark", fake_build_spark)
        monkeypatch.setattr(tools_cli.NamespaceMigrator, "run", fake_run)
        exit_code: int = tools_cli.main(
            [
                "migrate-namespace",
                "--source-namespace",
                "old-ns",
                "--target-namespace",
                "new-ns",
                "--base-uri",
                str(tmp_path),
            ]
        )
        assert exit_code == EXIT_PARTIAL_FAILURE
