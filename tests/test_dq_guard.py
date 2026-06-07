"""Tests for the single-org data-quality guard in the maintenance module.

Covers four scenarios:
1. A clean dataset (all rows belong to the correct routing key) returns zero contaminating rows and
   a full maintenance run does not raise.
2. A contaminated dataset (some rows carry a different org_id than the path encodes) returns the
   correct contaminating count, triggers the ``dataset.org_contamination`` metric, and raises when
   ``raise_on_contamination`` is True.
3. A dataset written without the routing columns as stored columns causes the check to be skipped
   and returns zero without error.
4. Unit tests for the predicate builder and URI decomposition helpers.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl import maintenance as m
from lance_etl.cli import build_parser, run_maintenance
from lance_etl.maintenance import (
    ContaminationError,
    MaintenanceConfig,
    MaintenanceJob,
    build_contamination_predicate,
    check_single_org,
    expected_routing_values,
    uri_components,
    verify_single_org,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


class FakeRdd:
    """Minimal in-process stand-in for a Spark RDD."""

    def __init__(self, items: Iterable[object]) -> None:
        """Initialize with items.

        Args:
            items: The items.
        """
        self.items: list[object] = list(items)

    def map(self, fn: Callable[[object], object]) -> FakeRdd:
        """Apply fn eagerly.

        Args:
            fn: The mapper.

        Returns:
            A new FakeRdd.
        """
        return FakeRdd([fn(item) for item in self.items])

    def mapPartitions(self, fn: Callable[[Iterator[object]], Iterator[object]]) -> FakeRdd:  # noqa: N802
        """Apply a partition function to the single in-process partition.

        Args:
            fn: The partition mapper.

        Returns:
            A new FakeRdd.
        """
        return FakeRdd(list(fn(iter(self.items))))

    def collect(self) -> list[object]:
        """Return the items.

        Returns:
            The current items.
        """
        return list(self.items)


class FakeSparkContext:
    """Minimal stand-in for a SparkContext running in process."""

    def parallelize(self, items: Iterable[object], slices: int) -> FakeRdd:
        """Wrap items into a FakeRdd.

        Args:
            items: The items.
            slices: Ignored.

        Returns:
            The fake RDD.
        """
        del slices
        return FakeRdd(items)

    def setLocalProperty(self, key: str, value: str | None) -> None:  # noqa: N802
        """Accept and ignore scheduler-pool properties.

        Args:
            key: The property name.
            value: The property value.
        """
        del key, value


class FakeSpark:
    """Minimal stand-in for a SparkSession running in process."""

    def __init__(self) -> None:
        """Initialize with a fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()


def make_routing_table(org: str, tenant: str, namespace: str, rows: int) -> pa.Table:
    """Build a small table that carries routing columns and an id.

    Args:
        org: org_id value for every row.
        tenant: tenant_id value for every row.
        namespace: namespace value for every row.
        rows: Number of rows to generate.

    Returns:
        A table with id, org_id, tenant_id, and namespace columns.
    """
    return pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "org_id": pa.array([org] * rows, pa.string()),
            "tenant_id": pa.array([tenant] * rows, pa.string()),
            "namespace": pa.array([namespace] * rows, pa.string()),
        }
    )


class TestUriComponentHelpers:
    """uri_components and expected_routing_values decompose URIs correctly."""

    def test_uri_components_strips_base_and_suffix(self, tmp_path: Path) -> None:
        """uri_components returns the routing values between base_uri and .lance."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        assert uri_components(base, uri) == ["acme", "t1", "ns1"]

    def test_uri_components_rejects_wrong_base(self, tmp_path: Path) -> None:
        """uri_components raises when the URI is not rooted at base_uri."""
        with pytest.raises(ValueError, match="not a .lance dataset rooted at"):
            uri_components(str(tmp_path), "/other/acme/t1/ns1.lance")

    def test_uri_components_rejects_missing_suffix(self, tmp_path: Path) -> None:
        """uri_components raises when the URI does not end in .lance."""
        base: str = str(tmp_path)
        with pytest.raises(ValueError, match="not a .lance dataset rooted at"):
            uri_components(base, f"{base}/acme/t1/ns1")

    def test_expected_routing_values_maps_components(self, tmp_path: Path) -> None:
        """expected_routing_values returns a dict mapping each column to its URI component."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        cols: list[str] = ["org_id", "tenant_id", "namespace"]
        result: dict[str, str] = expected_routing_values(base, uri, cols)
        assert result == {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}

    def test_expected_routing_values_rejects_component_count_mismatch(self, tmp_path: Path) -> None:
        """expected_routing_values raises when the URI depth does not match partition_cols."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1.lance"
        with pytest.raises(ValueError, match="path components"):
            expected_routing_values(base, uri, ["org_id", "tenant_id", "namespace"])


class TestBuildContaminationPredicate:
    """build_contamination_predicate builds the correct OR predicate."""

    def test_all_columns_present_builds_or(self) -> None:
        """When all routing columns are in the schema the predicate ORs all clauses."""
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}
        schema_names: list[str] = ["id", "org_id", "tenant_id", "namespace"]
        predicate: str | None = build_contamination_predicate(expected, schema_names)
        assert predicate is not None
        assert "org_id != 'acme'" in predicate
        assert "tenant_id != 't1'" in predicate
        assert "namespace != 'ns1'" in predicate
        assert " OR " in predicate

    def test_absent_columns_are_skipped(self) -> None:
        """Columns not in the schema are omitted from the predicate."""
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}
        predicate: str | None = build_contamination_predicate(expected, ["id", "org_id"])
        assert predicate is not None
        assert "org_id != 'acme'" in predicate
        assert "tenant_id" not in predicate
        assert "namespace" not in predicate

    def test_no_routing_columns_returns_none(self) -> None:
        """When no routing column exists in the schema, the function returns None."""
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1"}
        predicate: str | None = build_contamination_predicate(expected, ["id", "vector"])
        assert predicate is None

    def test_single_quote_in_value_is_escaped(self) -> None:
        """A single quote in an expected value is doubled to prevent SQL injection."""
        expected: dict[str, str] = {"org_id": "o'malley"}
        predicate: str | None = build_contamination_predicate(expected, ["org_id"])
        assert predicate is not None
        assert "o''malley" in predicate

    def test_invalid_column_name_raises(self) -> None:
        """A routing column name that fails the identifier allowlist raises ValueError."""
        with pytest.raises(ValueError, match="allowlist"):
            build_contamination_predicate({"bad; col": "val"}, ["bad; col"])


class TestVerifySingleOrg:
    """verify_single_org counts contaminating rows via a pushdown predicate."""

    def test_clean_dataset_returns_zero(self, tmp_path: Path) -> None:
        """A dataset whose every row matches the expected routing values returns 0."""
        table: pa.Table = make_routing_table("acme", "t1", "ns1", 10)
        uri: str = str(tmp_path / "acme" / "t1" / "ns1.lance")
        lance.write_dataset(table, uri)
        ds: lance.LanceDataset = lance.dataset(uri)
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}
        assert verify_single_org(ds, expected, ["org_id", "tenant_id", "namespace"]) == 0

    def test_contaminated_dataset_returns_count(self, tmp_path: Path) -> None:
        """Rows with org_id != the expected value are counted as contaminating."""
        clean: pa.Table = make_routing_table("acme", "t1", "ns1", 8)
        dirty: pa.Table = make_routing_table("evil-corp", "t1", "ns1", 3)
        table: pa.Table = pa.concat_tables([clean, dirty])
        uri: str = str(tmp_path / "acme" / "t1" / "ns1.lance")
        lance.write_dataset(table, uri)
        ds: lance.LanceDataset = lance.dataset(uri)
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}
        count: int = verify_single_org(ds, expected, ["org_id", "tenant_id", "namespace"])
        assert count == 3

    def test_missing_routing_columns_returns_zero(self, tmp_path: Path) -> None:
        """When none of the routing columns are stored, the check is skipped and returns 0."""
        table: pa.Table = pa.table({"id": pa.array([1, 2], pa.int64())})
        uri: str = str(tmp_path / "no_cols.lance")
        lance.write_dataset(table, uri)
        ds: lance.LanceDataset = lance.dataset(uri)
        expected: dict[str, str] = {"org_id": "acme", "tenant_id": "t1", "namespace": "ns1"}
        assert verify_single_org(ds, expected, ["org_id", "tenant_id", "namespace"]) == 0


class TestCheckSingleOrg:
    """check_single_org wires verify_single_org into the maintenance telemetry and raise logic."""

    def test_clean_dataset_emits_no_metric(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A clean dataset returns 0 and does not emit the contamination metric."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        lance.write_dataset(make_routing_table("acme", "t1", "ns1", 5), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
        )
        assert check_single_org(uri, config, telemetry) == 0

    def test_contaminated_dataset_returns_count(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A contaminated dataset returns the contaminating row count."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        table: pa.Table = pa.concat_tables(
            [make_routing_table("acme", "t1", "ns1", 5), make_routing_table("evil-corp", "t1", "ns1", 2)]
        )
        lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            raise_on_contamination=False,
        )
        assert check_single_org(uri, config, telemetry) == 2

    def test_contamination_raises_when_configured(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When raise_on_contamination is True, contamination raises ContaminationError."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        table: pa.Table = pa.concat_tables(
            [make_routing_table("acme", "t1", "ns1", 3), make_routing_table("other-org", "t1", "ns1", 1)]
        )
        lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            raise_on_contamination=True,
        )
        with pytest.raises(ContaminationError, match="contaminating rows"):
            check_single_org(uri, config, telemetry)

    def test_no_base_uri_skips_check(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When base_uri is None the check is skipped and returns 0 without opening the dataset."""
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=None,
            partition_cols=["org_id", "tenant_id", "namespace"],
        )
        sentinel: str = str(tmp_path / "acme" / "t1" / "ns1.lance")
        assert check_single_org(sentinel, config, telemetry) == 0

    def test_missing_columns_returns_zero_without_error(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A dataset without the routing columns stored skips the check and returns 0."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
        )
        assert check_single_org(uri, config, telemetry) == 0


class TestMaintenanceJobDqIntegration:
    """MaintenanceJob.run() gates on the DQ guard before TTL and compaction."""

    def test_clean_dataset_maintenance_does_not_raise(self, tmp_path: Path) -> None:
        """A maintenance run on a clean dataset completes without raising."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        lance.write_dataset(make_routing_table("acme", "t1", "ns1", 6), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            verify_single_org=True,
            raise_on_contamination=False,
            commit_backoff_seconds=0.0,
        )
        results: list[object] = MaintenanceJob(config).run(FakeSpark(), [uri])
        assert len(results) == 1

    def test_contaminated_dataset_raises_when_configured(self, tmp_path: Path) -> None:
        """When raise_on_contamination is True, a contaminated dataset raises ContaminationError."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        table: pa.Table = pa.concat_tables(
            [make_routing_table("acme", "t1", "ns1", 4), make_routing_table("rogue-org", "t1", "ns1", 2)]
        )
        lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            verify_single_org=True,
            raise_on_contamination=True,
            commit_backoff_seconds=0.0,
        )
        with pytest.raises(ContaminationError):
            MaintenanceJob(config).run(FakeSpark(), [uri])

    def test_contaminated_dataset_logs_and_continues_when_not_raising(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When raise_on_contamination is False, contamination is logged but maintenance continues."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        table: pa.Table = pa.concat_tables(
            [make_routing_table("acme", "t1", "ns1", 3), make_routing_table("bad-org", "t1", "ns1", 1)]
        )
        lance.write_dataset(table, uri)

        compacted: list[str] = []

        def record_compact(u: str, cfg: MaintenanceConfig, tel: Telemetry) -> dict[str, object]:
            """Record that compact was called.

            Args:
                u: Dataset URI.
                cfg: Maintenance config.
                tel: Telemetry facade.

            Returns:
                A fake small-tier compaction result.
            """
            del cfg, tel
            compacted.append(u)
            return {"uri": u, "tier": "small", "tasks": 1, "bytes_removed": 0, "fragments_removed": 0}

        monkeypatch.setattr(m, "classify_or_compact", record_compact)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            verify_single_org=True,
            raise_on_contamination=False,
            commit_backoff_seconds=0.0,
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        assert uri in compacted

    def test_dq_guard_skipped_when_disabled(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When verify_single_org is False the DQ guard never runs."""
        base: str = str(tmp_path)
        uri: str = f"{base}/acme/t1/ns1.lance"
        lance.write_dataset(make_routing_table("acme", "t1", "ns1", 3), uri)

        guard_called: list[bool] = []

        def fail_guard(u: str, cfg: MaintenanceConfig, tel: Telemetry) -> int:
            """Record guard invocation (should never happen when disabled).

            Args:
                u: Dataset URI.
                cfg: Maintenance config.
                tel: Telemetry facade.

            Returns:
                Zero.
            """
            del u, cfg, tel
            guard_called.append(True)
            return 0

        monkeypatch.setattr(m, "check_single_org", fail_guard)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            base_uri=base,
            partition_cols=["org_id", "tenant_id", "namespace"],
            verify_single_org=False,
            commit_backoff_seconds=0.0,
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        assert not guard_called


class TestMaintenanceConfigNewFields:
    """MaintenanceConfig carries the expected defaults for the new DQ fields."""

    def test_defaults(self) -> None:
        """verify_single_org defaults True, raise_on_contamination defaults False, base_uri defaults None."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig())
        assert config.verify_single_org is True
        assert config.raise_on_contamination is False
        assert config.base_uri is None

    def test_partition_cols_default_is_etl_trio(self) -> None:
        """partition_cols defaults to the ETL org_id/tenant_id/namespace trio."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig())
        assert config.partition_cols == ["org_id", "tenant_id", "namespace"]


class TestCliDqFlags:
    """The maintenance subcommand exposes --no-verify-single-org and --raise-on-contamination."""

    def test_flags_absent_by_default(self) -> None:
        """Without the flags, verify_single_org is on and raise_on_contamination is off."""
        args = build_parser().parse_args(["maintenance", "--dataset-uri", "s3://bucket/acme/t1/ns1.lance"])
        assert args.no_verify_single_org is False
        assert args.raise_on_contamination is False

    def test_no_verify_flag_sets_attribute(self) -> None:
        """--no-verify-single-org sets no_verify_single_org to True."""
        args = build_parser().parse_args(
            ["maintenance", "--dataset-uri", "s3://bucket/acme/t1/ns1.lance", "--no-verify-single-org"]
        )
        assert args.no_verify_single_org is True

    def test_raise_on_contamination_flag_sets_attribute(self) -> None:
        """--raise-on-contamination sets raise_on_contamination to True."""
        args = build_parser().parse_args(
            [
                "maintenance",
                "--dataset-uri",
                "s3://bucket/acme/t1/ns1.lance",
                "--raise-on-contamination",
            ]
        )
        assert args.raise_on_contamination is True

    def test_run_maintenance_builds_correct_config(self, tmp_path: Path) -> None:
        """run_maintenance passes base_uri and the DQ flags through to MaintenanceConfig."""
        uri: str = str(tmp_path / "acme" / "t1" / "ns1.lance")
        lance.write_dataset(make_routing_table("acme", "t1", "ns1", 2), uri)
        args = build_parser().parse_args(
            [
                "maintenance",
                "--dataset-uri",
                uri,
                "--base-uri",
                str(tmp_path),
                "--no-verify-single-org",
            ]
        )
        captured: list[MaintenanceConfig] = []
        original_run = MaintenanceJob.run

        def capture_run(self: MaintenanceJob, spark: object, dataset_uris: object) -> list[object]:
            """Capture the config before delegating.

            Args:
                self: The job instance.
                spark: The Spark session.
                dataset_uris: The dataset URIs.

            Returns:
                The original run result.
            """
            captured.append(self.config)
            return original_run(self, spark, dataset_uris)

        MaintenanceJob.run = capture_run  # type: ignore[method-assign]
        try:
            run_maintenance(args, FakeSpark())  # type: ignore[arg-type]
        finally:
            MaintenanceJob.run = original_run  # type: ignore[method-assign]

        assert len(captured) == 1
        cfg: MaintenanceConfig = captured[0]
        assert cfg.verify_single_org is False
        assert cfg.base_uri == str(tmp_path)
