"""Tests for the maintenance job: per-row TTL expiration, compaction, and version cleanup.

Covers the per-row TTL predicate (timestamp plus a Duration lifetime column), validation safety, the default-off
no-op, an end-to-end run that deletes only expired rows and then compacts, and the delete-before-compact ordering of
the run. The functional TTL tests use a tiny real Lance dataset with a ``Duration`` TTL column so the delete path
exercises actual Lance timestamp-plus-duration arithmetic. Ordering is pinned with an in-process fake Spark so the
fan-out callables run in the driver process and can be observed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark

import lance_etl.maintenance.job as maintenance_job
from lance_etl.maintenance import (
    MaintenanceConfig,
    MaintenanceJob,
    build_ttl_predicate,
    compute_cutoff,
    run_ttl_on_open_dataset,
    validate_column_name,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

TS_COLUMN: str = "ts"
TTL_COLUMN: str = "ttl"


def make_ttl_table(rows: int, ts_base: datetime, step: timedelta, lifetime: timedelta) -> pa.Table:
    """Build a table with id, an event timestamp column, and a per-row Duration TTL column.

    Args:
        rows: Number of rows.
        ts_base: Timestamp of the first row.
        step: Time between consecutive rows.
        lifetime: The per-row TTL lifetime stored in the Duration column.

    Returns:
        A table with ``id`` (int64), ``ts`` (timestamp[us, UTC]), and ``ttl`` (duration[us]) columns.
    """
    ids: pa.Array = pa.array(range(rows), pa.int64())
    timestamps: pa.Array = pa.array([ts_base + step * i for i in range(rows)], pa.timestamp("us", tz="UTC"))
    lifetimes: pa.Array = pa.array([lifetime for _ in range(rows)], pa.duration("us"))
    return pa.table({"id": ids, TS_COLUMN: timestamps, TTL_COLUMN: lifetimes})


@pytest.fixture
def ttl_dataset(tmp_path: Path) -> tuple[str, int, int]:
    """Write a fragmented Lance dataset with expired and fresh rows by per-row TTL.

    Six rows have an event timestamp 100 days ago with a 1-day lifetime (expired), and four rows have an event
    timestamp 1 day ago with a 100-day lifetime (still alive). The dataset is split into several fragments so
    compaction has work to do.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        ``(uri, expired_count, alive_count)``.
    """
    now: datetime = datetime.now(tz=UTC)
    expired: pa.Table = make_ttl_table(6, now - timedelta(days=100), timedelta(hours=1), timedelta(days=1))
    alive: pa.Table = make_ttl_table(4, now - timedelta(days=1), timedelta(hours=1), timedelta(days=100))
    uri: str = str(tmp_path / "ttl.lance")
    lance.write_dataset(pa.concat_tables([expired, alive]), uri, max_rows_per_file=2)
    return uri, 6, 4


class TestMaintenanceConfigDefaults:
    """MaintenanceConfig carries the expected TTL defaults."""

    def test_ttl_off_by_default(self, telemetry_config: TelemetryConfig) -> None:
        """ttl_column defaults to None so TTL is off."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
        assert config.ttl_column is None
        assert config.ttl_active() is False

    def test_ttl_active_when_column_set(self, telemetry_config: TelemetryConfig) -> None:
        """Naming a ttl_column turns TTL on."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, ttl_column="ttl")
        assert config.ttl_active() is True

    def test_ts_column_default(self, telemetry_config: TelemetryConfig) -> None:
        """The default event timestamp column matches ETLConfig.ts_col."""
        assert MaintenanceConfig(telemetry=telemetry_config).ts_column == "event_timestamp"


class TestPredicateSafety:
    """The per-row TTL delete predicate and its column validation are safe."""

    def test_build_predicate_format(self) -> None:
        """build_ttl_predicate renders timestamp-plus-duration arithmetic against a typed literal."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 30, 45, 123456, tzinfo=UTC)
        predicate: str = build_ttl_predicate("ts", "ttl", cutoff)
        assert predicate == "ts + ttl < TIMESTAMP '2025-03-15T12:30:45.123456'"

    def test_build_predicate_converts_to_utc(self) -> None:
        """build_ttl_predicate converts a non-UTC cutoff to UTC."""
        eastern: datetime = datetime(2025, 3, 15, 8, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
        predicate: str = build_ttl_predicate("event_time", "lifetime", eastern)
        assert "2025-03-15T13:00:00.000000" in predicate

    def test_validate_rejects_injection(self, tmp_path: Path) -> None:
        """validate_column_name raises KeyError for names not present in the schema, including injection attempts."""
        uri: str = str(tmp_path / "safe.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        with pytest.raises(KeyError, match="not present"):
            validate_column_name("ts; DROP TABLE", ds.schema)

    def test_validate_rejects_unknown(self, tmp_path: Path) -> None:
        """validate_column_name raises KeyError when the column is absent from the schema."""
        uri: str = str(tmp_path / "nots.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        with pytest.raises(KeyError, match="not present"):
            validate_column_name("ts", ds.schema)

    def test_validate_accepts_valid(self, tmp_path: Path) -> None:
        """A column passing the allowlist and present in the schema does not raise."""
        uri: str = str(tmp_path / "valid.lance")
        ds: lance.LanceDataset = lance.write_dataset(
            pa.table({"id": pa.array([1], pa.int64()), "ts": pa.array([datetime.now(tz=UTC)], pa.timestamp("us"))}),
            uri,
        )
        validate_column_name("ts", ds.schema)


class TestPerRowTtlDelete:
    """run_ttl_on_open_dataset removes only rows whose lifetime has elapsed."""

    def test_deletes_only_expired_rows(self, ttl_dataset: tuple[str, int, int], telemetry: Telemetry) -> None:
        """Rows whose event timestamp plus per-row lifetime is before now are deleted; the rest survive."""
        uri, expired_count, alive_count = ttl_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), ttl_column=TTL_COLUMN, ts_column=TS_COLUMN, commit_backoff_seconds=0.0
        )
        dataset: lance.LanceDataset = lance.dataset(uri)
        result: dict[str, object] = run_ttl_on_open_dataset(dataset, uri, config, compute_cutoff(), telemetry)
        assert result["ttl_rows_deleted"] == expired_count
        assert result["skipped"] == ""
        assert lance.dataset(uri).count_rows() == alive_count

    def test_keeps_rows_with_long_lifetime(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When every row's lifetime outlasts its age, nothing is deleted."""
        now: datetime = datetime.now(tz=UTC)
        table: pa.Table = make_ttl_table(8, now - timedelta(days=5), timedelta(hours=1), timedelta(days=365))
        uri: str = str(tmp_path / "alive.lance")
        dataset: lance.LanceDataset = lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), ttl_column=TTL_COLUMN, ts_column=TS_COLUMN, commit_backoff_seconds=0.0
        )
        result: dict[str, object] = run_ttl_on_open_dataset(dataset, uri, config, compute_cutoff(), telemetry)
        assert result["ttl_rows_deleted"] == 0
        assert lance.dataset(uri).count_rows() == 8

    def test_skips_dataset_without_ttl_column(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A dataset lacking the TTL column is skipped rather than failing."""
        uri: str = str(tmp_path / "no_ttl.lance")
        dataset: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1, 2], pa.int64())}), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), ttl_column=TTL_COLUMN, ts_column=TS_COLUMN, commit_backoff_seconds=0.0
        )
        result: dict[str, object] = run_ttl_on_open_dataset(dataset, uri, config, compute_cutoff(), telemetry)
        assert result["ttl_rows_deleted"] == 0
        assert result["skipped"] != ""


class TestTtlOffIsNoop:
    """With no TTL column configured the TTL step never runs."""

    def test_run_with_ttl_off_deletes_nothing(self, ttl_dataset: tuple[str, int, int]) -> None:
        """A maintenance run without ttl_column compacts but deletes no rows."""
        uri, _, _ = ttl_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), target_rows_per_fragment=1000, commit_backoff_seconds=0.0
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        assert lance.dataset(uri).count_rows() == 10

    def test_run_with_ttl_off_never_calls_delete(
        self, ttl_dataset: tuple[str, int, int], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The TTL pass is skipped entirely when ttl_column is None."""
        uri, _, _ = ttl_dataset

        def fail_delete(*args: object, **kwargs: object) -> dict[str, object]:
            """Fail if the TTL delete is ever invoked with TTL off."""
            del args, kwargs
            raise AssertionError("run_ttl_on_open_dataset must not run when ttl_column is None")

        monkeypatch.setattr(maintenance_job, "run_ttl_on_open_dataset", fail_delete)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), target_rows_per_fragment=1000, commit_backoff_seconds=0.0
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])


class TestRunOrdering:
    """A maintenance run uses a consolidated per-dataset pass for DQ, TTL, and compaction."""

    def test_plan_fan_out_covers_all_datasets(
        self, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The plan fan-out calls plan_one_dataset for every dataset in the fleet.

        With the unified design, plan_one_dataset handles TTL, the skip check, and the
        compaction plan in one executor task per dataset. Patching plan_one_dataset at the
        module level lets the test observe that every URI is planned exactly once.

        Args:
            telemetry_config: The test telemetry configuration.
            monkeypatch: Pytest monkeypatch fixture.
        """
        processed: list[str] = []

        def record_plan(
            uri: str, config: MaintenanceConfig, cutoff: datetime | None, tel: Telemetry
        ) -> dict[str, object]:
            """Record that plan_one_dataset was called for this URI."""
            del config, cutoff, tel
            processed.append(uri)
            return {"uri": uri, "tasks": 0, "bytes_removed": 0, "fragments_removed": 0}

        monkeypatch.setattr(maintenance_job, "plan_one_dataset", record_plan)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, ttl_column="ttl")
        MaintenanceJob(config).run(FakeSpark(), ["a.lance", "b.lance"])
        assert processed == ["a.lance", "b.lance"]

    def test_run_expires_then_compacts_real_dataset(self, ttl_dataset: tuple[str, int, int]) -> None:
        """An end-to-end run deletes expired rows and compacts the survivors into one fragment."""
        uri, _, alive_count = ttl_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            ttl_column=TTL_COLUMN,
            ts_column=TS_COLUMN,
            target_rows_per_fragment=1000,
            commit_backoff_seconds=0.0,
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        dataset: lance.LanceDataset = lance.dataset(uri)
        assert dataset.count_rows() == alive_count
        assert len(dataset.get_fragments()) == 1
        assert dataset.get_fragments()[0].metadata.deletion_file is None
