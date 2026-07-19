"""Tests for the maintenance job: retention-window expiry, compaction, and version cleanup.

Covers the retention predicate (a ``ts`` column compared against a ``now - retention_seconds`` cutoff), validation
safety, the default-off no-op, an end-to-end run that deletes only expired rows and then compacts, and the
delete-before-compact ordering of the run. The functional retention tests use a tiny real Lance dataset with a ``ts``
timestamp column so the delete path exercises actual Lance timestamp arithmetic. Ordering is pinned with an in-process
fake Spark so the fan-out callables run in the driver process and can be observed.
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
    build_retention_predicate,
    compute_cutoff,
    run_retention_on_open_dataset,
    validate_column_name,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

TS_COLUMN: str = "ts"
RETENTION_SECONDS: int = 10 * 24 * 3600


def make_retention_table(rows: int, ts_base: datetime, step: timedelta) -> pa.Table:
    """Build a table with an id column and a ``ts`` timestamp column.

    Args:
        rows: Number of rows.
        ts_base: Timestamp of the first row.
        step: Time between consecutive rows.

    Returns:
        A table with ``id`` (int64) and ``ts`` (timestamp[us, UTC]) columns.
    """
    ids: pa.Array = pa.array(range(rows), pa.int64())
    timestamps: pa.Array = pa.array([ts_base + step * i for i in range(rows)], pa.timestamp("us", tz="UTC"))
    return pa.table({"id": ids, TS_COLUMN: timestamps})


@pytest.fixture
def retention_dataset(tmp_path: Path) -> tuple[str, int, int]:
    """Write a fragmented Lance dataset with expired and fresh rows by ``ts`` age.

    Six rows have a ``ts`` 100 days ago (older than the ten-day retention window, so expired), and four rows have a
    ``ts`` 1 day ago (inside the window, so still alive). The dataset is split into several fragments so compaction has
    work to do.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        ``(uri, expired_count, alive_count)``.
    """
    now: datetime = datetime.now(tz=UTC)
    expired: pa.Table = make_retention_table(6, now - timedelta(days=100), timedelta(hours=1))
    alive: pa.Table = make_retention_table(4, now - timedelta(days=1), timedelta(hours=1))
    uri: str = str(tmp_path / "retention.lance")
    lance.write_dataset(pa.concat_tables([expired, alive]), uri, max_rows_per_file=2)
    return uri, 6, 4


class TestMaintenanceConfigDefaults:
    """MaintenanceConfig carries the expected retention defaults."""

    def test_retention_off_by_default(self, telemetry_config: TelemetryConfig) -> None:
        """retention_seconds defaults to None so record expiry is off."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
        assert config.retention_seconds is None
        assert config.retention_active() is False

    def test_retention_active_when_window_set(self, telemetry_config: TelemetryConfig) -> None:
        """Setting a retention_seconds window turns record expiry on."""
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, retention_seconds=RETENTION_SECONDS)
        assert config.retention_active() is True

    def test_ts_column_default(self, telemetry_config: TelemetryConfig) -> None:
        """The default ts column matches ETLConfig.ts_col."""
        assert MaintenanceConfig(telemetry=telemetry_config).ts_column == "ts"


class TestPredicateSafety:
    """The retention delete predicate and its column validation are safe."""

    def test_build_predicate_format(self) -> None:
        """build_retention_predicate renders a ts comparison against a typed literal."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 30, 45, 123456, tzinfo=UTC)
        predicate: str = build_retention_predicate("ts", cutoff)
        assert predicate == (
            "arrow_cast(ts, 'Timestamp(Microsecond, \"UTC\")') < "
            "arrow_cast('2025-03-15T12:30:45.123456', 'Timestamp(Microsecond, \"UTC\")')"
        )

    def test_build_predicate_converts_to_utc(self) -> None:
        """build_retention_predicate converts a non-UTC cutoff to UTC."""
        eastern: datetime = datetime(2025, 3, 15, 8, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
        predicate: str = build_retention_predicate("event_time", eastern)
        assert "2025-03-15T13:00:00.000000" in predicate

    def test_build_predicate_is_explicitly_utc(self) -> None:
        """build_retention_predicate forces both comparison sides to an explicit UTC timestamp type.

        A bare ``TIMESTAMP '...'`` literal is always timezone-naive, so the predicate wraps BOTH the ts column and the
        cutoff literal in ``arrow_cast(..., 'Timestamp(Microsecond, "UTC")')``. The comparison is then a direct
        UTC-instant comparison with no naive operand and no reliance on implicit coercion with whatever timezone the
        column happens to carry.
        """
        cutoff: datetime = datetime(2025, 3, 15, 12, 30, 45, 123456, tzinfo=UTC)
        predicate: str = build_retention_predicate("ts", cutoff)
        assert "arrow_cast(ts, 'Timestamp(Microsecond, \"UTC\")')" in predicate
        assert "arrow_cast('2025-03-15T12:30:45.123456', 'Timestamp(Microsecond, \"UTC\")')" in predicate
        assert "TIMESTAMP '" not in predicate

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


class TestRetentionDelete:
    """run_retention_on_open_dataset removes only rows whose ts is outside the retention window."""

    def test_deletes_only_expired_rows(self, retention_dataset: tuple[str, int, int], telemetry: Telemetry) -> None:
        """Rows whose ts is before the retention cutoff are deleted; the rest survive."""
        uri, expired_count, alive_count = retention_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=RETENTION_SECONDS,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        dataset: lance.LanceDataset = lance.dataset(uri)
        result: dict[str, object] = run_retention_on_open_dataset(
            dataset, uri, config, compute_cutoff(RETENTION_SECONDS), telemetry
        )
        assert result["retention_rows_deleted"] == expired_count
        assert result["skipped"] == ""
        assert lance.dataset(uri).count_rows() == alive_count

    def test_keeps_rows_inside_window(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When every row's ts is inside the retention window, nothing is deleted."""
        now: datetime = datetime.now(tz=UTC)
        table: pa.Table = make_retention_table(8, now - timedelta(days=5), timedelta(hours=1))
        uri: str = str(tmp_path / "alive.lance")
        dataset: lance.LanceDataset = lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=365 * 24 * 3600,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        result: dict[str, object] = run_retention_on_open_dataset(
            dataset, uri, config, compute_cutoff(config.retention_seconds), telemetry
        )
        assert result["retention_rows_deleted"] == 0
        assert lance.dataset(uri).count_rows() == 8

    def test_skips_dataset_without_ts_column(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A dataset lacking the ts column is skipped rather than failing."""
        uri: str = str(tmp_path / "no_ts.lance")
        dataset: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1, 2], pa.int64())}), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=RETENTION_SECONDS,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        result: dict[str, object] = run_retention_on_open_dataset(
            dataset, uri, config, compute_cutoff(RETENTION_SECONDS), telemetry
        )
        assert result["retention_rows_deleted"] == 0
        assert result["skipped"] != ""


class TestRetentionOffIsNoop:
    """With no retention window configured the retention step never runs."""

    def test_run_with_retention_off_deletes_nothing(self, retention_dataset: tuple[str, int, int]) -> None:
        """A maintenance run without retention_seconds compacts but deletes no rows."""
        uri, _, _ = retention_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), target_rows_per_fragment=1000, commit_backoff_seconds=0.0
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        assert lance.dataset(uri).count_rows() == 10

    def test_run_with_retention_off_never_calls_delete(
        self, retention_dataset: tuple[str, int, int], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The retention pass is skipped entirely when retention_seconds is None."""
        uri, _, _ = retention_dataset

        def fail_delete(*args: object, **kwargs: object) -> dict[str, object]:
            """Fail if the retention delete is ever invoked with retention off."""
            del args, kwargs
            raise AssertionError("run_retention_on_open_dataset must not run when retention_seconds is None")

        monkeypatch.setattr(maintenance_job, "run_retention_on_open_dataset", fail_delete)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(), target_rows_per_fragment=1000, commit_backoff_seconds=0.0
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])


class TestRunOrdering:
    """A maintenance run uses a consolidated per-dataset pass for DQ, retention, and compaction."""

    def test_plan_fan_out_covers_all_datasets(
        self, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The plan fan-out calls plan_one_dataset for every dataset in the fleet.

        With the unified design, plan_one_dataset handles retention, the skip check, and the compaction plan in one
        executor task per dataset. Patching plan_one_dataset at the module level lets the test observe that every URI
        is planned exactly once.

        Args:
            telemetry_config: The test telemetry configuration.
            monkeypatch: Pytest monkeypatch fixture.
        """
        processed: list[str] = []

        def record_plan(
            uri: str,
            config: MaintenanceConfig,
            cutoff: datetime | None,
            tel: Telemetry,
            cleanup_slot: int | None = None,
        ) -> dict[str, object]:
            """Record that plan_one_dataset was called for this URI."""
            del config, cutoff, tel, cleanup_slot
            processed.append(uri)
            return {"uri": uri, "tasks": 0, "bytes_removed": 0, "fragments_removed": 0}

        monkeypatch.setattr(maintenance_job, "plan_one_dataset", record_plan)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, retention_seconds=RETENTION_SECONDS)
        MaintenanceJob(config).run(FakeSpark(), ["a.lance", "b.lance"])
        assert processed == ["a.lance", "b.lance"]

    def test_run_expires_then_compacts_real_dataset(self, retention_dataset: tuple[str, int, int]) -> None:
        """An end-to-end run deletes expired rows and compacts the survivors into one fragment."""
        uri, _, alive_count = retention_dataset
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=RETENTION_SECONDS,
            ts_column=TS_COLUMN,
            target_rows_per_fragment=1000,
            commit_backoff_seconds=0.0,
        )
        MaintenanceJob(config).run(FakeSpark(), [uri])
        dataset: lance.LanceDataset = lance.dataset(uri)
        assert dataset.count_rows() == alive_count
        assert len(dataset.get_fragments()) == 1
        assert dataset.get_fragments()[0].metadata.deletion_file is None


class TestPlanOpenCounts:
    """plan_one_dataset reuses one dataset handle instead of re-opening per step.

    At long-tail fleet scale every extra ``lance.dataset`` open multiplies into millions of object-store round trips
    per run, so these tests pin the exact open counts of the plan phase's paths by wrapping ``lance.dataset`` with a
    counting delegate.
    """

    def counting_dataset(self, monkeypatch: pytest.MonkeyPatch) -> list[str]:
        """Install a counting wrapper around ``lance.dataset`` and return its call log.

        Args:
            monkeypatch: Pytest monkeypatch fixture.

        Returns:
            A list receiving one URI entry per ``lance.dataset`` call made after installation.
        """
        opens: list[str] = []
        real_dataset = lance.dataset

        def counting(uri: str, *args: object, **kwargs: object) -> lance.LanceDataset:
            """Delegate to the real open while recording the call.

            Args:
                uri: Dataset URI being opened.
                *args: Positional arguments forwarded to the real open.
                **kwargs: Keyword arguments forwarded to the real open.

            Returns:
                The real dataset handle.
            """
            opens.append(uri)
            return real_dataset(uri, *args, **kwargs)

        monkeypatch.setattr(lance, "dataset", counting)
        return opens

    def test_idle_skip_path_opens_once(
        self, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A single-fragment dataset with retention off costs exactly one open on the skip path."""
        uri: str = str(tmp_path / "idle.lance")
        lance.write_dataset(pa.table({"id": pa.array([1, 2], pa.int64())}), uri)
        opens: list[str] = self.counting_dataset(monkeypatch)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
        result = maintenance_job.plan_one_dataset(uri, config, None, Telemetry.create(telemetry_config))
        assert result["skipped"].startswith("only 1 fragment")
        assert opens == [uri]

    def test_multi_fragment_plan_opens_once(
        self, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A compactable dataset with retention off plans its rewrite tasks from the single open."""
        uri: str = str(tmp_path / "busy.lance")
        lance.write_dataset(pa.table({"id": pa.array(range(10), pa.int64())}), uri, max_rows_per_file=2)
        opens: list[str] = self.counting_dataset(monkeypatch)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
        result = maintenance_job.plan_one_dataset(uri, config, None, Telemetry.create(telemetry_config))
        assert result["task_jsons"]
        assert opens == [uri]

    def test_retention_commit_refreshes_exactly_once(
        self,
        retention_dataset: tuple[str, int, int],
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A retention delete costs the initial open, the retried delete's own open, and one refresh."""
        uri, _, _ = retention_dataset
        opens: list[str] = self.counting_dataset(monkeypatch)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=telemetry_config,
            retention_seconds=RETENTION_SECONDS,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        result = maintenance_job.plan_one_dataset(
            uri, config, compute_cutoff(RETENTION_SECONDS), Telemetry.create(telemetry_config)
        )
        assert result["retention_rows_deleted"] == 6
        assert opens == [uri, uri, uri]

    def test_cleanup_with_handle_opens_nothing(
        self, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cleanup_dataset given an open handle performs zero additional opens."""
        uri: str = str(tmp_path / "clean.lance")
        lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        handle: lance.LanceDataset = lance.dataset(uri)
        opens: list[str] = self.counting_dataset(monkeypatch)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config)
        maintenance_job.cleanup_dataset(uri, config, Telemetry.create(telemetry_config), handle)
        assert opens == []
