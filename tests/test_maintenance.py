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

import lance_etl.maintenance.cli as maintenance_cli
import lance_etl.maintenance.job as maintenance_job
from lance_etl.cliutil import EXIT_PARTIAL_FAILURE
from lance_etl.fanout import count_failed
from lance_etl.maintenance import (
    MaintenanceConfig,
    MaintenanceJob,
    build_retention_predicate,
    compute_cutoff,
    retention_predicate,
    run_retention_on_open_dataset,
    validate_column_name,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


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


DELETED_COLUMN: str = "is_deleted"

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


class TestTombstoneAwarePredicate:
    """retention_predicate expires live rows and tombstones on separate clocks when configured."""

    def test_no_deleted_column_keeps_plain_predicate(self) -> None:
        """Without a deleted column the predicate is the exact plain ts comparison."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig(), retention_seconds=RETENTION_SECONDS)
        assert retention_predicate(config, cutoff) == build_retention_predicate("ts", cutoff)

    def test_unbounded_replay_horizon_never_expires_tombstones(self) -> None:
        """With no replay horizon the predicate matches only live rows so tombstones are retained."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=RETENTION_SECONDS,
            deleted_column=DELETED_COLUMN,
            replay_horizon_seconds=None,
        )
        predicate: str = retention_predicate(config, cutoff)
        assert predicate == f"(NOT {DELETED_COLUMN} AND {build_retention_predicate('ts', cutoff)})"

    def test_replay_horizon_beyond_retention_uses_older_tombstone_cutoff(self) -> None:
        """A tombstone clause compares ts against now minus the larger of the two windows."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        record_seconds: int = 10 * 24 * 3600
        replay_seconds: int = 30 * 24 * 3600
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=record_seconds,
            deleted_column=DELETED_COLUMN,
            replay_horizon_seconds=replay_seconds,
        )
        tombstone_cutoff: datetime = cutoff - timedelta(seconds=replay_seconds - record_seconds)
        expected: str = (
            f"((NOT {DELETED_COLUMN} AND {build_retention_predicate('ts', cutoff)}) OR "
            f"({DELETED_COLUMN} AND {build_retention_predicate('ts', tombstone_cutoff)}))"
        )
        assert retention_predicate(config, cutoff) == expected

    def test_replay_horizon_within_retention_matches_record_cutoff(self) -> None:
        """When the horizon is shorter than retention the tombstone clock equals the record clock."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=30 * 24 * 3600,
            deleted_column=DELETED_COLUMN,
            replay_horizon_seconds=5 * 24 * 3600,
        )
        base: str = build_retention_predicate("ts", cutoff)
        expected: str = f"((NOT {DELETED_COLUMN} AND {base}) OR ({DELETED_COLUMN} AND {base}))"
        assert retention_predicate(config, cutoff) == expected


class TestTombstoneRetentionDelete:
    """The tombstone-aware retention delete honors both the record window and the replay horizon."""

    def test_expires_live_and_horizon_expired_tombstones_only(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A live row past retention and a tombstone past the replay horizon are deleted; the rest survive."""
        now: datetime = datetime.now(tz=UTC)
        record_seconds: int = 10 * 24 * 3600
        replay_seconds: int = 30 * 24 * 3600
        table: pa.Table = pa.table(
            {
                "id": pa.array([0, 1, 2, 3], pa.int64()),
                TS_COLUMN: pa.array(
                    [
                        now - timedelta(days=20),
                        now - timedelta(days=5),
                        now - timedelta(days=20),
                        now - timedelta(days=40),
                    ],
                    pa.timestamp("us", tz="UTC"),
                ),
                DELETED_COLUMN: pa.array([False, False, True, True], pa.bool_()),
            }
        )
        uri: str = str(tmp_path / "tombstone-retention.lance")
        lance.write_dataset(table, uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=record_seconds,
            deleted_column=DELETED_COLUMN,
            replay_horizon_seconds=replay_seconds,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        dataset: lance.LanceDataset = lance.dataset(uri)
        result: dict[str, object] = run_retention_on_open_dataset(
            dataset, uri, config, compute_cutoff(record_seconds), telemetry
        )
        assert result["retention_rows_deleted"] == 2
        survivors: list[int] = sorted(int(row["id"]) for row in lance.dataset(uri).to_table().to_pylist())
        assert survivors == [1, 2]


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
        assert "error" not in result
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

    def test_fails_dataset_without_ts_column(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """A dataset lacking the configured ts column is a counted contract-violation failure."""
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
        assert result["error"]
        assert result["phase"] == "retention-config"

    def test_fleet_run_counts_missing_ts_column_as_failed(self, tmp_path: Path) -> None:
        """A fleet-level MaintenanceJob.run counts a missing-ts-column dataset as failed.

        Regression guard for PR-02 finding 3: the configured ``ts`` column being absent from a
        dataset's schema is a contract violation, not benign "nothing to do", so it must be
        visible to :func:`~lance_etl.fanout.count_failed` and, in turn, to the maintenance CLI's
        exit code.
        """
        uri: str = str(tmp_path / "no_ts_fleet.lance")
        lance.write_dataset(pa.table({"id": pa.array([1, 2], pa.int64())}), uri)
        config: MaintenanceConfig = MaintenanceConfig(
            telemetry=TelemetryConfig(),
            retention_seconds=RETENTION_SECONDS,
            ts_column=TS_COLUMN,
            commit_backoff_seconds=0.0,
        )
        results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [uri])
        assert count_failed(results) == 1

    def test_fleet_run_counts_unopenable_dataset_as_failed(self, tmp_path: Path) -> None:
        """A fleet-level MaintenanceJob.run counts a nonexistent dataset URI as failed.

        Regression guard for PR-02 finding 1: an unopenable dataset must be visible to
        :func:`~lance_etl.fanout.count_failed`, not silently reported as a benign skip.
        """
        missing_uri: str = str(tmp_path / "does_not_exist.lance")
        config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig(), commit_backoff_seconds=0.0)
        results: list[dict[str, object]] = MaintenanceJob(config).run(FakeSpark(), [missing_uri])
        assert count_failed(results) == 1

    def test_cli_exits_partial_failure_on_unopenable_dataset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The maintenance CLI's exit code reflects a fleet run over one unopenable dataset.

        Regression guard for PR-02: the maintenance ``run`` subcommand must map an isolated open
        failure to :data:`~lance_etl.cliutil.EXIT_PARTIAL_FAILURE`, not exit ``0`` as if the whole
        fleet run succeeded.
        """
        monkeypatch.setattr(maintenance_cli, "build_spark", fake_build_spark)
        missing_uri: str = str(tmp_path / "does_not_exist.lance")
        exit_code: int = maintenance_cli.main(["run", "--dataset-uri", missing_uri])
        assert exit_code == EXIT_PARTIAL_FAILURE


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
