"""Unit tests for the snapshot-resolved Iceberg incremental read.

Iceberg 1.10 rejects the ``start-timestamp`` / ``end-timestamp`` read options outside changelog scans, so
``read_increment`` resolves the configured wall-clock window to snapshot ids through the ``{table}.snapshots``
metadata table and reads with ``start-snapshot-id`` / ``end-snapshot-id`` (incremental append scan) when a prior
snapshot bound exists, falling back to a ``snapshot-id``-pinned full batch scan on the first run and to an empty
DataFrame when the window resolves to no snapshots. These tests exercise :func:`lance_etl.etl.snapshot_id_bounds`
boundary semantics with a mocked snapshots metadata read, and every ``read_increment`` branch with patched bounds, so
no Spark session is required.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import lance_etl.etl as etl_module
from lance_etl.etl import ETLConfig, IcebergToLanceETL, snapshot_id_bounds
from lance_etl.telemetry import TelemetryConfig


def epoch_ms(moment: datetime) -> int:
    """Return the epoch milliseconds of a datetime.

    Args:
        moment: The instant to convert.

    Returns:
        Epoch milliseconds.
    """
    return int(moment.timestamp() * 1000)


def spark_with_snapshots(rows: list[dict[str, Any]]) -> MagicMock:
    """Build a mock Spark session whose snapshots metadata read returns the given rows.

    Args:
        rows: Mapping-like rows carrying ``committed_at`` datetimes and ``snapshot_id`` integers.

    Returns:
        The mock session wired so ``spark.read.format("iceberg").load(...).select(...).collect()`` yields the rows.
    """
    spark: MagicMock = MagicMock()
    spark.read.format.return_value.load.return_value.select.return_value.collect.return_value = rows
    return spark


def reader_spark() -> tuple[MagicMock, MagicMock]:
    """Build a mock Spark session with a chainable Iceberg reader.

    Returns:
        The mock session and its reader, whose ``option`` calls chain and whose ``load`` returns a DataFrame mock.
    """
    spark: MagicMock = MagicMock()
    reader: MagicMock = spark.read.format.return_value
    reader.option.return_value = reader
    return spark, reader


@pytest.fixture
def etl(tmp_path: Path, telemetry_config: TelemetryConfig) -> IcebergToLanceETL:
    """Build a default-configuration ETL rooted at a temporary directory.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        The ETL under test.
    """
    return IcebergToLanceETL(ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config))


SNAPSHOT_TIMES: list[datetime] = [
    datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
    datetime(2024, 6, 1, 6, 0, tzinfo=UTC),
    datetime(2024, 6, 1, 12, 0, tzinfo=UTC),
]


def snapshot_rows() -> list[dict[str, Any]]:
    """Return three snapshot rows committed at :data:`SNAPSHOT_TIMES` with ids 101, 102, 103.

    Returns:
        The mock snapshots metadata rows, deliberately unordered.
    """
    rows: list[dict[str, Any]] = [
        {"committed_at": moment, "snapshot_id": 101 + index} for index, moment in enumerate(SNAPSHOT_TIMES)
    ]
    return [rows[2], rows[0], rows[1]]


class TestSnapshotIdBounds:
    """snapshot_id_bounds resolves wall-clock windows to exclusive-start / inclusive-end snapshot ids."""

    def test_window_between_snapshots(self) -> None:
        """A window opening after the first snapshot and closing after the second yields (first, second)."""
        spark: MagicMock = spark_with_snapshots(snapshot_rows())
        start_ms: int = epoch_ms(SNAPSHOT_TIMES[0]) + 1
        end_ms: int = epoch_ms(SNAPSHOT_TIMES[1]) + 1
        assert snapshot_id_bounds(spark, "db.t", start_ms, end_ms) == (101, 102, True)
        spark.read.format.return_value.load.assert_called_once_with("db.t.snapshots")

    def test_snapshot_at_window_start_is_read(self) -> None:
        """A snapshot committed exactly at the window start falls inside the window, not the start bound."""
        spark: MagicMock = spark_with_snapshots(snapshot_rows())
        start_ms: int = epoch_ms(SNAPSHOT_TIMES[1])
        end_ms: int = epoch_ms(SNAPSHOT_TIMES[2]) + 1
        assert snapshot_id_bounds(spark, "db.t", start_ms, end_ms) == (101, 103, True)

    def test_snapshot_at_window_end_is_read(self) -> None:
        """A snapshot committed exactly at the window end is the inclusive end bound."""
        spark: MagicMock = spark_with_snapshots(snapshot_rows())
        start_ms: int = epoch_ms(SNAPSHOT_TIMES[0]) + 1
        end_ms: int = epoch_ms(SNAPSHOT_TIMES[2])
        assert snapshot_id_bounds(spark, "db.t", start_ms, end_ms) == (101, 103, True)

    def test_no_snapshot_before_window_start(self) -> None:
        """A window opening before the first snapshot has no start bound (first run)."""
        spark: MagicMock = spark_with_snapshots(snapshot_rows())
        end_ms: int = epoch_ms(SNAPSHOT_TIMES[2]) + 1
        assert snapshot_id_bounds(spark, "db.t", 0, end_ms) == (None, 103, True)

    def test_empty_snapshots_table(self) -> None:
        """A table without snapshots yields no bounds at all."""
        spark: MagicMock = spark_with_snapshots([])
        assert snapshot_id_bounds(spark, "db.t", 0, epoch_ms(SNAPSHOT_TIMES[2])) == (None, None, False)

    def test_window_after_last_snapshot_has_no_new_snapshots(self) -> None:
        """A window opening after every snapshot resolves equal bound ids and no new snapshots."""
        spark: MagicMock = spark_with_snapshots(snapshot_rows())
        start_ms: int = epoch_ms(SNAPSHOT_TIMES[2]) + 1
        end_ms: int = start_ms + 1000
        assert snapshot_id_bounds(spark, "db.t", start_ms, end_ms) == (103, 103, False)


class TestReadIncrement:
    """read_increment picks the incremental, first-run, or empty read from the resolved snapshot bounds."""

    def test_incremental_scan_uses_snapshot_id_options(
        self, etl: IcebergToLanceETL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With both bounds resolved the read sets start-snapshot-id (exclusive) and end-snapshot-id (inclusive)."""
        monkeypatch.setattr(etl_module, "snapshot_id_bounds", MagicMock(return_value=(11, 22, True)))
        spark, reader = reader_spark()
        result = etl.read_increment(spark, "db.t", 100, 200)
        assert result is reader.load.return_value
        reader.load.assert_called_once_with("db.t")
        options: set[tuple[str, str]] = {call.args for call in reader.option.call_args_list}
        assert options == {("start-snapshot-id", "11"), ("end-snapshot-id", "22")}

    def test_first_run_falls_back_to_pinned_batch_scan(
        self, etl: IcebergToLanceETL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a snapshot before the window start the read is a full batch scan pinned with snapshot-id."""
        monkeypatch.setattr(etl_module, "snapshot_id_bounds", MagicMock(return_value=(None, 22, True)))
        spark, reader = reader_spark()
        result = etl.read_increment(spark, "db.t", 100, 200)
        assert result is reader.load.return_value
        options: set[tuple[str, str]] = {call.args for call in reader.option.call_args_list}
        assert options == {("snapshot-id", "22")}

    def test_no_snapshots_returns_empty_frame(self, etl: IcebergToLanceETL, monkeypatch: pytest.MonkeyPatch) -> None:
        """A window with no resolvable end snapshot returns the current schema with zero rows."""
        monkeypatch.setattr(etl_module, "snapshot_id_bounds", MagicMock(return_value=(None, None, False)))
        spark, reader = reader_spark()
        result = etl.read_increment(spark, "db.t", 100, 200)
        assert result is reader.load.return_value.limit.return_value
        reader.load.return_value.limit.assert_called_once_with(0)
        reader.option.assert_not_called()

    def test_no_new_snapshots_in_window_returns_empty_frame(
        self, etl: IcebergToLanceETL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A window containing no newly committed snapshot returns zero rows even with resolved bound ids."""
        monkeypatch.setattr(etl_module, "snapshot_id_bounds", MagicMock(return_value=(22, 22, False)))
        spark, reader = reader_spark()
        result = etl.read_increment(spark, "db.t", 100, 200)
        assert result is reader.load.return_value.limit.return_value
        reader.load.return_value.limit.assert_called_once_with(0)
        reader.option.assert_not_called()

    def test_iceberg_read_options_are_merged(self, etl: IcebergToLanceETL, monkeypatch: pytest.MonkeyPatch) -> None:
        """Configured extra Iceberg read options reach the non-empty read alongside the snapshot bounds."""
        etl.config.iceberg_read_options = {"split-size": "134217728"}
        monkeypatch.setattr(etl_module, "snapshot_id_bounds", MagicMock(return_value=(11, 22, True)))
        spark, reader = reader_spark()
        etl.read_increment(spark, "db.t", 100, 200)
        options: set[tuple[str, str]] = {call.args for call in reader.option.call_args_list}
        assert ("split-size", "134217728") in options
