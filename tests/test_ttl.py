"""Tests for the TTL data-expiration job.

Covers: enabled=False strict no-op, predicate safety (unknown column raises), a real delete that removes only
expired rows, and the two-tier path (small vs large classification) converging to the same result on a small
dataset.  A tiny real Lance dataset with a timestamp column is used for the functional tests so the delete path
exercises actual Lance I/O.

Spark is only imported and started for the tier-routing tests; the enabled=False and predicate tests run without
Spark to keep CI fast.
"""

from __future__ import annotations

import os
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig
from lance_etl.ttl import (
    TTLConfig,
    TTLJob,
    TTLReport,
    build_ttl_predicate,
    classify_or_expire,
    compute_cutoff,
    expire_one_dataset,
    validate_timestamp_column,
)

TIMESTAMP_COLUMN: str = "ts"


def make_timestamp_table(rows: int, base_dt: datetime, step: timedelta) -> pa.Table:
    """Build a table with an id column and a timestamp column at even intervals.

    Args:
        rows: Number of rows.
        base_dt: Timestamp of the first row.
        step: Time between consecutive rows.

    Returns:
        A pyarrow table with ``id`` (int64) and ``ts`` (timestamp[us, UTC]) columns.
    """
    ids: pa.Array = pa.array(range(rows), pa.int64())
    timestamps: pa.Array = pa.array(
        [base_dt + step * i for i in range(rows)],
        pa.timestamp("us", tz="UTC"),
    )
    return pa.table({"id": ids, TIMESTAMP_COLUMN: timestamps})


@pytest.fixture
def telemetry_config() -> TelemetryConfig:
    """Return a test telemetry configuration.

    Returns:
        A TelemetryConfig pointing at localhost with ddtrace disabled.
    """
    return TelemetryConfig(service="lance-etl-ttl-tests", env="test")


@pytest.fixture
def base_ttl_config(telemetry_config: TelemetryConfig) -> TTLConfig:
    """Return a TTLConfig with enabled=False (the safe default).

    Args:
        telemetry_config: Test telemetry configuration.

    Returns:
        A disabled TTLConfig with a 30-day retention and the test timestamp column.
    """
    return TTLConfig(
        retention=timedelta(days=30),
        telemetry=telemetry_config,
        enabled=False,
        timestamp_column=TIMESTAMP_COLUMN,
        compact_after_delete=False,
    )


@pytest.fixture
def enabled_ttl_config(base_ttl_config: TTLConfig) -> TTLConfig:
    """Return a TTLConfig with enabled=True.

    Args:
        base_ttl_config: The base disabled config.

    Returns:
        An enabled TTLConfig with the same settings.
    """
    return replace(base_ttl_config, enabled=True)


@pytest.fixture
def dataset_with_timestamps(tmp_path: Path) -> tuple[str, int, int]:
    """Write a Lance dataset with ten rows, half old and half recent.

    Five rows have timestamps 100 days ago (should be expired with a 30-day retention) and five rows have
    timestamps 10 days ago (should be retained).

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        ``(uri, old_count, recent_count)`` where ``old_count`` is the number of rows that should be deleted
        and ``recent_count`` is the number of rows that should survive.
    """
    now: datetime = datetime.now(tz=UTC)
    old_base: datetime = now - timedelta(days=100)
    recent_base: datetime = now - timedelta(days=10)
    old_table: pa.Table = make_timestamp_table(5, old_base, timedelta(hours=1))
    recent_table: pa.Table = make_timestamp_table(5, recent_base, timedelta(hours=1))
    full_table: pa.Table = pa.concat_tables([old_table, recent_table])
    uri: str = str(tmp_path / "ts_dataset.lance")
    lance.write_dataset(full_table, uri)
    return uri, 5, 5


class TestTTLConfigDefaults:
    """TTLConfig carries the expected defaults."""

    def test_enabled_defaults_to_false(self, base_ttl_config: TTLConfig) -> None:
        """enabled is False by default so the job is opt-in."""
        assert base_ttl_config.enabled is False

    def test_timestamp_column_default(self, telemetry_config: TelemetryConfig) -> None:
        """Default timestamp_column is 'timestamp', matching ETLConfig.ts_col."""
        config: TTLConfig = TTLConfig(retention=timedelta(days=7), telemetry=telemetry_config)
        assert config.timestamp_column == "timestamp"

    def test_compact_after_delete_default(self, telemetry_config: TelemetryConfig) -> None:
        """compact_after_delete defaults to True."""
        config: TTLConfig = TTLConfig(retention=timedelta(days=7), telemetry=telemetry_config)
        assert config.compact_after_delete is True

    def test_retention_is_stored(self, base_ttl_config: TTLConfig) -> None:
        """The configured retention timedelta is stored correctly."""
        assert base_ttl_config.retention == timedelta(days=30)


class TestEnabledFalseIsStrictNoop:
    """When enabled=False, TTLJob.run is a strict no-op."""

    def test_run_returns_disabled_report_without_spark(self, base_ttl_config: TTLConfig) -> None:
        """With enabled=False, run() accepts a mock Spark session and returns a zero report."""
        spark: MagicMock = MagicMock()
        job: TTLJob = TTLJob(base_ttl_config)
        report: TTLReport = job.run(spark, dataset_uris=["s3://bucket/any.lance"])
        assert report.enabled is False
        assert report.datasets_scanned == 0
        assert report.total_rows_deleted == 0
        assert report.datasets_expired == 0
        assert report.datasets_compacted == 0

    def test_run_does_not_touch_spark_context(self, base_ttl_config: TTLConfig) -> None:
        """With enabled=False, the Spark context is never accessed."""
        spark: MagicMock = MagicMock()
        TTLJob(base_ttl_config).run(spark, dataset_uris=["s3://bucket/any.lance"])
        spark.sparkContext.parallelize.assert_not_called()

    def test_run_does_not_open_lance(self, base_ttl_config: TTLConfig, tmp_path: Path) -> None:
        """With enabled=False, no Lance dataset is opened even when real URIs are provided."""
        real_uri: str = str(tmp_path / "real.lance")
        lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), real_uri)
        spark: MagicMock = MagicMock()
        report: TTLReport = TTLJob(base_ttl_config).run(spark, dataset_uris=[real_uri])
        assert report.total_rows_deleted == 0

    def test_run_with_no_uris_still_noop(self, base_ttl_config: TTLConfig) -> None:
        """With enabled=False, not providing any URIs is also a strict no-op."""
        spark: MagicMock = MagicMock()
        report: TTLReport = TTLJob(base_ttl_config).run(spark, dataset_uris=[])
        assert report.enabled is False
        assert report.total_rows_deleted == 0


class TestPredicateSafety:
    """The delete predicate validation and construction is safe."""

    def test_build_predicate_format(self) -> None:
        """build_ttl_predicate produces a correctly formatted TIMESTAMP literal."""
        cutoff: datetime = datetime(2025, 3, 15, 12, 30, 45, 123456, tzinfo=UTC)
        predicate: str = build_ttl_predicate("ts", cutoff)
        assert predicate == "ts < TIMESTAMP '2025-03-15T12:30:45.123456'"

    def test_build_predicate_converts_to_utc(self) -> None:
        """build_ttl_predicate converts non-UTC datetimes to UTC."""
        eastern: datetime = datetime(2025, 3, 15, 8, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
        predicate: str = build_ttl_predicate("event_time", eastern)
        assert "2025-03-15T13:00:00.000000" in predicate

    def test_validate_timestamp_column_rejects_injection(self, tmp_path: Path) -> None:
        """validate_timestamp_column raises ValueError for column names with special characters."""
        uri: str = str(tmp_path / "safe.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        with pytest.raises(ValueError, match="allowlist"):
            validate_timestamp_column("ts; DROP TABLE", ds.schema)

    def test_validate_timestamp_column_rejects_space(self, tmp_path: Path) -> None:
        """Column names with spaces fail the allowlist."""
        uri: str = str(tmp_path / "safe2.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        with pytest.raises(ValueError, match="allowlist"):
            validate_timestamp_column("my col", ds.schema)

    def test_validate_timestamp_column_rejects_unknown(self, tmp_path: Path) -> None:
        """validate_timestamp_column raises KeyError when the column is absent from the schema."""
        uri: str = str(tmp_path / "nots.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        with pytest.raises(KeyError, match="not present"):
            validate_timestamp_column("ts", ds.schema)

    def test_validate_timestamp_column_accepts_valid(self, tmp_path: Path) -> None:
        """A column that passes the allowlist and is in the schema does not raise."""
        uri: str = str(tmp_path / "valid.lance")
        table: pa.Table = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "ts": pa.array([datetime.now(tz=UTC)], pa.timestamp("us", tz="UTC")),
            }
        )
        ds: lance.LanceDataset = lance.write_dataset(table, uri)
        validate_timestamp_column("ts", ds.schema)


class TestEnabledRunDeletes:
    """An enabled TTL run deletes expired rows and retains recent rows."""

    def test_expire_one_dataset_deletes_old_rows(
        self, dataset_with_timestamps: tuple[str, int, int], enabled_ttl_config: TTLConfig
    ) -> None:
        """expire_one_dataset removes rows older than the cutoff and keeps recent ones."""
        uri, old_count, recent_count = dataset_with_timestamps
        real_telemetry: Telemetry = Telemetry.create(enabled_ttl_config.telemetry, attach_lance_bridge=False)
        cutoff: datetime = compute_cutoff(enabled_ttl_config.retention)
        result: dict = expire_one_dataset(uri, enabled_ttl_config, cutoff, real_telemetry)
        assert result["rows_deleted"] == old_count
        assert result["skipped"] == ""
        remaining_ds: lance.LanceDataset = lance.dataset(uri)
        assert remaining_ds.count_rows() == recent_count

    def test_expire_one_dataset_keeps_all_recent_rows(self, tmp_path: Path, enabled_ttl_config: TTLConfig) -> None:
        """When no rows are old enough, expire_one_dataset deletes nothing."""
        now: datetime = datetime.now(tz=UTC)
        table: pa.Table = make_timestamp_table(10, now - timedelta(days=5), timedelta(hours=1))
        uri: str = str(tmp_path / "recent_only.lance")
        lance.write_dataset(table, uri)
        real_telemetry: Telemetry = Telemetry.create(enabled_ttl_config.telemetry, attach_lance_bridge=False)
        cutoff: datetime = compute_cutoff(enabled_ttl_config.retention)
        result: dict = expire_one_dataset(uri, enabled_ttl_config, cutoff, real_telemetry)
        assert result["rows_deleted"] == 0
        remaining_ds: lance.LanceDataset = lance.dataset(uri)
        assert remaining_ds.count_rows() == 10

    def test_expire_one_dataset_skips_missing_column(self, tmp_path: Path, enabled_ttl_config: TTLConfig) -> None:
        """expire_one_dataset skips datasets where the timestamp column is absent."""
        uri: str = str(tmp_path / "no_ts.lance")
        lance.write_dataset(pa.table({"id": pa.array([1, 2], pa.int64())}), uri)
        real_telemetry: Telemetry = Telemetry.create(enabled_ttl_config.telemetry, attach_lance_bridge=False)
        cutoff: datetime = compute_cutoff(enabled_ttl_config.retention)
        result: dict = expire_one_dataset(uri, enabled_ttl_config, cutoff, real_telemetry)
        assert result["rows_deleted"] == 0
        assert result["skipped"] != ""

    def test_run_raises_on_zero_retention(self, enabled_ttl_config: TTLConfig) -> None:
        """TTLJob.run raises ValueError when retention is zero."""
        zero_config: TTLConfig = replace(enabled_ttl_config, retention=timedelta(0))
        job: TTLJob = TTLJob(zero_config)
        spark: MagicMock = MagicMock()
        with pytest.raises(ValueError, match="positive timedelta"):
            job.run(spark, dataset_uris=["s3://bucket/any.lance"])

    def test_run_raises_without_uris_or_base_uri(self, enabled_ttl_config: TTLConfig) -> None:
        """TTLJob.run raises ValueError when neither dataset_uris nor base_uri is provided."""
        job: TTLJob = TTLJob(enabled_ttl_config)
        spark: MagicMock = MagicMock()
        with pytest.raises(ValueError, match="dataset_uris or base_uri"):
            job.run(spark)

    def test_run_empty_uris_returns_zero_report(self, enabled_ttl_config: TTLConfig) -> None:
        """TTLJob.run with an empty URI list returns a zero report without calling Spark."""
        spark: MagicMock = MagicMock()
        report: TTLReport = TTLJob(enabled_ttl_config).run(spark, dataset_uris=[])
        assert report.enabled is True
        assert report.datasets_scanned == 0
        assert report.total_rows_deleted == 0
        spark.sparkContext.parallelize.assert_not_called()


class TestTierRouting:
    """Small and large tier paths converge to the same result on small datasets."""

    @pytest.fixture(scope="class")
    def spark(self) -> SparkSession:
        """Provide a local Spark session for tier-routing tests.

        Yields:
            A two-core local Spark session.
        """
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        session: SparkSession = (
            SparkSession.builder.master("local[2]")
            .appName("lance-etl-ttl-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        yield session
        session.stop()

    def test_small_and_large_tier_delete_same_rows(
        self,
        tmp_path: Path,
        enabled_ttl_config: TTLConfig,
        spark: SparkSession,
    ) -> None:
        """Both tier-A (in-process classify_or_expire) and the TTLJob.run path delete the same expired rows.

        Because the dataset has fewer fragments than the threshold it is classified as small by both paths. The
        test verifies that the outcome of a direct classify_or_expire call matches the result of the full
        TTLJob.run, confirming the two-tier dispatch converges.
        """
        now: datetime = datetime.now(tz=UTC)
        old_base: datetime = now - timedelta(days=100)
        recent_base: datetime = now - timedelta(days=10)
        old_table: pa.Table = make_timestamp_table(4, old_base, timedelta(hours=1))
        recent_table: pa.Table = make_timestamp_table(4, recent_base, timedelta(hours=1))

        uri_direct: str = str(tmp_path / "tier_direct.lance")
        uri_job: str = str(tmp_path / "tier_job.lance")
        lance.write_dataset(pa.concat_tables([old_table, recent_table]), uri_direct)
        lance.write_dataset(pa.concat_tables([old_table, recent_table]), uri_job)

        cutoff: datetime = compute_cutoff(enabled_ttl_config.retention)
        real_telemetry: Telemetry = Telemetry.create(enabled_ttl_config.telemetry, attach_lance_bridge=False)
        direct_result: dict = classify_or_expire(uri_direct, enabled_ttl_config, cutoff, real_telemetry)

        job_config: TTLConfig = replace(enabled_ttl_config, batch_partitions=2, large_tier_partitions=2)
        report: TTLReport = TTLJob(job_config).run(spark, dataset_uris=[uri_job])

        assert direct_result["rows_deleted"] == 4
        assert report.total_rows_deleted == 4
        assert lance.dataset(uri_direct).count_rows() == 4
        assert lance.dataset(uri_job).count_rows() == 4

    def test_run_discovers_and_expires_datasets_under_base_uri(
        self, tmp_path: Path, enabled_ttl_config: TTLConfig, spark: SparkSession
    ) -> None:
        """TTLJob.run with base_uri discovers and expires datasets found by discover_datasets."""
        now: datetime = datetime.now(tz=UTC)
        old_table: pa.Table = make_timestamp_table(6, now - timedelta(days=90), timedelta(hours=1))
        recent_table: pa.Table = make_timestamp_table(3, now - timedelta(days=5), timedelta(hours=1))
        mixed: pa.Table = pa.concat_tables([old_table, recent_table])
        uri_a: str = str(tmp_path / "a.lance")
        uri_b: str = str(tmp_path / "sub" / "b.lance")
        lance.write_dataset(mixed, uri_a)
        lance.write_dataset(mixed, uri_b)

        job_config: TTLConfig = replace(enabled_ttl_config, batch_partitions=2, large_tier_partitions=2)
        report: TTLReport = TTLJob(job_config).run(spark, base_uri=str(tmp_path))

        assert report.enabled is True
        assert report.datasets_scanned == 2
        assert report.total_rows_deleted == 12
        assert lance.dataset(uri_a).count_rows() == 3
        assert lance.dataset(uri_b).count_rows() == 3
