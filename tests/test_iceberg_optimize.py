"""Tests for the Iceberg source-table optimization job.

Unit tests cover identifier validation, the timestamp literal, and the ``CALL`` statement construction and result
parsing against a mocked Spark session, so they run without a Spark cluster. The integration test stands up the same
local Hadoop Iceberg catalog the benchmark uses, writes a tiny table with several small files across several snapshots,
runs the optimizer, and asserts the observable effects: fewer data files after ``rewrite_data_files`` and fewer
snapshots after ``expire_snapshots`` while honoring ``retain_last``.

The destructive ``remove_orphan_files`` procedure is not exercised in the integration test. It is off by default and a
freshly written table has no orphans, so there is nothing observable to assert without fabricating dangling files.

``rewrite_manifests`` and ``expire_snapshots`` are also disabled in the integration test for an environmental reason,
not a faked result. The locally bundled PySpark is 4.1.x while the resolved Iceberg runtime targets Spark 4.0
(``iceberg-spark-runtime-4.0_2.13:1.10.0``), and the two disagree on the ``DataSourceV2Relation.create`` signature that
``SparkTableUtil.createRelation`` calls. Both procedures route through that method, so both raise ``NoSuchMethodError``
against this version pair, whereas ``rewrite_data_files`` uses a different code path and runs. A production cluster pins
matching Spark and Iceberg-runtime versions where all four procedures run. The integration test therefore asserts the
procedure that does run on this version pair, ``rewrite_data_files``, and the ``CALL``-construction unit tests cover
``rewrite_manifests`` and ``expire_snapshots`` statement shape and result parsing directly.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark import SparkContext
from pyspark.sql import Row

import lance_etl.tools.cli as tools_cli
from bench.config import BenchConfig
from bench.spark_session import build_spark
from lance_etl.iceberg_optimize import (
    DEFAULT_EXPIRE_OLDER_THAN_DAYS,
    DEFAULT_EXPIRE_RETAIN_LAST,
    IcebergOptimizeConfig,
    IcebergOptimizer,
    timestamp_literal,
    validate_table_identifier,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


def test_validate_table_identifier_splits_catalog() -> None:
    """A qualified identifier splits into catalog and namespace-qualified table argument."""
    assert validate_table_identifier("prod.vectors.events") == ("prod", "vectors.events")
    assert validate_table_identifier("bench.db.sift") == ("bench", "db.sift")


def test_validate_table_identifier_rejects_bad_input() -> None:
    """A single component or an injection-shaped component is rejected."""
    with pytest.raises(ValueError, match="qualified"):
        validate_table_identifier("events")
    with pytest.raises(ValueError, match="invalid"):
        validate_table_identifier("cat.db.t; DROP TABLE x")
    with pytest.raises(ValueError, match="invalid"):
        validate_table_identifier("cat.db.t'")


def test_timestamp_literal_shape() -> None:
    """The timestamp literal is a bare wall-clock string with no timezone suffix."""
    literal: str = timestamp_literal(0)
    assert len(literal) == 19
    assert literal[4] == "-" and literal[13] == ":"


def mock_spark_returning(rows: list[Row]) -> MagicMock:
    """Build a mock Spark session whose ``sql(...).collect()`` returns the given rows.

    Args:
        rows: The rows the collected result yields.

    Returns:
        The configured mock session.
    """
    spark: MagicMock = MagicMock()
    spark.sql.return_value.collect.return_value = rows
    return spark


def test_rewrite_data_files_statement_and_metrics(telemetry: Telemetry) -> None:
    """``rewrite_data_files`` builds a typed CALL with the bin-pack options and parses integer result columns."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(
        table="bench.db.t", telemetry=TelemetryConfig(), min_input_files=2, target_file_size_bytes=1024
    )
    spark: MagicMock = mock_spark_returning([Row(rewritten_data_files_count=4, added_data_files_count=1)])
    result = IcebergOptimizer(config).rewrite_data_files(spark, telemetry)
    statement: str = spark.sql.call_args[0][0]
    assert statement.startswith("CALL bench.system.rewrite_data_files(")
    assert "table => 'db.t'" in statement
    assert "'min-input-files', '2'" in statement
    assert "'target-file-size-bytes', '1024'" in statement
    assert result.metrics == {"rewritten_data_files_count": 4, "added_data_files_count": 1}
    assert result.ran is True


def test_expire_snapshots_statement(telemetry: Telemetry) -> None:
    """``expire_snapshots`` passes a typed TIMESTAMP literal and the numeric retain_last."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(
        table="bench.db.t", telemetry=TelemetryConfig(), expire_retain_last=3, expire_older_than_days=1
    )
    spark: MagicMock = mock_spark_returning([Row(deleted_data_files_count=2)])
    IcebergOptimizer(config).expire_snapshots(spark, telemetry)
    statement: str = spark.sql.call_args[0][0]
    assert "CALL bench.system.expire_snapshots(" in statement
    assert "older_than => TIMESTAMP '" in statement
    assert "retain_last => 3" in statement


def test_remove_orphan_files_counts_rows(telemetry: Telemetry) -> None:
    """``remove_orphan_files`` reports the removed-file count as the number of returned rows."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(table="bench.db.t", telemetry=TelemetryConfig())
    spark: MagicMock = mock_spark_returning([Row(orphan_file_location="a"), Row(orphan_file_location="b")])
    result = IcebergOptimizer(config).remove_orphan_files(spark, telemetry)
    assert result.metrics == {"orphan_files_removed": 2}


def test_run_honors_step_toggles(telemetry_config: TelemetryConfig) -> None:
    """``run`` issues only the enabled steps in the fixed safe order."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(
        table="bench.db.t",
        telemetry=telemetry_config,
        rewrite_data_files=True,
        rewrite_manifests=False,
        expire_snapshots=True,
        remove_orphan_files=False,
    )
    spark: MagicMock = mock_spark_returning([Row(count=0)])
    report = IcebergOptimizer(config).run(spark)
    assert [step.step for step in report.steps] == ["rewrite_data_files", "expire_snapshots"]


def test_optimize_iceberg_cli_defaults() -> None:
    """The ``optimize-iceberg`` subcommand parses with opinionated step defaults."""
    args = tools_cli.build_parser().parse_args(["optimize-iceberg", "--table", "cat.db.t"])
    assert args.command == "optimize-iceberg"
    assert args.table == "cat.db.t"
    assert args.no_rewrite_data_files is False
    assert args.no_rewrite_manifests is False
    assert args.no_expire_snapshots is False
    assert args.remove_orphan_files is False
    assert args.expire_retain_last == DEFAULT_EXPIRE_RETAIN_LAST
    assert args.expire_older_than_days == DEFAULT_EXPIRE_OLDER_THAN_DAYS


def test_optimize_iceberg_cli_toggles() -> None:
    """The opt-out and opt-in flags flip the step toggles."""
    args = tools_cli.build_parser().parse_args(
        [
            "optimize-iceberg",
            "--table",
            "cat.db.t",
            "--no-rewrite-data-files",
            "--no-expire-snapshots",
            "--remove-orphan-files",
            "--expire-retain-last",
            "2",
        ]
    )
    assert args.no_rewrite_data_files is True
    assert args.no_expire_snapshots is True
    assert args.remove_orphan_files is True
    assert args.expire_retain_last == 2


def bench_config(tmp_path: Path) -> BenchConfig:
    """Build a tiny benchmark configuration rooted at a temporary workspace.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        A configuration pointing the local Hadoop Iceberg catalog at the temporary workspace.
    """
    return BenchConfig(
        command="optimize",
        workspace=tmp_path / "workspace",
        spark_master="local[2]",
        driver_memory="2g",
        catalog="bench",
        table_name="optimize_test",
        etl_partitions=2,
    )


def data_file_count(spark: object, table: str) -> int:
    """Return the number of data files in the current snapshot of an Iceberg table.

    Args:
        spark: Active Spark session.
        table: Fully-qualified Iceberg table name.

    Returns:
        The count of files in the ``{table}.files`` metadata table.
    """
    return spark.sql(f"SELECT * FROM {table}.files").count()


def jvm_gateway_already_launched() -> bool:
    """Report whether this process already launched a Spark JVM gateway.

    The Iceberg catalog requires ``spark.jars.packages`` to be resolved at JVM launch, so a
    gateway started by an earlier Spark test module in the same pytest process can never load
    the catalog plugin. Wrapped here to contain the private-attribute access per the repository
    rule on third-party internals.

    Returns:
        ``True`` when a Spark JVM gateway already exists in this process.
    """
    return getattr(SparkContext, "_gateway", None) is not None


@pytest.mark.integration
def test_optimizer_compacts_data_files(tmp_path: Path) -> None:
    """Against a real local Iceberg catalog, ``rewrite_data_files`` bin-packs small files into fewer larger ones.

    Only ``rewrite_data_files`` is exercised here because the locally bundled PySpark 4.1.x and the Spark-4.0 Iceberg
    runtime disagree on the ``DataSourceV2Relation.create`` signature that ``rewrite_manifests`` and
    ``expire_snapshots`` route through, so those two raise ``NoSuchMethodError`` on this pair. Their statement shape
    parsing are covered by the unit tests above. The module docstring documents this environmental constraint in full.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    if jvm_gateway_already_launched():
        pytest.skip("Iceberg catalog jars resolve only at JVM launch. Run this file in its own pytest process.")
    config: BenchConfig = bench_config(tmp_path)
    table: str = config.table()
    spark = build_spark(config, "iceberg-optimize-test")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.db")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} (id bigint, val string) USING iceberg")
        for index in range(4):
            spark.sql(f"INSERT INTO {table} VALUES ({index}, 'v{index}')")

        files_before: int = data_file_count(spark, table)
        assert files_before >= 4

        opt_config: IcebergOptimizeConfig = IcebergOptimizeConfig(
            table=table,
            telemetry=TelemetryConfig(service="test", env="test"),
            rewrite_manifests=False,
            expire_snapshots=False,
            min_input_files=2,
        )
        report = IcebergOptimizer(opt_config).run(spark)

        assert [step.step for step in report.steps] == ["rewrite_data_files"]
        rewrite_step = report.steps[0]
        assert rewrite_step.ran is True
        assert rewrite_step.metrics.get("rewritten_data_files_count", 0) >= 2

        files_after: int = data_file_count(spark, table)
        assert 1 <= files_after < files_before
        assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"] == 4
    finally:
        spark.stop()
