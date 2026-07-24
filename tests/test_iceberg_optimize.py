"""Tests for the Iceberg source-table optimization job.

Unit tests cover identifier validation, the timestamp literal, and the ``CALL`` statement construction and result
parsing against a mocked Spark session, so they run without a Spark cluster. The integration test stands up the same
local Hadoop Iceberg catalog the benchmark uses, writes a tiny table with several small files across several snapshots,
runs the optimizer, and asserts that ``rewrite_data_files`` reduces the data-file count.

The destructive ``remove_orphan_files`` procedure is not exercised in the integration test. It is off by default and a
freshly written table has no orphans, so there is nothing observable to assert without fabricating dangling files.

``rewrite_manifests`` is disabled in the integration test for an environmental reason, not a faked result. The locally
bundled PySpark is 4.1.x while the resolved Iceberg runtime targets Spark 4.0
(``iceberg-spark-runtime-4.0_2.13:1.10.0``), and the two disagree on the ``DataSourceV2Relation.create`` signature that
``SparkTableUtil.createRelation`` calls. ``rewrite_manifests`` routes through that method, so it raises
``NoSuchMethodError`` against this version pair, whereas ``rewrite_data_files`` uses a different code path and runs. A
production cluster pins matching Spark and Iceberg-runtime versions. The integration test therefore asserts the
procedure that does run on this version pair, and the unit tests cover ``rewrite_manifests`` statement shape directly.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

import lance_etl.tools.cli as tools_cli
from bench.config import BenchConfig
from bench.spark_session import build_spark
from lance_etl.iceberg_optimize import (
    DEFAULT_MIN_INPUT_FILES,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
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


def test_timestamp_literal_rejects_negative_age() -> None:
    """A future destructive cutoff cannot be constructed accidentally."""
    with pytest.raises(ValueError, match="non-negative"):
        timestamp_literal(-1)


def mock_spark_returning(rows: list[Row]) -> MagicMock:
    """Build a mock Spark session whose SQL result exposes bounded actions over the given rows.

    Args:
        rows: The rows the collected result yields.

    Returns:
        The configured mock session.
    """
    spark: MagicMock = MagicMock()
    spark.sql.return_value.take.return_value = rows[:1]
    spark.sql.return_value.count.return_value = len(rows)
    return spark


def test_rewrite_data_files_statement_and_metrics(telemetry: Telemetry) -> None:
    """``rewrite_data_files`` builds a typed CALL with the bin-pack options and parses integer result columns."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(table="bench.db.t", telemetry=TelemetryConfig())
    spark: MagicMock = mock_spark_returning([Row(rewritten_data_files_count=4, added_data_files_count=1)])
    result = IcebergOptimizer(config).rewrite_data_files(spark, telemetry)
    statement: str = spark.sql.call_args[0][0]
    assert statement.startswith("CALL bench.system.rewrite_data_files(")
    assert "table => 'db.t'" in statement
    assert f"'min-input-files', '{DEFAULT_MIN_INPUT_FILES}'" in statement
    assert f"'target-file-size-bytes', '{DEFAULT_TARGET_FILE_SIZE_BYTES}'" in statement
    assert result.metrics == {"rewritten_data_files_count": 4, "added_data_files_count": 1}


def test_remove_orphan_files_counts_rows(telemetry: Telemetry) -> None:
    """``remove_orphan_files`` counts returned paths without collecting them to the driver."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(table="bench.db.t", telemetry=TelemetryConfig())
    spark: MagicMock = mock_spark_returning([Row(orphan_file_location="a"), Row(orphan_file_location="b")])
    result = IcebergOptimizer(config).remove_orphan_files(spark, telemetry)
    assert result.metrics == {"orphan_files_removed": 2}
    spark.sql.return_value.count.assert_called_once_with()
    spark.sql.return_value.take.assert_not_called()


def test_run_honors_step_toggles(telemetry_config: TelemetryConfig) -> None:
    """``run`` issues only the enabled steps in the fixed safe order."""
    config: IcebergOptimizeConfig = IcebergOptimizeConfig(
        table="bench.db.t",
        telemetry=telemetry_config,
        rewrite_data_files=True,
        rewrite_manifests=False,
        remove_orphan_files=True,
    )
    spark: MagicMock = mock_spark_returning([Row(count=0)])
    report = IcebergOptimizer(config).run(spark)
    assert [step.step for step in report.steps] == ["rewrite_data_files", "remove_orphan_files"]


def test_optimize_iceberg_cli_defaults() -> None:
    """The ``optimize-iceberg`` subcommand parses with opinionated step defaults."""
    args = tools_cli.build_parser().parse_args(["optimize-iceberg", "--table", "cat.db.t"])
    assert args.command == "optimize-iceberg"
    assert args.table == "cat.db.t"
    assert args.no_rewrite_data_files is False
    assert args.no_rewrite_manifests is False
    assert args.remove_orphan_files is False


def test_optimize_iceberg_cli_toggles() -> None:
    """The opt-out and opt-in flags flip the step toggles."""
    args = tools_cli.build_parser().parse_args(
        [
            "optimize-iceberg",
            "--table",
            "cat.db.t",
            "--no-rewrite-data-files",
            "--remove-orphan-files",
        ]
    )
    assert args.no_rewrite_data_files is True
    assert args.remove_orphan_files is True


@pytest.mark.parametrize(
    "removed_flag",
    ["--expire-snapshots", "--expire-retain-last", "--expire-older-than-days"],
)
def test_snapshot_expiration_flags_are_not_exposed(removed_flag: str) -> None:
    """Age-only snapshot expiration cannot bypass the durable PostgreSQL retention gate.

    Args:
        removed_flag: Former unsafe operator flag.
    """
    argv: list[str] = ["optimize-iceberg", "--table", "cat.db.t", removed_flag]
    if removed_flag != "--expire-snapshots":
        argv.append("7")
    with pytest.raises(SystemExit):
        tools_cli.build_parser().parse_args(argv)


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


@pytest.mark.integration
def test_optimizer_compacts_data_files(tmp_path: Path, fresh_spark_gateway: None) -> None:
    """Against a real local Iceberg catalog, ``rewrite_data_files`` bin-packs small files into fewer larger ones.

    Only ``rewrite_data_files`` is exercised here because the locally bundled PySpark 4.1.x and the Spark-4.0 Iceberg
    runtime disagree on the ``DataSourceV2Relation.create`` signature that ``rewrite_manifests`` routes through, so it
    raises ``NoSuchMethodError`` on this pair. Its statement shape is covered by the unit tests above. The module
    docstring documents this environmental constraint in full.

    Args:
        tmp_path: Pytest-provided temporary directory.
        fresh_spark_gateway: Guard ensuring Iceberg packages can enter the driver classpath.
    """
    del fresh_spark_gateway
    config: BenchConfig = bench_config(tmp_path)
    table: str = config.table()
    spark = build_spark(config, "iceberg-optimize-test")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.db")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} (id bigint, val string) USING iceberg")
        row_count: int = DEFAULT_MIN_INPUT_FILES + 1
        for index in range(row_count):
            spark.sql(f"INSERT INTO {table} VALUES ({index}, 'v{index}')")

        files_before: int = data_file_count(spark, table)
        assert files_before >= row_count

        opt_config: IcebergOptimizeConfig = IcebergOptimizeConfig(
            table=table,
            telemetry=TelemetryConfig(service="test", env="test"),
            rewrite_manifests=False,
        )
        report = IcebergOptimizer(opt_config).run(spark)

        assert [step.step for step in report.steps] == ["rewrite_data_files"]
        rewrite_step = report.steps[0]
        assert rewrite_step.metrics.get("rewritten_data_files_count", 0) > 0

        files_after: int = data_file_count(spark, table)
        assert 1 <= files_after < files_before
        assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"] == row_count
    finally:
        spark.stop()
