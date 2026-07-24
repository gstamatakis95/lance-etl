"""Spark correctness and memory pins used by the executor-backed benchmark jobs."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import bench.spark_session as spark_session
from bench.config import BenchConfig


def test_benchmark_spark_pins_production_executor_safety(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The benchmark cannot enable duplicate task attempts or unbounded Arrow batches.

    Args:
        monkeypatch: Scoped Spark builder and environment replacement.
        tmp_path: Isolated benchmark workspace.
    """
    builder: MagicMock = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    session: MagicMock = MagicMock()
    session.sparkContext.getConf.return_value.get.return_value = "false"
    builder.getOrCreate.return_value = session
    monkeypatch.setattr(spark_session, "SparkSession", SimpleNamespace(builder=builder))
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    config: BenchConfig = BenchConfig(command="prepare", workspace=tmp_path, etl_partitions=7)

    assert spark_session.build_spark(config, "bench-test") is session

    configuration: dict[str, object] = {call.args[0]: call.args[1] for call in builder.config.call_args_list}
    assert configuration["spark.speculation"] == "false"
    assert configuration["spark.python.use.daemon"] == "false"
    assert configuration["spark.python.worker.faulthandler.enabled"] == "true"
    assert configuration["spark.sql.execution.pyspark.udf.faulthandler.enabled"] == "true"
    assert configuration["spark.sql.shuffle.partitions"] == "7"
    assert configuration["spark.sql.adaptive.enabled"] == "true"
    assert configuration["spark.sql.adaptive.advisoryPartitionSizeInBytes"] == "64m"
    assert configuration["spark.sql.adaptive.coalescePartitions.initialPartitionNum"] == "7"
    assert configuration["spark.sql.execution.arrow.maxRecordsPerBatch"] == "4096"


def test_benchmark_spark_rejects_unsafe_reused_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A reused context cannot bypass the benchmark's speculation correctness pin.

    Args:
        monkeypatch: Scoped Spark builder replacement.
        tmp_path: Isolated benchmark workspace.
    """
    builder: MagicMock = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    session: MagicMock = MagicMock()
    session.sparkContext.getConf.return_value.get.return_value = "true"
    builder.getOrCreate.return_value = session
    monkeypatch.setattr(spark_session, "SparkSession", SimpleNamespace(builder=builder))
    config: BenchConfig = BenchConfig(command="prepare", workspace=tmp_path)

    with pytest.raises(RuntimeError, match="spark.speculation enabled"):
        spark_session.build_spark(config, "bench-test")

    session.stop.assert_called_once_with()
