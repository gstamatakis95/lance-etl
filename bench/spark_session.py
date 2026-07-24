"""Spark session construction with a local Hadoop Iceberg catalog, plus shared benchmark telemetry.

The Iceberg Spark runtime is resolved at session start via ``spark.jars.packages``. The default coordinates target the
newest Iceberg release with a Spark 4 runtime and can be overridden with ``--iceberg-package`` if the installed Spark
minor version needs a different artifact. The session timezone is pinned to UTC so the ETL's ``TIMESTAMP`` window
literals compare deterministically against the generated ``updated_at`` values. The worker Python is pinned to the
driver's interpreter via ``PYSPARK_PYTHON`` (the only knob ``SparkContext`` consults when launched through the builder
rather than spark-submit) so executor tasks resolve the same virtualenv (numpy, pylance) even when the harness runs
without the venv activated on ``PATH``.

:func:`bench_telemetry_config` lives here so every benchmark phase (ingest, index, compact) imports it from one
place rather than reaching across into the index module.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from pyspark.sql import SparkSession

from bench.config import BenchConfig
from bench.results import ensure_dir
from lance_etl.spark_process import SPARK_CORE_CONF_PINS, ensure_spark_process_safety
from lance_etl.telemetry import TelemetryConfig


def bench_telemetry_config() -> TelemetryConfig:
    """Build the offline-safe telemetry configuration for benchmark jobs.

    DogStatsD sends are fire-and-forget UDP so no agent is required in the benchmark environment.
    When ``--capture-telemetry`` is active, :func:`~bench.telemetry_capture.telemetry_capture_session`
    sets ``LANCE_BENCH_STATSD_HOST`` and ``LANCE_BENCH_STATSD_PORT`` in the process environment
    before any Spark session is created.  This function reads those variables so that both the
    driver process and every Spark executor (which inherit the driver environment) direct their
    DogStatsD packets to the local capture listener instead of the default loopback port 8125.

    Returns:
        A ``TelemetryConfig`` tagged for the bench service and environment, with the statsd
        host and port resolved from ``LANCE_BENCH_STATSD_HOST`` / ``LANCE_BENCH_STATSD_PORT``
        when present, otherwise defaulting to ``localhost:8125``.
    """
    host: str = os.environ.get("LANCE_BENCH_STATSD_HOST", "localhost")
    port_str: str = os.environ.get("LANCE_BENCH_STATSD_PORT", "8125")
    try:
        port: int = int(port_str)
    except ValueError:
        port = 8125
    return TelemetryConfig(service="lance-bench", env="bench", statsd_host=host, statsd_port=port)


def build_spark(config: BenchConfig, app_name: str) -> SparkSession:
    """Build (or reuse) a Spark session configured for the local Iceberg catalog.

    Args:
        config: Benchmark configuration.
        app_name: Spark application name.

    Returns:
        The active Spark session.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse: str = ensure_dir(config.warehouse_dir()).resolve().as_uri()
    ivy_dir: str = str(ensure_dir(config.workspace / "ivy").resolve())
    builder: Any = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        .config("spark.jars.packages", config.iceberg_package)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{config.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{config.catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", warehouse)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.etl_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", str(config.etl_partitions))
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
    )
    key: str
    value: str
    for key, value in SPARK_CORE_CONF_PINS.items():
        builder = builder.config(key, value)
    session: SparkSession = builder.getOrCreate()
    ensure_spark_process_safety(session, "running benchmark jobs")
    return session
