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

from pyspark.sql import SparkSession

from bench.config import BenchConfig
from bench.results import ensure_dir
from lance_etl.telemetry import TelemetryConfig


def bench_telemetry_config() -> TelemetryConfig:
    """Build the offline-safe telemetry configuration for benchmark jobs.

    DogStatsD sends are fire-and-forget UDP so no agent is required in the benchmark environment.

    Returns:
        A ``TelemetryConfig`` tagged for the bench service and environment.
    """
    return TelemetryConfig(service="lance-bench", env="bench")


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
    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        .config("spark.jars.packages", config.iceberg_package)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{config.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{config.catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", warehouse)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.etl_partitions))
    )
    return builder.getOrCreate()
