"""End-to-end skew, fan-out, and cross-window tests for the full Spark ETL path.

These run the real :meth:`IcebergToLanceETL.run_on_dataframe` on a four-core local session so the
salted routing shuffle fans a skewed big dataset across several concurrent merge writers while a long
tail of tiny datasets is routed alongside it. They pin three properties end to end: a big org fans out
without duplicating keys and resolves planted duplicates last-write-wins, a fleet of tiny orgs sizes
the shuffle by the dataset floor, and the ``source.ts >= target.ts`` upsert guard holds across two
full Spark runs, not just at the sink level.

The sources are built with ``spark.range`` and column expressions rather than materialized Python
rows so a forty-thousand-row big org stays cheap to construct.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import lance
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from lance_etl.etl import ETLConfig, IcebergToLanceETL, dataset_uri
from lance_etl.etl.plan import RoutingPlan, compute_routing_plan
from lance_etl.telemetry import TelemetryConfig

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a four-core local Spark session pinned to the test interpreter.

    Yields:
        A four-core local session with a UTC timezone so the salted shuffle runs several partitions
        concurrently against the same dataset URI.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[4]")
        .appName("lance-etl-skew-tests")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def skew_schema() -> StructType:
    """Return the contract-satisfying schema shared by every skew source.

    Returns:
        A schema with the routing columns, ``vector_id``, ``op``, the two required timestamps, and a
        ``payload`` column used to check the last-write-wins winner.
    """
    return StructType(
        [
            StructField("org_id", StringType(), False),
            StructField("tenant_id", StringType(), False),
            StructField("namespace", StringType(), False),
            StructField("vector_id", StringType(), False),
            StructField("op", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("payload", StringType(), False),
        ]
    )


def range_rows(frame: DataFrame, org: str, key_prefix: str, payload: str, ts_seconds: int) -> DataFrame:
    """Shape a ``spark.range`` frame into skew-source rows for one org.

    Args:
        frame: A frame carrying an ``id`` column from ``spark.range``.
        org: The org_id literal for every row.
        key_prefix: Prefix joined with ``id`` to form each ``vector_id``.
        payload: The payload literal for every row.
        ts_seconds: Epoch seconds used for both timestamps.

    Returns:
        A frame conforming to :func:`skew_schema`.
    """
    return frame.select(
        F.lit(org).alias("org_id"),
        F.lit("t1").alias("tenant_id"),
        F.lit("n1").alias("namespace"),
        F.concat(F.lit(key_prefix), F.col("id").cast("string")).alias("vector_id"),
        F.lit("insert").alias("op"),
        F.timestamp_seconds(F.lit(ts_seconds)).alias("event_timestamp"),
        F.timestamp_seconds(F.lit(ts_seconds)).alias("processing_timestamp"),
        F.lit(payload).alias("payload"),
    )


def test_skewed_big_org_fans_out_without_duplicates(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """A skewed big org fans across sub-buckets with zero duplicate keys and LWW-resolved dups.

    The big org carries forty thousand distinct keys plus a hundred planted duplicates whose second
    occurrence carries a newer timestamp, alongside thirty tiny orgs of three rows each. A small
    ``bucket_rows`` forces the big org onto ``K`` key-hash sub-buckets (capped at
    ``max_buckets_per_dataset``), so several merge writers commit to the one dataset concurrently.
    Because the salt is a pure function of the merge key, no key fans across sub-buckets, so the big
    dataset ends with exactly the distinct-key count and no duplicate ``vector_id``. The planted
    duplicates resolve to the newer payload, and every tiny org keeps its exact rows.

    Args:
        spark: The module-scoped four-core local session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    distinct_keys: int = 40_000
    planted_dups: int = 100
    big_primary: DataFrame = range_rows(spark.range(0, distinct_keys), "bigorg", "big-", "old", ts_seconds=1_000)
    big_newer: DataFrame = range_rows(spark.range(0, planted_dups), "bigorg", "big-", "new", ts_seconds=2_000)
    tiny_orgs: int = 30
    rows_each: int = 3
    tiny: DataFrame = spark.range(0, tiny_orgs * rows_each).select(
        F.concat(F.lit("tiny"), (F.col("id") / rows_each).cast("int").cast("string")).alias("org_id"),
        F.lit("t1").alias("tenant_id"),
        F.lit("n1").alias("namespace"),
        F.concat(F.lit("small-"), F.col("id").cast("string")).alias("vector_id"),
        F.lit("insert").alias("op"),
        F.timestamp_seconds(F.lit(500)).alias("event_timestamp"),
        F.timestamp_seconds(F.lit(500)).alias("processing_timestamp"),
        F.lit("tiny").alias("payload"),
    )
    source: DataFrame = big_primary.unionByName(big_newer).unionByName(tiny)

    config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        bucket_rows=5_000,
        max_buckets_per_dataset=8,
        merge_batch_bytes=256 * 1024,
        conflict_retries=50,
        retry_backoff_seconds=0.05,
    )
    IcebergToLanceETL(config).run_on_dataframe(source)

    big_table = lance.dataset(dataset_uri(config, "bigorg", "t1", "n1")).to_table()
    vids: list[str] = big_table["vector_id"].to_pylist()
    assert len(vids) == distinct_keys
    assert len(set(vids)) == distinct_keys, "big org fanned out with duplicate keys"

    payload_by_id: dict[str, str] = dict(zip(vids, big_table["payload"].to_pylist(), strict=True))
    for i in range(planted_dups):
        assert payload_by_id[f"big-{i}"] == "new", "planted duplicate did not resolve to the newer row"
    assert payload_by_id["big-20000"] == "old"

    for org_index in range(tiny_orgs):
        tiny_uri: str = dataset_uri(config, f"tiny{org_index}", "t1", "n1")
        assert lance.dataset(tiny_uri).count_rows() == rows_each


def test_many_orgs_partition_floor(spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
    """Two hundred tiny orgs size the shuffle by the dataset floor and each dataset is written.

    With ``datasets_per_task=16`` the planned width must reach at least ``ceil(trio_count / 16)`` so
    the long tail is not serialized into a handful of tasks. The full merge then materializes every
    org's dataset.

    Args:
        spark: The module-scoped four-core local session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    num_orgs: int = 200
    rows_each: int = 2
    source: DataFrame = spark.range(0, num_orgs * rows_each).select(
        F.concat(F.lit("org"), (F.col("id") / rows_each).cast("int").cast("string")).alias("org_id"),
        F.lit("t1").alias("tenant_id"),
        F.lit("n1").alias("namespace"),
        F.concat(F.lit("k"), F.col("id").cast("string")).alias("vector_id"),
        F.lit("insert").alias("op"),
        F.timestamp_seconds(F.lit(500)).alias("event_timestamp"),
        F.timestamp_seconds(F.lit(500)).alias("processing_timestamp"),
        F.lit("x").alias("payload"),
    )
    config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        datasets_per_task=16,
        conflict_retries=50,
        retry_backoff_seconds=0.05,
    )
    plan: RoutingPlan = compute_routing_plan(source, config)
    assert plan.trio_count == num_orgs
    assert plan.num_partitions >= -(-num_orgs // 16)

    IcebergToLanceETL(config).run_on_dataframe(source)
    written: list[Path] = list(Path(tmp_path).glob("*/t1/n1.lance"))
    assert len(written) == num_orgs


def test_cross_window_out_of_order_full_spark_path(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """Two full Spark runs in reverse timestamp order keep the newer window's payload.

    The first run writes a newer window (higher event_timestamp), the second run replays the same
    keys with an older window. The ``source.ts >= target.ts`` upsert guard, previously pinned only
    at the sink level, must hold through the whole Spark path so the stored rows retain the newer
    window's values.

    Args:
        spark: The module-scoped four-core local session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    ts_new: datetime = datetime(2024, 6, 1, tzinfo=UTC)
    ts_old: datetime = datetime(2024, 1, 1, tzinfo=UTC)
    keys: list[str] = [f"k{i}" for i in range(5)]
    window_a: list[tuple] = [("wo", "t1", "n1", k, "insert", ts_new, ts_new, "A") for k in keys]
    window_b: list[tuple] = [("wo", "t1", "n1", k, "insert", ts_old, ts_old, "B") for k in keys]

    config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        num_partitions=2,
        conflict_retries=50,
        retry_backoff_seconds=0.05,
    )
    IcebergToLanceETL(config).run_on_dataframe(spark.createDataFrame(window_a, skew_schema()))
    IcebergToLanceETL(config).run_on_dataframe(spark.createDataFrame(window_b, skew_schema()))

    table = lance.dataset(dataset_uri(config, "wo", "t1", "n1")).to_table()
    assert table.num_rows == 5
    assert set(table["payload"].to_pylist()) == {"A"}, "older window overwrote the newer stored rows"
