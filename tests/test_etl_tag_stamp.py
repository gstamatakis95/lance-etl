"""Tests for the ETL's hourly interval-tag stamping.

The ETL stamps every dataset a run wrote with the configured truncated-hour interval tag,
after all batches commit. The tag is create-or-move: a later run within the same hour advances
the tag to the newest version, so the tag always marks the latest version produced in its
hour. These tests drive :meth:`IcebergToLanceETL.run_on_dataframe` on a local Spark session
against real Lance datasets and assert through ``dataset.tags``.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import lance
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import lance_etl.etl.job as etl_job
from lance_etl.etl import ETLConfig, IcebergToLanceETL, dataset_uri
from lance_etl.telemetry import TelemetryConfig

DIMENSION: int = 8

HOUR_TAG: str = "20260611T120000Z"

TS: datetime = datetime(2024, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session pinned to the test interpreter.

    Yields:
        A two-core local session with a UTC timezone and four shuffle partitions.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-tag-stamp-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def source_schema() -> StructType:
    """Return the Spark schema of the tag-stamp test source.

    Returns:
        A schema carrying the routing, key, timestamp, window, and op columns plus the vectors
        map, matching the SQL contract column types.
    """
    return StructType(
        [
            StructField("vector_id", StringType(), False),
            StructField("org_id", StringType(), False),
            StructField("tenant_id", StringType(), False),
            StructField("namespace", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("op", StringType(), False),
            StructField("vectors", MapType(StringType(), ArrayType(FloatType())), True),
            StructField("texts", MapType(StringType(), StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )


def stamp_config(tmp_path: Path, telemetry_config: TelemetryConfig, tag_stamp: str | None) -> ETLConfig:
    """Build an ETL configuration with the given interval-tag stamp.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        tag_stamp: The pre-formatted tag name, or ``None`` to disable stamping.

    Returns:
        The ETL configuration.
    """
    return ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        num_partitions=4,
        tag_stamp=tag_stamp,
    )


def source_row(vector_id: str, org: str, tenant: str, namespace: str) -> tuple:
    """Build one insert row for the given routing key.

    Args:
        vector_id: The row's vector id.
        org: Routing org id.
        tenant: Routing tenant id.
        namespace: Routing namespace.

    Returns:
        A row tuple matching :func:`source_schema`.
    """
    vector: list[float] = [float(index) for index in range(DIMENSION)]
    return (vector_id, org, tenant, namespace, TS, TS, "insert", {"vector": vector}, {"t": "x"}, {"k": "v"})


def test_tag_stamp_creates_hour_tag_on_every_written_dataset(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """A run with ``tag_stamp`` set tags each written dataset's latest version with the hour tag."""
    config: ETLConfig = stamp_config(tmp_path, telemetry_config, HOUR_TAG)
    rows: list[tuple] = [
        source_row("v1", "o1", "t1", "n1"),
        source_row("v2", "o2", "t2", "n2"),
    ]
    IcebergToLanceETL(config).run_on_dataframe(spark.createDataFrame(rows, source_schema()))

    for key in [("o1", "t1", "n1"), ("o2", "t2", "n2")]:
        dataset: lance.LanceDataset = lance.dataset(dataset_uri(config, *key))
        assert HOUR_TAG in list(dataset.tags.list())
        assert dataset.tags.get_version(HOUR_TAG) == dataset.version


def test_second_run_in_the_same_hour_moves_the_tag_to_the_newest_version(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """The hour tag advances to the latest version instead of duplicating or failing."""
    config: ETLConfig = stamp_config(tmp_path, telemetry_config, HOUR_TAG)
    etl: IcebergToLanceETL = IcebergToLanceETL(config)
    etl.run_on_dataframe(spark.createDataFrame([source_row("v1", "o1", "t1", "n1")], source_schema()))
    first_tagged: int = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).tags.get_version(HOUR_TAG)

    etl.run_on_dataframe(spark.createDataFrame([source_row("v2", "o1", "t1", "n1")], source_schema()))
    dataset: lance.LanceDataset = lance.dataset(dataset_uri(config, "o1", "t1", "n1"))
    moved: int = dataset.tags.get_version(HOUR_TAG)

    assert moved > first_tagged
    assert moved == dataset.version
    assert list(dataset.tags.list()).count(HOUR_TAG) == 1


def test_no_tag_stamp_never_calls_update_serving_tags(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default configuration performs no tag fan-out at all."""
    calls: list[str] = []

    def recording_update(*args: object, **kwargs: object) -> list[dict[str, object]]:
        """Record any unexpected stamp fan-out."""
        del args, kwargs
        calls.append("called")
        return []

    monkeypatch.setattr(etl_job, "update_serving_tags", recording_update)
    config: ETLConfig = stamp_config(tmp_path, telemetry_config, None)
    IcebergToLanceETL(config).run_on_dataframe(
        spark.createDataFrame([source_row("v1", "o1", "t1", "n1")], source_schema())
    )

    assert calls == []
    assert list(lance.dataset(dataset_uri(config, "o1", "t1", "n1")).tags.list()) == []
