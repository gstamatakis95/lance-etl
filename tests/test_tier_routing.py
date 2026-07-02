"""Tests for the indexer's small/large classification, pending the slice-C fleet unification.

Compaction routing is unified (see test_fleet_orchestration.py). The indexer classification
tests remain until the indexing fleet unification lands.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from conftest import make_vector_table, write_fragmented_dataset
from pyspark.sql import SparkSession

from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.telemetry import TelemetryConfig


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session pinned to the test interpreter.

    Yields:
        A two-core local session with the Spark UI disabled.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-tier-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_indexer_classify_splits_small_and_large(spark: SparkSession, tmp_path: Path) -> None:
    """The indexer routes a tiny dataset to the small tier and a many-fragment one to the large tier."""
    tiny: str = str(tmp_path / "tiny.lance")
    huge: str = str(tmp_path / "huge.lance")
    write_fragmented_dataset(tiny, make_vector_table(rows=20, dim=8), max_rows_per_file=20)
    write_fragmented_dataset(huge, make_vector_table(rows=100, dim=8), max_rows_per_file=20)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(), small_dataset_fragment_threshold=3, large_dataset_row_threshold=None
    )

    small, large = LanceIndexer(config).classify(spark, [tiny, huge])

    assert small == [tiny]
    assert large == [huge]


def test_indexer_classify_routes_row_heavy_dataset_to_large_tier(spark: SparkSession, tmp_path: Path) -> None:
    """The indexer defers a dataset over the row threshold to the large tier despite few fragments.

    The fragment count alone (2) is below the small-tier threshold, so only the row dimension routes
    this dataset to the distributed segment fan-out.
    """
    tiny: str = str(tmp_path / "tiny.lance")
    row_heavy: str = str(tmp_path / "rowheavy.lance")
    write_fragmented_dataset(tiny, make_vector_table(rows=20, dim=8), max_rows_per_file=20)
    write_fragmented_dataset(row_heavy, make_vector_table(rows=100, dim=8), max_rows_per_file=50)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        small_dataset_fragment_threshold=8,
        large_dataset_row_threshold=100,
    )

    small, large = LanceIndexer(config).classify(spark, [tiny, row_heavy])

    assert small == [tiny]
    assert large == [row_heavy]
