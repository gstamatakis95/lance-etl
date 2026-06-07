"""Tests proving the two-tier small/large routing in compaction and indexing.

The per-tenant population is power-law shaped: a tiny dataset must take the cheap in-process path and a huge one must
take the distributed fan-out. These tests pin that routing decision for both the compactor (no Spark needed: the
classification and the small-tier compaction both run in process) and the indexer (the classification job that splits
datasets by fragment count).
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
from lance_etl.maintenance import MaintenanceConfig, classify_or_compact
from lance_etl.telemetry import Telemetry, TelemetryConfig


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


def test_tiny_dataset_takes_cheap_in_process_compaction(tmp_path: Path, telemetry: Telemetry) -> None:
    """A dataset below the fragment threshold is compacted in process and reports the small tier."""
    uri: str = str(tmp_path / "tiny.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=100, dim=8), max_rows_per_file=25)
    config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig(), large_dataset_fragment_threshold=128)

    result: dict[str, object] = classify_or_compact(uri, config, telemetry)

    assert result["tier"] == "small"
    assert result["fragments_removed"] == 4
    assert result["fragments_added"] == 1


def test_huge_dataset_defers_to_fan_out_without_compacting(tmp_path: Path, telemetry: Telemetry) -> None:
    """A dataset above the fragment threshold is only classified, leaving the rewrite to the distributed tier."""
    uri: str = str(tmp_path / "huge.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=100, dim=8), max_rows_per_file=25)
    config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig(), large_dataset_fragment_threshold=2)

    result: dict[str, object] = classify_or_compact(uri, config, telemetry)

    assert result == {"uri": uri, "tier": "large", "fragments": 4}
    assert "fragments_removed" not in result


def test_indexer_classify_splits_small_and_large(spark: SparkSession, tmp_path: Path) -> None:
    """The indexer routes a tiny dataset to the small tier and a many-fragment one to the large tier."""
    tiny: str = str(tmp_path / "tiny.lance")
    huge: str = str(tmp_path / "huge.lance")
    write_fragmented_dataset(tiny, make_vector_table(rows=20, dim=8), max_rows_per_file=20)
    write_fragmented_dataset(huge, make_vector_table(rows=100, dim=8), max_rows_per_file=20)
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig(), small_dataset_fragment_threshold=3)

    small, large = LanceIndexer(config).classify(spark, [tiny, huge])

    assert small == [tiny]
    assert large == [huge]
