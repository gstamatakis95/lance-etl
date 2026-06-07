"""Tests for the ingestion-timestamp column and merge conflict-visibility metrics.

Covers the ``_ingested_at`` payload column stamped by the Spark ETL (presence, population, timestamp type, exclusion
from collapse keys and partition validation), the schema-evolution path that re-adds the column to a dataset created
before it existed, and the conflict-count bucket tag plus retry counter emitted by :func:`apply_merge`.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pyarrow.types as pat
import pytest
from pyspark.sql import SparkSession

from lance_etl import etl as etl_module
from lance_etl.etl import (
    INGESTED_AT_COLUMN,
    ETLConfig,
    IcebergToLanceETL,
    apply_merge,
    conflict_bucket,
    dataset_uri,
)
from lance_etl.telemetry import TelemetryConfig

SOURCE_DDL: str = (
    "vector_id string, org_id string, tenant_id string, namespace string, timestamp bigint, op string, "
    "vectors map<string,array<float>>, metadata map<string,string>"
)


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
        .appName("lance-etl-ingested-at-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def sample_rows() -> list[tuple]:
    """Return two insert rows routed to a single ``o1/t1/n1`` dataset.

    Returns:
        Two row tuples matching :data:`SOURCE_DDL`.
    """
    return [
        ("v1", "o1", "t1", "n1", 1, "insert", {"emb": [1.0, 2.0]}, {"k": "a"}),
        ("v2", "o1", "t1", "n1", 1, "insert", {"emb": [3.0, 4.0]}, {"k": "b"}),
    ]


class TestFreshIngest:
    """A fresh ingest stamps every routed row with a populated timestamp column."""

    def test_column_present_and_populated_as_timestamp(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The default ``_ingested_at`` column appears, is a timestamp, and carries no nulls."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=4)
        frame = spark.createDataFrame(sample_rows(), SOURCE_DDL)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table()
        assert INGESTED_AT_COLUMN in table.column_names
        stamped: pa.ChunkedArray = table[INGESTED_AT_COLUMN]
        assert pat.is_timestamp(stamped.type)
        assert stamped.null_count == 0


class TestSchemaEvolution:
    """A dataset created before the column existed gains it on the next ingest."""

    def test_existing_dataset_without_column_evolves(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Dropping the column then re-ingesting re-adds it and populates the updated rows.

        This simulates a pre-existing dataset created before the ingestion-timestamp column was introduced, exercising
        the explicit one-time ``add_columns`` evolution path in :func:`apply_merge`.
        """
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=4)
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        etl.run_on_dataframe(spark.createDataFrame(sample_rows(), SOURCE_DDL))

        uri: str = dataset_uri(config, "o1", "t1", "n1")
        dataset: lance.LanceDataset = lance.dataset(uri)
        dataset.drop_columns([INGESTED_AT_COLUMN])
        assert INGESTED_AT_COLUMN not in lance.dataset(uri).schema.names

        etl.run_on_dataframe(spark.createDataFrame(sample_rows(), SOURCE_DDL))

        table: pa.Table = lance.dataset(uri).to_table()
        assert INGESTED_AT_COLUMN in table.column_names
        stamped: pa.ChunkedArray = table[INGESTED_AT_COLUMN]
        assert pat.is_timestamp(stamped.type)
        assert stamped.null_count == 0


class TestCollapseExclusion:
    """The ingestion-timestamp column is excluded from the collapse dedup keys."""

    def test_same_key_different_ingested_at_collapses(
        self, spark: SparkSession, telemetry_config: TelemetryConfig, tmp_path: Path
    ) -> None:
        """Two rows sharing the real key collapse to one even with differing ingestion timestamps."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        collapse_ddl: str = (
            "org_id string, tenant_id string, namespace string, vector_id string, timestamp bigint, _ingested_at string"
        )
        frame = spark.createDataFrame(
            [
                ("o1", "t1", "n1", "v1", 1, "2024-01-01 00:00:00"),
                ("o1", "t1", "n1", "v1", 2, "2024-06-06 00:00:00"),
            ],
            collapse_ddl,
        )
        collapsed = etl.collapse(frame)
        rows: list[Any] = collapsed.collect()
        assert len(rows) == 1
        assert rows[0]["timestamp"] == 2


class TestRoutingExclusion:
    """The ingestion-timestamp column is never a routing or required column."""

    def test_not_required_by_validate_schema(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """validate_schema passes without the ingestion-timestamp column in the source, and routing omits it."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        source: MagicMock = MagicMock()
        source.columns = ["vector_id", "timestamp", "op", "vectors", "metadata", "org_id", "tenant_id", "namespace"]
        etl.validate_schema(source)
        assert INGESTED_AT_COLUMN not in config.routing_cols()


def fake_merge_dataset(execute_side_effect: list[Any]) -> MagicMock:
    """Build a mock dataset whose merge builder yields a controllable execute side effect.

    The builder methods all return the builder so the fluent merge chain works, the schema already carries the
    ingestion-timestamp column so the schema-evolution add is skipped, and ``execute`` replays the given side effect
    (exceptions are raised, dicts are returned) to force a controlled number of commit conflicts.

    Args:
        execute_side_effect: The ``execute`` side-effect list, mixing conflict exceptions and a final stats dict.

    Returns:
        A mock dataset ready to be returned from a patched ``lance.dataset``.
    """
    builder: MagicMock = MagicMock()
    builder.when_matched_update_all.return_value = builder
    builder.when_not_matched_insert_all.return_value = builder
    builder.conflict_retries.return_value = builder
    builder.retry_timeout.return_value = builder
    builder.execute.side_effect = execute_side_effect
    dataset: MagicMock = MagicMock()
    dataset.merge_insert.return_value = builder
    dataset.schema.names = ["vector_id", "_ingested_at"]
    return dataset


def conflict_group() -> pa.Table:
    """Return a one-row upsert group carrying the op and ingestion-timestamp columns.

    Returns:
        A table with ``vector_id``, ``op``, and ``_ingested_at`` columns.
    """
    return pa.table(
        {
            "vector_id": pa.array(["v1"]),
            "op": pa.array(["insert"]),
            "_ingested_at": pa.array([1_700_000_000_000_000], pa.timestamp("us")),
        }
    )


def distribution_tags(telemetry: MagicMock, name: str) -> list[str]:
    """Return the tags of the first distribution call with the given metric name.

    Args:
        telemetry: The mock telemetry facade.
        name: The metric name to look up.

    Returns:
        The tags passed to that distribution call.
    """
    for call in telemetry.distribution.call_args_list:
        if call.args[0] == name:
            return call.kwargs["tags"]
    raise AssertionError(f"no distribution call named {name!r}")


class TestConflictVisibility:
    """The merge timing carries a conflict bucket tag and a retry counter is emitted on conflicts."""

    def test_bucket_function(self) -> None:
        """The bucket function maps counts to the low-cardinality tag values."""
        assert conflict_bucket(0) == "0"
        assert conflict_bucket(1) == "1"
        assert conflict_bucket(2) == "2+"
        assert conflict_bucket(9) == "2+"

    def test_conflicts_tag_bucket_and_emit_counter(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """Two forced conflicts tag merge_ms with conflicts:2+ and emit the retry counter with value 2."""
        stats: dict[str, Any] = {"num_inserted_rows": 1, "num_updated_rows": 0, "num_deleted_rows": 0}
        dataset: MagicMock = fake_merge_dataset(
            [
                OSError("LanceError(IO): Commit conflict for version 7"),
                OSError("LanceError(IO): Commit conflict for version 8"),
                stats,
            ]
        )
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path), telemetry=telemetry_config, conflict_retries=5, retry_backoff_seconds=0.0
        )
        telemetry: MagicMock = MagicMock()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(etl_module.lance, "dataset", MagicMock(return_value=dataset))
            upserted, deleted = apply_merge(config, telemetry, ("o1", "t1", "n1"), conflict_group())

        assert upserted == 1
        assert deleted == 0
        assert dataset.merge_insert.return_value.execute.call_count == 3
        assert distribution_tags(telemetry, "dataset.merge_ms") == ["conflicts:2+"]
        counter_calls = [c for c in telemetry.incr.call_args_list if c.args[0] == "dataset.merge_conflict_retries"]
        assert len(counter_calls) == 1
        assert counter_calls[0].kwargs["value"] == 2
        assert counter_calls[0].kwargs["tags"] == ["conflicts:2+"]

    def test_no_conflict_bucket_zero_no_counter(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A clean merge tags merge_ms with conflicts:0 and emits no retry counter."""
        dataset: MagicMock = fake_merge_dataset(
            [{"num_inserted_rows": 1, "num_updated_rows": 0, "num_deleted_rows": 0}]
        )
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path), telemetry=telemetry_config, conflict_retries=5, retry_backoff_seconds=0.0
        )
        telemetry: MagicMock = MagicMock()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(etl_module.lance, "dataset", MagicMock(return_value=dataset))
            apply_merge(config, telemetry, ("o1", "t1", "n1"), conflict_group())

        assert distribution_tags(telemetry, "dataset.merge_ms") == ["conflicts:0"]
        counter_calls = [c for c in telemetry.incr.call_args_list if c.args[0] == "dataset.merge_conflict_retries"]
        assert counter_calls == []
