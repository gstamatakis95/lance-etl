"""Tests confirming that no ingestion-timestamp column is written and event-time collapse is correct.

After ADR 0016, the ``_ingested_at`` column is removed. The source event timestamp (``ETLConfig.ts_col``,
default ``"timestamp"``) is the single canonical clock. These tests assert that the written dataset schema
does not carry ``_ingested_at``, that event-time last-write-wins collapse still works correctly, and that the
merge-conflict visibility metrics continue to fire as expected.
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
import pytest
from pyspark.sql import SparkSession

from lance_etl import etl as etl_module
from lance_etl.etl import (
    ETLConfig,
    IcebergToLanceETL,
    apply_merge,
    conflict_bucket,
    dataset_uri,
)
from lance_etl.telemetry import TelemetryConfig

INGESTED_AT_COLUMN: str = "_ingested_at"

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
        .appName("lance-etl-event-time-tests")
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


class TestNoIngestedAtColumn:
    """The written dataset schema does not carry the removed ingestion-timestamp column."""

    def test_column_absent_after_ingest(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A fresh ingest does not produce an ``_ingested_at`` column in the written dataset.

        This is the primary regression guard for ADR 0016: the column must not reappear.
        """
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=4)
        frame = spark.createDataFrame(sample_rows(), SOURCE_DDL)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table()
        assert INGESTED_AT_COLUMN not in table.column_names

    def test_event_timestamp_column_present(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The event timestamp column (``ts_col``) is present in the written dataset.

        The event timestamp is the single canonical clock after ADR 0016.
        """
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=4)
        frame = spark.createDataFrame(sample_rows(), SOURCE_DDL)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table()
        assert config.ts_col in table.column_names


class TestCollapseEventTime:
    """The event timestamp drives last-write-wins collapse correctly."""

    def test_same_key_higher_timestamp_wins(
        self, spark: SparkSession, telemetry_config: TelemetryConfig, tmp_path: Path
    ) -> None:
        """Two rows sharing a key collapse to the one with the higher event timestamp."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        collapse_ddl: str = (
            "org_id string, tenant_id string, namespace string, vector_id string, timestamp bigint, op string"
        )
        frame = spark.createDataFrame(
            [
                ("o1", "t1", "n1", "v1", 1, "insert"),
                ("o1", "t1", "n1", "v1", 2, "insert"),
            ],
            collapse_ddl,
        )
        collapsed = etl.collapse(frame)
        rows: list[Any] = collapsed.collect()
        assert len(rows) == 1
        assert rows[0]["timestamp"] == 2


class TestRoutingExclusion:
    """The event timestamp column is not a routing column."""

    def test_ts_col_not_in_routing_cols(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """The timestamp column is not listed in routing_cols."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        assert config.ts_col not in config.routing_cols()


def fake_merge_dataset(execute_side_effect: list[Any]) -> MagicMock:
    """Build a mock dataset whose merge builder yields a controllable execute side effect.

    The builder methods all return the builder so the fluent merge chain works, and ``execute`` replays the
    given side effect (exceptions are raised, dicts are returned) to force a controlled number of commit
    conflicts. The schema carries only ``vector_id`` so there is no ``_ingested_at`` to evolve.

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
    dataset.schema.names = ["vector_id"]
    return dataset


def conflict_group() -> pa.Table:
    """Return a one-row upsert group carrying only the key and op columns.

    Returns:
        A table with ``vector_id`` and ``op`` columns. No ``_ingested_at`` column.
    """
    return pa.table(
        {
            "vector_id": pa.array(["v1"]),
            "op": pa.array(["insert"]),
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
