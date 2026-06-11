"""Tests for the dynamic map-column pivot in the ETL and the collapse last-write-wins guarantee.

Every key of the ``vectors``, ``texts``, and ``metadata`` map columns becomes a concrete column in
the written Lance dataset. The key is the column name and the value is the column value. The pivot
runs per-dataset on the executor so each org's dataset contains only the keys that org uses. These
tests cover the end-to-end Spark pivot and the ``validate_schema`` safety checks (present-but-wrong
map column type, and a map column that is not a map).

The ``TestCollapseGuarantee`` class verifies that ``IcebergToLanceETL.collapse`` enforces at most one
row per ``(routing key, vector_id)`` before rows reach ``apply_merge``. This is the invariant that
makes chunked merge commits order-safe: because collapse runs before partitioning, no key can appear
in more than one chunk when ``merge_batch_rows`` slices the upsert table.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
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

from lance_etl.etl import ETLConfig, IcebergToLanceETL, dataset_uri
from lance_etl.telemetry import TelemetryConfig

DIMENSION: int = 8


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
        .appName("lance-etl-pivot-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


TS: datetime = datetime(2024, 1, 1, tzinfo=UTC)


def source_schema() -> StructType:
    """Return the Spark schema of the pivot test source with the three map columns.

    Returns:
        A schema carrying the routing, key, timestamp, window, and op columns plus the vectors,
        texts, and metadata maps, matching the SQL contract column types.
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


def pivot_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build an ETL configuration that exercises the dynamic map pivot.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        An ETLConfig using all defaults. The ``vectors``, ``texts``, and ``metadata`` map columns are
        pivoted dynamically: every key present in the data becomes a concrete column with its type
        inferred from the Arrow data.
    """
    return ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        num_partitions=4,
    )


class TestPivotEndToEnd:
    """The ETL pivots all map keys into concrete indexable columns."""

    def test_pivot_writes_concrete_columns(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Every distinct map key becomes a concrete typed column in the written dataset.

        Vector keys land as fixed-size-list columns, text keys as string columns, and metadata
        keys as string columns. The map columns themselves are not written to Lance. A key absent
        from a row yields NULL for that column in that row.
        """
        config: ETLConfig = pivot_config(tmp_path, telemetry_config)
        vector_a: list[float] = [float(i) for i in range(DIMENSION)]
        vector_b: list[float] = [float(i + 1) for i in range(DIMENSION)]
        rows: list[tuple] = [
            (
                "v1",
                "o1",
                "t1",
                "n1",
                TS,
                TS,
                "insert",
                {"vector": vector_a, "ignored": vector_b},
                {"text": "alpha"},
                {"k": "a"},
            ),
            ("v2", "o1", "t1", "n1", TS, TS, "insert", {"vector": vector_b}, {}, {"k": "b"}),
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table().sort_by("vector_id")
        assert table.schema.field("vector").type == pa.list_(pa.float32(), DIMENSION)
        assert table.schema.field("text").type == pa.string()
        assert table.schema.field("k").type == pa.string()
        assert "vectors" not in table.column_names
        assert "texts" not in table.column_names
        assert "metadata" not in table.column_names
        assert "metadata_keys" not in table.column_names
        assert "metadata_values" not in table.column_names

        by_id: dict[str, int] = {vid: i for i, vid in enumerate(table["vector_id"].to_pylist())}
        assert table["vector"].to_pylist()[by_id["v1"]] == vector_a
        assert table["text"].to_pylist()[by_id["v1"]] == "alpha"
        assert table["text"].to_pylist()[by_id["v2"]] is None
        assert table["k"].to_pylist()[by_id["v1"]] == "a"
        assert table["k"].to_pylist()[by_id["v2"]] == "b"

    def test_pivoted_columns_are_index_eligible(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The pivoted vector column carries a fixed-size-list type and the text column a string type.

        These are exactly the column types the IVF_RQ and INVERTED index handlers require, so a
        tiny FTS index can be built directly over the pivoted text column.
        """
        config: ETLConfig = pivot_config(tmp_path, telemetry_config)
        rows: list[tuple] = [
            (
                str(i),
                "o1",
                "t1",
                "n1",
                TS,
                TS,
                "insert",
                {"vector": [float(i)] * DIMENSION},
                {"text": f"word{i} common"},
                {},
            )
            for i in range(8)
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)

        dataset: lance.LanceDataset = lance.dataset(dataset_uri(config, "o1", "t1", "n1"))
        assert dataset.schema.field("vector").type == pa.list_(pa.float32(), DIMENSION)
        assert dataset.schema.field("text").type == pa.string()
        dataset.create_scalar_index("text", "INVERTED")
        names: set[str] = {description.name for description in dataset.describe_indices()}
        assert "text_idx" in names


class TestValidateSchema:
    """validate_schema rejects a map column that is present but not a MapType."""

    TIMESTAMP_COLS: set[str] = {"event_timestamp", "processing_timestamp"}

    def make_source(self, columns: list[str], map_fields: dict[str, object] | None = None) -> MagicMock:
        """Build a mock DataFrame exposing the given columns and schema field types.

        Timestamp columns (``event_timestamp``, ``processing_timestamp``) default to
        ``TimestampType`` so the schema satisfies the SQL contract. All other columns default to
        ``StringType`` unless overridden via ``map_fields``.

        Args:
            columns: The column names the mock reports.
            map_fields: Mapping of column name to its Spark data type, overriding the defaults.

        Returns:
            A mock with ``columns`` and a ``schema.fields`` list of named typed fields.
        """
        types: dict[str, object] = map_fields or {}
        source: MagicMock = MagicMock()
        source.columns = columns
        source.schema.fields = [
            StructField(
                name,
                types.get(name, TimestampType() if name in self.TIMESTAMP_COLS else StringType()),
                True,
            )
            for name in columns  # type: ignore[arg-type]
        ]
        return source

    def base_columns(self) -> list[str]:
        """Return the always-required non-map columns.

        Returns:
            The key, timestamp, op, window, and routing columns matching the SQL contract defaults.
        """
        return [
            "vector_id",
            "event_timestamp",
            "processing_timestamp",
            "op",
            "org_id",
            "tenant_id",
            "namespace",
        ]

    def test_non_map_vectors_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A vectors column present in the source but not a MapType fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source([*self.base_columns(), "vectors"], {"vectors": StringType()})
        with pytest.raises(ValueError, match="must be a MapType"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_non_map_metadata_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A metadata column present but not a MapType fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source([*self.base_columns(), "metadata"], {"metadata": StringType()})
        with pytest.raises(ValueError, match="must be a MapType"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_absent_map_column_passes(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A map column absent from the source is allowed; the pivot step skips it gracefully."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source(self.base_columns())
        IcebergToLanceETL(config).validate_schema(source)

    def test_valid_map_columns_pass(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """All three map columns present as MapType passes validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source(
            [*self.base_columns(), "vectors", "texts", "metadata"],
            {
                "vectors": MapType(StringType(), ArrayType(FloatType())),
                "texts": MapType(StringType(), StringType()),
                "metadata": MapType(StringType(), StringType()),
            },
        )
        IcebergToLanceETL(config).validate_schema(source)


class TestCollapseGuarantee:
    """collapse() enforces last-write-wins uniqueness per (routing key, vector_id) before apply_merge."""

    def collapse_schema(self) -> StructType:
        """Return the Spark schema used by collapse uniqueness tests.

        Returns:
            A schema with routing, key, event and processing timestamps, op, and a texts map.
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
                StructField("texts", MapType(StringType(), StringType()), True),
            ]
        )

    def test_duplicate_key_collapses_to_most_recent(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Two rows for the same vector_id collapse to the most-recent event_timestamp row.

        This validates the order-safety invariant: because collapse runs before any partitioning,
        no key can appear in more than one chunk when merge_batch_rows slices the upsert table.
        The older row is discarded and the newer row's payload is written to the dataset.
        """
        ts_old: datetime = datetime(2024, 1, 1, tzinfo=UTC)
        ts_new: datetime = datetime(2024, 6, 1, tzinfo=UTC)
        rows: list[tuple] = [
            ("dup_id", "o1", "t1", "ns1", ts_old, ts_old, "insert", {"txt": "old"}),
            ("dup_id", "o1", "t1", "ns1", ts_new, ts_new, "insert", {"txt": "new"}),
            ("unique_id", "o1", "t1", "ns1", ts_new, ts_new, "insert", {"txt": "only"}),
        ]
        frame = spark.createDataFrame(rows, self.collapse_schema())
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=2)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "ns1")).to_table().sort_by("vector_id")
        assert table.num_rows == 2, f"expected 2 rows after collapse, got {table.num_rows}"
        vids: list[str] = table["vector_id"].to_pylist()
        assert vids == ["dup_id", "unique_id"]
        txt_values: list[str | None] = table["txt"].to_pylist()
        txt_by_id: dict[str, str | None] = dict(zip(vids, txt_values, strict=True))
        assert txt_by_id["dup_id"] == "new", "collapse must keep the most-recent row"

    def test_unique_keys_pass_through_unchanged(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """When all vector_ids are distinct, collapse does not drop any row.

        A group with N unique vector_ids must produce exactly N rows in the written dataset.
        """
        ts: datetime = datetime(2024, 3, 15, tzinfo=UTC)
        rows: list[tuple] = [(f"v{i}", "o2", "t2", "ns2", ts, ts, "insert", {"txt": f"val{i}"}) for i in range(10)]
        frame = spark.createDataFrame(rows, self.collapse_schema())
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=2)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        dataset = lance.dataset(dataset_uri(config, "o2", "t2", "ns2"))
        assert dataset.count_rows() == 10
