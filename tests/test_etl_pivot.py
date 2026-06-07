"""Tests for the named-vector and named-text map pivot in the ETL.

The ETL pivots declared keys of the ``vectors`` and ``texts`` map columns into concrete indexable columns: a named
vector becomes a fixed-size-list column the IVF_RQ index can target and a named text field becomes a string column the
INVERTED index can target. Undeclared map keys are dropped, a declared key absent from a row yields NULL, and the
``metadata`` map stays flattened into parallel key/value arrays. These tests cover the end-to-end Spark pivot and the
``validate_schema`` safety checks (missing map column, colliding name, and invalid identifier).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
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
)

from lance_etl.arrow_types import resolve_type_map
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


def source_schema() -> StructType:
    """Return the Spark schema of the pivot test source with the three map columns.

    Returns:
        A schema carrying the routing, key, timestamp, and op columns plus the vectors, texts, and metadata maps.
    """
    return StructType(
        [
            StructField("vector_id", StringType(), False),
            StructField("org_id", StringType(), False),
            StructField("tenant_id", StringType(), False),
            StructField("namespace", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("op", StringType(), False),
            StructField("vectors", MapType(StringType(), ArrayType(FloatType())), True),
            StructField("texts", MapType(StringType(), StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )


def pivot_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build an ETL configuration that pivots a named vector and a named text field.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        A configuration declaring ``vector_fields=["vector"]`` and ``text_fields=["text"]``.
    """
    return ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        vector_fields=["vector"],
        text_fields=["text"],
        column_types=resolve_type_map({"vector": f"fixed_size_list<float32,{DIMENSION}>"}),
        num_partitions=4,
    )


class TestPivotEndToEnd:
    """The ETL pivots declared map keys into concrete indexable columns and flattens metadata."""

    def test_pivot_writes_concrete_columns(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A named vector and text become concrete typed columns while metadata stays flattened.

        Undeclared map keys are dropped and a declared key absent from a row yields NULL.
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
                "1",
                "insert",
                {"vector": vector_a, "ignored": vector_b},
                {"text": "alpha"},
                {"k": "a"},
            ),
            ("v2", "o1", "t1", "n1", "1", "insert", {"vector": vector_b}, {}, {"k": "b"}),
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table().sort_by("vector_id")
        assert table.schema.field("vector").type == pa.list_(pa.float32(), DIMENSION)
        assert table.schema.field("text").type == pa.string()
        assert "vectors" not in table.column_names
        assert "texts" not in table.column_names
        assert "metadata" not in table.column_names
        assert "metadata_keys" in table.column_names
        assert "metadata_values" in table.column_names

        by_id: dict[str, int] = {vid: i for i, vid in enumerate(table["vector_id"].to_pylist())}
        assert table["vector"].to_pylist()[by_id["v1"]] == vector_a
        assert table["text"].to_pylist()[by_id["v1"]] == "alpha"
        assert table["text"].to_pylist()[by_id["v2"]] is None
        assert table["metadata_keys"].to_pylist()[by_id["v1"]] == ["k"]
        assert table["metadata_values"].to_pylist()[by_id["v1"]] == ["a"]

    def test_pivoted_columns_are_index_eligible(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The pivoted vector column carries a fixed-size-list type and the text column a string type.

        These are exactly the column types the IVF_RQ and INVERTED index handlers require, so a tiny FTS index can be
        built directly over the pivoted text column.
        """
        config: ETLConfig = pivot_config(tmp_path, telemetry_config)
        rows: list[tuple] = [
            (
                str(i),
                "o1",
                "t1",
                "n1",
                "1",
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


class TestValidatePivot:
    """validate_schema rejects an unsafe or inconsistent pivot specification."""

    def make_source(self, columns: list[str], map_fields: dict[str, object] | None = None) -> MagicMock:
        """Build a mock DataFrame exposing the given columns and schema field types.

        Args:
            columns: The column names the mock reports.
            map_fields: Mapping of column name to its Spark data type, defaulting every map column to a string map.

        Returns:
            A mock with ``columns`` and a ``schema.fields`` list of named typed fields.
        """
        types: dict[str, object] = map_fields or {}
        source: MagicMock = MagicMock()
        source.columns = columns
        source.schema.fields = [
            StructField(name, types.get(name, StringType()), True)
            for name in columns  # type: ignore[arg-type]
        ]
        return source

    def base_columns(self) -> list[str]:
        """Return the always-required non-map columns.

        Returns:
            The key, timestamp, op, and routing columns.
        """
        return ["vector_id", "timestamp", "op", "org_id", "tenant_id", "namespace"]

    def test_missing_map_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """Declaring vector_fields without the vectors map column fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, vector_fields=["vector"])
        source: MagicMock = self.make_source(self.base_columns())
        with pytest.raises(ValueError, match="missing from the source"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_non_map_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A declared vectors column that is not a MapType fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, vector_fields=["vector"])
        source: MagicMock = self.make_source([*self.base_columns(), "vectors"], {"vectors": StringType()})
        with pytest.raises(ValueError, match="must be a MapType"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_colliding_field_name_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A pivot field name that collides with an existing column fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, vector_fields=["org_id"])
        source: MagicMock = self.make_source(
            [*self.base_columns(), "vectors"],
            {"vectors": MapType(StringType(), ArrayType(FloatType()))},
        )
        with pytest.raises(ValueError, match="collides with an existing or reserved column"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_invalid_field_name_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A pivot field name failing the identifier allowlist fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, vector_fields=["bad/name"])
        source: MagicMock = self.make_source(
            [*self.base_columns(), "vectors"],
            {"vectors": MapType(StringType(), ArrayType(FloatType()))},
        )
        with pytest.raises(ValueError, match="identifier allowlist"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_valid_pivot_passes(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A well-formed vector and text pivot specification passes validation."""
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path), telemetry=telemetry_config, vector_fields=["vector"], text_fields=["text"]
        )
        source: MagicMock = self.make_source(
            [*self.base_columns(), "vectors", "texts", "metadata"],
            {
                "vectors": MapType(StringType(), ArrayType(FloatType())),
                "texts": MapType(StringType(), StringType()),
                "metadata": MapType(StringType(), StringType()),
            },
        )
        IcebergToLanceETL(config).validate_schema(source)
