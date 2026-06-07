"""Dynamic write-partition routing: URI construction, validation, CLI parsing, and a Spark end-to-end test.

Covers the configurable ``ETLConfig.partition_cols`` dataset-path construction (byte-identical to the historical
``{org}/{tenant}/{namespace}.lance`` layout by default), partition-spec validation, the ``--partition-by`` CLI flag,
and a four-level end-to-end run on a local Spark session that routes a tiny DataFrame into
``{org}/{tenant}/{ns}/{region}.lance`` datasets using static partition columns only.
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

from lance_etl.cli import build_parser, parse_partition_cols
from lance_etl.etl import (
    ETLConfig,
    IcebergToLanceETL,
    dataset_uri,
    validate_partition_spec,
)
from lance_etl.telemetry import TelemetryConfig


@pytest.fixture
def base_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build a default-routing ETL configuration rooted at a temporary directory.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        The ETL configuration with default partition columns.
    """
    return ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)


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
        .appName("lance-etl-partition-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestDatasetUri:
    """dataset_uri builds validated paths from the configured partition columns."""

    def test_default_routing_is_byte_identical(self, base_config: ETLConfig) -> None:
        """The default configuration produces the historical three-level path."""
        uri: str = dataset_uri(base_config, "org1", "tenant1", "ns1")
        assert uri == f"{base_config.base_uri}/org1/tenant1/ns1.lance"

    def test_custom_partition_cols_build_deeper_path(self, base_config: ETLConfig) -> None:
        """Four partition columns produce a four-level dataset path in list order."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "region"]
        uri: str = dataset_uri(base_config, "org1", "tenant1", "ns1", "us-east-1")
        assert uri == f"{base_config.base_uri}/org1/tenant1/ns1/us-east-1.lance"

    def test_single_partition_col(self, base_config: ETLConfig) -> None:
        """A single partition column produces a one-level path."""
        base_config.partition_cols = ["namespace"]
        assert dataset_uri(base_config, "ns1") == f"{base_config.base_uri}/ns1.lance"

    def test_component_count_mismatch_raises(self, base_config: ETLConfig) -> None:
        """Passing the wrong number of routing values raises a clear error."""
        with pytest.raises(ValueError, match="expected 3 routing components"):
            dataset_uri(base_config, "org1", "tenant1")

    def test_invalid_component_raises(self, base_config: ETLConfig) -> None:
        """A component failing the path-component pattern raises, preventing traversal."""
        with pytest.raises(ValueError, match="invalid routing component"):
            dataset_uri(base_config, "org1", "tenant/1", "ns1")

    def test_null_component_raises(self, base_config: ETLConfig) -> None:
        """A null routing value raises instead of building a broken path."""
        with pytest.raises(ValueError, match="invalid routing component"):
            dataset_uri(base_config, "org1", None, "ns1")


class TestRoutingCols:
    """routing_cols mirrors partition_cols, the only routing model."""

    def test_default_returns_trio(self, base_config: ETLConfig) -> None:
        """The unmodified default yields the historical trio."""
        assert base_config.routing_cols() == ["org_id", "tenant_id", "namespace"]

    def test_explicit_partition_cols_are_returned(self, base_config: ETLConfig) -> None:
        """An explicit partition_cols list is returned verbatim in path order."""
        base_config.partition_cols = ["region", "namespace"]
        assert base_config.routing_cols() == ["region", "namespace"]


class TestPartitionValidation:
    """Partition specifications are validated at configuration-build time and against the source schema."""

    def test_empty_partition_cols_raises(self, base_config: ETLConfig) -> None:
        """An empty partition column list is rejected when the job is built."""
        base_config.partition_cols = []
        with pytest.raises(ValueError, match="at least one column"):
            IcebergToLanceETL(base_config)

    def test_duplicate_partition_cols_raise(self, base_config: ETLConfig) -> None:
        """Duplicate partition columns are rejected when the job is built."""
        base_config.partition_cols = ["org_id", "org_id"]
        with pytest.raises(ValueError, match="duplicate columns"):
            IcebergToLanceETL(base_config)

    def test_validate_partition_spec_accepts_valid_spec(self) -> None:
        """A well-formed specification passes config-build validation."""
        validate_partition_spec(["org_id", "region"])

    def test_missing_partition_col_fails_schema_validation(self, base_config: ETLConfig) -> None:
        """A partition column absent from the source fails validate_schema."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "region"]
        etl: IcebergToLanceETL = IcebergToLanceETL(base_config)
        source: MagicMock = MagicMock()
        source.columns = ["vector_id", "timestamp", "op", "vectors", "metadata", "org_id", "tenant_id", "namespace"]
        with pytest.raises(ValueError, match="region"):
            etl.validate_schema(source)

    def test_static_partition_col_passes_schema_validation(self, base_config: ETLConfig) -> None:
        """A partition column present in the source passes validate_schema."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "region"]
        etl: IcebergToLanceETL = IcebergToLanceETL(base_config)
        source: MagicMock = MagicMock()
        source.columns = [
            "vector_id",
            "timestamp",
            "op",
            "vectors",
            "metadata",
            "org_id",
            "tenant_id",
            "namespace",
            "region",
        ]
        etl.validate_schema(source)


class TestCliPartitionFlags:
    """The CLI exposes --partition-by on the etl subcommand."""

    def test_flags_default_to_absent(self) -> None:
        """Without the flag the namespace carries None so the configuration default applies."""
        args = build_parser().parse_args(
            ["etl", "--table", "db.t", "--start", "0", "--end", "1", "--base-uri", "s3://bucket/lance"]
        )
        assert args.partition_by is None

    def test_partition_by_is_parsed(self) -> None:
        """--partition-by lands on the namespace verbatim."""
        args = build_parser().parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "0",
                "--end",
                "1",
                "--base-uri",
                "s3://bucket/lance",
                "--partition-by",
                "org_id,tenant_id,namespace,region",
            ]
        )
        assert args.partition_by == "org_id,tenant_id,namespace,region"

    def test_no_partition_derive_flag(self) -> None:
        """The by-date --partition-derive flag is gone."""
        args = build_parser().parse_args(
            ["etl", "--table", "db.t", "--start", "0", "--end", "1", "--base-uri", "s3://bucket/lance"]
        )
        assert not hasattr(args, "partition_derive")

    def test_parse_partition_cols(self) -> None:
        """Comma-separated columns parse into a trimmed list. Absent stays None."""
        assert parse_partition_cols("org_id, tenant_id ,namespace") == ["org_id", "tenant_id", "namespace"]
        assert parse_partition_cols(None) is None

    def test_parse_partition_cols_empty_raises(self) -> None:
        """An empty --partition-by value raises a clear error."""
        with pytest.raises(ValueError, match="at least one column"):
            parse_partition_cols(" , ")


class TestFourLevelEndToEnd:
    """A tiny DataFrame routes into the correct {org}/{tenant}/{ns}/{region}.lance datasets via static columns."""

    def test_rows_land_in_correct_datasets(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Each row lands in exactly the dataset named by its four static routing values."""
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path),
            telemetry=telemetry_config,
            partition_cols=["org_id", "tenant_id", "namespace", "region"],
            num_partitions=4,
        )
        schema_ddl: str = (
            "vector_id string, org_id string, tenant_id string, namespace string, timestamp bigint, op string, "
            "vectors map<string,array<float>>, metadata map<string,string>, region string"
        )
        rows: list[tuple] = [
            ("v1", "o1", "t1", "n1", 1, "insert", {"emb": [1.0, 2.0]}, {"k": "a"}, "us-east-1"),
            ("v2", "o1", "t1", "n1", 1, "insert", {"emb": [3.0, 4.0]}, {"k": "b"}, "us-west-2"),
            ("v3", "o2", "t1", "n1", 1, "insert", {"emb": [5.0, 6.0]}, {"k": "c"}, "us-east-1"),
            ("v4", "o2", "t1", "n1", 1, "insert", {"emb": [7.0, 8.0]}, {"k": "d"}, "us-east-1"),
        ]
        frame = spark.createDataFrame(rows, schema_ddl)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        expected: dict[tuple[str, str, str, str], list[str]] = {
            ("o1", "t1", "n1", "us-east-1"): ["v1"],
            ("o1", "t1", "n1", "us-west-2"): ["v2"],
            ("o2", "t1", "n1", "us-east-1"): ["v3", "v4"],
        }
        for key, vector_ids in expected.items():
            uri: str = dataset_uri(config, *key)
            table: pa.Table = lance.dataset(uri).to_table().sort_by("vector_id")
            assert table["vector_id"].to_pylist() == vector_ids
            assert table["region"].to_pylist() == [key[3]] * len(vector_ids)
            assert set(table["org_id"].to_pylist()) == {key[0]}
        assert not (tmp_path / "o1" / "t1" / "n1.lance").exists()
