"""Dynamic write-partition routing: URI construction, validation, CLI parsing, and Spark end-to-end tests.

Covers the configurable ``ETLConfig.partition_cols`` dataset-path construction (byte-identical to the historical
``{org}/{tenant}/{namespace}.lance`` layout by default), the strftime-to-Spark format translation behind
``partition_derivations``, the ``--partition-by`` / ``--partition-derive`` CLI flags, and a four-level end-to-end run
on a local Spark session that routes a tiny DataFrame into ``{org}/{tenant}/{ns}/{event_date}.lance`` datasets.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from lance_etl.cli import build_parser, parse_partition_cols, parse_partition_derivations
from lance_etl.etl import (
    ETLConfig,
    IcebergToLanceETL,
    PartitionDerivation,
    dataset_uri,
    strftime_to_spark_format,
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


class TestStrftimeTranslation:
    """strftime patterns translate to Spark date_format patterns deterministically."""

    def test_day_pattern(self) -> None:
        """The canonical day partition pattern translates to yyyy-MM-dd."""
        assert strftime_to_spark_format("%Y-%m-%d") == "yyyy-MM-dd"

    def test_time_pattern(self) -> None:
        """Hour, minute, and second directives translate to HH:mm:ss."""
        assert strftime_to_spark_format("%H:%M:%S") == "HH:mm:ss"

    def test_literal_letters_are_quoted(self) -> None:
        """Bare literal letters are single-quoted so Spark does not treat them as pattern symbols."""
        assert strftime_to_spark_format("%YT%H") == "yyyy'T'HH"

    def test_percent_escape(self) -> None:
        """A doubled percent passes through as a literal percent."""
        assert strftime_to_spark_format("%Y%%") == "yyyy%"

    def test_unsupported_directive_raises(self) -> None:
        """An unsupported directive raises instead of silently producing wrong partitions."""
        with pytest.raises(ValueError, match="unsupported strftime directive"):
            strftime_to_spark_format("%Q")

    def test_trailing_bare_percent_raises(self) -> None:
        """A pattern ending in a bare percent raises."""
        with pytest.raises(ValueError, match="bare '%'"):
            strftime_to_spark_format("%Y-%")


class TestDatasetUri:
    """dataset_uri builds validated paths from the configured partition columns."""

    def test_default_routing_is_byte_identical(self, base_config: ETLConfig) -> None:
        """The default configuration produces the historical three-level path."""
        uri: str = dataset_uri(base_config, "org1", "tenant1", "ns1")
        assert uri == f"{base_config.base_uri}/org1/tenant1/ns1.lance"

    def test_custom_partition_cols_build_deeper_path(self, base_config: ETLConfig) -> None:
        """Four partition columns produce a four-level dataset path in list order."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "event_date"]
        uri: str = dataset_uri(base_config, "org1", "tenant1", "ns1", "2024-06-01")
        assert uri == f"{base_config.base_uri}/org1/tenant1/ns1/2024-06-01.lance"

    def test_single_partition_col(self, base_config: ETLConfig) -> None:
        """A single partition column produces a one-level path."""
        base_config.partition_cols = ["namespace"]
        assert dataset_uri(base_config, "ns1") == f"{base_config.base_uri}/ns1.lance"

    def test_component_count_mismatch_raises(self, base_config: ETLConfig) -> None:
        """Passing the wrong number of routing values raises a clear error."""
        with pytest.raises(ValueError, match="expected 3 routing components"):
            dataset_uri(base_config, "org1", "tenant1")

    def test_invalid_component_raises(self, base_config: ETLConfig) -> None:
        """A component failing path_component_pattern raises, preventing traversal."""
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

    def test_duplicate_derivation_names_raise(self, base_config: ETLConfig) -> None:
        """Duplicate derivation names are rejected when the job is built."""
        base_config.partition_derivations = [
            PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d"),
            PartitionDerivation("event_date", "processing_timestamp", "%Y"),
        ]
        with pytest.raises(ValueError, match="duplicate names"):
            IcebergToLanceETL(base_config)

    def test_bad_derivation_format_raises(self, base_config: ETLConfig) -> None:
        """A derivation with an unsupported strftime directive is rejected when the job is built."""
        base_config.partition_derivations = [PartitionDerivation("event_date", "processing_timestamp", "%Q")]
        with pytest.raises(ValueError, match="unsupported strftime directive"):
            IcebergToLanceETL(base_config)

    def test_validate_partition_spec_accepts_valid_spec(self) -> None:
        """A well-formed specification passes config-build validation."""
        validate_partition_spec(
            ["org_id", "event_date"], [PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")]
        )

    def test_missing_partition_col_fails_schema_validation(self, base_config: ETLConfig) -> None:
        """A partition column absent from the source and not derived fails validate_schema."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "event_date"]
        etl: IcebergToLanceETL = IcebergToLanceETL(base_config)
        source: MagicMock = MagicMock()
        source.columns = ["vector_id", "timestamp", "op", "vectors", "metadata", "org_id", "tenant_id", "namespace"]
        with pytest.raises(ValueError, match="event_date"):
            etl.validate_schema(source)

    def test_derived_partition_col_passes_schema_validation(self, base_config: ETLConfig) -> None:
        """A partition column produced by a derivation passes when its source column exists."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "event_date"]
        base_config.partition_derivations = [PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")]
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
            "processing_timestamp",
        ]
        etl.validate_schema(source)

    def test_missing_derivation_source_fails_schema_validation(self, base_config: ETLConfig) -> None:
        """A derivation whose source column is absent from the source fails validate_schema."""
        base_config.partition_cols = ["org_id", "tenant_id", "namespace", "event_date"]
        base_config.partition_derivations = [PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")]
        etl: IcebergToLanceETL = IcebergToLanceETL(base_config)
        source: MagicMock = MagicMock()
        source.columns = ["vector_id", "timestamp", "op", "vectors", "metadata", "org_id", "tenant_id", "namespace"]
        with pytest.raises(ValueError, match="processing_timestamp"):
            etl.validate_schema(source)


class TestCliPartitionFlags:
    """The CLI exposes --partition-by and --partition-derive on the etl subcommand."""

    def test_flags_default_to_absent(self) -> None:
        """Without the flags the namespace carries None so the configuration default applies."""
        args = build_parser().parse_args(
            ["etl", "--table", "db.t", "--start", "0", "--end", "1", "--base-uri", "s3://bucket/lance"]
        )
        assert args.partition_by is None
        assert args.partition_derive is None

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
                "org_id,tenant_id,namespace,event_date",
            ]
        )
        assert args.partition_by == "org_id,tenant_id,namespace,event_date"

    def test_partition_derive_is_repeatable(self) -> None:
        """--partition-derive accumulates repeated specs."""
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
                "--partition-derive",
                "event_date=processing_timestamp:%Y-%m-%d",
                "--partition-derive",
                "event_hour=processing_timestamp:%H",
            ]
        )
        assert args.partition_derive == [
            "event_date=processing_timestamp:%Y-%m-%d",
            "event_hour=processing_timestamp:%H",
        ]

    def test_parse_partition_cols(self) -> None:
        """Comma-separated columns parse into a trimmed list. Absent stays None."""
        assert parse_partition_cols("org_id, tenant_id ,namespace") == ["org_id", "tenant_id", "namespace"]
        assert parse_partition_cols(None) is None

    def test_parse_partition_cols_empty_raises(self) -> None:
        """An empty --partition-by value raises a clear error."""
        with pytest.raises(ValueError, match="at least one column"):
            parse_partition_cols(" , ")

    def test_parse_partition_derivations(self) -> None:
        """A well-formed spec parses into a PartitionDerivation."""
        parsed: list[PartitionDerivation] = parse_partition_derivations(["event_date=processing_timestamp:%Y-%m-%d"])
        assert parsed == [PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")]

    def test_parse_partition_derivations_requires_equals(self) -> None:
        """A spec without an equals sign raises."""
        with pytest.raises(ValueError, match="NAME=SOURCE:FORMAT"):
            parse_partition_derivations(["event_date"])

    def test_parse_partition_derivations_requires_colon(self) -> None:
        """A spec without a colon raises."""
        with pytest.raises(ValueError, match="NAME=SOURCE:FORMAT"):
            parse_partition_derivations(["event_date=processing_timestamp"])

    def test_parse_partition_derivations_rejects_empty_parts(self) -> None:
        """A spec with an empty name, source, or format raises."""
        with pytest.raises(ValueError, match="non-empty parts"):
            parse_partition_derivations(["=processing_timestamp:%Y"])


class TestSparkDerivation:
    """Derived partition columns materialize through Spark date_format before routing."""

    def test_date_derivation_produces_day_strings(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The canonical event_date derivation renders yyyy-MM-dd strings from a timestamp column.

        Midday timestamps are used so the date is stable regardless of the local-to-session timezone conversion
        Spark applies to naive datetimes.
        """
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path),
            telemetry=telemetry_config,
            partition_cols=["org_id", "tenant_id", "namespace", "event_date"],
            partition_derivations=[PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")],
        )
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        frame = spark.createDataFrame(
            [(datetime(2024, 6, 1, 12, 30),), (datetime(2024, 6, 2, 12, 5),)], "processing_timestamp timestamp"
        )
        values: list[str] = [row["event_date"] for row in etl.derive_partition_columns(frame).collect()]
        assert sorted(values) == ["2024-06-01", "2024-06-02"]


class TestFourLevelEndToEnd:
    """A tiny DataFrame routes into the correct {org}/{tenant}/{ns}/{event_date}.lance datasets."""

    def test_rows_land_in_correct_datasets(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Each row lands in exactly the dataset named by its four routing values, with the derived column stored."""
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path),
            telemetry=telemetry_config,
            partition_cols=["org_id", "tenant_id", "namespace", "event_date"],
            partition_derivations=[PartitionDerivation("event_date", "processing_timestamp", "%Y-%m-%d")],
            num_partitions=4,
        )
        schema_ddl: str = (
            "vector_id string, org_id string, tenant_id string, namespace string, timestamp bigint, op string, "
            "vectors map<string,array<float>>, metadata map<string,string>, processing_timestamp timestamp"
        )
        rows: list[tuple] = [
            ("v1", "o1", "t1", "n1", 1, "insert", {"emb": [1.0, 2.0]}, {"k": "a"}, datetime(2024, 6, 1, 12, 0)),
            ("v2", "o1", "t1", "n1", 1, "insert", {"emb": [3.0, 4.0]}, {"k": "b"}, datetime(2024, 6, 2, 12, 0)),
            ("v3", "o2", "t1", "n1", 1, "insert", {"emb": [5.0, 6.0]}, {"k": "c"}, datetime(2024, 6, 1, 12, 0)),
            ("v4", "o2", "t1", "n1", 1, "insert", {"emb": [7.0, 8.0]}, {"k": "d"}, datetime(2024, 6, 1, 12, 0)),
        ]
        frame = spark.createDataFrame(rows, schema_ddl)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        expected: dict[tuple[str, str, str, str], list[str]] = {
            ("o1", "t1", "n1", "2024-06-01"): ["v1"],
            ("o1", "t1", "n1", "2024-06-02"): ["v2"],
            ("o2", "t1", "n1", "2024-06-01"): ["v3", "v4"],
        }
        for key, vector_ids in expected.items():
            uri: str = dataset_uri(config, *key)
            table: pa.Table = lance.dataset(uri).to_table().sort_by("vector_id")
            assert table["vector_id"].to_pylist() == vector_ids
            assert table["event_date"].to_pylist() == [key[3]] * len(vector_ids)
            assert set(table["org_id"].to_pylist()) == {key[0]}
        assert not (tmp_path / "o1" / "t1" / "n1.lance").exists()
