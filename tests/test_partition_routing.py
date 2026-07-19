"""Dataset URI construction and CLI partition-flag parsing.

Covers the fixed three-level dataset-path construction (byte-identical to the historical
``{org}/{tenant}/{namespace}.lance`` layout), validation of component values, and the
``parse_partition_cols`` helper used by the ``migrate-namespace`` subcommand.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import lance_etl.tools.cli as tools_cli
from lance_etl.cliutil import parse_partition_cols
from lance_etl.etl import (
    ETLConfig,
    dataset_uri,
)
from lance_etl.telemetry import TelemetryConfig


@pytest.fixture
def base_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build a default-routing ETL configuration rooted at a temporary directory.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        The ETL configuration with default routing columns.
    """
    return ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)


class TestDatasetUri:
    """dataset_uri builds validated paths from the fixed routing columns."""

    def test_default_routing_is_byte_identical(self, base_config: ETLConfig) -> None:
        """The default configuration produces the historical three-level path."""
        uri: str = dataset_uri(base_config, "org1", "tenant1", "ns1")
        assert uri == f"{base_config.base_uri}/org1/tenant1/ns1.lance"

    def test_null_component_raises(self, base_config: ETLConfig) -> None:
        """A null routing value raises instead of building a broken path."""
        with pytest.raises(ValueError, match="invalid routing component"):
            dataset_uri(base_config, "org1", None, "ns1")


class TestCliPartitionFlags:
    """The CLI exposes --partition-by on the migrate-namespace subcommand only."""

    def test_migrate_namespace_partition_by_defaults_to_absent(self) -> None:
        """Without the flag, migrate-namespace carries None so the ROUTING_COLS default applies."""
        args = tools_cli.build_parser().parse_args(
            [
                "migrate-namespace",
                "--source-namespace",
                "old",
                "--target-namespace",
                "new",
                "--base-uri",
                "s3://bucket/lance",
            ]
        )
        assert args.partition_by is None

    def test_migrate_namespace_partition_by_is_parsed(self) -> None:
        """--partition-by on migrate-namespace lands on the namespace verbatim."""
        args = tools_cli.build_parser().parse_args(
            [
                "migrate-namespace",
                "--source-namespace",
                "old",
                "--target-namespace",
                "new",
                "--base-uri",
                "s3://bucket/lance",
                "--partition-by",
                "org_id,tenant_id,namespace,region",
            ]
        )
        assert args.partition_by == "org_id,tenant_id,namespace,region"

    def test_parse_partition_cols(self) -> None:
        """Comma-separated columns parse into a trimmed list. Absent stays None."""
        assert parse_partition_cols("org_id, tenant_id ,namespace") == ["org_id", "tenant_id", "namespace"]
        assert parse_partition_cols(None) is None

    def test_parse_partition_cols_empty_raises(self) -> None:
        """An empty --partition-by value raises a clear error."""
        with pytest.raises(ValueError, match="at least one column"):
            parse_partition_cols(" , ")
