"""Parse-level unit tests for the operator tools CLI parser.

Covers the argparse structure built by :func:`lance_etl.tools.cli.build_parser` for the ``recall``
and ``optimize-iceberg`` subcommands. Every assertion here stops at
``parser.parse_args(...)``. No subcommand handler is invoked and no Spark session is built.
"""

from __future__ import annotations

import argparse

import pytest

from lance_etl.telemetry import TelemetryConfig
from lance_etl.tools.cli import build_parser


def parse_tools_args(argv: list[str]) -> argparse.Namespace:
    """Parse an argument vector with the tools CLI parser.

    Args:
        argv: The argument vector after the program name.

    Returns:
        The parsed namespace.
    """
    return build_parser().parse_args(argv)


class TestRecallSubcommandParsing:
    """Parse-level tests for the recall subcommand."""

    def test_valid_argv_parses_expected_namespace(self) -> None:
        """A minimal valid recall invocation parses the required fields and their defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "recall",
                "--from",
                "2026-06-11T00:00:00",
                "--to",
                "2026-06-11T01:00:00",
                "--base-uri",
                "s3://bucket/root",
            ]
        )
        assert namespace.command == "recall"
        assert namespace.from_ts == "2026-06-11T00:00:00"
        assert namespace.to_ts == "2026-06-11T01:00:00"
        assert namespace.base_uri == "s3://bucket/root"

    def test_optional_flag_defaults_apply_when_omitted(self) -> None:
        """dd-site, max-samples, and vector-column take their documented defaults when omitted."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "recall",
                "--from",
                "2026-06-11T00:00:00",
                "--to",
                "2026-06-11T01:00:00",
                "--base-uri",
                "s3://bucket/root",
            ]
        )
        assert namespace.dd_site == "datadoghq.com"
        assert namespace.max_samples == 10_000
        assert namespace.vector_column == "vector"

    def test_max_samples_overrides_default(self) -> None:
        """A supplied --max-samples value is parsed as an int and replaces the default."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "recall",
                "--from",
                "2026-06-11T00:00:00",
                "--to",
                "2026-06-11T01:00:00",
                "--base-uri",
                "s3://bucket/root",
                "--max-samples",
                "500",
            ]
        )
        assert namespace.max_samples == 500

    def test_missing_required_base_uri_raises_system_exit(self) -> None:
        """Omitting the required --base-uri flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "recall",
                    "--from",
                    "2026-06-11T00:00:00",
                    "--to",
                    "2026-06-11T01:00:00",
                ]
            )

    def test_missing_required_from_raises_system_exit(self) -> None:
        """Omitting the required --from flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "recall",
                    "--to",
                    "2026-06-11T01:00:00",
                    "--base-uri",
                    "s3://bucket/root",
                ]
            )

    def test_non_integer_max_samples_raises_system_exit(self) -> None:
        """A non-integer --max-samples value fails the argparse type=int conversion."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "recall",
                    "--from",
                    "2026-06-11T00:00:00",
                    "--to",
                    "2026-06-11T01:00:00",
                    "--base-uri",
                    "s3://bucket/root",
                    "--max-samples",
                    "not-an-int",
                ]
            )


class TestOptimizeIcebergSubcommandParsing:
    """Parse-level tests for the optimize-iceberg subcommand."""

    def test_valid_argv_parses_expected_namespace(self) -> None:
        """A minimal valid optimize-iceberg invocation parses the required --table field."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
            ]
        )
        assert namespace.command == "optimize-iceberg"
        assert namespace.table == "catalog.namespace.table"

    def test_optional_flag_defaults_apply_when_omitted(self) -> None:
        """The boolean maintenance flags take their documented defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
            ]
        )
        assert namespace.no_rewrite_data_files is False
        assert namespace.no_rewrite_manifests is False
        assert namespace.remove_orphan_files is False

    def test_flags_override_defaults(self) -> None:
        """Supplied boolean flags replace their defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
                "--no-rewrite-data-files",
                "--no-rewrite-manifests",
                "--remove-orphan-files",
            ]
        )
        assert namespace.no_rewrite_data_files is True
        assert namespace.no_rewrite_manifests is True
        assert namespace.remove_orphan_files is True

    def test_missing_required_table_raises_system_exit(self) -> None:
        """Omitting the required --table flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(["optimize-iceberg"])

    @pytest.mark.parametrize(
        "removed_flag",
        ["--expire-snapshots", "--expire-retain-last", "--expire-older-than-days"],
    )
    def test_snapshot_expiration_flags_are_rejected(self, removed_flag: str) -> None:
        """Former age-only expiry flags cannot bypass the PostgreSQL retention gate.

        Args:
            removed_flag: Former unsafe operator flag.
        """
        argv: list[str] = ["optimize-iceberg", "--table", "catalog.namespace.table", removed_flag]
        if removed_flag != "--expire-snapshots":
            argv.append("7")
        with pytest.raises(SystemExit):
            parse_tools_args(argv)


class TestTopLevelParserDispatch:
    """Parse-level tests for the top-level command dispatch and shared defaults."""

    def test_invalid_subcommand_name_raises_system_exit(self) -> None:
        """An unknown subcommand name is rejected by argparse's subparser choice validation."""
        with pytest.raises(SystemExit):
            parse_tools_args(["not-a-real-subcommand"])

    def test_missing_subcommand_raises_system_exit(self) -> None:
        """The subcommand is required, so an empty argv raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args([])

    def test_log_level_default_applies_when_omitted(self) -> None:
        """The shared --log-level flag defaults to INFO on every subcommand."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
            ]
        )
        assert namespace.log_level == "INFO"

    def test_dd_service_env_version_defaults_apply_when_omitted(self) -> None:
        """The shared identity flags default to the TelemetryConfig field defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
            ]
        )
        assert namespace.dd_service == TelemetryConfig.service
        assert namespace.dd_env == TelemetryConfig.env
        assert namespace.dd_version == TelemetryConfig.version
