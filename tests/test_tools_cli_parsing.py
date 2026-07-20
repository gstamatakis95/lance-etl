"""Parse-level unit tests for the cliutil tag parsers and the operator tools CLI parser.

Covers :func:`lance_etl.cliutil.parse_window_tag` and :func:`lance_etl.cliutil.parse_hour_tag`
directly, and the argparse structure built by :func:`lance_etl.tools.cli.build_parser` for the
``recall``, ``migrate-namespace``, and ``optimize-iceberg`` subcommands. Every assertion here stops
at ``parser.parse_args(...)``: no subcommand handler (``run_recall``, ``run_migrate_namespace``,
``run_optimize_iceberg``) is ever invoked, and no Spark session is built.
"""

from __future__ import annotations

import argparse

import pytest

from lance_etl.cliutil import parse_hour_tag, parse_window_tag
from lance_etl.iceberg_optimize import DEFAULT_EXPIRE_OLDER_THAN_DAYS, DEFAULT_EXPIRE_RETAIN_LAST
from lance_etl.indexing import IndexJobConfig
from lance_etl.telemetry import TelemetryConfig
from lance_etl.tools.cli import build_parser


class TestParseWindowTag:
    """Unit tests for parse_window_tag."""

    def test_naive_datetime_converts_to_colon_free_utc_tag(self) -> None:
        """A naive ISO 8601 datetime is treated as UTC and formatted without colons."""
        assert parse_window_tag("2026-06-11T12:00:00") == "20260611T120000Z"

    def test_timezone_aware_datetime_converts_to_utc(self) -> None:
        """A timezone-aware datetime is converted to UTC before formatting."""
        assert parse_window_tag("2026-06-11T14:00:00+02:00") == "20260611T120000Z"

    def test_invalid_string_raises_value_error(self) -> None:
        """An unparseable string raises ValueError instead of propagating a different exception."""
        with pytest.raises(ValueError):
            parse_window_tag("not-a-datetime")


class TestParseHourTag:
    """Unit tests for parse_hour_tag."""

    def test_truncates_minute_and_second_to_zero(self) -> None:
        """Minutes, seconds, and microseconds are zeroed before formatting the hour tag."""
        assert parse_hour_tag("2026-06-11 12:34:56+00:00") == "20260611T120000Z"

    def test_timezone_aware_datetime_converts_to_utc_then_truncates(self) -> None:
        """A timezone-aware instant is converted to UTC before the hour truncation is applied."""
        assert parse_hour_tag("2026-06-11T14:34:56+02:00") == "20260611T120000Z"

    def test_invalid_string_raises_value_error(self) -> None:
        """An unparseable string raises ValueError instead of propagating a different exception."""
        with pytest.raises(ValueError):
            parse_hour_tag("not-a-datetime")


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


class TestMigrateNamespaceSubcommandParsing:
    """Parse-level tests for the migrate-namespace subcommand."""

    def test_valid_argv_parses_expected_namespace(self) -> None:
        """A minimal valid migrate-namespace invocation parses the required fields."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "migrate-namespace",
                "--source-namespace",
                "old-ns",
                "--target-namespace",
                "new-ns",
                "--base-uri",
                "s3://bucket/root",
            ]
        )
        assert namespace.command == "migrate-namespace"
        assert namespace.source_namespace == "old-ns"
        assert namespace.target_namespace == "new-ns"
        assert namespace.base_uri == "s3://bucket/root"

    def test_optional_flag_defaults_apply_when_omitted(self) -> None:
        """partition-by, no-recompact, no-reindex, overwrite-target, and metric take their defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "migrate-namespace",
                "--source-namespace",
                "old-ns",
                "--target-namespace",
                "new-ns",
                "--base-uri",
                "s3://bucket/root",
            ]
        )
        assert namespace.partition_by is None
        assert namespace.no_recompact is False
        assert namespace.no_reindex is False
        assert namespace.overwrite_target is False
        assert namespace.metric == IndexJobConfig.metric
        assert namespace.vector_column is None
        assert namespace.scalar_column is None
        assert namespace.bitmap_column is None
        assert namespace.zonemap_column is None
        assert namespace.text_column is None

    def test_store_true_flags_and_partition_by_override_defaults(self) -> None:
        """Passing the boolean flags and --partition-by flips them away from their defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "migrate-namespace",
                "--source-namespace",
                "old-ns",
                "--target-namespace",
                "new-ns",
                "--base-uri",
                "s3://bucket/root",
                "--partition-by",
                "org_id,tenant_id,namespace",
                "--no-recompact",
                "--no-reindex",
                "--overwrite-target",
            ]
        )
        assert namespace.partition_by == "org_id,tenant_id,namespace"
        assert namespace.no_recompact is True
        assert namespace.no_reindex is True
        assert namespace.overwrite_target is True

    def test_missing_required_target_namespace_raises_system_exit(self) -> None:
        """Omitting the required --target-namespace flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "migrate-namespace",
                    "--source-namespace",
                    "old-ns",
                    "--base-uri",
                    "s3://bucket/root",
                ]
            )

    def test_missing_required_base_uri_raises_system_exit(self) -> None:
        """Omitting the required --base-uri flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "migrate-namespace",
                    "--source-namespace",
                    "old-ns",
                    "--target-namespace",
                    "new-ns",
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
        """The boolean maintenance flags and the expiry tunables take their documented defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
            ]
        )
        assert namespace.no_rewrite_data_files is False
        assert namespace.no_rewrite_manifests is False
        assert namespace.expire_snapshots is False
        assert namespace.remove_orphan_files is False
        assert namespace.expire_retain_last == DEFAULT_EXPIRE_RETAIN_LAST
        assert namespace.expire_older_than_days == DEFAULT_EXPIRE_OLDER_THAN_DAYS

    def test_flags_and_expiry_tunables_override_defaults(self) -> None:
        """Supplied boolean flags and integer tunables replace their defaults."""
        namespace: argparse.Namespace = parse_tools_args(
            [
                "optimize-iceberg",
                "--table",
                "catalog.namespace.table",
                "--no-rewrite-data-files",
                "--no-rewrite-manifests",
                "--expire-snapshots",
                "--remove-orphan-files",
                "--expire-retain-last",
                "10",
                "--expire-older-than-days",
                "30",
            ]
        )
        assert namespace.no_rewrite_data_files is True
        assert namespace.no_rewrite_manifests is True
        assert namespace.expire_snapshots is True
        assert namespace.remove_orphan_files is True
        assert namespace.expire_retain_last == 10
        assert namespace.expire_older_than_days == 30

    def test_missing_required_table_raises_system_exit(self) -> None:
        """Omitting the required --table flag raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_tools_args(["optimize-iceberg"])

    def test_non_integer_expire_retain_last_raises_system_exit(self) -> None:
        """A non-integer --expire-retain-last value fails the argparse type=int conversion."""
        with pytest.raises(SystemExit):
            parse_tools_args(
                [
                    "optimize-iceberg",
                    "--table",
                    "catalog.namespace.table",
                    "--expire-retain-last",
                    "not-an-int",
                ]
            )


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
