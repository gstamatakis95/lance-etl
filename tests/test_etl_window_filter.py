"""Unit tests for the timestamp window filter on IcebergToLanceETL.

Covers ETLConfig field defaults, the no-op path (no bounds configured), and the CLI flag wiring so the new
``--window-start`` / ``--window-end`` / ``--window-column`` arguments reach ETLConfig correctly.

The ``apply_window_filter`` method applies Spark DataFrame filters, so the tests that exercise it use a lightweight
mock DataFrame that records which filter predicates are accumulated.  This avoids starting a full Spark session while
still verifying that the correct Column expressions are produced for both bounds.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lance_etl.cli import build_parser
from lance_etl.etl import ETLConfig, IcebergToLanceETL
from lance_etl.telemetry import TelemetryConfig


@pytest.fixture
def base_etl_config(tmp_path: Path) -> ETLConfig:
    """Return a minimal ETLConfig rooted at a temporary directory.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        An ETLConfig with default window settings (no filter).
    """
    return ETLConfig(
        base_uri=str(tmp_path),
        telemetry=TelemetryConfig(service="test", env="test"),
    )


class TestETLConfigWindowDefaults:
    """ETLConfig carries the correct defaults for the new window fields."""

    def test_window_start_defaults_to_none(self, base_etl_config: ETLConfig) -> None:
        """window_start is None by default (open lower bound)."""
        assert base_etl_config.window_start is None

    def test_window_end_defaults_to_none(self, base_etl_config: ETLConfig) -> None:
        """window_end is None by default (open upper bound)."""
        assert base_etl_config.window_end is None

    def test_window_column_defaults_to_updated_at(self, base_etl_config: ETLConfig) -> None:
        """window_column defaults to 'updated_at'."""
        assert base_etl_config.window_column == "updated_at"

    def test_window_bounds_can_be_set(self, base_etl_config: ETLConfig) -> None:
        """window_start and window_end can be set to ISO-8601 strings."""
        config: ETLConfig = replace(
            base_etl_config,
            window_start="2024-01-01T00:00:00Z",
            window_end="2024-01-02T00:00:00Z",
        )
        assert config.window_start == "2024-01-01T00:00:00Z"
        assert config.window_end == "2024-01-02T00:00:00Z"

    def test_window_column_can_be_overridden(self, base_etl_config: ETLConfig) -> None:
        """window_column can be overridden to a custom column name."""
        config: ETLConfig = replace(base_etl_config, window_column="event_time")
        assert config.window_column == "event_time"


class TestApplyWindowFilterNoop:
    """apply_window_filter returns the DataFrame unchanged when no bounds are set."""

    def test_no_bounds_returns_source_unchanged(self, base_etl_config: ETLConfig) -> None:
        """With neither bound set the original DataFrame object is returned without filter calls."""
        etl: IcebergToLanceETL = IcebergToLanceETL(base_etl_config)
        mock_df: MagicMock = MagicMock()
        result = etl.apply_window_filter(mock_df)
        assert result is mock_df
        mock_df.filter.assert_not_called()

    def test_only_window_start_applies_one_filter(self, base_etl_config: ETLConfig) -> None:
        """With only window_start set exactly one filter call is made with a lower-bound predicate."""
        config: ETLConfig = replace(base_etl_config, window_start="2024-06-01T00:00:00Z", window_column="updated_at")
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        mock_df: MagicMock = MagicMock()
        mock_df.filter.return_value = mock_df
        etl.apply_window_filter(mock_df)
        assert mock_df.filter.call_count == 1
        predicate: str = mock_df.filter.call_args[0][0]
        assert ">=" in predicate
        assert "updated_at" in predicate
        assert "2024-06-01T00:00:00Z" in predicate

    def test_only_window_end_applies_one_filter(self, base_etl_config: ETLConfig) -> None:
        """With only window_end set exactly one filter call is made with an upper-bound predicate."""
        config: ETLConfig = replace(base_etl_config, window_end="2024-06-02T00:00:00Z", window_column="updated_at")
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        mock_df: MagicMock = MagicMock()
        mock_df.filter.return_value = mock_df
        etl.apply_window_filter(mock_df)
        assert mock_df.filter.call_count == 1
        predicate: str = mock_df.filter.call_args[0][0]
        assert "<" in predicate
        assert "updated_at" in predicate
        assert "2024-06-02T00:00:00Z" in predicate

    def test_both_bounds_applies_two_filters(self, base_etl_config: ETLConfig) -> None:
        """With both bounds set two filter calls are chained and the result DataFrame is returned."""
        config: ETLConfig = replace(
            base_etl_config,
            window_start="2024-06-01T00:00:00Z",
            window_end="2024-06-02T00:00:00Z",
            window_column="event_ts",
        )
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        mock_df: MagicMock = MagicMock()
        mock_df.filter.return_value = mock_df
        result = etl.apply_window_filter(mock_df)
        assert mock_df.filter.call_count == 2
        assert result is mock_df
        predicates: list[str] = [mock_df.filter.call_args_list[i][0][0] for i in range(2)]
        assert any(">=" in p and "2024-06-01T00:00:00Z" in p for p in predicates)
        assert any("<" in p and "2024-06-02T00:00:00Z" in p for p in predicates)

    def test_custom_window_column_appears_in_predicate(self, base_etl_config: ETLConfig) -> None:
        """The window_column name is used verbatim in the generated filter predicate."""
        config: ETLConfig = replace(
            base_etl_config,
            window_start="2024-06-01T00:00:00Z",
            window_column="created_at",
        )
        etl: IcebergToLanceETL = IcebergToLanceETL(config)
        mock_df: MagicMock = MagicMock()
        mock_df.filter.return_value = mock_df
        etl.apply_window_filter(mock_df)
        predicate: str = mock_df.filter.call_args[0][0]
        assert "created_at" in predicate


class TestCLIWindowFlags:
    """The CLI parser exposes --window-start, --window-end, and --window-column on the etl subcommand."""

    def test_window_flags_have_correct_defaults(self) -> None:
        """Parsing without window flags yields None/updated_at defaults."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2024-01-02T00:00:00Z",
                "--base-uri",
                "s3://bucket/lance",
            ]
        )
        assert args.window_start is None
        assert args.window_end is None
        assert args.window_column == "updated_at"

    def test_window_start_is_parsed(self) -> None:
        """--window-start is accepted and stored on the namespace."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2024-01-02T00:00:00Z",
                "--base-uri",
                "s3://bucket/lance",
                "--window-start",
                "2024-06-01T00:00:00Z",
            ]
        )
        assert args.window_start == "2024-06-01T00:00:00Z"

    def test_window_end_is_parsed(self) -> None:
        """--window-end is accepted and stored on the namespace."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2024-01-02T00:00:00Z",
                "--base-uri",
                "s3://bucket/lance",
                "--window-end",
                "2024-06-02T00:00:00Z",
            ]
        )
        assert args.window_end == "2024-06-02T00:00:00Z"

    def test_window_column_is_parsed(self) -> None:
        """--window-column overrides the default column name."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2024-01-02T00:00:00Z",
                "--base-uri",
                "s3://bucket/lance",
                "--window-column",
                "event_time",
            ]
        )
        assert args.window_column == "event_time"

    def test_all_window_flags_together(self) -> None:
        """All three window flags can be supplied together."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "etl",
                "--table",
                "db.t",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2024-01-02T00:00:00Z",
                "--base-uri",
                "s3://bucket/lance",
                "--window-start",
                "2024-06-01T00:00:00Z",
                "--window-end",
                "2024-06-02T00:00:00Z",
                "--window-column",
                "created_at",
            ]
        )
        assert args.window_start == "2024-06-01T00:00:00Z"
        assert args.window_end == "2024-06-02T00:00:00Z"
        assert args.window_column == "created_at"

    def test_window_flags_not_present_on_compact(self) -> None:
        """The compact subcommand does not expose window flags."""
        parser = build_parser()
        args = parser.parse_args(["compact", "--datasets-file", "/tmp/ds.txt"])
        assert not hasattr(args, "window_start")
        assert not hasattr(args, "window_end")
        assert not hasattr(args, "window_column")

    def test_window_flags_not_present_on_index(self) -> None:
        """The index subcommand does not expose window flags."""
        parser = build_parser()
        args = parser.parse_args(["index", "--datasets-file", "/tmp/ds.txt"])
        assert not hasattr(args, "window_start")
        assert not hasattr(args, "window_end")
        assert not hasattr(args, "window_column")
