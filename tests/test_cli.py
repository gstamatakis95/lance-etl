"""Sanity tests for the command-line entry point."""

from __future__ import annotations

import pytest

from lance_etl.cli import build_parser, main, parse_key_values


def test_help_exits_zero() -> None:
    """``lance-etl --help`` exits with status 0."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0


def test_subcommand_help_exits_zero() -> None:
    """Every subcommand's ``--help`` exits with status 0."""
    for subcommand in ("etl", "compact", "index"):
        with pytest.raises(SystemExit) as exc_info:
            main([subcommand, "--help"])
        assert exc_info.value.code == 0


def test_parser_builds() -> None:
    """The argument parser constructs without error."""
    parser = build_parser()
    assert parser is not None


def test_parse_key_values() -> None:
    """Key-value pairs parse into a dictionary."""
    assert parse_key_values(["a=1", "b=two"]) == {"a": "1", "b": "two"}
    assert parse_key_values(None) == {}


REQUIRED_ETL_ARGV: list[str] = [
    "etl",
    "--table",
    "db.t",
    "--start",
    "0",
    "--end",
    "1",
    "--base-uri",
    "/tmp/lance",
]


def test_etl_has_no_ingested_at_flag() -> None:
    """The ingestion-timestamp column name is baked, so no ``--ingested-at-col`` flag is exposed."""
    args = build_parser().parse_args(REQUIRED_ETL_ARGV)
    assert not hasattr(args, "ingested_at_col")
