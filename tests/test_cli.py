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
    for subcommand in ("etl", "compact", "index", "ttl", "migrate-namespace"):
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
    """No ``--ingested-at-col`` flag is exposed because the ingestion-timestamp column was removed in ADR 0016."""
    args = build_parser().parse_args(REQUIRED_ETL_ARGV)
    assert not hasattr(args, "ingested_at_col")


REQUIRED_TTL_ARGV: list[str] = [
    "ttl",
    "--retention-days",
    "30",
    "--base-uri",
    "/tmp/lance",
]


def test_ttl_parses_required_args() -> None:
    """``ttl`` subcommand parses ``--retention-days`` and ``--base-uri``."""
    args = build_parser().parse_args(REQUIRED_TTL_ARGV)
    assert args.command == "ttl"
    assert args.retention_days == 30
    assert args.base_uri == "/tmp/lance"


def test_ttl_defaults() -> None:
    """``ttl`` subcommand defaults: timestamp_column=timestamp, no_compact=False."""
    args = build_parser().parse_args(REQUIRED_TTL_ARGV)
    assert args.timestamp_column == "timestamp"
    assert args.no_compact is False


def test_ttl_no_compact_flag() -> None:
    """``--no-compact`` sets no_compact to True."""
    args = build_parser().parse_args([*REQUIRED_TTL_ARGV, "--no-compact"])
    assert args.no_compact is True


def test_ttl_custom_timestamp_column() -> None:
    """``--timestamp-column`` overrides the default column name."""
    args = build_parser().parse_args([*REQUIRED_TTL_ARGV, "--timestamp-column", "event_time"])
    assert args.timestamp_column == "event_time"


def test_ttl_requires_retention_days() -> None:
    """``ttl`` subcommand fails without ``--retention-days``."""
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["ttl", "--base-uri", "/tmp/lance"])
    assert exc_info.value.code != 0


REQUIRED_MIGRATE_NS_ARGV: list[str] = [
    "migrate-namespace",
    "--source-namespace",
    "old-ns",
    "--target-namespace",
    "new-ns",
    "--base-uri",
    "/tmp/lance",
]


def test_migrate_namespace_parses_required_args() -> None:
    """``migrate-namespace`` subcommand parses source, target, and base-uri."""
    args = build_parser().parse_args(REQUIRED_MIGRATE_NS_ARGV)
    assert args.command == "migrate-namespace"
    assert args.source_namespace == "old-ns"
    assert args.target_namespace == "new-ns"
    assert args.base_uri == "/tmp/lance"


def test_migrate_namespace_defaults() -> None:
    """``migrate-namespace`` defaults: no_recompact=False, no_reindex=False, overwrite_target=False."""
    args = build_parser().parse_args(REQUIRED_MIGRATE_NS_ARGV)
    assert args.no_recompact is False
    assert args.no_reindex is False
    assert args.overwrite_target is False
    assert args.vector_column is None
    assert args.metric == "L2"
    assert args.scalar_column is None
    assert args.bitmap_column is None
    assert args.text_column is None
    assert args.partition_by is None


def test_migrate_namespace_toggle_flags() -> None:
    """``--no-recompact``, ``--no-reindex``, and ``--overwrite-target`` toggle correctly."""
    args = build_parser().parse_args(
        [*REQUIRED_MIGRATE_NS_ARGV, "--no-recompact", "--no-reindex", "--overwrite-target"]
    )
    assert args.no_recompact is True
    assert args.no_reindex is True
    assert args.overwrite_target is True


def test_migrate_namespace_index_column_flags() -> None:
    """Index column flags are accepted and stored on the ``migrate-namespace`` namespace."""
    args = build_parser().parse_args(
        [
            *REQUIRED_MIGRATE_NS_ARGV,
            "--vector-column",
            "vec",
            "--metric",
            "cosine",
            "--scalar-column",
            "updated_at",
            "--bitmap-column",
            "category",
            "--text-column",
            "body",
        ]
    )
    assert args.vector_column == "vec"
    assert args.metric == "cosine"
    assert args.scalar_column == ["updated_at"]
    assert args.bitmap_column == ["category"]
    assert args.text_column == ["body"]


def test_migrate_namespace_requires_source_namespace() -> None:
    """``migrate-namespace`` fails without ``--source-namespace``."""
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["migrate-namespace", "--target-namespace", "new", "--base-uri", "/tmp/lance"])
    assert exc_info.value.code != 0


def test_migrate_namespace_requires_target_namespace() -> None:
    """``migrate-namespace`` fails without ``--target-namespace``."""
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["migrate-namespace", "--source-namespace", "old", "--base-uri", "/tmp/lance"])
    assert exc_info.value.code != 0


def test_migrate_namespace_requires_base_uri() -> None:
    """``migrate-namespace`` fails without ``--base-uri``."""
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["migrate-namespace", "--source-namespace", "old", "--target-namespace", "new"])
    assert exc_info.value.code != 0
