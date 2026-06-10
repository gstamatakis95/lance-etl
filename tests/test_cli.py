"""Sanity tests for the per-job command-line entry points.

The aggregate ``lance_etl.cli`` dispatcher no longer exists. Tests are mapped to the four
per-job CLIs: ``lance_etl.etl.cli``, ``lance_etl.maintenance.cli``, ``lance_etl.indexing.cli``,
and ``lance_etl.tools.cli``.  Pure helpers (``parse_key_values``) live in ``lance_etl.cliutil``.
"""

from __future__ import annotations

import pytest

import lance_etl.etl.cli as etl_cli
import lance_etl.indexing.cli as indexing_cli
import lance_etl.maintenance.cli as maintenance_cli
import lance_etl.tools.cli as tools_cli
from lance_etl.cliutil import parse_key_values


def test_help_exits_zero() -> None:
    """Each per-job CLI exits with status 0 when passed ``--help``."""
    for cli_main in (etl_cli.main, indexing_cli.main, tools_cli.main):
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["--help"])
        assert exc_info.value.code == 0


def test_etl_parser_builds() -> None:
    """The ETL argument parser constructs without error."""
    parser = etl_cli.build_parser()
    assert parser is not None


def test_maintenance_parser_builds() -> None:
    """The maintenance argument parser constructs without error."""
    parser = maintenance_cli.build_parser()
    assert parser is not None


def test_indexing_parser_builds() -> None:
    """The indexing argument parser constructs without error."""
    parser = indexing_cli.build_parser()
    assert parser is not None


def test_tools_parser_builds() -> None:
    """The tools argument parser constructs without error."""
    parser = tools_cli.build_parser()
    assert parser is not None


def test_parse_key_values() -> None:
    """Key-value pairs parse into a dictionary."""
    assert parse_key_values(["a=1", "b=two"]) == {"a": "1", "b": "two"}
    assert parse_key_values(None) == {}


REQUIRED_ETL_ARGV: list[str] = [
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
    args = etl_cli.build_parser().parse_args(REQUIRED_ETL_ARGV)
    assert not hasattr(args, "ingested_at_col")


REQUIRED_MAINTENANCE_RUN_ARGV: list[str] = [
    "run",
    "--base-uri",
    "/tmp/lance",
]


def test_maintenance_parses_required_args() -> None:
    """``maintenance run`` subcommand parses dataset selection."""
    args = maintenance_cli.build_parser().parse_args(REQUIRED_MAINTENANCE_RUN_ARGV)
    assert args.command == "run"
    assert args.base_uri == "/tmp/lance"


def test_maintenance_ttl_defaults_off() -> None:
    """``maintenance run`` defaults: ttl_column=None (off), ts_column=event_timestamp."""
    args = maintenance_cli.build_parser().parse_args(REQUIRED_MAINTENANCE_RUN_ARGV)
    assert args.ttl_column is None
    assert args.ts_column == "event_timestamp"


def test_maintenance_ttl_column_flag() -> None:
    """``--ttl-column`` turns on per-row TTL and ``--ts-column`` overrides the clock column."""
    args = maintenance_cli.build_parser().parse_args(
        [*REQUIRED_MAINTENANCE_RUN_ARGV, "--ttl-column", "ttl", "--ts-column", "event_time"]
    )
    assert args.ttl_column == "ttl"
    assert args.ts_column == "event_time"


def test_ttl_subcommand_removed() -> None:
    """The standalone ``ttl`` subcommand no longer exists; TTL is folded into ``maintenance run``."""
    with pytest.raises(SystemExit) as exc_info:
        maintenance_cli.build_parser().parse_args(["ttl", "--base-uri", "/tmp/lance"])
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
    args = tools_cli.build_parser().parse_args(REQUIRED_MIGRATE_NS_ARGV)
    assert args.command == "migrate-namespace"
    assert args.source_namespace == "old-ns"
    assert args.target_namespace == "new-ns"
    assert args.base_uri == "/tmp/lance"


def test_migrate_namespace_defaults() -> None:
    """``migrate-namespace`` defaults: no_recompact=False, no_reindex=False, overwrite_target=False."""
    args = tools_cli.build_parser().parse_args(REQUIRED_MIGRATE_NS_ARGV)
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
    args = tools_cli.build_parser().parse_args(
        [*REQUIRED_MIGRATE_NS_ARGV, "--no-recompact", "--no-reindex", "--overwrite-target"]
    )
    assert args.no_recompact is True
    assert args.no_reindex is True
    assert args.overwrite_target is True


def test_migrate_namespace_index_column_flags() -> None:
    """Index column flags are accepted and stored on the ``migrate-namespace`` namespace."""
    args = tools_cli.build_parser().parse_args(
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
    assert args.vector_column == ["vec"]
    assert args.metric == "cosine"
    assert args.scalar_column == ["updated_at"]
    assert args.bitmap_column == ["category"]
    assert args.text_column == ["body"]


def test_migrate_namespace_requires_source_namespace() -> None:
    """``migrate-namespace`` fails without ``--source-namespace``."""
    with pytest.raises(SystemExit) as exc_info:
        tools_cli.build_parser().parse_args(
            ["migrate-namespace", "--target-namespace", "new", "--base-uri", "/tmp/lance"]
        )
    assert exc_info.value.code != 0


def test_migrate_namespace_requires_target_namespace() -> None:
    """``migrate-namespace`` fails without ``--target-namespace``."""
    with pytest.raises(SystemExit) as exc_info:
        tools_cli.build_parser().parse_args(
            ["migrate-namespace", "--source-namespace", "old", "--base-uri", "/tmp/lance"]
        )
    assert exc_info.value.code != 0


def test_migrate_namespace_requires_base_uri() -> None:
    """``migrate-namespace`` fails without ``--base-uri``."""
    with pytest.raises(SystemExit) as exc_info:
        tools_cli.build_parser().parse_args(
            ["migrate-namespace", "--source-namespace", "old", "--target-namespace", "new"]
        )
    assert exc_info.value.code != 0
