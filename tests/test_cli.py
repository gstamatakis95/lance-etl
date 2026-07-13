"""Sanity tests for the per-job command-line entry points.

The aggregate ``lance_etl.cli`` dispatcher no longer exists. Tests are mapped to the five
per-job CLIs: ``lance_etl.etl.cli``, ``lance_etl.maintenance.cli``, ``lance_etl.indexing.cli``,
``lance_etl.pipeline.cli``, and ``lance_etl.tools.cli``.  Pure helpers (``parse_key_values``
and ``parse_window_tag``) live in ``lance_etl.cliutil``.
"""

from __future__ import annotations

import pytest

import lance_etl.etl.cli as etl_cli
import lance_etl.indexing.cli as indexing_cli
import lance_etl.maintenance.cli as maintenance_cli
import lance_etl.pipeline.cli as pipeline_cli
import lance_etl.tools.cli as tools_cli
from lance_etl.cliutil import parse_hour_tag, parse_key_values, parse_window_tag


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


def test_etl_tag_stamp_defaults_off() -> None:
    """Without the flag, ``--tag-stamp`` is None so the ETL stamps no tags."""
    args = etl_cli.build_parser().parse_args(REQUIRED_ETL_ARGV)
    assert args.tag_stamp is None


def test_etl_tag_stamp_truncates_to_the_hour() -> None:
    """``--tag-stamp`` converts an Airflow datetime through parse_hour_tag."""
    args = etl_cli.build_parser().parse_args([*REQUIRED_ETL_ARGV, "--tag-stamp", "2026-06-11 12:34:56+00:00"])
    assert args.tag_stamp == "20260611T120000Z"


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


@pytest.mark.parametrize("flag", ["--cluster-rewrite", "--cluster-column", "--cluster-serve-tag"])
def test_maintenance_cluster_rewrite_flags_removed(flag: str) -> None:
    """Production maintenance rejects every clustered-overwrite command-line surface."""
    argv: list[str] = [*REQUIRED_MAINTENANCE_RUN_ARGV, flag]
    if flag == "--cluster-column":
        argv.append("embedding")
    with pytest.raises(SystemExit) as exc_info:
        maintenance_cli.build_parser().parse_args(argv)
    assert exc_info.value.code != 0


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


REQUIRED_PIPELINE_RUN_ARGV: list[str] = [
    "run",
    "--base-uri",
    "/tmp/lance",
]


def test_pipeline_parser_builds() -> None:
    """The pipeline argument parser constructs without error."""
    parser = pipeline_cli.build_parser()
    assert parser is not None


def test_pipeline_help_exits_zero() -> None:
    """The pipeline CLI exits with status 0 when passed ``--help``."""
    with pytest.raises(SystemExit) as exc_info:
        pipeline_cli.main(["--help"])
    assert exc_info.value.code == 0


def test_pipeline_run_parses_required_args() -> None:
    """``pipeline run`` subcommand parses dataset selection."""
    args = pipeline_cli.build_parser().parse_args(REQUIRED_PIPELINE_RUN_ARGV)
    assert args.command == "run"
    assert args.base_uri == "/tmp/lance"


def test_pipeline_run_defaults() -> None:
    """``pipeline run`` defaults: tag_keep_last=48, serve_tag=False, tag_stamp=None, rebuild=False."""
    args = pipeline_cli.build_parser().parse_args(REQUIRED_PIPELINE_RUN_ARGV)
    assert args.tag_keep_last == 48
    assert args.serve_tag is False
    assert args.tag_stamp is None
    assert args.rebuild is False
    assert args.ttl_column is None
    assert args.ts_column == "event_timestamp"


def test_pipeline_tag_keep_last_zero_disables() -> None:
    """``--tag-keep-last 0`` is accepted and later converted to None in run_run."""
    args = pipeline_cli.build_parser().parse_args([*REQUIRED_PIPELINE_RUN_ARGV, "--tag-keep-last", "0"])
    assert args.tag_keep_last == 0


def test_pipeline_serve_tag_flag() -> None:
    """``--serve-tag`` is accepted and stored as True."""
    args = pipeline_cli.build_parser().parse_args([*REQUIRED_PIPELINE_RUN_ARGV, "--serve-tag"])
    assert args.serve_tag is True


def test_pipeline_tag_stamp_converts_via_parse_window_tag() -> None:
    """``--tag-stamp`` converts the Airflow datetime string through parse_window_tag."""
    args = pipeline_cli.build_parser().parse_args(
        [*REQUIRED_PIPELINE_RUN_ARGV, "--tag-stamp", "2026-06-11 12:00:00+00:00"]
    )
    assert args.tag_stamp == "20260611T120000Z"


def test_pipeline_tag_stamp_t_separator() -> None:
    """``--tag-stamp`` accepts the T-separated ISO form."""
    args = pipeline_cli.build_parser().parse_args(
        [*REQUIRED_PIPELINE_RUN_ARGV, "--tag-stamp", "2026-06-11T12:00:00+00:00"]
    )
    assert args.tag_stamp == "20260611T120000Z"


def test_pipeline_index_column_flags_present() -> None:
    """Index column flags are accepted by the pipeline run subcommand."""
    args = pipeline_cli.build_parser().parse_args(
        [
            *REQUIRED_PIPELINE_RUN_ARGV,
            "--vector-column",
            "embedding",
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
    assert args.vector_column == ["embedding"]
    assert args.metric == "cosine"
    assert args.scalar_column == ["updated_at"]
    assert args.bitmap_column == ["category"]
    assert args.text_column == ["body"]


def test_pipeline_rebuild_flag() -> None:
    """``--rebuild`` is accepted and stored as True."""
    args = pipeline_cli.build_parser().parse_args([*REQUIRED_PIPELINE_RUN_ARGV, "--rebuild"])
    assert args.rebuild is True


def test_pipeline_ttl_column_flag() -> None:
    """``--ttl-column`` and ``--ts-column`` are accepted by the pipeline run subcommand."""
    args = pipeline_cli.build_parser().parse_args(
        [*REQUIRED_PIPELINE_RUN_ARGV, "--ttl-column", "ttl", "--ts-column", "event_time"]
    )
    assert args.ttl_column == "ttl"
    assert args.ts_column == "event_time"


def test_parse_window_tag_space_separator() -> None:
    """parse_window_tag handles Airflow-style space-separated datetimes."""
    result: str = parse_window_tag("2026-06-11 12:00:00+00:00")
    assert result == "20260611T120000Z"


def test_parse_window_tag_t_separator() -> None:
    """parse_window_tag handles T-separated ISO datetimes."""
    result: str = parse_window_tag("2026-06-11T12:00:00+00:00")
    assert result == "20260611T120000Z"


def test_parse_window_tag_non_utc_converts_to_utc() -> None:
    """parse_window_tag converts a non-UTC timezone to UTC before formatting."""
    result: str = parse_window_tag("2026-06-11T14:00:00+02:00")
    assert result == "20260611T120000Z"


def test_parse_window_tag_naive_treated_as_utc() -> None:
    """parse_window_tag treats a naive datetime (no timezone) as UTC."""
    result: str = parse_window_tag("2026-06-11T12:00:00")
    assert result == "20260611T120000Z"


def test_parse_window_tag_garbage_raises() -> None:
    """parse_window_tag raises ValueError on unparseable input."""
    with pytest.raises(ValueError, match="cannot parse"):
        parse_window_tag("not-a-date")


def test_parse_hour_tag_truncates_minutes_and_seconds() -> None:
    """parse_hour_tag zeroes minutes, seconds, and microseconds before formatting."""
    result: str = parse_hour_tag("2026-06-11T12:34:56.789+00:00")
    assert result == "20260611T120000Z"


def test_parse_hour_tag_on_the_hour_is_identity() -> None:
    """An instant already on the hour maps to that hour's tag."""
    result: str = parse_hour_tag("2026-06-11 12:00:00+00:00")
    assert result == "20260611T120000Z"


def test_parse_hour_tag_converts_to_utc_before_truncating() -> None:
    """A non-UTC instant is converted to UTC first, then truncated."""
    result: str = parse_hour_tag("2026-06-11T14:45:00+02:00")
    assert result == "20260611T120000Z"


def test_parse_hour_tag_naive_treated_as_utc() -> None:
    """A naive datetime is treated as UTC."""
    result: str = parse_hour_tag("2026-06-11T12:59:59")
    assert result == "20260611T120000Z"


def test_parse_hour_tag_garbage_raises() -> None:
    """parse_hour_tag raises ValueError on unparseable input."""
    with pytest.raises(ValueError, match="cannot parse"):
        parse_hour_tag("not-a-date")
