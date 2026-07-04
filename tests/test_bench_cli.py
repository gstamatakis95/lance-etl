"""Unit tests for BenchConfig command-line parsing across every subcommand."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import (
    DEFAULT_CORPUS_ROOT,
    DEFAULT_ICEBERG_PACKAGE,
    SIFT_BASE_COUNT,
    SUBCOMMANDS,
    BenchConfig,
    build_parser,
    parse_int_list,
    parse_refine_list,
)


def config_for(argv: list[str]) -> BenchConfig:
    """Parse an argument vector into a BenchConfig.

    Args:
        argv: The argument vector after the program name.

    Returns:
        The parsed configuration.
    """
    return BenchConfig.from_args(build_parser().parse_args(argv))


class TestListParsing:
    """The comma-list flag parsers."""

    def test_parse_int_list(self) -> None:
        """Comma-separated integers parse with whitespace tolerated."""
        assert parse_int_list("1, 10,25") == [1, 10, 25]

    def test_parse_refine_list_none(self) -> None:
        """'none' parses to None alongside integers."""
        assert parse_refine_list("none,5, 10") == [None, 5, 10]

    def test_parse_refine_list_empty_items_skipped(self) -> None:
        """Empty items are skipped."""
        assert parse_refine_list("5,,10,") == [5, 10]


class TestEverySubcommandParses:
    """Each subcommand parses with defaults and reports its own command."""

    @pytest.mark.parametrize("command", SUBCOMMANDS)
    def test_defaults(self, command: str) -> None:
        """Bare subcommands produce the documented defaults."""
        config: BenchConfig = config_for([command])
        assert config.command == command
        assert config.dataset == "sift1m"
        assert config.limit == SIFT_BASE_COUNT
        assert config.tenants == 1
        assert config.seed == 42
        assert config.batches == 1
        assert config.endpoint == "localhost:50051"
        assert config.nprobes == [1, 10, 25, 50, 100]
        assert config.refine_factors == [None, 5, 10]
        assert config.concurrency == [1, 8, 32]
        assert config.ivf_partitions is None
        assert config.max_queries is None
        assert config.iceberg_package == DEFAULT_ICEBERG_PACKAGE
        assert config.prewarm is False
        assert config.force is False


class TestSubcommandFlags:
    """Phase-relevant flags reach the configuration."""

    def test_download_flags(self) -> None:
        """Download accepts a pinned checksum, workspace, and corpus-root."""
        config: BenchConfig = config_for(
            ["download", "--sha256", "abc123", "--workspace", "/tmp/ws", "--corpus-root", "/tmp/corpora"]
        )
        assert config.sha256 == "abc123"
        assert config.workspace == Path("/tmp/ws").resolve()
        assert config.corpus_root == Path("/tmp/corpora").resolve()

    def test_corpus_root_default(self) -> None:
        """corpus_root defaults to bench/corpora relative to the package directory."""
        config: BenchConfig = config_for(["download"])
        assert config.corpus_root == DEFAULT_CORPUS_ROOT

    def test_prepare_flags(self) -> None:
        """Prepare accepts corpus-shape flags."""
        config: BenchConfig = config_for(
            ["prepare", "--limit", "5000", "--tenants", "4", "--seed", "7", "--num-clusters", "16", "--force"]
        )
        assert config.limit == 5000
        assert config.tenants == 4
        assert config.seed == 7
        assert config.num_clusters == 16
        assert config.force is True

    def test_ingest_flags(self) -> None:
        """Ingest accepts batches and ETL partitioning."""
        config: BenchConfig = config_for(["ingest", "--batches", "4", "--etl-partitions", "16"])
        assert config.batches == 4
        assert config.etl_partitions == 16

    def test_index_flags(self) -> None:
        """Index accepts the IVF sweep and sharding knobs."""
        config: BenchConfig = config_for(
            [
                "index",
                "--num-partitions",
                "256",
                "--num-shards",
                "32",
                "--vector-row-floor",
                "10",
                "--fts-with-position",
            ]
        )
        assert config.ivf_partitions == 256
        assert config.num_shards == 32
        assert config.vector_row_floor == 10
        assert config.fts_with_position is True

    def test_compact_flags(self) -> None:
        """Compact accepts the target fragment size."""
        config: BenchConfig = config_for(["compact", "--target-rows-per-fragment", "500000"])
        assert config.compact_target_rows == 500000

    def test_search_flags(self) -> None:
        """Search accepts the sweep grid, endpoint, query caps, and load knobs."""
        config: BenchConfig = config_for(
            [
                "search",
                "--endpoint",
                "localhost:9999",
                "--nprobes",
                "1,5",
                "--refine-factors",
                "none,20",
                "--max-queries",
                "100",
                "--concurrency",
                "2,4",
                "--load-duration",
                "5s",
                "--load-nprobes",
                "25",
                "--prewarm",
            ]
        )
        assert config.endpoint == "localhost:9999"
        assert config.nprobes == [1, 5]
        assert config.refine_factors == [None, 20]
        assert config.max_queries == 100
        assert config.concurrency == [2, 4]
        assert config.load_duration == "5s"
        assert config.load_nprobes == 25
        assert config.prewarm is True

    def test_report_and_run_id(self) -> None:
        """Report accepts an explicit run id and results root."""
        config: BenchConfig = config_for(["report", "--run-id", "run42", "--results-root", "/tmp/results"])
        assert config.run_id == "run42"
        assert config.run_dir() == Path("/tmp/results").resolve() / "run42"

    def test_all_flags(self) -> None:
        """All accepts the union of phase flags."""
        config: BenchConfig = config_for(["all", "--limit", "1000", "--batches", "2", "--tenants", "2"])
        assert config.command == "all"
        assert config.limit == 1000
        assert config.batches == 2
        assert config.tenants == 2


class TestDerivedPaths:
    """Derived identifiers and paths follow the documented shapes."""

    def test_prepared_key_includes_shape(self) -> None:
        """The prepared cache key encodes limit, tenants, seed, and clusters."""
        config: BenchConfig = config_for(["prepare", "--limit", "100", "--tenants", "2", "--seed", "3"])
        assert config.prepared_key() == "n100-t2-s3-c64"

    def test_prepared_key_prefixes_non_default_dataset(self) -> None:
        """Non-default datasets get a name-prefixed prepared key. sift1m keeps its historical key."""
        config: BenchConfig = config_for(["prepare", "--dataset", "gist1m", "--limit", "100"])
        assert config.prepared_key() == "gist1m-n100-t1-s42-c64"

    def test_table_name(self) -> None:
        """The Iceberg table identifier is catalog.db.table."""
        config: BenchConfig = config_for(["prepare", "--catalog", "c1", "--table-name", "t1"])
        assert config.table() == "c1.db.t1"

    def test_dataset_uris_match_etl_routing(self) -> None:
        """Dataset URIs follow base/org/tenant/namespace.lance per tenant."""
        config: BenchConfig = config_for(["ingest", "--tenants", "2", "--workspace", "/tmp/ws"])
        base: str = str(Path("/tmp/ws").resolve() / "lance")
        assert config.dataset_uris() == [f"{base}/org0/tenant0/ns.lance", f"{base}/org1/tenant0/ns.lance"]
