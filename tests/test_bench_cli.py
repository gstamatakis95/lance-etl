"""Unit tests for BenchConfig command-line parsing across every subcommand."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import (
    DEFAULT_CORPUS_ROOT,
    DEFAULT_ICEBERG_PACKAGE,
    RECALL_CUTOFFS,
    SIFT_BASE_COUNT,
    SUBCOMMANDS,
    BenchConfig,
    build_parser,
)


def config_for(argv: list[str]) -> BenchConfig:
    """Parse an argument vector into a BenchConfig.

    Args:
        argv: The argument vector after the program name.

    Returns:
        The parsed configuration.
    """
    return BenchConfig.from_args(build_parser().parse_args(argv))


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
        assert config.endpoint == ""
        assert config.ivf_partitions is None
        assert config.max_queries is None
        assert config.iceberg_package == DEFAULT_ICEBERG_PACKAGE
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

    def test_e2e_flags(self) -> None:
        """The reconciler-driven e2e accepts batches and the IVF sweep and sharding knobs."""
        config: BenchConfig = config_for(
            [
                "e2e",
                "--batches",
                "4",
                "--num-partitions",
                "256",
                "--num-shards",
                "32",
                "--vector-row-floor",
                "10",
                "--fts-with-position",
            ]
        )
        assert config.batches == 4
        assert config.ivf_partitions == 256
        assert config.num_shards == 32
        assert config.vector_row_floor == 10
        assert config.fts_with_position is True

    def test_search_flags(self, tmp_path: Path) -> None:
        """Search accepts a plaintext endpoint, version evidence, and a query cap."""
        config: BenchConfig = config_for(
            [
                "search",
                "--endpoint",
                "localhost:9999",
                "--search-expected-versions-path",
                str(tmp_path / "expected.json"),
                "--max-queries",
                "100",
            ]
        )
        assert config.endpoint == "localhost:9999"
        assert config.search_expected_versions_path == (tmp_path / "expected.json").resolve()
        assert config.max_queries == 100

    def test_report_and_run_id(self) -> None:
        """Report accepts an explicit run id and results root."""
        config: BenchConfig = config_for(["report", "--run-id", "run42", "--results-root", "/tmp/results"])
        assert config.run_id == "run42"
        assert config.run_dir() == Path("/tmp/results").resolve() / "run42"


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
        config: BenchConfig = config_for(["e2e", "--tenants", "2", "--workspace", "/tmp/ws"])
        base: str = str(Path("/tmp/ws").resolve() / "lance")
        assert config.dataset_uris() == [f"{base}/org0/tenant0/ns.lance", f"{base}/org1/tenant0/ns.lance"]


class TestSearchKValidation:
    """search_k must cover the deepest recall cutoff or recall_at silently deflates."""

    def test_search_k_below_deepest_cutoff_rejected(self) -> None:
        """A search_k smaller than max(RECALL_CUTOFFS) raises instead of silently deflating recall."""
        too_small: int = max(RECALL_CUTOFFS) - 1
        with pytest.raises(ValueError, match="search_k"):
            config_for(["search", "--search-k", str(too_small)])

    def test_search_k_covering_deepest_cutoff_accepted(self) -> None:
        """A search_k equal to the deepest cutoff is accepted."""
        config: BenchConfig = config_for(["search", "--search-k", str(max(RECALL_CUTOFFS))])
        assert config.search_k == max(RECALL_CUTOFFS)
