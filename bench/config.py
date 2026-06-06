"""Benchmark configuration and command-line parsing.

Defines :class:`BenchConfig`, the single dataclass shared by every benchmark phase, and the argparse parser for the
``python -m bench`` entry point. Every subcommand accepts the full flag set so one flag vector can drive the whole
``all`` chain; each phase simply reads the fields it needs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path

PACKAGE_DIR: Path = Path(__file__).resolve().parent
REPO_ROOT: Path = PACKAGE_DIR.parent
DEFAULT_WORKSPACE: Path = PACKAGE_DIR / "workspace"
DEFAULT_RESULTS_ROOT: Path = PACKAGE_DIR / "results"
DEFAULT_ICEBERG_PACKAGE: str = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
PROTO_PATH: Path = REPO_ROOT / "rust" / "search-api" / "proto" / "lance_etl" / "search" / "v1" / "search.proto"
SIFT_DIM: int = 128
SIFT_BASE_COUNT: int = 1_000_000
SIFT_QUERY_COUNT: int = 10_000
SIFT_GT_DEPTH: int = 100
TENANT_ID: str = "tenant0"
NAMESPACE: str = "ns"
SIFT_FILE_NAMES: tuple[str, str, str] = ("sift_base.fvecs", "sift_query.fvecs", "sift_groundtruth.ivecs")
SUBCOMMANDS: tuple[str, ...] = ("download", "prepare", "ingest", "index", "compact", "search", "report", "all")
PHASE_NAMES: tuple[str, ...] = ("download", "prepare", "ingest", "index", "compact", "search", "report")


def default_run_id() -> str:
    """Build a timestamp-based run identifier.

    Returns:
        A UTC timestamp string usable as a directory name.
    """
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def parse_int_list(text: str) -> list[int]:
    """Parse a comma-separated list of integers.

    Args:
        text: The raw flag value, such as ``"1,10,25"``.

    Returns:
        The parsed integers.
    """
    return [int(item.strip()) for item in text.split(",") if item.strip()]


def parse_refine_list(text: str) -> list[int | None]:
    """Parse a comma-separated refine-factor list where ``none`` means no refinement.

    Args:
        text: The raw flag value, such as ``"none,5,10"``.

    Returns:
        The parsed refine factors with ``None`` for the unrefined point.
    """
    result: list[int | None] = []
    for item in text.split(","):
        cleaned: str = item.strip().lower()
        if not cleaned:
            continue
        result.append(None if cleaned in ("none", "null") else int(cleaned))
    return result


@dataclass
class BenchConfig:
    """Configuration shared by every benchmark phase.

    Attributes:
        command: The subcommand being executed.
        workspace: Directory holding downloaded data, prepared artifacts, the Iceberg warehouse, and Lance datasets.
        results_root: Directory under which per-run result directories are created.
        run_id: Identifier of the current run; one run directory aggregates every phase's artifacts.
        limit: Number of base vectors to benchmark; the full corpus is 1M.
        tenants: Number of org datasets the vectors are split into round-robin.
        seed: Master seed for k-means sampling, vocabulary, and text generation.
        num_clusters: Coarse k-means cluster count driving the synthetic text vocabularies.
        words_per_cluster: Vocabulary size per cluster.
        common_words: Size of the shared common-word pool mixed into every document.
        words_per_text: Cluster-specific words per document.
        rows_per_slice: Base vectors generated per Spark task during prepare.
        batches: Sequential ETL merge batches during ingest; values above 1 create extra fragments for compaction.
        etl_partitions: Shuffle partition count handed to the ETL job.
        ivf_partitions: Explicit IVF partition count; ``None`` uses the indexer's size-aware policy.
        num_shards: Parallel segment builders per dataset during indexing.
        vector_row_floor: Row floor below which the vector index is skipped; lowered from the production default so
            small ``--limit`` runs still build an index.
        fts_with_position: Store token positions in the inverted index.
        compact_target_rows: Target rows per fragment for compaction; ``None`` uses the Lance default.
        iceberg_package: Maven coordinates of the Iceberg Spark runtime resolved at session start.
        spark_master: Spark master URL.
        driver_memory: Spark driver memory for the local-mode JVM.
        catalog: Name of the local Hadoop Iceberg catalog.
        table_name: Bare Iceberg table name under ``<catalog>.db``.
        endpoint: gRPC endpoint of the Rust search service.
        nprobes: Probed-partition sweep values for the recall mode.
        refine_factors: Refine-factor sweep values; ``None`` disables re-ranking.
        search_k: Neighbors requested per query; must cover the deepest recall cut-off.
        max_queries: Cap on query vectors per sweep point; ``None`` sends all 10k.
        fts_query_count: Synthetic full-text queries in the FTS leg.
        hybrid_query_count: Queries in the hybrid (vector + text, RRF) leg.
        concurrency: ghz concurrency levels for the load mode.
        load_duration: ghz test duration per concurrency level.
        load_nprobes: nprobes used by the load and hybrid legs.
        prewarm: Call the prewarm hook before timing first queries.
        sha256: Optional pinned checksum for the downloaded sift archive.
        force: Rebuild prepared artifacts even when a manifest already exists.
    """

    command: str
    workspace: Path = DEFAULT_WORKSPACE
    results_root: Path = DEFAULT_RESULTS_ROOT
    run_id: str = field(default_factory=default_run_id)
    limit: int = SIFT_BASE_COUNT
    tenants: int = 1
    seed: int = 42
    num_clusters: int = 64
    words_per_cluster: int = 40
    common_words: int = 20
    words_per_text: int = 8
    rows_per_slice: int = 50_000
    batches: int = 1
    etl_partitions: int = 8
    ivf_partitions: int | None = None
    num_shards: int = 8
    vector_row_floor: int = 1_024
    fts_with_position: bool = False
    compact_target_rows: int | None = None
    iceberg_package: str = DEFAULT_ICEBERG_PACKAGE
    spark_master: str = "local[*]"
    driver_memory: str = "8g"
    catalog: str = "bench"
    table_name: str = "sift"
    endpoint: str = "localhost:50051"
    nprobes: list[int] = field(default_factory=lambda: [1, 10, 25, 50, 100])
    refine_factors: list[int | None] = field(default_factory=lambda: [None, 5, 10])
    search_k: int = SIFT_GT_DEPTH
    max_queries: int | None = None
    fts_query_count: int = 100
    hybrid_query_count: int = 100
    concurrency: list[int] = field(default_factory=lambda: [1, 8, 32])
    load_duration: str = "15s"
    load_nprobes: int = 10
    prewarm: bool = False
    sha256: str | None = None
    force: bool = False

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> BenchConfig:
        """Build a configuration from parsed command-line arguments.

        Args:
            args: The parsed argparse namespace.

        Returns:
            The populated configuration.
        """
        values: dict[str, object] = {}
        for item in fields(cls):
            if hasattr(args, item.name):
                value: object = getattr(args, item.name)
                if value is not None or item.name in ("ivf_partitions", "compact_target_rows", "max_queries", "sha256"):
                    values[item.name] = value
        values["workspace"] = Path(args.workspace).resolve()
        values["results_root"] = Path(args.results_root).resolve()
        return cls(**values)  # type: ignore[arg-type]

    def table(self) -> str:
        """Return the fully qualified Iceberg table name.

        Returns:
            The ``catalog.db.table`` identifier.
        """
        return f"{self.catalog}.db.{self.table_name}"

    def sift_dir(self) -> Path:
        """Return the directory holding the extracted SIFT1M files.

        Returns:
            The ``sift`` directory under the workspace.
        """
        return self.workspace / "sift"

    def warehouse_dir(self) -> Path:
        """Return the local Iceberg warehouse directory.

        Returns:
            The warehouse directory under the workspace.
        """
        return self.workspace / "iceberg_warehouse"

    def lance_root(self) -> Path:
        """Return the base directory of the per-tenant Lance datasets.

        Returns:
            The Lance base directory under the workspace.
        """
        return self.workspace / "lance"

    def prepared_key(self) -> str:
        """Return the cache key identifying one prepared corpus shape.

        Returns:
            A key derived from the fields that change the corpus or ground truth.
        """
        return f"n{self.limit}-t{self.tenants}-s{self.seed}-c{self.num_clusters}"

    def prepared_dir(self) -> Path:
        """Return the directory of prepared artifacts for the current shape.

        Returns:
            The shape-specific directory under ``workspace/prepared``.
        """
        return self.workspace / "prepared" / self.prepared_key()

    def run_dir(self) -> Path:
        """Return the per-run results directory.

        Returns:
            The ``results_root/run_id`` directory.
        """
        return self.results_root / self.run_id

    def org_ids(self) -> list[str]:
        """Return the org identifiers, one per tenant.

        Returns:
            The org ids in tenant order.
        """
        return [f"org{tenant}" for tenant in range(self.tenants)]

    def dataset_uris(self) -> list[str]:
        """Return the per-tenant Lance dataset URIs the ETL routing produces.

        Returns:
            One dataset URI per org, matching ``lance_etl.etl.dataset_uri``.
        """
        base: str = str(self.lance_root())
        return [f"{base}/{org}/{TENANT_ID}/{NAMESPACE}.lance" for org in self.org_ids()]


def add_flags(parser: argparse.ArgumentParser) -> None:
    """Add the full benchmark flag set to a subcommand parser.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--run-id", dest="run_id", default=None)
    parser.add_argument("--limit", type=int, default=SIFT_BASE_COUNT, help="Base vectors to benchmark; default 1M")
    parser.add_argument("--tenants", type=int, default=1, help="Round-robin split into this many org datasets")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-clusters", type=int, default=64)
    parser.add_argument("--words-per-cluster", type=int, default=40)
    parser.add_argument("--common-words", type=int, default=20)
    parser.add_argument("--words-per-text", type=int, default=8)
    parser.add_argument("--rows-per-slice", type=int, default=50_000)
    parser.add_argument("--batches", type=int, default=1, help="Sequential ETL merge batches; >1 creates fragments")
    parser.add_argument("--etl-partitions", type=int, default=8)
    parser.add_argument("--num-partitions", dest="ivf_partitions", type=int, default=None, help="IVF partition count")
    parser.add_argument("--num-shards", type=int, default=8)
    parser.add_argument("--vector-row-floor", type=int, default=1_024)
    parser.add_argument("--fts-with-position", action="store_true")
    parser.add_argument("--target-rows-per-fragment", dest="compact_target_rows", type=int, default=None)
    parser.add_argument("--iceberg-package", default=DEFAULT_ICEBERG_PACKAGE)
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--catalog", default="bench")
    parser.add_argument("--table-name", default="sift")
    parser.add_argument("--endpoint", default="localhost:50051")
    parser.add_argument("--nprobes", type=parse_int_list, default=None, help="Comma list, e.g. 1,10,25,50,100")
    parser.add_argument(
        "--refine-factors", type=parse_refine_list, default=None, help="Comma list; 'none' disables, e.g. none,5,10"
    )
    parser.add_argument("--search-k", type=int, default=SIFT_GT_DEPTH)
    parser.add_argument("--max-queries", type=int, default=None, help="Cap query vectors per sweep point")
    parser.add_argument("--fts-queries", dest="fts_query_count", type=int, default=100)
    parser.add_argument("--hybrid-queries", dest="hybrid_query_count", type=int, default=100)
    parser.add_argument("--concurrency", type=parse_int_list, default=None, help="ghz concurrency levels, e.g. 1,8,32")
    parser.add_argument("--load-duration", default="15s")
    parser.add_argument("--load-nprobes", type=int, default=10)
    parser.add_argument("--prewarm", action="store_true")
    parser.add_argument("--sha256", default=None, help="Pinned sha256 of sift.tar.gz")
    parser.add_argument("--force", action="store_true", help="Rebuild prepared artifacts")


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level benchmark argument parser.

    Returns:
        The parser with one subcommand per benchmark phase plus ``all``.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="python -m bench", description=__doc__)
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)
    help_texts: dict[str, str] = {
        "download": "Fetch and verify the SIFT1M corpus",
        "prepare": "Write the Iceberg source table, synthetic text, and ground truth",
        "ingest": "Run the real Iceberg-to-Lance ETL into per-tenant datasets",
        "index": "Build IVF_RQ, BTREE, BITMAP, and INVERTED indices with LanceIndexer",
        "compact": "Compact the datasets with LanceCompactor and record fragment counts",
        "search": "Run recall, FTS, hybrid, and ghz load modes against the gRPC server",
        "report": "Aggregate run artifacts into summary.md, results.csv, and pareto.png",
        "all": "Run the full chain: download, prepare, ingest, index, compact, search, report",
    }
    for name in SUBCOMMANDS:
        sub: argparse.ArgumentParser = subparsers.add_parser(name, help=help_texts[name])
        add_flags(sub)
    return parser
