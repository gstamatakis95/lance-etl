"""Benchmark configuration and command-line parsing.

Defines :class:`BenchConfig`, the single dataclass shared by every benchmark phase, and the argparse parser for the
``python -m bench`` entry point. Every subcommand accepts the full flag set so one flag vector can drive the whole
``all`` chain. Each phase simply reads the fields it needs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PACKAGE_DIR: Path = Path(__file__).resolve().parent
REPO_ROOT: Path = PACKAGE_DIR.parent
DEFAULT_WORKSPACE: Path = PACKAGE_DIR / "workspace"
DEFAULT_CORPUS_ROOT: Path = PACKAGE_DIR / "corpora"
DEFAULT_RESULTS_ROOT: Path = PACKAGE_DIR / "results"
DEFAULT_ICEBERG_PACKAGE: str = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
PROTO_PATH: Path = REPO_ROOT / "rust" / "search-api" / "proto" / "lance_etl" / "v1" / "lance_etl.proto"
SIFT_DIM: int = 128
SIFT_BASE_COUNT: int = 1_000_000
SIFT_QUERY_COUNT: int = 10_000
SIFT_GT_DEPTH: int = 100
TENANT_ID: str = "tenant0"
NAMESPACE: str = "ns"
SIFT_FILE_NAMES: tuple[str, str, str] = ("sift_base.fvecs", "sift_query.fvecs", "sift_groundtruth.ivecs")
SUBCOMMANDS: tuple[str, ...] = (
    "download",
    "prepare",
    "ingest",
    "index",
    "compact",
    "search",
    "report",
    "all",
    "e2e",
    "experiment",
    "qualify",
)
PHASE_NAMES: tuple[str, ...] = ("download", "prepare", "ingest", "index", "compact", "search", "report")
RECALL_CUTOFFS: tuple[int, ...] = (1, 10, 100)
"""Recall cut-off depths scored by the search and e2e legs; ``search_k`` must cover the deepest one."""


def default_run_id() -> str:
    """Build a timestamp-based run identifier.

    Returns:
        A UTC timestamp string usable as a directory name.
    """
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def parse_env_pairs(pairs: list[str] | None) -> dict[str, str]:
    """Parse repeatable ``KEY=VALUE`` environment overrides into a dict.

    Args:
        pairs: The raw flag values, or ``None`` when the flag was never given.

    Returns:
        The parsed environment mapping, empty when no pairs were given.

    Raises:
        ValueError: If a pair carries no ``=`` separator or an empty key.
    """
    env: dict[str, str] = {}
    for pair in pairs or []:
        key, separator, value = pair.partition("=")
        if not separator or not key:
            raise ValueError(f"--server-env expects KEY=VALUE, got {pair!r}")
        env[key] = value
    return env


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
        dataset: Name of the registered dataset adapter driving the run. Defaults to the canonical SIFT1M corpus.
        workspace: Directory holding prepared artifacts, the Iceberg warehouse, and Lance datasets.
        corpus_root: Shared corpus cache directory; downloads land here once and are reused across workspaces.
        results_root: Directory under which per-run result directories are created.
        run_id: Identifier of the current run. One run directory aggregates every phase's artifacts.
        limit: Number of base vectors to benchmark. The full corpus is 1M.
        tenants: Number of org datasets the vectors are split into round-robin.
        seed: Master seed for k-means sampling, vocabulary, and text generation.
        num_clusters: Coarse k-means cluster count driving the cluster-seeded text vocabularies.
        words_per_cluster: Vocabulary size per cluster.
        common_words: Size of the shared common-word pool mixed into every document.
        words_per_text: Cluster-specific words per document.
        rows_per_slice: Base vectors generated per Spark task during prepare.
        batches: Sequential ETL merge batches during ingest. Values above 1 create extra fragments for compaction.
        etl_partitions: Shuffle partition count handed to the ETL job.
        ivf_partitions: Explicit IVF partition count. ``None`` uses the indexer's size-aware policy.
        num_shards: Fragments covered by one segment-build task during indexing.
        vector_row_floor: Row floor below which the vector index is skipped. Lowered from the production default so
            small ``--limit`` runs still build an index.
        fts_with_position: Store token positions in the inverted index.
        compact_target_rows: Target rows per fragment for compaction. ``None`` uses the Lance default.
        iceberg_package: Maven coordinates of the Iceberg Spark runtime resolved at session start.
        spark_master: Spark master URL.
        driver_memory: Spark driver memory for the local-mode JVM.
        catalog: Name of the local Hadoop Iceberg catalog.
        table_name: Bare Iceberg table name under ``<catalog>.db``.
        endpoint: gRPC endpoint of the Rust search service.
        nprobes: Probed-partition sweep values for the recall mode.
        refine_factors: Refine-factor sweep values. ``None`` disables re-ranking.
        search_k: Neighbors requested per query. Must cover the deepest recall cut-off.
        max_queries: Cap on query vectors per sweep point. ``None`` sends all 10k.
        fts_query_count: Deterministic full-text queries drawn from cluster vocabularies in the FTS leg.
        hybrid_query_count: Queries in the hybrid (vector + text, RRF) leg.
        concurrency: ghz concurrency levels for the load mode.
        load_duration: ghz test duration per concurrency level.
        load_nprobes: nprobes used by the load and hybrid legs.
        prewarm: Call the prewarm hook before timing first queries.
        sha256: Optional pinned checksum for the downloaded sift archive.
        force: Rebuild prepared artifacts even when a manifest already exists.
        warmup_queries: Queries issued at the maximum nprobes before the timed sweep. Set to 0 to skip warmup.
        no_text: When True, omit the ``texts`` column and skip FTS/hybrid index and search legs entirely.
        capture_telemetry: When True, start the local DogStatsD and OTLP capture listeners for the duration of the
            run and write all telemetry to ``{workspace}/telemetry/``.
        statsd_port: UDP port for the local DogStatsD capture listener (default 19125, avoids clash with a real agent
            on 8125).
        otlp_port: gRPC port for the local OTLP trace capture receiver (default 14317, avoids clash with a real agent
            on 4317).
        server_bin: Explicit path of the search-api binary the experiment spawns. ``None`` resolves the release
            build then the debug build.
        build_server: When True, run ``cargo build --release`` for search-api before spawning it.
        spawn_server: When True (the default) the experiment spawns and owns a server. Disable to measure against
            an externally managed server at ``endpoint``.
        server_env: Extra environment variables for the spawned server, from repeatable ``--server-env KEY=VALUE``
            flags. This is how an iteration varies server-side knobs such as the cache backend or cache budgets.
        baseline: Run id of a previous experiment whose ``metrics.json`` is diffed against this run's headline
            numbers.
        qualification_rows: Rows in the bounded deterministic scale and fault cohort.
        allow_large_qualification: Explicit opt-in for a synthetic cohort above the local safety bound.
    """

    command: str
    dataset: str = "sift1m"
    workspace: Path = DEFAULT_WORKSPACE
    corpus_root: Path = DEFAULT_CORPUS_ROOT
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
    warmup_queries: int = 100
    no_text: bool = False
    capture_telemetry: bool = False
    statsd_port: int = 19125
    otlp_port: int = 14317
    server_bin: str | None = None
    build_server: bool = False
    spawn_server: bool = True
    server_env: dict[str, str] = field(default_factory=dict)
    baseline: str | None = None
    qualification_rows: int = 25_000
    allow_large_qualification: bool = False

    def __post_init__(self) -> None:
        """Validate cross-field invariants after the dataclass fields are populated.

        Raises:
            ValueError: If ``search_k`` is smaller than the deepest :data:`RECALL_CUTOFFS` depth.
                ``recall_at`` slices the retrieved-id array to the cut-off width, so a shorter
                array silently caps recall below its true value instead of raising, which would
                make ``search_k`` misconfiguration masquerade as a real recall drop.
        """
        deepest_cutoff: int = max(RECALL_CUTOFFS)
        if self.search_k < deepest_cutoff:
            raise ValueError(
                f"search_k={self.search_k} is below the deepest recall cutoff {deepest_cutoff} "
                f"(RECALL_CUTOFFS={RECALL_CUTOFFS}); recall_at_{deepest_cutoff} would be silently "
                f"deflated by the shorter retrieved-id array. Pass --search-k >= {deepest_cutoff}."
            )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> BenchConfig:
        """Build a configuration from parsed command-line arguments.

        Args:
            args: The parsed argparse namespace.

        Returns:
            The populated configuration.
        """
        values: dict[str, Any] = {}
        nullable_fields: frozenset[str] = frozenset(
            {"ivf_partitions", "compact_target_rows", "max_queries", "sha256", "statsd_port", "otlp_port"}
        )
        values["server_env"] = parse_env_pairs(getattr(args, "server_env", None))
        for item in fields(cls):
            if item.name == "server_env":
                continue
            if hasattr(args, item.name):
                value: Any = getattr(args, item.name)
                if value is not None or item.name in nullable_fields:
                    values[item.name] = value
        values["workspace"] = Path(args.workspace).resolve()
        values["corpus_root"] = Path(args.corpus_root).resolve()
        values["results_root"] = Path(args.results_root).resolve()
        return cls(**values)

    def table(self) -> str:
        """Return the fully qualified Iceberg table name.

        Returns:
            The ``catalog.db.table`` identifier.
        """
        return f"{self.catalog}.db.{self.table_name}"

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

        The default ``sift1m`` dataset keeps its historical un-prefixed key. Other datasets are prefixed with their
        adapter name so prepared artifacts never collide across datasets. When ``no_text`` is set the key carries a
        ``-notext`` suffix so text and no-text artifacts never collide.

        Returns:
            A key derived from the dataset and the fields that change the corpus or ground truth.
        """
        shape: str = f"n{self.limit}-t{self.tenants}-s{self.seed}-c{self.num_clusters}"
        if self.no_text:
            shape = f"{shape}-notext"
        if self.dataset == "sift1m":
            return shape
        return f"{self.dataset}-{shape}"

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

    def telemetry_dir(self) -> Path:
        """Return the directory under which all telemetry capture files are written.

        Returns:
            The ``workspace/telemetry`` directory.
        """
        return self.workspace / "telemetry"


def add_flags(parser: argparse.ArgumentParser) -> None:
    """Add the full benchmark flag set to a subcommand parser.

    Args:
        parser: The subcommand parser to extend.
    """
    parser.add_argument("--dataset", default="sift1m", help="Registered dataset adapter name; default sift1m")
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument(
        "--corpus-root",
        type=Path,
        default=DEFAULT_CORPUS_ROOT,
        dest="corpus_root",
        help="Shared corpus cache; downloads land here once and are reused across workspaces",
    )
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
    parser.add_argument(
        "--search-k",
        type=int,
        default=SIFT_GT_DEPTH,
        help=f"Neighbors requested per query; must be >= {max(RECALL_CUTOFFS)}, the deepest recall cutoff",
    )
    parser.add_argument("--max-queries", type=int, default=None, help="Cap query vectors per sweep point")
    parser.add_argument("--fts-queries", dest="fts_query_count", type=int, default=100)
    parser.add_argument("--hybrid-queries", dest="hybrid_query_count", type=int, default=100)
    parser.add_argument("--concurrency", type=parse_int_list, default=None, help="ghz concurrency levels, e.g. 1,8,32")
    parser.add_argument("--load-duration", default="15s")
    parser.add_argument("--load-nprobes", type=int, default=10)
    parser.add_argument("--prewarm", action="store_true")
    parser.add_argument("--sha256", default=None, help="Pinned sha256 of sift.tar.gz")
    parser.add_argument("--force", action="store_true", help="Rebuild prepared artifacts")
    parser.add_argument("--warmup-queries", type=int, default=100, help="Warmup queries before timed sweep; 0 skips")
    parser.add_argument(
        "--no-text",
        dest="no_text",
        action="store_true",
        help="Omit text column and skip FTS/hybrid legs; required for pure-vector corpora",
    )
    parser.add_argument(
        "--capture-telemetry",
        dest="capture_telemetry",
        action="store_true",
        help="Start local DogStatsD and OTLP capture listeners for the run duration",
    )
    parser.add_argument(
        "--statsd-port",
        dest="statsd_port",
        type=int,
        default=19125,
        help="UDP port for the local DogStatsD capture listener (default 19125)",
    )
    parser.add_argument(
        "--server-bin",
        default=None,
        help="Path to the search-api binary the experiment spawns (default: release then debug build)",
    )
    parser.add_argument(
        "--build-server",
        dest="build_server",
        action="store_true",
        help="Run cargo build --release for search-api before spawning it",
    )
    parser.add_argument(
        "--no-spawn-server",
        dest="spawn_server",
        action="store_false",
        help="Do not spawn a server; use the externally managed one at --endpoint",
    )
    parser.add_argument(
        "--server-env",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Extra environment for the spawned server, repeatable (e.g. SEARCH_API_CACHE_BACKEND=redis)",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Run id of a previous experiment to print a metrics delta against",
    )
    parser.add_argument(
        "--otlp-port",
        dest="otlp_port",
        type=int,
        default=14317,
        help="gRPC port for the local OTLP trace capture receiver (default 14317)",
    )
    parser.add_argument(
        "--qualification-rows",
        type=int,
        default=25_000,
        help="Rows in the deterministic local scale qualification cohort",
    )
    parser.add_argument(
        "--allow-large-qualification",
        action="store_true",
        help="Allow a qualification cohort above the local one-million-row safety bound",
    )


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
        "prepare": "Write the Iceberg source table, cluster-seeded text corpus, and ground truth",
        "ingest": "Run the real Iceberg-to-Lance ETL into per-tenant datasets",
        "index": "Build IVF_RQ, BTREE, BITMAP, and INVERTED indices with LanceIndexer",
        "compact": "Compact the datasets with MaintenanceJob and record fragment counts",
        "search": "Run recall, FTS, hybrid, and ghz load modes against the gRPC server",
        "report": "Aggregate run artifacts into summary.md, results.csv, and pareto.png",
        "all": "Run the full chain: download, prepare, ingest, index, compact, search, report",
        "e2e": "Batch-major e2e: per-batch ETL+index+compact+tag, historical-tag verification, optional gRPC legs",
        "experiment": "One agent iteration: prepare if needed, spawn the server, e2e, sizes, sweep, metrics.json",
        "qualify": "Measure deterministic mutation collapse, skew, shuffle width, capacity, and external scale gates",
    }
    for name in SUBCOMMANDS:
        sub: argparse.ArgumentParser = subparsers.add_parser(name, help=help_texts[name])
        add_flags(sub)
    return parser
