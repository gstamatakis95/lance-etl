"""Build the benchmark indices with the project's real ``LanceIndexer``.

Four index stages run sequentially over all per-tenant datasets so each index type gets its own wall-time measurement:
IVF_RQ on ``vector`` (sweepable ``--num-partitions``, defaulting to the indexer's size-aware policy), BTREE on
``vector_id``, BITMAP on the low-cardinality ``category`` column, and INVERTED (BM25) on ``text``. Each stage uses the
production two-tier orchestration in ``lance_etl.indexing`` unchanged.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from bench.config import BenchConfig
from bench.results import save_phase
from bench.spark_session import build_spark

logger: logging.Logger = logging.getLogger(__name__)


def bench_telemetry_config() -> Any:
    """Build the offline-safe telemetry configuration for benchmark jobs.

    Returns:
        A ``TelemetryConfig``; DogStatsD sends are fire-and-forget UDP so no agent is required.
    """
    from lance_etl.telemetry import TelemetryConfig

    return TelemetryConfig(service="lance-bench", env="bench")


def index_stages(config: BenchConfig) -> list[tuple[str, Any]]:
    """Build one ``IndexJobConfig`` per index type.

    Args:
        config: Benchmark configuration.

    Returns:
        ``(stage_name, job_config)`` pairs in build order.
    """
    from lance_etl.indexing import IndexJobConfig

    telemetry = bench_telemetry_config()
    shared: dict[str, Any] = {"telemetry": telemetry, "num_shards": config.num_shards}
    return [
        (
            "vector_ivf_rq",
            IndexJobConfig(
                vector_column="vector",
                num_partitions=config.ivf_partitions,
                metric="L2",
                vector_min_rows=config.vector_row_floor,
                **shared,
            ),
        ),
        ("btree_vector_id", IndexJobConfig(scalar_columns=["vector_id"], **shared)),
        ("bitmap_category", IndexJobConfig(bitmap_columns=["category"], **shared)),
        (
            "fts_text",
            IndexJobConfig(text_columns=["text"], fts_with_position=config.fts_with_position, **shared),
        ),
    ]


def run_index(config: BenchConfig) -> dict[str, Any]:
    """Build every index stage and record per-stage wall times and stats.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    from lance_etl.indexing import LanceIndexer

    spark = build_spark(config, "bench-index")
    uris: list[str] = config.dataset_uris()
    stages: list[dict[str, Any]] = []
    total_seconds: float = 0.0
    try:
        for name, job_config in index_stages(config):
            logger.info("building index stage %s over %d datasets", name, len(uris))
            started: float = time.perf_counter()
            stats: list[dict[str, Any]] = LanceIndexer(job_config).run(spark, uris)
            elapsed: float = time.perf_counter() - started
            total_seconds += elapsed
            stages.append({"stage": name, "seconds": round(elapsed, 3), "datasets": stats})
    finally:
        spark.stop()
    return save_phase(config, "index", {"stages": stages, "total_seconds": round(total_seconds, 3)})
