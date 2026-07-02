"""Build the benchmark indices with the project's real ``LanceIndexer``.

Four index stages run sequentially over all per-tenant datasets so each index type gets its own wall-time measurement:
IVF_RQ on ``vector`` (sweepable ``--num-partitions``, defaulting to the indexer's size-aware policy), BTREE on
``vector_id``, BITMAP on the low-cardinality ``category`` column, and INVERTED (BM25) on ``text``. The ``vector`` and
``text`` columns are the concrete columns the ETL pivots out of the source ``vectors`` and ``texts`` maps, so the index
handlers target them by name exactly as for any other concrete column. Each stage uses the production two-tier
orchestration in ``lance_etl.indexing`` unchanged. When ``--no-text`` is set the FTS stage is omitted entirely so no
INVERTED index is built and the three-index vector-only layout is correct.

:func:`union_index_config` builds a single :class:`~lance_etl.indexing.IndexJobConfig` covering all column types
at once. It is used by the e2e pipeline path where the production ``PipelineJob`` drives one combined ``LanceIndexer``
call instead of a separate stage per index type. The phase-major ``index`` subcommand continues to use
:func:`index_stages` so per-type wall-time measurements are preserved there.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from bench.config import BenchConfig
from bench.datasets import adapter_for
from bench.results import save_phase
from bench.spark_session import bench_telemetry_config, build_spark
from lance_etl.indexing import IndexJobConfig, LanceIndexer

logger: logging.Logger = logging.getLogger(__name__)


def union_index_config(config: BenchConfig) -> IndexJobConfig:
    """Build one :class:`~lance_etl.indexing.IndexJobConfig` covering all column types.

    Produces a single config used by the e2e pipeline path's ``PipelineJob``, which runs one
    combined ``LanceIndexer`` call rather than a separate stage per index type.  The vector
    parameters (``num_partitions``, ``metric``, ``vector_min_rows``) are wired identically to
    what :func:`index_stages` passes in its ``vector_ivf_rq`` stage.  FTS options are included
    only when ``config.no_text`` is ``False``.

    Args:
        config: Benchmark configuration.

    Returns:
        An ``IndexJobConfig`` with vector, scalar, bitmap, and (optionally) text columns set.
    """
    shared: dict[str, Any] = {"telemetry": bench_telemetry_config(), "fragments_per_index_task": config.num_shards}
    kwargs: dict[str, Any] = {
        "vector_columns": ["vector"],
        "num_partitions": config.ivf_partitions,
        "metric": adapter_for(config).metric,
        "vector_min_rows": config.vector_row_floor,
        "scalar_columns": ["vector_id"],
        "bitmap_columns": ["category"],
        **shared,
    }
    if not config.no_text:
        kwargs["text_columns"] = ["text"]
        kwargs["fts_with_position"] = config.fts_with_position
    return IndexJobConfig(**kwargs)


def index_stages(config: BenchConfig) -> list[tuple[str, IndexJobConfig]]:
    """Build one ``IndexJobConfig`` per index type.

    The FTS stage is omitted when ``config.no_text`` is True, leaving only the three vector-oriented
    stages (IVF_RQ, BTREE on vector_id, BITMAP on category).

    Args:
        config: Benchmark configuration.

    Returns:
        ``(stage_name, job_config)`` pairs in build order.
    """
    shared: dict[str, Any] = {"telemetry": bench_telemetry_config(), "fragments_per_index_task": config.num_shards}
    stages: list[tuple[str, IndexJobConfig]] = [
        (
            "vector_ivf_rq",
            IndexJobConfig(
                vector_columns=["vector"],
                num_partitions=config.ivf_partitions,
                metric=adapter_for(config).metric,
                vector_min_rows=config.vector_row_floor,
                **shared,
            ),
        ),
        ("btree_vector_id", IndexJobConfig(scalar_columns=["vector_id"], **shared)),
        ("bitmap_category", IndexJobConfig(bitmap_columns=["category"], **shared)),
    ]
    if not config.no_text:
        stages.append(
            (
                "fts_text",
                IndexJobConfig(text_columns=["text"], fts_with_position=config.fts_with_position, **shared),
            )
        )
    return stages


def run_index(config: BenchConfig) -> dict[str, Any]:
    """Build every index stage and record per-stage wall times and stats.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
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
