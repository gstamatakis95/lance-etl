"""Compact the benchmark datasets with the project's real ``MaintenanceJob``.

Records total wall time plus the fragment count of every dataset before and after the run. The fine-grained
plan/execute/commit stage timings are emitted by ``lance_etl.maintenance`` itself as Datadog distributions
(``dataset.rewrite_ms``, ``dataset.commit_ms``). This phase records the end-to-end wall time and the per-dataset
metrics dictionary the compactor returns (fragments removed/added, files removed/added, bytes reclaimed).

Compaction runs with ``defer_index_remap=False`` so covering indices are remapped inline during the commit. The
benchmark searches the datasets right after compacting, and on the pinned lance build the deferred
``__lance_frag_reuse`` path leaves indexed vector queries failing with a missing-fragment take error, so the search
phase must run against fully remapped indices.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import lance

from bench.config import BenchConfig
from bench.results import save_phase
from bench.spark_session import bench_telemetry_config, build_spark
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob

logger: logging.Logger = logging.getLogger(__name__)


def fragment_counts(uris: list[str]) -> dict[str, int]:
    """Count the fragments of each dataset.

    Args:
        uris: Dataset URIs.

    Returns:
        Fragment count per URI.
    """
    return {uri: len(lance.dataset(uri).get_fragments()) for uri in uris}


def run_compact(config: BenchConfig) -> dict[str, Any]:
    """Compact every dataset and record fragment counts before and after.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    uris: list[str] = config.dataset_uris()
    before: dict[str, int] = fragment_counts(uris)
    compaction_config = MaintenanceConfig(
        telemetry=bench_telemetry_config(),
        target_rows_per_fragment=config.compact_target_rows,
        defer_index_remap=False,
    )
    spark = build_spark(config, "bench-compact")
    try:
        started: float = time.perf_counter()
        stats: list[dict[str, Any]] = MaintenanceJob(compaction_config).run(spark, uris)
        elapsed: float = time.perf_counter() - started
    finally:
        spark.stop()
    after: dict[str, int] = fragment_counts(uris)
    return save_phase(
        config,
        "compact",
        {
            "total_seconds": round(elapsed, 3),
            "fragments_before": before,
            "fragments_after": after,
            "datasets": stats,
        },
    )
