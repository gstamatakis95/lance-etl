"""Run the real Iceberg-to-Lance ETL over the benchmark source table.

Drives the production :meth:`lance_etl.etl.IcebergToLanceETL.run` end to end, source read included: the same
configuration the ``lance-etl etl`` CLI would build (routing columns, last-write-wins collapse on ``updated_at``, the
fixed-size-list vector cast, the routing shuffle, and the executor-side ``merge_insert``) plus the production
``apply_window_filter`` pushdown. ``read_increment`` resolves the snapshot window through the ``{table}.snapshots``
metadata table. The benchmark passes a snapshot window of ``[0, now]`` that brackets the table's entire history, so no
snapshot precedes the window start and the read takes the production first-run fallback — a full batch scan pinned to
the window's last snapshot via the ``snapshot-id`` option. The source table is written once by prepare, so the
``updated_at`` window filter is the per-batch slicer.

With ``--batches B`` the synthetic day is split into B consecutive windows and the ETL runs once per window, producing
B merge commits (and therefore multiple fragments) per dataset for the compaction phase to consume.
"""

from __future__ import annotations

import logging
import shutil
import time
from datetime import timedelta
from typing import Any

import lance

from bench.config import BenchConfig
from bench.datasets import adapter_for
from bench.prepare import BASE_DAY, MINUTES_PER_DAY
from bench.results import save_phase
from bench.spark_session import bench_telemetry_config, build_spark
from lance_etl.arrow_types import resolve_type_map
from lance_etl.etl import ETLConfig, IcebergToLanceETL

logger: logging.Logger = logging.getLogger(__name__)


def batch_windows(batches: int) -> list[tuple[str, str]]:
    """Split the synthetic day into consecutive ``updated_at`` windows.

    Args:
        batches: Number of windows.

    Returns:
        ``(start, end)`` Spark timestamp literals, end-exclusive, covering the whole day.
    """
    boundaries: list[int] = [batch * MINUTES_PER_DAY // batches for batch in range(batches)] + [MINUTES_PER_DAY]
    windows: list[tuple[str, str]] = []
    for index in range(batches):
        start: str = (BASE_DAY + timedelta(minutes=boundaries[index])).strftime("%Y-%m-%d %H:%M:%S")
        end: str = (BASE_DAY + timedelta(minutes=boundaries[index + 1])).strftime("%Y-%m-%d %H:%M:%S")
        windows.append((start, end))
    return windows


def etl_config(config: BenchConfig, dimension: int, window: tuple[str, str]) -> ETLConfig:
    """Build the production ETL configuration for one batch window.

    Mirrors exactly what ``lance-etl etl`` builds from the benchmark argv: every field not listed keeps the CLI
    default, which equals the ``ETLConfig`` default.

    Args:
        config: Benchmark configuration.
        dimension: The dataset's vector dimension, driving the fixed-size-list cast.
        window: The ``updated_at`` window literals for the pushdown filter.

    Returns:
        The ETL configuration for the window.
    """
    return ETLConfig(
        base_uri=str(config.lance_root()),
        telemetry=bench_telemetry_config(),
        ts_col="updated_at",
        column_types=resolve_type_map({"vector": f"fixed_size_list<float32,{dimension}>"}),
        num_partitions=config.etl_partitions,
        window_start=window[0],
        window_end=window[1],
        window_column="updated_at",
    )


def run_etl_window(config: BenchConfig, dimension: int, index: int, window: tuple[str, str]) -> float:
    """Run the production ETL for one batch window and return its wall time.

    Calls the production :meth:`lance_etl.etl.IcebergToLanceETL.run` with a snapshot window of ``[0, now]``: the
    source table is written once by prepare, so every batch reads the same pinned snapshot through the
    ``read_increment`` first-run fallback and the ``updated_at`` window filter slices out this batch's rows.

    Args:
        config: Benchmark configuration.
        dimension: The dataset's vector dimension.
        index: Zero-based window index, used for the Spark application name.
        window: The ``updated_at`` window literals.

    Returns:
        The wall time of the window's ETL run in seconds.
    """
    spark = build_spark(config, f"bench-ingest-{index}")
    started: float = time.perf_counter()
    try:
        etl: IcebergToLanceETL = IcebergToLanceETL(etl_config(config, dimension, window))
        etl.run(spark, config.table(), 0, int(time.time() * 1000))
    finally:
        spark.stop()
    return time.perf_counter() - started


def dataset_row_counts(config: BenchConfig) -> dict[str, int]:
    """Count the rows of every produced Lance dataset.

    Args:
        config: Benchmark configuration.

    Returns:
        Row count per dataset URI.
    """
    return {uri: lance.dataset(uri).count_rows() for uri in config.dataset_uris()}


def run_ingest(config: BenchConfig) -> dict[str, Any]:
    """Run the real ETL once per batch window and record throughput.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    lance_root = config.lance_root()
    if lance_root.exists():
        shutil.rmtree(lance_root)

    dimension: int = adapter_for(config).dimension
    windows: list[tuple[str, str]] = batch_windows(config.batches)
    batch_results: list[dict[str, Any]] = []
    total_seconds: float = 0.0
    for index, window in enumerate(windows):
        logger.info("etl batch %d/%d window [%s, %s)", index + 1, len(windows), window[0], window[1])
        elapsed: float = run_etl_window(config, dimension, index, window)
        total_seconds += elapsed
        batch_results.append({"window_start": window[0], "window_end": window[1], "seconds": round(elapsed, 3)})

    rows: dict[str, int] = dataset_row_counts(config)
    total_rows: int = sum(rows.values())
    return save_phase(
        config,
        "ingest",
        {
            "batches": batch_results,
            "total_seconds": round(total_seconds, 3),
            "total_rows": total_rows,
            "rows_per_second": round(total_rows / total_seconds, 1) if total_seconds else 0.0,
            "dataset_rows": rows,
        },
    )
