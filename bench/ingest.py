"""Run the real Iceberg-to-Lance ETL over the benchmark source table.

Drives ``lance_etl.cli.main`` with a constructed argv for full fidelity: the same code path production uses, including
the Iceberg incremental read, the ``--window-start`` / ``--window-end`` timestamp pushdown flags, the routing shuffle,
and the executor-side ``merge_insert``. With ``--batches B`` the synthetic day is split into B consecutive windows and
the ETL runs once per window, producing B merge commits (and therefore multiple fragments) per dataset for the
compaction phase to consume.

The Spark session is created here with the Iceberg catalog configuration before each CLI invocation; the CLI's
``getOrCreate`` then reuses it and stops it when the window finishes.
"""

from __future__ import annotations

import logging
import shutil
import time
from datetime import timedelta
from typing import Any

import lance

from bench.config import BenchConfig
from bench.prepare import BASE_DAY, MINUTES_PER_DAY
from bench.results import save_phase
from bench.spark_session import build_spark

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


def snapshot_bounds_ms(config: BenchConfig) -> tuple[int, int]:
    """Read the Iceberg table's snapshot commit-time bounds for the incremental read.

    Args:
        config: Benchmark configuration.

    Returns:
        ``(start_ms, end_ms)`` bracketing every snapshot of the table.
    """
    spark = build_spark(config, "bench-ingest-bounds")
    row = spark.sql(
        f"SELECT unix_millis(MIN(committed_at)) AS start_ms, unix_millis(MAX(committed_at)) AS end_ms "
        f"FROM {config.table()}.snapshots"
    ).collect()[0]
    return int(row["start_ms"]) - 60_000, int(row["end_ms"]) + 60_000


def etl_argv(config: BenchConfig, start_ms: int, end_ms: int, window: tuple[str, str]) -> list[str]:
    """Build the ``lance-etl etl`` argument vector for one batch window.

    Args:
        config: Benchmark configuration.
        start_ms: Iceberg incremental-read start in epoch milliseconds.
        end_ms: Iceberg incremental-read end in epoch milliseconds.
        window: The ``updated_at`` window literals for the pushdown filter.

    Returns:
        The argv handed to ``lance_etl.cli.main``.
    """
    from bench.config import SIFT_DIM

    return [
        "--app-name",
        "bench-etl",
        "etl",
        "--table",
        config.table(),
        "--start",
        str(start_ms),
        "--end",
        str(end_ms),
        "--base-uri",
        str(config.lance_root()),
        "--ts-col",
        "updated_at",
        "--column-type",
        f"vector=fixed_size_list<float32,{SIFT_DIM}>",
        "--num-partitions",
        str(config.etl_partitions),
        "--retry-timeout",
        "120",
        "--window-column",
        "updated_at",
        "--window-start",
        window[0],
        "--window-end",
        window[1],
        "--dd-service",
        "lance-bench",
        "--dd-env",
        "bench",
    ]


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

    Raises:
        RuntimeError: If any ETL invocation exits non-zero.
    """
    from lance_etl.cli import main as lance_etl_main

    lance_root = config.lance_root()
    if lance_root.exists():
        shutil.rmtree(lance_root)

    start_ms, end_ms = snapshot_bounds_ms(config)
    windows: list[tuple[str, str]] = batch_windows(config.batches)
    batch_results: list[dict[str, Any]] = []
    total_seconds: float = 0.0
    for index, window in enumerate(windows):
        build_spark(config, f"bench-ingest-{index}")
        argv: list[str] = etl_argv(config, start_ms, end_ms, window)
        logger.info("etl batch %d/%d window [%s, %s)", index + 1, len(windows), window[0], window[1])
        started: float = time.perf_counter()
        exit_code: int = lance_etl_main(argv)
        elapsed: float = time.perf_counter() - started
        if exit_code != 0:
            raise RuntimeError(f"lance-etl etl exited {exit_code} for window {window}")
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
