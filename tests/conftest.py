"""Shared fixtures for the lance-etl test suite.

Disables ddtrace agent flushing before any module under test imports ddtrace, and provides telemetry and dataset-builder
fixtures used across the suite. Also puts the repository root on ``sys.path`` so the un-packaged top-level ``bench``
benchmarking package is importable by the ``test_bench_*`` modules.
"""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path

os.environ.setdefault("DD_TRACE_ENABLED", "false")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lance
import pyarrow as pa
import pytest

from lance_etl.telemetry import Telemetry, TelemetryConfig


@pytest.fixture
def telemetry_config() -> TelemetryConfig:
    """Return a telemetry configuration pointing at a local statsd sink.

    Returns:
        A default telemetry configuration; DogStatsD sends are fire-and-forget
        UDP so no agent needs to listen.
    """
    return TelemetryConfig(service="lance-etl-tests", env="test")


@pytest.fixture
def telemetry(telemetry_config: TelemetryConfig) -> Telemetry:
    """Return a telemetry facade without the Lance event bridge.

    Args:
        telemetry_config: The test telemetry configuration.

    Returns:
        A telemetry facade safe to use offline.
    """
    return Telemetry.create(telemetry_config, attach_lance_bridge=False)


def make_vector_table(rows: int, dim: int, seed: int = 7) -> pa.Table:
    """Build a table with id, vector, category, and text columns.

    Args:
        rows: Number of rows to generate.
        dim: Fixed-size-list vector dimension.
        seed: Random seed for reproducible vectors.

    Returns:
        The generated table.
    """
    rng: random.Random = random.Random(seed)
    values: pa.Array = pa.array([rng.random() for _ in range(rows * dim)], pa.float32())
    vectors: pa.Array = pa.FixedSizeListArray.from_arrays(values, dim)
    return pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "vector": vectors,
            "category": pa.array([f"cat{i % 4}" for i in range(rows)]),
            "text": pa.array([f"word{i % 10} common" for i in range(rows)]),
        }
    )


def write_fragmented_dataset(uri: str, table: pa.Table, max_rows_per_file: int) -> lance.LanceDataset:
    """Write a table to a Lance dataset split into multiple fragments.

    Args:
        uri: Destination dataset URI.
        table: The table to write.
        max_rows_per_file: Row cap per fragment file, controlling fragment count.

    Returns:
        The written dataset handle.
    """
    return lance.write_dataset(table, uri, max_rows_per_file=max_rows_per_file)
