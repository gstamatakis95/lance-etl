"""On-disk size measurement of the per-tenant Lance datasets.

Walks every ``*.lance`` directory under the Lance root and splits bytes by top-level layout
directory: raw data (``data/``), indices (``_indices/``), and metadata (``_versions/``,
``_transactions/``, ``_deletions/``, and anything else). This is the size axis of the
latency/recall/size experiment loop, measured directly from the filesystem with no Spark or
Lance open involved.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger: logging.Logger = logging.getLogger(__name__)

DATA_DIR: str = "data"

INDICES_DIR: str = "_indices"


def directory_bytes(root: Path) -> int:
    """Sum the size of every regular file under a directory.

    Args:
        root: The directory to walk. A missing directory counts as zero bytes.

    Returns:
        Total bytes across all files, recursively.
    """
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def dataset_sizes(dataset_dir: Path) -> dict[str, Any]:
    """Measure one dataset's on-disk footprint split by layout directory.

    Args:
        dataset_dir: The ``*.lance`` dataset directory.

    Returns:
        Bytes for data, indices, metadata (everything else), and the total.
    """
    data_bytes: int = directory_bytes(dataset_dir / DATA_DIR)
    index_bytes: int = directory_bytes(dataset_dir / INDICES_DIR)
    total_bytes: int = directory_bytes(dataset_dir)
    return {
        "dataset": dataset_dir.name,
        "data_bytes": data_bytes,
        "index_bytes": index_bytes,
        "meta_bytes": total_bytes - data_bytes - index_bytes,
        "total_bytes": total_bytes,
    }


def measure_dataset_sizes(lance_root: Path) -> dict[str, Any]:
    """Measure every dataset under the Lance root and aggregate fleet totals.

    Args:
        lance_root: The base directory holding the per-tenant ``*.lance`` datasets.

    Returns:
        Per-dataset size records plus fleet totals and the index-to-data overhead ratio.
    """
    datasets: list[dict[str, Any]] = [
        dataset_sizes(path) for path in sorted(lance_root.rglob("*.lance")) if path.is_dir()
    ]
    data_bytes: int = sum(record["data_bytes"] for record in datasets)
    index_bytes: int = sum(record["index_bytes"] for record in datasets)
    meta_bytes: int = sum(record["meta_bytes"] for record in datasets)
    total_bytes: int = sum(record["total_bytes"] for record in datasets)
    return {
        "datasets": datasets,
        "dataset_count": len(datasets),
        "data_bytes": data_bytes,
        "index_bytes": index_bytes,
        "meta_bytes": meta_bytes,
        "total_bytes": total_bytes,
        "index_to_data_ratio": round(index_bytes / data_bytes, 4) if data_bytes else 0.0,
    }
