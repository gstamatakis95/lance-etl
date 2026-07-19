"""Unit tests for the bench reconciler helpers that need neither Spark nor PostgreSQL."""

from __future__ import annotations

from dataclasses import replace

import pytest

from bench.config import BenchConfig
from bench.reconcile import batch_windows_by_ordinal, bench_spec_index_names


def bench_config(limit: int, batches: int) -> BenchConfig:
    """Build a minimal e2e configuration for the bigann adapter.

    Args:
        limit: Corpus row budget.
        batches: Requested append batch count.

    Returns:
        A configuration targeting the bigann adapter at the requested scale.
    """
    return replace(BenchConfig(command="e2e", dataset="bigann"), limit=limit, batches=batches)


def test_batch_windows_cover_the_corpus_without_gaps() -> None:
    """Consecutive windows tile the whole ordinal range with strictly positive width."""
    windows: list[tuple[int, int]] = batch_windows_by_ordinal(bench_config(1_200, 4))
    assert len(windows) == 4
    assert windows[0][0] == 0
    assert windows[-1][1] == 1_200
    for first, last in windows:
        assert last > first
    for index in range(len(windows) - 1):
        assert windows[index][1] == windows[index + 1][0]


def test_batches_above_limit_raises_readable_error() -> None:
    """More batches than rows would drive a repartition(0) and is rejected up front."""
    with pytest.raises(ValueError, match="exceeds the row budget"):
        batch_windows_by_ordinal(bench_config(4, 8))


def test_non_positive_batches_raise() -> None:
    """A non-positive batch count is rejected before any window arithmetic runs."""
    with pytest.raises(ValueError, match="must be positive"):
        batch_windows_by_ordinal(bench_config(1_200, 0))


def test_bench_spec_index_names_include_every_production_family() -> None:
    """The bench spec declares all six production indexes including the INVERTED full-text index."""
    names: frozenset[str] = bench_spec_index_names(bench_config(1_200, 2))
    assert names == frozenset(
        {
            "vector_idx",
            "text_fts_idx",
            "cluster_idx",
            "ts_idx",
            "ts_zonemap_idx",
            "is_deleted_bitmap_idx",
        }
    )
