"""Unit tests for the bench recall helpers fixed in the retrospective review.

Covers the ground-truth alignment of :func:`bench.e2e.org_catalog_recall` when individual
queries error mid-sweep, and the deterministic id tie-breaking plus ragged-width handling of
:func:`bench.groundtruth.merge_topk_partials`. Both were review findings: the former silently
deflated recall by comparing surviving results against the wrong ground-truth rows, and the
latter made ground truth depend on ``rows_per_slice`` when distances tied.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest

import bench.e2e as bench_e2e
from bench.config import RECALL_CUTOFFS, BenchConfig
from bench.groundtruth import brute_force_topk, brute_force_topk_scored, merge_topk_partials


def alignment_config(tmp_path_str: str) -> BenchConfig:
    """Build a single-tenant bench configuration for the alignment test.

    ``search_k`` covers the deepest :data:`RECALL_CUTOFFS` depth so ``BenchConfig`` construction
    passes its own validation. The stubbed search in this module ignores the ``k`` argument
    entirely, so the exact value carries no bearing on what is asserted here.

    Args:
        tmp_path_str: Workspace directory for the config.

    Returns:
        A configuration with one org and a ``search_k`` covering every recall cutoff.
    """
    return BenchConfig(command="e2e", workspace=tmp_path_str, tenants=1, search_k=max(RECALL_CUTOFFS))


class TestOrgRecallAlignment:
    """org_catalog_recall scores surviving queries against their own ground-truth rows."""

    def test_failed_query_does_not_shift_ground_truth(self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """A query that errors mid-sweep is excluded by index, not by truncation.

        Query 1 fails. Queries 0, 2, and 3 return exactly their own ground-truth rows, so
        recall must be 1.0. Pre-fix, query 2's result was compared against query 1's truth and
        recall dropped spuriously.
        """
        ground_truth: np.ndarray = np.array([[0, 1, 2], [10, 11, 12], [20, 21, 22], [30, 31, 32]], dtype=np.int64)
        queries: np.ndarray = np.zeros((4, 8), dtype=np.float32)

        def fake_search(
            stub: Any,
            pb2: Any,
            config: BenchConfig,
            org: str,
            query: np.ndarray,
            k: int,
            expected_version: int,
        ) -> tuple[Any, float]:
            """Return each query's own truth, failing on query index 1."""
            del stub, pb2, config, org, query, k, expected_version
            index: int = fake_search.calls
            fake_search.calls += 1
            if index == 1:
                raise RuntimeError("transient gRPC failure")
            return SimpleNamespace(results=ground_truth[index]), 1.0

        fake_search.calls = 0
        monkeypatch.setattr(bench_e2e, "vector_search", fake_search)
        monkeypatch.setattr(bench_e2e, "result_vector_ids", lambda results: np.asarray(results, dtype=np.int64))

        point = bench_e2e.org_catalog_recall(
            object(), object(), alignment_config(str(tmp_path)), "org0", queries, ground_truth, 1
        )
        assert point is not None
        assert point["queries"] == 3
        assert point["failed_queries"] == 1
        assert point["recall_at_1"] == 1.0

    def test_all_queries_failing_returns_none(self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """A sweep where every query errors yields None instead of a fake point."""

        def always_fail(*args: Any, **kwargs: Any) -> tuple[Any, float]:
            """Raise for every query."""
            del args, kwargs
            raise RuntimeError("server down")

        monkeypatch.setattr(bench_e2e, "vector_search", always_fail)
        point = bench_e2e.org_catalog_recall(
            object(),
            object(),
            alignment_config(str(tmp_path)),
            "org0",
            np.zeros((2, 8), dtype=np.float32),
            np.zeros((2, 3), dtype=np.int64),
            1,
        )
        assert point is None


class TestMergeTopkPartials:
    """merge_topk_partials is exact, ragged-width safe, and id-deterministic on ties."""

    def test_ragged_widths_merge_exactly(self) -> None:
        """Partials of different widths merge into the exact global top-k."""
        rng: np.random.Generator = np.random.default_rng(7)
        base: np.ndarray = rng.random((60, 4), dtype=np.float32)
        queries: np.ndarray = rng.random((5, 4), dtype=np.float32)
        ids: np.ndarray = np.arange(60, dtype=np.int64)
        first = brute_force_topk_scored(base[:50], ids[:50], queries, 10)
        second = brute_force_topk_scored(base[50:], ids[50:], queries, 10)
        assert first[0].shape[1] == 10
        assert second[0].shape[1] == 10
        merged: np.ndarray = merge_topk_partials([first, second], 10)
        reference: np.ndarray = brute_force_topk(base, ids, queries, 10)
        np.testing.assert_array_equal(merged, reference)

    def test_short_partial_widths_are_accepted(self) -> None:
        """A tenant slice with fewer than k rows contributes a narrower partial without error."""
        rng: np.random.Generator = np.random.default_rng(11)
        queries: np.ndarray = rng.random((3, 4), dtype=np.float32)
        wide = brute_force_topk_scored(rng.random((20, 4), dtype=np.float32), np.arange(20, dtype=np.int64), queries, 8)
        narrow = brute_force_topk_scored(
            rng.random((2, 4), dtype=np.float32), np.array([100, 101], dtype=np.int64), queries, 8
        )
        assert narrow[0].shape[1] == 2
        merged: np.ndarray = merge_topk_partials([wide, narrow], 8)
        assert merged.shape == (3, 8)

    def test_equal_distances_break_ties_by_id(self) -> None:
        """Duplicate distances select the lowest ids regardless of partial order."""
        distances: np.ndarray = np.zeros((1, 3), dtype=np.float32)
        low = (np.array([[5, 7, 9]], dtype=np.int64), distances)
        high = (np.array([[6, 8, 10]], dtype=np.int64), distances)
        forward: np.ndarray = merge_topk_partials([low, high], 4)
        backward: np.ndarray = merge_topk_partials([high, low], 4)
        np.testing.assert_array_equal(forward, np.array([[5, 6, 7, 8]], dtype=np.int64))
        np.testing.assert_array_equal(forward, backward)
