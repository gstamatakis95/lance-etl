"""Unit tests for the bench brute-force ground truth and recall computation."""

from __future__ import annotations

import numpy as np

from bench.groundtruth import brute_force_topk, recall_at


def reference_topk(base: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    """Compute the exact top-k by full distance matrix and argsort.

    Args:
        base: The base vectors.
        queries: The query vectors.
        k: Neighbors per query.

    Returns:
        Row indices of the nearest base vectors per query.
    """
    distances: np.ndarray = ((queries[:, None, :] - base[None, :, :]) ** 2).sum(axis=2)
    return np.argsort(distances, axis=1)[:, :k]


class TestBruteForceTopk:
    """brute_force_topk matches a naive argsort reference."""

    def test_matches_argsort_reference(self) -> None:
        """The chunked merge produces exactly the argsort result on a toy set."""
        rng: np.random.Generator = np.random.default_rng(11)
        base: np.ndarray = rng.normal(size=(57, 8)).astype(np.float32)
        queries: np.ndarray = rng.normal(size=(9, 8)).astype(np.float32)
        expected: np.ndarray = reference_topk(base, queries, 10)
        actual: np.ndarray = brute_force_topk(base, np.arange(len(base)), queries, 10, base_chunk=13, query_chunk=4)
        np.testing.assert_array_equal(actual, expected)

    def test_returns_global_ids(self) -> None:
        """The provided base ids are returned instead of positional indices."""
        rng: np.random.Generator = np.random.default_rng(5)
        base: np.ndarray = rng.normal(size=(20, 4)).astype(np.float32)
        ids: np.ndarray = np.arange(3, 3 + 20 * 7, 7, dtype=np.int64)
        queries: np.ndarray = base[:2] + 0.001
        actual: np.ndarray = brute_force_topk(base, ids, queries, 1, base_chunk=6)
        np.testing.assert_array_equal(actual[:, 0], ids[:2])

    def test_k_clamped_to_base_size(self) -> None:
        """Requesting more neighbors than base vectors clamps the depth."""
        base: np.ndarray = np.asarray([[0.0, 0.0], [1.0, 1.0]], dtype=np.float32)
        queries: np.ndarray = np.asarray([[0.1, 0.1]], dtype=np.float32)
        actual: np.ndarray = brute_force_topk(base, np.arange(2), queries, 10)
        assert actual.shape == (1, 2)
        np.testing.assert_array_equal(actual[0], [0, 1])


class TestRecallAt:
    """recall_at computes mean overlap fractions."""

    def test_perfect_recall(self) -> None:
        """Identical retrieved and expected ids score 1.0."""
        expected: np.ndarray = np.asarray([[1, 2, 3, 4], [5, 6, 7, 8]], dtype=np.int64)
        retrieved: list[np.ndarray] = [np.asarray([3, 1, 2, 4]), np.asarray([8, 7, 6, 5])]
        assert recall_at(expected, retrieved, 4) == 1.0

    def test_partial_recall(self) -> None:
        """Half overlap scores 0.5."""
        expected: np.ndarray = np.asarray([[1, 2, 3, 4]], dtype=np.int64)
        retrieved: list[np.ndarray] = [np.asarray([1, 2, 90, 91])]
        assert recall_at(expected, retrieved, 4) == 0.5

    def test_cutoff_limits_both_sides(self) -> None:
        """Only the first k of each side participate."""
        expected: np.ndarray = np.asarray([[1, 2, 3, 4]], dtype=np.int64)
        retrieved: list[np.ndarray] = [np.asarray([1, 99, 2, 3])]
        assert recall_at(expected, retrieved, 2) == 0.5

    def test_short_retrieval_counts_misses(self) -> None:
        """Retrieving fewer than k rows lowers recall instead of erroring."""
        expected: np.ndarray = np.asarray([[1, 2, 3, 4]], dtype=np.int64)
        retrieved: list[np.ndarray] = [np.asarray([1])]
        assert recall_at(expected, retrieved, 4) == 0.25

    def test_empty_query_set(self) -> None:
        """An empty query set scores zero instead of dividing by zero."""
        assert recall_at(np.empty((0, 4), dtype=np.int64), [], 4) == 0.0
