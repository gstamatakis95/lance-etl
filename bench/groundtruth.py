"""Brute-force ground truth and recall computation for the ANN benchmark.

Ground truth is computed with exact batched L2 distances in numpy so subset runs (``--limit``) and multi-tenant splits
stay self-consistent: each tenant's ground truth ranks only that tenant's base vectors and stores global vector ids.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def brute_force_topk(
    base: np.ndarray,
    base_ids: np.ndarray,
    queries: np.ndarray,
    k: int,
    base_chunk: int = 100_000,
    query_chunk: int = 1_024,
) -> np.ndarray:
    """Compute the exact k nearest base vectors per query under L2 distance.

    Distances are expanded as ``|q|^2 - 2 q.b + |b|^2`` over query and base chunks so memory stays bounded. Per-chunk
    candidates are merged into a running top-k and fully sorted at the end.

    Args:
        base: The ``(rows, dim)`` base vectors.
        base_ids: Global id per base row, returned in the result.
        queries: The ``(num_queries, dim)`` query vectors.
        k: Neighbors per query. Clamped to the base size.
        base_chunk: Base rows per distance chunk.
        query_chunk: Queries per distance chunk.

    Returns:
        An int64 ``(num_queries, k)`` array of global ids ordered nearest first.
    """
    base32: np.ndarray = np.asarray(base, dtype=np.float32)
    queries32: np.ndarray = np.asarray(queries, dtype=np.float32)
    ids: np.ndarray = np.asarray(base_ids, dtype=np.int64)
    depth: int = min(k, len(base32))
    output: np.ndarray = np.empty((len(queries32), depth), dtype=np.int64)
    for query_start in range(0, len(queries32), query_chunk):
        block: np.ndarray = queries32[query_start : query_start + query_chunk]
        block_norms: np.ndarray = np.sum(block * block, axis=1)
        best_distances: np.ndarray = np.full((len(block), depth), np.inf, dtype=np.float32)
        best_ids: np.ndarray = np.full((len(block), depth), -1, dtype=np.int64)
        for base_start in range(0, len(base32), base_chunk):
            chunk: np.ndarray = base32[base_start : base_start + base_chunk]
            chunk_norms: np.ndarray = np.sum(chunk * chunk, axis=1)
            distances: np.ndarray = block_norms[:, None] - 2.0 * (block @ chunk.T) + chunk_norms[None, :]
            take: int = min(depth, distances.shape[1])
            partition: np.ndarray = np.argpartition(distances, take - 1, axis=1)[:, :take]
            candidate_distances: np.ndarray = np.take_along_axis(distances, partition, axis=1).astype(np.float32)
            candidate_ids: np.ndarray = ids[base_start : base_start + base_chunk][partition]
            merged_distances: np.ndarray = np.concatenate([best_distances, candidate_distances], axis=1)
            merged_ids: np.ndarray = np.concatenate([best_ids, candidate_ids], axis=1)
            keep: np.ndarray = np.argpartition(merged_distances, depth - 1, axis=1)[:, :depth]
            best_distances = np.take_along_axis(merged_distances, keep, axis=1)
            best_ids = np.take_along_axis(merged_ids, keep, axis=1)
        order: np.ndarray = np.argsort(best_distances, axis=1, kind="stable")
        output[query_start : query_start + len(block)] = np.take_along_axis(best_ids, order, axis=1)
    return output


def recall_at(expected: np.ndarray, retrieved: Sequence[np.ndarray] | np.ndarray, k: int) -> float:
    """Compute mean recall@k of retrieved ids against ground-truth ids.

    Args:
        expected: Ground-truth global ids, ``(num_queries, >= k)``, nearest first.
        retrieved: Retrieved global ids per query, each at least ``k`` long when the search succeeded.
        k: The recall cut-off.

    Returns:
        The mean fraction of the true top-k found in the retrieved top-k.
    """
    total: float = 0.0
    count: int = len(expected)
    if count == 0:
        return 0.0
    for index in range(count):
        truth: np.ndarray = np.asarray(expected[index][:k], dtype=np.int64)
        found: np.ndarray = np.asarray(retrieved[index][:k], dtype=np.int64)
        total += len(np.intersect1d(truth, found)) / float(k)
    return total / count
