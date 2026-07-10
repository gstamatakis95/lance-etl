"""Exact per-sample grading: brute-force vector top-k, Okapi BM25, recall@k, nDCG@k, and MRR.

Brute-force scoring streams ``(id, vector)`` batches through numpy, keeps a per-batch partial top-k merged into a
running top-k, and computes ``recall@k = |served ids in true top-k| / min(k, candidate_count)``. The denominator is
capped at the candidate count so a perfect retrieval over a filtered set smaller than k still scores 1.0.

Beyond recall@k this module grades each served ranking with nDCG@k and MRR derived from the same distance-ordered
brute-force ground truth (no new labels). Graded relevance is the position in the exact top-k: the item at true rank
``j`` (1-based) is assigned grade ``n - j + 1`` where ``n = min(k, candidate_count)``, so the exact nearest result
carries the largest grade and an item outside the exact top-k carries grade ``0``. nDCG@k is the served ranking's DCG
over those grades divided by the ideal DCG of the exact ranking, and MRR is the reciprocal of the served rank at which
the single exact top result (the first element of the ground-truth order) appears, or ``0`` when it is absent.

A text sample is graded against an exact Okapi BM25 ranking over the named text columns at the pinned dataset
version, recomputed here as the ground-truth top-k, then graded with the same recall/nDCG/MRR functions. A hybrid
sample fuses the exact vector top-k and the exact BM25 top-k with the recorded fusion strategy
(:func:`~lance_etl.recall.queries.fuse_legs`) before grading the served ids.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from lance_etl.recall.config import BM25_B, BM25_K1, DISTANCE_TYPES
from lance_etl.recall.queries import FusionReplayError, fuse_legs, tokenize_text
from lance_etl.recall.source import RecallSample


@dataclass(frozen=True)
class SampleScore:
    """The scoring outcome for one sample.

    Attributes:
        sample_id: The sample's capture UUID.
        org_id: Organization routing component, used for the org-level table rows.
        k: The requested result count.
        query_type: The sample's query type, used for the per-query-type table rows and metric tag.
        nprobes_min: Lower nprobes bound, or None for the index default.
        nprobes_max: Upper nprobes bound, or None for the index default.
        refine_factor: Refine factor, or None when unset.
        recall: The measured recall@k, or None when the sample was skipped.
        ndcg: The measured nDCG@k, or None when the sample was skipped.
        mrr: The measured reciprocal rank of the exact top result, or None when the sample was skipped.
        version_drift: True when the recorded version was unavailable and scoring fell back to latest.
        skip_reason: A bounded-cardinality reason when the sample was skipped, otherwise None.
    """

    sample_id: str
    org_id: str
    k: int
    nprobes_min: int | None
    nprobes_max: int | None
    refine_factor: int | None
    recall: float | None
    version_drift: bool
    skip_reason: str | None
    query_type: str = "vector"
    ndcg: float | None = None
    mrr: float | None = None


def resolve_dataset(
    uri: str, version: int, storage_options: dict[str, Any] | None
) -> tuple[lance.LanceDataset | None, bool]:
    """Open a dataset checked out at the recorded version, falling back to latest on drift.

    Args:
        uri: The dataset URI.
        version: The committed version that served the sampled queries.
        storage_options: Object-store options forwarded to pylance.

    Returns:
        ``(dataset, version_drift)`` where the dataset is None when the URI cannot be opened at all, and
        ``version_drift`` is True when the recorded version was unavailable and the latest version was opened instead.
    """
    try:
        return lance.dataset(uri, version=version, storage_options=storage_options), False
    except (ValueError, OSError, RuntimeError):
        try:
            return lance.dataset(uri, storage_options=storage_options), True
        except (ValueError, OSError, RuntimeError):
            return None, False


def index_default_distance_type(dataset: lance.LanceDataset, vector_column: str) -> str:
    """Read the default distance metric from the dataset's vector index metadata.

    Samples that omit ``recall.distance_type`` were served with the index metric, so the brute-force replay must use
    the same metric. When no vector index covers the column or the statistics omit the metric, ``l2`` is returned as
    the Lance default.

    Args:
        dataset: The opened dataset.
        vector_column: The vector column the queries searched.

    Returns:
        The lowercase distance type, one of :data:`~lance_etl.recall.config.DISTANCE_TYPES` or ``l2``.
    """
    try:
        descriptions: list[Any] = dataset.describe_indices()
    except (ValueError, OSError, RuntimeError):
        return "l2"
    for description in descriptions:
        if vector_column not in getattr(description, "field_names", []):
            continue
        try:
            stats: dict[str, Any] = dataset.stats.index_stats(description.name)
        except (ValueError, OSError, RuntimeError):
            continue
        entries: list[Any] = stats.get("indices") or []
        candidates: list[Any] = [stats, *entries]
        for entry in candidates:
            if isinstance(entry, dict):
                metric: Any = entry.get("metric_type")
                if isinstance(metric, str) and metric.lower() in DISTANCE_TYPES:
                    return metric.lower()
    return "l2"


def compute_distances(candidates: np.ndarray, query: np.ndarray, distance_type: str) -> np.ndarray:
    """Compute exact distances between candidate vectors and a query.

    ``l2`` returns the squared Euclidean distance, which preserves the L2 ranking exactly and is what recall needs.
    ``cosine`` returns ``1 - cosine_similarity`` with zero-norm rows pinned to distance 1.0. ``dot`` returns the
    negated dot product matching Lance's dot distance ordering. ``hamming`` counts differing components.

    Args:
        candidates: A ``(rows, dim)`` float64 matrix of candidate vectors.
        query: A ``(dim,)`` float64 query vector.
        distance_type: One of :data:`~lance_etl.recall.config.DISTANCE_TYPES`.

    Returns:
        A ``(rows,)`` distance vector where smaller is closer.

    Raises:
        ValueError: If the distance type is unknown.
    """
    if distance_type == "l2":
        deltas: np.ndarray = candidates - query
        return np.einsum("ij,ij->i", deltas, deltas)
    if distance_type == "cosine":
        norms: np.ndarray = np.linalg.norm(candidates, axis=1) * float(np.linalg.norm(query))
        dots: np.ndarray = candidates @ query
        safe: np.ndarray = np.where(norms > 0.0, norms, 1.0)
        return np.where(norms > 0.0, 1.0 - dots / safe, 1.0)
    if distance_type == "dot":
        return -(candidates @ query)
    if distance_type == "hamming":
        return np.count_nonzero(candidates != query, axis=1).astype(np.float64)
    raise ValueError(f"unknown distance type: {distance_type!r}")


def fixed_size_list_to_numpy(column: pa.Array) -> np.ndarray:
    """Convert a fixed-size-list Arrow array into a 2-D float64 numpy matrix.

    Args:
        column: The fixed-size-list array, with nulls already filtered out.

    Returns:
        A ``(rows, dim)`` float64 matrix.
    """
    flat: pa.Array = column.flatten()
    values: np.ndarray = np.asarray(flat.to_numpy(zero_copy_only=False), dtype=np.float64)
    return values.reshape(len(column), column.type.list_size)


def merge_top_k(
    best_ids: list[Any], best_dists: np.ndarray, add_ids: list[Any], add_dists: np.ndarray, k: int
) -> tuple[list[Any], np.ndarray]:
    """Stable-merge a new partial top-k into a running top-k.

    Concatenates the running best ids and distances with the incoming partial, keeps the ``k`` smallest distances by a
    stable argsort, and carries the aligned ids. The stable sort keeps the running-best (earlier) entries ahead of the
    incoming ones on ties, so the merge order reflects scan order: this is the single primitive shared by the
    per-batch streaming merge and the per-fragment reduce, which is what makes the fanned-out reduce bit-identical to
    the single-stream scan.

    Args:
        best_ids: The running best ids in ascending-distance order.
        best_dists: The running best distances aligned with ``best_ids``.
        add_ids: The incoming partial's ids.
        add_dists: The incoming partial's distances aligned with ``add_ids``.
        k: The number of results to keep.

    Returns:
        ``(merged_ids, merged_dists)`` truncated to the ``k`` smallest distances in ascending-distance order.
    """
    merged_dists: np.ndarray = np.concatenate([best_dists, np.asarray(add_dists, dtype=np.float64)])
    merged_ids: list[Any] = best_ids + list(add_ids)
    order: np.ndarray = np.argsort(merged_dists, kind="stable")[:k]
    return [merged_ids[index] for index in order], merged_dists[order]


def reduce_partial_top_k(partials: list[tuple[list[Any], list[float]]], k: int) -> tuple[list[Any], list[float]]:
    """Reduce per-fragment partial top-k results into one exact top-k.

    Folds the partials with :func:`merge_top_k` in the order given, which the caller must supply in ascending fragment
    scan order so the result matches the single-stream scan exactly, ties included.

    Args:
        partials: One ``(ids, distances)`` partial top-k per fragment, in ascending fragment scan order.
        k: The number of results to keep.

    Returns:
        ``(true_top_k_ids, true_top_k_distances)`` in ascending-distance order.
    """
    best_ids: list[Any] = []
    best_dists: np.ndarray = np.empty(0, dtype=np.float64)
    for ids, dists in partials:
        best_ids, best_dists = merge_top_k(best_ids, best_dists, ids, np.asarray(dists, dtype=np.float64), k)
    return best_ids, best_dists.tolist()


def brute_force_top_k_scored(
    dataset: lance.LanceDataset,
    query: np.ndarray,
    k: int,
    distance_type: str,
    id_column: str,
    vector_column: str,
    filter_sql: str | None,
    batch_size: int,
    fragments: list[lance.LanceFragment] | None = None,
) -> tuple[list[Any], list[float], int]:
    """Compute the exact top-k ids and their distances for a query by scanning the dataset.

    Streams ``(id, vector)`` batches, computes exact distances per batch in numpy, and merges each batch's partial
    top-k into a running top-k so memory stays bounded by ``batch_size + k``. When ``fragments`` is given the scan is
    restricted to those fragments, which is how the large tier computes one fragment's partial top-k; the per-fragment
    partials are then reduced with :func:`reduce_partial_top_k`.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        query: The float64 query vector.
        k: The requested result count.
        distance_type: One of :data:`~lance_etl.recall.config.DISTANCE_TYPES`.
        id_column: Name of the unique id column.
        vector_column: Name of the fixed-size-list vector column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.
        fragments: The fragments to restrict the scan to, or None to scan the whole dataset.

    Returns:
        ``(true_top_k_ids, true_top_k_distances, candidate_count)`` where the ids are in ascending-distance order, the
        distances are aligned with them, and the count is the number of rows that passed the filter and carried a
        non-null vector.

    Raises:
        ValueError: If a batch's vector dimension does not match the query dimension.
    """
    scanner: lance.LanceScanner = dataset.scanner(
        columns=[id_column, vector_column], filter=filter_sql, batch_size=batch_size, fragments=fragments
    )
    best_ids: list[Any] = []
    best_dists: np.ndarray = np.empty(0, dtype=np.float64)
    candidate_count: int = 0
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table: pa.Table = pa.Table.from_batches([batch])
        vectors: pa.ChunkedArray = table.column(vector_column)
        if vectors.null_count:
            table = table.filter(pc.is_valid(vectors))
            vectors = table.column(vector_column)
        if table.num_rows == 0:
            continue
        candidates: np.ndarray = fixed_size_list_to_numpy(vectors.combine_chunks())
        if candidates.shape[1] != query.shape[0]:
            raise ValueError(
                f"query dimension {query.shape[0]} does not match dataset vector dimension {candidates.shape[1]}"
            )
        distances: np.ndarray = compute_distances(candidates, query, distance_type)
        candidate_count += table.num_rows
        best_ids, best_dists = merge_top_k(best_ids, best_dists, table.column(id_column).to_pylist(), distances, k)
    return best_ids, best_dists.tolist(), candidate_count


def brute_force_top_k(
    dataset: lance.LanceDataset,
    query: np.ndarray,
    k: int,
    distance_type: str,
    id_column: str,
    vector_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], int]:
    """Compute the exact top-k ids for a query by scanning the dataset.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        query: The float64 query vector.
        k: The requested result count.
        distance_type: One of :data:`~lance_etl.recall.config.DISTANCE_TYPES`.
        id_column: Name of the unique id column.
        vector_column: Name of the fixed-size-list vector column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.

    Returns:
        ``(true_top_k_ids, candidate_count)`` where the ids are in ascending-distance order and the count is the
        number of rows that passed the filter and carried a non-null vector.

    Raises:
        ValueError: If a batch's vector dimension does not match the query dimension.
    """
    ids, scores, count = brute_force_top_k_scored(
        dataset, query, k, distance_type, id_column, vector_column, filter_sql, batch_size
    )
    del scores
    return ids, count


def ranking_quality(
    true_ids_ordered: list[Any], served_ids: list[Any], k: int, candidate_count: int
) -> tuple[float, float, float]:
    """Grade a served ranking against the exact ground-truth order with recall@k, nDCG@k, and MRR.

    Graded relevance is the position in the exact top-k: the item at true rank ``j`` (1-based) is assigned grade
    ``n - j + 1`` where ``n = len(true_ids_ordered)``, so the exact top result carries the largest grade and an item
    outside the exact top-k carries grade ``0``. nDCG@k is the served ranking's discounted cumulative gain over those
    grades divided by the ideal discounted cumulative gain of the exact order, with the standard ``1 / log2(rank + 1)``
    position discount. MRR is the reciprocal of the served rank at which the single exact top result (the first element
    of the ground-truth order) appears, or ``0`` when it is absent from the served top-k.

    Args:
        true_ids_ordered: The exact ground-truth ids in best-first order, length ``min(k, candidate_count)``.
        served_ids: The served result ids in rank order.
        k: The requested result count.
        candidate_count: The number of eligible candidates the ground truth was drawn from.

    Returns:
        ``(recall, ndcg, mrr)`` over the served top-k.
    """
    denominator: int = min(k, candidate_count)
    served_top: list[Any] = list(served_ids)[:k]
    n: int = len(true_ids_ordered)
    grade_map: dict[Any, int] = {tid: n - idx for idx, tid in enumerate(true_ids_ordered)}
    hits: int = len(set(grade_map) & set(served_top))
    recall: float = hits / denominator if denominator > 0 else 0.0
    dcg: float = sum(grade_map.get(sid, 0) / math.log2(pos + 2) for pos, sid in enumerate(served_top))
    idcg: float = sum((n - j) / math.log2(j + 2) for j in range(n))
    ndcg: float = dcg / idcg if idcg > 0 else 0.0
    mrr: float = 0.0
    if true_ids_ordered:
        top_true: Any = true_ids_ordered[0]
        if top_true in served_top:
            mrr = 1.0 / (served_top.index(top_true) + 1)
    return recall, ndcg, mrr


def bm25_column_scores(
    token_lists: list[list[str]], query_terms: list[str], operator: str
) -> tuple[np.ndarray, np.ndarray]:
    """Compute exact Okapi BM25 scores and a match mask for one column.

    Uses the Lucene BM25 parameterization with ``k1`` of :data:`~lance_etl.recall.config.BM25_K1`, ``b`` of
    :data:`~lance_etl.recall.config.BM25_B`, and the always-positive inverse document frequency
    ``ln(1 + (N - df + 0.5) / (df + 0.5))``. A document matches under the ``or`` operator when it contains at least
    one query term and under the ``and`` operator when it contains all of them.

    Args:
        token_lists: One token list per document, aligned with the candidate order.
        query_terms: The tokenized query terms.
        operator: ``or`` or ``and``.

    Returns:
        ``(scores, matched)`` where ``scores`` holds the BM25 score per document and ``matched`` is the boolean match
        mask per document.
    """
    count: int = len(token_lists)
    scores: np.ndarray = np.zeros(count, dtype=np.float64)
    if count == 0 or not query_terms:
        return scores, np.zeros(count, dtype=bool)
    lengths: np.ndarray = np.asarray([len(tokens) for tokens in token_lists], dtype=np.float64)
    avgdl: float = float(lengths.mean()) if lengths.sum() > 0 else 1.0
    if avgdl == 0.0:
        avgdl = 1.0
    counters: list[Counter[str]] = [Counter(tokens) for tokens in token_lists]
    unique_terms: list[str] = sorted(set(query_terms))
    present: dict[str, np.ndarray] = {}
    for term in unique_terms:
        tf: np.ndarray = np.asarray([counter.get(term, 0) for counter in counters], dtype=np.float64)
        present[term] = tf
        df: int = int(np.count_nonzero(tf))
        idf: float = math.log(1.0 + (count - df + 0.5) / (df + 0.5))
        denom: np.ndarray = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * lengths / avgdl)
        scores += np.where(tf > 0, idf * (tf * (BM25_K1 + 1.0)) / denom, 0.0)
    term_present: np.ndarray = np.vstack([present[term] > 0 for term in unique_terms])
    matched: np.ndarray = term_present.all(axis=0) if operator == "and" else term_present.any(axis=0)
    return scores, matched


def bm25_top_k(
    dataset: lance.LanceDataset,
    field_queries: list[tuple[str, list[str], str, float]],
    k: int,
    id_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], list[float], int]:
    """Compute the exact BM25 top-k ids and scores over the named text columns.

    Materializes the candidate text columns at the pinned version, computes per-column BM25 with
    :func:`bm25_column_scores`, sums the boosted column scores, and ranks the documents that matched at least one
    clause. Ties break on the id column so the reference order is deterministic.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        field_queries: The ``(column, terms, operator, boost)`` clauses to score.
        k: The requested result count.
        id_column: Name of the unique id column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.

    Returns:
        ``(true_top_k_ids, true_top_k_scores, candidate_count)`` where the ids are in descending-score order, the
        scores are aligned with them, and the count is the number of documents that matched at least one clause.
    """
    needed_columns: list[str] = sorted({clause[0] for clause in field_queries})
    scanner: lance.LanceScanner = dataset.scanner(
        columns=[id_column, *needed_columns], filter=filter_sql, batch_size=batch_size
    )
    ids: list[Any] = []
    column_tokens: dict[str, list[list[str]]] = {column: [] for column in needed_columns}
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table: pa.Table = pa.Table.from_batches([batch])
        ids.extend(table.column(id_column).to_pylist())
        for column in needed_columns:
            column_tokens[column].extend(tokenize_text(value) for value in table.column(column).to_pylist())
    total: int = len(ids)
    if total == 0:
        return [], [], 0
    scores: np.ndarray = np.zeros(total, dtype=np.float64)
    matched_any: np.ndarray = np.zeros(total, dtype=bool)
    for column, terms, operator, boost in field_queries:
        column_scores, matched = bm25_column_scores(column_tokens[column], terms, operator)
        scores += boost * np.where(matched, column_scores, 0.0)
        matched_any |= matched
    matched_indices: list[int] = [index for index in range(total) if matched_any[index]]
    matched_indices.sort(key=lambda index: (-scores[index], ids[index]))
    top: list[int] = matched_indices[:k]
    return [ids[index] for index in top], [float(scores[index]) for index in top], len(matched_indices)


def skipped_score(sample: RecallSample, reason: str, version_drift: bool = False) -> SampleScore:
    """Build the score record for a skipped sample.

    Args:
        sample: The sample that was skipped.
        reason: The bounded-cardinality skip reason.
        version_drift: Whether the dataset was opened at a drifted version before the skip.

    Returns:
        A score with ``recall=None`` and the reason recorded.
    """
    return SampleScore(
        sample_id=sample.sample_id,
        org_id=sample.org_id,
        k=sample.k,
        query_type=sample.query_type,
        nprobes_min=sample.nprobes_min,
        nprobes_max=sample.nprobes_max,
        refine_factor=sample.refine_factor,
        recall=None,
        version_drift=version_drift,
        skip_reason=reason,
    )


def scored_sample(sample: RecallSample, recall: float, ndcg: float, mrr: float, version_drift: bool) -> SampleScore:
    """Build the score record for a successfully scored sample.

    Args:
        sample: The scored sample.
        recall: The measured recall@k.
        ndcg: The measured nDCG@k.
        mrr: The measured reciprocal rank of the exact top result.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The populated score record.
    """
    return SampleScore(
        sample_id=sample.sample_id,
        org_id=sample.org_id,
        k=sample.k,
        query_type=sample.query_type,
        nprobes_min=sample.nprobes_min,
        nprobes_max=sample.nprobes_max,
        refine_factor=sample.refine_factor,
        recall=recall,
        ndcg=ndcg,
        mrr=mrr,
        version_drift=version_drift,
        skip_reason=None,
    )


def grade_against_reference(
    sample: RecallSample, true_ids: list[Any], candidate_count: int, version_drift: bool
) -> SampleScore:
    """Grade one served ranking against an exact single-leg reference top-k.

    Shared by the vector and text scorers and by the large-tier reduce, so every path grades identically once it holds
    the exact ground-truth ids and candidate count.

    Args:
        sample: The sample to grade.
        true_ids: The exact ground-truth ids in best-first order.
        candidate_count: The number of eligible candidates the reference was drawn from.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, skipped with ``empty_candidate_set`` when no candidate was eligible.
    """
    if candidate_count == 0:
        return skipped_score(sample, "empty_candidate_set", version_drift)
    recall, ndcg, mrr = ranking_quality(true_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr, version_drift)


def grade_hybrid_reference(
    sample: RecallSample,
    vector_ids: list[Any],
    vector_scores: list[float],
    vector_count: int,
    text_ids: list[Any],
    text_scores: list[float],
    text_count: int,
    version_drift: bool,
) -> SampleScore:
    """Grade one served hybrid ranking against the exact vector and BM25 references fused with the recorded strategy.

    Shared by the whole-dataset hybrid scorer and the large-tier path, where the vector leg arrives from the
    per-fragment reduce and the BM25 leg from the whole-dataset scan.

    Args:
        sample: The hybrid sample to grade.
        vector_ids: The exact vector leg ids in best-first order.
        vector_scores: The exact vector leg distances aligned with ``vector_ids``.
        vector_count: The vector leg candidate count.
        text_ids: The exact BM25 leg ids in best-first order.
        text_scores: The exact BM25 leg scores aligned with ``text_ids``.
        text_count: The BM25 leg candidate count.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, skipped when both legs are empty or the fusion replay fails.
    """
    if vector_count == 0 and text_count == 0:
        return skipped_score(sample, "empty_candidate_set", version_drift)
    try:
        fused_ids: list[Any] = fuse_legs(
            sample.fusion or {}, vector_ids, vector_scores, text_ids, text_scores, sample.k
        )
    except FusionReplayError:
        return skipped_score(sample, "fusion_replay", version_drift)
    candidate_count: int = len(fused_ids)
    recall, ndcg, mrr = ranking_quality(fused_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr, version_drift)
