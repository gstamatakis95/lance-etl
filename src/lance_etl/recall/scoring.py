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
version. BM25 uses two streaming passes so corpus statistics remain exact while memory stays bounded by one Arrow
batch, the query vocabulary, and the running top-k. A hybrid sample fuses the exact vector top-k and the exact BM25
top-k with the recorded fusion strategy (:func:`~lance_etl.recall.queries.fuse_legs`) before grading the served ids.
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


class DatasetOpenError(RuntimeError):
    """A recorded dataset could not be opened for a reason other than version retention."""


def recorded_version_absent(error: BaseException, version: int) -> bool:
    """Recognize the pinned Lance manifest-not-found error for one exact version.

    Args:
        error: Exact-version open failure.
        version: Requested committed version.

    Returns:
        Whether the failure specifically names the missing version manifest.
    """
    normalized: str = str(error).replace("\\", "/")
    return f"_versions/{version}.manifest was not found" in normalized


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
        skip_reason: A bounded-cardinality reason when the sample was skipped, otherwise None.
    """

    sample_id: str
    org_id: str
    k: int
    nprobes_min: int | None
    nprobes_max: int | None
    refine_factor: int | None
    recall: float | None
    skip_reason: str | None
    query_type: str = "vector"
    ndcg: float | None = None
    mrr: float | None = None


def resolve_dataset(uri: str, version: int, storage_options: dict[str, Any] | None) -> lance.LanceDataset | None:
    """Open a dataset only at the exact version that served the captured query.

    Args:
        uri: The dataset URI.
        version: The committed version that served the sampled queries.
        storage_options: Object-store options forwarded to pylance.

    Returns:
        The exact dataset version, or ``None`` when that version was retained away.

    Raises:
        DatasetOpenError: If the recorded version is invalid or an open fails for a reason other than retention.
    """
    if version < 1:
        raise DatasetOpenError(f"recorded dataset version must be positive: {version}")
    try:
        return lance.dataset(uri, version=version, storage_options=storage_options)
    except (ValueError, OSError, RuntimeError) as error:
        if not recorded_version_absent(error, version):
            raise DatasetOpenError(f"exact dataset version could not be opened: {uri}@{version}") from error
        return None


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
        if vector_column not in description.field_names:
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
    seen_served: set[Any] = set()
    dcg: float = 0.0
    for position, served_id in enumerate(served_top):
        if served_id in seen_served:
            continue
        seen_served.add(served_id)
        dcg += grade_map.get(served_id, 0) / math.log2(position + 2)
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


@dataclass(frozen=True)
class Bm25CorpusStats:
    """Bounded corpus-global statistics needed for exact BM25 scoring.

    Attributes:
        document_count: Number of documents passing the scanner filter.
        average_lengths: Average token count per queried column.
        inverse_document_frequencies: BM25 inverse document frequency per queried column and term.
    """

    document_count: int
    average_lengths: dict[str, float]
    inverse_document_frequencies: dict[tuple[str, str], float]


def collect_bm25_corpus_stats(
    dataset: lance.LanceDataset,
    field_queries: list[tuple[str, list[str], str, float]],
    filter_sql: str | None,
    batch_size: int,
) -> Bm25CorpusStats:
    """Scan text columns once to collect exact bounded BM25 corpus statistics.

    Args:
        dataset: Opened and version-pinned dataset.
        field_queries: Validated ``(column, terms, operator, boost)`` clauses.
        filter_sql: Internally generated filter string, or None.
        batch_size: Scanner batch size.

    Returns:
        Document count, per-column average lengths, and per-term inverse document frequencies.
    """
    needed_columns: list[str] = sorted({clause[0] for clause in field_queries})
    terms_by_column: dict[str, set[str]] = {column: set() for column in needed_columns}
    for column, terms, operator, boost in field_queries:
        del operator, boost
        terms_by_column[column].update(terms)
    length_sums: dict[str, int] = {column: 0 for column in needed_columns}
    document_frequencies: dict[tuple[str, str], int] = {
        (column, term): 0 for column in needed_columns for term in terms_by_column[column]
    }
    scanner: lance.LanceScanner = dataset.scanner(columns=needed_columns, filter=filter_sql, batch_size=batch_size)
    document_count: int = 0
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table: pa.Table = pa.Table.from_batches([batch])
        document_count += table.num_rows
        for column in needed_columns:
            requested_terms: set[str] = terms_by_column[column]
            for value in table.column(column).to_pylist():
                tokens: list[str] = tokenize_text(value)
                length_sums[column] += len(tokens)
                for term in requested_terms.intersection(tokens):
                    document_frequencies[(column, term)] += 1
    average_lengths: dict[str, float] = {}
    for column in needed_columns:
        total_length: int = length_sums[column]
        average_lengths[column] = total_length / document_count if document_count > 0 and total_length > 0 else 1.0
    inverse_document_frequencies: dict[tuple[str, str], float] = {}
    for key, frequency in document_frequencies.items():
        inverse_document_frequencies[key] = math.log(1.0 + (document_count - frequency + 0.5) / (frequency + 0.5))
    return Bm25CorpusStats(document_count, average_lengths, inverse_document_frequencies)


def score_bm25_document(
    column_tokens: dict[str, list[str]],
    field_queries: list[tuple[str, list[str], str, float]],
    stats: Bm25CorpusStats,
) -> tuple[float, bool]:
    """Score one document exactly from precomputed corpus-global BM25 statistics.

    Args:
        column_tokens: Token list per queried text column for one document.
        field_queries: Validated ``(column, terms, operator, boost)`` clauses.
        stats: Corpus statistics from the first streaming pass.

    Returns:
        The summed boosted score and whether at least one clause matched.
    """
    counters: dict[str, Counter[str]] = {column: Counter(tokens) for column, tokens in column_tokens.items()}
    score: float = 0.0
    matched_any: bool = False
    for column, terms, operator, boost in field_queries:
        unique_terms: list[str] = sorted(set(terms))
        if not unique_terms:
            continue
        counter: Counter[str] = counters[column]
        frequencies: list[int] = [counter.get(term, 0) for term in unique_terms]
        matched: bool = all(frequencies) if operator == "and" else any(frequencies)
        if not matched:
            continue
        matched_any = True
        average_length: float = stats.average_lengths[column]
        normalization: float = BM25_K1 * (1.0 - BM25_B + BM25_B * len(column_tokens[column]) / average_length)
        clause_score: float = 0.0
        for term, frequency in zip(unique_terms, frequencies, strict=True):
            if frequency == 0:
                continue
            inverse_document_frequency: float = stats.inverse_document_frequencies[(column, term)]
            clause_score += inverse_document_frequency * (frequency * (BM25_K1 + 1.0)) / (frequency + normalization)
        score += boost * clause_score
    return score, matched_any


def bm25_top_k(
    dataset: lance.LanceDataset,
    field_queries: list[tuple[str, list[str], str, float]],
    k: int,
    id_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], list[float], int]:
    """Compute the exact BM25 top-k ids and scores over the named text columns.

    The first pass collects only per-column length and document-frequency statistics. The second pass scores one
    batch at a time and truncates the running ranking to ``k`` after every batch. Memory is bounded by the query
    vocabulary, ``batch_size``, and ``k`` while the result remains exact. Ties break on the id column so the reference
    order is deterministic.

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
    stats: Bm25CorpusStats = collect_bm25_corpus_stats(dataset, field_queries, filter_sql, batch_size)
    if stats.document_count == 0:
        return [], [], 0
    scanner: lance.LanceScanner = dataset.scanner(
        columns=[id_column, *needed_columns], filter=filter_sql, batch_size=batch_size
    )
    top: list[tuple[Any, float]] = []
    matched_count: int = 0
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table = pa.Table.from_batches([batch])
        ids: list[Any] = table.column(id_column).to_pylist()
        values_by_column: dict[str, list[Any]] = {column: table.column(column).to_pylist() for column in needed_columns}
        for index, record_id in enumerate(ids):
            column_tokens: dict[str, list[str]] = {
                column: tokenize_text(values[index]) for column, values in values_by_column.items()
            }
            score, matched = score_bm25_document(column_tokens, field_queries, stats)
            if matched:
                matched_count += 1
                top.append((record_id, score))
        top.sort(key=lambda item: (-item[1], item[0]))
        del top[k:]
    return [item[0] for item in top], [item[1] for item in top], matched_count


def skipped_score(sample: RecallSample, reason: str) -> SampleScore:
    """Build the score record for a skipped sample.

    Args:
        sample: The sample that was skipped.
        reason: The bounded-cardinality skip reason.

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
        skip_reason=reason,
    )


def scored_sample(sample: RecallSample, recall: float, ndcg: float, mrr: float) -> SampleScore:
    """Build the score record for a successfully scored sample.

    Args:
        sample: The scored sample.
        recall: The measured recall@k.
        ndcg: The measured nDCG@k.
        mrr: The measured reciprocal rank of the exact top result.

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
        skip_reason=None,
    )


def grade_against_reference(sample: RecallSample, true_ids: list[Any], candidate_count: int) -> SampleScore:
    """Grade one served ranking against an exact single-leg reference top-k.

    Shared by the vector and text scorers and by the large-tier reduce, so every path grades identically once it holds
    the exact ground-truth ids and candidate count.

    Args:
        sample: The sample to grade.
        true_ids: The exact ground-truth ids in best-first order.
        candidate_count: The number of eligible candidates the reference was drawn from.

    Returns:
        The sample's score, skipped with ``empty_candidate_set`` when no candidate was eligible.
    """
    if candidate_count == 0:
        return skipped_score(sample, "empty_candidate_set")
    recall, ndcg, mrr = ranking_quality(true_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr)


def grade_hybrid_reference(
    sample: RecallSample,
    record_ids: list[Any],
    vector_scores: list[float],
    vector_count: int,
    text_ids: list[Any],
    text_scores: list[float],
    text_count: int,
) -> SampleScore:
    """Grade one served hybrid ranking against the exact vector and BM25 references fused with the recorded strategy.

    Shared by the whole-dataset hybrid scorer and the large-tier path, where the vector leg arrives from the
    per-fragment reduce and the BM25 leg from the whole-dataset scan.

    Args:
        sample: The hybrid sample to grade.
        record_ids: The exact vector leg ids in best-first order.
        vector_scores: The exact vector leg distances aligned with ``record_ids``.
        vector_count: The vector leg candidate count.
        text_ids: The exact BM25 leg ids in best-first order.
        text_scores: The exact BM25 leg scores aligned with ``text_ids``.
        text_count: The BM25 leg candidate count.

    Returns:
        The sample's score, skipped when both legs are empty or the fusion replay fails.
    """
    if vector_count == 0 and text_count == 0:
        return skipped_score(sample, "empty_candidate_set")
    try:
        fused_ids: list[Any] = fuse_legs(
            sample.fusion or {}, record_ids, vector_scores, text_ids, text_scores, sample.k
        )
    except FusionReplayError:
        return skipped_score(sample, "fusion_replay")
    candidate_count: int = len(fused_ids)
    recall, ndcg, mrr = ranking_quality(fused_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr)
