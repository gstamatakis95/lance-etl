"""RecallAuditJob and the two-tier Spark fan-out that scores every sampled query and reports retrieval quality.

The driver groups parsed samples by ``(dataset_uri, dataset_version)`` and fans the groups out to Spark executors
with ``parallelize().map()``, mirroring the established executor patterns in ``etl.py`` and ``indexing.py``. A probe
job classifies each group by fragment count: the tail of tiny groups is packed into the small tier where one task
scores many datasets end to end, and big groups go to the large tier where the vector brute force fans out per
fragment and reduces keyed partials exactly on executors, while the BM25 leg (whose corpus-global statistics cannot
be sharded) stays whole-dataset. Each executor opens its dataset only at the recorded version and
skips samples whose exact version is unavailable. The driver receives one reduced leg per sample,
aggregates the scores into a report table, logs it, and emits bounded-cardinality Datadog gauges per
RPC and query-type bucket.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import lance
import numpy as np
from pyspark.sql import SparkSession

from lance_etl.recall.config import BATCH_SIZE, RecallJobConfig
from lance_etl.recall.queries import TextQueryTranslationError, resolve_filter_sql, text_query_field_queries
from lance_etl.recall.scoring import (
    DatasetOpenError,
    SampleScore,
    bm25_top_k,
    brute_force_top_k,
    brute_force_top_k_scored,
    grade_against_reference,
    grade_hybrid_reference,
    index_default_distance_type,
    reduce_partial_top_k,
    resolve_dataset,
    skipped_score,
)
from lance_etl.recall.source import RecallSample, SpanSource, parse_samples
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)

MAX_PARTIAL_CANDIDATES_PER_TASK: int = 20_000
"""Maximum sum of requested top-k candidates retained by one exact-scoring task."""

RPC_PARAMETER_BUCKET_RANGES: tuple[tuple[int, int], ...] = (
    (1, 1),
    (2, 2),
    (3, 4),
    (5, 8),
    (9, 16),
    (17, 32),
    (33, 64),
)
"""Finite exponential ranges used to bound RPC report groups and metric tags."""


@dataclass(frozen=True)
class AggregateRow:
    """One aggregate bucket of the recall report.

    Attributes:
        bucket: Human-readable bucket label for the stdout table.
        samples: Number of successfully scored samples in the bucket.
        mean_recall: Mean recall@k over scored samples, or None when none scored.
        mean_ndcg: Mean nDCG@k over scored samples, or None when none scored.
        mean_mrr: Mean MRR over scored samples, or None when none scored.
        p50: Median recall@k over scored samples, or None when none scored.
        p95: 95th-percentile recall@k over scored samples, or None when none scored.
        skip_count: Samples in the bucket that were skipped.
        nprobes_min: Bounded lower-nprobes range label, or None outside RPC buckets.
        nprobes_max: Bounded upper-nprobes range label, or None outside RPC buckets.
        refine_factor: Bounded refine-factor range label, or None outside RPC buckets.
        query_type: Query-type bucket key carried for metric tagging, None outside query-type buckets.
        is_rpc_bucket: True for RPC-parameter buckets, which are emitted as metrics tagged with the RPC parameters.
        is_query_type_bucket: True for query-type buckets, which are emitted as metrics tagged with the query type.
    """

    bucket: str
    samples: int
    mean_recall: float | None
    mean_ndcg: float | None
    mean_mrr: float | None
    p50: float | None
    p95: float | None
    skip_count: int
    nprobes_min: str | None = None
    nprobes_max: str | None = None
    refine_factor: str | None = None
    query_type: str | None = None
    is_rpc_bucket: bool = False
    is_query_type_bucket: bool = False


@dataclass(frozen=True)
class RecallReport:
    """The full output of one recall-audit run.

    Attributes:
        rows: Aggregate rows in table order: overall, then RPC buckets, then org buckets.
        scores: Every per-sample scoring outcome.
        parse_skips: Count of records skipped at parse time, keyed by reason.
    """

    rows: list[AggregateRow]
    scores: list[SampleScore]
    parse_skips: dict[str, int]


def sample_dataset_uri(base_uri: str, sample: RecallSample) -> str:
    """Build the dataset URI for one sample's routing components.

    The components were validated against the path allowlist at parse time, so the URI is confined to the
    routing-key prefix.

    Args:
        base_uri: Root location under which per-tenant datasets live.
        sample: The parsed sample.

    Returns:
        The ``{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`` URI.
    """
    base: str = base_uri.rstrip("/")
    return f"{base}/{sample.org_id}/{sample.tenant_id}/{sample.namespace}.lance"


def score_vector_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    default_distance: str,
    config: RecallJobConfig,
) -> SampleScore:
    """Score one vector sample against an already-opened dataset.

    Args:
        dataset: The dataset checked out at the sample's recorded version.
        sample: The vector sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    distance_type: str = sample.distance_type or default_distance
    query: np.ndarray = np.asarray(sample.query_vector, dtype=np.float64)
    try:
        true_ids, candidate_count = brute_force_top_k(
            dataset,
            query,
            sample.k,
            distance_type,
            config.id_column,
            config.vector_column,
            filter_sql,
            BATCH_SIZE,
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error")
    return grade_against_reference(sample, true_ids, candidate_count)


def score_text_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    config: RecallJobConfig,
) -> SampleScore:
    """Score one text sample against the exact BM25 reference at the pinned version.

    Args:
        dataset: The dataset checked out at the sample's recorded version.
        sample: The text sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        config: The job configuration.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation")
    try:
        true_ids, true_scores, candidate_count = bm25_top_k(
            dataset, field_queries, sample.k, config.id_column, filter_sql, BATCH_SIZE
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error")
    del true_scores
    return grade_against_reference(sample, true_ids, candidate_count)


def score_hybrid_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
) -> SampleScore:
    """Score one hybrid sample by fusing exact vector and exact BM25 references at the pinned version.

    The vector and text legs are each computed to the fused ``k`` (the common case where the leg ``k`` inherits the
    fused ``k``), then merged with the recorded fusion strategy before grading the served ids.

    Args:
        dataset: The dataset checked out at the sample's recorded version.
        sample: The hybrid sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation")
    distance_type: str = sample.distance_type or default_distance
    query: np.ndarray = np.asarray(sample.query_vector, dtype=np.float64)
    try:
        record_ids, vector_scores, vector_count = brute_force_top_k_scored(
            dataset,
            query,
            sample.k,
            distance_type,
            config.id_column,
            config.vector_column,
            filter_sql,
            BATCH_SIZE,
        )
        text_ids, text_scores, text_count = bm25_top_k(
            dataset, field_queries, sample.k, config.id_column, filter_sql, BATCH_SIZE
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error")
    return grade_hybrid_reference(sample, record_ids, vector_scores, vector_count, text_ids, text_scores, text_count)


def score_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
) -> SampleScore:
    """Score one sample against an already-opened dataset, dispatching on the query type.

    Args:
        dataset: The dataset checked out at the sample's recorded version.
        sample: The sample to score.
        schema_columns: The dataset schema's column names, for filter and text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.

    Returns:
        The sample's score, with ``recall=None`` and a reason when the sample had to be skipped.
    """
    if sample.result_ids is None:
        return skipped_score(sample, "null_result_ids")
    filter_sql, filter_skip = resolve_filter_sql(sample, schema_columns)
    if filter_skip is not None:
        return skipped_score(sample, filter_skip)
    if sample.query_type == "text":
        return score_text_sample(dataset, sample, filter_sql, schema_columns, config)
    if sample.query_type == "hybrid":
        return score_hybrid_sample(dataset, sample, filter_sql, schema_columns, default_distance, config)
    return score_vector_sample(dataset, sample, filter_sql, default_distance, config)


def score_version_group(
    uri: str, version: int, samples: list[RecallSample], config: RecallJobConfig
) -> list[SampleScore]:
    """Score every sample of one ``(uri, version)`` group on an executor.

    Opens the dataset once at the recorded version, resolves the index-metric default distance once,
    and then scores each sample. A retained-away version is skipped rather than replaced with latest.

    Args:
        uri: The dataset URI shared by the group.
        version: The recorded dataset version shared by the group.
        samples: The samples to score.
        config: The job configuration.

    Returns:
        One score per sample.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    with telemetry.timed("recall.group_ms"):
        try:
            dataset = resolve_dataset(uri, version, config.storage_options)
        except DatasetOpenError:
            telemetry.incr("recall.dataset_unavailable")
            return [skipped_score(sample, "dataset_unavailable") for sample in samples]
        if dataset is None:
            telemetry.incr("recall.version_missing")
            return [skipped_score(sample, "version_missing") for sample in samples]
        schema_columns: frozenset[str] = frozenset(dataset.schema.names)
        scorable_samples: list[RecallSample] = [sample for sample in samples if sample.result_ids is not None]
        needs_vector: bool = any(sample.query_type in ("vector", "hybrid") for sample in scorable_samples)
        id_missing: bool = bool(scorable_samples) and config.id_column not in schema_columns
        vector_missing: bool = needs_vector and config.vector_column not in schema_columns
        if id_missing or vector_missing:
            telemetry.incr("recall.missing_columns")
            scores: list[SampleScore] = []
            for sample in samples:
                if sample.result_ids is None:
                    scores.append(skipped_score(sample, "null_result_ids"))
                elif id_missing or sample.query_type in ("vector", "hybrid"):
                    scores.append(skipped_score(sample, "missing_columns"))
                else:
                    scores.append(score_sample(dataset, sample, schema_columns, "l2", config))
            return scores
        default_distance: str = index_default_distance_type(dataset, config.vector_column) if needs_vector else "l2"
        return [score_sample(dataset, sample, schema_columns, default_distance, config) for sample in samples]


def vector_leg_samples(samples: list[RecallSample]) -> list[RecallSample]:
    """Select the samples of a group that need an exact vector leg.

    Vector and hybrid samples carry a query vector and are fanned out per fragment. Samples with a null served-id
    capture are excluded because they skip before any scan.

    Args:
        samples: The group's samples.

    Returns:
        The vector-bearing samples that are scorable.
    """
    return [s for s in samples if s.query_type in ("vector", "hybrid") and s.result_ids is not None]


def text_leg_samples(samples: list[RecallSample]) -> list[RecallSample]:
    """Select the samples of a group that need an exact BM25 leg.

    Text and hybrid samples are scored against the whole-dataset BM25 reference because the BM25 inverse document
    frequency and average document length are corpus-global statistics that cannot be sharded per fragment without
    changing the scores, so the reference stays whole-dataset even in the large tier.

    Args:
        samples: The group's samples.

    Returns:
        The text-bearing samples that are scorable.
    """
    return [s for s in samples if s.query_type in ("text", "hybrid") and s.result_ids is not None]


def fragment_vector_partials(
    uri: str, version: int, fragment_id: int, samples: list[RecallSample], config: RecallJobConfig
) -> dict[str, dict[str, Any]]:
    """Compute one fragment's partial vector top-k for each vector-bearing sample of a large group.

    Runs on an executor. Opens the dataset at the recorded version, restricts the brute-force scan to the single
    fragment identified by ``fragment_id``, and returns a per-sample partial top-k for executor-side reduction.
    Looking up the exact fragment handle by identifier avoids re-enumerating the full fragment inventory in every
    task. Per-sample skip decisions that are deterministic across fragments are returned as skip markers.

    ``version`` is the exact version pinned by :meth:`RecallAuditJob.classify_groups`. If retention
    removes it between planning and execution, the task skips. This guarantees the fragment task
    list and every partial refer to the same immutable snapshot.

    Args:
        uri: The dataset URI shared by the group.
        version: The recorded dataset version shared by the group.
        fragment_id: Exact fragment identifier from the pinned classification probe.
        samples: The vector-bearing samples to score against this fragment.
        config: The job configuration.

    Returns:
        A mapping from sample id to either ``{"status": "partial", "ids", "dists", "count"}`` or
        ``{"status": "skip", "reason"}``. Every sample is skipped with reason ``"version_missing"`` when the
        pinned version disappeared.
    """
    if fragment_id < 0:
        return {sample.sample_id: {"status": "skip", "reason": "version_missing"} for sample in samples}
    try:
        dataset = resolve_dataset(uri, version, config.storage_options)
    except DatasetOpenError:
        return {sample.sample_id: {"status": "skip", "reason": "dataset_unavailable"} for sample in samples}
    if dataset is None:
        return {sample.sample_id: {"status": "skip", "reason": "version_missing"} for sample in samples}
    schema_columns: frozenset[str] = frozenset(dataset.schema.names)
    default_distance: str = index_default_distance_type(dataset, config.vector_column)
    fragment: lance.LanceFragment | None = dataset.get_fragment(fragment_id)
    if fragment is None:
        return {sample.sample_id: {"status": "skip", "reason": "fragment_missing"} for sample in samples}
    partials: dict[str, dict[str, Any]] = {}
    for sample in samples:
        filter_sql, filter_skip = resolve_filter_sql(sample, schema_columns)
        if filter_skip is not None:
            partials[sample.sample_id] = {"status": "skip", "reason": filter_skip}
            continue
        distance_type: str = sample.distance_type or default_distance
        query: np.ndarray = np.asarray(sample.query_vector, dtype=np.float64)
        try:
            ids, dists, count = brute_force_top_k_scored(
                dataset,
                query,
                sample.k,
                distance_type,
                config.id_column,
                config.vector_column,
                filter_sql,
                BATCH_SIZE,
                fragments=[fragment],
            )
        except (ValueError, OSError, RuntimeError):
            partials[sample.sample_id] = {"status": "skip", "reason": "scan_error"}
            continue
        partials[sample.sample_id] = {"status": "partial", "ids": ids, "dists": dists, "count": count}
    return partials


def reduce_vector_legs(sample: RecallSample, fragment_partials: list[tuple[int, dict[str, Any]]]) -> dict[str, Any]:
    """Reduce one sample's per-fragment vector partials into one exact leg, or propagate a skip.

    The partials are reduced in ascending fragment-index order with
    :func:`~lance_etl.recall.scoring.reduce_partial_top_k`, the same stable merge the single-stream scan uses across
    batches, so the reduced top-k equals the whole-dataset brute force exactly. A deterministic per-fragment skip
    marker (every fragment agrees) becomes the leg's skip, independent of the order in which the fan-out tasks
    returned. An empty partial set is itself a skip with the ``missing_partials`` reason.

    Args:
        sample: The sample whose vector leg is reduced.
        fragment_partials: The ``(fragment_index, payload)`` partials gathered from the fan-out tasks.

    Returns:
        Either ``{"status": "leg", "ids", "dists", "count"}`` or ``{"status": "skip", "reason"}``.
    """
    if not fragment_partials:
        return {"status": "skip", "reason": "missing_partials"}
    skip_reasons: set[str] = {payload["reason"] for _, payload in fragment_partials if payload["status"] == "skip"}
    if skip_reasons:
        reason: str = next(iter(skip_reasons)) if len(skip_reasons) == 1 else "inconsistent_partials"
        return {"status": "skip", "reason": reason}
    ordered: list[tuple[int, dict[str, Any]]] = sorted(fragment_partials, key=lambda item: item[0])
    partials: list[tuple[list[Any], list[float]]] = [(payload["ids"], payload["dists"]) for _, payload in ordered]
    ids, dists = reduce_partial_top_k(partials, sample.k)
    count: int = sum(int(payload["count"]) for _, payload in ordered)
    return {"status": "leg", "ids": ids, "dists": dists, "count": count}


def vector_partial_accumulator(fragment_id: int, payload: dict[str, Any], k: int) -> dict[str, Any]:
    """Convert one fragment payload into a bounded associative reduction state.

    Candidate ordering carries the fragment identifier and within-fragment rank explicitly, so
    arbitrary Spark reduction order preserves the same stable tie order as an ascending-fragment
    whole-dataset scan.

    Args:
        fragment_id: Stable fragment identifier.
        payload: Fragment partial or skip marker.
        k: Requested result count.

    Returns:
        Bounded accumulator containing at most ``k`` candidates or a skip-reason set.

    Raises:
        ValueError: If a partial payload has inconsistent ids and distances.
    """
    if payload["status"] == "skip":
        return {"status": "skip", "k": k, "reasons": (str(payload["reason"]),)}
    ids: list[Any] = list(payload["ids"])
    dists: list[float] = [float(value) for value in payload["dists"]]
    if len(ids) != len(dists):
        raise ValueError("vector partial ids and distances have different lengths")
    candidates: list[tuple[float, int, int, Any]] = [
        (distance, fragment_id, rank, record_id)
        for rank, (record_id, distance) in enumerate(zip(ids, dists, strict=True))
    ]
    return {"status": "partial", "k": k, "candidates": candidates[:k], "count": int(payload["count"])}


def merge_vector_accumulators(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Associatively merge two bounded vector-partial accumulators.

    Args:
        left: First bounded accumulator.
        right: Second bounded accumulator.

    Returns:
        One accumulator with at most ``k`` candidates.

    Raises:
        ValueError: If the accumulators disagree on ``k``.
    """
    left_k: int = int(left["k"])
    right_k: int = int(right["k"])
    if left_k != right_k:
        raise ValueError("vector partial accumulators disagree on k")
    if left["status"] == "skip" or right["status"] == "skip":
        reasons: set[str] = set(left.get("reasons", ())) | set(right.get("reasons", ()))
        return {"status": "skip", "k": left_k, "reasons": tuple(sorted(reasons))}
    candidates: list[tuple[float, int, int, Any]] = sorted(
        [*left["candidates"], *right["candidates"]],
        key=lambda candidate: (candidate[0], candidate[1], candidate[2]),
    )[:left_k]
    return {
        "status": "partial",
        "k": left_k,
        "candidates": candidates,
        "count": int(left["count"]) + int(right["count"]),
    }


def vector_accumulator_leg(accumulator: dict[str, Any]) -> dict[str, Any]:
    """Render one reduced accumulator as the established vector-leg payload.

    Args:
        accumulator: Fully reduced sample accumulator.

    Returns:
        A leg or deterministic skip payload for :func:`combine_large_group_scores`.
    """
    if accumulator["status"] == "skip":
        reasons: tuple[str, ...] = tuple(accumulator["reasons"])
        reason: str = reasons[0] if len(reasons) == 1 else "inconsistent_partials"
        return {"status": "skip", "reason": reason}
    candidates: list[tuple[float, int, int, Any]] = accumulator["candidates"]
    return {
        "status": "leg",
        "ids": [candidate[3] for candidate in candidates],
        "dists": [candidate[0] for candidate in candidates],
        "count": int(accumulator["count"]),
    }


@dataclass(frozen=True)
class LargeTierWork:
    """Bounded driver inputs and sample lookups for one large-tier execution."""

    vector_groups: list[tuple[int, str, int, int]]
    vector_sample_chunks_by_group: dict[int, list[list[RecallSample]]]
    vector_sample_by_key: dict[tuple[int, str], RecallSample]
    text_groups: list[tuple[int, str, int, int]]
    text_samples_by_work: dict[tuple[int, int], list[RecallSample]]


def chunk_samples_by_k(samples: list[RecallSample]) -> list[list[RecallSample]]:
    """Split samples so one task retains a bounded aggregate number of top-k candidates.

    Args:
        samples: Samples in deterministic input order.

    Returns:
        Non-empty chunks whose ``sum(sample.k)`` does not exceed
        :data:`MAX_PARTIAL_CANDIDATES_PER_TASK`.

    Raises:
        ValueError: If a programmatic sample bypassed parser bounds and exceeds the task budget.
    """
    chunks: list[list[RecallSample]] = []
    current: list[RecallSample] = []
    candidates: int = 0
    for sample in samples:
        if sample.k > MAX_PARTIAL_CANDIDATES_PER_TASK:
            raise ValueError("sample k exceeds the per-task exact candidate budget")
        if current and candidates + sample.k > MAX_PARTIAL_CANDIDATES_PER_TASK:
            chunks.append(current)
            current = []
            candidates = 0
        current.append(sample)
        candidates += sample.k
    if current:
        chunks.append(current)
    return chunks


def prepare_large_tier_work(items: list[tuple[str, int, list[RecallSample], int]]) -> LargeTierWork:
    """Build group-level work without expanding fragment inventories on the driver.

    Args:
        items: Classified large-tier groups.

    Returns:
        Group seeds and sample lookups for the vector and text stages.
    """
    vector_groups: list[tuple[int, str, int, int]] = []
    vector_sample_chunks_by_group: dict[int, list[list[RecallSample]]] = {}
    vector_sample_by_key: dict[tuple[int, str], RecallSample] = {}
    text_groups: list[tuple[int, str, int, int]] = []
    text_samples_by_work: dict[tuple[int, int], list[RecallSample]] = {}
    for index, (uri, version, samples, fragment_count) in enumerate(items):
        vector_samples: list[RecallSample] = vector_leg_samples(samples)
        if vector_samples:
            vector_groups.append((index, uri, version, fragment_count))
            vector_sample_chunks_by_group[index] = chunk_samples_by_k(vector_samples)
            for sample in vector_samples:
                vector_sample_by_key[(index, sample.sample_id)] = sample
        text_samples: list[RecallSample] = text_leg_samples(samples)
        if text_samples:
            for chunk_index, chunk in enumerate(chunk_samples_by_k(text_samples)):
                text_groups.append((index, uri, version, chunk_index))
                text_samples_by_work[(index, chunk_index)] = chunk
    return LargeTierWork(
        vector_groups,
        vector_sample_chunks_by_group,
        vector_sample_by_key,
        text_groups,
        text_samples_by_work,
    )


def enumerate_vector_fragment_work(
    work: tuple[int, str, int, int], storage_options: dict[str, Any] | None, sample_chunk_count: int
) -> Iterator[tuple[int, str, int, int, int]]:
    """Enumerate one pinned group's fragment work on an executor.

    Args:
        work: Group index, URI, effective version, and probed fragment count.
        storage_options: Object-store options forwarded to pylance.
        sample_chunk_count: Positive number of bounded sample chunks for the group.

    Yields:
        One fragment and sample-chunk work item. A negative fragment sentinel preserves a
        deterministic skip when the pinned dataset or its inventory disappears between stages.
    """
    group_index, uri, version, expected_count = work
    try:
        dataset = resolve_dataset(uri, version, storage_options)
    except DatasetOpenError:
        for chunk_index in range(sample_chunk_count):
            yield group_index, uri, version, -1, chunk_index
        return
    if dataset is None:
        for chunk_index in range(sample_chunk_count):
            yield group_index, uri, version, -1, chunk_index
        return
    fragments: list[lance.LanceFragment] = dataset.get_fragments()
    if len(fragments) != expected_count:
        for chunk_index in range(sample_chunk_count):
            yield group_index, uri, version, -1, chunk_index
        return
    for fragment in fragments:
        for chunk_index in range(sample_chunk_count):
            yield group_index, uri, version, int(fragment.fragment_id), chunk_index


def whole_dataset_text_legs(
    uri: str, version: int, samples: list[RecallSample], config: RecallJobConfig
) -> dict[str, dict[str, Any]]:
    """Compute the whole-dataset exact BM25 leg for each text-bearing sample of a large group.

    Runs on an executor. The BM25 reference stays whole-dataset because its corpus-global statistics cannot be sharded
    per fragment without changing the scores. Skip decisions mirror the whole-dataset scorers.

    Args:
        uri: The dataset URI shared by the group.
        version: The recorded dataset version shared by the group.
        samples: The text-bearing samples to score.
        config: The job configuration.

    Returns:
        A mapping from sample id to either ``{"status": "leg", "ids", "scores", "count"}`` or
        ``{"status": "skip", "reason"}``.
    """
    try:
        dataset = resolve_dataset(uri, version, config.storage_options)
    except DatasetOpenError:
        return {sample.sample_id: {"status": "skip", "reason": "dataset_unavailable"} for sample in samples}
    if dataset is None:
        return {sample.sample_id: {"status": "skip", "reason": "version_missing"} for sample in samples}
    schema_columns: frozenset[str] = frozenset(dataset.schema.names)
    legs: dict[str, dict[str, Any]] = {}
    for sample in samples:
        filter_sql, filter_skip = resolve_filter_sql(sample, schema_columns)
        if filter_skip is not None:
            legs[sample.sample_id] = {"status": "skip", "reason": filter_skip}
            continue
        try:
            field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
                sample.text_query, sample.text_columns, schema_columns
            )
        except TextQueryTranslationError:
            legs[sample.sample_id] = {"status": "skip", "reason": "text_query_translation"}
            continue
        try:
            ids, scores, count = bm25_top_k(dataset, field_queries, sample.k, config.id_column, filter_sql, BATCH_SIZE)
        except (ValueError, OSError, RuntimeError):
            legs[sample.sample_id] = {"status": "skip", "reason": "scan_error"}
            continue
        legs[sample.sample_id] = {"status": "leg", "ids": ids, "scores": scores, "count": count}
    return legs


def combine_large_group_scores(
    samples: list[RecallSample],
    vector_legs: dict[str, dict[str, Any]],
    text_legs: dict[str, dict[str, Any]],
) -> list[SampleScore]:
    """Grade a large group's samples from their reduced vector legs and whole-dataset BM25 legs.

    Vector samples grade against the reduced vector leg, text samples against the BM25 leg, and hybrid samples fuse the
    two legs with the recorded strategy. Skip reasons carried on a leg propagate to the sample, and the skip-reason
    vocabulary is identical to the whole-dataset path.

    Args:
        samples: The group's samples in capture order.
        vector_legs: The reduced vector legs keyed by sample id, for vector and hybrid samples.
        text_legs: The whole-dataset BM25 legs keyed by sample id, for text and hybrid samples.

    Returns:
        One score per sample.
    """
    scores: list[SampleScore] = []
    for sample in samples:
        if sample.result_ids is None:
            scores.append(skipped_score(sample, "null_result_ids"))
            continue
        if sample.query_type == "vector":
            leg: dict[str, Any] = vector_legs[sample.sample_id]
            if leg["status"] == "skip":
                scores.append(skipped_score(sample, leg["reason"]))
            else:
                scores.append(grade_against_reference(sample, leg["ids"], leg["count"]))
        elif sample.query_type == "text":
            leg = text_legs[sample.sample_id]
            if leg["status"] == "skip":
                scores.append(skipped_score(sample, leg["reason"]))
            else:
                scores.append(grade_against_reference(sample, leg["ids"], leg["count"]))
        else:
            vector_leg: dict[str, Any] = vector_legs[sample.sample_id]
            text_leg: dict[str, Any] = text_legs[sample.sample_id]
            if vector_leg["status"] == "skip":
                scores.append(skipped_score(sample, vector_leg["reason"]))
            elif text_leg["status"] == "skip":
                scores.append(skipped_score(sample, text_leg["reason"]))
            else:
                scores.append(
                    grade_hybrid_reference(
                        sample,
                        vector_leg["ids"],
                        vector_leg["dists"],
                        vector_leg["count"],
                        text_leg["ids"],
                        text_leg["scores"],
                        text_leg["count"],
                    )
                )
    return scores


def optional_label(value: str | None, fallback: str) -> str:
    """Render an optional bounded bucket key for labels and tags.

    Args:
        value: The optional value.
        fallback: The label used when the value is None.

    Returns:
        The rendered label.
    """
    return fallback if value is None else str(value)


def rpc_parameter_bucket(value: int | None, absent: str) -> str:
    """Map one raw RPC parameter into a finite exponential range vocabulary.

    The emitted ranges are ``nonpositive``, ``1``, ``2``, ``3-4``, ``5-8``, ``9-16``, ``17-32``, ``33-64``, and
    ``65+`` plus the caller-provided absent label. This caps each tag at ten possible values even when malformed or
    future captures carry arbitrary integers.

    Args:
        value: Raw captured parameter, or None when absent.
        absent: Stable label for an absent parameter.

    Returns:
        The bounded bucket label.
    """
    if value is None:
        return absent
    if value <= 0:
        return "nonpositive"
    for lower, upper in RPC_PARAMETER_BUCKET_RANGES:
        if lower <= value <= upper:
            return str(lower) if lower == upper else f"{lower}-{upper}"
    return "65+"


def rpc_bucket_label(nprobes_min: str, nprobes_max: str, refine_factor: str) -> str:
    """Build the table label for one RPC-parameter bucket.

    Args:
        nprobes_min: Bounded lower-nprobes range label.
        nprobes_max: Bounded upper-nprobes range label.
        refine_factor: Bounded refine-factor range label.

    Returns:
        The bucket label, for example ``rpc nprobes=5-8..17-32 refine=2``.
    """
    return f"rpc nprobes={nprobes_min}..{nprobes_max} refine={refine_factor}"


def mean_or_none(values: list[float]) -> float | None:
    """Return the mean of the values, or None when the list is empty.

    Args:
        values: The values to average.

    Returns:
        The mean, or None.
    """
    return float(np.asarray(values, dtype=np.float64).mean()) if values else None


def summarize_bucket(
    bucket: str,
    scores: list[SampleScore],
    nprobes_min: str | None = None,
    nprobes_max: str | None = None,
    refine_factor: str | None = None,
    query_type: str | None = None,
    is_rpc_bucket: bool = False,
    is_query_type_bucket: bool = False,
) -> AggregateRow:
    """Aggregate one bucket of scores into a report row.

    Args:
        bucket: The bucket label.
        scores: The scores in the bucket, including skipped ones.
        nprobes_min: Bounded lower-nprobes range label carried for metric tagging.
        nprobes_max: Bounded upper-nprobes range label carried for metric tagging.
        refine_factor: Bounded refine-factor range label carried for metric tagging.
        query_type: Query-type bucket key carried for metric tagging.
        is_rpc_bucket: Whether this row is an RPC-parameter bucket eligible for metric emission.
        is_query_type_bucket: Whether this row is a query-type bucket eligible for metric emission.

    Returns:
        The aggregate row with the mean recall, nDCG, and MRR plus recall p50 and p95 over the scored samples only.
    """
    recalls: list[float] = [score.recall for score in scores if score.recall is not None]
    ndcgs: list[float] = [score.ndcg for score in scores if score.ndcg is not None]
    mrrs: list[float] = [score.mrr for score in scores if score.mrr is not None]
    values: np.ndarray = np.asarray(recalls, dtype=np.float64)
    return AggregateRow(
        bucket=bucket,
        samples=len(recalls),
        mean_recall=mean_or_none(recalls),
        mean_ndcg=mean_or_none(ndcgs),
        mean_mrr=mean_or_none(mrrs),
        p50=float(np.percentile(values, 50)) if recalls else None,
        p95=float(np.percentile(values, 95)) if recalls else None,
        skip_count=sum(1 for score in scores if score.skip_reason is not None),
        nprobes_min=nprobes_min,
        nprobes_max=nprobes_max,
        refine_factor=refine_factor,
        query_type=query_type,
        is_rpc_bucket=is_rpc_bucket,
        is_query_type_bucket=is_query_type_bucket,
    )


def aggregate_scores(scores: list[SampleScore]) -> list[AggregateRow]:
    """Aggregate scores into the report rows: overall, per RPC bucket, per query type, then per org.

    Args:
        scores: Every per-sample scoring outcome.

    Returns:
        The aggregate rows in table order.
    """
    rows: list[AggregateRow] = [summarize_bucket("overall", scores)]
    rpc_groups: dict[tuple[str, str, str], list[SampleScore]] = {}
    query_type_groups: dict[str, list[SampleScore]] = {}
    org_groups: dict[str, list[SampleScore]] = {}
    for score in scores:
        rpc_key: tuple[str, str, str] = (
            rpc_parameter_bucket(score.nprobes_min, "default"),
            rpc_parameter_bucket(score.nprobes_max, "default"),
            rpc_parameter_bucket(score.refine_factor, "unset"),
        )
        rpc_groups.setdefault(rpc_key, []).append(score)
        query_type_groups.setdefault(score.query_type, []).append(score)
        org_groups.setdefault(score.org_id, []).append(score)
    for rpc_key in sorted(rpc_groups, key=lambda key: rpc_bucket_label(*key)):
        rows.append(
            summarize_bucket(
                rpc_bucket_label(*rpc_key),
                rpc_groups[rpc_key],
                nprobes_min=rpc_key[0],
                nprobes_max=rpc_key[1],
                refine_factor=rpc_key[2],
                is_rpc_bucket=True,
            )
        )
    for query_type in sorted(query_type_groups):
        rows.append(
            summarize_bucket(
                f"query_type {query_type}",
                query_type_groups[query_type],
                query_type=query_type,
                is_query_type_bucket=True,
            )
        )
    for org_id in sorted(org_groups):
        rows.append(summarize_bucket(f"org {org_id}", org_groups[org_id]))
    return rows


def format_metric(value: float | None) -> str:
    """Format one recall statistic for the table.

    Args:
        value: The statistic, or None when the bucket has no scored samples.

    Returns:
        The fixed-precision rendering, or ``-`` for None.
    """
    return "-" if value is None else f"{value:.4f}"


def format_report(report: RecallReport) -> str:
    """Render the recall report as a fixed-width text table.

    Args:
        report: The report to render.

    Returns:
        The multi-line table, followed by parse-skip and score-skip reason summaries when any were counted.
    """
    width: int = max([len("bucket"), *(len(row.bucket) for row in report.rows)])
    header: str = (
        f"{'bucket':<{width}}  {'samples':>7}  {'recall':>8}  {'ndcg':>8}  {'mrr':>8}  "
        f"{'p50':>8}  {'p95':>8}  {'skipped':>7}"
    )
    lines: list[str] = [header]
    for row in report.rows:
        lines.append(
            f"{row.bucket:<{width}}  {row.samples:>7}  {format_metric(row.mean_recall):>8}  "
            f"{format_metric(row.mean_ndcg):>8}  {format_metric(row.mean_mrr):>8}  "
            f"{format_metric(row.p50):>8}  {format_metric(row.p95):>8}  {row.skip_count:>7}"
        )
    if report.parse_skips:
        rendered: str = ", ".join(f"{reason}={count}" for reason, count in sorted(report.parse_skips.items()))
        lines.append(f"parse skips: {rendered}")
    score_skips: Counter[str] = Counter(score.skip_reason for score in report.scores if score.skip_reason is not None)
    if score_skips:
        rendered = ", ".join(f"{reason}={count}" for reason, count in sorted(score_skips.items()))
        lines.append(f"score skips: {rendered}")
    return "\n".join(lines)


def emit_bucket_metrics(telemetry: Telemetry, row: AggregateRow, tags: list[str]) -> None:
    """Emit the recall, nDCG, and MRR gauges for one bucket under shared tags.

    Args:
        telemetry: The driver telemetry facade.
        row: The aggregate row to emit.
        tags: The shared metric tags for the bucket.
    """
    if row.mean_recall is not None:
        telemetry.gauge("recall.measured", row.mean_recall, tags=tags)
    if row.mean_ndcg is not None:
        telemetry.gauge("recall.ndcg", row.mean_ndcg, tags=tags)
    if row.mean_mrr is not None:
        telemetry.gauge("recall.mrr", row.mean_mrr, tags=tags)


def emit_recall_metrics(telemetry: Telemetry, rows: list[AggregateRow]) -> None:
    """Emit the recall, nDCG, and MRR gauges per RPC-parameter bucket and per query-type bucket.

    RPC buckets are tagged with the RPC parameters and query-type buckets with the query type. Both tag sets are
    bounded-cardinality. Org-level numbers stay in the stdout table so the metric tag cardinality does not explode
    with the organization count.

    Args:
        telemetry: The driver telemetry facade.
        rows: The aggregate rows of the report.
    """
    for row in rows:
        if row.is_rpc_bucket:
            emit_bucket_metrics(
                telemetry,
                row,
                [
                    f"nprobes_min:{optional_label(row.nprobes_min, 'default')}",
                    f"nprobes_max:{optional_label(row.nprobes_max, 'default')}",
                    f"refine_factor:{optional_label(row.refine_factor, 'unset')}",
                ],
            )
        elif row.is_query_type_bucket and row.query_type is not None:
            emit_bucket_metrics(telemetry, row, [f"query_type:{row.query_type}"])


@dataclass
class RecallAuditJob:
    """Replays sampled vector, text, and hybrid queries against pinned dataset versions and reports retrieval quality.

    Each sample is scored with recall@k, nDCG@k, and MRR against an exact reference computed at the recorded dataset
    version: brute-force nearest neighbors for vector legs and exact Okapi BM25 for text legs, fused with the recorded
    strategy for hybrid samples.
    """

    config: RecallJobConfig

    def classify_groups(
        self, spark: SparkSession, items: list[tuple[str, int, list[RecallSample]]]
    ) -> tuple[
        list[tuple[str, int, list[RecallSample]]],
        list[tuple[str, int, list[RecallSample], int]],
    ]:
        """Split ``(uri, version)`` groups into the packed small tier and the per-fragment large tier.

        One distributed probe job opens each group's dataset at the recorded version, reads its fragment count, and
        records whether it is scorable, so the driver never opens a dataset itself.
        Groups whose dataset is missing, lacks a column required by its query mix, repeats a sample id, or has at most
        ``large_group_fragment_threshold`` fragments go to the small tier. Larger scorable groups go to the large tier
        with only the exact version and fragment count carried forward. Fragment identifiers
        are enumerated on an executor in the large-tier job, keeping unbounded fragment inventories off the driver.
        Pinning the version prevents later fragment tasks from mixing snapshots.

        Args:
            spark: Active Spark session.
            items: The ``(uri, version, samples)`` groups to classify.

        Returns:
            ``(small, large)`` where small items are ``(uri, version, samples)`` and large items are
            ``(uri, version, samples, fragment_count)``.
        """
        config: RecallJobConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options
        threshold: int = config.large_group_fragment_threshold
        id_column: str = config.id_column
        vector_column: str = config.vector_column
        keys: list[tuple[str, int]] = [(item[0], item[1]) for item in items]
        requirements: dict[tuple[str, int], tuple[bool, bool]] = {
            (uri, version): (
                any(sample.result_ids is not None for sample in samples),
                any(sample.result_ids is not None and sample.query_type in ("vector", "hybrid") for sample in samples),
            )
            for uri, version, samples in items
        }

        def probe(key: tuple[str, int]) -> tuple[int, bool]:
            """Probe one group's exact dataset size and scorability on an executor.

            Args:
                key: The ``(uri, version)`` group key.

            Returns:
                Fragment count and scorability. The count is zero when the exact version cannot be opened.
            """
            uri, version = key
            needs_id, needs_vector = requirements[key]
            try:
                dataset = resolve_dataset(uri, version, storage_options)
            except DatasetOpenError:
                return 0, False
            if dataset is None:
                return 0, False
            columns: frozenset[str] = frozenset(dataset.schema.names)
            scorable: bool = (not needs_id or id_column in columns) and (not needs_vector or vector_column in columns)
            fragment_count: int = int(dataset.stats.dataset_stats()["num_fragments"])
            return fragment_count, scorable

        slices: int = max(1, min(config.small_tier_slices, len(keys)))
        probes: list[tuple[int, bool]] = spark.sparkContext.parallelize(keys, slices).map(probe).collect()
        small: list[tuple[str, int, list[RecallSample]]] = []
        large: list[tuple[str, int, list[RecallSample], int]] = []
        for (uri, version, samples), (fragment_count, scorable) in zip(items, probes, strict=True):
            unique_sample_ids: bool = len({sample.sample_id for sample in samples}) == len(samples)
            if scorable and unique_sample_ids and fragment_count > threshold:
                large.append((uri, version, samples, fragment_count))
            else:
                small.append((uri, version, samples))
        return small, large

    def run_small_tier(
        self, spark: SparkSession, items: list[tuple[str, int, list[RecallSample]]], telemetry: Telemetry
    ) -> list[SampleScore]:
        """Score many small groups in one batched Spark job, packing several groups per task.

        Fewer slices than groups means one task scores many small datasets end-to-end with the unchanged
        :func:`score_version_group`, amortizing task scheduling and cold opens across the power-law tail.

        Args:
            spark: Active Spark session.
            items: The small-tier ``(uri, version, samples)`` groups.
            telemetry: Driver telemetry facade.

        Returns:
            One score per sample across the small groups.
        """
        config: RecallJobConfig = self.config

        def score_group(item: tuple[str, int, list[RecallSample]]) -> list[SampleScore]:
            """Score one version group on an executor.

            Args:
                item: The ``(uri, version, samples)`` group.

            Returns:
                One score per sample in the group.
            """
            return score_version_group(item[0], item[1], item[2], config)

        slices: int = max(1, min(config.small_tier_slices, len(items)))
        with telemetry.timed("recall.small_tier_ms"):
            collected: list[list[SampleScore]] = (
                spark.sparkContext.parallelize(items, slices).map(score_group).collect()
            )
        telemetry.gauge("recall.small_groups", len(items))
        return [score for group in collected for score in group]

    def run_large_tier(
        self,
        spark: SparkSession,
        items: list[tuple[str, int, list[RecallSample], int]],
        telemetry: Telemetry,
    ) -> list[SampleScore]:
        """Score large groups with per-fragment scans and keyed executor-side exact reduction.

        One Spark job computes a partial vector top-k per ``(group, fragment)`` for every vector and hybrid sample, and
        keyed combiners reduce each sample's partials associatively while retaining only its best ``k`` candidates.
        The driver collects only one reduced vector leg per sample. A second Spark job computes the whole-dataset BM25
        leg for text and hybrid samples, whose corpus-global statistics cannot be sharded.

        Args:
            spark: Active Spark session.
            items: The large-tier ``(uri, version, samples, fragment_count)`` groups.
            telemetry: Driver telemetry facade.

        Returns:
            One score per sample across the large groups.
        """
        config: RecallJobConfig = self.config
        telemetry.gauge("recall.large_groups", len(items))
        prepared: LargeTierWork = prepare_large_tier_work(items)
        expected_vector_fragments: int = sum(work[3] for work in prepared.vector_groups)
        expected_vector_work: int = sum(
            work[3] * len(prepared.vector_sample_chunks_by_group[work[0]]) for work in prepared.vector_groups
        )

        def vector_task(
            work: tuple[int, str, int, int, int],
        ) -> list[tuple[tuple[int, str], dict[str, Any]]]:
            """Compute one fragment's partial vector top-k for a large group on an executor.

            Args:
                work: The ``(group_index, uri, version, fragment_id, sample_chunk_index)`` unit.

            Returns:
                One keyed partial per vector-bearing sample for the executor shuffle.
            """
            partials: dict[str, dict[str, Any]] = fragment_vector_partials(
                work[1],
                work[2],
                work[3],
                prepared.vector_sample_chunks_by_group[work[0]][work[4]],
                config,
            )
            return [
                (
                    (work[0], sample_id),
                    vector_partial_accumulator(work[3], payload, prepared.vector_sample_by_key[(work[0], sample_id)].k),
                )
                for sample_id, payload in partials.items()
            ]

        def reduce_vector_sample(
            item: tuple[tuple[int, str], dict[str, Any]],
        ) -> tuple[int, str, dict[str, Any]]:
            """Reduce one sample's grouped fragment partials on an executor.

            Args:
                item: ``((group_index, sample_id), accumulator)`` reduced by Spark.

            Returns:
                Group index, sample id, and exact reduced vector leg.
            """
            key, accumulator = item
            return key[0], key[1], vector_accumulator_leg(accumulator)

        def text_task(work: tuple[int, str, int, int]) -> tuple[int, dict[str, dict[str, Any]]]:
            """Compute the whole-dataset BM25 legs for a large group on an executor.

            Args:
                work: The ``(group_index, uri, version, sample_chunk_index)`` unit.

            Returns:
                ``(group_index, legs)`` for the driver grade.
            """
            return work[0], whole_dataset_text_legs(
                work[1],
                work[2],
                prepared.text_samples_by_work[(work[0], work[3])],
                config,
            )

        with telemetry.timed("recall.large_tier_ms"):
            vector_results: list[tuple[int, str, dict[str, Any]]] = []
            if prepared.vector_groups:
                vector_slices: int = max(1, min(config.large_tier_slices, expected_vector_work))
                enumeration_slices: int = max(1, min(config.large_tier_slices, len(prepared.vector_groups)))
                vector_results = (
                    spark.sparkContext.parallelize(prepared.vector_groups, enumeration_slices)
                    .flatMap(
                        lambda work: enumerate_vector_fragment_work(
                            work,
                            config.storage_options,
                            len(prepared.vector_sample_chunks_by_group[work[0]]),
                        )
                    )
                    .repartition(vector_slices)
                    .flatMap(vector_task)
                    .reduceByKey(merge_vector_accumulators, vector_slices)
                    .map(reduce_vector_sample)
                    .collect()
                )
            text_results: list[tuple[int, dict[str, dict[str, Any]]]] = []
            if prepared.text_groups:
                text_slices: int = max(1, min(config.large_tier_slices, len(prepared.text_groups)))
                text_results = (
                    spark.sparkContext.parallelize(prepared.text_groups, text_slices).map(text_task).collect()
                )

        vector_by_group: dict[int, dict[str, dict[str, Any]]] = {}
        for group_index, sample_id, leg in vector_results:
            vector_by_group.setdefault(group_index, {})[sample_id] = leg
        text_by_group: dict[int, dict[str, dict[str, Any]]] = {}
        for group_index, legs in text_results:
            text_by_group.setdefault(group_index, {}).update(legs)

        telemetry.gauge("recall.large_group_fragments", expected_vector_fragments)
        scores: list[SampleScore] = []
        for index, item in enumerate(items):
            samples: list[RecallSample] = item[2]
            vector_legs: dict[str, dict[str, Any]] = vector_by_group.get(index, {})
            text_legs: dict[str, dict[str, Any]] = text_by_group.get(index, {})
            scores.extend(combine_large_group_scores(samples, vector_legs, text_legs))
        return scores

    def run(self, spark: SparkSession, source: SpanSource, from_ms: int, to_ms: int) -> RecallReport:
        """Fetch, parse, score, aggregate, and report one window of recall samples with two-tier scoring.

        The driver fetches and parses the spans and groups samples by ``(dataset_uri, dataset_version)``. A probe job
        classifies the groups by fragment count: the tail of tiny groups is packed into the small tier where one task
        scores many datasets, and big groups go to the large tier where the vector brute force fans out per fragment
        and reduces exactly through a keyed executor shuffle. The driver aggregates one reduced leg per sample, logs
        the table, and emits the per-bucket gauges.

        Args:
            spark: Active Spark session.
            source: The span source to fetch from.
            from_ms: Window start in epoch milliseconds, inclusive.
            to_ms: Window end in epoch milliseconds, inclusive.

        Returns:
            The full report, for callers and tests.
        """
        config: RecallJobConfig = self.config
        telemetry: Telemetry = Telemetry.create(config.telemetry)
        with telemetry.span("lance.recall.run") as run_span:
            records: list[dict[str, Any]] = list(source.fetch(from_ms, to_ms, config.max_samples))
            samples, parse_skips = parse_samples(records)
            groups: dict[tuple[str, int], list[RecallSample]] = {}
            for sample in samples:
                key: tuple[str, int] = (sample_dataset_uri(config.base_uri, sample), sample.dataset_version)
                groups.setdefault(key, []).append(sample)
            items: list[tuple[str, int, list[RecallSample]]] = [
                (uri, version, group) for (uri, version), group in groups.items()
            ]
            scores: list[SampleScore] = []
            if items:
                small, large = self.classify_groups(spark, items)
                run_span.set_tag("small_groups", len(small))
                run_span.set_tag("large_groups", len(large))
                with telemetry.timed("recall.score_ms"):
                    if small:
                        scores.extend(self.run_small_tier(spark, small, telemetry))
                    if large:
                        scores.extend(self.run_large_tier(spark, large, telemetry))
            report: RecallReport = RecallReport(rows=aggregate_scores(scores), scores=scores, parse_skips=parse_skips)
            logger.info("recall audit results:\n%s", format_report(report))
            emit_recall_metrics(telemetry, report.rows)
            run_span.set_tag("samples", len(samples))
            telemetry.gauge("recall.samples_fetched", len(records))
            telemetry.gauge("recall.samples_scored", sum(1 for score in scores if score.recall is not None))
            telemetry.gauge("recall.samples_skipped", sum(1 for score in scores if score.skip_reason is not None))
            return report
