"""RecallAuditJob and the two-tier Spark fan-out that scores every sampled query and reports retrieval quality.

The driver groups parsed samples by ``(dataset_uri, dataset_version)`` and fans the groups out to Spark executors
with ``parallelize().map()``, mirroring the established executor patterns in ``etl.py`` and ``indexing.py``. A probe
job classifies each group by fragment count: the tail of tiny groups is packed into the small tier where one task
scores many datasets end to end, and big groups go to the large tier where the vector brute force fans out per
fragment and is reduced exactly on the driver, while the BM25 leg (whose corpus-global statistics cannot be sharded)
stays whole-dataset. Each executor opens its dataset checked out at the recorded version (falling back to the latest
version with a drift flag when the recorded version was cleaned up) and scores every sample in the group. The driver
aggregates the per-sample scores into a report table (overall, per RPC-parameter bucket, per query-type bucket, per
organization), logs it, and emits bounded-cardinality Datadog gauges per RPC and query-type bucket.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any

import lance
import numpy as np
from pyspark.sql import SparkSession

from lance_etl.recall.config import BATCH_SIZE, RecallJobConfig
from lance_etl.recall.queries import TextQueryTranslationError, resolve_filter_sql, text_query_field_queries
from lance_etl.recall.scoring import (
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
        drift_count: Samples in the bucket scored against a drifted (latest) version.
        skip_count: Samples in the bucket that were skipped.
        nprobes_min: RPC bucket key carried for metric tagging, None outside RPC buckets.
        nprobes_max: RPC bucket key carried for metric tagging, None outside RPC buckets.
        refine_factor: RPC bucket key carried for metric tagging, None outside RPC buckets.
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
    drift_count: int
    skip_count: int
    nprobes_min: int | None = None
    nprobes_max: int | None = None
    refine_factor: int | None = None
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
    version_drift: bool,
) -> SampleScore:
    """Score one vector sample against an already-opened dataset.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The vector sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

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
        return skipped_score(sample, "scan_error", version_drift)
    return grade_against_reference(sample, true_ids, candidate_count, version_drift)


def score_text_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one text sample against the exact BM25 reference at the pinned version.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The text sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation", version_drift)
    try:
        true_ids, true_scores, candidate_count = bm25_top_k(
            dataset, field_queries, sample.k, config.id_column, filter_sql, BATCH_SIZE
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error", version_drift)
    del true_scores
    return grade_against_reference(sample, true_ids, candidate_count, version_drift)


def score_hybrid_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one hybrid sample by fusing exact vector and exact BM25 references at the pinned version.

    The vector and text legs are each computed to the fused ``k`` (the common case where the leg ``k`` inherits the
    fused ``k``), then merged with the recorded fusion strategy before grading the served ids.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The hybrid sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation", version_drift)
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
        return skipped_score(sample, "scan_error", version_drift)
    return grade_hybrid_reference(
        sample, record_ids, vector_scores, vector_count, text_ids, text_scores, text_count, version_drift
    )


def score_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one sample against an already-opened dataset, dispatching on the query type.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The sample to score.
        schema_columns: The dataset schema's column names, for filter and text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with ``recall=None`` and a reason when the sample had to be skipped.
    """
    if sample.result_ids is None:
        return skipped_score(sample, "null_result_ids", version_drift)
    filter_sql, filter_skip = resolve_filter_sql(sample, schema_columns)
    if filter_skip is not None:
        return skipped_score(sample, filter_skip, version_drift)
    if sample.query_type == "text":
        return score_text_sample(dataset, sample, filter_sql, schema_columns, config, version_drift)
    if sample.query_type == "hybrid":
        return score_hybrid_sample(dataset, sample, filter_sql, schema_columns, default_distance, config, version_drift)
    return score_vector_sample(dataset, sample, filter_sql, default_distance, config, version_drift)


def score_version_group(
    uri: str, version: int, samples: list[RecallSample], config: RecallJobConfig
) -> list[SampleScore]:
    """Score every sample of one ``(uri, version)`` group on an executor.

    Opens the dataset once at the recorded version (falling back to latest with a drift flag when the version was
    cleaned up), resolves the index-metric default distance once, and then scores each sample.

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
        dataset, version_drift = resolve_dataset(uri, version, config.storage_options)
        if dataset is None:
            telemetry.incr("recall.dataset_missing")
            return [skipped_score(sample, "dataset_missing") for sample in samples]
        schema_columns: frozenset[str] = frozenset(dataset.schema.names)
        if config.id_column not in schema_columns or config.vector_column not in schema_columns:
            telemetry.incr("recall.missing_columns")
            return [skipped_score(sample, "missing_columns", version_drift) for sample in samples]
        if version_drift:
            telemetry.incr("recall.version_drift")
        default_distance: str = index_default_distance_type(dataset, config.vector_column)
        return [
            score_sample(dataset, sample, schema_columns, default_distance, config, version_drift) for sample in samples
        ]


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
    uri: str, version: int, fragment_index: int, samples: list[RecallSample], config: RecallJobConfig
) -> dict[str, dict[str, Any]]:
    """Compute one fragment's partial vector top-k for each vector-bearing sample of a large group.

    Runs on an executor. Opens the dataset at the recorded version, restricts the brute-force scan to the single
    fragment at ``fragment_index`` in the dataset's fragment order, and returns a per-sample partial top-k that the
    driver reduces across fragments. Per-sample skip decisions that are deterministic across fragments (filter
    translation, scan errors such as a vector-dimension mismatch) are returned as skip markers.

    ``fragment_index`` was planned by :meth:`RecallAuditJob.classify_groups` against a fragment count read at a
    possibly-earlier probe. If the recorded version has since expired and
    :func:`~lance_etl.recall.scoring.resolve_dataset` falls back to the latest snapshot, a concurrent compaction may
    have consolidated fragments in the meantime, so the freshly-opened dataset can carry fewer fragments than the
    plan assumed. ``fragment_index`` is bounds-checked
    against this dataset's own fragment count instead of indexing blindly, so a shrink degrades to a per-sample skip
    rather than an uncaught ``IndexError`` that would otherwise kill the whole large-tier job. Growth (more fragments
    than planned) is not compensated here: the flat per-fragment task list is sized once, upstream, from the count
    :meth:`RecallAuditJob.classify_groups` observed, so fragments that appeared afterward are silently left
    unscanned by this bounded fix alone. Closing that gap would mean re-probing fragment counts immediately before
    every large-tier run, which was judged too invasive for this fix.

    Args:
        uri: The dataset URI shared by the group.
        version: The recorded dataset version shared by the group.
        fragment_index: The position of the fragment in the dataset's fragment order, as planned upstream.
        samples: The vector-bearing samples to score against this fragment.
        config: The job configuration.

    Returns:
        A mapping from sample id to either ``{"status": "partial", "ids", "dists", "count"}`` or
        ``{"status": "skip", "reason"}``. Every sample is skipped with reason ``"fragment_missing"`` when
        ``fragment_index`` no longer exists in the freshly-opened dataset.
    """
    dataset, _ = resolve_dataset(uri, version, config.storage_options)
    if dataset is None:
        return {sample.sample_id: {"status": "skip", "reason": "dataset_missing"} for sample in samples}
    fragments: list[lance.LanceFragment] = dataset.get_fragments()
    if fragment_index >= len(fragments):
        logger.warning(
            "recall large-tier: fragment %d no longer exists in %s at version %d (dataset now has %d fragments); "
            "skipping %d sample(s) for this fragment instead of scoring a stale index",
            fragment_index,
            uri,
            version,
            len(fragments),
            len(samples),
        )
        return {sample.sample_id: {"status": "skip", "reason": "fragment_missing"} for sample in samples}
    schema_columns: frozenset[str] = frozenset(dataset.schema.names)
    default_distance: str = index_default_distance_type(dataset, config.vector_column)
    fragment: lance.LanceFragment = fragments[fragment_index]
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
        return {"status": "skip", "reason": next(iter(skip_reasons))}
    ordered: list[tuple[int, dict[str, Any]]] = sorted(fragment_partials, key=lambda item: item[0])
    partials: list[tuple[list[Any], list[float]]] = [(payload["ids"], payload["dists"]) for _, payload in ordered]
    ids, dists = reduce_partial_top_k(partials, sample.k)
    count: int = sum(int(payload["count"]) for _, payload in ordered)
    return {"status": "leg", "ids": ids, "dists": dists, "count": count}


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
    dataset, _ = resolve_dataset(uri, version, config.storage_options)
    if dataset is None:
        return {sample.sample_id: {"status": "skip", "reason": "dataset_missing"} for sample in samples}
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
    version_drift: bool,
    vector_legs: dict[str, dict[str, Any]],
    text_legs: dict[str, dict[str, Any]],
) -> list[SampleScore]:
    """Grade a large group's samples from their reduced vector legs and whole-dataset BM25 legs.

    Vector samples grade against the reduced vector leg, text samples against the BM25 leg, and hybrid samples fuse the
    two legs with the recorded strategy. Skip reasons carried on a leg propagate to the sample, and the skip-reason
    vocabulary is identical to the whole-dataset path.

    Args:
        samples: The group's samples in capture order.
        version_drift: Whether the dataset was opened at a drifted version, carried from the classification probe.
        vector_legs: The reduced vector legs keyed by sample id, for vector and hybrid samples.
        text_legs: The whole-dataset BM25 legs keyed by sample id, for text and hybrid samples.

    Returns:
        One score per sample.
    """
    scores: list[SampleScore] = []
    for sample in samples:
        if sample.result_ids is None:
            scores.append(skipped_score(sample, "null_result_ids", version_drift))
            continue
        if sample.query_type == "vector":
            leg: dict[str, Any] = vector_legs[sample.sample_id]
            if leg["status"] == "skip":
                scores.append(skipped_score(sample, leg["reason"], version_drift))
            else:
                scores.append(grade_against_reference(sample, leg["ids"], leg["count"], version_drift))
        elif sample.query_type == "text":
            leg = text_legs[sample.sample_id]
            if leg["status"] == "skip":
                scores.append(skipped_score(sample, leg["reason"], version_drift))
            else:
                scores.append(grade_against_reference(sample, leg["ids"], leg["count"], version_drift))
        else:
            vector_leg: dict[str, Any] = vector_legs[sample.sample_id]
            text_leg: dict[str, Any] = text_legs[sample.sample_id]
            if vector_leg["status"] == "skip":
                scores.append(skipped_score(sample, vector_leg["reason"], version_drift))
            elif text_leg["status"] == "skip":
                scores.append(skipped_score(sample, text_leg["reason"], version_drift))
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
                        version_drift,
                    )
                )
    return scores


def optional_label(value: int | None, fallback: str) -> str:
    """Render an optional integer bucket key for labels and tags.

    Args:
        value: The optional value.
        fallback: The label used when the value is None.

    Returns:
        The rendered label.
    """
    return fallback if value is None else str(value)


def rpc_bucket_label(nprobes_min: int | None, nprobes_max: int | None, refine_factor: int | None) -> str:
    """Build the table label for one RPC-parameter bucket.

    Args:
        nprobes_min: Lower nprobes bound, or None for the index default.
        nprobes_max: Upper nprobes bound, or None for the index default.
        refine_factor: Refine factor, or None when unset.

    Returns:
        The bucket label, for example ``rpc nprobes=8..32 refine=2``.
    """
    low: str = optional_label(nprobes_min, "default")
    high: str = optional_label(nprobes_max, "default")
    refine: str = optional_label(refine_factor, "unset")
    return f"rpc nprobes={low}..{high} refine={refine}"


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
    nprobes_min: int | None = None,
    nprobes_max: int | None = None,
    refine_factor: int | None = None,
    query_type: str | None = None,
    is_rpc_bucket: bool = False,
    is_query_type_bucket: bool = False,
) -> AggregateRow:
    """Aggregate one bucket of scores into a report row.

    Args:
        bucket: The bucket label.
        scores: The scores in the bucket, including skipped ones.
        nprobes_min: RPC bucket key carried for metric tagging.
        nprobes_max: RPC bucket key carried for metric tagging.
        refine_factor: RPC bucket key carried for metric tagging.
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
        drift_count=sum(1 for score in scores if score.version_drift),
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
    rpc_groups: dict[tuple[int | None, int | None, int | None], list[SampleScore]] = {}
    query_type_groups: dict[str, list[SampleScore]] = {}
    org_groups: dict[str, list[SampleScore]] = {}
    for score in scores:
        rpc_key: tuple[int | None, int | None, int | None] = (score.nprobes_min, score.nprobes_max, score.refine_factor)
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
        f"{'p50':>8}  {'p95':>8}  {'drift':>5}  {'skipped':>7}"
    )
    lines: list[str] = [header]
    for row in report.rows:
        lines.append(
            f"{row.bucket:<{width}}  {row.samples:>7}  {format_metric(row.mean_recall):>8}  "
            f"{format_metric(row.mean_ndcg):>8}  {format_metric(row.mean_mrr):>8}  "
            f"{format_metric(row.p50):>8}  {format_metric(row.p95):>8}  {row.drift_count:>5}  {row.skip_count:>7}"
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


class RecallAuditJob:
    """Replays sampled vector, text, and hybrid queries against pinned dataset versions and reports retrieval quality.

    Each sample is scored with recall@k, nDCG@k, and MRR against an exact reference computed at the recorded dataset
    version: brute-force nearest neighbors for vector legs and exact Okapi BM25 for text legs, fused with the recorded
    strategy for hybrid samples.
    """

    def __init__(self, config: RecallJobConfig) -> None:
        """Initialize the job.

        Args:
            config: The job configuration.
        """
        self.config: RecallJobConfig = config

    def classify_groups(
        self, spark: SparkSession, items: list[tuple[str, int, list[RecallSample]]]
    ) -> tuple[list[tuple[str, int, list[RecallSample]]], list[tuple[str, int, list[RecallSample], bool, int]]]:
        """Split ``(uri, version)`` groups into the packed small tier and the per-fragment large tier.

        One distributed probe job opens each group's dataset at the recorded version, reads its fragment count, and
        records whether it is scorable and whether the version drifted, so the driver never opens a dataset itself.
        Groups whose dataset is missing, lacks the id or vector column, or has at most
        ``large_group_fragment_threshold`` fragments go to the small tier, where the missing-dataset and missing-column
        cases are handled identically by :func:`score_version_group`. Larger scorable groups go to the large tier with
        their fragment count and drift flag carried forward.

        Args:
            spark: Active Spark session.
            items: The ``(uri, version, samples)`` groups to classify.

        Returns:
            ``(small, large)`` where small items are ``(uri, version, samples)`` and large items are
            ``(uri, version, samples, version_drift, fragments)``.
        """
        config: RecallJobConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options
        threshold: int = config.large_group_fragment_threshold
        id_column: str = config.id_column
        vector_column: str = config.vector_column
        keys: list[tuple[str, int]] = [(uri, version) for uri, version, _ in items]

        def probe(key: tuple[str, int]) -> tuple[int, bool, bool]:
            """Probe one group's dataset size, scorability, and version drift on an executor.

            Args:
                key: The ``(uri, version)`` group key.

            Returns:
                ``(fragments, scorable, version_drift)`` where ``fragments`` is -1 when the dataset cannot be opened.
            """
            uri, version = key
            dataset, drift = resolve_dataset(uri, version, storage_options)
            if dataset is None:
                return -1, False, False
            columns: frozenset[str] = frozenset(dataset.schema.names)
            scorable: bool = id_column in columns and vector_column in columns
            return len(dataset.get_fragments()), scorable, drift

        slices: int = max(1, min(config.small_tier_slices, len(keys)))
        probes: list[tuple[int, bool, bool]] = spark.sparkContext.parallelize(keys, slices).map(probe).collect()
        small: list[tuple[str, int, list[RecallSample]]] = []
        large: list[tuple[str, int, list[RecallSample], bool, int]] = []
        for (uri, version, samples), (fragments, scorable, drift) in zip(items, probes, strict=True):
            if scorable and fragments > threshold:
                large.append((uri, version, samples, drift, fragments))
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
        self, spark: SparkSession, items: list[tuple[str, int, list[RecallSample], bool, int]], telemetry: Telemetry
    ) -> list[SampleScore]:
        """Score large groups by fanning the vector brute force out per fragment and reducing exactly on the driver.

        One Spark job computes a partial vector top-k per ``(group, fragment)`` for every vector and hybrid sample, and
        the driver reduces the partials per sample with the same stable merge the single-stream scan uses, so the
        reduced top-k is bit-identical to the whole-dataset brute force. A second Spark job computes the whole-dataset
        BM25 leg for text and hybrid samples, whose corpus-global statistics cannot be sharded. The driver then grades
        vector samples from the reduced leg, text samples from the BM25 leg, and hybrid samples from the fusion of both.

        Args:
            spark: Active Spark session.
            items: The large-tier ``(uri, version, samples, version_drift, fragments)`` groups.
            telemetry: Driver telemetry facade.

        Returns:
            One score per sample across the large groups.
        """
        config: RecallJobConfig = self.config
        telemetry.gauge("recall.large_groups", len(items))
        vector_work: list[tuple[int, str, int, int, list[RecallSample]]] = []
        text_work: list[tuple[int, str, int, list[RecallSample]]] = []
        for index, (uri, version, samples, drift, fragments) in enumerate(items):
            del drift
            vector_samples: list[RecallSample] = vector_leg_samples(samples)
            if vector_samples:
                vector_work.extend(
                    (index, uri, version, fragment_index, vector_samples) for fragment_index in range(fragments)
                )
            text_samples: list[RecallSample] = text_leg_samples(samples)
            if text_samples:
                text_work.append((index, uri, version, text_samples))

        def vector_task(
            work: tuple[int, str, int, int, list[RecallSample]],
        ) -> tuple[int, int, dict[str, dict[str, Any]]]:
            """Compute one fragment's partial vector top-k for a large group on an executor.

            Args:
                work: The ``(group_index, uri, version, fragment_index, samples)`` unit.

            Returns:
                ``(group_index, fragment_index, partials)`` for the driver reduce.
            """
            return work[0], work[3], fragment_vector_partials(work[1], work[2], work[3], work[4], config)

        def text_task(work: tuple[int, str, int, list[RecallSample]]) -> tuple[int, dict[str, dict[str, Any]]]:
            """Compute the whole-dataset BM25 legs for a large group on an executor.

            Args:
                work: The ``(group_index, uri, version, samples)`` unit.

            Returns:
                ``(group_index, legs)`` for the driver grade.
            """
            return work[0], whole_dataset_text_legs(work[1], work[2], work[3], config)

        with telemetry.timed("recall.large_tier_ms"):
            vector_results: list[tuple[int, int, dict[str, dict[str, Any]]]] = []
            if vector_work:
                vector_slices: int = max(1, min(config.large_tier_slices, len(vector_work)))
                vector_results = spark.sparkContext.parallelize(vector_work, vector_slices).map(vector_task).collect()
            text_results: list[tuple[int, dict[str, dict[str, Any]]]] = []
            if text_work:
                text_slices: int = max(1, min(config.large_tier_slices, len(text_work)))
                text_results = spark.sparkContext.parallelize(text_work, text_slices).map(text_task).collect()

        partials_by_group: dict[int, dict[str, list[tuple[int, dict[str, Any]]]]] = {}
        for group_index, fragment_index, partials in vector_results:
            per_sample: dict[str, list[tuple[int, dict[str, Any]]]] = partials_by_group.setdefault(group_index, {})
            for sample_id, payload in partials.items():
                per_sample.setdefault(sample_id, []).append((fragment_index, payload))
        text_by_group: dict[int, dict[str, dict[str, Any]]] = {index: legs for index, legs in text_results}

        telemetry.gauge("recall.large_group_fragments", len(vector_work))
        scores: list[SampleScore] = []
        for index, item in enumerate(items):
            samples: list[RecallSample] = item[2]
            drift: bool = item[3]
            vector_legs: dict[str, dict[str, Any]] = {}
            for sample in vector_leg_samples(samples):
                fragment_partials: list[tuple[int, dict[str, Any]]] = partials_by_group.get(index, {}).get(
                    sample.sample_id, []
                )
                vector_legs[sample.sample_id] = reduce_vector_legs(sample, fragment_partials)
            text_legs: dict[str, dict[str, Any]] = text_by_group.get(index, {})
            scores.extend(combine_large_group_scores(samples, drift, vector_legs, text_legs))
        return scores

    def run(self, spark: SparkSession, source: SpanSource, from_ms: int, to_ms: int) -> RecallReport:
        """Fetch, parse, score, aggregate, and report one window of recall samples with two-tier scoring.

        The driver fetches and parses the spans and groups samples by ``(dataset_uri, dataset_version)``. A probe job
        classifies the groups by fragment count: the tail of tiny groups is packed into the small tier where one task
        scores many datasets, and big groups go to the large tier where the vector brute force fans out per fragment
        and reduces exactly on the driver. The driver aggregates, logs the table, and emits the per-bucket gauges.

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
