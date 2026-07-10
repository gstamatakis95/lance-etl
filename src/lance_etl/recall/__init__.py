"""Offline recall audit replaying sampled vector, text, and hybrid queries against pinned Lance dataset versions.

The Rust gRPC search service samples a fraction of queries onto Datadog spans, capturing the query (vector, text-query
node tree, or both for a hybrid fusion), the RPC search parameters, the typed filter AST as JSON, the committed Lance
dataset version that served the query, and the served result ids in rank order. :class:`RecallAuditJob` fetches those
spans, replays each query as an exact reference computation against the dataset checked out at the recorded version,
and reports recall@k, nDCG@k, and MRR per RPC-parameter bucket, per query type, and per organization.

The implementation is split by concern:

- :mod:`~lance_etl.recall.config` — :class:`RecallJobConfig` and the module-level constants shared across the
  package (identifier and path allowlists, bounded vocabularies, BM25 parameters, and the scanner batch size).
- :mod:`~lance_etl.recall.source` — span fetching (:class:`SpanSource`, :class:`DatadogSpanSource`,
  :class:`InMemorySpanSource`) and parsing flat ``recall.*`` attribute dictionaries into :class:`RecallSample`.
- :mod:`~lance_etl.recall.queries` — span-to-query translation: the typed filter AST to a Lance scanner filter
  string, the typed text-query node tree to BM25 field clauses, text tokenization, and hybrid fusion replay.
- :mod:`~lance_etl.recall.scoring` — exact grading: brute-force vector top-k, Okapi BM25, and the recall@k, nDCG@k,
  and MRR math shared by every query type.
- :mod:`~lance_etl.recall.job` — :class:`RecallAuditJob` and the two-tier Spark fan-out (a packed small tier and a
  per-fragment large tier) that scores every sample and renders the aggregate report.

Filter replay and the no-raw-SQL rule: the repository forbids accepting raw SQL strings in the gRPC filter API
because client-supplied strings cannot be trusted. This package honors that rule even though it hands the Lance
scanner a SQL string, because the string never crosses a trust boundary. It is generated internally from the typed
filter AST that the Rust service captured from its own typed ``Filter`` proto. Clients never supply strings at any
point. Every column identifier is validated against both the ``[A-Za-z_][A-Za-z0-9_]*`` allowlist and the dataset
schema, and every literal is rendered through the typed value renderer, so the generated string is a pure function of
validated typed data.
"""

from __future__ import annotations

from lance_etl.recall.config import RecallJobConfig
from lance_etl.recall.job import (
    AggregateRow,
    RecallAuditJob,
    RecallReport,
    aggregate_scores,
    emit_recall_metrics,
    format_report,
    fragment_vector_partials,
    reduce_vector_legs,
    sample_dataset_uri,
    score_version_group,
)
from lance_etl.recall.queries import (
    FilterTranslationError,
    FusionReplayError,
    TextQueryTranslationError,
    filter_ast_to_sql,
    fuse_legs,
    text_query_field_queries,
)
from lance_etl.recall.scoring import (
    SampleScore,
    bm25_column_scores,
    bm25_top_k,
    brute_force_top_k,
    brute_force_top_k_scored,
    compute_distances,
    ranking_quality,
    reduce_partial_top_k,
    resolve_dataset,
)
from lance_etl.recall.source import (
    DatadogSpanSource,
    InMemorySpanSource,
    RecallSample,
    build_spans_request_body,
    flatten_recall_attributes,
    parse_recall_sample,
    parse_samples,
)

__all__ = [
    "AggregateRow",
    "DatadogSpanSource",
    "FilterTranslationError",
    "FusionReplayError",
    "InMemorySpanSource",
    "RecallAuditJob",
    "RecallJobConfig",
    "RecallReport",
    "RecallSample",
    "SampleScore",
    "TextQueryTranslationError",
    "aggregate_scores",
    "bm25_column_scores",
    "bm25_top_k",
    "brute_force_top_k",
    "brute_force_top_k_scored",
    "build_spans_request_body",
    "compute_distances",
    "emit_recall_metrics",
    "filter_ast_to_sql",
    "flatten_recall_attributes",
    "format_report",
    "fragment_vector_partials",
    "fuse_legs",
    "parse_recall_sample",
    "parse_samples",
    "ranking_quality",
    "reduce_partial_top_k",
    "reduce_vector_legs",
    "resolve_dataset",
    "sample_dataset_uri",
    "score_version_group",
    "text_query_field_queries",
]
