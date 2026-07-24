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

from lance_etl.recall.config import RecallJobConfig as RecallJobConfig
from lance_etl.recall.job import (
    AggregateRow as AggregateRow,
)
from lance_etl.recall.job import (
    RecallAuditJob as RecallAuditJob,
)
from lance_etl.recall.job import (
    RecallReport as RecallReport,
)
from lance_etl.recall.job import (
    aggregate_scores as aggregate_scores,
)
from lance_etl.recall.job import (
    emit_recall_metrics as emit_recall_metrics,
)
from lance_etl.recall.job import (
    format_report as format_report,
)
from lance_etl.recall.job import (
    fragment_vector_partials as fragment_vector_partials,
)
from lance_etl.recall.job import (
    reduce_vector_legs as reduce_vector_legs,
)
from lance_etl.recall.job import (
    sample_dataset_uri as sample_dataset_uri,
)
from lance_etl.recall.job import (
    score_version_group as score_version_group,
)
from lance_etl.recall.queries import (
    FilterTranslationError as FilterTranslationError,
)
from lance_etl.recall.queries import (
    FusionReplayError as FusionReplayError,
)
from lance_etl.recall.queries import (
    TextQueryTranslationError as TextQueryTranslationError,
)
from lance_etl.recall.queries import (
    filter_ast_to_sql as filter_ast_to_sql,
)
from lance_etl.recall.queries import (
    fuse_legs as fuse_legs,
)
from lance_etl.recall.queries import (
    text_query_field_queries as text_query_field_queries,
)
from lance_etl.recall.scoring import (
    SampleScore as SampleScore,
)
from lance_etl.recall.scoring import (
    bm25_column_scores as bm25_column_scores,
)
from lance_etl.recall.scoring import (
    bm25_top_k as bm25_top_k,
)
from lance_etl.recall.scoring import (
    brute_force_top_k as brute_force_top_k,
)
from lance_etl.recall.scoring import (
    brute_force_top_k_scored as brute_force_top_k_scored,
)
from lance_etl.recall.scoring import (
    compute_distances as compute_distances,
)
from lance_etl.recall.scoring import (
    ranking_quality as ranking_quality,
)
from lance_etl.recall.scoring import (
    reduce_partial_top_k as reduce_partial_top_k,
)
from lance_etl.recall.scoring import (
    resolve_dataset as resolve_dataset,
)
from lance_etl.recall.source import (
    DatadogSpanSource as DatadogSpanSource,
)
from lance_etl.recall.source import (
    InMemorySpanSource as InMemorySpanSource,
)
from lance_etl.recall.source import (
    RecallSample as RecallSample,
)
from lance_etl.recall.source import (
    build_spans_request_body as build_spans_request_body,
)
from lance_etl.recall.source import (
    flatten_recall_attributes as flatten_recall_attributes,
)
from lance_etl.recall.source import (
    parse_recall_sample as parse_recall_sample,
)
from lance_etl.recall.source import (
    parse_samples as parse_samples,
)
