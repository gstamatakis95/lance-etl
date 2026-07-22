# `lance_etl.recall`

Offline recall-quality audit. The Rust gRPC search service deterministically samples a fraction of
served queries onto Datadog spans (`DEFAULT_RECALL_SAMPLE_RATE` in the Rust `config.rs`, ADR 0008 —
see [caching-and-observability.md](../../../docs/adr/caching-and-observability.md)), capturing the
query, the RPC search parameters, the typed filter AST, the committed Lance dataset version that
served it, and the served result ids in rank order. `RecallAuditJob` fetches those spans, replays
each one as an exact reference computation against the dataset checked out at the recorded version,
and reports recall@k, nDCG@k, and MRR bucketed by RPC parameters, query type, and organization. The
operator entry point is the `recall` subcommand of the [tools CLI](../tools/README.md), and
`RecallJobConfig`/`RecallAuditJob` are also driven directly by `bench/search.py`'s recall legs. See
the [package README](../README.md) for the reconciliation cycle whose publications this job audits.

Recording the exact serving version is what makes the score exact rather than approximate under
concurrent writes: the audit must run inside the maintenance cleanup retention horizon so the
recorded version has not been version-pruned, or scoring falls back to the latest version with a
`version_drift` flag on every affected sample. A structurally-below-1.0 recall for `text`/`hybrid`
samples is an expected signal, not necessarily a bug: `queries.tokenize_text` is a Unicode
word-splitting approximation of Lance's own tokenizer, so a genuine tokenizer divergence between the
reference and the index shows up as measured sub-1.0 recall on text legs.

## No-raw-SQL rule and why this package is exempt

AGENTS.md hard rule 7 forbids raw SQL strings in the gRPC filter API because client-supplied strings
cannot be trusted. `queries.filter_ast_to_sql` generates a Lance scanner filter string, which on its
face looks like exactly what that rule forbids. It is not a violation: the string is built from the
typed filter AST the Rust service itself captured from its own typed `Filter` proto, so no client
ever supplies a string at any point in this path. Every column identifier still passes the
`[A-Za-z_][A-Za-z0-9_]*` allowlist (`FILTER_COLUMN_PATTERN`) plus a dataset-schema membership check,
and every literal is rendered through the externally-tagged `render_filter_literal` (`{"int": ...}`,
`{"float": ...}`, `{"string": ...}`, `{"bool": ...}`), so the generated string is a pure function of
validated, typed data rather than of untrusted input.

## Modules

| Module | Responsibility |
|---|---|
| `config.py` | `RecallJobConfig` and the module constants every other module shares: identifier/path allowlists, the typed compare-op table, bounded query-type and distance-type vocabularies, BM25 constants, scanner batch size |
| `source.py` | `SpanSource` protocol, `DatadogSpanSource` (Spans search API v2 over `urllib.request`), `InMemorySpanSource` (tests), and `parse_recall_sample`/`parse_samples`, which turn a flat `recall.*` attribute dict into a `RecallSample` |
| `queries.py` | Filter-AST-to-SQL translation, the shared `tokenize_text` reference tokenizer, text-query node-tree translation (`match`/`multi_match`), and hybrid fusion replay (`fuse_legs`: RRF and weighted) |
| `scoring.py` | Exact grading: brute-force vector top-k (`brute_force_top_k`/`brute_force_top_k_scored`), Okapi BM25 (`bm25_top_k`), and the shared `ranking_quality` (recall@k/nDCG@k/MRR) math |
| `job.py` | `RecallAuditJob` — the driver-side fetch/group/classify/score/aggregate/report pipeline and its two-tier Spark fan-out |

## Sample parsing (`source.py`)

`DatadogSpanSource.fetch` pages through the Datadog Spans search API v2 (`@recall.sample:true`
filter, cursor pagination) using only `DD_API_KEY`/`DD_APP_KEY` from the environment — no HTTP
client dependency beyond the standard library. `flatten_recall_attributes` normalizes both the flat
(`recall.sample_id`) and nested (`custom.recall.sample_id`) shapes the Spans API can return into one
flat dict. `parse_recall_sample` then validates every field strictly: routing components go through
`attr_path_component`, which combines the `PATH_COMPONENT_PATTERN` allowlist with an explicit
rejection of `.`/`..` so a malformed capture cannot escape the routing-key path prefix, and JSON
array/object attributes are decoded and shape-checked before use. A field that fails validation
raises `SampleParseError` with a bounded-cardinality `reason` key rather than raising out of the
whole batch. `parse_samples` collects these into a `dict[str, int]` skip-reason count instead of
losing the batch to one bad record.

## Query replay (`queries.py`)

`filter_ast_to_sql` recursively renders `compare`/`in_list`/`is_null`/`is_not_null`/`between`/
`and`/`or`/`not` AST nodes into a Lance scanner filter string, raising `FilterTranslationError` on
any unknown tag, invalid column, or malformed literal. `text_query_field_queries` extracts
`(column, terms, operator, boost)` scoring clauses from a `match` or `multi_match` text-query node
(other node shapes are rejected as `TextQueryTranslationError` so the sample is skipped rather than
scored against an unsupported reference). `fuse_legs` replays a captured hybrid fusion spec exactly:
`rrf` sums `1 / (rrf_k + rank + 1)` per leg matching the Rust `rrf_fuse` math, and `weighted` forms
`vector_weight * vector_norm + (1 - vector_weight) * text_norm` over min-max normalized leg scores,
both breaking ties on id for determinism.

## Scoring (`scoring.py`)

`resolve_dataset` opens the dataset pinned at the sample's recorded version, falling back to the
latest version (with `version_drift=True`) only when the pinned version is unavailable, and
returning `None` only when the URI cannot be opened at all. `brute_force_top_k_scored` streams
`(id, vector)` batches through numpy, merging each batch's partial top-k into a running top-k via
`merge_top_k` so memory stays bounded by `batch_size + k` regardless of dataset size. An optional
`fragments` argument restricts the scan to specific fragments, which is the primitive the large-tier
fan-out (below) uses per fragment. `bm25_top_k` materializes the candidate text columns once,
computes per-column Okapi BM25 (`k1`=`BM25_K1`, `b`=`BM25_B`) via `bm25_column_scores`, sums boosted
column scores, and ranks documents that matched at least one clause, breaking ties on id.
`ranking_quality` computes all three metrics from one exact ground-truth order: graded relevance is
`n - j` for the item at 1-based true rank `j` (so the top-1 item outranks everything), recall@k is
`|served ∩ true-top-k| / min(k, candidate_count)`, nDCG@k divides the served ranking's DCG by the
ideal DCG of the true order, and MRR is the reciprocal rank at which the single true top-1 item
appears in the served list, `0` if absent.

## The two-tier fan-out (`job.py`)

`RecallAuditJob.run` fetches and parses spans on the driver, groups samples by `(dataset_uri,
dataset_version)`, then classifies each group with one distributed probe job
(`classify_groups`) that opens each group's dataset, reads its fragment count, and checks
scorability and version drift — the driver itself never opens a dataset. Groups at or below
`large_group_fragment_threshold` (default 32) go to the **small tier** (`run_small_tier`): packed
`config.small_tier_slices` ways so one task scores many small groups end to end with the unchanged
`score_version_group`, amortizing task scheduling and cold-open cost across a long tail of tiny
per-tenant datasets.

Groups above the threshold go to the **large tier** (`run_large_tier`), which splits by leg:

- The vector leg fans out `(group, fragment)` pairs (`fragment_vector_partials`), each computing one
  fragment's partial top-k. The driver reduces per-sample partials in ascending fragment order
  (`reduce_vector_legs` -> `reduce_partial_top_k`) with the same `merge_top_k` stable merge the
  single-stream scan uses, so the reduced top-k is bit-identical (ties included) to a whole-dataset
  brute force. `fragment_index` is bounds-checked against the freshly-opened dataset's own fragment
  count, degrading a shrunk fragment set to a per-sample `fragment_missing` skip rather than an
  uncaught `IndexError`.
- The BM25 leg stays whole-dataset (`whole_dataset_text_legs`) because inverse document frequency
  and average document length are corpus-global statistics that cannot be sharded per fragment
  without changing the scores.

`combine_large_group_scores` grades vector samples from the reduced leg, text samples from the BM25
leg, and hybrid samples by fusing both with the recorded strategy. `aggregate_scores` builds the
report table in order — overall, per RPC-parameter bucket (`nprobes_min`/`nprobes_max`/
`refine_factor`), per query type, per organization — and `emit_recall_metrics` gauges only the RPC
and query-type buckets (bounded cardinality). Org-level numbers stay in the stdout table so the
metric tag space does not grow with tenant count.

## Tests

`tests/test_recall.py` covers sample parsing, filter-AST translation, span sources, and the
`tools/cli.py` recall subcommand surface. `tests/test_recall_quality.py` covers nDCG/MRR grading
against hand-computable rankings plus end-to-end text and hybrid replay through
`InMemorySpanSource` against tiny real local Lance datasets. `tests/test_recall_scoring.py` covers
brute-force scoring, version pinning, and the aggregation/report math. `tests/test_recall_tiering.py`
covers the small/large size split and asserts the fanned-out, driver-reduced top-k equals the
single-stream whole-dataset brute force exactly, both at the primitive level and through the full
job, with Spark replaced everywhere by the `FakeSpark`/`FakeSparkContext` fixtures in
`tests/conftest.py` since this job only ever calls `parallelize().map().collect()`.
