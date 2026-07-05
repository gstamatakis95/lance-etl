# RAG and agent-memory use cases on Lance / LanceDB

How teams run Lance and LanceDB as the retrieval store in production RAG and agent-memory systems, mapped to the
lance-etl pipeline (Iceberg to per-org Lance, segment-API indexing, compaction, and the Rust gRPC search service).

Verification legend used throughout:
- VERIFIED-CHECKOUT: confirmed against the read-only lance checkout at `/Users/gstamatakis/IdeaProjects/lance`.
- VERIFIED-DOCS: confirmed against lance.org / docs.lancedb.com documentation pages.
- BLOG: vendor blog, case-study, or third-party assertion, not independently confirmed against code or docs.

All lance `path:line` citations below were spot-checked against the checkout during this research round.

---

## Angle 1 — RAG architectures on Lance: ingest, layout, freshness

### 1.1 Chunk-embed-store is the standard ingest shape, and chunking dominates retrieval quality
Production RAG pipelines read documents, chunk them (fixed-size with overlap or semantic-boundary), embed each chunk,
and write chunk-row plus vector plus metadata into the store. Multiple guides assert chunking quality constrains
retrieval accuracy more than embedding-model choice (one cited 2025 study claims 87 percent vs 13 percent accuracy for
adaptive vs fixed-size chunking).
- Sources: PremAI production-RAG guide (https://blog.premai.io/building-production-rag-architecture-chunking-evaluation-monitoring-2026-guide/),
  Alon Agmon Rust + LanceDB + Candle indexing pipeline (https://medium.com/data-science/scale-up-your-rag-a-rust-powered-indexing-pipeline-with-lancedb-and-candle-cc681c6162e8).
- Status: BLOG.
- Relevance to lance-etl: chunking and embedding happen upstream in Iceberg. lance-etl owns the store side. Our
  `etl.py` collapse-and-merge keeps one terminal row per `key_col`, which fits chunk-level upsert if the chunk id is the
  key. Worth confirming the upstream producer emits a stable per-chunk key so re-chunking a changed document replaces
  old chunks rather than orphaning them.

### 1.2 Freshness via incremental re-indexing of changed documents, not full rebuilds
The dominant freshness pattern is incremental re-ingestion: hash documents, re-embed only changed ones, upsert changed
chunks, and delete chunks of removed or access-revoked documents. Daily hash-diff jobs suffice for most pipelines,
event-driven webhooks for real-time sources.
- Sources: PremAI guide (above). LanceDB incremental processing for multimodal freshness asserted on lancedb.com.
- Status: BLOG.
- Relevance to lance-etl: this is exactly our shape. `merge_insert(on=[key_col])` with
  `when_matched_update_all(condition=source.ts > target.ts)` plus `when_matched_delete` gives last-write-wins upsert and
  tombstoning (`etl.py:496-502`). `_ingested_at` stamping (`etl.py:732-748`) and the CDF `Dataset.delta()` path noted in
  the round-2 README give us change-since-last-run materialization. Gap: document-deletion-cascades-to-chunks is an
  upstream contract, not something our keyed merge enforces on its own.

### 1.3 Per-tenant dataset layout and access-control-as-metadata-filter
Multi-tenant RAG either isolates per-tenant datasets/tables or co-locates with a tenant/access column filtered at query
time. Guidance is explicit: filter by tenant and access level during retrieval (pre-filter), never retrieve-then-filter,
to avoid scanning the whole corpus and to keep top-k correct.
- Sources: PremAI guide (above), LanceDB filtering docs (https://docs.lancedb.com/search/filtering).
- Status: BLOG plus VERIFIED-DOCS for the prefilter mechanism.
- Relevance to lance-etl: we use physical per-org isolation, dataset URI
  `{base}/{org_id}/{tenant_id}/{namespace}.lance` (`recall.py:749-763`, `etl.py` partition routing default
  `(org_id, tenant_id, namespace)`). That is the stronger isolation model than a tenant column. Our gRPC prefilter
  (`FilterMode::Prefilter` default, `query.rs:22-29`) covers the within-dataset attribute case.

### 1.4 Lance + Iceberg as a two-format lakehouse: Iceberg for BI, Lance for AI access patterns
LanceDB positions Iceberg (long-narrow OLAP, three-level metadata) and Lance (file plus table plus catalog, single-level
metadata, random access, native multimodal blobs) as complementary on the same object store. The blog does not detail an
Iceberg-to-Lance movement mechanism, only that both share the bucket.
- Source: From BI to AI: Lance and Iceberg (https://www.lancedb.com/blog/from-bi-to-ai-lance-and-iceberg).
- Status: BLOG.
- Relevance to lance-etl: validates our core architecture choice (read Iceberg, write Lance). The blog leaves the
  movement mechanism unspecified, which is precisely what our Spark ETL implements. No public LanceDB primitive does the
  Iceberg-to-Lance bridge we built, so our pipeline fills a real gap.

---

## Angle 2 — Retrieval-quality techniques

### 2.1 Hybrid retrieval (vector + BM25/FTS) with RRF as the default fusion
LanceDB hybrid search runs a vector leg and a full-text BM25 leg and fuses them. The default reranker is `RRFReranker()`,
reciprocal-rank fusion, chosen because it operates on ranks not scores and so avoids the score-incompatibility that
breaks naive weighted averaging. `query_type="hybrid"`, `vector_column_name`, and `fts_columns` select the legs.
- Sources: Hybrid Search docs (https://docs.lancedb.com/search/hybrid-search), Reranking docs
  (https://docs.lancedb.com/reranking), hybrid+BM25 blog
  (https://www.lancedb.com/blog/hybrid-search-combining-bm25-and-semantic-search-for-better-results-with-lan-1358038fe7e6).
- Status: VERIFIED-DOCS (RRF default, rank-based fusion, leg selection).
- Relevance to lance-etl: our gRPC service already implements this. `HybridQuery` carries a vector leg, a text leg, and
  a `FusionSpec` (`query.rs:213-224`), and `FusionSpec::Rrf` defaults to `rrf_k = 60.0` (`fusion.rs:10,23-27`). We match
  the LanceDB default behavior including the rank-based formula `1/(rrf_k + rank)` (`fusion.rs:45-49`).

### 2.2 Weighted (linear-combination) fusion is the alternative, model-free, knob
`LinearCombinationReranker` normalizes vector and FTS scores and blends with a single `weight`, default `0.7` (70 percent
semantic, 30 percent FTS). It and RRF are the model-free, low-cost options. There is also `MRRReranker`.
- Source: Reranking docs (https://docs.lancedb.com/reranking), Linear Combination reranker
  (https://lancedb.github.io/lancedb/reranking/linear_combination/).
- Status: VERIFIED-DOCS.
- Relevance to lance-etl: our `FusionSpec` is an enum with only an `Rrf` variant today (`fusion.rs:14-21`), explicitly
  designed so new strategies slot in as variants with their own `fuse` arm. A `Weighted { vector_weight }` variant is a
  small, well-scoped addition that matches LanceDB's second built-in.

### 2.3 Reranking (cross-encoder / hosted LLM) layered on top of ANN is standard in production RAG
Beyond model-free fusion, LanceDB ships model-based rerankers: `CrossEncoderReranker` (sentence-transformers),
`CohereReranker`, `JinaReranker`, `OpenaiReranker`, `VoyageAIReranker`, `ColbertReranker`, `AnswerdotaiRerankers`. Ten
built-ins total. Guidance: reach for model-free (RRF, LinearCombination) when cost and latency dominate, model-based when
relevance matters and you can afford to score every query-document pair. Rerankers apply to vector, FTS, or hybrid
results, not only hybrid. Production playbooks treat two-stage retrieve-then-rerank as the default for quality.
- Sources: Reranking docs (https://docs.lancedb.com/reranking), Cross-encoder reranker
  (https://lancedb.com/documentation/reranking/cross_encoder/), hybrid+rerank playbook
  (https://optyxstack.com/rag-reliability/hybrid-search-reranking-playbook).
- Status: VERIFIED-DOCS (reranker list and applicability).
- Relevance to lance-etl: this is our biggest functional gap. The gRPC service fuses with RRF and returns
  (`grpc/mod.rs`, `fusion.rs`) but has no reranking stage. We have no cross-encoder or LLM rerank hook. See gaps below.

### 2.4 Pre-filter vs post-filter at query time
Pre-filtering (default, `prefilter=True`) applies the `where(...)` predicate before ANN so only matching rows are
searched, keeping top-k exact relative to the filter. Post-filtering (`prefilter=False`) searches first then filters, can
be lower-latency for expensive or non-indexable predicates, but may return fewer than `limit` rows or zero. Scalar
indexes (BTREE, BITMAP, LABEL_LIST for list columns) are strongly recommended on filtered columns.
- Source: Filtering docs (https://docs.lancedb.com/search/filtering).
- Status: VERIFIED-DOCS.
- Relevance to lance-etl: our domain models both modes (`FilterMode::Prefilter` default vs `Postfilter`,
  `query.rs:21-29`, plus `maximum_nprobes` "only effective with a prefilter" at `query.rs:46-47`), matching LanceDB
  semantics exactly. Our BTREE/BITMAP segment-API builds (`indexing.py`, AGENTS rule 6) are the recommended scalar
  indexes that make pre-filter cheap. Typed `Filter` AST (`domain/filter.rs`, rule 7) is the safe equivalent of the SQL
  where clause.

### 2.5 Multi-vector / late-interaction (ColBERT, ColPali) is supported, cosine-only, IVF_PQ-backed, MaxSim-scored
LanceDB stores multiple vectors per row as a nested list `pa.list_(pa.list_(float32, dim))`, indexes them with the
standard `IVF_PQ` index, and scores with MaxSim (sum over query vectors of max similarity to any document vector). Only
the cosine metric is supported for multivector search. The late-interaction blog recommends a two-stage pattern: FTS or
semantic search narrows candidates, then ColPali MaxSim reranks, because brute-force MaxSim over all documents does not
scale (556 pages took 30-34 seconds in their test).
- Sources: Multivector search docs (https://docs.lancedb.com/search/multivector-search), late-interaction blog
  (https://www.lancedb.com/blog/late-interaction-efficient-multi-modal-retrievers-need-more-than-just-a-vector-index).
- Status: VERIFIED-DOCS for the API. VERIFIED-CHECKOUT for the cosine-only constraint:
  `rust/lance/src/index/vector.rs:542` and `:1228` both emit "multivector type supports only cosine distance", and ANN
  over multivectors is wired in `rust/lance/src/dataset/scanner.rs:4652`.
- Relevance to lance-etl: our indexer builds single-vector IVF_RQ only (AGENTS rule 6). Multivector would force IVF_PQ
  and cosine, conflicting with our IVF_RQ standard, and our schema assumes a fixed-size-list vector column
  (`recall.py:857-868`). This is a larger architectural change, not a drop-in. Track as future, not near-term.

---

## Angle 3 — Eval and observability for RAG retrieval

### 3.1 The retrieval metric set: recall@k, precision@k, MRR, nDCG, context-precision
The standard offline retrieval metrics are recall@k, precision@k, MRR, and nDCG. nDCG handles graded (not binary)
relevance and discounts by rank logarithmically, rewarding relevant items at rank 1 over rank 10. Context-precision
measures what fraction of retrieved chunks actually contribute to the answer.
- Sources: Braintrust RAG eval (https://www.braintrust.dev/articles/what-is-rag-evaluation and /rag-evaluation-metrics),
  langcopilot recall@k-to-faithfulness (https://langcopilot.com/posts/2025-09-17-rag-evaluation-101-from-recall-k-to-answer-faithfulness),
  GeeksforGeeks RAG metrics (https://www.geeksforgeeks.org/nlp/evaluation-metrics-for-retrieval-augmented-generation-rag-systems/).
- Status: BLOG (industry-consensus definitions).
- Relevance to lance-etl: our recall-audit job computes recall@k only (`recall.py:1004-1014`), as binary
  set-intersection of served ids against brute-force truth. It does not compute nDCG, precision@k, or MRR, and it has no
  graded relevance. recall@k is the right primary metric for ANN-vs-exact drift, but nDCG/MRR would catch rank-order
  degradation that recall@k misses.

### 3.2 Golden sets plus online sampling of production queries for offline scoring
Teams maintain a golden set (30-50 real or realistic queries) as a regression gate, and continuously sample live
production queries for offline or A/B scoring. Modern practice blends reference-based dev-time eval with reference-free
LLM-as-a-judge for production monitoring.
- Sources: Braintrust (above), Label Your Data RAG eval 2026 (https://labelyourdata.com/articles/llm-fine-tuning/rag-evaluation),
  Anyscale RAG eval (https://docs.anyscale.com/rag/evaluation).
- Status: BLOG.
- Relevance to lance-etl: our recall job already does the online-sampling half well, and arguably better than most: the
  Rust service samples real production vector queries onto Datadog spans (query vector, RPC params, typed filter AST,
  served version, served ids), and the Spark job replays each against the exact pinned dataset version with brute-force
  truth (`recall.py:1-28`, `telemetry/recall.rs`). This is a self-labeling golden set generated from production, which is
  the strongest form of the pattern. Two extensions worth noting: (a) we have no LLM-judge relevance layer (we measure
  ANN-vs-exact agreement, not answer relevance), and (b) we sample vector queries only, not FTS or hybrid queries.

### 3.3 Version-pinned offline replay is unusually rigorous
Most RAG eval pipelines score against a moving index. Our job pins to the exact committed Lance version that served each
sampled query and falls back to latest with a drift flag only when that version was cleaned up (`recall.py:766-786`,
`120-145` SampleScore.version_drift). This removes index-drift as a confound in recall measurement.
- Status: internal design observation, no external equivalent found in the surveyed sources.
- Relevance to lance-etl: this is a differentiator. The freshness-vs-cleanup interaction (README round-2 item 3) directly
  feeds it: keep the cleanup horizon longer than the recall-audit lookback window or drift counts will rise.

---

## Angle 4 — Scale and multitenancy in RAG

### 4.1 Billion-vector single-store performance claims
LanceDB asserts 1.3 ms p99 to search 1 billion vectors with IVF-PQ on an r5.8xlarge, ingestion of 3 million 512-dim
vectors per minute on one GPU-builder machine, and "index billions of vectors in hours" with ">10,000 QPS at <50 ms".
Indexes (IVF-PQ, IVF-HNSW, inverted text, bitmap) live next to data in object storage with stateless compute that scales
to zero.
- Sources: BrightCoding LanceDB overview (https://www.blog.brightcoding.dev/2025/09/25/lancedb-the-open-source-multimodal-ai-lakehouse-that-delivers-millisecond-vector-search-across-billions-of-images-text-audio-files/),
  IVF_PQ benchmark blog (https://www.lancedb.com/blog/benchmarking-lancedb-92b01032874a-2), Lance+Iceberg blog (above).
- Status: BLOG (vendor benchmarks, hardware-specific).
- Relevance to lance-etl: validates the 1B-row-per-namespace target in our README. Compute-storage separation with
  stateless query nodes is the model our Rust service plus disk cache plus prewarm implements.

### 4.2 Index-type cost/latency/recall tradeoffs: IVF_PQ vs IVF_RQ vs IVF_HNSW vs IVF_FLAT
Verified guidance from the vector-index docs:
- IVF_PQ: compression 1/64 to 1/16 of raw (depends on `num_sub_vectors`, start `dimension // 8`), often higher accuracy
  than IVF_RQ at similar perf for dimensions <=256.
- IVF_RQ (RaBitQ): very strong compression, around 1/32 of raw, preferred for maximum compression, better for filtered
  workloads than HNSW-backed variants.
- IVF_HNSW_FLAT / _SQ / _PQ: strong quality at low latency, but higher latency variance under metadata-filtered queries.
- IVF_FLAT: highest recall (no quantization), and the only variant for binary vectors with Hamming distance.
- Explicit rule: "If your vector search frequently includes metadata filters (where(...)), prefer IVF_RQ or IVF_PQ."
- Knobs: `nprobes` (sets both `minimum_nprobes` and `maximum_nprobes`), `ef` (start ~1.5*k, up to 10*k), `refine_factor`
  (reads extra candidates and re-ranks in memory to recover quantization recall loss).
- Sources: Vector index docs (https://docs.lancedb.com/indexing/vector-index).
- Status: VERIFIED-DOCS. RaBitQ / IVF_RQ existence VERIFIED-CHECKOUT (`rust/lance/src/index/vector/ivf.rs`,
  `details.rs`, `builder.rs`); `build_rq_model` python binding VERIFIED-CHECKOUT (`python/python/lance/dataset.py:4021`,
  test at `python/python/tests/test_vector_index.py:3047`).
- Relevance to lance-etl: our standard IVF_RQ choice (AGENTS rule 6) is exactly LanceDB's recommendation for
  filter-heavy workloads, and our gRPC service is filter-heavy by design (typed Filter AST, prefilter default). Our
  domain query already exposes `nprobes`, `minimum_nprobes`, `maximum_nprobes`, `refine_factor`, and `ef`
  (`query.rs:42-51`), the full LanceDB knob set. One nuance: docs say IVF_PQ can beat IVF_RQ on accuracy for dim<=256,
  so for low-dim embedding models a per-org index-type choice could be worth benchmarking via the recall job.

### 4.3 Caching / prewarm for tenant cold-start and blue-green cutover
Stateless query nodes reading from object storage suffer cold-start latency on first tenant access. The general pattern
is to warm caches and to cut over atomically between index versions. LanceDB's own tag-based serving (a `prod` tag per
dataset, atomic `tags.update`) is the blue-green primitive.
- Sources: BrightCoding overview (above) for stateless compute, README round-2 item 2 for tag-based blue/green.
- Status: BLOG plus internal.
- Relevance to lance-etl: directly matches our disk cache, Prewarm (`rust/search-api/src/lance/prewarm.rs`,
  `domain/prewarm.rs`), and the blue-green plan (`market-research/prewarm-blue-green-plan.md`). The power-law tail (most
  orgs tiny, rarely queried) is the cold-start hot spot, so prewarm-on-first-touch plus a small per-tenant cache budget
  is the right shape.

### 4.4 Freshness vs compaction tradeoff
Frequent merge_insert upserts fragment the dataset and grow delta indexes, which compaction and index-merge must clean
up, but compaction contends with concurrent ingest and index builds. This is the central operational tension our README
and concurrency docs already analyze in depth.
- Sources: README round-1/round-2 takeaways, lancedb issue #2751 referenced in README item 4.
- Status: internal, cross-referenced.
- Relevance to lance-etl: covered by our DAG ordering (etl >> compact >> index), `optimize_indices()` append-only
  maintenance, delta-merge thresholds, and stable-row-id endgame (README items 3-7, 10). The RAG-specific angle: higher
  freshness cadence raises delta-index count, which raises query latency, which the recall job and Datadog latency
  metrics should jointly monitor to find the cadence sweet spot.

---

## Angle 5 — Named production cases (primary sources)

### 5.1 Harvey (legal AI) — enterprise RAG on LanceDB Enterprise
Harvey selected LanceDB Enterprise for production after evaluating it against Postgres+pgvector. Claimed: sub-2-second
latency for 15 million rows with metadata filtering (vs pgvector sub-2-second at 500K embeddings), data in
customer-controlled buckets for privacy, three data sources (user uploads 1-50 docs, vault projects up to ~100K docs,
third-party corpuses of millions of legal docs), serving 45 countries, 91 percent preference over ChatGPT in tax law.
- Sources: LanceDB July 2025 newsletter (https://www.lancedb.com/blog/newsletter-july-2025), ZenML LLMOps writeup
  (https://www.zenml.io/llmops-database/enterprise-grade-rag-systems-for-legal-ai-platform), Harvey blog
  (https://www.harvey.ai/blog/enterprise-grade-rag-systems), talk
  (https://www.startuphub.ai/ai-news/ai-video/2025/building-enterprise-grade-rag-lessons-from-the-legal-frontier/).
- Status: BLOG.
- Relevance to lance-etl: closest analogue to our metadata-filtered-search-at-scale target. Sub-2s on 15M filtered rows
  validates our typed Filter AST plus scalar-index direction. Customer-controlled buckets mirror our per-org dataset
  isolation.

### 5.2 WeRide (autonomous driving) — multimodal retrieval over billions
WeRide ingests camera/lidar/radar embeddings plus metadata into LanceDB on an in-house storage cluster, achieving
millisecond P99 for top-1000 text-to-image or image-query retrieval over billions of items, 90x ML-developer
productivity, 3x training-time reduction, and data-mining time from 1 week to 1 hour.
- Source: WeRide case study (https://www.lancedb.com/blog/werides-data-platform-transformation-how-lancedb-fuels-model-development-velocity).
- Status: BLOG.
- Relevance to lance-etl: the README already names WeRide as our cadence analogue (scheduled re-indexing over growing
  embeddings). Confirms billion-scale per-namespace is realistic and that scheduled compaction-plus-index matches a real
  production workload.

### 5.3 Dosu — living knowledge base for codebases and agents
Dosu uses LanceDB as a versioned knowledge layer, citing millisecond search on millions of vectors, built-in
versioning, 90 percent label accuracy, and 70 percent less manual triage.
- Source: LanceDB July 2025 newsletter (https://www.lancedb.com/blog/newsletter-july-2025), case-study index
  (https://www.lancedb.com/category/case-study).
- Status: BLOG.
- Relevance to lance-etl: versioning-as-a-feature is something we already exploit (version-pinned recall replay,
  blue-green tags). Validates Lance versioning as a production-grade asset, not just a dev convenience.

### 5.4 Agent-memory plugins (OpenClaw memory-lancedb, memory-lancedb-pro)
A class of agent-memory plugins uses LanceDB as the long-term store with hybrid vector+BM25 retrieval fused via RRF,
cross-encoder reranking (Jina, Voyage, etc.), multi-scope isolation per agent, and a decay-based lifecycle (Weibull) that
lets stale memories fade. Embedded, filesystem-native deployment is the cited fit reason.
- Sources: LanceDB memory-layer blog (https://www.lancedb.com/blog/openclaw-lancedb-memory-layer),
  memory-lancedb-pro (https://github.com/CortexReach/memory-lancedb-pro), hybrid plugin
  (https://termo.ai/skills/memory-lancedb-hybrid).
- Status: BLOG.
- Relevance to lance-etl: these are embedded single-node deployments, the opposite of our distributed multi-tenant
  service, but two ideas transfer: (a) hybrid+rerank is treated as table-stakes for memory recall quality, reinforcing
  the rerank-hook gap, and (b) decay/lifecycle scoring (recency boost, time decay) is a retrieval-time re-weighting we do
  not have. Our `_ingested_at` column already carries the signal a recency boost would need.

---

## Gaps and candidate features for lance-etl

Ranked by value-to-effort for our distributed multi-tenant shape.

1. Rerank hook in the gRPC service (highest value). We fuse with RRF but never rerank. Add an optional post-fusion
   reranking stage to the search path so a cross-encoder or hosted LLM reranker can re-score the fused top-N. Keep it
   declarative like `FusionSpec`: a `RerankSpec` carried on the request, with a model-free no-op default so latency is
   unchanged when unused. This closes the single biggest functional gap vs LanceDB hybrid search and the agent-memory
   plugins. Note the no-raw-SQL and typed-AST conventions extend naturally to a typed rerank spec.

2. Weighted fusion variant (low effort). Add `FusionSpec::Weighted { vector_weight }` (LanceDB default 0.7) alongside
   `Rrf`. The enum was built for this (`fusion.rs:14-21`). Lets operators trade rank-based for score-based fusion per
   query without a model.

3. nDCG and MRR in the recall-audit job (medium effort, high signal). recall@k is binary and misses rank-order
   regressions. Since brute-force already produces distance-ordered truth (`recall.py:931`), nDCG with graded relevance
   from inverse-truth-rank and MRR are computable from data we already have, no new labels needed. Adds the rank-quality
   dimension the RAG-eval consensus expects.

4. Extend recall sampling to FTS and hybrid queries (medium effort). Today we sample vector queries only
   (`recall.py:474-498`). FTS recall (BM25 served vs brute-force term match) and hybrid recall (fused served vs an
   offline-fused reference) would cover the legs we cannot currently audit.

5. Recency-aware re-weighting at query time (low-medium effort). Agent-memory systems boost recent rows. We already stamp
   `_ingested_at`. An optional decay term applied to fused scores would serve memory-style use cases without schema
   change.

6. LLM-judge relevance layer for online sampled queries (larger effort). Our replay measures ANN-vs-exact agreement, not
   answer relevance. A reference-free LLM-judge over a small sampled fraction would add the answer-quality dimension that
   recall@k structurally cannot.

7. Multivector / late-interaction support (largest effort, track-only). Would require IVF_PQ plus cosine, conflicting
   with our IVF_RQ standard and fixed-size-list schema assumption. Real but architectural. Revisit only if a ColBERT/
   ColPali tenant requirement appears.

---

## Claims we could not verify

- LanceDB RRFReranker default rank constant K=60: our service uses 60 (`fusion.rs:10`) and 60 is the widely cited RRF
  default, but the reranking docs page text fetched did not state the numeric K. Treat the docs-side default as
  UNVERIFIED-DOCS pending a source that prints the value.
- "1.3 ms p99 to search 1 billion vectors with IVF-PQ on r5.8xlarge" and "3 million 512-dim vectors/min on one machine":
  vendor-blog benchmarks, hardware-specific, not reproduced here. BLOG only.
- ">10,000 QPS at <50 ms latency" and "index billions of vectors in hours": LanceDB marketing figures from the
  Lance+Iceberg blog, not independently confirmed. BLOG only.
- Harvey "sub-2-second latency for 15 million rows with metadata filtering" vs pgvector "500K embeddings": appears in the
  ZenML and newsletter writeups but the underlying benchmark methodology is not published. BLOG only.
- WeRide "millisecond P99 for top-1000 over billions" and "90x productivity / 3x training / 1 week to 1 hour": vendor
  case-study figures, BLOG only.
- "Adaptive chunking 87 percent vs fixed-size 13 percent accuracy" (PremAI guide citing a 2025 study): the underlying
  study was not located, numbers may be context-specific. BLOG only.
- The late-interaction blog detail "1030 patches per document at 128-dim" and "556 pages took 30-34 seconds": single-
  experiment figures from one blog post, not generalizable. BLOG only.
</content>
</invoke>
