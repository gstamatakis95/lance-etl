# Use cases: who runs Lance in production and for what

Round-2 survey of public Lance and LanceDB production deployments, organized by use-case family. Each entry
carries its sources and a "relevance to our shape" line, read through our lens: per-namespace fleets of up to
30,000 org datasets, up to 1 billion rows, power-law sizes, three concurrent jobs (ingest via merge_insert,
compaction, indexing), a Rust gRPC search service with disk-backed caching, Datadog observability, S3-class
object storage.

Only findings that an adversarial verification pass rated `verified` or `partially` are included. Entries
marked **[partial]** had at least one sub-claim corrected or left unconfirmed. Claims that were outright
contradicted are collected once in the final "corrected misconceptions" section.

This file complements round-1 production-patterns.md, which already covers Krylasov (700 M single machine),
Pure Storage FlashBlade (100 M compaction benchmark), and the upstream performance-guide facts. Netflix,
Metagenomi, WeRide, Cognee, Exa, and Character.ai appear in both rounds. Here they are expanded with the
round-2 verified detail and the corrections found during verification.

---

## Family 1: multimodal ML training and media data lakes

The dominant Lance use case: petabyte-scale tables that combine raw media (frames, audio, point clouds),
canonical metadata, and ML outputs (embeddings, features) under one schema, feeding both hybrid search and
PyTorch/JAX dataloaders, with zero-copy column append as the iteration primitive.

### Netflix — Media Data Lake **[partial]**

Petabyte-scale multimodal creative assets (video frames, multichannel audio, captions, subtitles, scripts,
embeddings, annotations) across the catalog, unified as "Media Tables" combining canonical metadata and ML
outputs. Sustains 20,000+ QPS of vector search and 5+ million IOPS from a distributed NVMe SSD cache fleet.
Billions of vectors indexed in hours. Hybrid queries (vector plus FTS plus SQL) on the same tables, direct
PyTorch and JAX dataloaders, and Python UDFs for declarative feature extraction. Netflix formalized a
"Media ML Data Engineering" specialization to operate it. Zero-copy schema evolution lets schema change
without petabyte rewrites.

- Sources: https://www.lancedb.com/blog/case-study-netflix , https://www.infoq.com/news/2025/08/netflix-ml-data-eng/
- Verified: 20,000+ QPS, 5+ million IOPS from NVMe, the "Media ML Data Engineering" specialization, Lance as
  the underlying Media Tables format, and InfoQ independently confirming the lake is powered by LanceDB.
- Corrected: scale is "petabytes", not "tens of petabytes". All figures originate from the LanceDB marketing
  blog, not a Netflix primary engineering source. The 5 M IOPS NVMe figure describes Netflix's own cache
  fleet, which is architecturally a peer to (not the same thing as) our Rust service disk_cache.
- Relevance to our shape: this is the closest reference architecture for the read side. 20K QPS plus a
  multi-million-IOPS NVMe cache is the load profile our gRPC search service plus disk_cache and store_cache
  are built for. Hybrid vector-plus-SQL filtering validates the typed Filter AST.

### Runway — 1.8 TB in-memory video training pipeline

Generative-AI video company using Lance for model training. The cited capability is appending columns without
rewriting datasets, combined with fast random access and multimodal support, over a 1.8 TB in-memory video
training pipeline.

- Sources: https://www.lancedb.com/customers (Runway quote: "Lance transformed our model training pipeline")
- Verified: customer page shows the 1.8 TB figure and the append-without-rewrite quote, corroborated by the
  lance.org data-evolution guide.
- Relevance to our shape: validates zero-copy column append, the same mechanism our ETL relies on when
  merge_insert adds columns without full rewrites, and the blob/take dataloader path for the head orgs.

### ByteDance Volcano Engine LAS — PB-scale autonomous-driving data lake **[partial]**

LAS (Lake for AI Service) uses Lance as core storage. A named automotive client processes ~10 PB camera data,
1 PB LiDAR point clouds, and 1 TB annotation metadata. Dynamic schema management adds annotation columns
without rewriting history (~30% storage cost reduction). ZSTD achieves ~70% compression on point clouds.
Column projection plus efficient row indexing yields 96% GPU utilization, and 40% faster model-training
delivery.

- Sources: https://www.lancedb.com/blog/volcano-engine-autonomous-driving-data-lake-solution
- Verified: the 10 PB / 1 PB / 1 TB split, ZSTD ~70% on point clouds, 96% GPU utilization, 40% training
  acceleration, 30% storage reduction. ZSTD encoding exists in the checkout
  (`rust/lance-encoding/src/compression.rs`).
- Corrected: the "3x EB-scale data processing efficiency" wording is wrong. The post says "3x PB-scale" and
  "3x end-to-end (10 PB label processing: 4 days to 1 day)". It is PB-scale, not EB-scale.
- Relevance to our shape: zero-cost column append at PB scale plus the value of ZSTD for cold-tail storage
  cost. The 96% GPU utilization via efficient I/O informs dataloader design for head orgs.

### Midjourney and Character.AI — high-traffic image and multimodal search **[partial]**

Both are named LanceDB production customers. Midjourney chose LanceDB as the only solution meeting its
high-traffic and large-scale requirements. Character.AI is cited at 100 M+ images processed in parallel and
migrated FTS off ElasticSearch.

- Sources: https://techcrunch.com/2024/05/15/lancedb-which-counts-midjourney-as-a-customer-is-building-databases-for-multimodal-ai/ ,
  https://www.lancedb.com/customers ,
  https://medium.com/crv-insights/powertothedeveloper-crvs-investment-in-lancedb-1873ad82adac
- Verified: TechCrunch names Midjourney, Character.ai, WeRide, and Airtable as customers. Character.AI
  "100M+ images processed in parallel" appears on the customer page.
- Corrected: the Nadia Ali quote is NOT in the TechCrunch article. It appears in the CRV Insights post with
  slightly different wording ("LanceDB was the only one that could meet the high-traffic and large scale
  requirements we had"). The "90% latency reduction" claim has no confirmed primary source. Keep both with
  caveats.
- Relevance to our shape: Character.AI's 100 M-parallel-image scale is in range of our 1B-row head orgs, and
  validates the embedded-no-server model our search API wraps.

---

## Family 2: autonomous driving and sensor fleets

Recurring scheduled re-indexing over continuously growing multimodal sensor embeddings, with metadata-filtered
vector search as the dominant query, is the operational shape closest to ours.

### WeRide — autonomous driving, 90x ML productivity

Commercial autonomous-driving company (Nasdaq: WRD) with driverless permits in four countries. Sensor data
(camera, radar, lidar, video) is embedded and ingested into LanceDB on an in-house storage cluster. Indexes
are rebuilt on weekly and monthly cadences. Engineers run text-to-image and image-to-image search with
metadata filtering, returning top-1000 results for feature engineering and training. Results: 90x ML developer
productivity for data exploration and debugging, 3x reduction in training time via better I/O, edge-case data
mining cut from 1 week to 1 hour, millisecond P99 for top-1000 retrieval.

- Sources: https://www.lancedb.com/blog/werides-data-platform-transformation-how-lancedb-fuels-model-development-velocity
- Verified: all six headline claims confirmed verbatim (90x, 3x, 1 week to 1 hour, weekly plus monthly
  re-index, in-house storage cluster, millisecond P99 for top-1000).
- Relevance to our shape: the single best operational analogue. Recurring scheduled re-indexing maps directly
  to our compaction plus index Airflow DAG cadence, and metadata-filter-on-vector maps to our Filter AST. The
  weekly/monthly batch cadence validates not rebuilding continuously.

(ByteDance LAS in Family 1 is also an autonomous-driving deployment.)

---

## Family 3: scientific and genomics at billion-vector scale

### Metagenomi — 3.5 B protein embeddings, serverless Lambda plus S3

Enzyme discovery reframed as nearest-neighbor search. MGXdb holds ~3.5 billion 960-dim protein embeddings
(AMPLIFY_350M), ~12.9 TB on S3, bin-packed into ~200 M-vector buckets, each a separate table with an IVF-PQ
cosine index. AWS Step Functions fans out parallel Lambdas, one per bucket, results aggregated in pandas.
Indexing took 108 compute hours on i4i.8xlarge. Up to 50,000 ANN results per query at fractions of a cent, no
persistent servers. More recent reporting cites growth toward ~15 billion entries.

- Sources: https://aws.amazon.com/blogs/architecture/a-scalable-elastic-database-and-search-solution-for-1b-vectors-built-on-lancedb-and-amazon-s3/ ,
  https://talkpython.fm/episodes/show/488
- Verified: 3.5 billion vectors, 960-dim AMPLIFY_350M, ~12.9 TB, 108 compute hours, up to 50,000 ANN per
  Lambda, IVF-PQ cosine, ~200 M-vector buckets, Step Functions. The ~15 billion growth figure is not in the
  AWS blog but is corroborated by the CEO on Talk Python citing a 15B-vectors-in-one-table customer.
- Relevance to our shape: validates serverless LanceDB-on-S3 at multi-billion scale. Bucket sharding parallels
  our per-fragment segment-API index build (create_index_uncommitted per shard, merge on driver). Already
  expanded in round-1 production-patterns.md.

---

## Family 4: enterprise RAG, code, and knowledge intelligence

High-velocity upserts into many tables, metadata-filtered retrieval at single-digit-million to 15 M rows, and
air-gapped single-binary deployment dominate this family.

### Harvey — legal AI, enterprise RAG across 45 countries

Legal AI platform that chose LanceDB Enterprise as its primary production vector database (Postgres plus
pgvector kept only for prototyping on small public data). Three data classes: per-thread user uploads (1-50
docs), Vault project storage (1,000-10,000 docs), and third-party regulatory databases across jurisdictions.
Sub-2-second P50 latency on 15 million rows with metadata filtering, using a massive-scale IVF-PQ index with
tunable recall/latency. Selected for horizontal scale (storage decoupled from compute), privacy via
customer-controlled buckets, and air-gapped deployment. 91% user preference over off-the-shelf ChatGPT on
complex tax-law queries.

- Sources: https://www.harvey.ai/blog/enterprise-grade-rag-systems , https://x.com/harvey__ai/status/1892715372782157880
- Verified: all seven claims confirmed, including "<2s P50 for 15M rows with metadata filtering", "primarily
  use LanceDB Enterprise in production", customer-controlled buckets, tunable IVF-PQ, 45 countries, 91%
  preference.
- Relevance to our shape: the single best validation of the Filter AST direction. Metadata filtering on 15 M
  rows at <2s P50 is exactly the workload `grpc/domain/filter.rs` targets. Confirms the horizontal,
  S3-backed, storage-decoupled-from-compute model.

### CodeRabbit — AI code review, 50K+ daily PRs **[partial]**

LanceDB is the "context engine" for AI code review: a living knowledge graph of historical PRs, issue metadata
(Jira/Linear), dependency relationships, architectural patterns, and chat logs. Processes 50,000+ daily PRs at
P99 under 1 second, dynamically querying tens of thousands of tables, millions of reviews monthly. High-velocity
upserts ingest new commits without downtime. Single lightweight binary enables cloud and air-gapped on-prem.

- Sources: https://www.lancedb.com/blog/case-study-coderabbit
- Verified: 50K+ daily PRs at P99 under 1s, tens of thousands of tables, millions of reviews monthly, single
  air-gapped binary, "reduces code issues by up to 50%", scales 100x without runaway cost.
- Corrected: the "50% reduction in PR merge time" claim is misstated. The page says "50%+ reduction in manual
  review effort", not merge time.
- Relevance to our shape: validates the multi-table fan-out query pattern across tens of thousands of tables
  (our 30K-namespace fleet) and embedded single-binary, S3-only deployment of the search service.

### Dosu — codebase knowledge base, 70% triage reduction

Transforms codebases into living knowledge bases with real-time semantic search and built-in versioning.
90% label accuracy in automated issue classification, 70% reduction in manual triage, millisecond search over
millions of vectors. Built-in versioning enables time-travel queries over evolving codebases.

- Sources: https://www.lancedb.com/blog/newsletter-july-2025 , https://www.lancedb.com/customers
- Verified: all claims confirmed in the July 2025 newsletter, plus the founder quote. Lance versioning and
  time travel are real in source (`python/python/lance/dataset.py` versions, checkout_version, tags).
- Relevance to our shape: validates Lance dataset versioning, which our ETL uses implicitly through merge_insert
  version tracking and our compaction read_version pinning. Time travel underpins reproducibility tagging.

### Second Dinner (Marvel Snap) — game-dev RAG **[partial]**

Used LanceDB Cloud to power internal AI tools: proprietary codebases, design docs, and Jira tickets embedded
into a Slack-integrated RAG system. Prototyping cut from months to hours. QA: 81% of AI-generated tests
outperform human-written ones (20.5% rated considerably better). Production-ready in two weeks. LanceDB Cloud
handled compaction, incremental indexing, and full rebuilds transparently without disrupting live services.

- Sources: https://www.lancedb.com/blog/second-dinners-secret-weapon-lancedb-powered-rag-for-faster-smarter-game-development
- Verified: 81% / 20.5% test quality, months-to-hours, production in two weeks, and managed compaction plus
  incremental indexing plus full rebuilds without service disruption.
- Corrected: the exact "3-5x cost advantage" multiplier was not restated verbatim (page says "cost-effective
  than alternatives"). Keep with caveat.
- Relevance to our shape: validates that managed compaction and incremental indexing can run without
  disrupting live search, the exact non-blocking property our compaction.py and indexing.py must preserve.
  The zero-ops framing matches our Airflow automation goal.

---

## Family 5: embedded and per-tenant agent memory

### Cognee — agent memory with per-user isolated stores **[partial]**

AI memory and knowledge-management platform giving truly isolated vector stores per user and per test, plus a
knowledge-graph layer over vector search.

- Sources: https://www.lancedb.com/customers (and the cognee.ai blog, not independently re-fetched)
- Verified: the customer page shows "10x More Efficient Vector Storage" and the "truly isolated vector stores
  per user and per test" quote.
- Corrected/unverified: the SQLite + LanceDB + Kuzu default-stack detail and the Qdrant/pgvector/Neo4j
  swap-in capability come from the cognee.ai blog and were not confirmed from the customer page. Keep with
  caveat.
- Relevance to our shape: per-user isolation directly validates our dataset-per-namespace design across 30,000
  namespaces. The 10x storage-efficiency claim is a useful anchor for estimating S3 cost across the fleet.

### AnythingLLM — embedded local/private RAG

Open-source platform for chatting with documents and agentic workflows with full local privacy. LanceDB is the
default embedded vector DB, claimed as the only embedded option in the Node.js ecosystem. Powers both the RAG
pipeline and agent memory, serverless and setup-free, even on Copilot AI PCs.

- Sources: https://www.lancedb.com/blog/anythingllms-competitive-edge-lancedb-for-seamless-rag-and-agent-workflows
- Verified: all four claims confirmed (Node.js-only embedded option, nearly 100% of users on LanceDB,
  serverless setup-free, runs on Copilot AI PCs). The Node.js-uniqueness claim is the publisher's assertion,
  not independently audited.
- Relevance to our shape: validates the embedded-no-separate-server deployment model our Rust gRPC service
  wraps, where LanceDB opens dataset files directly from disk or S3.

---

## Family 6: web-scale search indexes

### Exa — petabyte web index, fragment-level column surgery

Search engine for AI systems over hundreds of billions of web pages, petabytes of raw content, entirely on
Lance on S3. The "exa-d" architecture has Logical (dependency graphs of column relationships), Storage (Lance
fragments for surgical updates), and Execution (Ray Data with stateful GPU/CPU actors) layers. The most-relied-on
Lance capability is writing or deleting a single column for a specific fragment without rewriting the rest, with
atomic manifest operations on S3 for distributed consistency. The system diffs ideal-vs-actual dataset state and
computes only missing or invalid columns.

- Sources: https://exa.ai/blog/exa-d , https://www.lancedb.com/customers
- Verified: all claims confirmed in the Exa blog, with atomic-manifest/ACID-commit corroborated by the Lance
  table-format spec.
- Relevance to our shape: fragment-based incremental materialization is architecturally identical to our ETL
  merge_insert pattern, and "atomic manifest on S3" is exactly the commit protocol our compaction and index
  segment commits depend on. Already in round-1 production-patterns.md.

---

## Family 7: platform and infrastructure collaborations

### Uber AI Infrastructure — multi-base layout for multi-bucket datasets

Uber's AI Infrastructure team co-designed Lance's multi-base layout: a single dataset spanning multiple S3
buckets for parallel reads and writes, while keeping file references as relative paths plus a separately tracked
list of base URIs, so datasets relocate without metadata rewrites. Contrasts with Iceberg and Delta absolute
paths. The specific Uber workload is undisclosed.

- Sources: https://www.lancedb.com/blog/rethinking-table-file-paths-lance-multi-base-layout
- Verified: attribution to Uber's AI Infrastructure team, multi-bucket spanning, relative-path portability,
  and the favorable contrast vs Iceberg/Delta.
- Relevance to our shape: if we ever shard a single head-org namespace across multiple buckets, this is the
  feature to use. It also underpins the shallow-clone and distributed index-commit mechanics (base_paths) our
  pipeline leverages. See production-techniques.md.

---

## Adoption signals (context, verified)

- Funding: $8 M seed (May 2024, led by CRV, Essence VC, Swift Ventures, $11 M total including YC); $30 M
  Series A (June 2025, HN id=44366880) framed as a shift from "mere vector database into a next-gen data
  management platform for AI".
- Traction: ~600,000 OSS downloads/month as of May 2024.
- Scale ceiling: CEO Chang She (Talk Python) cites enterprise customers with 15 billion vectors in one table
  requiring GPU-accelerated indexing (GPU cutting indexing by "more than like 15, 20X"). GPU acceleration is
  real in source (`python/python/lance/dataset.py:3325-3438`, accelerator param gated to IVF_PQ).
- Sources: https://techcrunch.com/2024/05/15/lancedb-which-counts-midjourney-as-a-customer-is-building-databases-for-multimodal-ai/ ,
  https://news.ycombinator.com/item?id=44366880 , https://talkpython.fm/episodes/show/488
- Relevance to our shape: 15 B vectors in a single table is the same order of magnitude as our 1B-row head
  orgs across a 30K-namespace fleet, validating the architecture ceiling. GPU indexing speedup is relevant if
  we adopt GPU nodes (caveat: that path is IVF_PQ in the LanceDB SDK, while our build uses IVF_RQ segment API).

---

## Corrected misconceptions

These specific sub-claims were contradicted during verification. They are stated once here with the correction
so they are not propagated.

1. Netflix scale is "petabytes", NOT "tens of petabytes". And the figures are LanceDB marketing, not a Netflix
   primary engineering source.
2. ByteDance LAS efficiency gain is "3x PB-scale" (and "10 PB label processing: 4 days to 1 day"), NOT "3x
   EB-scale".
3. CodeRabbit delivered "50%+ reduction in manual review effort", NOT "50% reduction in PR merge time".
4. The Midjourney "only solution that could meet our high-traffic and large-scale requirements" quote is from
   the CRV Insights post, NOT the TechCrunch article. The "90% latency reduction" for Midjourney/Character.AI
   has no confirmed primary source.
5. Second Dinner's "3-5x cost advantage" was not restated verbatim in the results section (page says
   "cost-effective than alternatives").
