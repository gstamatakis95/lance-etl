# Production patterns: maintaining massive Lance datasets

Distilled from public production case studies and the upstream performance guide, read through the lens of our
workload: up to 30,000 orgs per namespace, up to 1 billion rows total, power-law distributed (most orgs tiny, a
small head holding most data), with ingestion (merge_insert upserts and deletes), compaction, and indexing all
running concurrently against the same datasets.

## Case studies

### Vladyslav Krylasov — 700 M vectors on a single machine

Source: https://sprytnyk.dev/posts/running-lancedb-in-production/

Scale: 700 million vectors in one table, 1.6 TB final footprint, 32 CPU / 128 GB RAM.

Lessons:

- IVF_PQ index creation failed on the full 700 M dataset from RAM exhaustion. The fix was indexing in 50 M-vector
  batches with `num_partitions=sqrt(batch_rows)` and `num_sub_vectors=dimension/16`, calling `optimize()` between
  batches.
- Versioning caused rapid storage explosion. The `_transactions` and `_versions` directories ballooned to terabytes.
  Hourly cleanup plus a cron deleting stale index directories kept storage under control. Note that the post used
  `cleanup_older_than=timedelta(seconds=0)`, which the official docs explicitly warn against with any concurrent
  writers (it causes all concurrent write operations to fail). It worked there only because writes were serialized.
- Memory leaks from unclosed connections and tables hit production before lancedb 0.25.0. Fix: singleton connector
  and `open_table()` per request.
- Migrating from the prior solution cut monthly cost from $30,000 to $7,000.

Maintenance practices: hourly `optimize()` with `delete_unverified=True`, automated deletion of old index
directories (keep only latest scalar and vector indexes), batch indexing, redirected `TMPDIR` for large index
builds to avoid `/tmp` exhaustion.

### Metagenomi — 3.5 B protein embeddings on S3

Source: https://aws.amazon.com/blogs/architecture/a-scalable-elastic-database-and-search-solution-for-1b-vectors-built-on-lancedb-and-amazon-s3/

Scale: 3.5 billion 960-dim vectors, ~12.9 TB on S3, bucketed into ~200 M-vector chunks (17-18 buckets).

Lessons:

- Each ~200 M bucket is a separate table with its own IVF_PQ index (`num_partitions=sqrt(bucket_rows)`,
  `num_sub_vectors=dimension/16`). Ingest plus index of 3.5 B vectors took 108 compute-hours on i4i.8xlarge
  storage-optimized instances.
- Lambda queries buckets in parallel via Step Functions, up to 50,000 nearest neighbors per request with linear
  latency scaling. Larger buckets cut query cost but raise per-query latency, and 200 M was the chosen optimum.

Maintenance practices: static sharded architecture, each bucket append-only after initial load, no running
compaction. NVMe storage-optimized instances were required for both ingestion and index builds.

### Netflix — petabyte-scale media data lake

Source: https://www.lancedb.com/blog/case-study-netflix

Scale: petabyte-scale multimodal assets, 20,000+ sustained vector QPS, 5 M+ IOPS from a distributed NVMe cache
fleet.

Lessons:

- Zero-copy data evolution (schema changes without petabyte-scale rewrites) is critical for iteration speed.
- Incremental feature computation with checkpointing and preemption support is required for GPU inference jobs at
  this scale. Full recomputation is never viable.
- A distributed NVMe SSD cache fleet absorbs hot-path reads and reduces cloud storage read amplification.

### WeRide — autonomous driving sensor data

Source: https://www.lancedb.com/blog/werides-data-platform-transformation-how-lancedb-fuels-model-development-velocity

Scale: billions of multimodal sensor data points, continuously growing, sub-millisecond P99 for top-1000 retrieval.

Lessons:

- Scheduled re-indexing on weekly and monthly cadences as new embeddings accumulate, rather than continuous
  rebuild. Data mining time dropped from 1 week to 1 hour and ML training time dropped 3x from improved data I/O.
- Hybrid vector plus metadata-filter queries are the dominant production pattern.

### Cognee — multi-tenant per-workspace isolation

Source: https://www.lancedb.com/blog/case-study-cognee

Scale: per-workspace isolated LanceDB stores, file-based isolation with one directory per tenant, up to 10x more
efficient vector storage than the prior approach.

Lessons:

- File-based per-tenant isolation eliminates shared-state complexity. Compaction, indexing, and cleanup run
  independently per tenant with zero cross-tenant blocking.
- The Memify pipeline does continuous incremental refinement (clean stale nodes, reweight facts) instead of full
  rebuilds across many small isolated stores.

### Exa — petabyte web index, fragment-level column surgery

Source: https://www.lancedb.com/customers

Lessons:

- Fragment-level column operations (write or delete a single column for a specific fragment without rewriting the
  rest) are the primary maintenance primitive at petabyte scale, used for surgical index repair (re-embed a slice)
  and sweeping backfills (new model across billions of rows) without I/O amplification.

### Pure Storage FlashBlade — compaction and concurrency benchmark

Source: https://blog.everpuredata.com/purely-technical/scale-lancedb-production-ai/

Scale: 100.8 M-row table benchmark with IVF_PQ.

Lessons:

- Compaction reduced fragments from 507 to 168 (67%) in about 5 minutes, with ~4.3 ms write and ~2.3 ms read
  latency during the operation. Compaction is cheap and non-blocking at the 100 M scale when run regularly.
- S3 conditional writes (`If-None-Match`) are required for atomic commits and preventing silent data loss from
  concurrent writers.
- Compression varies dramatically by content: 1.2:1 for high-dim embeddings, 3.6:1 for low-dim vectors, 11.9:1
  for metadata-only data.

### Character.ai — FTS migration

Source: https://www.lancedb.com/customers

Migrating full-text search from ElasticSearch to LanceDB cut p90 latency by over 90%, with 100 M+ images
processed in parallel. No maintenance specifics disclosed.

## Performance-guide facts that bound our design

All from https://lance.org/guide/performance/ unless noted.

- Fragment count: keep under 100 for most use cases, with more allowed above 500 M rows. Always batch inserts.
- IO threads: default 8 local, 64 cloud. Use 128-256 on cloud to saturate network, raising `io_buffer_size`
  proportionally (~32 MB per IO thread minimum). Scan memory is roughly
  `(2 * io_buffer_size) + (batch_size * num_compute_threads)`.
- Caches: 1 GiB metadata LRU and 6 GiB index LRU by default (`index_cache_size_bytes`). Caches are not shared
  between table objects, so reuse one handle per dataset.
- Cloud AIMD throttling starts at 2000 req/s and converges to S3's 5000 req/s ceiling in ~10 s. Cold-start latency
  for small tenants is dominated by this ramp-up.
- IVF training memory is `num_partitions * 256 * dimension * 4` bytes. Quantizer storage at 100 M rows: PQ ~9.7
  GiB, SQ ~72.3 GiB, RQ ~10.8 GiB.
- Version cleanup default retention is 7 days, tagged versions exempt. Never set `cleanup_older_than` to zero
  with concurrent writers, and treat ~10 minutes as the absolute floor
  (https://docs.lancedb.com/tables/versioning, lancedb issue #2470).
- `optimize()` trigger threshold: 100,000+ records added or modified, or 20+ modification operations
  (lancedb issue #3201).
- Transaction throughput on object storage is roughly 1-4 commits/second per dataset because manifest writes are
  sequential (lance discussion #4151). Batch writes client-side before `merge_insert`.
- Bitmap indexes are currently extremely slow for large range predicates, so use BTree for ranges.
- Compaction to a single huge output file (~1 TB) is unreliable on cloud storage, so prefer multi-file targets
  (lance-format issue #2822). Partial-progress compaction (lance issue #3947) is not implemented, so compaction
  commits are all-or-nothing today.
- Geneva/Enterprise backfill guidance: operations that conflict with backfill are `compact_files()`, merge_insert
  with updates, and `delete()`. Insert-only merge_insert, `add()`, and reads are safe. Sequence ingest, then
  backfill, then compact (https://docs.lancedb.com/geneva/jobs/conflicts).

## Distilled patterns for our 30k-org power-law shape

1. Shard the head, batch the tail. Every billion-row deployment (Krylasov, Metagenomi) shards index builds into
   50-200 M-row units because single-pass training exceeds memory. Our segment API gives this per-fragment, but
   the lesson transfers to IVF training memory sizing and to capping partition counts on head orgs.

2. Per-tenant directory isolation is the proven multi-tenant model (Cognee). Maintenance for each org is fully
   independent, and the real cost moves to the scheduler. Batch maintenance sweeps across dormant tail orgs
   instead of scheduling 30,000 individual jobs, and exploit cheap no-op gates so sweeping a tiny unchanged org
   costs near zero.

3. Version cleanup is a first-class workload, not an afterthought. Continuous ingestion plus per-cycle system
   operations (index optimize, compaction) increments versions fast, and unmanaged `_versions` and
   `_transactions` directories reach terabytes (Krylasov). Decouple cleanup cadence from compaction cadence, tag
   versions needed for reproducibility, and keep the retention horizon longer than the longest-running job.

4. Incremental maintenance beats rebuild everywhere it exists. Netflix checkpoints feature computation, Cognee
   refines incrementally, WeRide batches re-indexing into scheduled windows, Exa patches single columns per
   fragment. For us this means `optimize_indices()` delta appends for the tail and scheduled bounded merges for
   the head, never unconditional full rebuilds.

5. Compaction at the 100 M scale is a minutes-long, low-impact operation when run regularly (Pure Storage), and
   an unreliable monster when deferred until thousands of files must merge into terabyte targets. Frequent
   bounded compaction on head orgs, skip-below-threshold for the tail.

6. Atomicity comes from conditional writes plus retry, not locks. Stateless compute over object storage with
   conditional-put commits is the common architecture (Pure Storage, Metagenomi). Conflicts are normal operation,
   so retry budgets and backoff are part of the design, not error handling.

7. The 1-4 tx/s per-dataset commit ceiling is architectural. High-frequency small writes must be coalesced
   client-side before commit. Per-org write batching in the ETL window is the correct response, and our
   one-merge_insert-per-routing-key-per-run design already conforms.

8. Storage-optimized NVMe matters for index builds and hot reads at the head. Metagenomi needed i4i instances for
   builds, Netflix runs an NVMe cache fleet for reads. Tail orgs need neither.

## Ordering caveat from the field

Public guidance (lancedb issue #2751, open as of Oct 2025) reports merge_insert failing with "fragment id does
not exist" when compaction runs before index optimize in the same cycle, and recommends optimizing indexes before
compacting. Our own checkout-verified analysis recommends the opposite DAG order (compact before index) to merge
fresh fragments before any index covers them, eliminating inline remap cost for new data (see
optimization-recommendations.md). The two are reconcilable: #2751 is filed against the lancedb OSS client and
its `use_index` merge path, while our pipeline pins a lance build and routes commits through retry wrappers.
Before shipping the reorder, verify #2751 status against the pinned build and confirm merge_insert behaves after
a compact-then-index cycle in the e2e suite.
