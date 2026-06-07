# Production techniques catalog

Round-2 techniques survey across three areas: object store and multi-writer behavior, dataset lifecycle (tags,
versioning, schema evolution, blobs, stable row ids), and ecosystem (catalogs, query engines, streaming, GPU).

Each technique states what it is, evidence (guide URL plus checkout `path:line` where one exists), a maturity
verdict, and a concrete "apply to lance-etl?" recommendation: yes-now, later, or no (with reason). Findings
that could not be verified are not silently dropped, they are listed in the closing appendix.

Checkout root for all `path:line` citations: `/Users/gstamatakis/IdeaProjects/lance`.

Cross-references: round-1 concurrency-and-coexistence.md (conflict matrix, retry layers),
optimization-recommendations.md (compaction/index verdicts), production-patterns.md (case studies and
performance-guide bounds), trace-events-and-distributed-writes.md (commit and observability internals).

---

## A. Object store and multi-writer

### A1. Conditional-put is the default S3 commit strategy; DynamoDB is opt-in

- What: Lance commits via two atomic primitives, rename-if-not-exists and put-if-not-exists (conditional PUT).
  Since AWS added If-None-Match on PutObject (2024-08-20), plain `s3://` routes to a ConditionalPutCommitHandler
  by default. The DynamoDB external manifest store (`s3+ddb://bucket/path?ddbTableName=<name>`) is opt-in, for
  S3-compatible stores lacking conditional put.
- Evidence: https://lance.org/format/table/transaction/ ;
  `rust/lance-table/src/io/commit.rs:1050-1075` (commit_handler_from_url routes s3/gs/az to
  ConditionalPutCommitHandler, struct at `:1456`); DynamoDB schema `rust/lance-table/src/io/commit/dynamodb.rs:100-101`
  (PK base_uri string, SK version number), validated `:188-209`; `DDB_URL_QUERY_KEY="ddbTableName"` at
  `commit.rs:771`, scheme handling `:1082-1101`. No user-facing storage_options key overrides the strategy;
  selection is internal via commit_handler_from_url.
- Maturity: production, default.
- Apply to lance-etl: yes-now (confirm posture, no code change). On modern S3 our three concurrent jobs need no
  DynamoDB. If any deployment target is an older S3-compatible store without conditional put, use `s3+ddb://`
  and a DynamoDB table with hash key base_uri + range key version, and remember that store needs its own DR
  replication (S3 CRR does not cover DynamoDB).

### A2. MVCC multi-writer topology, conflict outcomes, and retry budget

- What: optimistic concurrency, each commit appends an immutable manifest. Operations resolve as Rebaseable
  (auto-merged, e.g. Append-vs-Append, disjoint Delete-vs-Delete), Retryable (re-read and retry, e.g.
  Delete/Rewrite on overlapping fragments), or Incompatible. The distributed pattern: workers write_fragments,
  a coordinator commits with the right LanceOperation and read_version. ReserveFragments must precede a Rewrite.
- Evidence: https://lance.org/format/table/transaction/ (Append compatible with most ops including itself;
  disjoint deletes rebaseable; ReserveFragments-before-Rewrite); retry defaults
  `rust/lance/src/dataset/write/retry.rs:27` (`retry_timeout = 30s`), `:26` (`max_retries: 10`); merge_insert
  mirrors these at `python/python/lance/dataset.py:2515,2713`.
- Maturity: production.
- Apply to lance-etl: yes-now (already aligned). Our commit_with_retries wrappers must be sized against the
  30s/10-retry inner loop, not the stale "8 attempts" string from issue #3086. See round-1
  concurrency-and-coexistence.md for the per-operation matrix and the binding gap; nothing here changes those
  verdicts.

### A3. UUID fragment naming and V2 manifest paths for S3 at fleet scale

- What: fragment filenames encode a 16-byte UUID (first 3 bytes to a 24-char binary string for max S3
  partition entropy, remaining 13 bytes to 26-char hex = 50-char name), so fragment data files self-distribute
  across S3 partitions with no manual prefix sharding. V2 manifest naming `{u64::MAX - version:020}.manifest`
  gives O(1) latest-manifest lookup via one lexicographic LIST instead of listing all manifests. Optional
  `_versions/latest_version_hint.json` reduces that to a HEAD and is always safe to delete.
- Evidence: https://lance.org/format/table/layout/ (filename encoding, hint file safe-to-delete);
  https://lance.org/format/table/transaction/ (V2 manifest name). OSS write param is `enable_v2_manifest_paths`
  (`python/python/lance/dataset.py:4225`; `rust/lance/src/dataset.rs:1216`), and online migration via
  `migrate_manifest_paths_v2` (`python/python/lance/dataset.py:4592-4604`, idempotent, must not run
  concurrently with other ops).
- Maturity: production. The lancedb<0.10.0 reader-incompatibility and the #2790/PR#2798 association are
  external claims, see appendix.
- Apply to lance-etl: yes-now. For 30,000 datasets sharing a bucket, enable `enable_v2_manifest_paths` on all
  new datasets and migrate existing ones, so each dataset open is one LIST (or a HEAD with the hint file)
  instead of O(version_count) LIST requests. The UUID scheme means no application-level prefix engineering is
  needed for fragment data.

### A4. storage_options tuning: timeouts, retries, IO threads, AIMD limiter

- What: object-store config via storage_options or env vars. Defaults: download_retry_count 3,
  client_max_retries 3, client_retry_timeout 180s, connect_timeout 5s, request_timeout 30s. Concurrency:
  LANCE_IO_THREADS default 64 cloud / 8 local (guide recommends 128-256 cloud to saturate bandwidth, raise
  io_buffer_size ~32 MB per IO thread). Cloud stores are auto-wrapped with an AIMD rate limiter:
  initial 2000, max 5000, min 1 req/s, decrease factor 0.5, additive increment 300 req/s, burst 100; halve on
  429/503, climb on sustained success. s3_express is accepted as a storage_option key.
- Evidence: https://lance.org/guide/object_store/ , https://lance.org/guide/performance/ ;
  `rust/lance-io/src/object_store.rs:81,988-993` (download_retry_count), `:997-1003` (client_max_retries),
  `:1006-1012` (client_retry_timeout), `:61` (DEFAULT_CLOUD_IO_PARALLELISM=64), `:59` (local 8), `:582-584`
  (env override); AIMD defaults `rust/lance-core/src/utils/aimd.rs:44-56`, burst
  `rust/lance-io/src/object_store/throttle.rs:120`, env names `:186-194`; s3_express
  `rust/lance-io/src/object_store/providers/aws.rs:190`.
- Maturity: production.
- Apply to lance-etl: yes-now for the search service, later for ETL. The Rust gRPC service's prewarm and
  disk_cache do heavy parallel S3 I/O, so raise LANCE_IO_THREADS toward 128-256 with proportionally larger
  io_buffer_size and benchmark. The AWS 3.5B-vector blog's `timeout: 120s` is worth adopting for large-row
  reads. For a 30K-dataset fleet running ingest plus compaction plus indexing simultaneously, the AIMD
  max of 5000 req/s per process may need tuning, validate under Datadog before raising.

### A5. GCS and Azure native atomicity; no storage-class API

- What: GCS (`gs://`) and Azure Blob (`az://`) both natively support atomic commits, so no DynamoDB-equivalent
  coordinator is ever needed. Lance has no storage-class API, tiering is a bucket/lifecycle concern.
- Evidence: `rust/lance-table/src/io/commit.rs:1050-1075` (gs/az route to ConditionalPutCommitHandler);
  https://lance.org/guide/object_store/ (gs:// and az:// schemes, storage_class listed under omissions); zero
  `storage_class` matches in `rust/lance-io/src`.
- Maturity: production.
- Apply to lance-etl: yes-now (informational). If we deploy on GCS or Azure, drop any DynamoDB setup entirely.
  Tiering for the cold tail is done via bucket lifecycle rules, never via lance.

### A6. Storage-class cost engineering for the cold tail

- What: with no lance storage-class API, tiering is bucket-level. Lance reads `_versions/`, `_indices/`, and
  `_refs/` on every dataset open, so those prefixes must stay in an immediately-retrievable tier. Fragment
  `data/` files can be tiered independently. Intelligent-Tiering on `data/` plus Standard on metadata prefixes
  is the pragmatic baseline for a power-law fleet where most datasets are cold.
- Evidence: `_indices/` = INDICES_DIR `rust/lance/src/dataset.rs:145`; `_refs/tags` and `_refs/branches`
  read during open/cleanup `rust/lance/src/dataset/refs.rs:894,898`, `cleanup.rs:157`; no storage_class key in
  source (A5). S3-pricing specifics (Intelligent-Tiering retrieval fees, Express One Zone same-AZ EC2) are AWS
  docs, see appendix.
- Maturity: technique is an operations pattern, not a lance feature.
- Apply to lance-etl: later. With 30K power-law datasets, apply lifecycle rules archiving `data/` of dormant
  orgs to Glacier Instant Retrieval (millisecond access), and NEVER move `_versions/`, `_indices/`, `_refs/`,
  or `_transactions/` to a delayed-restore tier (Glacier Flexible/Deep), which would break dataset open. Gate
  on measured cold-org access patterns before rollout.

### A7. Multi-region is read-scale only

- What: Lance has no native multi-region write distribution. All concurrent writes must target one endpoint,
  whose atomic put-if-not-exists/rename-if-not-exists primitive enforces consistency. Multi-region is
  active-passive: primary writer region, async-replicated read replicas (e.g. S3 CRR), with replica-region
  readers opening the copied directory read-only.
- Evidence: https://lance.org/format/table/transaction/ (no multi-region guidance; single-endpoint atomic
  primitive; copying the dataset directory preserves all data). S3 CRR asynchrony and DynamoDB Global Tables
  lag are AWS facts, see appendix.
- Maturity: lance conclusion is sound; the surrounding cloud mechanics are external.
- Apply to lance-etl: later / partial. Use one authoritative write region per dataset and CRR for DR only. If
  per-org regional residency is ever required, shard datasets per org region at the application layer (active-
  active across endpoints is unsupported). Tag versions before any cross-region failover.

---

## B. Lifecycle: tags, versioning, schema evolution, blobs, stable row ids

### B1. Tags and version pinning for blue/green serving cutovers

- What: git-style tags via `dataset.tags`. `create(name, reference)` accepts an int version, a tag name, or a
  `(branch, version)` tuple. `update(name, reference)` atomically repoints (it is a commit). `delete(name)`
  removes only the label, not the data. Open at a tag with `lance.dataset(uri, version="tag")`, read-only,
  creating no version. Tagged versions are exempt from cleanup.
- Evidence: https://lance.org/guide/tags_and_branches/ ; `python/python/lance/dataset.py:6857-6876` (create),
  `:6890-6908` (update), `:6878-6888` (delete), `:2951-2957` (cleanup exemption); open-at-tag
  `python/python/lance/__init__.py:111-113`; tag paths `_refs/tags` `rust/lance/src/dataset/refs.rs:894`.
- Maturity: production.
- Apply to lance-etl: yes-now. Maintain a `prod` tag per dataset, validate a freshly ingested-plus-indexed
  version, then `tags.update("prod", new_version)` as an O(1) pointer swap. The gRPC search service opens
  `version="prod"`, giving atomic blue/green cutover across the fleet and protecting the serving version from
  cleanup.

### B2. Time travel as audit and rollback

- What: every write makes a monotonic version. `versions()` lists them with timestamps, `checkout_version(N)`
  views a snapshot (no new version), `restore()` (no args, on a checked-out version) commits a new version
  resetting data to that snapshot. Transaction files `_transactions/{read_version}-{uuid}.txn` record commit
  provenance.
- Evidence: `python/python/lance/dataset.py:2756-2769` (versions), `:2850-2875` (checkout_version), `:2877-2883`
  (restore creates a commit); txn naming `rust/lance/src/io/commit.rs:127`.
- Maturity: production.
- Apply to lance-etl: yes-now. After a bad ingest, roll back via `checkout_version(N).restore()` with no data
  copy. Tag known-good versions before destructive operations. Note: the API is `checkout_version(N)` then
  `restore()`, there is no `dataset.checkout(version)` and `restore` takes no argument.

### B3. Schema evolution: add/alter/drop columns and UDF backfill

- What: fragment-local schema changes. `add_columns` supports schema-only (instant), SQL expression, or
  `@lance.batch_udf(checkpoint_file=...)` (resumable from the last batch on failure). `alter_columns` renames
  (metadata-only) or casts a column (rewrites only that column, drops its index). `drop_columns` is
  metadata-only until compaction plus cleanup reclaim disk. Schema changes conflict with most concurrent
  writes.
- Evidence: https://lance.org/guide/data_evolution/ ; batch_udf `python/python/lance/udf.py:62-92`;
  drop_columns `python/python/lance/dataset.py:2479-2506`; alter_columns `:2254-2312`.
- Maturity: production.
- Apply to lance-etl: later. For embedding-dimension or feature-column rollouts across the fleet, use
  `batch_udf` with a checkpoint file for fault-tolerant backfill, and quiesce other writers per dataset during
  the schema op (it conflicts with merge_insert/compaction). Sequence as ingest, then backfill, then compact,
  consistent with round-1 production-patterns.md. The FixedSizeList-width-change-needs-3-steps claim is
  unverified, see appendix.

### B4. Blob v2 and take-based dataloading

- What: BlobType is a PyArrow extension (`lance.blob.v2`, struct of data/uri/position/size), requiring
  data_storage_version 2.2. `take_blobs(col, ids|addresses|indices)` (exactly one selector) returns seekable
  BlobFile objects with `read_range(offset, length)` that does not move the cursor, usable directly by PyAV or
  PIL. `read_blobs` is a separate throughput-oriented planned read returning `(row_address, bytes)` pairs.
- Evidence: https://lance.org/guide/data_types/ ; `python/python/lance/blob.py:64-81` (extension + struct),
  `:287-289` (read_range), `:143-159` (str=URI, bytes=inline); take_blobs
  `python/python/lance/dataset.py:2094-2139`; read_blobs `:2141-2194`.
- Maturity: production (requires data_storage_version 2.2).
- Apply to lance-etl: later. For head-org multimodal training over 1B rows, the shuffle-then-fetch pattern
  (`take` for scalar plus embedding columns, then `take_blobs(indices=same)` for raw media) gives lazy
  seekable access without materializing all blobs. Gate on adopting data_storage_version 2.2 fleet-wide.

### B5. Stable row ids and the Fragment Reuse Index

- What: `enable_stable_row_ids=True` at write time assigns 64-bit ids that survive compaction (NOT updates,
  which tombstone and reassign). The Fragment Reuse Index (`defer_index_remap=True` in compaction) records
  old-to-new address mappings instead of rebuilding indexes, decoupling compaction from index rebuild and
  removing the Rewrite-vs-CreateIndex conflict for the common case. With stable row ids, compaction needs no
  index remapping at all.
- Evidence: write param `python/python/lance/dataset.py:4230,7035,4295-4299`; defer_index_remap
  `rust/lance/src/dataset/optimize.rs:197` (default false `:240`), gating
  `needs_remapping = uses_stable_row_ids() && !defer_index_remap` `:1893`. KNOWN GAP: the distributed
  `Compaction.commit` binding discards plan-time defer_index_remap, hard-coding `CompactionOptions::default()`
  with a live TODO at `python/src/dataset/optimize.rs:567` (range `:559-575`).
- Maturity: stable row ids production; FRI present but defeated on the distributed commit path by the binding
  gap.
- Apply to lance-etl: later (structural endgame, already flagged in round-1 README takeaway #10). New datasets
  should enable `enable_stable_row_ids` (and `enable_v2_manifest_paths`, independent params) so future compaction
  needs no remap. Migrating 30K existing datasets and a verification pass on the pinned build are the blockers.
  Until the binding gap is fixed upstream, serialize compact and index per dataset on the large tier.

### B6. Format and manifest migration across versions

- What: data_storage_version 2.1 became default in lance 5.0.0 (non-leaf fields get column_indices=-1); 2.2 is
  required for blob v2 and rejects legacy `lance-encoding:blob` metadata on write. `migrate_manifest_paths_v2`
  is idempotent but must run to completion with no concurrent ops. IndexSegmentBuilder was removed in 7.2.0
  (replaced by commit/merge_existing_index_segments).
- Evidence: https://lance.org/guide/migration/ , https://lance.org/guide/data_types/ ;
  `python/python/lance/dataset.py:4592-4604` (migrate, idempotent + no-concurrency);
  `commit_existing_index_segments`/`merge_existing_index_segments` present at `:4175`.
- Maturity: production.
- Apply to lance-etl: later. Plan a coordinated fleet migration to data_storage_version 2.1/2.2 and to V2
  manifest paths, scheduled per dataset in a window with no concurrent writers. Our indexing already uses the
  current segment API, so the IndexSegmentBuilder removal does not affect us. The "full-copy required to change
  data_storage_version" claim is unverified, see appendix.

### B7. Retention and cleanup under concurrent readers

- What: `cleanup_old_versions(older_than, retain_versions, delete_unverified=False,
  error_if_tagged_old_versions=True, delete_rate_limit)`. Default older_than is 14 days (NOT 7).
  delete_unverified keeps unreferenced-but-recent files unless 7+ days old. Never set older_than=0 with
  concurrent writers (deletes the base manifest mid-commit). delete_rate_limit throttles S3 deletes to dodge
  503 SlowDown.
- Evidence: `python/python/lance/dataset.py:2934-2936` (default 14 days), `:2917` (error_if_tagged), `:2918`
  (delete_rate_limit); tagged-version skip `rust/lance/src/dataset/cleanup.rs:157-177,229-233`; the separate
  7-day unverified threshold `cleanup.rs:131`.
- Maturity: production.
- Apply to lance-etl: yes-now (validate config). Keep the cleanup horizon longer than the longest head-org job
  (10 minutes is the absolute floor), never use older_than=0 or delete_unverified=True with concurrent writers,
  set delete_rate_limit on large fleets, and tag versions needed for reproducibility. This refines round-1
  takeaway #8: the low-level default is 14 days, not the 7 days some docs cite.

### B8. Shallow clone and multi-base layout for dataset forking

- What: the manifest's base_paths array plus per-DataFile base_id lets a clone reference source data files by
  absolute path without copying, with only new writes going to the clone. Underpins branching and shallow
  clone. `create_branch(name)` forks at the current head with independent version history.
- Evidence: https://lance.org/format/table/ ; base_id `rust/lance-table/src/format/fragment.rs:56`;
  create_branch `python/python/lance/dataset.py:918-955`.
- Maturity: production.
- Apply to lance-etl: later. Shallow clone enables per-org dataset forks (write-audit-publish, experiment
  branches) without data duplication, and multi-base is the mechanism behind our distributed index-commit
  pattern. Adopt for staged validation workflows once tag-based blue/green (B1) is in place.

---

## C. Ecosystem: catalogs, engines, streaming, GPU

### C1. Lance-Spark connector **[partial]**

- What: official DSv2 connector (`org.lance.spark.LanceNamespaceSparkCatalog`). Full DDL/DML including CREATE
  INDEX (BTREE and FTS distributed), OPTIMIZE, VACUUM, MERGE INTO. Cache env vars: LANCE_INDEX_CACHE_SIZE 6GB,
  LANCE_METADATA_CACHE_SIZE 1GB, LANCE_ALLOCATOR_SIZE Long.MAX_VALUE. Maven group is `org.lance`. Spark 4.0/4.1
  are Scala 2.13 only; 3.4/3.5 are 2.12 and 2.13.
- Evidence: https://lance.org/integrations/spark/config/ , https://lance.org/integrations/spark/install/
- Maturity: partial. Catalog class, Scala matrices, cache defaults, and Maven group verified. Version labels
  (v0.4.0 "stable" vs beta), MERGE-INTO version attribution, and the SPJ/zonemap/LIMIT-pushdown claims are
  unverified (Maven shows only beta artifacts), see appendix.
- Apply to lance-etl: no for index dispatch (reason: our distributed indexing already uses the Python segment
  API per AGENTS.md rule #6, and the Spark connector's CREATE INDEX is a different dispatch path we do not need
  to adopt). Later as an optional ingest path. The cache env vars do apply to any Spark executor JVMs we run.

### C2. Lance-Ray distributed indexing and dataloading **[partial]**

- What: `lance-ray` (prerelease index https://pypi.fury.io/lance-format/) exposes read_lance/write_lance,
  create_scalar_index/create_index/optimize_indices, add_columns (UDF with `ray_remote_args={'num_gpus': N}`),
  vector_search. Distributed scalar building supports only INVERTED/FTS and BTREE; num_workers auto-caps at
  fragment count; too many workers makes too many index partitions and degrades query latency.
- Evidence: https://lance.org/integrations/ray/distributed-indexing/ , https://lance.org/integrations/ray/
- Maturity: partial. Supported index types, num_workers cap, and the prerelease index URL verified. The
  internal call names `create_fragment_index()` and `merge_inverted_index_metadata()` do NOT exist in this
  checkout (which uses `create_index_uncommitted` `python/python/lance/dataset.py:3957`,
  `merge_existing_index_segments` `:4175`, `merge_index_metadata` `:4124`), and lance-ray is an external
  package, so its dispatch path is unconfirmed, see appendix.
- Apply to lance-etl: no for now (reason: AGENTS.md rule #6 fixes our index path on the verified segment API;
  adopting lance-ray would mean depending on an unverified internal dispatch that may diverge from our rules).
  Revisit only if we move compute from Spark to Ray and can verify the segment flow matches.

### C3. DuckDB Lance extension **[partial]**

- What: DuckDB core extension (since 1.5.2). `lance_vector_search(path, column, query_vector, k, prefilter)`,
  `lance_fts(path, column, query_text, k, prefilter)`, `lance_hybrid_search(path, vec_col, query_vector,
  text_col, query_text, k, prefilter, alpha, oversample_factor)`. `CREATE INDEX ... USING IVF_FLAT`. Benchmarks
  (69K rows, M1 Max): vector indexed cold 12ms vs Parquet 761ms; FTS cold 21ms vs Parquet 12ms (Lance slower
  cold for raw FTS).
- Evidence: https://duckdb.org/docs/current/core_extensions/lance , https://duckdb.org/2026/05/21/test-driving-lance
- Maturity: partial. Function signatures, IVF_FLAT DDL, and benchmark numbers verified. The
  core-since-1.5.2 framing, prefilter hard-fail vs best-effort semantics, and Arrow-extension-type query
  failures are unverified, see appendix.
- Apply to lance-etl: later (ops tooling only). Useful for ad-hoc SQL debugging and lightweight reporting on
  our datasets without a Spark cluster. Not a serving path, our gRPC service stays the production reader.

### C4. Lance Namespace catalog **[partial]**

- What: open spec for managing collections of Lance tables. Directory Catalog (in-process, filesystem/S3/GCS/
  Azure hierarchy) and REST Catalog (client-server OpenAPI). External adapters (separate impls repo) cover
  Hive, Unity, Glue, Polaris (Generic Table API, format='lance'), Gravitino, Iceberg REST, OneLake. REST spec
  is currently single-level (multi-level is open issue #58).
- Evidence: https://pypi.org/project/lance-namespace/ (Python 0.8.2), https://lance.org/format/namespace/ ;
  local checkout `rust/lance-namespace-impls/src` has only dir.rs and rest.rs (external adapters live in the
  separate Java/Python repo).
- Maturity: partial. Python 0.8.2, single-level REST limitation, and Polaris-via-Generic-Table verified.
  impls v0.3.0 versioning, BigLake adapter, and Hive pool-size default are unverified, see appendix.
- Apply to lance-etl: later. For our 30K-org fleet, the Directory Catalog is the simplest deployment. A REST
  catalog plus Glue/Unity adapter becomes worthwhile if we need fleet-wide access control and governance. The
  single-level REST limitation matters for deep namespace isolation, track issue #58.

### C5. Streaming ingestion and Change Data Feed

- What: no first-party Kafka connector or Flink sink as of mid-2026 (Flink CDC gap is open issue #3961). The
  recommended streaming pattern is micro-batch: consume from Kafka, batch into Arrow, write_fragments, commit
  with `LanceOperation.Append(fragments)` and read_version under optimistic concurrency. Upserts use
  merge_insert. CDF is available in pylance via `Dataset.delta()` returning a DatasetDelta with
  list_transactions/get_inserted_rows/get_updated_rows.
- Evidence: https://github.com/lance-format/lance/issues/3961 ; TRANSACTIONS_DIR `rust/lance/src/dataset.rs:147`,
  txn format `rust/lance/src/io/commit.rs:127`; Append `python/python/lance/dataset.py:5472,5501`; CDF
  `python/src/dataset.rs:3134` (delta builder), `:3488`; row-version columns
  `rust/lance-datafusion/src/projection.rs:155-156`, `rust/lance/src/dataset/delta.rs:304,362`.
- Maturity: micro-batch and CDF are production; no maintained streaming connector.
- Apply to lance-etl: yes-now for CDF (incremental orchestration), no for a Flink connector (reason: none
  exists, any Kafka-to-Lance path would be custom fragment writes or Spark). Use `Dataset.delta()` to drive
  incremental Airflow/Dagster materialization, consuming only changed rows since the last run instead of full
  scans. The optimistic-concurrency commit model is already what our three jobs rely on.

### C6. GPU-accelerated index building **[partial]**

- What: OSS GPU support is IVF_PQ only, via the LanceDB Python SDK `accelerator='cuda'|'mps'` param (GPU
  accelerates KMeans/IVF training; PQ codebook and assignment are CPU). OSS GPU indexing is sync-SDK only.
  Benchmarks: NVIDIA L4 ~26x, Apple M2 Max ~19x vs CPU. CAGRA (cuVS) is a feature request (#6534), no shipped
  implementation.
- Evidence: https://docs.lancedb.com/indexing/gpu-indexing , https://github.com/lance-format/lance/issues/6534 ;
  accelerator param in pylance create_index `python/python/lance/dataset.py:3325-3438` (gated to IVF_PQ at
  `:3427`).
- Maturity: partial. accelerator name/values and sync-SDK-only verified. PyTorch>2.0 hard-vs-soft dependency,
  the PQ-not-GPU breakdown, and CAGRA merge status are unverified, see appendix.
- Apply to lance-etl: no (reason: AGENTS.md rule #6 fixes our vector path on IVF_RQ via the segment API, while
  the OSS GPU accelerator is gated to IVF_PQ in the LanceDB SDK). Revisit only if a future release exposes GPU
  acceleration for IVF_RQ create_index_uncommitted.

### C7. DataFusion integration

- What: the `lance` crate's datafusion module exposes LanceTableProvider for SQL over a dataset with automatic
  column and filter pushdown; UDFs like `contains_tokens` require explicit `register_functions(&ctx)`. Python
  FFI exposes `FFILanceTableProvider` from `lance`.
- Evidence: https://lance.org/integrations/datafusion/ ; `rust/lance/src/datafusion/dataframe.rs:39,48`
  (LanceTableProvider::new(Arc<Dataset>, with_row_id, with_row_addr)); UDF `rust/lance-datafusion/src/udf.rs:16,98,138`;
  FFI `python/src/lib.rs:385-430`, export `python/python/lance/__init__.py:31`; workspace version
  `Cargo.toml:34` = 8.0.0-beta.6.
- Maturity: production (this is what our gRPC service uses internally for filter_to_expr).
- Apply to lance-etl: yes-now (confirms current approach). Our search service already converts the Filter AST
  to DataFusion Expr via this path. Corrections vs the finding: the type lives in the `lance` crate (not
  lance-datafusion), the constructor takes a concrete `Arc<Dataset>` (not `Arc<dyn Dataset>`), and the version
  is 8.0.0-beta.6 (not 2.0.1). The domain/filter.rs column-name allowlist is OUR validation, not provided by
  lance-datafusion, so rule #7 remains our responsibility.

### C8. Trino Lance connector **[partial]**

- What: Java connector with DDL (CREATE/DROP/SHOW SCHEMA/TABLE, DESCRIBE, REPLACE) and DML (INSERT, UPDATE,
  DELETE, MERGE), reading datasets and individual fragments, schema auto-discovery via lance-namespace impls.
- Evidence: https://lance.org/integrations/trino/ , https://github.com/lance-format/lance-trino
- Maturity: partial. DML and DDL sets verified. v0.3.0 release date, predicate pushdown, Maven group, and
  minimum Trino version are unverified, see appendix.
- Apply to lance-etl: no for the pipeline (reason: not a component we need). Possible future ad-hoc analytics
  path, lower priority than DuckDB (C3) for the same role.

### C9. Orchestration patterns (Airflow/Dagster)

- What: no first-party dagster-lance or airflow-provider-lance package exists. The pattern is standard
  PythonOperator/@asset wrapping pylance or Ray jobs, with the orchestrator scheduling heavy compute that runs
  in Spark/Ray, not row-level Lance work in operators. Dagster incremental assets can consume CDF.
- Evidence: PyPI search (no such packages); https://www.lancedb.com/blog/lance-namespace-lancedb-and-ray ;
  CDF via `python/src/dataset.rs:3134`.
- Maturity: production pattern, no library.
- Apply to lance-etl: yes-now (confirms current design). Our `airflow/lance_etl_dag.py` already schedules
  Spark jobs (etl -> index -> compact) rather than doing row-level work in operators, matching AGENTS.md rule
  #5. CDF (C5) is the upgrade path for incremental materialization.

---

## Appendix: claims we could not verify

Listed so they are not lost. Each needs confirmation against a primary source or the pinned build before being
relied upon.

- A1: issue #2793 / PR #3483 merged into a specific released version; a storage_options key to override the
  commit strategy (none found in source); s3+ddb cleanup-TTL >=10 minutes guidance.
- A3: lancedb<0.10.0 cannot read V2 manifest paths; issue #2790 / PR #2798 association.
- A4: io_buffer_size and batch-size exact figures (from the performance guide, not re-verified line-by-line);
  the separate LANCE_AIMD_MAX_BACKOFF_MS default of 300ms is distinct from the 300 req/s additive increment.
- A6: S3 Intelligent-Tiering having no retrieval fee for >=128KB objects; Express One Zone requiring same-AZ
  EC2 (AWS docs).
- A7: S3 CRR replicates all object types; S3 CRR is asynchronous; DynamoDB Global Tables ~1s lag; DynamoDB not
  covered by S3 CRR (AWS docs).
- B3: changing FixedSizeList width via alter_columns being unsupported and needing a 3-step add+drop+rename
  (only in docs.lancedb.com, not the official guide or source).
- B5: updates tombstone-and-reassign row ids; updates ~600x slower (issue #6404 / PR #6465); RowIdIndex inline
  for <100KB (GitHub discussion #3694, not located in checkout).
- B6: upgrading data_storage_version requires a full dataset copy (lancedb blog only); enable_v2_manifest_paths
  making datasets unreadable by lancedb<0.10.0 (docs.lancedb.com only).
- C1: lance-spark v0.4.0 "stable" vs beta; MERGE-INTO version attribution; SPJ / zonemap fragment pruning /
  LIMIT pushdown / broadcast-hash-join claims.
- C2: lance-ray internal calls create_fragment_index() and merge_inverted_index_metadata() (names absent from
  this checkout; external package).
- C3: lance extension core-since-1.5.2 framing; prefilter=true hard-fail vs prefilter=false best-effort; Arrow
  extension-type query failures.
- C4: lance-namespace-impls v0.3.0 (Apr 2026) as current; Google BigLake adapter; Hive client.pool-size
  default 3.
- C5: CDF row-version columns stored specifically run-length-encoded in the fragment manifest (columns exist,
  RLE-in-manifest location not pinned).
- C6: PyTorch>2.0 hard vs soft dependency; PQ codebook/assignment not GPU-accelerated; CAGRA #6534 merge
  status.
- C8: lance-trino v0.3.0 current release; predicate pushdown; Maven group id; minimum Trino version.
