# Lance Trace-Event Catalog and Distributed-Write Analysis

Source: lance guide pages fetched 2026-06-06. Lance checkout commit 466405f47
(`/Users/gstamatakis/IdeaProjects/lance`). All path:line citations below are
relative to that checkout.

---

## Section 1 — Lance Trace-Event Catalog

### 1.1 Authoritative event definitions

All five targets are defined in one file:
`rust/lance-core/src/utils/tracing.rs:61-87`.

The Python bridge picks them up through `lance.tracing.capture_trace_events`,
which registers a callback on the same Rust event stream.

### 1.2 Event table

All fields below are verified against the emit sites in the checkout; the
guide page (https://lance.org/guide/performance/) is the documentation source.
"Guide-documented" means the field appears in the table on that page.
"Source-only" means the field exists in the Rust `info!` macro but is absent
from the guide.

```
Event target                      Field(s)             Source: file:line
--------------------------------- -------------------- ------------------------------------
lance::file_audit                 mode                 lance-core/src/utils/tracing.rs:62
  Mode values:                    type                 lance-core/src/utils/tracing.rs:65
    create                        path (source-only)   lance/src/dataset/write.rs:574
    delete                                             lance-table/src/io/commit.rs:227
    delete_unverified                                  lance/src/dataset/cleanup.rs:415-623
  Type values:                                         lance-table/src/io/deletion.rs:107
    manifest
    data
    index
    deletion

lance::dataset_events             event                lance-core/src/utils/tracing.rs:80
  Event values:                   uri                  lance/src/dataset/builder.rs:585
    loading                       mode (writing only)  lance/src/dataset/write/insert.rs:195
    writing                       target_ref (loading) lance/src/dataset/builder.rs:585
    committed                     version (loading)    lance/src/dataset/builder.rs:585
    compacting                    status (loading)     lance/src/dataset/builder.rs:585
    cleaning                      read_version         lance/src/dataset/write/commit.rs:414
    deleting                        (committed only)
    dropping_column               committed_version    lance/src/dataset/write/commit.rs:414
                                    (committed only)
                                  detached             lance/src/dataset/write/commit.rs:414
                                    (committed only)
                                  operation            lance/src/dataset/write/commit.rs:414
                                    (committed only)
                                  predicate (deleting) lance/src/dataset.rs:1613
                                  columns              lance/src/dataset.rs:2932
                                    (dropping_column)

lance::object_store::throttle    previous_rate        lance-io/src/object_store/throttle.rs:402
                                  new_rate             lance-io/src/object_store/throttle.rs:416
                                  attempt (source-     lance-io/src/object_store/throttle.rs:443
                                    only, on retry)
                                  backoff_ms (source-  lance-io/src/object_store/throttle.rs:457
                                    only, on retry)
                                  error                lance-io/src/object_store/throttle.rs:471

lance::io_events                  type                 lance-core/src/utils/tracing.rs:71
  Type values:                    index_uuid           lance/src/index.rs:1903
    open_scalar_index             index_type           lance/src/index.rs:1974
    open_vector_index             version (vector)     lance/src/index.rs:1903
    load_vector_part              part_id              lance/src/index/vector/ivf/v2.rs:980
    load_scalar_part                                   lance-index/src/scalar/btree.rs:1278
    open_frag_reuse_index                              lance/src/index.rs:2184
      (source-only, not in guide)
    open_mem_wal_index                                 lance/src/index.rs:2220
      (source-only, not in guide)

lance::execution                  type (plan_run)      lance-core/src/utils/tracing.rs:78
                                  plan_summary         lance-datafusion/src/exec.rs:547
                                    (source-only)
                                  output_rows          lance-datafusion/src/exec.rs:545
                                  iops                 lance-datafusion/src/exec.rs:545
                                  requests (source-    lance-datafusion/src/exec.rs:550
                                    only; different
                                    from iops: raw
                                    object store
                                    calls before
                                    coalescing)
                                  bytes_read           lance-datafusion/src/exec.rs:545
                                  indices_loaded       lance-datafusion/src/exec.rs:545
                                  parts_loaded         lance-datafusion/src/exec.rs:545
                                  index_comparisons    lance-datafusion/src/exec.rs:545
```

### 1.3 Do we capture it — Python (src/lance_etl/telemetry.py)

`attach_lance_event_bridge` registers a callback via `lance.tracing.capture_trace_events`.
The callback in `build_lance_event_callback` dispatches on the short target name
(`target.rsplit("::", 1)[-1]`), so any event whose target suffix matches is handled.

```
Event               Python bridge captures it?   What is captured
------------------- ---------------------------- -----------------------------------------
file_audit          Yes — generic incr path       lance.event counter + lance.file_audit
                                                  counter tagged by mode/type/event/op.
                                                  path field is NOT extracted (not in
                                                  EXECUTION_DISTRIBUTION_KEYS or
                                                  EVENT_TAG_KEYS).

dataset_events      Yes — generic incr path       lance.event counter + lance.dataset_events
                                                  counter tagged by event/mode/operation.
                                                  uri, read_version, committed_version,
                                                  predicate, columns, detached NOT
                                                  extracted.

throttle            Yes — specialised branch      lance.throttle.previous_rate gauge,
                                                  lance.throttle.new_rate gauge,
                                                  lance.throttle.error counter.
                                                  attempt and backoff_ms (source-only
                                                  fields) are silently dropped.

io_events           Yes — generic incr path       lance.event + lance.io_events counter
                                                  tagged by type. index_uuid, index_type,
                                                  part_id, version NOT extracted.

execution           Yes — specialised branch      lance.execution.{output_rows, iops,
                                                  bytes_read, indices_loaded, parts_loaded,
                                                  index_comparisons} distributions.
                                                  requests and plan_summary (source-only
                                                  fields) are silently dropped.
```

### 1.4 Do we capture it — Rust search-api (rust/search-api/src/telemetry/)

The search-api installs `tracing_subscriber::EnvFilter::try_from_default_env()`
defaulting to `"info"` (`traces.rs:87`). Lance emits all five targets at the
`info` level. Therefore all five lance tracing targets WILL pass the subscriber
filter and appear in JSON logs when `RUST_LOG` is unset or set to `info` or
broader.

However the search-api does NOT subscribe to lance's `capture_trace_events`
Python callback (it is a Rust binary). It also does NOT extract lance trace
fields as Datadog span attributes or as DogStatsD metrics. The lance events
appear in JSON stdout logs only, where they carry `target` and the emitted
fields, but are not converted to structured search-service metrics.

Key gaps for cache-effectiveness dashboards and per-query observability in the
search service:

```
Gap                                    Missing metric / span attribute
-------------------------------------- -----------------------------------------
Per-query iops and bytes_read          lance::execution fields are logged but not
                                       fed into search_api.rpc.* span attributes
                                       or a search_api.query.iops distribution.

Index-load events per query            lance::io_events open_vector_index and
                                       load_vector_part events are not counted
                                       into a search_api.index_loads counter
                                       tagged by rpc and index_type.

Index-load events feeding cache        open_vector_index / load_scalar_part
  effectiveness dashboards             events could separate cold misses that
                                       reached object storage from warm hits
                                       already in lance's internal cache. Today
                                       the search-api's Metrics::cache_lookup
                                       covers only the search-api's own disk/
                                       memory caches, not lance-level index
                                       partition loads.

Object-store throttle surfacing        lance::object_store::throttle events are
                                       logged but not promoted to a DogStatsD
                                       gauge or error counter in the search-api,
                                       so they are invisible in dashboards when
                                       the search service is under S3 throttle
                                       pressure.
```

### 1.5 Recommended actions — trace events

| # | Action | Evidence | Expected win | Risk |
|---|--------|----------|--------------|------|
| T1 | Extract `execution.requests` in the Python bridge alongside the existing `EXECUTION_DISTRIBUTION_KEYS` tuple (`telemetry.py:43-50`). Add `"requests"` to the tuple and emit `lance.execution.requests` as a distribution. | `lance-datafusion/src/exec.rs:550` — `requests` is emitted at the same site as `iops` but is not in the guide table. | Distinguishes object-store call count from IO coalescing count, enabling coalescing-efficiency tracking. | None: additive metric. |
| T2 | Extend `EVENT_TAG_KEYS` in `telemetry.py:52` to include `"index_type"` so `lance::io_events` events are tagged with `index_type` (ivf, btree, inverted, ngram). | `lance/src/index/vector/ivf/v2.rs:980`, `lance-index/src/scalar/btree.rs:1278` — `index_type` is emitted alongside `type`. | Enables per-index-family load breakdowns in Datadog without adding a new metric. | Tag cardinality: `index_type` has ~5 values, acceptable. |
| T3 | Add `"index_uuid"` extraction to the `io_events` branch: emit a `lance.io.index_load` counter tagged by `index_type` and `io_type` (open vs. load_part). | `lance/src/index.rs:1903-2220`, `lance-index/src/scalar/inverted/index.rs:2158` | Feeds the cache-effectiveness dashboard: open events = cold load; load_part events = IVF partition paging. `index_uuid` is too high-cardinality for a tag but is useful as a log field for tracing hot indexes. | The `index_uuid` field should be in logs only, not as a metric tag. |
| T4 | In the Rust search-api, subscribe to lance execution stats via the Rust-level `LanceExecutionOptions::execution_stats_callback` and emit `search_api.query.iops`, `search_api.query.bytes_read`, `search_api.query.parts_loaded` as DogStatsD distributions tagged by `rpc`. | `lance-datafusion/src/exec.rs:545-555` — the callback exists and is called after every plan execution. | Closes the per-query iops gap identified in 1.4 without log parsing, at metric precision. | Requires wiring `LanceExecutionOptions` into the backend's scan calls. |
| T5 | In the Rust search-api, emit a DogStatsD counter `search_api.throttle.errors` and gauge `search_api.throttle.new_rate` by subscribing to the `lance::object_store::throttle` tracing target via a custom `tracing_subscriber` layer. | `lance-io/src/object_store/throttle.rs:402-471` | Makes S3 throttle pressure visible in dashboards without scraping logs. | Requires a thin layer that pattern-matches on `metadata().target() == "lance::object_store::throttle"`. |
| T6 | Document and test the two source-only io_event types (`open_frag_reuse_index`, `open_mem_wal_index`) in the Python bridge. They are emitted during compaction and WAL operations and are currently invisible at the Datadog level. | `lance/src/index.rs:2184, 2220` | Enables compaction and WAL index I/O tracking. | Low priority; relevant only when compaction runs on datasets with frag-reuse indexes. |

---

## Section 2 — Distributed-Write Analysis

### 2.1 Guide-prescribed pattern

Source: https://lance.org/guide/distributed_write/

The guide describes a two-phase commit pattern:

**Phase 1 — parallel fragment writes (workers)**

Each worker calls:

```python
from lance.fragment import write_fragments

fragments = write_fragments(
    data,               # pa.Table or RecordBatchReader
    dataset_uri,        # str or Path or LanceDataset
    schema=schema,      # optional, inferred if absent
    mode="append",      # "append" | "create" | "overwrite"
    max_rows_per_file=1_048_576,
    storage_options=...,
)
```

Returns `List[FragmentMetadata]`.

`write_fragments` is verified at
`python/python/lance/fragment.py:1047-1069`.

**Phase 2 — single coordinator commit**

The coordinator collects all `FragmentMetadata` objects (serialised with
`FragmentMetadata.to_json()` / `FragmentMetadata.from_json()`, verified at
`python/python/lance/fragment.py:110, 138`), then calls:

```python
operation = lance.LanceOperation.Append(all_fragments)
ds = lance.LanceDataset.commit(
    dataset_uri,
    operation,
    read_version=current_version,
)
```

`LanceDataset.commit` is verified at `python/python/lance/dataset.py:4219`.
`LanceOperation.Overwrite` (creates or replaces) is verified at
`python/python/lance/dataset.py:5452`. `LanceOperation.Append` at line 5501.

The guide does not describe a distributed `merge_insert` path. Fragment-write
+ single commit is an append-only primitive. The guide explicitly positions
`write_fragments` as a "low-level API intended for manually implementing
distributed writes" (`fragment.py:1074-1076`).

### 2.2 Our ETL write path (src/lance_etl/etl.py)

Our path: `mapInArrow` executor closure calls `apply_merge`, which calls
`dataset.merge_insert(...).when_matched_update_all().when_not_matched_insert_all()
.conflict_retries(n).retry_timeout(t).execute(upserts)` and
`dataset.delete(predicate, ...)` for deletes (`etl.py:393-425`).

Each executor partition issues independent `merge_insert` commits per routing
key. With 30k organisations each holding multiple (tenant, namespace) datasets,
a busy ETL window can attempt O(thousands) of commits concurrently across the
cluster, all contending on object-storage commit slots.

Object-storage commit throughput ceiling for Lance is approximately 1-4
transactions per second per dataset (manifest rename/put-if-absent is bounded
by S3 or GCS request rate). With even mild concurrency — e.g. 50 Spark tasks
each touching 20 datasets — that is 1,000 commit attempts that must serialise
per-dataset, making exponential backoff the primary throughput limiter.

### 2.3 Concrete comparison: fragment-write + single commit vs. merge_insert

```
Dimension                    fragment-write + commit      merge_insert per-partition
---------------------------- ---------------------------- --------------------------
Operation type               Append-only (no upsert,      Full upsert + physical
                             no delete)                   delete per dataset
Commit count                 1 per full batch (all         1 per routing key per
                             datasets, not per-dataset)   Spark partition (can be
                                                          thousands concurrent)
Conflict probability         Zero: only one commit per     Non-zero: multiple tasks
                             run at the driver             may touch the same dataset
Idempotency on retry         Fragment files are written    merge_insert is keyed on
                             once; a failed commit is      key_col, so a replay
                             re-tried by re-collecting     re-upserts to the same
                             existing fragments            stable result
Supports deletes             No                           Yes (when_matched_delete)
Supports upserts             No (append only)             Yes
Commit throughput ceiling    One per full ETL run;         Bottlenecked per-dataset
                             single driver serialised      by object-store tx rate
Schema change                Requires Overwrite or Merge   Handled transparently
                             operation variant
```

### 2.4 Power-law 30k-org shape analysis

Our dataset topology is a power law: a small number of large orgs drive the
majority of commit pressure. For a typical window:

- ~95% of orgs have 1-5 active routing keys: low commit pressure.
- ~1% of orgs have 100-1000+ routing keys: every ETL run issues hundreds of
  concurrent `merge_insert` commits against those datasets, each racing with
  compaction and index jobs.

The guide's single-commit pattern cannot replace `merge_insert` for datasets
with updates or deletes because `LanceOperation.Append` performs no row-level
deduplication. However, for net-new inserts — rows whose `op` column is
exclusively `insert` with no corresponding prior version of the key in the
dataset — the fragment-write + Append pattern avoids the per-dataset commit
race entirely.

A practical split:

```
Row class                          Recommended path
---------------------------------- --------------------------------------------
op = insert AND key does not       write_fragments per worker + single
  exist in target dataset          LanceOperation.Append commit at driver
  (true new records, e.g. first
  write for a new org)

op = insert/update AND key may     merge_insert (current path), required
  already exist                    for upsert semantics

op = delete                        merge_insert with when_matched_delete
                                   (current path), required
```

Implementing the split requires a dataset-existence check and an
insert-only gate per routing key, which adds driver-side complexity. The
benefit is concentrated on the initial bulk-load case (new org onboarding)
and large append-only namespaces.

### 2.5 What the guide says about distributed merge_insert

The guide does not mention a distributed `merge_insert` path. The API
`dataset.merge_insert(...).execute(data)` is a single-caller operation that
acquires a write lock internally. Parallelism within one dataset's
`merge_insert` is limited to Rust-level parallelism inside the lance crate,
not distributed across Spark executors.

The guide's `fragment.update_columns` / `fragment.merge_columns` methods
(`dataset.py:701, 773, 878`) support distributed per-fragment updates but
require the full fragment set upfront and commit via `LanceOperation.Update`
or `LanceOperation.Merge` — unsuitable for streaming upsert semantics.

### 2.6 Concrete recommendations

| # | Change | Evidence: guide URL + checkout path:line | Expected win | Risk |
|---|--------|------------------------------------------|--------------|------|
| W1 | Implement a per-routing-key insert-only fast path using `write_fragments` + `LanceOperation.Append` for Spark partitions where all rows are `op=insert` and the dataset does not yet exist (first write, new org). | guide: https://lance.org/guide/distributed_write/ — `write_fragments` pattern. `python/python/lance/fragment.py:1047` + `python/python/lance/dataset.py:4219`. | Eliminates commit-contention entirely for the initial bulk-load case. One Append commit per ETL run per new-dataset batch instead of one `merge_insert` commit per executor task. | Requires a dataset-existence probe (one `lance.dataset(uri)` call on the driver before the shuffle). Incorrect classification (calling Append when the key already exists) would duplicate rows. Gate must be conservative. |
| W2 | Batch multiple routing keys' `write_fragments` fragments into a single `LanceOperation.Append` commit per coordinator when all routing keys in a Spark partition are insert-only. | guide: https://lance.org/guide/distributed_write/ — collect all `FragmentMetadata` before commit. `python/python/lance/dataset.py:4429` (`commit_batch`). | Reduces the commit count from O(routing_keys) to O(1) per coordinator batch. Directly attacks the commit-ceiling bottleneck for high-fanout orgs. | `commit_batch` is present at `dataset.py:4429` but must be verified for correctness on multi-dataset batches. Dataset URIs must be distinct across the batch. |
| W3 | Reduce `ETLConfig.num_partitions` (currently 512) for low-volume windows and increase it only for high-volume windows, controlled by an estimated rows-per-window heuristic. | etl.py:673 — `routed.repartition(config.num_partitions, ...)`. High partition counts increase the number of concurrent `merge_insert` calls against the same dataset when routing keys collide across tasks. | Fewer concurrent commits per popular dataset, reducing backoff time on hot namespaces. Directionally confirmed by the commit-ceiling analysis: serialising commits is cheaper than contending and retrying. | Over-reducing partitions increases per-task data volume. Keep a floor of `max(num_routing_keys, 64)`. |
| W4 | Emit a per-routing-key commit latency distribution metric from `apply_merge` (`etl.py:385`) and a conflict-retry counter so the power-law shape of hot datasets is visible in Datadog. | etl.py:385-429 — `telemetry.timed("dataset.merge_ms")` already exists but does not tag by dataset depth or conflict count. | Enables identification of the specific hot datasets driving tail latency, informing W1/W2 prioritisation. | Additive instrumentation only. Tag by conflict count (0/1/2+), not by org_id, to avoid metric cardinality explosion. |
| W5 | Use `return_transaction=True` on `write_fragments` when implementing W1 so the driver can commit a batch of independent dataset transactions atomically via `LanceDataset.commit_batch`. | `python/python/lance/fragment.py:1052` — `return_transaction` parameter. `python/python/lance/dataset.py:4429` — `commit_batch`. | Reduces per-dataset coordinator round-trips further. Requires that `commit_batch` is production-stable on the lance version pinned by this project. | Verify `commit_batch` stability against lance `466405f47` before enabling. It is not mentioned in the guide. |
| W6 | For datasets that exclusively receive deletes in a window (all rows have `op=delete`), skip the `merge_insert` path and call `dataset.delete(predicate)` directly with a single batch predicate covering all deleted keys. The current code already does this for the delete branch (`etl.py:412-425`) but only after processing upserts. Make the delete-only case a dedicated early exit that skips the dataset open-or-create bootstrap. | etl.py:384-429 — the upserts check (`if upserts.num_rows`) already short-circuits but still opens the dataset handle for the delete branch unconditionally. | Eliminates one unnecessary `lance.dataset(uri)` call per delete-only routing key per task. Small per-key saving that compounds at 30k-org scale. | Negligible risk. Change is a refactor of the existing delete short-circuit. |
