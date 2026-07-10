# ETL and data model — architecture decisions

This document consolidates the architecture decisions governing the Iceberg-to-Lance ETL and
the per-dataset data model. Each section keeps its original ADR number so references like
"ADR 0016" in code and docs resolve here. Superseded decisions are summarized at the end.

## ADR 0003 — Incremental Iceberg reads via snapshot-id bounds

Status: Accepted

Iceberg 1.10 categorically rejects `start-timestamp` / `end-timestamp` read options on batch
scans (they are valid only for changelog scans, verified against the resolved runtime jar). The
ETL therefore resolves each wall-clock window to snapshot ids before reading: a helper
(`snapshot_id_bounds` in `etl/job.py`) queries the `{table}.snapshots` metadata table for the
last snapshot strictly before the window start (exclusive lower bound) and the last snapshot at
or before the window end (inclusive upper bound), then reads with `start-snapshot-id` /
`end-snapshot-id` as an incremental append scan. The first run with no prior snapshot falls back
to a full batch scan pinned with `snapshot-id` at the end bound. A window that resolves to no new
snapshots returns an empty frame, gated by a `has_new_snapshots` flag so a narrow window whose
bounds resolve to the same snapshot does not silently skip data. The orthogonal
`--window-start` / `--window-end` / `--window-column` pushdown filter composes on top.

## ADR 0004 — Routing targets and duplicate semantics

Status: Accepted (dynamic partition targets later fixed to the trio)

Routing is the fixed trio `org_id/tenant_id/namespace`: every row maps to exactly one dataset at
`{base}/{org_id}/{tenant_id}/{namespace}.lance` with no cross-org sharing. `dataset_uri` (in
`etl/sink.py`) validates every path component, and dataset discovery for indexing and
maintenance is a recursive `*.lance` glob. Because each key lives in exactly one dataset, the
per-dataset `merge_insert` keyed on `key_col` is the sole dedup mechanism.

Two pieces of the original decision were later removed. The `partition_derivations` /
`--partition-derive` strftime machinery went away with by-date partitioning (ADR 0014), and the
configurable `partition_cols` knob on the ETL was fixed to the trio (a `--partition-by` flag
survives only on the `migrate-namespace` operator tool). The single-org contamination guard that
once ran in maintenance was also removed in the unified-orchestration rework.

## ADR 0016 — Event-time canonical clock

Status: Accepted (supersedes ADR 0011)

The source event timestamp column (`ETLConfig.ts_col`, now defaulting to `"event_timestamp"`
per ADR 0024) is the single canonical clock. The `_ingested_at` ingestion-timestamp column was
removed entirely: maintaining two time columns created a second, derived time axis that drifted
from the event axis on retries and backfills, and event-time range queries are naturally scalar
range filters on the event timestamp (pruned by a BTREE index) rather than filters on ingest
time. Existing datasets carrying the column keep working, the column simply is not written or
updated. The accepted tradeoff: ingest-age retention is not expressible, retention is by event
age only (the per-row TTL design measures against the event clock).

## ADR 0024 — Dynamic per-dataset map pivot: every key becomes a column

Status: Accepted (supersedes ADR 0020)

Every distinct key present in a routing group's `vectors`, `texts`, and `metadata` maps becomes
a concrete column in that group's dataset. The pivot (`pivot_map_columns` in `etl/pivot.py`)
runs per dataset on the executor after the shuffle collocates rows, so each org's schema
contains only the keys that org actually uses — no cross-org schema pollution across a
power-law fleet of 30k+ orgs, and no operator enumeration of searchable fields at deploy time.

Mechanics that still govern the code:

- Keys colliding with an existing or reserved column are skipped and metered
  (`dataset.invalid_map_keys`) rather than failing the job.
- Vector map values arrive as `list<float32>` (contract `ARRAY<FLOAT>`), the fixed-size-list
  dimension is inferred from the first non-null entry, float64 is normalized to float32, and an
  all-null vector column stays a nullable list with no FSL cast.
- Text and metadata keys become `string` columns — real, filter-eligible, scalar-index-ready.
- New keys in later windows are absorbed by grow-only `add_columns` schema evolution. No code
  path ever removes a dataset column.
- The input schema carries no type uncertainty: `docs/iceberg-source-table.sql` is the single
  typed contract, `validate_schema` verifies it fully, and all casts are contract-driven. The
  string-spec type parsing (`arrow_types.py`, `ETLConfig.column_types`) was deleted.
- The TTL column (`ttl`, `BIGINT` seconds) is cast automatically to `pa.duration("s")` so the
  maintenance delete predicate `event_timestamp + ttl < now` evaluates natively.
- Defaults align with the SQL contract: `ts_col="event_timestamp"`,
  `window_column="processing_timestamp"`.

## ADR 0032 (ETL half) — Hourly interval tags at write time

Status: Accepted

Every ETL run stamps each dataset it wrote with a Lance tag named after the truncated UTC hour
it was produced (`%Y%m%dT%H%M%SZ`, via `cliutil.parse_hour_tag`, wired as `--tag-stamp` and
passed `{{ data_interval_end }}` by the Airflow DAG). The stamp runs once on the driver after
all batches commit (`IcebergToLanceETL.stamp_interval_tags`) and is create-or-move: a later run
within the same hour advances that hour's tag to the newest version, so a tag always marks the
latest version produced in its hour. Tagged versions are exempt from version cleanup until the
pipeline prunes old interval tags. The query-pinning half of ADR 0032 lives in
`serving-filters-and-tags.md`.

## ADR 0034 — Adaptive salted routing, streaming merge, and spark_batches removal

Status: Accepted

The ETL routes each hourly Iceberg increment into one Lance dataset per `org_id/tenant_id/namespace`
trio (ADR 0004) via last-write-wins `merge_insert`. At production scale a single run touches more
than 30k org datasets, the distribution is power-law, and one org can hold up to roughly 1B rows.
Three properties of the old router failed at that scale.

- **OOM on a hot trio.** The routing shuffle was `repartition(*ROUTING_COLS)`, which hashes by trio,
  so one org's entire increment landed in exactly one shuffle partition. An executor then
  materialized that whole partition (`pa.Table.from_batches`) to merge it. A 1B-row org OOMs, and
  AQE cannot help: it only coalesces hash buckets, it never splits one.
- **Byte-only task sizing against a fixed per-merge cost.** Task count scaled with bytes (the 64MB
  AQE advisory), but each trio carries a fixed ~1-2s per-merge commit cost regardless of size. A
  64MB partition holding thousands of tiny orgs therefore serialized hours of commits into one
  task. The lever that mattered was the number of orgs per task, and bytes did not express it.
- **The wrong-shaped memory lever.** The only knob, `spark_batches`, was a static run-uniform count
  that split the increment into that many sequential Spark jobs. It had to be sized for the worst
  org in the run and so penalized every run that happened to carry only small orgs.

### The adaptive routing plan

Routing now begins with one native Spark aggregation, `groupBy(org_id, tenant_id, namespace).count()`
(`compute_routing_plan` in `etl/plan.py`, producing a `RoutingPlan`). The driver keeps only (a) the
global aggregates `trio_count` and `total_rows` and (b) the small set of "big" trios whose count
exceeds `bucket_rows`. Driver memory is therefore O(big trios) and holds steady past 1M+ trios,
because the long power-law tail of small orgs never crosses the driver. The aggregation is also the
first action in the job, which gives a true empty-window short-circuit: the null-routing
`Observation` and a zero-row check are read before any collapse or merge shuffle runs.

Two numbers come out of the plan. K is the number of sub-buckets for a big dataset, and N is the
number of shuffle partitions for the whole run.

```
K = min(max_buckets_per_dataset, ceil(rows / bucket_rows))            # per big trio
N = clamp(max(ceil(total_rows / bucket_rows),
              ceil(trio_count / datasets_per_task)),
          1, max_shuffle_partitions)
```

N is set **explicitly** rather than left to AQE. AQE coalesces partitions by bytes only, which is
exactly the many-tiny-orgs failure mode from above. The dataset-count floor
`ceil(trio_count / datasets_per_task)` is the term that makes task count scale with the number of
orgs, not just with bytes, so a run of 30k tiny orgs still fans out across enough tasks to keep the
fixed per-merge cost parallel.

### Salted shuffle and key-disjointness by construction

`apply_salted_shuffle` broadcast-joins the big trios against a tiny `{trio -> K}` table and shuffles
on `(org_id, tenant_id, namespace, pmod(xxhash64(vector_id), coalesce(K, 1)))`. Small trios are not
in the big set, so they take K=1 and keep a single writer. The salt helper column is dropped before
the merge, so it is never written into any dataset.

The salt is a pure function of the merge key `vector_id`. Every row sharing a key gets the same salt
and lands in exactly one partition, so the K concurrent sub-bucket writers for one big dataset are
**key-disjoint by construction**. This disjointness is load-bearing for correctness, not merely an
optimization. The datasets declare no unenforced primary key, so Lance 8.0.0 performs no insert-side
conflict detection: there is no `inserted_rows_filter` bloom, and two concurrent `merge_insert`
writers inserting the SAME key would silently produce duplicate rows. Salting on the key guarantees a
given key is only ever written by one task. Verified against the lance 8.0.0 source: concurrent
full-column `merge_insert.execute()` on disjoint keys rebase at the row level via deletion-vector
union and both succeed, and genuine overlaps retry automatically under `conflict_retries`. K
concurrent disjoint-key writers per dataset are therefore safe.

### Commit contention at scale (the K-does-not-buy-commit-throughput limit)

The salted shuffle fans a big dataset across K concurrent `merge_insert.execute()` writers, and the
previous subsection established that this fan-out is correctness-safe. It is worth being precise about
what K actually buys, because the answer is not "more commit throughput to that dataset."

A single Lance dataset has ONE manifest, and every commit is a compare-and-swap against it. All K
writers targeting one dataset serialize through that single CAS point. K therefore parallelizes the
COMPUTE of the merge — the map pivot, the DataFusion hash-join build side, and deletion-vector
construction — and it bounds executor memory, because each task materializes only its own bucket
through the streaming grouper. What K does NOT do is raise the dataset's commit throughput. Commit
throughput for one hot dataset is bounded by the object store's per-dataset manifest-write ceiling,
which the market-research notes put at roughly 1 to 4 transactions per second regardless of how many
writers are pushing.

The worked numbers make the ceiling concrete. Each bucket commits about
`bucket_rows / (merge_batch_bytes / row_width)` chunks sequentially, and each chunk is one commit. For
a hot dataset receiving R rows in one window across K buckets, the total commit count to that dataset
is about `K * (R/K) / rows_per_chunk = R / rows_per_chunk`. It scales with total rows and chunk size,
not with K. At a 64 MiB `merge_batch_bytes` and a 128-dim float32 row of about 512 bytes that is about
131K rows per chunk, so a 50M-row window into one dataset is about 380 commits. At 1 to 4 tx/sec that
is roughly 95 to 380 seconds of wall-clock for that one dataset no matter how large K is. A 1B-row
single-dataset merge is proportionally longer.

Two retry layers absorb the contention rather than failing under it. The operation-level retry inside
`merge_insert.execute()` (`conflict_retries`, ETL default 10) re-runs the whole merge on a
`RetryableCommitConflict`. Beneath it, Lance's manifest-CAS loop retries the physical commit (default
20). The hard cap is `retry_timeout` (ETL default 120s) PER commit attempt-sequence, which gives
generous headroom for a single commit to win the CAS even under heavy contention. Exhaustion surfaces
loudly as `TooMuchWriteContention`. It is never a silent corruption or a lost write.

The operator levers all already exist for a hot dataset approaching the ceiling. Lower
`max_buckets_per_dataset` to reduce the number of concurrent contenders. Raise `merge_batch_bytes` to
commit fewer and larger chunks, trading a larger DataFusion build side for fewer commits. Raise
`retry_timeout` for a slow object store. Monitor `dataset.merge_conflict_retries`, already emitted, to
watch contention building before it exhausts the budget.

The bulk-append path (Phase 2, a later subsection) sidesteps this entirely for the empty-or-new-dataset
case. It writes fragments in parallel and commits them with a SINGLE `commit_batch`, so a fresh 1B-row
backfill pays one commit rather than thousands.

This ceiling is a fundamental property of single-writer-per-dataset object-store commits, not a defect
of this design. The levers above are the sanctioned response to it. Local-disk tests cannot exercise
the object-store ceiling, because a local manifest CAS is far faster than a remote one, so validating a
specific K against a specific store is an operational check rather than a unit test.

### Bulk-append fast path for new or empty datasets

The merge path is the correct engine for incremental change routing, but its per-key, per-commit
machinery is pure overhead when the target dataset is brand new or empty. There is nothing to match
against and no delete can hit an existing row, yet a billion-row backfill still routes through
thousands of `merge_insert` commits and piles the contention retries of the previous subsection onto
each dataset's single manifest. Phase 2 (`etl/bulk.py`) adds a fast path for exactly that case,
wired into `run_on_dataframe` ahead of the merge via `run_bulk_phase`.

**Eligibility.** A trio takes the fast path only when it is BOTH big (already salted with `K > 1` by
the routing plan) AND its dataset is absent or reports `count_rows() == 0`. `plan_bulk_append`
answers emptiness from the manifest without scanning rows. A non-empty dataset is left to the merge
path, whose idempotent upsert is required to reconcile existing rows. Small trios stay on the merge
path unchanged. Every bulk-appended trio is excluded from the merge input by a broadcast left-anti
join (`exclude_bulk_trios`), so no row is ever written by both paths.

**Canonical-schema derivation and why it is needed.** Each parallel append task sees only its own
key-hash slice of the trio, so different tasks observe different subsets of the `vectors`, `texts`,
and `metadata` map keys. Left to infer their own schema, they would emit structurally incompatible
fragments that cannot commit together. `derive_bulk_schemas` therefore computes ONE canonical
`pa.Schema` per trio on the driver, from the same collapsed non-delete rows the pivot will consume,
in the exact column order `pivot_map_columns` produces. Each task then pivots with those canonical
vector dimensions and `align_to_schema` null-fills the keys its slice lacks, so every task's
fragments are union-compatible. `test_canonical_schema_matches_pivot_output` pins that the derived
schema equals the pivot output field for field.

**One commit instead of thousands.** Each `(task, trio)` streams its aligned chunks into ONE
`write_fragments(..., mode="append", return_transaction=True)`, and the driver merges every task's
transaction for a trio into ONE physical `commit_batch` append (`commit_bulk_transactions`). A fresh
1B-row backfill therefore pays a single commit rather than thousands, sidestepping the per-dataset
manifest-write ceiling documented in the contention subsection above entirely.

**Tunables.** Two `ETLConfig` fields govern the path.

- `bulk_append` (default `True`): the operational kill switch. Set it to `False` to route every trio
  through the merge path.
- `max_bulk_tasks_per_dataset` (default `1024`): the cap on parallel append tasks per bulk-eligible
  dataset. Appends carry no per-key commit contention, so this cap sits far above the merge-writer
  cap `max_buckets_per_dataset`.

**Idempotency and crash safety.** A raw append is not idempotent the way `merge_insert` is, so the
path relies on the emptiness guard rather than per-key matching. A replayed window finds the dataset
non-empty and falls back to the idempotent merge path, which converges without duplicating rows
(`test_replay_falls_back_to_merge_no_duplicates`). A crash after `write_fragments` but before
`commit_batch` leaves the written fragment files unreferenced by any manifest, so they are invisible
to every reader and are reclaimed by the maintenance job's orphan-file cleanup. No partial rows ever
become visible.

**Bootstrap re-check demotion.** `bootstrap_bulk_datasets` creates each eligible dataset empty at its
canonical schema, then re-reads `count_rows()` and demotes any trio that gained rows between planning
and bootstrap. A trio a concurrent writer filled in that race window is handed back to the merge path
instead of being double-written, so the fast path never races the merge path into the same dataset.

**Three by-design divergences from the merge path.** The bulk output equals the merge output
byte-for-byte in the common case, and the three exceptions are all safe precisely because the target
is new or empty.

1. Vector-dimension inference. Bulk uses the most-frequent value length per key (count descending,
   size ascending on ties), which is more robust against a single short vector than the merge path's
   first-non-null inference. The two agree whenever a key's vectors are uniform, which they are in
   any well-formed source.
2. Fully-null vector keys. A vector key that is null in every row of a trio carries no dimension to
   fix, so bulk omits it. The merge path would materialise it as a raw all-null `list<float>` column.
   This is the one degenerate shape the fast path does not reproduce.
3. `commit_batch` is not per-key-idempotent. Re-committing the same fragments would duplicate rows,
   unlike a replayed `merge_insert`. This is safe here because the target is a brand-new empty
   dataset with no concurrent ETL writer, and `commit_batch` runs its own inner rebase retry, so an
   ordinary append rebases without the outer wrapper ever re-running the action.

### Streaming merge

The executor no longer materializes a whole partition. `stream_routing_groups` (in `etl/pivot.py`,
consumed by `merge_partition` in `etl/job.py`) consumes the sorted partition as a stream of Arrow
batches, buffering only the current routing-key group and flushing it to `apply_merge` when the key
changes OR the buffered bytes reach `merge_batch_bytes`. Executor memory scales with one dataset
group, not the whole partition, which is what removes the hot-trio OOM. The byte-budget flush is
order-safe because collapse already guarantees at most one row per merge key reaches the merge, so a
mid-group flush can never split a key across two `merge_insert` calls.

### spark_batches removal and the new tunables

`spark_batches` is removed entirely: the config field, the `--spark-batches` CLI flag, and the
sequential batch loop are gone. Adaptive sizing from the actual per-trio counts supersedes it. This
is a breaking change with no deprecation shim, which is acceptable in this repository. Four new
`ETLConfig` tunables replace it.

- `bucket_rows` (default `2_000_000`): target rows per sub-bucket and the threshold above which a
  trio is treated as big. It sets the grain of both K and N.
- `max_buckets_per_dataset` (default `32`): the cap on K, bounding how many concurrent writers a
  single hot dataset can fan out into.
- `datasets_per_task` (default `64`): target number of small orgs merged per task, via the N floor
  `ceil(trio_count / datasets_per_task)`.
- `max_shuffle_partitions` (default `32_768`): the hard ceiling on N.

### Interval-tag stamping fleet fix

`stamp_interval_tags` (ADR 0032) previously stamped datasets from a single point. It now fans out
over `max(512, ceil(len(uris) / datasets_per_task))` partitions, so stamping 300k datasets after a
run does not serialize behind one worker.

### Known gap: cross-window stale deletes

`merge_insert.when_matched_delete()` takes NO condition in Lance 8.0.0. A replayed or late ETL
window that carries an OLD delete op for a key can therefore physically remove a NEWER stored row
for that key. Within a single window this is mitigated because `collapse` selects the terminal op
per key, so only the last op in the window is applied. Across windows it is unguarded: an
out-of-order window delete wins over a newer row that a later-numbered window already wrote.

Upserts do NOT have this problem. They are guarded by the `source.ts >= target.ts` update condition
(`build_update_condition` in `etl/sink.py`), so a stale update simply does not apply. Only deletes
are exposed, because the delete predicate carries no timestamp guard.

This is an accepted and documented limitation for Phase 1, not a defect to be fixed here. A
condition-carrying delete would require an upstream Lance API that does not yet exist. The behavior
is pinned by `test_stale_cross_window_delete_removes_newer_row` in `tests/test_etl_concurrency.py`,
which references this ADR: the test asserts the current (stale-delete-wins) behavior so that any
future change to it is a deliberate, reviewed break rather than a silent regression.

## Superseded decisions

- **ADR 0011 — `_ingested_at` ingestion-timestamp column.** Superseded by ADR 0016. The column
  and its stamping machinery were removed.
- **ADR 0020 — Static declared-field map pivot.** Superseded by ADR 0024. The
  `vector_fields` / `text_fields` declarations, the `--vector-field` / `--text-field` /
  `--column-type` flags, and the positional `metadata_keys` / `metadata_values` arrays were all
  removed.
