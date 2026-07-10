# ETL Production-Readiness Report

**Scope:** `src/lance_etl/etl/` — the Iceberg-to-Lance ETL that routes hourly increments into one
Lance dataset per `(org_id, tenant_id, namespace)` trio.

**Goal:** scale to far more than 30,000 org datasets per run, and absorb a single billion-row org
without OOM by breaking its ingestion into multiple independent Spark tasks that still preserve
order-of-operations by timestamp. Keep the heavy work on native Spark operators rather than UDFs.

**Outcome:** both goals are met and verified. The change landed in two phases, is covered by unit
and integration tests, and was validated end-to-end against a real Iceberg source. One operational
check remains before a "battle-tested at scale" claim, described at the end.

---

## The problems found

Three concrete issues were identified by reading the code and tracing the data flow.

1. **A single unbounded-memory site.** The routing shuffle repartitioned by the routing trio, so
   every row of one org landed in exactly one Spark partition. The executor then rebuilt that whole
   partition into a single in-memory Arrow table before writing. A billion-row org exhausted memory
   there. Spark AQE could not help, because it only coalesces hash buckets and never splits one, and
   the Arrow batch-size cap was defeated by the table rebuild.

2. **Task count scaled with bytes, not org count.** With AQE sizing partitions by a 64 MB byte
   target, thousands of tiny orgs packed into one partition. Each org still pays a fixed per-merge
   cost of roughly one to two seconds to open its dataset and commit, so one task could serialize
   hours of commits at fleet scale.

3. **The only memory lever was the wrong shape.** The existing `spark_batches` knob split the
   increment into that many sequential Spark jobs. It was static, uniform across all orgs, and had
   to be sized for the worst org, so every hourly run paid the cost even when all orgs were tiny.

A fourth property was confirmed to be already sound: the last-write-wins collapse reduces the input
to at most one row per key before the shuffle, and the sink guards cross-window ordering with a
`source.ts >= target.ts` condition. That means splitting one org's rows across parallel tasks by a
hash of the key is correctness-preserving by construction.

## Design decisions confirmed with the user

Before implementing, four decisions were settled.

1. Cover both the steady-state increment path and the initial-backfill path for huge orgs.
2. Allow capped concurrent merge writers per dataset for big orgs, sized adaptively.
3. Remove `spark_batches` entirely, since it is superseded. Breaking changes are acceptable in this
   repository.
4. Treat the known cross-window stale-delete gap (Lance's `when_matched_delete` takes no condition)
   as documented behavior plus a pinning test, not a code change.

A read of the Lance 8.0.0 source confirmed the APIs that make the design possible. Concurrent
`merge_insert.execute()` calls on disjoint keys rebase at the row level and both succeed, even on the
same fragment. `write_fragments(return_transaction=True)` plus `commit_batch` gives a parallel bulk
path that commits many fragments in one transaction. The `execute_uncommitted` variant was ruled out
because it drops the row-level rebase information.

---

## Phase 1 — adaptive salted routing, streaming merge, `spark_batches` removal

**New module `plan.py`.** A single native Spark aggregation counts rows per trio. The driver
collects only the global totals and the small set of "big" trios whose count exceeds a threshold, so
driver memory stays proportional to the number of big orgs rather than the total org count. From that
it derives two numbers. K is the number of key-hash sub-buckets for each big org. N is the shuffle
partition width, taken as the larger of a row-based floor and an org-count-based floor, so task count
now grows with the number of orgs, not only with bytes. The same pass also produces the
null-routing-row count and enables a true empty-window short-circuit before any expensive shuffle.

**Salted shuffle.** Big orgs are broadcast-joined with a tiny table mapping each big trio to its K,
and the shuffle key gains a salt equal to `pmod(xxhash64(vector_id), K)`. The helper column is
dropped before the merge, so it is never written into any dataset. Because the salt is a pure
function of the merge key, every row of one key gets the same salt and lands in exactly one
partition. Concurrent sub-bucket writers for one dataset are therefore key-disjoint. This is
load-bearing, because the datasets declare no primary key and Lance would otherwise not detect two
writers inserting the same key.

**Streaming merge.** The executor now consumes each sorted shuffle partition as a stream of Arrow
batches through a new `stream_routing_groups` generator, buffering only the current org's group and
flushing it when the key changes or the buffered bytes reach the configured budget. Executor memory
scales with one org's group instead of the whole partition. This is the structural fix for the OOM.

**Removed `spark_batches`** entirely — the config field, the sequential loop, the CLI flag, and its
tests. New config tunables replace it with meaning: `bucket_rows`, `max_buckets_per_dataset`,
`datasets_per_task`, and `max_shuffle_partitions`, each with a one-line docstring.

**Fleet fix.** Interval-tag stamping now fans out over a width that scales with the dataset count, so
stamping hundreds of thousands of datasets does not serialize.

## Phase 2 — bulk-append fast path for new or empty datasets

**New module `bulk.py`.** For a big org whose dataset is absent or empty, the ETL now writes
fragments in parallel across the sub-buckets and commits them all with a single `commit_batch`, which
is one commit instead of thousands of per-key merge commits. This sidesteps the commit-throughput
ceiling entirely for the backfill case.

The hard part solved here is schema agreement. Each parallel task sees a different subset of the map
keys, so the driver first derives one canonical schema per org via native Spark (distinct keys per
map, and the most-frequent vector length per key), then broadcasts it. Each task pivots its slice,
aligns it to the canonical schema by null-filling any keys it did not see, and appends. Two real
production-correctness subtleties surfaced and were reconciled so the bulk output matches the merge
output byte-for-byte: timestamps are re-stamped to the Spark session time zone (the driver's Arrow
conversion normalizes to UTC while the executor path uses the session zone), and base-column
nullability is preserved from the source rather than forced.

Eligibility is checked on the driver with metadata-only reads. A dataset that is absent or has zero
rows takes the fast path. On replay, a dataset that now has rows falls back to the idempotent merge
path, so a re-run never duplicates data. A dataset that gained rows in the race window between
planning and bootstrap is demoted and also flows to the merge path, so no row is ever dropped.
Column roles are persisted after the commit so backfilled datasets are visible to the indexer.

---

## Ordering guarantees

Order-of-operations is preserved end-to-end by event timestamp, independent of how the work is
parallelized.

- Within a window, the collapse keeps the last-write-wins terminal event per key.
- Splitting an org across sub-buckets is safe because all events for a key hash to the same bucket,
  so per-bucket collapse equals the global winner for that key.
- Across windows, the sink's `source.ts >= target.ts` guard prevents a later batch carrying an older
  timestamp from overwriting a newer stored value.
- The one exception is physical deletes, which Lance cannot condition on a timestamp. A cross-window
  stale delete can remove a newer row. This is documented in ADR 0034 and pinned by a test, per the
  agreed decision to document rather than engineer around it.

## Verification

- The non-bench suite is green on `etl/venv`: 540 passed, 1 skipped, 1 xfailed. The xfail is the
  pre-existing pylance 8.0.0 concurrent-merge-against-BTREE-delta regression, not introduced here.
  Both ruff format and ruff check pass.
- Twenty-one integration tests run against real local Spark. They cover the skewed big-org fan-out
  (exact row counts, zero duplicate keys, correct last-write-wins across concurrent writers), the
  many-org partition floor that pins the greater-than-30k scaling law, cross-window ordering through
  the full Spark path, and the ten bulk-append cases (byte-for-byte equivalence with the merge path,
  replay idempotency, demotion, heterogeneous-key alignment, delete no-ops, and role persistence).
- The real Iceberg-to-Lance end-to-end path was validated. Three bench e2e tests drive the true
  production entry point `IcebergToLanceETL.run()`, which resolves Iceberg snapshot bounds and reads
  a real Iceberg table before collapse, routing, and merge through the restructured code. These are
  the only tests that exercise the seam between a real Iceberg-sourced DataFrame and the ETL, and
  that seam is where the time-zone and nullability subtleties live. Running them required installing
  `grpcio`, `grpcio-tools`, and the `bench` dependency group into `etl/venv`.

## The one remaining operational gate

The design does not claim to beat a fundamental limit. A single Lance dataset commits through one
manifest compare-and-swap, which on object storage tops out at roughly one to four transactions per
second per dataset. K concurrent writers parallelize the compute and bound the memory, but they do
not raise a single dataset's commit throughput, so a very hot dataset is wall-clock-bound by that
ceiling regardless of K. The steady-state merge path absorbs this with two retry layers and a
per-commit timeout, and the bulk-append path removes it entirely for backfills by paying one commit.
The math and the four operator levers are documented in ADR 0034. Validating a specific K against a
specific object store is an operational check that local-disk tests cannot perform, because a local
manifest CAS is far faster than object storage. That check against real or emulated cloud storage is
the remaining item before a fully battle-tested claim.

## Files changed

- New source: `src/lance_etl/etl/plan.py`, `src/lance_etl/etl/bulk.py`.
- Modified source: `src/lance_etl/etl/job.py`, `pivot.py`, `cli.py`, `__init__.py`.
- New tests: `tests/test_etl_plan.py`, `test_etl_streaming_merge.py`, `test_etl_skew.py`,
  `test_etl_bulk_append.py`. Modified: `test_etl_pivot.py`, `test_etl_concurrency.py`, `test_cli.py`.
- Docs: ADR 0034 added to `docs/adr/etl-and-data-model.md` with its index row, plus updates to
  `AGENTS.md` and `README.md`.

The working tree is staged for review and has not been committed.
