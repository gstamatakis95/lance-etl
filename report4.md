# Review-Fix Batch Report

**Scope:** the fixes applied in response to the full-project review (see the review that produced the
findings below). Covers the Rust search service, the Python maintenance and indexing fleet jobs, the
shared infrastructure, the benchmark harness, and the Airflow DAGs. The ETL package
(`src/lance_etl/etl/`) was out of scope and owned by a separate effort. Git was left untouched for the
owner to review and commit.

**Current repo state note:** at the time this batch landed, the fast gate
(`etl/venv/bin/pytest -q -m "not integration"`) was `613 passed` (up from a `589` pre-batch baseline).
A separate clustered-rewrite feature (`maintenance/cluster.py`, ADR 0041) landed afterward and moved
the baseline to `636`. That feature is not part of this batch. This report covers only the review-fix
work.

---

## Executive summary

The full-project review found no critical bugs and confirmed the security-critical invariants hold
(typed filters and no raw SQL, cross-org path isolation, infallible metrics, all-commits-retry-wrapped,
segment-API compliance). It surfaced one HIGH scale finding, several MEDIUMs, and cheap LOWs. All of
them are now fixed and verified across five parallel workstreams. The fast test gate was green at
`613 passed, 1 skipped, 1 xfailed` (the xfail is the documented pylance 8.0.0 BTREE-delta regression),
the maintenance and indexing integration subset was green (idempotency, concurrency coexistence, V2
manifest), the Rust crate passed `cargo fmt`, `cargo clippy -- -D warnings`, `cargo build`, and
`cargo test`, and `ruff` was clean across the tree.

---

## H1 (priority) — Object-store centroid cache

**Problem.** The indexing build phase collected every active vector org's IVF centroids to the driver
into one dict, then broadcast the whole dict to every executor even though each task needs only its
own. Driver and executor memory scaled with fleet composition, the broadcast was never released and
was recreated each replan round, and a JVM-level executor OOM there failed the stage and aborted the
whole per-dataset-isolated run. It was the single place memory grew with fleet composition and the one
hole in the isolation model.

**Fix (ADR 0040, supersedes ADR 0025).** Centroids are now persisted to an object-store sidecar and
each build task reads its own dataset's centroids per-task. There is no broadcast.

- Persistence uses lance's native `lance.indices.IvfModel.save(uri, storage_options=)` /
  `IvfModel.load(uri, storage_options=)` single-file format, which threads `storage_options` through
  lance's own object-store layer (the same credential path as every other lance call). The
  `create_index` `ivf_centroids_file` parameter was deliberately avoided because it bypasses
  `storage_options`.
- The sidecar path is a sibling of the `.lance` directory, `{uri}.artifacts/{index_name}.{rows_at_train}.ivf`,
  which dataset discovery ignores because its final component does not end in `.lance`.
- `rows_at_train` (already in the manifest config KV, free to read) is the staleness fingerprint.
  Centroids change only when the bootstrap commits, which is exactly when a new `rows_at_train` is
  written, so a matching-fingerprint sidecar is a valid reuse and a mismatch is an automatic
  invalidation. The fingerprint is encoded in the path, so conditional reuse is a plain file-existence
  check.
- The bootstrap persists centroids best-effort after its commit. A sidecar write failure emits a
  metric and logs, but never fails the already-committed index build.
- Each build task reads the sidecar first and falls back to `get_ivf_model` on a miss (legacy or
  first-run indexes), opportunistically backfilling. Correctness never regresses even if the sidecar
  write lags or fails.
- The entire fleet artifact-resolve phase (`resolve_fleet_artifacts`, `resolve_vector_artifacts`,
  `drop_errored_vector_specs`, the artifact-error and artifact-extras dicts, the broadcast, and the
  `centroids_to_ipc`/`centroids_from_ipc` helpers) was removed. Failure isolation is preserved by the
  existing per-shard build guard: a vector index whose centroids cannot resolve now raises inside its
  build shard, is caught as a per-index build error, and is excluded from the commit phase. The net
  outcome is identical, with the failure phase changing from artifact-resolve to build.

**Verified.** `test_fleet_idempotency` remains a strict no-op on a second run (a sidecar read commits
nothing, so the dataset version is unchanged), the concurrency coexistence test passes with the
reworked in-process indexing path, and a new `test_centroid_sidecar.py` covers bootstrap-write plus
reload round-trip, segments-mode reuse that provably skips `get_ivf_model`, a missing sidecar falling
back correctly, and a `save` failure not failing the bootstrap.

---

## Rust search service

- MEDIUM — unbounded top-k. `validate_k` only rejected `k == 0`, so a small request with a huge `k`
  on the flat or text or unindexed path made Lance return the whole dataset and materialize it into a
  multi-gigabyte JSON response. Added a configurable `search_max_k` (default `10_000`, env-driven),
  applied to the vector, text, and fused paths, and critically to the derived `fetch = k + offset`
  that is what Lance actually materializes.
- LOW — integer overflow. `fetch` now uses `saturating_add`, and `k`/`offset` convert to `i64` via
  `i64::try_from(...).unwrap_or(i64::MAX)` instead of a raw cast that could wrap a large `u64` offset
  to negative.
- LOW — epoch multiply overflow in `time_literal` now uses `checked_mul`, returning an invalid-argument
  error instead of wrapping in release.
- LOW — internal error leakage. The catch-all error arm now logs the full Lance error server-side via
  `tracing` and returns a generic internal message, so raw paths and engine detail never reach the
  client.
- LOW — FTS recursion depth. Added an explicit `MAX_FTS_DEPTH` guard in both the proto and lance
  conversion sites, mirroring the filter path's cap, so depth safety does not rest solely on prost's
  default recursion limit.

All added with unit and integration tests. `cargo fmt`, `cargo clippy -- -D warnings`, `cargo build`,
and `cargo test` all pass clean.

---

## Python shared and maintenance

- MEDIUM — TTL predicate timezone. The per-row TTL delete predicate compared a timezone-aware column
  against a timezone-naive literal, which could be silently offset under a non-UTC session timezone on
  a row-deletion path. Both sides are now cast to explicit UTC:
  `arrow_cast({ts} + {ttl}, 'Timestamp(Microsecond, "UTC")') < arrow_cast('{cutoff}', 'Timestamp(Microsecond, "UTC")')`.
  This is a direct UTC-instant comparison with no naive operand and no reliance on implicit coercion.
  Confirmed empirically against a real Lance dataset that the both-sides-explicit predicate parses,
  runs, and gives the correct deletion result.
- MEDIUM — filesystem client reconstructed per prefix. `discover_datasets` rebuilt an S3/GCS/Azure
  filesystem on every first-level prefix inside the executor fan-out. It now resolves the filesystem
  once per Spark task and reuses it across all prefixes in that task.
- MEDIUM — path-traversal in identifier validation. The `recall.py` path-component pattern allowed
  `.` and `..`, so an org id of `..` could escape the tenant tree. Both `recall.py` and
  `migrate_namespace.py` now reject `.` and `..` explicitly.
- LOW — wasted terminal backoff. `commit_with_retries` slept one more time after the final failed
  attempt before re-raising, adding up to tens of seconds of delay before a genuinely-failing commit
  surfaced its error. It now skips the sleep on the last attempt. The stale `DEFAULT_COMMIT_RETRIES`
  docstring was corrected.
- LOW — duplicate input URIs. `load_dataset_uris` now dedupes while preserving order, so the same
  dataset cannot self-conflict from a repeated flag or an explicit URI also found by discovery.
- PLAUSIBLE — recall two-phase fragment-count race. The large-tier recall path probed a fragment
  count once and reused it, so a concurrent compaction shrinking the fragment count raised an uncaught
  IndexError that killed the whole large-tier job. It now bounds-checks the fragment index against the
  freshly-opened dataset and turns a missing fragment into a per-sample skip that flows through the
  existing skip-reason aggregation, keeping the recall numbers read-correct.

---

## Benchmark harness

Both fixes close silent number-deflation paths, which are dangerous precisely because they do not error.

- MEDIUM — `search_k` was not validated against the recall cutoffs. A `--search-k` below the maximum
  cutoff silently deflated `recall_at_100` because the retrieved array was shorter than the cutoff,
  not because the index missed neighbors. `RECALL_CUTOFFS` is now a single shared source of truth and
  `BenchConfig` rejects a `search_k` below `max(RECALL_CUTOFFS)` at construction.
- MEDIUM — malformed result IDs were dropped silently. Server-side schema or wire-encoding drift would
  silently truncate retrieved-id lists and deflate every recall, FTS, and hybrid number. The result
  parser now fails loud, since every call site projects `vector_id` and a missing one can only mean a
  bug the benchmark must not absorb.

---

## Airflow

- MEDIUM — the ETL DAG and the pipeline DAG both defaulted to `@hourly` with no code-level coupling,
  so on a fresh install they could fire at the same time and collide on the same dataset, with only
  commit-retry as the backstop. The pipeline DAG default schedule is now staggered off `@hourly`
  (still operator-overridable via the existing Variable). A stagger was chosen over an
  `ExternalTaskSensor` because both schedules are independently operator-tunable, so a fixed
  execution-delta sensor would silently break the moment either schedule changed.
- The overstated "overlapping runs are safe by design" DAG docstring was corrected to match ADR 0038's
  honest position. Overlap safety currently rests on commit-retry, the structural no-overlap guarantee
  is an operational scheduling responsibility, and `max_active_runs=1` only serializes a DAG against
  itself. ADR 0038 notes the shipped stagger reduces but does not eliminate the collision.

---

## Coverage honesty

H1's actual payload, per-executor object-store reads replacing the broadcast and the fleet-scale
memory win, is exercised in-process on local disk (the in-process Spark stand-in and local
filesystem), not in a real distributed or cloud run. Its cloud correctness rests on
`IvfModel.save/load` threading `storage_options` through the same path as every other lance call
(confirmed during exploration) plus the concurrent-backfill safety argument. A torn sidecar write
cannot corrupt the current build, because each shard builds from its own hit-or-fallback and the
sidecar is a future-run cache, and a torn or partial file is swallowed into a fallback rather than
read as valid-but-wrong. That is sound reasoning plus unit tests, but it is the same pre-existing
no-real-scale and no-distributed-test gap the review already named. It is deferred, not closed. The
other deferred item is a CI-guaranteed Python-to-Rust integration test. Both are test-infrastructure
efforts rather than defects.

---

## Verification commands

```bash
etl/venv/bin/pytest -q -m "not integration"                       # 613 passed at batch time
etl/venv/bin/pytest -q -m integration tests/test_fleet_idempotency.py \
  tests/test_concurrent_coexistence.py tests/test_v2_manifest_paths.py
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/
cd rust/search-api && cargo fmt && cargo clippy -- -D warnings && cargo build && cargo test
```
