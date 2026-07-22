# Production-readiness and simplification review

- **Date**: 2026-07-22
- **Commit reviewed**: `22e0615bb635801297ab21170d9d3d8e763d7a9a` (branch `gs/f1`, clean tree)
- **Reviewer basis**: independent adversarial pass. Every finding below was verified directly
  against the code (and, where relevant, against the lance `release/v8.0` checkout at
  `/Users/gstamatakis/IdeaProjects/lance`). `uvx ruff format --check` and `uvx ruff check` were
  re-run at HEAD and are clean.

**Verdict.** The core is in genuinely good shape. The lease/fence SQL, the completion-marker
protocol, the replay-safe merge, and the Rust service's resource lifecycle all survived adversarial
tracing (details in "Verified clean"). No BLOCKER was found. What remains is concentrated in two
MAJOR failure-path honesty gaps (the reconciler loop has no isolation around result
*reconciliation*, and the operator fleet CLIs report real open failures as benign "skipped" with
exit 0), a set of MINOR classification and doc-drift issues left behind by the recent overhaul, and
three concrete dead-code simplifications. Nothing found contradicts the settled decisions
(segment-API recipes, no stable row IDs, plaintext local-first transport, 9-table control plane).

---

## MAJOR

### PR-01 — Result reconciliation is outside the dispatcher's failure-isolation boundary

**Files**: `src/lance_etl/reconciler/service.py:386-391` (`BoundedDispatcher.run`),
`src/lance_etl/reconciler/service.py:404-422` (`execute_isolated`),
`src/lance_etl/reconciler/cli.py:93-102` (`run_loop`),
`src/lance_etl/reconciler/README.md:115-133`,
raise sites in `src/lance_etl/state/repository.py` (`complete_ingest` -> `assert_expectations_match`
at 1907, `completed_ingest_matches` at 1989, `enqueue_publish_work` at 2059-2060,
`publish_dataset` -> `validate_live_publication` at 2344-2354 and
`validate_completed_publication` at 2437-2462).

**Defect.** `execute_isolated` wraps only `executor.execute(claim)`. The subsequent
`self.results.reconcile(result)` call is unwrapped, and `ResultReconciler.reconcile` has real raise
paths: `StateTransitionError` from every divergent-replay validation in the repository, and
`ValueError` from the `required_*` narrowing helpers on a malformed result. Any such exception
escapes `BoundedDispatcher.run`, abandons the remaining claims in the batch (their rows stay
`RUNNING` until lease expiry), and kills the `run` loop entirely (`run_loop` catches only
`KeyboardInterrupt`). The reconciler README explicitly claims per-dataset failure isolation
(ADR 0035) and that a stale transition "returns False ... rather than raising" — that is true only
for the fence-moved case, not for the divergence-detected case.

**Failure scenario.** Any persistent state divergence on one dataset (a replayed completed INGEST
whose digest differs after a PostgreSQL restore, a manually edited work row, a publication replay
whose evidence differs) makes the looping reconciler crash on every cycle. One poisoned dataset
wedges the entire local reconciler, with no bounded error evidence persisted on the offending row,
and every co-claimed dataset's work is delayed by a full lease expiry (default 15 minutes) per
crash.

**Fix direction.** In `BoundedDispatcher.run`, wrap the `self.results.reconcile(result)` call per
claim. On `StateTransitionError`, call `repository.block_work(claim, "STATE_DIVERGENCE",
bounded message)` (best effort — it may itself return `False` on a moved fence) and count the claim
in a new or existing summary bucket, then continue with the next claim. On any other exception, log
and count, then continue. Update `reconciler/README.md` to describe the actual boundary.

**Acceptance criteria.** A unit test that stubs the repository so `complete_ingest` raises
`StateTransitionError` for claim 1 of a 2-claim batch, and asserts that (a) `run()` returns
normally, (b) claim 2 was still reconciled, and (c) claim 1's row received a durable BLOCKED
transition attempt. Existing suite stays green: `.venv/bin/pytest -m "not integration"`.

**Blast radius.** 2 files plus tests (`reconciler/service.py`, `reconciler/README.md`, one test
module). No schema change.

### PR-02 — Real failures reported as benign "skipped": invisible to `count_failed` and exit codes

**Files**: `src/lance_etl/indexing/runner.py:416-420`,
`src/lance_etl/maintenance/job.py:405-412`, `src/lance_etl/maintenance/job.py:655-664`,
`src/lance_etl/fanout.py:66-88` (`dataset_result_failed` / `count_failed`),
consumers `src/lance_etl/indexing/cli.py:87`, `src/lance_etl/maintenance/cli.py:122,146,179`,
`src/lance_etl/cliutil.py:493-499`, and `src/lance_etl/migrate_namespace.py:630-655` with
`src/lance_etl/tools/cli.py:233-236`.

**Defect.** `dataset_result_failed` decides failure solely by the presence of an `"error"` key.
Three except blocks catch genuine failures but return a `"skipped"`-shaped marker with no
`"error"` key:

1. `indexing/runner.py:416-420` — the dataset cannot be *opened* (`FileNotFoundError` / `OSError`
   / `ValueError`) -> `{"uri": ..., "indexes": [], "skipped": str(exc)}`.
2. `maintenance/job.py:655-664` — same open failure in the compaction planner ->
   `{"uri": ..., "skipped": ..., "bytes_removed": 0}`.
3. `maintenance/job.py:405-412` — the configured `ts`/deleted column is missing from the schema (a
   contract violation, not "nothing to do") -> retention silently skipped.

The operator fleet CLIs therefore exit `0` after an entire fleet run in which datasets were
unreadable. Separately, `migrate_namespace.py:645-654` computes `compacted =
len(MaintenanceJob(...).run(...))` and `indexed = len(LanceIndexer(...).run(...))` — `len()`
counts error-marker and skip-marker datasets as successes, and `tools/cli.py` returns `None`
(exit 0) without ever applying `count_failed`. The reconciler path is *not* silently corrupted by
this (an unopenable candidate later fails loudly at `candidate_version`), so the blast is confined
to the operator library CLIs and the migrate report, but there the run reports success on real
failure.

**Fix direction.** Split the marker vocabulary: keep `"skipped"` strictly for genuine no-work
outcomes (`dataset.skipped_no_work`, "nothing to compact"), and change the three sites above to
return `{"error": ..., "phase": "open"}` (or `"phase": "retention-config"`) so
`dataset_result_failed` counts them. In `migrate_namespace.optimize`, count successes as
`len(results) - count_failed(results)` and surface the failed count on `MigrateReport`. Have
`tools/cli.py run_migrate_namespace` return the failed count so `run_cli_main` maps it to
`EXIT_PARTIAL_FAILURE`.

**Acceptance criteria.** Unit tests asserting: a fleet run over one nonexistent URI yields
`count_failed(...) == 1` for both `LanceIndexer.run` and `MaintenanceJob.run`, a
missing-`ts`-column dataset yields a counted failure, and the maintenance/indexing CLIs exit `3`.
`migrate_namespace` report test asserting failed datasets are not counted as compacted/indexed.

**Blast radius.** 5 files plus tests (`indexing/runner.py`, `maintenance/job.py`,
`migrate_namespace.py`, `tools/cli.py`, possibly `fanout.py` docstring), no schema change.

---

## MINOR

### PR-03 — Ingest runner's catch-all misclassifies non-contract errors as terminal blocks

**Files**: `src/lance_etl/reconciler/workers.py:187-238` (`DistributedIngestRunner.run`).

**Defect.** The single `try` spans the scan, validation, digest, marker probe, terminal write, and
marker finalize phases, and `except (AnalysisException, ValueError)` converts anything caught into
`blocked_result(..., "SOURCE_PROFILE_VIOLATION", ...)` — a terminal BLOCKED state requiring
operator repair. Only the scan and validation phases can legitimately produce a *contract*
violation. A driver-side `ValueError` from any later phase (or an `AnalysisException` caused by a
transient catalog problem during job submission) is permanently blocked under a misleading code
instead of retried.

**Fix direction.** Narrow the `try` to end after `select_profile_fields` /
`normalize_terminal` construction plus the null-`record_id` and conflict checks. Let exceptions
from `compute_source_digest`, `applied_completion_marker`, `write_terminal`, and `finalize_marker`
propagate to `execute_isolated`, which already converts them into `UNEXPECTED_WORKER_FAILURE`
retries with bounded attempts.

**Acceptance criteria.** Unit test injecting a `ValueError` from the write phase and asserting the
result kind is RETRY, not BLOCKED. Existing `SOURCE_PROFILE_VIOLATION` tests stay green.

**Blast radius.** 1 file plus tests.

### PR-04 — Fuzz oracle never exercises resurrection or double-delete

**Files**: `bench/fuzz_workload.py:432-501` (`do_insert`, `do_update`, `do_delete`),
`bench/fuzz.py` (verification consumes the same oracle).

**Defect.** `do_delete` removes the key from `alive` permanently, `do_insert` always allocates a
fresh key (`allocate_key` via `next_ordinal`), and `do_update` draws only from `alive`. Two real
production paths are therefore never covered by the oracle: (a) *resurrection* — an upsert for a
previously tombstoned `record_id` at a higher source sequence must flip `is_deleted` back and
replace the payload through the `when_matched_update_all` watermark path in
`etl/replay_sink.py:46-56`, and (b) *re-delete* — a second tombstone over an existing tombstone.
The fuzz e2e being green on two seeds says nothing about these paths.

**Fix direction.** Add a `revive` scenario: with a small probability, `do_insert` (or a new
`do_revive`) picks a previously deleted key (track a `dead` set), emits an upsert with a bumped
`payload_version`, and returns the key to `alive`. Add a `redelete` scenario drawing from `dead`.
The oracle already keys terminal state by `record_id`, so no oracle change is needed beyond the
generator emitting the ops.

**Acceptance criteria.** `python -m bench fuzz` green on two seeds with the new scenarios present
in `op_scenarios` counts, and at least one revived key asserted live with the regenerated payload
in `fuzz.json` verification evidence.

**Blast radius.** 1-2 files (`bench/fuzz_workload.py`, possibly `bench/fuzz.py` knobs/docs).

### PR-05 — Alembic downgrade leaves `require_draft_spec_revision` behind, breaking re-upgrade

**Files**: `migrations/versions/0001_control_plane.py:1137` (creation) and `:1288-1303`
(`downgrade`).

**Defect.** `create_lifecycle_triggers` creates five functions. `downgrade` drops only four —
`require_draft_spec_revision(uuid)` is missing from the drop list. After `alembic downgrade base`,
a subsequent `alembic upgrade head` fails on `CREATE FUNCTION require_draft_spec_revision` with
"function already exists" (the migration uses `CREATE FUNCTION`, not `CREATE OR REPLACE`).

**Fix direction.** Add `op.execute(sa.text("DROP FUNCTION IF EXISTS
require_draft_spec_revision(uuid)"))` to `downgrade`.

**Acceptance criteria.** Against `LANCE_ETL_TEST_DATABASE_URL`: `alembic upgrade head`, `alembic
downgrade base`, `alembic upgrade head` all succeed in sequence.

**Blast radius.** 1 file.

### PR-06 — `lance-etl-reconcile status` exits 0 when unhealthy

**Files**: `src/lance_etl/reconciler/cli.py:204-207`, `src/lance_etl/reconciler/service.py:483-523`
(`evaluate_slo`).

**Defect.** `main` prints the `SloStatus` JSON and returns `0` unconditionally. A `status`
invocation reporting `healthy: false` (blocked work, over-budget queue or retention age) still
exits `0`, so any shell-level health check or cron wrapper sees success. The
`reconciler.healthy` gauge does exist, but the process boundary lies.

**Fix direction.** For the `status` command only, return a nonzero exit (suggest `3`, matching the
fleet partial-failure convention) when `healthy` is false. Keep `run`/`run-once` semantics
unchanged (retries are normal operation there).

**Acceptance criteria.** CLI test asserting exit code for a stubbed unhealthy status, and exit 0
for healthy.

**Blast radius.** 1 file plus tests.

### PR-07 — `bench search` headline status hardcoded to MEASURED over a failed load leg

**Files**: `bench/search.py:589-596` (`run_search_against_endpoint`),
`bench/search.py:447-473` (`run_load_leg` can return `status: "FAILED"`).

**Defect.** The top-level phase document is built with `"status": "MEASURED"` unconditionally. A
load leg in which every concurrency level was rejected is disclosed only inside `result["load"]`.
The standalone `bench search` phase then exits `0` (its `cli.py` returns 1 only on exceptions).
The e2e path is honest (it raises on a FAILED search leg) — only the standalone phase misreports.

**Fix direction.** Derive the headline status from the legs (`"FAILED"` when the load leg failed,
or when the sweep produced no measurements), and raise or return nonzero from the phase when the
headline is FAILED, mirroring `run_e2e_body`.

**Acceptance criteria.** Unit test on the aggregation function: fully rejected load levels produce
a top-level FAILED and a nonzero CLI exit.

**Blast radius.** 1 file plus tests.

### PR-08 — Bench `drain_reconciler` declares quiescence while retries are still pending

**Files**: `bench/reconcile.py:633-673`.

**Defect.** The drain exits when one cycle enqueues 0 snapshots and claims 0 items. Work sitting in
`RETRY_WAIT` with `next_attempt_at` in the future (default retry base delay is 30s,
`state/settings.py:29`) is claimable later but invisible to that predicate, so a transient failure
makes the drain return "quiescent" totals and the downstream verification then fails with
misleading missing-data evidence instead of "work still retrying".

**Fix direction.** Also consult `repository.control_plane_status()`: treat the queue as quiescent
only when `retry_wait_work == 0` and `due_work == 0` (pending/blocked handling unchanged —
blocked already raises by default). When retries are pending, sleep briefly and continue the cycle
loop instead of returning.

**Acceptance criteria.** Unit test with a stub application whose first cycle reports a retry and
zero claims, asserting the drain keeps cycling until the retry resolves or the cycle bound raises.

**Blast radius.** 1 file plus tests.

### PR-09 — Small resource-lifecycle gaps in bench

**Files**: `bench/grpc_client.py:89-111` (`open_stub`), `:126-149` (`open_ready_stub`),
`bench/search_server.py:231` (log file opened before the `try`), `bench/reconcile.py:187`
(`admin_engine` created before its `try`), `bench/prepare.py:208` (Spark session created before
its `try`).

**Defect.** `open_stub` closes the channel on the readiness-timeout path but returns only the stub
on success, so no caller can ever close the underlying `grpc.insecure_channel` — one orphaned
channel per search phase, reclaimed only at process exit. The other three are narrow windows where
a resource is created immediately before its `try/finally` and leaks only if the next one or two
statements raise.

**Fix direction.** Return `(channel, stub)` from `open_stub` (or a small context manager) and close
in the callers' `finally` (`bench/search.py:505-511` region, `bench/e2e.py:347` region). Move the
three pre-`try` creations inside their `try` blocks or add a guard `try`.

**Acceptance criteria.** `python -m bench e2e --dataset sift1m` still green. Grep shows no
`insecure_channel` without a paired close.

**Blast radius.** 4 files.

### PR-10 — Rust: silent score default and index-probe failure conflation

**Files**: `rust/search-api/src/lance/backend.rs:742` (`rows_to_hits`),
`:504-506` and `:536-538` (`dataset_has_vector_index` / `dataset_has_fts_index`),
`rust/search-api/src/telemetry/recall.rs:400`.

**Defect.** (a) A hit row whose score column is missing or non-numeric silently ranks with
`score = 0.0` while the adjacent `record_id` extraction correctly errors — a malformed score
becomes a mis-ranked real result. (b) `let Ok(metas) = dataset.load_indices().await else { return
false; }` conflates an index-metadata *load failure* with "dataset has no index", silently turning
off the `fast_search` default (performance degradation with no counter or log). (c) recall capture
falls back to `"[]"` on serialization failure, which would record zero recall for a served query
(near-impossible in practice, noted for completeness).

**Fix direction.** (a) Make a missing/non-numeric score an `Err(SearchError::internal(...))`, same
as `record_id`. (b) Log a warning and emit a counter (for example reuse `cache.backend_errors`
style: a new `index_probe_errors`) when `load_indices` fails, keeping the `false` return as the
degrade path.

**Acceptance criteria.** `cargo test --locked` green with a new unit test feeding a scoreless row
into `rows_to_hits` and asserting an internal error.

**Blast radius.** 2 files.

### PR-11 — Stale "authenticated" language contradicting the plaintext-only reality

**Files**: `docs/etl-data-flow.md:339` ("Optional authenticated search legs..."),
`rust/search-api/src/domain/prewarm.rs:107`, `rust/search-api/src/lance/provider.rs:107`,
`rust/search-api/src/lance/provider.rs:709`.

**Defect.** Four doc/doc-comment sites still describe the search legs or the exact-prewarm path as
"authenticated". The transport is now plaintext and unauthenticated everywhere
(`grpc/admin.rs:1`, `main.rs:96-128`, root and rust AGENTS.md), and the bench clients are
unconditionally `grpc.insecure_channel`. Word-level drift, but it survived the doc refresh and will
mislead the next reader about a security property the system does not have.

**Fix direction.** Reword all four sites to "replica-local" / "unauthenticated loopback". No code
change.

**Acceptance criteria.** `grep -rn "authenticated" docs/ rust/search-api/src | grep -v -i
"unauthenticated"` returns only Iceberg writer-provenance and object-store credential contexts.

**Blast radius.** 4 files, comments only (rust doc comment edits need `cargo fmt` + clippy re-run).

### PR-12 — Operator-doc drift: broken runbook query, Airflow remnants, incomplete Rust layout map

**Files**: `docs/production-release.md:200-206`, `src/lance_etl/state/repository.py:1523`,
`rust/search-api/AGENTS.md:47-49` (layout section).

**Defect.** (a) The production-release runbook's `psql` query selects `airflow_ctx_dag_id`,
`airflow_ctx_dag_run_id`, `airflow_ctx_task_id`, `airflow_ctx_map_index`, `airflow_ctx_try_number`
— columns that no longer exist in `dataset_work` (verified against `migrations/versions/
0001_control_plane.py` and `state/tables.py`). An operator pasting it gets a SQL error.
(b) `repository.py:1523` docstring still says "Validated local or Airflow launch provenance" while
`WorkLauncherKind` (`state/types.py:97-100`) has exactly one variant, `LOCAL`. (c) The rust
`AGENTS.md` layout omits `src/grpc/admin.rs`, `src/grpc/admission.rs`, `src/grpc/timeout.rs`, and
`proto/lance_etl/internal/v1/admin.proto`, all of which exist and carry normative behavior (the
admin service rides the main port 8080).

**Fix direction.** Trim the runbook query to live columns, fix the docstring, add the four missing
entries to the layout map with one-line descriptions.

**Acceptance criteria.** The runbook `psql` query executes against a migrated
`lance_etl_test` schema. `grep -rn airflow src/ docs/` returns only the historical-context line in
`docs/iceberg_to_lance_project_state.md`.

**Blast radius.** 3 files, docs/docstrings only.

### PR-13 — `SEARCH_API_PORT=8081` collides with the fixed health port without a config guard

**Files**: `rust/search-api/src/config.rs:25-29` (`DEFAULT_PORT` 8080, fixed
`DEFAULT_HEALTH_PORT` 8081), `:344` (env override), `rust/search-api/src/main.rs:105-107,126-127`.

**Defect.** The health port is a fixed constant while the search port is env-overridable. Setting
`SEARCH_API_PORT=8081` makes the second bind fail with a bare address-in-use error at startup —
loud, but confusing, and nothing in `Config::from_env` names the actual conflict.

**Fix direction.** In `Config::from_env`, reject `port == DEFAULT_HEALTH_PORT` with an explicit
message.

**Acceptance criteria.** Config unit test asserting the rejection message.

**Blast radius.** 1 file plus a test.

---

## SIMPLIFICATION

### PR-14 — `ResultKind.PHASE_ADVANCED` has no producer: dead result path

**Files**: `src/lance_etl/reconciler/results.py:16,27,74-75`,
`src/lance_etl/reconciler/service.py:292-306` (reconcile branch), `:397`
(`DispatchSummary.advanced`), `src/lance_etl/reconciler/README.md:127-129`,
`bench/reconcile.py` drain totals key `"advanced"`.

**Defect.** No code anywhere in `src/`, `bench/`, or `tests/` constructs a `WorkResult` with
`ResultKind.PHASE_ADVANCED` (verified by grep — only the enum member, its validation clause, the
reconcile branch, and the summary counter exist). `ConfiguredPublicationRunner` checkpoints phases
through `advance_publish_phases` on the PUBLISH_SUCCEEDED path instead. The branch, the
`next_phase` result field, the `advanced` counter, and the README paragraph describing the
"zero-delay `retry_work`" flow are maintenance surface for a flow that cannot occur.

**Fix direction.** Delete the enum member, the `next_phase` field and its validation, the reconcile
branch (and `required_phase`), the `advanced` counter from `DispatchSummary` and the bench totals,
and the README sentence. Alternatively, if incremental phase checkpointing from workers is a wanted
future behavior, wire a producer — but today it is dead either way.

**Acceptance criteria.** `grep -rn PHASE_ADVANCED src bench tests` empty, pytest green, bench e2e
drain totals schema updated consistently.

**Blast radius.** 4 files plus tests referencing `DispatchSummary` field order.

### PR-15 — Dead `ClusterReader` surface in the Rust service

**Files**: `rust/search-api/src/domain/clusters.rs` (trait plus types),
`rust/search-api/src/lance/index_reader.rs` (`impl ClusterReader` at 121, metric emission at
134-135), `rust/search-api/src/telemetry/metrics.rs:660-677` (`clusters_read`,
`clusters_centroids`), `rust/search-api/src/telemetry/mod.rs:68-71`, layout entries in
`rust/search-api/AGENTS.md:27,45`.

**Defect.** The proto surface has exactly four RPCs (`VectorSearch`, `TextSearch`, `HybridSearch`,
`PrewarmExact` — verified in `proto/`). Nothing in `grpc/`, the integration tests, or the bench
calls `ClusterReader::read_clusters`. The bench "clusters" leg computes cluster hit rates from
prepared artifacts in Python (`bench/search.py:250-271`), not through the service. The trait, its
impl, the two metrics, and the telemetry docs describe an RPC that was removed.

**Fix direction.** Delete `domain/clusters.rs`, `lance/index_reader.rs`, the two metric methods and
their tests, the `mod.rs` re-exports, and the doc/layout mentions. Re-run `cargo clippy --locked
-- -D warnings` and `cargo test --locked`.

**Acceptance criteria.** `grep -rn "ClusterReader\|clusters_read\|clusters_centroids"
rust/search-api/src` empty, cargo gates clean.

**Blast radius.** 5-6 Rust files plus 2 doc files.

### PR-16 — `WorkProvenance` plumbs a single-variant enum through the claim path

**Files**: `src/lance_etl/state/types.py:97-120`, `src/lance_etl/state/repository.py:1407-1429,
1506-1560` (`provenance` parameters), `src/lance_etl/reconciler/service.py:364` (dispatcher
field).

**Defect.** `WorkLauncherKind` has exactly one member (`LOCAL`), so `WorkProvenance`, its
`validate()`, and the `provenance` parameters on `claim_due_work` / `claim_locked_row` /
`BoundedDispatcher` plumb a constant. Hard rule 10 keeps the `launcher_kind` *column* as an audit
label, and this proposal keeps it: only the Python plumbing collapses to stamping the constant
`WorkLauncherKind.LOCAL.value` inside `claim_locked_row`.

**Fix direction.** Remove `WorkProvenance` and the `provenance` parameters, stamp the constant at
the single write site, keep the enum and column untouched. Do this only if the team has no near-term
plan for a second launcher kind — otherwise leave as is (this is a judgment call, not a defect).

**Acceptance criteria.** pytest green, `dataset_work.launcher_kind` still written as `LOCAL` (assert
in an existing claim test).

**Blast radius.** 3 files plus tests.

---

## Verified clean (no action needed — checked in depth at this commit)

- **Lease/fence SQL** (`state/repository.py`): claiming bumps the dataset `fence_epoch` so any
  zombie holder of the same dataset is invalidated. Lane ordering (`lane_order_predicate`) plus
  the same-seq INGEST-before-PUBLISH kind rank means at most one claimable item per dataset, so the
  fence bump can never kill legitimately concurrent work. `release_expired_leases`,
  `sweep_stale_work` (stale non-ingest rows made visible as BLOCKED), and the terminal transitions'
  locked re-checks (`lock_work_and_state` -> `locked_lease_matches` before any expectation assert)
  are ordered correctly. Idempotent replay of `complete_ingest` and `publish_dataset` validates
  full evidence equality before returning success.
- **Completion marker monotonicity** (`etl/completion.py`): the probe/finalize race against a
  zombie executor is safe because concurrent `UpdateConfig` commits on the same keys are a commit
  conflict in lance 8 (verified in the checkout:
  `rust/lance/src/dataset/transaction.rs` `get_upsert_config_keys`/`upsert_key_conflict`, enforced
  by `io/commit/conflict_resolver.rs`), and `commit_with_retries` re-reads before each attempt, so
  the loser re-observes the newer window and no-ops. The ambiguous-failure path re-reads and
  accepts only a desired marker.
- **Replay-safe merge** (`etl/replay_sink.py`): per-key source-sequence watermark condition,
  same-sequence digest-conflict detection both pre- and post-merge, post-merge reconciliation
  verification, tombstone payload-clearing validation, and the create-race fallback in
  `open_or_create_replay_dataset` are all correct. The marker-first probe in
  `applied_completion_marker` correctly prevents re-merging a window that maintenance may have
  physically pruned.
- **Commit-retry layering** (`telemetry.py`): the marker set excludes `TooMuchWriteContention` and
  `IncompatibleTransaction`, so the wrapper never stacks on Lance's inner retry loop. Verified
  against the checkout's `write/retry.rs` behavior description.
- **Publication qualification**: pin-first checkpointing makes the publish path idempotent across
  crashes (pin resolution short-circuits maintenance/indexing on retry), counts are computed at the
  pinned version, `required_unindexed_fragments` hard-fails on a missing coverage key rather than
  assuming zero, and `resolved_actual_index_kind` / `observed_index_type` handle the pylance 8
  FTS `Unknown` quirk in a data-driven way.
- **Rust service lifecycle**: all spawned tasks (search, health, readiness, janitor, redis registry
  hygiene) hold owned handles and are aborted on every shutdown path (`main.rs`,
  `provider.rs::Drop`). All Moka caches are capacity-bounded, the handle LRU is weight-bounded with
  per-version keys aging out by capacity/TTL, and the disk prefix registry prunes swept
  directories. The store byte cache never caches `_latest.manifest`, `latest_version_hint.json`,
  or tag JSON, and the negative-open cache stores only definitive absences.
- **Admission and timeouts**: all three public RPCs pass through the admission controller and the
  per-route timeout layer, `PrewarmExact` gets the long budget plus its own single-permit
  semaphore, and deadline expiry emits the standard rpc metrics.
- **Config surface**: every env var read in `rust/search-api/src/config.rs`,
  `state/settings.py`, and `reconciler/config.py` has live consumers (no dead knobs), and the four
  retired `SEARCH_API_*` vars are explicitly tested as ignored.
- **Docs vs code counts**: 9 tables, 3+1 RPCs, ports 8080/8081/8125, `lance-etl-reconcile` as the
  only installed script, and the fresh-clone bootstrap sequence in the root README all match the
  code.
- **Hard-rule compliance spot checks**: no `enable_stable_row_ids` anywhere in code, the only
  `create_scalar_index` call is the INVERTED path in `indexing/runner.py:693`, no raw SQL in the
  Rust gRPC/domain layers, and `uvx ruff format --check` plus `uvx ruff check` pass at HEAD.
- **Bench exit-code spine**: `bench/cli.py` returns 1 on any phase exception, `run_e2e_body` raises
  on failed publication verification and on a FAILED self-hosted search leg, and qualification
  raises on incomplete evidence (PR-07 is the one standalone-phase exception).
- **Spark/engine/subprocess lifecycle**: `run_with_spark`, the reconciler runtime construction
  failure path, `isolated_control_plane`, and `self_hosted_search_api` (DEVNULL/file stdout, no
  PIPE deadlock, terminate-then-kill escalation) are all finally-covered, apart from the narrow
  windows listed in PR-09.

## Open user decisions (product calls, not fixes)

1. **`PROD_READINESS_INSTRUCTIONS.md` (repo root) is a completed, stale plan — recommend
   deletion.** It was written 2026-07-19 against `b54f1c1` and every actionable phase has shipped
   (9-table schema, `record_id`/`ts` renames, `pipeline/` removal, `docs/etl-data-flow.md`, dead
   Rust items, `.dockerignore`). It also predates the TLS/auth removal, so its remaining prose
   actively contradicts the current tree (14/12-table counts, `vector_id`, TLS-era config
   discussion). Nothing in it is still load-bearing. Per repo convention the user handles git, so
   deletion is left to you.
2. **Stray `etl/venv/` directory at the repo root.** Gitignored, left over from the pre-overhaul
   era, and the old plan explicitly reserved its deletion to the user. Delete when convenient.
3. **`bench/LEDGER.md` historical rows** (rows 15-16) describe a credential-gated search leg and
   CLI flags that no longer exist. The ledger is append-only history, so the recommendation is to
   leave it untouched — noted here only so future reviews do not re-flag it.
4. **PR-16 (WorkProvenance collapse)** is contingent on whether a second launcher kind is planned.
5. **`status` exit-code semantics (PR-06)**: if you prefer "status is informational, exit 0 always",
   reject PR-06 and document the convention in `reconciler/README.md` instead.
