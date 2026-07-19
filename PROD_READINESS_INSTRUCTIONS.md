# Simplification and production-readiness instructions

Instructions for finishing the simplification of lance-etl on branch `gs/f1`, fixing the known
issues, simplifying the PostgreSQL control-plane schema, and verifying the result is production
ready. Written 2026-07-19 against commit `b54f1c1` (working tree clean, up to date with
`origin/gs/f1`).

Baseline verified at the time of writing: `pytest -m "not integration"` passes (844 passed, 12
skipped), `uvx ruff check` and `uvx ruff format --check` are clean, `cargo check` and
`cargo clippy -- -D warnings` are clean. Exactly one Rust test fails (see Phase 1). The big
structural removals (Airflow DAGs, `.github/workflows/`, `deploy/`, `containers/`, migration
`0002`) are already committed and nothing in live code references them. Do not redo that work.

## Ground rules

1. Read `AGENTS.md` in full first. All of its hard rules apply to every change below, especially:
   no `#` inline comments, no leading-underscore names, docstrings and full type hints everywhere,
   run `uvx ruff format` and `uvx ruff check` on `src/ tests/ bench/ migrations/` after every
   Python change, no prose semicolons in Markdown, and the segment-API index recipes.
2. Do not commit or stage anything. Leave the git tree for the user to review and commit.
3. Breaking changes are fine. No backward-compatibility shims, no deprecation paths, no dual-read
   code. This includes the database schema — the control plane is a local PostgreSQL that can be
   recreated from scratch.
4. Never add `enable_stable_row_ids` in any form (ADR 0010, rejected not deferred). Never add JSON
   configuration blobs to the control plane. Never propose cross-org or shared-dataset designs.
5. Do not restore anything under `claude/`, `deploy/`, `containers/`, or `.github/workflows/`.
   `tests/test_release_assets.py::test_reconciler_release_contract_is_local_first` asserts the
   absence of `containers/` and `deploy/` — keep that test passing.
6. Do not delete or move `docs/iceberg_to_lance_project_state.md` without an explicit instruction
   from the user. Update 2026-07-19: the user has since deleted `report_sol.md` themselves and
   explicitly ordered the deletion of `market-research/` (already executed). Any remaining
   references to `market-research/` or `report_sol.md` in tracked docs are now dangling and must
   be removed (see Phase 5.3).

## Phase 1 — Fix known issues

### 1.1 Fix the one failing Rust test

`rust/search-api`: `cargo test` fails deterministically on
`lance::error::tests::emitted_logs_never_contain_raw_uri_or_engine_detail`
(`src/lance/error.rs:192`, assertion at line 211). The first assertion in the test (capturing the
`tracing::warn!` for `DatasetNotFound`) passes. The second `classify_lance_error` call — the
catch-all arm's `tracing::error!(error_class = "internal", ...)` — is not captured by the test's
`tracing_subscriber::fmt()` writer even though the test calls `rebuild_interest_cache()`. The
symptom pattern points at tracing callsite-interest caching or test-subscriber isolation, not at a
logic bug in `classify_lance_error` itself.

Steps: root-cause it (check whether another test registers a global default subscriber, whether
the `error!` callsite interest was cached under a different max level, and whether
`with_default` scoping covers the second call), fix either the test harness or the production
code, and confirm `cargo test` is fully green. Do not delete or `#[ignore]` the test — it guards
the URI-redaction contract, which is a real production property.

### 1.2 Remove dead Rust items

All three are unreferenced anywhere in `src/` or `tests/` (verified against all 281 `pub` items):

- `rust/search-api/src/config.rs:108` — `pub const DEFAULT_ID_COLUMN`
- `rust/search-api/src/lance/provider.rs:197` — `pub async fn with_telemetry`
- `rust/search-api/src/domain/fusion.rs:13` — `pub const DEFAULT_WEIGHTED_VECTOR_WEIGHT`

Delete them. Re-run `cargo check`, `cargo clippy -- -D warnings`, `cargo test`.

### 1.3 Delete the orphaned `.dockerignore`

No Dockerfile exists anywhere in the tracked tree and `compose.yaml` only pulls
`postgres:17-alpine`, so `.dockerignore` scopes a build context that no longer exists. It also
still lists `fable_report1.md`, a file that was never tracked and does not exist. Delete
`.dockerignore`.

### 1.4 Fix the stale CLI docstrings

`src/lance_etl/indexing/cli.py:4` claims its `main()` is consumed by a `lance-etl-index` script,
but `pyproject.toml` registers only `lance-etl-reconcile`. Fix that docstring to say the module is
an uninstalled operator CLI reachable via `python -m`. Check `etl/cli.py`, `maintenance/cli.py`,
and `pipeline/cli.py` for the same claim and align their module docstrings with how
`src/lance_etl/AGENTS.md` already describes `tools/` ("Uninstalled operator library CLI"). If
Phase 3.1 deletes the legacy cluster, skip the files it removes.

### 1.5 Annotate stale references in `market-research/` (superseded)

Superseded 2026-07-19: the user ordered `market-research/` deleted entirely, which has been done.
The original instructions below are kept only for the record.

Four files reference the deleted `airflow/` directory as if it still exists:
`market-research/optimization-recommendations.md:25`, `market-research/production-techniques.md:388`,
`market-research/use-cases.md:113` and `:205`, `market-research/knob-reduction-analysis.md:63`.
Do not rewrite the documents. Add the same three-line historical disclaimer that `report_sol.md`
already carries at its top (a note that the document is historical input and that Airflow,
Kubernetes, CI workflows, and remote deployment surfaces have since been removed) to each of the
four files. `market-research/` is exempt from the no-semicolon rule, but write the disclaimer
without semicolons anyway.

## Phase 2 — Simplify the PostgreSQL schema

The control plane is defined twice, deliberately and in exact sync: DDL in
`migrations/versions/0001_control_plane.py` and SQLAlchemy Core metadata in
`src/lance_etl/state/tables.py` (used by `migrations/env.py` for autogenerate). Every schema
change below must be made in both places, keeping constraint names identical, and then verified
with a fresh `alembic revision --autogenerate` producing an empty diff. Because migration 0001 is
the only migration and the database is local and recreatable, edit 0001 in place rather than
adding a new revision — this repo already squashed 0002 into 0001 the same way. After any schema
change, update the table list and count in `AGENTS.md` rule 10 and in
`docs/confluence-architecture-overview.md` (both currently say "exactly 14 tables").

### 2.1 Merge the two index-option tables into `index_definitions` (14 → 12 tables)

`vector_index_options` (11 IVF_RQ columns) and `fts_index_options` (4 INVERTED columns) are both
strictly 1:1 with `index_definitions`, so the merge needs no JSON and does not violate the no-blob
rule. Steps:

1. In `0001_control_plane.py` and `tables.py`: add the 11 vector columns and 4 FTS columns to
   `index_definitions` as nullable. Replace the two per-table type CHECKs
   (`ck_vector_index_options_type`, `ck_fts_index_options_type`) with CHECK constraints on
   `index_definitions` of the form: `index_type = 'IVF_RQ'` implies the vector columns are
   non-null and the FTS columns are null, `index_type = 'INVERTED'` implies the reverse, and
   scalar types imply all option columns are null. Carry over the per-column value CHECKs.
2. Drop the two tables, their composite FKs
   (`fk_vector_index_options_definition_type`, `fk_fts_index_options_definition_type`), and the
   two lifecycle triggers `enforce_vector_options_lifecycle_trigger` and
   `enforce_fts_options_lifecycle_trigger`. The `enforce_spec_option_lifecycle()` plpgsql function
   loses both call sites — delete it too. The existing `enforce_index_definitions_lifecycle_trigger`
   already freezes non-DRAFT `index_definitions` rows, which now covers the option columns.
   Keep `uq_index_definitions_id_type` and `uq_index_definitions_revision_id_type` only if
   something still depends on them after the FKs are gone — otherwise drop them.
3. Update the seed data in `0001_control_plane.py` (the `op.bulk_insert` rows built from
   `DEFAULT_INDEX_IDS`) to write options inline on the `index_definitions` rows.
4. Rewrite the Python plumbing: `vector_option_row_values` (`state/repository.py:396`),
   `fts_option_row_values` (`repository.py:427`), the two option INSERTs in
   `create_draft_spec_revision` (`repository.py:663-672`), `decode_vector_options`
   (`state/specs.py:1374`), `decode_fts_options` (`specs.py:1397`), and `option_rows_by_index`
   (`specs.py:1414`, currently a fan-out join across the two tables) all collapse into
   single-table reads and writes on `index_definitions`. Keep the `VectorIndexOptions` and
   `FtsIndexOptions` Python dataclasses as the decoded shapes — only their storage changes.
5. Downstream consumers (`reconciler/workers.py:1102-1125`, `indexing/` config) read the decoded
   dataclasses, not the tables, and should need no change. Verify anyway.
6. Update the affected tests (`tests/test_state_postgres.py`, `tests/test_state_specs.py`) and
   the 14-table language in `AGENTS.md` and the architecture overview doc.

### 2.2 Drop the `airflow_ctx_*` provenance columns from `dataset_work`

The five columns (`airflow_ctx_dag_id`, `airflow_ctx_dag_run_id`, `airflow_ctx_task_id`,
`airflow_ctx_map_index`, `airflow_ctx_try_number`) are written at claim time
(`state/repository.py:1578-1582`) and read back by nothing anywhere in the codebase. Airflow
itself is fully removed. Drop:

- the five columns and their CHECKs in `0001_control_plane.py` and `tables.py`
- `WorkLauncherKind.AIRFLOW` and the provenance capture fields in `state/types.py:110-183`
- the write path in `repository.py:1578-1582`
- the tests covering them (`tests/test_state_postgres.py:622-658`, `tests/test_state_types.py:80-95`)
- every mention of `AIRFLOW_CTX_*` provenance in `AGENTS.md` (rule 10 and the launch-provenance
  paragraph), `src/lance_etl/AGENTS.md:100`, `src/lance_etl/README.md:70-72`, `README.md:37-38`,
  `docs/adr/postgresql-dataset-control-plane.md`, `docs/confluence-architecture-overview.md:107-109`,
  and `docs/production-release.md:163-165`

If a generic launcher label is still wanted for audit, keep the single `launcher_kind` column with
its remaining enum values and drop only the five Airflow-shaped columns.

### 2.3 Give the seed constants a single source of truth

The deterministic seed UUIDs and the configuration digest are hand-duplicated in three places:
`migrations/versions/0001_control_plane.py:21-43`, `src/lance_etl/state/specs.py:39-65`, and a
hardcoded digest literal in `tests/test_state_specs.py:226`. Make
`0001_control_plane.py` import the constants from `lance_etl.state.specs` (migrations already
import `lance_etl.state.tables` in `env.py`, so the dependency direction is established), and
change the test to assert the digest of `production_default_spec_revision()` equals the constant
it ships with rather than a copy-pasted hex string. Result: changing the production default spec
in one place breaks nothing silently.

### 2.4 Schema changes NOT to make

These were evaluated and rejected — do not "simplify" them:

- Do not fold `publication_indexes` into `dataset_publications`. It is a genuine N:1 relation
  (one row per index per publication) and merging would require a forbidden JSON column.
- Do not drop `distinct_row_count` / `distinct_live_row_count` from `dataset_publications` or
  `unindexed_fragment_count` from `publication_indexes`. Their equality and zero CHECKs are the
  stored proof that publications have no duplicate `vector_id`s and full index coverage.
- Do not remove the lifecycle triggers that remain after 2.1. They are deliberate
  defense-in-depth behind the repository methods, catching raw-SQL or future-code bypasses.

### 2.5 Verify the schema work

Against a fresh database (`docker compose up -d postgres`): run `lance-etl-reconcile migrate`,
then `alembic revision --autogenerate` and confirm an empty diff, then run the full pytest suite.
The Postgres-backed tests in `tests/test_state_postgres.py` must pass against the new shape.
This verification applies again after 2.6.

### 2.6 Deeper consolidation to 9 tables (user-directed, added 2026-07-19)

The user has asked for fewer tables than the 12 that 2.1 produces and has authorized breaking
changes everywhere, including the Rust service. Three further reductions, none of which needs a
JSON column:

1. Merge `dataset_state` into `datasets` (12 to 11). Move `materialized_spec_revision_id`,
   `last_applied_source_snapshot_seq`, `ingest_lance_uri`, `ingest_lance_version`,
   `active_publication_id`, and `fence_epoch` onto `datasets` and drop `dataset_state`. The
   original write-contention justification for the split does not hold for a single local
   reconciler process. Repoint every `dataset_state.c.*` call site in
   `src/lance_etl/state/repository.py` (about 15) and remove the three-table join in
   `resolve_serving_dataset`. CRITICAL cross-crate coupling: the serving-catalog SQL in
   `rust/search-api/src/catalog.rs` (`resolve_row`, the `const QUERY`) joins `datasets`,
   `dataset_state`, and `dataset_publications` — rewrite that query for the merged shape and keep
   it fully parameterized, then run the Rust test suite.
2. Merge `dataset_specs` into `dataset_spec_revisions` (11 to 10). `spec_id` becomes a plain
   non-FK UUID column on revisions, and `name` plus `description` are carried on every revision
   row (denormalized on purpose, grow-only). Keep the one-ACTIVE-per-spec partial unique index on
   `(spec_id) WHERE state = 'ACTIVE'`. `iceberg_sources.default_spec_id` stays as a plain UUID
   column, and the deferred constraint triggers that enforce "default spec has an ACTIVE
   revision" are rewritten against `dataset_spec_revisions`. `ControlPlaneRepository.create_spec`
   collapses into `create_draft_spec_revision` (a spec now exists only as its revisions).
3. Eliminate `reconciler_settings` entirely (10 to 9). It is a singleton loaded exactly once at
   process startup and already requires a restart to change, which makes it process bootstrap
   configuration, not durable state. Move the 14 tunables into the existing `ReconcilerSettings`
   dataclass in `src/lance_etl/state/settings.py`, sourced from environment variables (or CLI
   flags) with the current server defaults as code defaults, one-line docstring per field.
   Delete the table, its seed row, and `ControlPlaneRepository.reconciler_settings()`. Update
   AGENTS.md rule 10, which currently says settings live in the database.

Final table set (9): `dataset_spec_revisions`, `dataset_fields`, `index_definitions`,
`iceberg_sources`, `datasets`, `source_snapshots`, `dataset_work`, `dataset_publications`,
`publication_indexes`. Going lower requires JSON blobs, which remain forbidden. Update the table
list and count in AGENTS.md rule 10 and `docs/confluence-architecture-overview.md` to the final
9-table set, and re-run all of 2.5 (fresh migrate, empty autogenerate diff, full pytest, plus
`cargo test` for the catalog query change).

## Phase 3 — Simplify the project

### 3.1 Retire the legacy ETL/pipeline cluster (the largest win, ~4,500 LOC)

`src/lance_etl/etl/` (job, plan, sink, bulk, pivot, mutation, digest, completion, replay_sink,
cli) and `src/lance_etl/pipeline/` are labeled legacy in `src/lance_etl/AGENTS.md` and are never
imported by the production reconciler path. They are kept alive solely because `bench/e2e.py`
imports `PipelineConfig` / `PipelineJob`, plus four test modules (`tests/test_cli.py`,
`tests/test_pipeline.py`, `tests/test_poisoned_dataset.py`, `tests/test_fleet_idempotency.py`).

Do this in two steps, in order:

1. Port `bench/e2e.py` to drive the production path instead: register the source and spec through
   `ControlPlaneRepository` and run ingestion via the reconciler
   (`lance-etl-reconcile run-once` or the `reconciler.service` API) rather than constructing a
   `PipelineJob` directly. The benchmark must still produce the same evidence artifacts the
   release runbook consumes (`docs/production-release.md` binds release evidence to qualified
   bench runs — read it before touching bench). Verify with a real `python -m bench e2e` run,
   not just the bench unit tests.
2. Only after step 1 is proven: delete `src/lance_etl/etl/` and `src/lance_etl/pipeline/` and the
   four legacy test modules, update `src/lance_etl/AGENTS.md` and `src/lance_etl/README.md`
   module inventories, and re-run the full suite.

If step 1 turns out to be larger than expected, stop and report — shipping the schema and fixes
phases without this one is acceptable, deleting the cluster while bench still imports it is not.

Outcome update 2026-07-19: step 1 is done (bench drives the reconciler, proven by live
integration runs) and `pipeline/` plus the four legacy test modules are deleted. `etl/` turned
out to be partly production code: the reconciler imports `etl.digest`, `etl.mutation`,
`etl.completion`, `etl.pivot`, `etl.replay_sink`, and `etl.sink`. Revised plan: keep those as a
shared library, delete only `etl/job.py`, `etl/plan.py`, `etl/bulk.py`, and `etl/cli.py`, retire
the legacy bench commands (`ingest`, `index`, `compact`, `all`) that ride `IcebergToLanceETL`,
and remove the now-dead `bench/indexes.py::union_index_config`.

### 3.3 Fix the two publish-gate bugs the bench port surfaced (production blockers)

Both were empirically confirmed under the pinned pylance 8.0.0 and live in
`src/lance_etl/reconciler/workers.py`. They mean the bundled production default spec cannot
publish through the reconciler:

1. With `compaction_enabled=True`, the publish gate qualifies a candidate version that has no
   indexes present, and every publication then blocks with `INCOMPLETE_INDEX_COVERAGE`.
2. A fully built committed INVERTED index is rejected by the `index_kind_matches` gate because
   pylance 8.0.0's `describe_indices()` reports its type as `Unknown`. Any version-dependent
   handling must branch on `lance.__version__`, never on attribute probing.

The fix is proven when the production default spec revision publishes end-to-end through the
reconciler with compaction and the FTS index enabled.

### 3.2 Options to raise with the user (do not act unilaterally)

- `report_sol.md` (66 KB) and `docs/iceberg_to_lance_project_state.md` (2,337 lines) are
  self-disclaimed historical documents nothing references. Options: move both under
  `market-research/` as archived inputs, or delete them. Ask first — report files have been
  deleted out of scope before and that must not happen again.
- CI was deleted with the move to local-first operation, so there is currently no automated gate
  on pushes. If any CI is wanted, propose a single minimal workflow that runs exactly the Phase 6
  gates. Restoring the old ci/integration/release workflows is not wanted.
- The local `.venv` still contains `apache-airflow` packages that `uv.lock` no longer references,
  and a stray `etl/venv/` directory exists at the repo root (gitignored). Suggest the user re-run
  `uv sync` and remove `etl/venv/` if they no longer use it. Do not delete either yourself.

## Phase 4 — Rename the data-contract columns (user-directed, added 2026-07-19, do this LAST among code phases)

The user has ordered two source-contract renames. They cut across the Python package, the schema,
the Rust service, bench, tests, and docs, so they run after every other code phase (1 through 3)
and before the data-flow document (Phase 5) and final gates (Phase 6).

### 4.1 `vector_id` becomes `record_id`

`vector_id` is the unique row key of the whole system (the KEY role in `dataset_fields`, the
dedup contract behind the `distinct_row_count` CHECKs, the join key for all row-level
operations). Rename it to `record_id` everywhere: the seed field definitions and CHECK
constraints in `migrations/versions/0001_control_plane.py` and `src/lance_etl/state/tables.py`,
the spec constants in `state/specs.py`, every Python read/write path, the Rust service (grep the
whole `rust/` tree — proto field names, defaults, recall capture, docs), bench, tests, and every
prose mention in AGENTS.md files, README files, and `docs/`. Start with a repo-wide
`grep -rn "vector_id"` inventory and end with zero hits outside `docs/adr/` history sections
that are explicitly describing past decisions.

### 4.2 Three time columns become one `ts` (user decision 2026-07-19)

The source contract currently carries three time-like columns: `event_timestamp` (the EVENT_TIME
role, `iceberg_sources.event_time_column`, the Rust `DEFAULT_EVENT_TIMESTAMP_COLUMN`),
`processing_timestamp` (the Iceberg partition column, `hours(processing_timestamp)` in the
partition contract), and `ttl` (the TTL role, `iceberg_sources.ttl_column`, per-row expiry).
The user chose the full collapse: exactly ONE time column named `ts` remains.

1. `event_timestamp` and `processing_timestamp` merge into a single required `ts` column. Every
   read of either now reads `ts`. The Iceberg partition contract becomes
   `(tenant_id, namespace, org_id, hours(ts))`, updated in `source/contract.py`, the scan
   predicates in `source/scans.py`, the bench source writer, and everywhere else the partition
   spec is validated or constructed.
2. The `ttl` column and the TTL role are removed from the record contract. Retention and expiry
   derive from `ts` plus the retention policy on the spec revision instead of a per-row value.
   `iceberg_sources.ttl_column` is dropped, as is whatever `iceberg_sources` column names the
   old processing/partition timestamp, replaced by a single `ts_column` default `'ts'`.
3. Event-time last-write-wins collapse now orders on `ts`. If any ordering tie-break or
   correctness property genuinely depended on having two distinct timestamps, STOP and report to
   the orchestrator instead of guessing at semantics.

Renaming happens in the edited migration 0001 in place (breaking change, fresh database), with
`tables.py` kept in exact sync and the 2.5 verification loop re-run. After this phase, update the
recorded configuration digest expectations wherever tests assert them.

## Phase 5 — Write the ETL data-flow document

After Phase 4 lands (so names and schema are final), write ONE new file: `docs/etl-data-flow.md`.
It must explain, in detail and in prose a new engineer can follow, how a record travels through
the system: Iceberg source table and snapshot ledger, the reconciler loop claiming deterministic
`dataset_work` with leases and fence epochs, local Spark execution with all heavy work in
executors, the Lance `merge_insert` write path through `commit_with_retries`, index builds via
the segment API (vector, scalar, and FTS paths), publication with its evidence rows and
row-count invariants, retention, prewarm, and finally how the Rust search-api resolves a serving
dataset from the control plane and serves vector, text, and hybrid queries. Include one mermaid
sequence or flow diagram. Use the post-rename names (`record_id`, `ts`) and the final 9-table
schema. No prose semicolons. Link the relevant ADRs rather than restating them.

## Phase 6 — Production-readiness verification gates

Run all of these at the very end, after Phases 4 and 5. Every gate must pass before the work is
done:

1. `uvx ruff format src/ tests/ bench/ migrations/` — no reformats.
2. `uvx ruff check src/ tests/ bench/ migrations/` — exit 0.
3. `.venv/bin/python -m pytest tests/ -m "not integration" -q` — zero failures (baseline: 844
   passed, 12 skipped, before Phase 2/3 change the counts).
4. In `rust/search-api`: `cargo check`, `cargo clippy --all-targets -- -D warnings`, and
   `cargo test` — all clean, including the test fixed in 1.1.
5. Fresh-database check from 2.5: `lance-etl-reconcile migrate` on a clean Postgres, empty
   autogenerate diff, Postgres-backed tests green.
6. `tests/test_release_assets.py` still passes (local-first contract, exact version pins, and
   `docs/production-release.md` content assertions — if Phase 2 or 3 edited that doc, this test
   is the canary).
7. End-to-end: one `python -m bench e2e` qualification run per the release runbook, exercising
   the reconciler path (mandatory if 3.1 was done, recommended otherwise).
8. Grep gates: no `#` inline comments and no leading-underscore definitions in
   `src/ tests/ bench/ migrations/`, no prose semicolons in `README.md`, `AGENTS.md`,
   `CLAUDE.md`, or `docs/`, and no references to `airflow/`, `deploy/`, `containers/`, or
   `.github/workflows` outside self-disclaimed historical documents.
9. Post-rename grep gates: zero references to `market-research/` or `report_sol.md` anywhere in
   tracked files, and zero occurrences of `vector_id`, `event_timestamp`, or the record-level
   `ttl` column outside explicitly historical ADR passages.

## Known non-issues (verified — do not "fix")

- The single production `.expect()` at `rust/search-api/src/cache/index_cache.rs:233` is
  structurally safe (the `Option` is only cleared on guard drop). Leave it.
- Graceful shutdown, bounded admission, bounded caches, and fail-fast config validation in the
  Rust service are all in place and correct.
- The repository layer uses SQLAlchemy Core expressions throughout — no raw SQL to remove.
- The lifecycle triggers overlapping the repository-method checks are intentional
  defense-in-depth, not duplication.
- `docs/production-release.md` and `docs/confluence-architecture-overview.md` already describe
  the local-first architecture accurately.
