# Autonomous production implementation runbook

Status: complete implementation specification and durable restart context

Audit date: 2026-07-13

Audited lance-etl baseline: 93e72a2edcef1e7e8be8796986aaa9213991670c

Required Lance baseline: v8.0.0 at 15f2ff594a25b97f9bedd21a253b612ce14e39ec

## Agent directive

If you are the implementation agent reading this file, this file is your complete project brief.
Read it fully, read every required AGENTS.md guide, inspect the current repository, and start
implementing immediately. Do not answer with another architecture report or wait for another
prompt while safe work remains.

Continue through the ordered commits in this runbook. After each green atomic commit, proceed to
the next incomplete stage. Use the execution ledger and Git history to resume after context
compaction or agent replacement.

The terminal condition is:

- every implementable stage is committed on the dedicated branch
- every locally runnable required check passes
- the execution ledger is current
- any gate requiring unavailable external credentials, infrastructure, or billion-row hardware is
  recorded precisely as BLOCKED with the command and environment needed to run it

Do not create a pull request. Do not push. Do not deploy. Do not mutate production data or
services.

## Authorized scope

Breaking changes are explicitly authorized.

Replace obsolete behavior directly. Do not add:

- compatibility aliases
- deprecated flags
- dual-write paths
- transitional DAGs
- legacy protobuf fields
- adapters for the old public API
- migrations for experimental Lance datasets
- extra PostgreSQL entities to preserve the old architecture

Delete or rewrite superseded code, tests, documentation, ADR decisions, configuration, and
generated interfaces in the same commit that introduces the replacement.

Use a clean pinned Iceberg baseline and new physical Lance URIs for the new system. Never delete
and recreate a dataset at an old URI. Lance and service caches can retain immutable metadata by
URI and version path.

Preserve unrelated user changes. Never reset, clean, stash, discard, or overwrite work that you
did not create.

## Required reading and API ground truth

Before editing any file:

1. Read AGENTS.md completely.
2. Read src/lance_etl/AGENTS.md before Python, Airflow, benchmark, test, or related documentation
   changes.
3. Read rust/search-api/AGENTS.md before Rust or protobuf changes.
4. Read the relevant README and ADR files for the current stage.
5. Inspect current Git status, recent commits, and all existing diffs.

### Local Lance repository is read-only reference

The Lance repository at:

~~~text
/Users/gstamatakis/IdeaProjects/lance
~~~

is read-only API ground truth. Never edit, format, fetch, switch, reset, clean, commit, or create a
worktree from that checkout.

Its current branch can be newer than Lance 8. Always inspect the exact v8.0.0 tag. Safe read-only
commands include:

~~~bash
git -C /Users/gstamatakis/IdeaProjects/lance rev-parse 'v8.0.0^{commit}'
git -C /Users/gstamatakis/IdeaProjects/lance ls-tree -r --name-only v8.0.0
git -C /Users/gstamatakis/IdeaProjects/lance grep -n 'symbol_name' v8.0.0 -- path/to/subtree
git -C /Users/gstamatakis/IdeaProjects/lance show v8.0.0:path/to/file
~~~

Use the installed pylance 8 package as the Python binding authority when the Rust source and
Python surface differ. Do not guess an API or assume Lance main behavior applies to v8.

The hard repository decisions remain mandatory:

- no leading underscores on defined names
- no inline comments
- complete Python type hints and Google-style docstrings
- imports only at module top
- heavy Lance I/O and compute only in Spark executors
- the exact type-specific Lance segment index recipes
- typed filter AST only, never raw SQL
- no stable row IDs
- all Python Lance commits through commit_with_retries
- low-cardinality Datadog telemetry
- no prose semicolons in Markdown

## Branch and commit workflow

Use one local branch named production-readiness unless the repository is already on a dedicated
implementation branch containing this work.

Start with:

~~~bash
git status --short --branch
git log -5 --oneline
git branch --show-current
~~~

If still on main and the branch does not exist:

~~~bash
git switch -c production-readiness
~~~

If production-readiness already exists, inspect it before switching. Continue completed work
instead of recreating it. Never use a destructive reset to force the branch into an expected
state.

If report_sol.md is untracked or modified when implementation begins, commit it alone first:

~~~bash
git add report_sol.md
git diff --cached --check
git diff --cached
git commit -m "docs: add autonomous production implementation runbook"
~~~

If HEAD is newer than the audited baseline, inspect every intervening commit and reconcile the
implementation plan with current code. Preserve newer fixes. Do not blindly restore the audited
snapshot.

### Atomic commit loop

For every implementation commit:

1. Select the earliest incomplete stage from the execution ledger.
2. Inspect the relevant code and tests.
3. Write a bounded plan for that stage.
4. Delegate independent bounded work where useful.
5. Implement only the current invariant.
6. Run focused tests while developing.
7. Run every mandatory check for the languages touched.
8. Review the entire diff for correctness and scope.
9. Update the execution ledger in this file.
10. Stage explicit paths only.
11. Inspect the staged diff and commit.
12. Continue to the next stage without waiting for a user prompt.

Before committing:

~~~bash
git status --short
git diff --check
git diff --stat
git diff
git add path/to/file path/to/other-file
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "imperative atomic description"
git show --stat --oneline HEAD
git status --short --branch
~~~

Never use git add -A. The root agent is the only agent allowed to create commits.

Every commit must build, include its tests and documentation, and leave no known broken
intermediate state. A large stage may be split at a clean independently buildable invariant.
Never mix unrelated stages.

## Effective use of subagents

The root agent is the orchestrator, architectural reviewer, integrator, test authority, and commit
owner. Use lower-cost capable subagents for bounded work that materially reduces elapsed time or
expensive root context.

Use no more than three subagents at once.

Good subagent tasks include:

- focused repository inventories
- test and migration mapping
- mechanical removals and renames
- one isolated module with explicit owned paths
- documentation reconciliation
- focused failure diagnosis
- read-only post-change review
- benchmark and test-harness work after interfaces stabilize

Reserve root-agent judgment for:

- PostgreSQL state invariants
- Iceberg lineage and snapshot ownership
- duplicate and tombstone semantics
- zombie-worker safety
- exact-version publication
- authorization boundaries
- public API decisions
- integration review and commits

### Concurrency rules

Subagents share the same checkout. Their edits are immediately visible.

- Run only one Python writer at a time.
- A Python writer and a Rust writer may run concurrently when their contracts are already fixed.
- A third slot may perform read-only review, command audit, or benchmark analysis.
- Never let two agents edit the same file or subsystem.
- Never run repository-wide Ruff formatting while another writer is active.
- Stop active writers before root integration formatting and tests.
- Keep dependency locks, shared AGENTS files, shared README files, ADR indexes, CI workflows,
  protobuf definitions, and tests/conftest.py root-owned unless one agent receives explicit
  ownership.
- Subagents never create branches, commits, pushes, or pull requests.

Python state, source, mutation, and orchestration stages are sequential. Rust search analysis,
release audits, and read-only benchmark preparation can run earlier, but their commits follow the
ordered plan.

### Subagent assignment template

Use this structure:

~~~text
Read AGENTS.md, report_sol.md, and the relevant package AGENTS.md completely.

Implement only <one concrete deliverable> from <stage>.
Owned paths: <exact paths>.
Forbidden paths: <paths or subsystems>.

Breaking changes are allowed. Do not add compatibility behavior.
Do not create a branch, commit, push, or pull request.
Do not reset or discard existing work.
Stop and message the root before changing an unowned file.

Acceptance criteria:
- <criterion>
- <criterion>

Run focused tests. Return only:
1. Files changed
2. Commands and outcomes
3. Acceptance criteria satisfied
4. Remaining risks or cross-boundary work
~~~

For high-risk changes, use a separate read-only reviewer:

~~~text
Review the current diff for <stage>. Do not edit files.
Check persisted-state invariants, retry behavior, crash boundaries, security, and tests.
Report findings by severity with file and line evidence.
State explicitly when no actionable finding remains.
~~~

### Cost and effectiveness rules

- Do not spawn an agent for work the root can finish and verify quickly.
- Do not give a subagent the entire report as an undifferentiated task.
- Prefer one concrete deliverable with explicit ownership and acceptance criteria.
- Reuse a domain agent for a close follow-up instead of paying another agent to reread the repo.
- Use read-only agents before commissioning competing implementations.
- Require concise evidence, not a narrative restatement of this report.
- Interrupt agents that drift, duplicate work, or cross ownership.
- Treat a subagent success report as evidence, not verification.
- Root must inspect every diff and rerun relevant checks.
- For state, mutation, publication, and authorization, prefer one implementer, one independent
  reviewer, and root adjudication.

## Production architecture

### Source and partition contract

Iceberg is the only ingestion and replay source. Do not add Kafka or another event store.

The exact production partition specification is:

~~~text
PARTITIONED BY (tenant_id, namespace, org_id, hours(processing_timestamp))
~~~

The hour transform is a pruning dimension, not the ingestion cursor. A late event can be appended
into an older hour partition. Snapshot membership must still capture it.

One serial planner must:

1. Read the table UUID and current main snapshot.
2. Continue from the newest SEALED or COMPLETE source window.
3. Walk the parent-linked snapshot ancestry to a pinned head.
4. Classify every snapshot.
5. Record exact append spans and trusted logical maintenance no-ops.
6. Inspect added-file manifests for touched target and hour partitions.
7. Insert the source window and target work atomically.
8. Fail closed on a changed UUID, fork, physical delete, untrusted overwrite, or unknown operation.

Read each append contribution as its own parent-to-child span. Do not span a maintenance overwrite
and treat replacement files as CDC.

Retries read the exact immutable plan and filter the named tenant_id, namespace, org_id, and proven
touched hours. A target touched across several hours is one work item and is collapsed once.

Remove wall-clock source ownership and apply_window_filter from correctness. Do not collect and
sort complete snapshot history on the driver.

The initial run is a pinned baseline. Rebuild into new physical URIs from complete retained CDC
history or a validated canonical source baseline. Normal incremental planning begins only after
the baseline is validated.

Snapshot expiration stays behind the oldest unfinished source window plus the replay and recovery
horizon. Run Iceberg optimization separately from ingestion.

### Three-table PostgreSQL control plane

Use exactly three logical application tables. PostgreSQL contains coordination only, never CDC
rows, vectors, index files, centroids, models, query results, or replica warmth.

The STATE stage must add explicit direct dependencies on the psycopg 3 driver, SQLAlchemy Core,
and Alembic. Do not rely on Airflow transitive packages and do not create ORM mappings. Keep
transactions and constraints visible in a small SQL-oriented repository. Test against a real
PostgreSQL service through a Docker-backed local fixture and CI service. SQLite is not an accepted
substitute for claim and locking tests.

#### source_windows

Required fields:

- window_seq monotonic primary key
- deterministic_window_key unique
- kind BASELINE or INCREMENT
- table_uuid
- nullable from_snapshot_id only for BASELINE
- to_snapshot_id
- lineage_digest
- immutable read_plan JSONB
- state SEALED, COMPLETE, or BLOCKED
- timestamps
- bounded error_code

The newest SEALED or COMPLETE row is the discovery cursor. The oldest non-COMPLETE row is the
retention floor.

#### targets

Required fields:

- target_id primary key
- tenant_id
- namespace
- org_id
- lance_uri unique
- profile_id
- lane_tail_work_id
- last_applied_window_seq
- served_lance_version
- fence_epoch
- updated_at

The named identity tuple is unique. Derive lance_uri from strictly validated components and the
deployment base. Never accept a public arbitrary URI.

#### target_work

Required fields:

- work_id primary key
- dedupe_key unique
- target_id
- nullable source_window_seq
- kind INGEST, MAINTAIN, or REBUILD
- predecessor_work_id
- state PENDING, RUNNING, RETRY_WAIT, SUCCEEDED, or BLOCKED
- phase INGEST, MAINTAIN, INDEX, VALIDATE, PREWARM, or PUBLISH
- run_token
- lease_epoch
- lease_expires_at
- attempt_count
- next_attempt_at
- input_lance_version
- output_lance_version
- source_row_count
- source_digest
- artifact_manifest_uri
- artifact_digest
- bounded error code and message
- timestamps

One routine work row advances through all phases. It replaces separate lease, failure, attempt,
artifact, promotion, and warmth entities.

Use unique idempotency constraints and FOR UPDATE SKIP LOCKED claims. Enqueue each target behind
its lane tail. A worker may claim only due work whose predecessor succeeded.

Keep active and recent work in PostgreSQL. Range-partition the same logical target_work table and
archive completed rows after the replay and audit horizon. Do not create a second history entity.

Do not add tables for cursors, staged rows, leases, attempts, failures, artifacts, promotions,
tags, replica warmth, garbage collection, shards, generations, policies, or branches.

### Durable retry and mutation semantics

Use three retry layers:

1. Short object-store and Lance operation retries.
2. Bounded commit_with_retries conflict handling with reopen.
3. Durable target_work retries with full-jitter exponential backoff.

Airflow retries only systemic dispatcher failures. Retry count and backoff are code-owned, not
user settings.

Every stored row carries:

- lance_etl_window_seq
- lance_etl_event_digest
- is_deleted

Incoming rows update only when window_seq is greater than stored window_seq. A stale zombie cannot
overwrite or delete newer state.

Within one source window:

1. Validate routing, vector_id, operation, and timestamps.
2. Canonicalize maps by sorted entries.
3. Compute SHA-256 over identity, operation, timestamps, and full payload.
4. Collapse exact duplicates.
5. Select greatest event_timestamp per target and vector_id.
6. Give DELETE explicit precedence over UPSERT at equal timestamp.
7. BLOCK distinct equal-time UPSERT payloads as source conflicts.

Across windows, later window_seq wins regardless of event_timestamp.

Replace physical when_matched_delete with a guarded tombstone upsert. Search always injects
is_deleted = false. Keep tombstones through the replay horizon.

UPSERT is a complete post-image. Before merge, union allowed new fields with the current target
schema and materialize explicit null for every absent existing payload field.

After all salted groups succeed, one executor-side finalizer commits
lance_etl.last_applied_window_seq and lance_etl.last_applied_source_digest in dataset config. It
never moves the marker backward. Advance target_work only after that marker is durable.

If a target partially commits before failure, retry the same pinned plan. Already written rows
no-op, missing rows apply, and the marker commits only after complete success.

Disable raw bulk append until deterministic ambiguous-outcome reconciliation exists. Disable
clustered overwrite until its memory and writer-overlap behavior are qualified. Never enable
stable row IDs.

Audit destination uniqueness by vector_id. Repair duplicates through REBUILD into a new URI,
validate uniqueness, rebuild indexes, prewarm, and promote.

### One reconciler and minimal configuration

Delete the two old DAGs and replace them with one serialized production DAG:

1. plan_and_enqueue_window
2. run_due_target_work
3. reconcile_results
4. gate_source_retention
5. emit_slo_status

Set max_active_runs to one initially. A Spark worker may claim a bounded batch from one source
window and coalesce its partition-pruned Iceberg scan. Ownership and completion remain per target.

A poison target blocks only its own successors. Healthy lanes continue. BLOCKED work alerts and
keeps the source retention floor.

Scheduled runs accept no user parameters.

Deployment-owned settings are limited to:

- deployment profile
- Iceberg catalog and table
- new Lance base URI
- PostgreSQL connection
- Spark connection and resource class
- object-store credentials
- Datadog identity
- search service addresses, TLS material, JWT issuer, audience, and JWKS location

Restricted operator tools may accept work_id, target identity, approved exact source range, and
dry-run.

Remove manual time windows, datasets_file, raw index flags, TTL and tag toggles, arbitrary Spark
JSON, bucket and batch knobs, conflict retry knobs, cache knobs, probes, refine, fast-search, and
exact-scan controls from routine Airflow and public users.

### Exact index and HEAD publication

Preserve the exact Lance v8 segment recipes in AGENTS.md.

Centroids, the RaBitQ model, and configuration form one immutable content-addressed artifact
generation. Store one manifest URI and digest in target_work and committed index metadata. Every
retry reuses the same generation.

For each target:

1. Capture exact post-maintenance data version.
2. Build every required index.
3. Capture exact indexed version.
4. Validate schema, row count, live vector-ID uniqueness, vector dimensions, required index
   coverage, and artifact digest at that version.
5. Prewarm every serving replica against that exact version.
6. Verify every required replica resolved the same version.
7. Move HEAD to that explicit version.
8. Read HEAD back.
9. Store served_lance_version and mark work SUCCEEDED transactionally.

If database completion fails after the tag move, retry observes HEAD at the desired version and
finishes. Unexpected newer HEAD fails closed.

Never publish with target_version omitted. Search production resolves HEAD only. Latest and
arbitrary versions or tags are restricted internal administration.

Routine work stays at one deterministic physical dataset. New URIs are only for baseline, rebuild,
destructive schema migration, future re-sharding, or disaster recovery.

### Search production boundary

Remove IntakeService and StdoutSink from code, protobuf, docs, and tests. Iceberg is the only write
contract.

Before exposure:

- require gRPC TLS
- validate a bearer JWT against deployment-owned issuer, audience, and JWKS settings
- require exact tenant_id, namespace, and org_id authorization claims for the requested target
- require a separate admin role claim for internal administration
- derive URI server-side
- scope storage credentials to the allowed prefix
- separate or remove administrative RPCs
- validate every path component
- add process-global and per-tenant admission
- enforce bounded k, projection, response bytes, queue time, and deadlines
- make readiness dependency-aware
- add bounded graceful drain
- remove target identity and URI from normal telemetry
- ensure outer route timeouts record timeout outcomes

The public search request exposes only target, query, bounded k, typed filter AST, allowlisted
projection, exact time range, and product-semantic fusion.

The server owns probes, refine, ef, fast or exact mode, filter execution mode, WAND, cache behavior,
fallback, and deadlines.

Replace google.protobuf.Struct results with a typed response carrying vector_id, typed projection,
score or distance, served_version, partial, and bounded warnings.

Always project vector_id internally. Deduplicate each result leg by vector_id and fuse hybrid
results by vector_id, never physical Lance row ID. Fetch a bounded surplus so deduplication does
not underfill k.

Fast search must cover every required index at the exact HEAD version or fail closed. Fix
second-resolution time conversion so the documented millisecond half-open range has no false
positive start.

### Billion-row boundary

The initial architecture targets billions of rows across the fleet. It does not claim that one
logical target containing one billion rows meets query SLOs.

Do not add sharding to the initial PostgreSQL schema. Add one target_shards table and a versioned
routing catalog only when measured commit throughput, memory, fragment budgets, latency, or recall
requires it.

Qualify vector profiles separately by dimension, metric, row width, RQ bits, filters, and hardware
at:

- 1 million rows
- 10 million rows
- 50 million rows
- 100 million rows
- largest promised target
- 1 billion rows only when single-target support is required

Measure build memory and time, artifact size, recall, cold and warm latency, object-store requests,
fragment growth, PostgreSQL queue throughput, retry backlog, and rebuild amplification.

Evaluate a separate online vector engine only if measured Lance sharding cannot meet a real global
or whale-query requirement. Lance remains the durable validated source.

## Ordered commit plan

Implement these stages in order. A stage may be split into smaller green commits when an invariant
would otherwise be hidden in a large diff.

### 0. Safety removal

Commit intent:

~~~text
refactor: remove unsafe production surfaces
~~~

Implement:

- code-owned production defaults disable raw bulk append and clustered rewrite
- remove IntakeService and StdoutSink
- require HEAD serving in production
- remove unsafe public execution knobs that can be deleted independently
- update tests and docs for the breaking removals

Acceptance:

- removed surfaces are unreachable
- repository remains green
- old DAGs remain only until the reconciler replacement commit

### 1. STATE-01

Commit intent:

~~~text
feat: add durable control plane
~~~

Implement:

- explicit PostgreSQL runtime and migration dependencies
- the three application tables and constraints
- typed repository and state transitions
- lane enqueue, SKIP LOCKED claim, lease renew, retry, success, and block
- bounded error handling and archive policy
- reproducible real-PostgreSQL local and CI test harness

Acceptance:

- duplicate planning creates no duplicate work
- concurrent claimers cannot own the same work
- predecessor ordering holds
- expired lease can be reclaimed
- stale lease epoch cannot complete work
- PostgreSQL restart at each transition remains recoverable

### 2. SOURCE-01

Commit intent:

~~~text
feat: plan exact Iceberg source windows
~~~

Implement:

- parent-linked lineage walk
- snapshot classification
- immutable read_plan
- manifest-derived target and hour discovery
- exact target scans and batched scan optimization
- atomic window and work insertion
- retention watermark
- pinned baseline
- removal of row-time ownership

Acceptance:

- late old-hour append is captured
- append, trusted replace, append emits only append CDC
- untrusted overwrite and physical delete block
- retry reads the same plan after newer snapshots arrive
- driver memory does not grow with total rows or complete history

### 3. MUTATION-01

Commit intent:

~~~text
feat: make Lance mutations replay safe
~~~

Implement:

- SHA-256 canonical identity
- exact duplicate collapse and conflict detection
- full-row null materialization
- window-sequence guarded upsert and tombstone
- completion marker
- partial-commit retry
- uniqueness audit and rebuild
- removal of cross-window timestamp guard
- disable or delete raw bulk path

Acceptance:

- 100 repeated retries converge
- later window with older event time wins
- stale zombie cannot change newer state
- omitted fields clear
- delete and recreate works
- equal-time conflicting UPSERT blocks
- existing duplicate repair creates one live vector_id

### 4. ORCH-01

Commit intent:

~~~text
refactor: replace workflows with durable reconciler
~~~

Implement:

- delete both old DAGs
- one reconciler DAG
- fixed code-owned systemic retry policy
- bounded queue drain
- batched claims
- retention gate
- restricted repair commands
- remove old Airflow variables, CLI flags, and docs

Acceptance:

- ETL and maintenance cannot overlap outside a target lane
- successful targets do not replay because another target failed
- Airflow outage loses no work
- Airflow DAG test runs without skipping
- scheduled DAG has no routine user parameters

### 5. PROMOTE-01

Commit intent:

~~~text
feat: publish exact validated Lance versions
~~~

Implement:

- immutable index artifact generation
- exact input and output versions
- exact validation and prewarm
- explicit HEAD update and read-back
- idempotent database completion
- rollback and cleanup protection

Acceptance:

- crash at every step converges
- unexpected HEAD fails closed
- search never serves unvalidated Latest
- centroid and RaBitQ generations cannot mix
- required index coverage is complete at served version

### 6. SEARCH-01

Commit intent:

~~~text
refactor: harden production search API
~~~

Implement:

- breaking protobuf and service simplification
- authentication and authorization
- URI derivation
- typed responses
- vector-ID fusion and deduplication
- exact time semantics
- global admission and fairness
- readiness and bounded drain
- truthful timeout accounting
- privacy-safe telemetry

Acceptance:

- cross-target access fails before object-store access
- public requests cannot select execution knobs or arbitrary versions
- duplicate physical rows cannot produce duplicate logical hits
- overload sheds without unbounded memory
- served_version is always returned
- normal metrics and spans contain no target identity or URI

### 7. RELEASE-01

Commit intent:

~~~text
build: lock production release inputs
~~~

Implement:

- exact pylance 8 pin
- locked Python installation
- Rust 1.91.0 toolchain file
- locked Cargo commands
- pinned CI actions and tools
- mandatory Airflow and integration jobs
- remove continue-on-error from required benchmark gates
- immutable container images
- deployment manifests
- SBOM, vulnerability checks, canary, and rollback commands

Acceptance:

- clean checkout reproduces dependencies
- every CI command is locked
- release image records exact Git and Lance versions
- rollback restores prior HEAD

### 8. SCALE-01

Commit intent:

~~~text
test: add scale and failure qualification
~~~

Implement:

- deterministic skewed data generation
- duplicates, late rows, deletes, and wide schema cohorts
- PostgreSQL queue load
- crash injection at every external write
- object-store failure injection
- recall and latency cohorts
- capacity artifact with hardware, cache state, commit, and Lance version

Acceptance:

- all local fault tests pass
- SIFT1M smoke passes
- largest available scale passes with documented headroom
- unavailable 100M or 1B infrastructure is recorded as an exact external gate
- no unmeasured billion-row claim is made

## Running and verification

This branch receives no hosted CI because there is no pull request and current workflows target
main. Every commit must be locally green.

### Locked environment setup

Standardize on .venv:

~~~bash
uv sync --locked --all-groups --python 3.14
source .venv/bin/activate
java -version
protoc --version
~~~

If dependencies intentionally change:

~~~bash
uv lock
git diff -- pyproject.toml uv.lock
uv sync --locked --all-groups --python 3.14
~~~

Install the declared Rust toolchain until rust-toolchain.toml exists:

~~~bash
rustup toolchain install 1.91.0 --profile minimal --component rustfmt,clippy
~~~

Rust builds require protoc. Redis tests require redis-server to run rather than skip.

### After every Python, Airflow, test, or benchmark edit

~~~bash
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/
uvx ruff format --check src/ tests/ airflow/ bench/
.venv/bin/pytest path/to/relevant_test.py -x -q
.venv/bin/pytest -m "not integration"
~~~

After Airflow changes:

~~~bash
.venv/bin/pytest tests/test_airflow_dags.py -v
~~~

An Airflow skip is a failure for an orchestration commit.

### After every Rust or protobuf edit

From rust/search-api:

~~~bash
cargo +1.91.0 fmt
cargo +1.91.0 fmt --check
cargo +1.91.0 clippy --locked -- -D warnings
cargo +1.91.0 test --locked
~~~

For release milestones:

~~~bash
cargo +1.91.0 build --release --locked
~~~

### Stage-boundary gates

From repository root:

~~~bash
uvx ruff format --check src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/
.venv/bin/pytest -m "not integration"
.venv/bin/pytest tests/test_airflow_dags.py -v
.venv/bin/pytest -m integration \
  --ignore=tests/test_bench_e2e.py \
  --ignore=tests/test_bench_e2e_tagged.py
~~~

Then from rust/search-api:

~~~bash
cargo +1.91.0 fmt --check
cargo +1.91.0 clippy --locked -- -D warnings
cargo +1.91.0 test --locked
cargo +1.91.0 build --release --locked
~~~

The control-plane commit must add and document one reproducible PostgreSQL integration command.
Run it at every later stage boundary.

Offline Spark benchmark integration:

~~~bash
.venv/bin/pytest -m integration \
  tests/test_bench_e2e.py \
  tests/test_bench_e2e_tagged.py \
  -x -q
~~~

Fast benchmark unit check:

~~~bash
.venv/bin/pytest tests/test_bigann_io.py -x -q
~~~

SIFT1M smoke after the release server is built:

~~~bash
python -m bench all \
  --dataset sift1m \
  --batches 2 \
  --etl-partitions 4 \
  --num-partitions 128 \
  --endpoint localhost:50051 \
  --workspace bench/workspace \
  --results-root bench/results
~~~

Do not run 100M or 1B qualification as a routine commit test. Those runs require declared remote
storage, compute, time, and cost approval. Preserve metrics, exact commit, exact Lance versions,
hardware, dataset checksum, and cache state for every scale result.

### Required fault matrix

Before branch completion, cover:

- delayed append into an old hour partition
- trusted replace between append snapshots
- untrusted overwrite
- changed table UUID and forked lineage
- crash before and after every PostgreSQL transition
- crash before and after every Lance commit
- lease expiry with a live zombie
- exact duplicate UPSERT and DELETE
- conflicting equal-time UPSERT
- same vector_id across hours and windows
- stale delete, delete then recreate, and null clearing
- partial salted target commit
- ambiguous index and HEAD publication
- cache corruption and restart
- Redis unavailable and slow
- object-store throttle, timeout, and stale read
- global overload and graceful-drain deadline
- backup, restore, and HEAD rollback

Do not weaken a failing test to make a commit green. If a test encodes an intentionally removed
interface, replace it with a test of the new invariant in the same commit.

## Execution ledger

The root agent owns this ledger. Update the current stage to DONE only in the commit that satisfies
its acceptance criteria. Git history supplies the commit hash.

Allowed states are NOT_STARTED, IN_PROGRESS, DONE, and BLOCKED.

| Stage | State | Verification | Notes or blocker |
|---|---|---|---|
| Safety removal | NOT_STARTED | | |
| STATE-01 | NOT_STARTED | | |
| SOURCE-01 | NOT_STARTED | | |
| MUTATION-01 | NOT_STARTED | | |
| ORCH-01 | NOT_STARTED | | |
| PROMOTE-01 | NOT_STARTED | | |
| SEARCH-01 | NOT_STARTED | | |
| RELEASE-01 | NOT_STARTED | | |
| SCALE-01 | NOT_STARTED | | |

## Continuation and stopping rules

Continue after every commit. Do not stop because:

- a stage is difficult
- tests are slow
- context was compacted
- one subagent failed
- the implementation spans multiple commits
- an obsolete interface requires broad deletion

After compaction or restart, reread this file, AGENTS.md, the ledger, Git status, and recent commits,
then resume the earliest incomplete stage.

Stop and request user direction only when:

- work would mutate production or another external system
- a required secret or credential is unavailable
- a product decision not made here would change persisted data semantics or authorization
- unrelated user changes overlap required files
- exact Lance v8 evidence proves a mandatory invariant cannot be implemented
- a blocker remains after focused diagnosis and safe alternatives are exhausted

When blocked, keep the branch clean when possible and record:

- exact stage
- evidence with file and line
- commands attempted
- why safe alternatives failed
- exact user decision or external resource required
- next command after unblock

## Start now

The implementation agent should now:

1. Read all required guides.
2. Inspect current branch, status, diffs, and recent commits.
3. Create or continue production-readiness.
4. Commit this report alone if it is not already committed.
5. Mark Safety removal IN_PROGRESS locally.
6. Use bounded subagents for an independent safety inventory and test plan.
7. Implement the first green atomic commit.
8. Update the ledger, commit, and continue through the remaining stages.
