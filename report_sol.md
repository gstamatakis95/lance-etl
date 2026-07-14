# Autonomous production implementation runbook

Status: complete implementation specification and durable restart context

Audit date: 2026-07-14

Audited lance-etl baseline: branch gs/f1 at 6cef023, including the reviewed fixed-HEAD serving
safety slice

Required Lance baseline: v8.0.0 at 15f2ff594a25b97f9bedd21a253b612ce14e39ec

## Production decision and current implementation state

This repository is not production ready yet. It has a sound Lance v8 indexing core and the current
branch has already removed several unsafe surfaces, but durable source ownership, target-scoped
retry state, replay-safe tombstones, exact publication orchestration, authenticated serving, and
scale qualification remain release blockers.

The initial production claim is billions of rows across the fleet. It is not a claim that one
logical target containing one billion rows meets latency, recall, or recovery objectives.

The following work is already present and must not be reimplemented or reverted:

| Change | Current state |
|---|---|
| Raw bulk append and clustered overwrite | Production defaults and command surfaces are disabled in 6c9ffaa. The qualification code remains for tests only |
| Placeholder online intake | IntakeService and StdoutSink were removed in fd82a0c. Iceberg is now the only write contract |
| Python HEAD publication | fbac425 requires an explicit target Lance version and removes implicit latest-version promotion |
| Rust production serving | 6cef023 makes ordinary serving resolve the fixed HEAD tag and removes the serve-by-tag, serve-tag, and deprecated disk-cache-disable environment switches |
| Current verification | Python non-integration suite: 679 passed and 36 deselected. Airflow DAG suite: 6 passed. Rust formatting and Clippy passed. Rust locked suite: 180 passed |

The fixed-HEAD slice is now committed. Green local tests do not replace cloud object-store,
PostgreSQL, security, chaos, recall, or billion-row gates. This table is audit-time evidence. The
execution ledger near the end is the authoritative mutable status after the next implementation
commit.

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

The checked-in source column is `processing_timestamp`. In the shorthand `hour(ts)`, `ts` means
that column. If production uses a different timestamp, change the DDL and code-owned source
contract together. Never let Airflow select the column.

Add `mutation_version BIGINT NOT NULL` to the Iceberg CDC contract. It is non-decreasing for each
named target and vector_id. Every distinct mutation must strictly increase it, while equality is
allowed only for an exact duplicate of the same immutable mutation. This is the only reliable way
to distinguish an exact duplicate from an older mutation delivered in a later snapshot. If the
producer cannot supply this sequence, production can support deterministic ingestion-order wins,
but it cannot truthfully claim out-of-order duplicate safety. Do not launch with that ambiguity.

`event_timestamp` remains query and TTL time. `processing_timestamp` remains the Iceberg hour
partition. Neither column is the cross-snapshot mutation sequence.

The hour transform is a pruning dimension, not the ingestion cursor. A late event can be appended
into an older hour partition. Snapshot membership must still capture it. Spark supports reads
pinned by `snapshot-id`, which is the required basis for deterministic replay
([Iceberg Spark read options](https://iceberg.apache.org/docs/latest/spark-configuration/)).

Use one Iceberg snapshot as one source window. An append snapshot is one immutable CDC increment.
Its direct parent is the exclusive start and the snapshot itself is the inclusive end. A trusted
logical maintenance snapshot is recorded as a completed no-op. The initial canonical snapshot is
one BASELINE window. Do not coalesce consecutive append snapshots in the first implementation.
Coalescing is an internal optimization only after the one-snapshot path is qualified.

One serial planner must:

1. Read the table UUID and current main snapshot, then validate that the active partition spec
   exactly matches the required named transforms.
2. Stop if the newest recorded row is BLOCKED. Otherwise continue from its snapshot.
3. Walk the parent-linked snapshot ancestry to a pinned head.
4. Order snapshots by parent and Iceberg sequence number, never commit timestamp and snapshot ID.
5. Classify exactly one row for each next snapshot before creating work.
6. For APPEND, inspect ADDED manifest entries for touched target and hour partitions.
7. For APPEND, create any missing validated target with the release's code-owned profile, then
   insert the source window and one target work item per touched target in one transaction.
8. For trusted replace or trusted maintenance overwrite, create no target work and complete the
   source window as a logical no-op.
9. Fail closed on a changed UUID, partition-spec change, fork, physical delete, untrusted
   overwrite, or unknown operation.

Trust maintenance only when the selected catalog supplies an authenticated allowlisted writer
identity, the snapshot carries an explicit marker such as `lance_etl.logical_change=false`, and
manifest invariants match the approved rewrite procedure. Operation name or a caller-written
summary marker alone is insufficient. If the catalog cannot prove writer identity, v1 blocks every
non-append snapshot and scheduled source rewrites remain disabled. An untrusted overwrite blocks
discovery until an operator classifies it.

Work identity is `(append snapshot, target)`, not `(hour, target)` and not an Iceberg file. A target
touched across several hour partitions in one snapshot is one work item. Its retry reads the exact
parent-to-snapshot increment and filters `tenant_id`, `namespace`, and `org_id`. Iceberg uses the
hour partitions in the immutable snapshot manifests for pruning. Do not persist a second hours
array that can disagree with those manifests. All hours for the target are unioned and collapsed
once. This uses the physical partition layout without turning processing time into correctness
state.

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
- table_uuid
- snapshot_id
- nullable parent_snapshot_id for the initial baseline only
- iceberg_sequence_number
- kind BASELINE, APPEND, or TRUSTED_MAINTENANCE
- state SEALED, COMPLETE, or BLOCKED
- timestamps
- bounded error_code

The pair `(table_uuid, snapshot_id)` is unique. The Iceberg sequence number is monotonic within the
table UUID. The newest recorded row is the audit tip. A BLOCKED source window halts discovery, and
the planner never records any descendant until that exact row is classified and transitioned. The
oldest non-COMPLETE row is the retention floor. The snapshot row and its target work are inserted
atomically, so a planner crash cannot advance discovery without durable work.

#### targets

Required fields:

- target_id primary key
- tenant_id
- namespace
- org_id
- ingest_lance_uri
- nullable served_lance_uri before first publication
- profile_id
- nullable last_applied_window_seq before first successful INGEST
- nullable last_applied_lance_version before first successful INGEST
- nullable served_lance_version before first publication
- fence_epoch
- updated_at

The named identity tuple is unique. Canonicalize and validate all three values with one bounded
ASCII-segment rule shared by Python and Rust before any Iceberg or object-store access. Reject
empty values, control characters, slash, backslash, dot segments, and traversal. Build the initial
ingest_lance_uri from the opaque target_id under the deployment base, not from raw tenant strings.
Only a fenced REBUILD or future re-sharding publication may change it. Never accept a public
arbitrary URI. The served URI and exact served version form the serving catalog and change in one
PostgreSQL compare-and-swap. Search fails closed while either served field is null. A serving-only
rollback changes the served tuple without redirecting later ingestion to an older URI.

`profile_id` names a versioned code-owned policy bundled with the release. It is not a foreign key
to another application table and it is not editable through Airflow or the search API.

#### target_work

Required fields:

- work_id primary key
- target_id
- nullable source_window_seq
- kind INGEST, SERVE, or REBUILD
- state PENDING, RUNNING, RETRY_WAIT, SUCCEEDED, or BLOCKED
- phase INGEST, MAINTAIN, INDEX, VALIDATE, PREWARM, or PUBLISH
- lease_token
- lease_expires_at
- attempt_count
- next_attempt_at
- data_lance_version
- indexed_lance_version
- nullable candidate_lance_uri until SERVE or REBUILD freezes its publication input
- expected_ingest_lance_uri
- nullable expected_served_lance_uri for initial publication
- nullable expected_served_lance_version for initial publication
- source_applied_at
- source_row_count
- source_digest
- artifact_manifest_uri
- artifact_digest
- bounded error code and message
- timestamps

An INGEST row ends when the exact source snapshot is durably applied and marked. Coalescing SERVE
rows perform due maintenance, indexing, validation, prewarm, and publication against applied Lance
versions. A SERVE failure never forces successful source ingestion to replay. REBUILD is the
exceptional exclusive path. These rows replace separate lease, failure, attempt, artifact,
promotion, and warmth entities.

`source_digest` is SHA-256 over one frozen byte stream for this target and snapshot. The stream
starts with the exact ASCII header `lance-etl-source-digest-v1\0`, followed by terminal
`(vector_id, mutation_version, event_digest)` tuples in unsigned UTF-8 byte order. Encode vector_id
as an unsigned 64-bit big-endian byte length followed by UTF-8 bytes, mutation_version as signed
64-bit big-endian, and event_digest as 32 raw bytes. Compute it with a distributed external sort and
one streaming executor-side reduction per target. It is independent of Spark partition count and
never collects terminal tuples to the driver. Set `source_applied_at` once, only after the matching
Lance dataset marker is durable. A source window becomes COMPLETE when every child INGEST row has
source_applied_at. SERVE and Lance-to-Lance REBUILD state never hold Iceberg retention.

Use unique idempotency constraints and FOR UPDATE SKIP LOCKED claims. An INGEST row is eligible only
when no smaller unfinished INGEST sequence exists for that target. Enforce that rule in the claim
transaction with an indexed anti-join, not mutable predecessor pointers. Partial unique indexes
allow only one RUNNING work row and one open SERVE row per target. Give due INGEST priority over a
SERVE row in RETRY_WAIT. On INGEST success, create or advance a PENDING or RETRY_WAIT SERVE row to
the newest last_applied_lance_version. Advancing it invalidates phase outputs derived from its old
input version. If SERVE is RUNNING, keep its exact input immutable. The target row's newer
last_applied_lance_version is the durable dirty signal. In the running row's completion
transaction, a successful older publication becomes SUCCEEDED and creates the next SERVE row,
while a failed older attempt replans the same row against the newest applied version. This lets
later source snapshots progress while a transient index or prewarm failure keeps retrying.

Derive INGEST work_id from target_id and source_window_seq. Derive REBUILD work_id from target_id
and a required operator request ID so retrying one request reuses one row while a later rebuild gets
a new identity. Generate each SERVE work_id when the prior generation closes. The partial unique
open-SERVE index, not a nullable source key, provides SERVE coalescing.

Claiming increments `targets.fence_epoch` and writes a fresh lease_token into the work row. Every
phase verifies both values before an external commit and again before advancing state.
Data merge retries may rebase only because the stored mutation version makes the mutation
monotonic. Maintenance, index publication, rebuild, and catalog publication operate against exact
input versions. They do not blindly rebase after a conflict. Extend commit_with_retries with a
guard callback and a replan result rather than bypassing the repository commit helper. A stale
worker may finish harmless computation, but it cannot complete control state or overwrite a newer
exact-version phase.

Keep only active and recent work in PostgreSQL. Launch with one unpartitioned target_work table so
its primary key and per-target partial unique indexes are global and obvious. Delete completed work
in bounded batches only after the replay, rollback, and audit horizons. Partial indexes on open
states keep claim scans bounded. Add native partitioning only after measured table pressure and a
focused ADR prove that deterministic work identity and per-target uniqueness remain
database-enforced. Do not create a second application history entity.

Do not add tables for cursors, staged rows, leases, attempts, failures, artifacts, promotions,
tags, replica warmth, garbage collection, shards, generations, policies, or branches.

### Durable retry and mutation semantics

Use three retry layers:

1. Short retries only for reads and writes proven idempotent by an exact operation identity.
2. Bounded commit_with_retries conflict handling with reopen.
3. Durable target_work retries with full-jitter exponential backoff.

Airflow retries only systemic dispatcher failures. Retry count and backoff are code-owned, not
user settings.

Transient target work has no small attempt ceiling. Keep it in RETRY_WAIT across DAG runs. INGEST
and an explicitly source-backed REBUILD may retry only while their exact Iceberg snapshot remains
inside the replay horizon. SERVE and Lance-to-Lance REBUILD retry from their persisted exact Lance,
artifact, and catalog inputs without holding Iceberg retention. Alert at fixed internal thresholds
such as attempts 5, 20, and 100 without changing the state to BLOCKED. BLOCKED is only for contract
failures such as invalid schema, conflicting mutation-version reuse, authorization, corrupt source
lineage, or an expired required input. A later operator retry changes state, not the work identity.
Use a fixed high Airflow policy for dispatcher crashes, with exponential backoff capped at one
hour. The initial code-owned dispatcher policy is 24 retries, one-minute base delay, exponential
backoff, and one-hour maximum delay. Durable target work continues across later DAG runs after
those attempts are exhausted. Do not multiply a high Airflow retry count by high inner object-store
and Lance retry counts.

Every stored row carries:

- lance_etl_window_seq
- lance_etl_mutation_version
- lance_etl_event_digest as fixed 32-byte binary SHA-256
- is_deleted

Incoming rows update only when mutation_version is greater than the stored mutation_version. A
lower version is a stale mutation and becomes a metered no-op. An equal version with the same
digest is an exact duplicate and becomes a no-op. An equal version with a different digest is a
source-contract conflict and blocks the target work. A stale zombie cannot overwrite or delete
newer state.

Detect conflicting reuse both inside the append snapshot and against existing target rows before
the first Lance write for that target. A conflict visible in preflight writes nothing. A conflict
introduced by an expired concurrent worker after preflight may leave guarded partial writes, but it
blocks the completion marker and publication. Conditional replay remains safe, and an operator must
resolve the source contract before retry or rebuild.

Within one source window:

1. Validate routing, vector_id, operation, and timestamps.
2. Canonicalize maps by sorted entries.
3. Compute SHA-256 over routing identity, vector_id, mutation version, operation, event timestamp,
   and the complete normalized payload. Exclude processing_timestamp, Iceberg snapshot and file
   metadata, and other delivery-envelope fields so a redelivery in a later processing hour remains
   an exact duplicate.
4. Collapse exact duplicates.
5. BLOCK any equal mutation version carrying a different operation or payload.
6. Select the greatest mutation_version per target and vector_id.

Freeze the canonical digest encoding as a persisted contract. Normalize timestamps to UTC
microseconds, encode null and each scalar type unambiguously, sort map keys, preserve list order,
and reject non-finite vector values. A release cannot change this encoding for an existing target.
An intentional replacement requires a fenced REBUILD into a new URI with every digest recomputed.

Across windows, greater mutation_version wins regardless of snapshot arrival order or
event_timestamp. `lance_etl_window_seq` remains the work provenance and completion fence. It is not
the business mutation order.

Implement the write with Lance v8's conditional merge update using
`target.lance_etl_mutation_version < source.lance_etl_mutation_version`, plus insert-if-absent. Do
not depend only on a pre-read. After every merge group, join the affected keys back to the reopened
dataset. For each incoming terminal row, the stored version must be greater, or it must be equal
with the same digest. Equal version with a different digest blocks before the dataset completion
marker. This post-write check closes the race where an expired worker commits after its successor's
initial conflict scan.

Replace physical when_matched_delete with a guarded tombstone upsert. Search always injects
is_deleted = false. Make `is_deleted` non-null and create its BITMAP index through the code-owned
profile. A tombstone keeps vector_id, window sequence, event digest, and ordering fields while
explicitly nulling payload columns. Keep tombstones through the replay and recovery horizon. Purge
them only after every older work item is impossible to replay and a validated recovery baseline
exists, otherwise an old UPSERT can resurrect a deleted key.

UPSERT is a complete post-image. Before merge, union allowed new fields with the current target
schema and materialize explicit null for every absent existing payload field.

The release-owned profile fixes allowed vector, text, and metadata names, types, vector dimensions,
distance metric, required indexes, and TTL behavior. Unknown keys, reserved-name conflicts,
dimension mismatches, and incompatible type changes BLOCK before any write. Production must not
silently skip a key, null an invalid vector, or grow schema from arbitrary user map keys.

After all salted groups succeed, one executor-side finalizer commits
lance_etl.last_applied_window_seq and lance_etl.last_applied_source_digest in dataset config. It
never moves the marker backward. Set source_applied_at, advance the target's applied fields, and
enqueue or refresh SERVE only after that marker is durable. This completes INGEST and releases its
Iceberg retention dependency even when later indexing or publication keeps retrying.

If a target partially commits before failure, retry the same pinned plan. Already written rows
no-op, missing rows apply, and the marker commits only after complete success.

The required idempotency boundary is explicit:

| Boundary | Durable identity | Retry result |
|---|---|---|
| Source discovery | `(table_uuid, snapshot_id)` | Existing source window is reused |
| INGEST enqueue | deterministic work_id for `(target_id, source_window_seq, INGEST)` | Existing lane item is reused |
| SERVE enqueue | partial unique open row for target_id and kind SERVE | Pending work is advanced or running work leaves a durable dirty target |
| REBUILD request | deterministic work_id for target_id and operator request ID | Retrying one request reuses its exact plan |
| Worker claim | work_id, lease_token, and target fence_epoch | Only the current owner can advance control state |
| Row mutation | vector_id plus lance_etl_mutation_version and digest | Exact replay is a no-op, old delivery is ignored, and conflicting reuse blocks |
| Index build | exact data version, fragment set, artifact digest | Existing valid segments are reused or the exact phase is replanned |
| Catalog publication | target_id, expected ingest and served tuples, candidate URI, exact served version, and fence epoch | Desired mapping is success, expected prior mapping is replaced by CAS, unexpected mapping blocks |
| Database completion | work_id plus exact data, index, or catalog version | Repeating completion is a no-op |

On every ambiguous Lance commit, reopen the dataset and reconcile the durable completion marker,
exact version, row count, and digest before retrying. Never infer failure only from a client
timeout. Never blindly repeat a raw append.

Disable raw bulk append until deterministic ambiguous-outcome reconciliation exists. Disable
clustered overwrite until its memory and writer-overlap behavior are qualified. Never enable
stable row IDs.

Audit destination uniqueness by vector_id. Repair duplicates through an exclusive REBUILD that
writes a new Lance version while the serving catalog keeps readers on the old exact version.
Validate uniqueness, rebuild indexes, prewarm the exact replacement version, then publish it.

### One reconciler and minimal configuration

Delete the two old DAGs and replace them with one serialized production DAG:

1. plan_and_enqueue_window
2. run_due_target_work
3. reconcile_results
4. gate_source_retention
5. emit_slo_status

Set max_active_runs to one initially. A Spark worker may claim a bounded batch from one source
window and coalesce its partition-pruned Iceberg scan. Ownership and completion remain per target.

A BLOCKED INGEST target blocks only its own later INGEST rows and holds the relevant source
retention floor. Healthy targets continue. A failed SERVE row alerts and retries without holding
Iceberg retention or preventing a later INGEST from taking priority.

Scheduled runs accept no user parameters.

Normal production values do not live in Airflow Variables or `dag_run.conf`. Render them from one
versioned deployment profile and the secret manager. Airflow shows workflow state and restricted
repair inputs, not engine tuning.

Deployment-owned settings are limited to:

- deployment profile
- Iceberg catalog and table
- new Lance base URI
- PostgreSQL connection
- Spark connection and resource class
- object-store credentials
- Datadog identity
- search service addresses, TLS material, JWT issuer, audience, and JWKS location

Restricted operator tools may accept an existing work_id, target identity for lookup, an allowlisted
action, and dry-run. They do not invent source ranges or bypass the one-snapshot ownership model.

Remove manual time windows, datasets_file, raw index flags, TTL and tag toggles, arbitrary Spark
JSON, bucket and batch knobs, conflict retry knobs, cache knobs, probes, refine, fast-search, and
exact-scan controls from routine Airflow and public users.

Also remove the startup prewarm-targets file. It warms a drifting Latest reference and duplicates
the publication workflow. PREWARM must be invoked privately for one target and one exact validated
version. Resource sizing, index selection, retention, retry policy, and search execution policy are
code-owned profile values reviewed through normal releases.

### Exact index and catalog publication

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
5. Create an immutable work-derived Lance publication tag that pins the candidate version against
   cleanup.
6. Prewarm every serving replica against that exact URI and version.
7. Verify every required replica resolved the same URI and version.
8. In one PostgreSQL transaction, compare-and-swap served_lance_uri and served_lance_version from
   the persisted expected tuple to candidate_lance_uri and indexed_lance_version, conditional on
   ingest_lance_uri and fence_epoch still matching the work lease, then mark the work SUCCEEDED.
   Publication does not increment the fence again. An exclusive REBUILD also swaps
   ingest_lance_uri and last_applied_lance_version to the candidate dataset in this transaction.
9. Read the committed catalog and work rows back. An ambiguous database result is reconciled from
   those rows before another attempt.
10. Mirror the active version to the Lance HEAD tag as an operator convenience and GC aid.

The PostgreSQL compare-and-swap is the publication fence. Lance v8 tag updates are unconditional
object-store writes and cannot reject a stale lease holder, so HEAD is not the serving authority.
If the database client loses the transaction result, retry reads both rows. The desired catalog
tuple with SUCCEEDED work is success, the unchanged expected tuple is retryable, and every other
tuple fails closed. A stale HEAD mirror is reconciled but cannot change search results.

Candidate pins use immutable work-derived names. Never reuse or move one. Reconciliation removes
an orphan candidate pin only after the maximum worker lifetime and only when no catalog row or
retained target_work result references its URI and version.

Persist candidate_lance_uri and the expected ingest and served catalog tuples before the first
external publication action. Retain them with every recent successful work row, so retry, cleanup,
and rollback never reconstruct a URI or expected version from mutable target state.

Search resolves an authenticated logical target through the targets row and opens only its exact
served_lance_uri and served_lance_version. Cache that small mapping briefly and invalidate it on
publication. A cache may serve the prior validated version during propagation, never unvalidated
Latest. Explicit Latest, version, URI, and arbitrary tag selectors are restricted internal
administration.

Lance tags still pin each retained publication version against cleanup
([Lance tag behavior](https://lance.org/guide/tags/)). They are not a second routing catalog. Do not
add movable active and rollback tags. Rollback selects an exact prior successful publication from
retained target_work and performs the same fenced served-catalog compare-and-swap without changing
ingest_lance_uri. Routine work stays at the current ingest URI. REBUILD may publish and adopt a new
ingest URI through the same targets row without adding another PostgreSQL entity. Future
one-to-many sharding requires the deferred target_shards extension.

### Search production boundary

Keep IntakeService and StdoutSink removed from code, protobuf, docs, and tests. Do not restore a
broker or online intake path. Iceberg is the only write contract.

Before exposure:

- require gRPC TLS
- validate a bearer JWT against deployment-owned issuer, audience, and JWKS settings
- require exact tenant_id, namespace, and org_id authorization claims for the requested target
- require a separate admin role claim for internal administration
- resolve served URI and exact served version from the targets catalog server-side
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
projection, exact time range, and an optional allowlisted product fusion mode.

The server owns probes, refine, ef, fast or exact mode, filter execution mode, WAND, cache behavior,
fallback, deadlines, raw vector weights, and RRF constants. Numeric fusion tuning remains in the
code-owned profile and is never a public request field.

Replace google.protobuf.Struct results with a typed response carrying vector_id, typed projection,
score or distance, served_version, partial, and bounded warnings.

Always project vector_id internally. Deduplicate each result leg by vector_id and fuse hybrid
results by vector_id, never physical Lance row ID. Fetch a bounded surplus so deduplication does
not underfill k.

Fast search must cover every required index at the exact catalog-served version or fail closed. Fix
second-resolution time conversion so the documented millisecond half-open range has no false
positive start.

### Billion-row boundary

The initial architecture targets billions of rows across the fleet. It does not claim that one
logical target containing one billion rows meets query SLOs.

At fleet scale, Airflow must not create one scheduler task per target. One bounded Spark worker job
claims batches from target_work, and executor partitions process target-scoped work. Driver memory
must scale with new snapshot ancestry plus touched targets, never source rows, all historical
snapshots, or the fleet's full fragment inventory. Executor memory must scale with one bounded
target chunk or index shard.

Every code-owned profile needs explicit budgets for rows per fragment, fragment count, versions,
deletion ratio, schema width, index delta count, artifact bytes, build memory, build duration,
object-store requests, and query fan-out. Crossing a soft budget enqueues maintenance. Crossing a
hard qualified limit blocks publication and requires a rebuild or the sharding decision. Never turn
these budgets into Airflow or public request knobs.

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

Already committed and required to remain:

- raw bulk append defaults off
- clustered rewrite has no production command surface
- IntakeService and StdoutSink are absent
- Python HEAD publication requires an explicit target version

Commit 6cef023 also makes DatasetRef::Serve resolve the fixed production HEAD tag, removes
SEARCH_API_SERVE_BY_TAG, SEARCH_API_SERVE_TAG, and the deprecated SEARCH_API_DISK_CACHE_DISABLED
alias, adds a missing-HEAD fail-closed test, strengthens removed-variable tests, and passes the
full locked Rust gate with 180 tests, formatting, Clippy, and the release build. Do not recreate
those changes. Remaining Safety work is to remove the startup prewarm-targets file and update its
tests and documentation. Repair the benchmark in the same safety stage. It currently prewarms Latest
and sends an unpinned request that now serves HEAD, while normal benchmark ingest never creates HEAD.
Make benchmark prewarm and query use the same explicit validated version, fix historical-tag
verification to assert the returned version, and make the built-server gate mandatory at the
appropriate integration milestone. Disable scheduled source snapshot expiration and prevent
Iceberg optimization from running before ingestion until SOURCE-01 supplies a durable retention
floor. Remove stale Intake dashboard text and add a descriptor test proving the Intake service
cannot return. Seed removed Airflow variables in negative tests and prove they are ignored. Do not
fold STATE-01 or the full SEARCH-01 public API redesign into this safety stage.

Acceptance:

- production entry points cannot reach removed or qualification-only mutation surfaces
- a normal request cannot serve unpromoted Latest
- startup cannot warm an unpromoted Latest target list
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
- ordered INGEST enqueue, coalesced SERVE enqueue, SKIP LOCKED claim, lease renew, retry, success,
  and block
- source-applied completion separate from serving completion
- bounded error handling and archive policy
- reproducible real-PostgreSQL local and CI test harness

Acceptance:

- duplicate planning creates no duplicate work
- repeated dirtying creates at most one pending SERVE row per target
- concurrent claimers cannot own the same work
- the smallest unfinished INGEST sequence is the only eligible source work for a target
- expired lease can be reclaimed
- stale lease token or target fence cannot complete work
- Python and Rust reject identical routing traversal cases before storage access
- PostgreSQL restart at each transition remains recoverable

### 2. SOURCE-01

Commit intent:

~~~text
feat: plan exact Iceberg source windows
~~~

Implement:

- one source window per Iceberg snapshot
- parent-linked lineage walk and sequence ordering
- snapshot classification
- manifest-derived target and hour discovery
- exact parent-to-append-snapshot target scans and batched scan optimization
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

- required mutation_version source contract
- SHA-256 canonical identity
- exact duplicate collapse and conflict detection
- full-row null materialization
- mutation-version guarded upsert and tombstone
- completion marker
- partial-commit retry
- uniqueness audit and rebuild
- removal of cross-window timestamp guard
- disable or delete raw bulk path

Acceptance:

- 100 repeated retries converge
- an exact duplicate in a later snapshot is a no-op
- a greater mutation version wins even with older event time
- an older mutation delivered in a later snapshot is ignored
- stale zombie cannot change newer state
- omitted fields clear
- delete and recreate with increasing mutation versions works
- conflicting reuse blocks before the worker writes when visible at preflight and before completion
  when introduced by a concurrent race
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
- index, prewarm, or publication failure does not hold Iceberg retention
- unapplied or BLOCKED INGEST does hold its exact snapshot retention floor
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
- fenced targets-catalog compare-and-swap
- immutable work-derived publication pins retained for active and rollback versions
- best-effort HEAD mirror and reconciliation
- idempotent database completion
- rollback and cleanup protection

Acceptance:

- crash at every step converges
- stale worker cannot publish after losing its fence
- unexpected catalog tuple fails closed
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
- catalog-backed exact URI and version resolution
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
- rollback restores the prior catalog URI and exact served version

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
- conflicting same-version mutation across one or several snapshots
- same vector_id across hours and windows
- stale delete, delete then recreate, and null clearing
- partial salted target commit
- ambiguous index and catalog publication
- cache corruption and restart
- Redis unavailable and slow
- object-store throttle, timeout, and stale read
- global overload and graceful-drain deadline
- backup, restore, and catalog rollback

Do not weaken a failing test to make a commit green. If a test encodes an intentionally removed
interface, replace it with a test of the new invariant in the same commit.

## Execution ledger

The root agent owns this ledger. Update the current stage to DONE only in the commit that satisfies
its acceptance criteria. Git history supplies the commit hash.

Allowed states are NOT_STARTED, IN_PROGRESS, DONE, and BLOCKED.

| Stage | State | Verification | Notes or blocker |
|---|---|---|---|
| Safety removal | IN_PROGRESS | Python: 679 passed, 36 deselected. Airflow: 6 passed. Rust fmt and Clippy green, 180 locked tests passed, release build green | Commits 6c9ffaa, fd82a0c, fbac425, and 6cef023 disabled raw bulk and clustered mutation controls, removed placeholder intake, required explicit Python HEAD publication, and committed fixed-HEAD serving with a missing-HEAD fail-closed test. Startup prewarm, benchmark version mismatch, benchmark tag verification, source retention, and stale Intake docs remain before DONE. |
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
3. Stay on the current dedicated implementation branch. Create production-readiness only when the
   checkout is still on main and no dedicated branch already contains this work.
4. Commit this report alone if it is not already committed.
5. Mark Safety removal IN_PROGRESS locally.
6. Use bounded subagents for an independent safety inventory and test plan.
7. Implement the first green atomic commit.
8. Update the ledger, commit, and continue through the remaining stages.
