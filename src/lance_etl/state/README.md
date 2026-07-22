# `lance_etl.state` — PostgreSQL control plane

This package owns the durable truth of the local Iceberg-to-Lance process. It is the only code in
the repository that opens a PostgreSQL transaction against the control plane, and it is the only
code that is allowed to. Every other package that touches durable state — `reconciler/` and
`bench/` — reads and writes it exclusively through `ControlPlaneRepository`. Nothing else
constructs a raw SQL statement against these tables.

For the end-to-end reconciliation cycle and the 9-table entity summary, see the package
[README](../README.md). For the hard coding rules that govern this package, see
[AGENTS.md](../AGENTS.md) and the repository-root `AGENTS.md`. For the full column-by-column and
transaction design this package implements, see
[ADR 0042](../../../docs/adr/postgresql-dataset-control-plane.md). For how the schema in
`tables.py` becomes a running database, see the [migrations README](../../../migrations/README.md).

## Module map

| Module | Responsibility |
|---|---|
| `tables.py` | SQLAlchemy Core metadata for the 9 control-plane tables: columns, CHECK constraints, composite foreign keys, and partial unique indexes |
| `specs.py` | The immutable `DatasetSpecRevision` graph — fields, indexes, typed IVF_RQ/INVERTED options — its validation, its SHA-256 configuration digest, and the bundled default specification |
| `types.py` | Frozen dataclasses for routing identity, source registration, work claims and execution context, publication evidence, and deterministic UUIDv5 identity derivation |
| `settings.py` | `ReconcilerSettings` — environment-sourced queue, lease, retry, retention, and SLO bounds, re-exported unchanged by `reconciler/config.py` |
| `repository.py` | `ControlPlaneRepository` — the sole owner of PostgreSQL transactions: spec lifecycle, snapshot and work enqueue, claim/lease/fence, terminal transitions, and retention |
| `__init__.py` | Re-exports the package's public surface (repository, specs, types) for `from lance_etl.state import ...` |

## `tables.py`: SQLAlchemy Core, not the ORM

Every table is a plain `sa.Table` against a shared `MetaData`. There is no declarative ORM layer,
no session, and no lazy relationship loading — every read is an explicit `sa.select(...)` executed
by `repository.py`. The database itself, not application code, enforces most of the data contract:

- CHECK constraints validate identifier and name patterns, non-negative counts, and closed
  enumerations (`SPEC_STATES`, `FIELD_ROLES`, `INDEX_TYPES`, and so on, defined as module-level
  tuples and rendered into SQL through `quoted_values`).
- A single "option shape" CHECK constraint on `index_definitions` enforces that IVF_RQ rows carry
  every IVF_RQ column and no INVERTED or scalar column, and symmetrically for the other two shapes,
  so the nullable-column-plus-CHECK design never lets a row carry options for the wrong family.
- Partial unique indexes encode state machine invariants directly in PostgreSQL: at most one
  `ACTIVE` revision per `spec_id` (`uq_dataset_spec_revisions_active`), at most one `KEY`,
  `EVENT_TIME`, or `TOMBSTONE` field per revision (`uq_dataset_fields_singleton_role`), at most one
  `RUNNING` work row per dataset (`uq_dataset_work_running_dataset`), and at most one open `PUBLISH`
  generation per dataset (`uq_dataset_work_open_publish`).
- Composite foreign keys tie lineage together so a row cannot reference an object from a different
  scope: `dataset_work` requires its snapshot to belong to the same source as its dataset, and
  `publication_indexes` requires its index-definition reference to carry the same `spec_revision_id`
  as the publication it evidences.

## `specs.py`: the immutable specification graph

`DatasetSpecRevision` is the complete, reproducible contract for one dataset generation: ordered
`DatasetField` rows, ordered `IndexDefinition` rows (each optionally carrying `VectorIndexOptions`
or `FtsIndexOptions`), and every ingestion, compaction, indexing, and publication policy value. Its
`validate()` method checks the full graph — canonical field order, singleton roles, index/field
family compatibility, positive bounds — and `compute_digest()` hashes the canonical semantic
configuration with SHA-256. `production_default_spec_revision()` builds the bundled default
specification whose digest is baked into `migrations/versions/0001_control_plane.py` as
`DEFAULT_CONFIGURATION_DIGEST_HEX`: changing the default specification in code without regenerating
that constant breaks the migration's seed-row round-trip test. `decode_dataset_spec_revision` and
its row-level helpers are the inverse: they turn three joined PostgreSQL row sets back into a
validated `DatasetSpecRevision`.

## `types.py`: identities, claims, and deterministic IDs

Every durable identity in the control plane is derived, not random, so that repeated planning or
repeated claim attempts converge on the same row instead of duplicating it:

- `deterministic_dataset_id(source_id, identity)` — `uuid5` over the source and the canonical
  `RoutingIdentity` string.
- `deterministic_ingest_work_id(dataset_id, source_snapshot_seq)` — one INGEST work row per
  dataset per source generation.
- `deterministic_publish_work_id(dataset_id, source_snapshot_seq, spec_revision_id)` — coalescible:
  replanning the same generation against the same revision always names the same PUBLISH row.
- `deterministic_rebuild_work_id(dataset_id, request_id)` — one REBUILD per operator-supplied
  idempotency key.
- `deterministic_publication_id(work_id)` — one immutable publication row per work generation.

`WorkClaim` is the fenced lease handed to a worker (work and dataset identity, kind, phase, lease
token, fence epoch, attempt count). `WorkExecutionContext` is the richer, read-only bundle a worker
actually executes against — claim plus resolved routing identity, source, and spec revision.
`WorkProvenance`/`WorkLauncherKind` record who launched a claimed attempt for audit purposes only:
`WorkLauncherKind` currently defines a single value, `LOCAL`, and the label never participates in
claim eligibility, ordering, retries, leases, or fencing.

## `settings.py`: process bootstrap, not a table

`ReconcilerSettings` is loop policy — poll interval, claim batch size, lease duration and heartbeat
interval, retry backoff bounds, retention and SLO horizons — built once at process startup by
`from_environment()` and never read from PostgreSQL. `retry_delay(attempt_count, work_id)` produces
a reproducible jittered exponential backoff: the jitter seed is `sha256(f"{work_id}:{attempt_count}")`,
so the same attempt on the same work item always proposes the same delay, which keeps retry timing
deterministic in tests. This module is re-exported unchanged as `reconciler.config.ReconcilerSettings`.

## `repository.py`: transaction ownership

`ControlPlaneRepository` wraps one SQLAlchemy `Engine` and exposes every state transition as one
method, each opening exactly one `engine.begin()` transaction (or `engine.connect()` for reads).
Nothing outside this class opens a transaction against the control-plane tables.

### Specification lifecycle: DRAFT -> ACTIVE -> RETIRED

A specification exists only as its revisions — there is no separate identity table. The lifecycle
is authored through four repository methods, matching the PostgreSQL triggers installed by the
migration (see the [migrations README](../../../migrations/README.md)):

1. `create_draft_spec_revision(revision, name, description)` inserts a complete DRAFT graph — the
   revision row plus its `dataset_fields` and `index_definitions` rows — in one transaction, after
   recomputing the digest. If the revision's identity already exists, the call is idempotent only
   when the persisted content is byte-identical. Otherwise it raises `StateTransitionError`.
2. `activate_spec_revision(revision_id)` locks every revision of the same `spec_id` in
   `revision_number` order with `FOR UPDATE`, retires the current `ACTIVE` row (if any), and
   promotes the DRAFT with a single fenced `UPDATE ... WHERE state = 'DRAFT'` whose `rowcount` is
   checked. If nothing needs to change (the revision is already ACTIVE) it is a no-op read.
3. `set_source_default_spec(source_id, spec_id)` repoints a source's default specification, after
   confirming the target `spec_id` currently has an ACTIVE revision.
4. `assign_dataset_spec_revision(dataset_id, revision_id)` changes one dataset's desired revision
   and, when the dataset has already materialized different content, enqueues a deterministic
   REBUILD. Convergence is **deferred** — the pointer is recorded but no REBUILD is enqueued, and
   `None` is returned — whenever a PUBLISH generation is already open for the dataset, because a
   REBUILD frozen against today's serving pointer would be stranded the moment that PUBLISH
   advances it. The next INGEST naturally re-freezes the desired revision, so the rollout still
   lands without an orphaned work row. `tests/test_state_rebuild_publish_wedge.py` pins exactly
   this defensive layer, plus the claim-time sweep and lane-order exclusion that recover a lane if
   a REBUILD is enqueued anyway.

PostgreSQL triggers, not just this Python code, reject any attempt to mutate ACTIVE or RETIRED
content — the repository's checks and the database's triggers are independent layers of the same
invariant.

### Work claiming: `FOR UPDATE SKIP LOCKED`, lease, and fence

`claim_due_work(limit, lease_duration, now, provenance)` is the only way a worker acquires
dataset-scoped work, and it is dataset-disjoint by construction:

1. `release_expired_leases` returns any `RUNNING` row whose `lease_expires_at` has passed to
   `RETRY_WAIT`.
2. `sweep_stale_work` moves any pending non-INGEST row whose frozen `expected_*` columns no longer
   match live `datasets` state to `BLOCKED` — such a row can never be claimed again, and leaving it
   pending would otherwise wedge `lane_order_predicate` for the whole dataset lane.
3. A due-work query joins `dataset_work` to `datasets`, filters by `due_predicate` (PENDING or
   RETRY_WAIT and `next_attempt_at <= now`), `lane_order_predicate` (every earlier still-reachable
   generation in the lane has already succeeded), and `expected_state_predicate` (frozen
   expectations still match dataset state), orders by source sequence then INGEST-before-others,
   and locks candidate rows with `.with_for_update(of=dataset_work, skip_locked=True)`. It
   over-fetches (`limit * 4`) because rows are then deduplicated in Python to at most one claim per
   dataset (`claimed_datasets`), so a concurrent worker can never receive two claims for the same
   dataset in one call.
4. Each surviving row is claimed individually in `claim_locked_row`: the owning `datasets.fence_epoch`
   is incremented, a fresh `uuid4()` lease token is minted, `attempt_count` increments, and the row
   moves to `RUNNING` with a bounded expiry. INGEST claims also re-freeze their `expected_*` columns
   from the current dataset row at this point, since a reclaimed INGEST must always target the
   dataset's live serving pointer.

Every later transition against a claim — `renew_lease`, `advance_phase`, `retry_work`, `block_work`,
`complete_ingest`, `publish_dataset` — is gated by `lease_is_current`, a single predicate requiring
all four of: `state = RUNNING`, `lease_token` equals the claim's token, `lease_expires_at > now`,
and the owning dataset's live `fence_epoch` still equals the claim's `fence_epoch`. A heartbeat
renewal (`renew_lease`) extends `lease_expires_at` **without** touching the fence. A brand-new claim
always bumps the fence. This quad — state, token, expiry, fence — is what lets a superseded worker
(one whose lease expired and was reclaimed by someone else) be rejected even if it is still alive and
still holds its old token: the fence moved out from under it.

### Terminal transitions

`complete_ingest` and `publish_dataset` share the same shape: lock the work row and its dataset row,
accept an identical replay as a no-op (`completed_ingest_matches` / the `dataset_publications` row
already existing with matching evidence), otherwise re-validate the lease and frozen expectations
under the lock, then commit. `complete_ingest` advances the mutable `datasets` row (materialized
revision, applied snapshot, ingest URI and version), marks the work row `SUCCEEDED`, atomically
enqueues the coalescible PUBLISH work item, and completes the source snapshot if every ingest work
item for it has now succeeded. `publish_dataset` inserts the immutable `dataset_publications` row
and its `publication_indexes` evidence rows (each validated against the frozen specification by
`validate_publication_indexes`), retires the previous active publication, and swaps
`datasets.active_publication_id` — all in the same transaction, so the serving catalog join
(`datasets -> dataset_publications`) never observes an intermediate state. A REBUILD's success also
moves `ingest_lance_uri`/`ingest_lance_version`/`materialized_spec_revision_id`, since a REBUILD
replaces the mutable ingest generation as well as the published one.

### Retry, blocking, and retention

`retry_work` returns a transient failure to `RETRY_WAIT` with a code-owned backoff delay, or to
`BLOCKED` with `MAX_ATTEMPTS_EXHAUSTED` once `attempt_count` reaches the bootstrap-configured
`max_attempts`. `block_work` records a terminal contract failure directly. `retry_blocked_work` is
the only way an operator repair (`lance-etl-reconcile repair --action retry-blocked`) reopens a
blocked row, and it does not touch source ownership or fencing beyond returning the row to PENDING.
`select_retention_floor` / `retention_floor` compute the exact oldest source snapshot that Iceberg
expiration must still preserve (blocked, still-open, or inside the source's replay horizon).
`claim_publication_cleanup` / `finalize_publication_cleanup` hand retired publications to the
reconciler's external cleanup sweep (see [`reconciler/README.md`](../reconciler/README.md)) before
their audit rows are deleted, and `delete_completed_audit` prunes old succeeded work and source
snapshot rows past the audit retention horizon.

## Invariants a maintainer must not break

- **Exactly 9 tables, no JSON configuration blob.** Every new data-path option is a typed, validated
  column on the appropriate normalized entity and is included in the specification digest — never a
  freeform document column. See ADR 0042's "Configuration boundary" section.
- **`repository.py` is the only transaction owner.** Do not open `engine.begin()` or
  `engine.connect()` against these tables anywhere outside this module.
- **Immutability is enforced twice.** Both the repository (`activate_spec_revision`'s DRAFT-only
  gate, `create_draft_spec_revision`'s round-trip check) and the PostgreSQL triggers installed by
  the migration reject content changes to ACTIVE or RETIRED specification graphs. Do not remove
  either layer to "simplify" the other.
- **`launcher_kind` is audit-only.** It records who claimed a work item and nothing else. Do not
  make it participate in claim eligibility, ordering, lease duration, or fencing, and do not add a
  second launcher kind without also updating the CHECK constraint and the enum in `types.py`.
- **Fence semantics are load-bearing.** A lease renewal must never advance `fence_epoch`, and a
  fresh claim must always advance it. Breaking either direction reopens the exact concurrency bug
  the fence exists to close (a superseded worker completing a claim it no longer owns).
- **No stable row IDs.** This package is pure PostgreSQL and never opens a Lance dataset itself, but
  it upholds the repository-wide rejection by never introducing a state column or claim contract
  that would require `enable_stable_row_ids` in the workers this control plane drives. See
  [ADR 0010](../../../docs/adr/rejected-and-operator-tools.md).

## Testing

| Test file | Covers |
|---|---|
| `tests/test_state_postgres.py` | Real-PostgreSQL: migration entity/seed shape, spec lifecycle, snapshot enqueue idempotency, claim/lease/fence expiry, ingest-to-publish transitions, retry/block bounds, retention floor, REBUILD, and retired-publication cleanup |
| `tests/test_state_rebuild_publish_wedge.py` | Real-PostgreSQL: the REBUILD-versus-open-PUBLISH wedge and its three defensive layers |
| `tests/test_state_specs.py` | Pure-Python: `DatasetSpecRevision` validation, digesting, and the bundled default specification |
| `tests/test_state_types.py` | Pure-Python: dataclass validation and deterministic UUID derivation |
| `tests/test_postgres_queue_load.py` | Real-PostgreSQL: bounded claim contention under concurrent workers |

Every real-PostgreSQL test is gated on the `LANCE_ETL_TEST_DATABASE_URL` environment variable
pointing at a disposable local PostgreSQL database and is skipped otherwise. Set it before working
on anything in this package:

```bash
export LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl_test'
.venv/bin/pytest tests/test_state_postgres.py tests/test_state_specs.py tests/test_state_types.py \
  tests/test_state_rebuild_publish_wedge.py -m "not integration"
```
