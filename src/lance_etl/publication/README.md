# `lance_etl.publication`

`publication/` owns the small set of primitives that make one Lance version safe to call "the
qualified answer for this dataset": an immutable, work-derived version pin, an exact Arrow schema
digest, and the rule that a candidate can go live only once every required serving replica confirms
it independently. It has no driver loop, no PostgreSQL access, and no CLI of its own. Everything
here is a pure, side-effect-light library imported by the local reconciler, most heavily
`reconciler/workers.py`, plus `reconciler/prewarm.py` and `reconciler/retention.py`. For the
reconciliation cycle this package sits inside — plan, claim, ingest, compact, index, publish,
prewarm, retire — see the package [README](../README.md) and [AGENTS.md](../AGENTS.md).

This is the smallest of the three packages covered by this documentation set: two modules, under
100 lines combined. Read the module-by-module table below, then the workflow section for how those
few functions add up to the exact-version publication contract used across the whole reconciler.

## Module-by-module

| File | Responsibility |
|---|---|
| `manifest.py` | Work-derived candidate pin naming, Lance v8 tag resolution, and Arrow schema fingerprinting |
| `workflow.py` | `PrewarmResult` evidence type and the all-replicas-agree prewarm validation rule |

### `manifest.py`

| Symbol | Purpose |
|---|---|
| `PIN_PATTERN` | Compiled allowlist regex, `^candidate-[0-9a-f]{32}$`, that every pin name must match |
| `candidate_pin_name(work_id: uuid.UUID) -> str` | Derives `candidate-{work_id.hex}` and raises `ValueError` if it somehow fails the allowlist |
| `tag_version(dataset: lance.LanceDataset, name: str) -> int \| None` | Wraps `dataset.tags.get_version(name)`, normalizing lance's "tag does not exist" `ValueError` into `None` instead of letting callers catch exceptions ad hoc |
| `schema_fingerprint(schema: pa.Schema) -> str` | SHA-256 hex digest of `schema.serialize().to_pybytes()`, so field order, types, nullability, and field-level metadata are all covered |

### `workflow.py`

| Symbol | Purpose |
|---|---|
| `PrewarmResult` | Frozen slotted dataclass: `replica: str`, `lance_uri: str`, `lance_version: int` — one replica's exact resolution |
| `validate_prewarm(results, candidate_lance_uri, indexed_lance_version) -> None` | Raises `ValueError` unless every configured replica reported, replica names are unique, and every result matches the expected URI and version exactly |

## The exact-version publication contract

A publication is not "compaction and indexing finished." It is a specific, immutable Lance
`(uri, version)` pair that has been pinned so it can never be mutated out from under a consumer, has
had its schema and row/fragment/index counts verified against the frozen `dataset_spec_revisions`
row, and — when the spec requires it — has been independently confirmed openable by every serving
replica. `ConfiguredPublicationRunner.run` in `reconciler/workers.py` is the orchestrator. This
package supplies the three primitives that make that orchestration safe.

### 1. The candidate pin

Each durable work row gets exactly one immutable Lance tag, so retries and duplicate `PUBLISH`/
`REBUILD` attempts converge on the same physical version instead of racing to create different
tags. `candidate_pin_name(work_id)` derives the tag name deterministically from the work UUID
(`candidate-{work_id.hex}`), so the pin name is reproducible from durable state alone with no side
table.

`ConfiguredPublicationRunner.pin_candidate` (`reconciler/workers.py`) creates the tag on an executor
after maintenance and indexing succeed:

1. Look up any existing tag with `tag_version`. If absent, call `dataset.tags.create(name,
   version)`.
2. If `tags.create` raises `ValueError` (a concurrent creator won the race), re-read the tag with
   `tag_version` again. If it still resolves to nothing, re-raise — this was a real failure, not a
   race.
3. If the resolved tag names a version other than the one just qualified, raise
   `ReplayConflict("immutable candidate pin names a different exact version")`. A pin is immutable:
   it is created once and never repointed.

`ConfiguredPublicationRunner.candidate_pin_version` uses `tag_version` to check for an
already-qualified candidate before redoing maintenance and indexing at the top of `run` — this is
what makes a retried `PUBLISH`/`REBUILD` work item skip straight to qualification when the pin
already exists.

The pin is retired, not immediately deleted, when a newer publication supersedes it.
`reconciler/retention.py` later reads the same `candidate_pin_name` / `tag_version` pair to resolve
and delete the tag (`dataset.tags.delete(pin)`) as part of bounded, crash-resumable publication
cleanup, once `state.repository` has confirmed externally that the row is safe to reclaim (not the
active publication, not referenced by open work). See
[ADR 0036](../../../docs/adr/fleet-orchestration-and-maintenance.md) (idle-dataset cleanup rotation)
for the retention bounds that gate this sweep.

### 2. The schema digest

`schema_fingerprint` hashes the serialized Arrow schema, including field metadata, so
`publication_evidence` (`reconciler/workers.py`) can store a 32-byte `schema_digest` on
`dataset_publications` that later reads can compare bit-for-bit against a freshly resolved schema
without re-deriving equivalence rules. The digest is schema-metadata sensitive by design — two
schemas that differ only in field metadata fingerprint differently, verified directly by
`tests/test_publication.py`.

### 3. All-replicas-agree prewarm

When the frozen spec sets `prewarm_required`, `ConfiguredPublicationRunner.run` calls
`self.prewarmer.prewarm(identity, candidate_uri, candidate_version)` before returning a
`PUBLISH_SUCCEEDED` result. The `ExactPrewarmer` protocol (`reconciler/prewarm.py`) returns a tuple
of `PrewarmResult`, one per replica that answered. `validate_prewarm` is the gate:

- At least one replica must have responded (`ValueError: publication requires at least one serving
  replica prewarm` on an empty tuple).
- Replica names must be unique (`ValueError: prewarm results contain duplicate replicas`).
- Every replica's `lance_uri` and `lance_version` must exactly equal the candidate's — any mismatch
  raises `ValueError: serving replicas resolved a different publication candidate: [...]` naming the
  offending replicas.

`LocalExactVersionPrewarmer` (`reconciler/prewarm.py`) is the one production implementation: it
opens the candidate at the exact pinned version on a local Spark executor
(`lance.dataset(uri, version=version)`), re-checks that the opened dataset reports the same URI and
version back, and returns a single `PrewarmResult("local", uri, version)`. A prewarm failure returns
a retryable `WorkResult` (`PREWARM_FAILED`) rather than blocking the dataset outright, since a
transient replica outage should not poison the candidate pin.

### How the pieces compose in `reconciler/workers.py`

`ConfiguredPublicationRunner.run` (`reconciler/workers.py:673`) is the actual publication workflow.
Reading top to bottom:

1. Resolve `candidate_uri` (ingest URI for `PUBLISH`, rewritten canonical URI for `REBUILD`).
2. Resolve `pin = candidate_pin_name(work_id)` and look up any already-qualified `candidate_version`
   via `candidate_pin_version`.
3. If unpinned: run maintenance (`MaintenanceJob`, see the [maintenance
   README](../maintenance/README.md)) when `spec.compaction_enabled`, then `run_indexing` (see the
   [indexing README](../indexing/README.md)) for every `spec.index_definitions` entry, then
   `pin_candidate` to mint the immutable tag.
4. Run `qualify_candidate`: count rows/fragments and verify every required index reports full
   coverage.
5. `persist_manifest` writes the qualification manifest artifact, and `publication_evidence`
   converts the qualification dictionary into a typed `PublicationEvidence` /
   `PublicationIndexEvidence` graph (`state/types.py`), including `schema_fingerprint(dataset.schema)`
   as the stored `schema_digest`.
6. If `spec.prewarm_required`, call the `ExactPrewarmer` and gate on `validate_prewarm`.
7. Return `WorkResult(kind=PUBLISH_SUCCEEDED, ...)` carrying the candidate URI, exact version,
   manifest URI/digest, and the typed evidence.

`state/repository.py` (`publish_generation`, around line 2200) is where that evidence becomes
durable: it inserts one `dataset_publications` row and one `publication_indexes` row per index
inside a single transaction, retires the dataset's previous `active_publication_id` (sets
`retired_at`), and atomically repoints `datasets.active_publication_id` at the new publication —
the same transaction that, for a `REBUILD`, also updates `ingest_lance_uri`,
`ingest_lance_version`, and `materialized_spec_revision_id`. `validate_publication_indexes`
(`state/repository.py`) is the durable-side cross-check: it compares each `PublicationIndexEvidence`
against the frozen `index_definitions` row for that spec revision, so evidence can never silently
drift from what the specification actually required.

One resolution quirk documented in [`AGENTS.md`](../AGENTS.md) matters here: `describe_indices()`
reports an INVERTED index published through the FTS atomic swap as `Unknown` because the hand-built
`Index` record carries no index details. `resolved_actual_index_kind` and `observed_index_type`
(`reconciler/workers.py`) fall back to `stats.index_stats(name)["index_type"]`, which is
version-independent and always carries the true kind, before the observed kind is recorded as
`PublicationIndexEvidence.actual_index_type`. This is why the evidence this package's types carry is
a genuine cross-check against the specification rather than a trivial restatement of the configured
type.

## How the reconciler consumes the manifest

Downstream readers never resolve "the latest version" by listing a Lance directory. They read
`datasets.active_publication_id -> dataset_publications.(lance_uri, lance_version)` from PostgreSQL,
which is the exact immutable pair validated above. The Rust search service does the same lookup (see
`../../../rust/search-api/AGENTS.md`) — it does not open Lance version history itself, it trusts the
row `lance_etl.publication` and the reconciler's PostgreSQL transaction already qualified.
`dataset_publications` additionally stores `manifest_uri`/`manifest_digest` (the qualification
artifact from `persist_manifest`) and `source_snapshot_seq`, so a publication is traceable back to
the exact Iceberg snapshot lineage it was built from — see the `source_snapshots` table description
in the package [README](../README.md#postgresql-entities).

## Invariants a maintainer must not break

- **A candidate pin is immutable.** Never repoint an existing `candidate-{work_id.hex}` tag to a
  different version. `pin_candidate` enforces this by raising `ReplayConflict` rather than
  overwriting — do not add a "force retag" path.
- **Prewarm must be all-or-nothing across configured replicas.** Do not weaken `validate_prewarm` to
  accept a majority or a best-effort subset — a mismatched replica means it is still serving stale
  data and must block publication, not degrade it silently.
- **The schema digest must stay metadata-sensitive.** Do not switch `schema_fingerprint` to a
  types-only or names-only hash — field metadata (Arrow field-level `KeyValueMetadata`) is part of
  what makes two schemas the same schema for qualification purposes.
- **No stable row IDs anywhere in this path.** Prewarm, pinning, and qualification all key on Lance
  version, not row ID — see root [`AGENTS.md`](../../../AGENTS.md) hard rule 8 and
  [ADR 0010](../../../docs/adr/rejected-and-operator-tools.md).
- **No raw SQL, no bespoke scheduler.** This package and its callers stay inside the typed
  SQLAlchemy Core repository layer. Do not add ad hoc SQL strings or a parallel state machine per
  root `AGENTS.md` hard rule 9 and 10.

## How to invoke

This package has no CLI and is never run standalone — it is a pure library imported by
`reconciler/workers.py`, `reconciler/prewarm.py`, and `reconciler/retention.py`. The only way to
exercise the full publication workflow end to end is through the reconciler:

```bash
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl'
uv run lance-etl-reconcile run-once
```

See the package [README](../README.md#commands) for the full reconciler command set.

## Testing pointers

| Test file | Covers |
|---|---|
| `tests/test_publication.py` | `candidate_pin_name`, `tag_version`, `schema_fingerprint`, `validate_prewarm` in isolation |
| `tests/test_reconciler_prewarm.py` | `LocalExactVersionPrewarmer` and the `ExactPrewarmer` protocol against `PrewarmResult` |
| `tests/test_local_runtime.py` | Runtime wiring that constructs the prewarmer used by `ConfiguredPublicationRunner` |
| `tests/test_reconciler_retention.py` | Publication-pin cleanup via `candidate_pin_name` / `tag_version` in `reconciler/retention.py` |

The full publish path (`ConfiguredPublicationRunner.run`, `publication_evidence`,
`validate_publication_indexes`, the atomic `active_publication_id` swap) is exercised through the
reconciler's own worker and repository tests rather than through this package's tests directly —
search `tests/` for `ConfiguredPublicationRunner`, `publish_generation`, and
`validate_publication_indexes` for that coverage.
