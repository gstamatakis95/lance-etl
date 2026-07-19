# Local Iceberg-to-Lance architecture overview

## Purpose

`lance-etl` keeps per-route Lance datasets synchronized with one registered Iceberg table. It runs
on one machine. PostgreSQL is the durable control plane, Spark is a local execution dependency, and
the optional Rust gRPC process serves exact published Lance versions.

The logical dataset key is `(tenant_id, namespace, org_id)`. There is one Lance dataset and one
active publication per key. Search is always scoped to one key.

## Component view

```text
Local Iceberg catalog and table
            |
            v
Python reconciler -> local Spark executors -> Lance datasets and index artifacts
            |                                      |
            v                                      v
       PostgreSQL <---------------- immutable publication evidence
            |
            v
optional local Rust search-api -> exact URI and version -> vector, text, hybrid search
```

The reconciler creates and stops Spark. PostgreSQL stores all durable planning, retry, fencing,
configuration, and publication state.

## PostgreSQL model

The application schema has exactly 9 tables:

| Area | Entities | Meaning |
|---|---|---|
| Dataset contract | `dataset_spec_revisions` | Immutable numbered behavior carrying its own `spec_id`, `name`, and `description` |
| Schema | `dataset_fields` | Ordered roles, types, nullability, and source projection |
| Indexes | `index_definitions` | Required Lance indexes with typed IVF_RQ and INVERTED options as nullable columns gated by per-type CHECK constraints |
| Source | `iceberg_sources`, `source_snapshots` | Registered table, storage namespace, column mapping, exact lineage, and blocked evidence |
| Dataset | `datasets` | First-class logical identity plus the mutable materialization cursor, fence, and active publication pointer |
| Execution | `dataset_work` | Deterministic work, current lease, attempt count, latest error, and exact fence state |
| Publication | `dataset_publications`, `publication_indexes` | Immutable Lance version and complete qualification evidence |

Dataset specifications store all schema and data-path policy. Loop policy is process bootstrap
configuration read from environment variables, not a database table. Environment values only start
local processes and identify first-run resources.

## Dataset specification

One active immutable revision includes:

- target field names, roles, physical types, source columns, map keys, vector dimensions, and
  nullability
- ingestion shuffle partitions, merge chunk rows, merge batch bytes, and rows per fragment
- compaction mode, target fragment sizing, source fragment and thread limits, index remap behavior,
  deletion materialization, cleanup horizon, and Lance version retention
- index task fan-out, delta consolidation bound, and stale-plan retry bound
- every index name, type, field, and order
- IVF_RQ distance metric, partition sizing, row floor, RaBitQ bits, streaming training, refinement,
  and retraining policy
- INVERTED positions, tokenizer, language, and maximum unindexed fragments
- local prewarm requirement, retained publication count, and artifact retention horizon

The revision digest covers every semantic value. Each work row and publication retains the exact
revision ID.

Configuration follows a strict DRAFT to ACTIVE to RETIRED lifecycle. A repository transaction
inserts each complete normalized DRAFT and recomputes its digest. Activation retires the former
ACTIVE revision. PostgreSQL triggers freeze the parent plus every field, index, and option after
activation. Source defaults and dataset assignments accept only specs with an ACTIVE revision.
Assigning a new desired revision to materialized data creates one deterministic REBUILD request.

## Source planning

The first run registers the Iceberg table UUID, catalog-qualified name, Lance storage namespace,
optional canonical baseline, replay horizon, and source-column mapping. PostgreSQL is authoritative
thereafter.

Planning follows direct parent snapshot lineage. Snapshot IDs are opaque identities. The process
records each accepted or rejected transition with sequence number, partition spec, operation, and
commit time. Unsupported source history becomes durable blocked evidence.

Qualified manifest entries reveal touched dataset routes. Planning creates new `datasets` rows and
deterministic ingest work in the same transaction.

## Work lifecycle

```text
PENDING -> RUNNING -> SUCCEEDED
             |
             v
         RETRY_WAIT -> RUNNING
             |
             v
           BLOCKED
```

Work kinds are `INGEST`, `PUBLISH`, and `REBUILD`. Processing phases are `INGEST`, `COMPACT`,
`INDEX`, `VALIDATE`, `PREWARM`, and `PUBLISH`.

Claiming uses `FOR UPDATE SKIP LOCKED`, a fresh lease token, and a higher dataset fence. Only one
work row may run per dataset. Later source snapshots cannot pass earlier unfinished ingest work.
Retry keeps the same work identity and increments its attempt count. The current token, lease
expiry, attempt count, and latest bounded error remain on that work row. The dataset fence rejects
results from superseded claims.

The same row records a `launcher_kind` audit label. It never changes queue ordering or work
eligibility.

## Data path

Ingestion reads one exact Iceberg transition and collapses terminal mutations per record. A source
sequence and content digest make replay deterministic. Heavy scans, Lance reads, merges, compaction,
and index segment construction run in Spark executor closures.

Compaction and indexing use the policy frozen in the work's specification revision. IVF_RQ,
BTREE, BITMAP, ZONEMAP, and INVERTED follow the type-specific segment recipes in `AGENTS.md`.

## Qualification and publication

A candidate is not serving merely because a Lance commit succeeded. Qualification records:

- exact URI and version
- schema digest
- total, distinct, live, and distinct-live row counts
- fragment count
- manifest URI and digest
- evidence for every required index, including indexed and unindexed fragment counts
- optional index artifact-generation digest

Local exact-version prewarm runs when required. Publication then appends immutable evidence, retires
the former publication, and changes `datasets.active_publication_id` in one transaction. A
failure leaves the former publication active.

## Search path

The local Rust service accepts only a logical target and typed query values. Its catalog resolves:

```text
datasets -> dataset_publications
```

The Lance backend receives one allowlisted URI and exact version. Clients cannot select a physical
route, mutable tag, raw SQL filter, or search execution knob.

## Local commands

```bash
createdb lance_etl
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl'
uv run lance-etl-reconcile migrate
uv run lance-etl-reconcile run-once
uv run lance-etl-reconcile status
```

For continuous local operation:

```bash
uv run lance-etl-reconcile run
```

Restricted repair can retry one blocked work row or enqueue a deterministic rebuild. It does not
edit serving pointers directly.

## Safety properties

- Exact Iceberg lineage, not time windows, defines source progress.
- PostgreSQL is the only durable state machine.
- One dataset lane prevents overlapping mutations.
- Fencing rejects stale executors after lease loss.
- Replay markers reconcile ambiguous Lance commit outcomes.
- Qualification and local prewarm precede atomic publication.
- Retention protects active, pending, and replay-required evidence.
- Move-stable row IDs remain prohibited.
