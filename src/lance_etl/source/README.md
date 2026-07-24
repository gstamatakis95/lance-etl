# `lance_etl.source`

Pure Iceberg source contracts, lineage validation, manifest classification, and exact Spark scan
construction. This package contains no PostgreSQL or Lance access. Spark is confined to the small
execution adapter in `scans.py`.

The durable runtime planner lives in `reconciler/iceberg.py`. It is the only top-level planning path.
It pins one Iceberg metadata generation, validates the registered source against the latest durable
checkpoint, walks a bounded direct-parent prefix, classifies each exact snapshot, verifies that the
live catalog still exposes the observed ancestry, and returns a `SourcePlan` for PostgreSQL enqueue.

## Modules

| Module | Responsibility |
|---|---|
| `models.py` | Frozen source metadata, manifest, target, window, rejection, and scan records |
| `errors.py` | Bounded planning, contract, lineage, baseline, and blocked-snapshot failures |
| `contract.py` | Fixed table identity and partition specification validation |
| `lineage.py` | Strict direct-parent snapshot chronology validation |
| `manifests.py` | Physical-change classification and deterministic target discovery |
| `scans.py` | Exact baseline or parent-to-child Spark reads for one durable work item |

There is intentionally no generic source-planner protocol. The former abstract planner duplicated
the durable provider while omitting PostgreSQL fences, bounded backlog behavior, final catalog
verification, and durable rejection handling.

## Source contract

The active Iceberg partition specification must contain exactly four ordered fields. The first
three are identity transforms for tenant, namespace, and organization. The fourth is `ts_hour`
with an `hour` transform over the canonical `ts` column. Once a checkpoint exists, the table
UUID and partition specification identity cannot change.

## Lineage

Lineage follows direct parent pointers from one pinned head to the durable checkpoint. Commit time
and snapshot identifier ordering are not authoritative. Validation rejects cycles, missing
ancestors, forks, non-increasing sequence numbers, table identity changes, and partition
specification changes.

The concrete adapter resolves ancestry from one immutable Java Iceberg metadata generation. It
retains only the bounded suffix needed by the current reconciliation cycle and then reloads current
metadata to prove that the live catalog name still identifies the same table and retains every
planned snapshot.

## Manifest classification

Append and fast-append snapshots are accepted only when changed entries belong to the snapshot,
no file is removed, no delete file is added, and any declared nonempty append has matching added
data-file evidence. Authenticated no-change maintenance rewrites may become
`TRUSTED_MAINTENANCE`. All physical deletes and untrusted rewrites become exact blocked evidence.

Target discovery returns only a sorted tuple of unique `TargetKey` values. Manifest partition
hours are validated as source evidence but are not copied into planning state because PostgreSQL
work and ingestion never consume them. The concrete adapter collects at most 10,000 distinct
target-hour manifest facts for one snapshot before reducing them to unique target identities.

## Plans and scans

`WindowPlan` contains only an exact snapshot, its accepted kind, and its target identities.
`SourcePlan` adds the pinned head, accepted window prefix, optional durable planning fence, and
optional first rejected child. Active partition identity already lives on each snapshot and is not
duplicated at plan level.

Spark scan state exists only while an `INGEST` work item executes. `snapshot_scan_options` returns
one `snapshot-id` for a baseline or an exclusive `start-snapshot-id` plus inclusive
`end-snapshot-id` for an append. `execute_spark_scan` reads those exact bounds, filters the canonical
route columns, and stamps the immutable Iceberg sequence number onto every row.

## Tests

`tests/test_source_planner.py` exercises the pure contract, lineage, manifest, target, and scan
primitives. `tests/test_reconciler.py` covers the single durable planning path, including baseline
qualification, bounded catch-up, exact rejection, source fences, and final catalog verification.
Real Iceberg behavior is covered by the integration-marked local end-to-end tests.
