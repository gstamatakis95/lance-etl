# `lance_etl.source`

Exact Iceberg snapshot discovery and deterministic, side-effect-free replay planning. Everything in
this package is pure: no PostgreSQL, no Lance, and (outside `scans.py`) no Spark. The reconciler's
`reconciler/iceberg.py` implements the `SourceCatalog` protocol against the real Iceberg metadata
tables and drives `SourcePlanner`, and `reconciler/planning.py` maps the resulting `SourcePlan`
onto durable PostgreSQL work rows. See the [package README](../README.md) for how a `SourcePlan`
becomes `dataset_work` and [AGENTS.md](../AGENTS.md) for the Iceberg 1.10 snapshot-window API note
(`start-snapshot-id`/`end-snapshot-id`, never `start-timestamp`/`end-timestamp`).

## Modules

| Module | Responsibility |
|---|---|
| `models.py` | Frozen dataclasses and enums shared by every other module: `SnapshotRecord`, `ManifestEntry`, `PartitionSpec`, `TargetKey`, `TouchedTarget`, `SourcePlan`, `WindowPlan`, `SparkScanPlan`, `WindowKind`, and the trust/baseline evidence types |
| `errors.py` | The `SourcePlanningError` hierarchy: `SourceContractError`, `SourceLineageError`, `SourceSnapshotBlockedError` (carries `snapshot_id` and a bounded `error_code`), `SourceBaselineError` |
| `contract.py` | Validates the fixed partition contract (`tenant_id`, `namespace`, `org_id`, `hour(ts)`) and that table identity has not drifted since the last recorded checkpoint |
| `lineage.py` | Walks direct-parent Iceberg snapshot ancestry from a pinned head back to a recorded stopping point, rejecting cycles, gaps, forks, and non-monotonic sequence numbers |
| `manifests.py` | Classifies one snapshot (`APPEND`, `TRUSTED_MAINTENANCE`, or a blocked exception) from its manifest entries, and discovers the `(tenant_id, namespace, org_id)` targets and pruning hours it touched |
| `planner.py` | `SourcePlanner` — the `SourceCatalog` protocol and the top-level `plan()` entry point that ties contract validation, lineage walking, classification, and target discovery together into one `SourcePlan` |
| `scans.py` | Builds one deterministic `SparkScanPlan` per target per window and executes it against a real Iceberg-backed `SparkSession` |

## The source contract

`REQUIRED_PARTITION_FIELDS` in `contract.py` fixes the active Iceberg partition specification to
exactly four fields in order: `tenant_id`, `namespace`, `org_id` (all `identity` transforms), and
`ts_hour` (`hour(ts)`). `validate_table_contract` checks this on every planner run and additionally
requires, once a `SourceCheckpoint` exists, that the table UUID and the active partition spec id
have not changed since that checkpoint — an operator cannot silently repoint a registered source at
a different table or reshape its partitioning underneath a running pipeline.

## Lineage validation (`lineage.py`)

`walk_snapshot_lineage` walks strictly by parent pointers from the pinned head snapshot back to an
exclusive stopping snapshot, never by commit timestamp or snapshot id ordering (both are
non-authoritative in Iceberg). It raises `SourceLineageError` on a cycle, a missing ancestor before
reaching the stop point, a stop snapshot that turns out not to be an ancestor of the pinned head, or
Iceberg sequence numbers that fail to increase strictly along the walked chain — including the
boundary check that the first descendant's sequence number exceeds the recorded ancestor's. Every
visited snapshot is also checked against the expected table UUID and partition spec id
(`validate_snapshot_identity`), so a lineage walk fails closed the moment identity drifts anywhere
along the chain, not only at the endpoints.

## Snapshot classification and blocked evidence (`manifests.py`)

`classify_snapshot` accepts only two logical operations for direct replay:

- `append`/`fast-append` becomes `WindowKind.APPEND`, but only if every manifest entry belongs to
  the snapshot itself (`status != EXISTING` for entries from other snapshots is rejected as
  `MANIFEST_SNAPSHOT_MISMATCH`), no entry is `DELETED` (`PHYSICAL_DELETE`), and no `ADDED` entry
  carries delete-file content (`DELETE_FILE`).
- `replace`/`overwrite` becomes `WindowKind.TRUSTED_MAINTENANCE` only when a `MaintenanceTrust`
  record proves an authenticated, allowlisted writer performed a manifest-invariant-preserving,
  logically-no-change rewrite with no delete file present (`validate_trusted_rewrite`). Anything
  short of every one of those conditions raises `UNTRUSTED_REWRITE`.

Every other operation, and any `delete`/`row_delta` snapshot, raises `blocked_snapshot_error` with a
bounded `error_code` (`PHYSICAL_DELETE`, `DELETE_FILE`, `MANIFEST_SNAPSHOT_MISMATCH`,
`UNTRUSTED_REWRITE`, `PARTITION_SPEC_CHANGED`, `UNKNOWN_OPERATION`) and the offending
`snapshot_id`, both of which the reconciler persists as durable `source_snapshots` blocked evidence
so a later replay attempt must present the identical classification rather than being silently
retried past. `discover_added_targets` (for `APPEND` windows) and `discover_baseline_targets` (for
the initial canonical baseline) turn a snapshot's manifest entries into a deterministic, sorted
tuple of `TouchedTarget` records, each carrying the sorted set of pruning hours the target's data
files span. `validate_entry_spec` rejects any entry that used a different partition specification
than its owning snapshot.

## Planning (`planner.py`)

`SourcePlanner.plan(table, checkpoint, baseline)` is the single entry point:

- With no `checkpoint` (first run for this source), it delegates to `plan_from_baseline`, which
  requires a separately validated `BaselineProof` (`validate_baseline_proof` checks table UUID,
  partition spec id, `canonical=True`, and zero `distinct_mutation_conflicts`) and returns a
  `WindowKind.BASELINE` window for the baseline snapshot followed by classified windows for every
  strict descendant.
- With a `checkpoint`, it walks lineage from the pinned current head back to the checkpoint's
  snapshot, requires the first descendant's sequence number to exceed the checkpoint's recorded
  sequence, and classifies each descendant snapshot in ancestry order via `plan_snapshot`.
- With no current snapshot at all (an empty table), it returns an empty `SourcePlan` immediately.

Every window's Spark scans are built by `build_window`, which calls `build_spark_scan` once per
touched target — never for `TRUSTED_MAINTENANCE` windows, which by definition carry no Lance-bound
data change. `SourcePlan.retention_snapshot_id()` returns the oldest snapshot the plan still needs
(the baseline snapshot itself for a baseline plan, or the first window's parent for an incremental
plan), which is the Iceberg-side retention floor a source-table optimization pass must not prune
past while this plan is in flight.

## Spark scans (`scans.py`)

`build_spark_scan` renders one window into Iceberg reader options: a single `snapshot-id` option
for a baseline window, or `start-snapshot-id`/`end-snapshot-id` bounding the exclusive-to-inclusive
parent-to-snapshot range for an append window (raising `SourcePlanningError` if the snapshot lacks a
direct parent). `execute_spark_scan` applies those options plus a typed equality filter on the three
routing columns and stamps `lance_etl_source_sequence` (the snapshot's Iceberg sequence number) onto
every row via `withColumn`, which is what lets `etl/mutation.py` and `etl/replay_sink.py` order
mutations without re-deriving sequence identity downstream.

## Tests

`tests/test_source_planner.py` is the primary unit-test suite: pure dataclass fixtures exercise
contract validation, lineage walking (cycles, gaps, non-monotonic sequences), snapshot
classification (accepted appends, blocked deletes, untrusted rewrites), target discovery, and the
full `SourcePlanner.plan` baseline and incremental paths, all without Spark or Iceberg. This
package's `SourceCatalog` protocol implementation against a real Iceberg-backed Spark session is
exercised end-to-end by `tests/test_reconciler.py` and the integration-marked
`tests/test_local_e2e.py`.
