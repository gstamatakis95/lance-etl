# 0019. Namespace copy/migrate utility

Status: Accepted

## Context

A dataset lives at `base_uri/<val1>/<val2>/.../<valN>.lance` where the path components are the values of the
configured `partition_cols` (default `org_id`, `tenant_id`, `namespace`). A namespace is a single path component,
so one namespace spans many datasets, one per `(org, tenant)` pair that uses it. Operators need a way to migrate
a whole namespace or apply a bulk change across it: rename a namespace, move it under reorganized routing, or
rebuild every dataset in it with new index parameters or fresh compaction. Doing this by hand across thousands
of datasets is error prone, and a destructive in-place rewrite leaves no safe rollback if the new layout is
wrong.

## Decision

Add `src/lance_etl/migrate_namespace.py` with a `MigrateConfig`, a `NamespaceMigrator`, and a `MigrateReport`.
The default behaviour is copy plus optimize, keep source. For every dataset whose namespace component equals
`source_namespace`, the job writes a copy at the same address with the namespace component swapped to
`target_namespace`. The source datasets are never deleted.

Per-dataset namespace swap. Source datasets are discovered with `cloud_storage.discover_datasets` and filtered
to those whose namespace path component matches `source_namespace`. Each target URI is the same routing path with
only the namespace component replaced. Every path component, including the source and target namespace names, is
validated against the same allowlist the ETL uses (`etl.PATH_COMPONENT_PATTERN`), so a namespace name can never
inject a traversal or collide a route. A target that already exists fails the whole run unless `overwrite_target`
is set.

Reuse, not reimplementation. After the copy the targets are optimized in the production pipeline order: write,
then recompact, then reindex. Recompaction reuses `compaction.LanceCompactor` ([0002](0002-two-tier-compaction-orchestration.md))
and reindexing reuses `indexing.LanceIndexer` with its segment-API index flows
([0001](0001-distributed-indexing-segment-api.md)). A copy carries no indexes, so reindex is the step that makes
the migrated namespace searchable. The index columns are supplied through an `IndexJobConfig`. When none is
supplied, reindex is skipped because the columns to build cannot be guessed.

Two-tier scale mirrors compaction. The set of source datasets is classified by fragment count in one distributed
job that also resolves each target URI and tests target existence. Small datasets are copied whole inside one
executor task each, batched into a single Spark job through the shared `fan_out_per_dataset` helper. Large
datasets keep a distributed per-dataset copy: the driver pins the source version and shards its fragment ids,
executors read their shard and write new fragment files into the target with `write_fragments` in create mode,
and the driver commits all fragments in one `LanceOperation.Overwrite` transaction. The driver only plans, lists,
and commits. All heavy read and write I/O runs in executors. Every commit goes through
`telemetry.commit_with_retries`.

## Consequences

Migrations and bulk namespace changes become a single declarative job that respects the power-law dataset
distribution: the long tail copies cheaply in one batched job and the head fans its copy out across executors.
Keeping the source intact gives the blue-green flip story for free ([0013](0013-blue-green-serving.md)): migrate
to the new namespace, verify the copy and its rebuilt indexes, then repoint serving by moving the `prod` tag with
`compaction.update_serving_tag`. A tag move alone does not refresh a running serving process, so prewarm the new
namespace by explicit version before the flip. Rollback is trivial because the original namespace is still there.

The copy doubles storage for the migrated namespace until the operator deletes the source after a successful
cutover. Deletion is left to the operator rather than automated, so a premature cleanup can never race a
verification still in progress. Stable row ids are not used on either side
([0010](0010-stable-row-ids-rejected.md)), so row ids are not preserved across the copy. The migration is keyed
by content, not row id, and recall is verified against the rebuilt indexes, so this is not a regression for the
copy plus optimize workflow.
