# 0002. Two-tier compaction orchestration for the 30k-org power law

Status: Accepted (amended)

## Amendment (2026-06): folded into a maintenance job

The two-tier compaction described below is unchanged in mechanics, but it is no longer a standalone job. It now lives
in `src/lance_etl/maintenance.py` as one step of `MaintenanceJob` (renamed from `LanceCompactor`), configured by
`MaintenanceConfig` (renamed from `CompactionConfig`). A maintenance run applies three ordered steps per dataset.
First, per-row TTL expiration deletes expired rows when a TTL column is configured (see [ADR
0018](0018-ttl-expiration.md)). Second, the two-tier compaction below runs. Third, version cleanup prunes old
versions. The TTL delete runs before compaction so the compaction reclaims the storage the expired rows occupied, and
version cleanup runs at the tail of each dataset's compaction (`compact_small_dataset` and `compact_one` both call
`cleanup_dataset`). The blue-green serving-tag helpers (`update_serving_tag` and friends) and the manifest-path
migration move into the same module unchanged.

The CLI subcommand is renamed `compact` -> `maintenance` and the Airflow task is renamed `compact` -> `maintenance`
(chain stays `etl >> maintenance >> index`). The run span is renamed `lance.compaction.run` -> `lance.maintenance.run`
and the default FAIR scheduler pool is renamed `lance-compaction` -> `lance-maintenance`. The compaction metrics
(`dataset.compacted`, `dataset.committed`, `run.fragments_removed`, and the stage timings) keep their names. TTL adds
`dataset.ttl_rows_deleted`, `dataset.ttl_expired`, `dataset.ttl_commit_conflict`, `run.ttl_rows_deleted`, and
`run.ttl_datasets_expired`.

## Context

Org datasets follow a power law: 1B rows over 30k orgs averages roughly 33k rows each, but a small head holds
most data and a long tail holds almost nothing. A single sequential per-dataset driver loop launching one Spark
job per dataset per index type would mean on the order of 120k blocking jobs, days of pure scheduling overhead
before any work. The distributed `Compaction.commit` Python binding also hard-codes `CompactionOptions::default`,
so options set at plan time are ignored on that path.

## Decision

Compact and index in two tiers keyed on fragment count. Tier A (small datasets, below a configurable fragment
threshold) batches many dataset URIs into one Spark job where each executor task compacts or indexes a whole
dataset in process with the non-distributed calls (`Compaction.execute`, plain `create_index`), which honor
every option. Tier B (large datasets) keeps the distributed plan/execute/commit fan-out and drives multiple
datasets concurrently from a driver thread pool pinned to Spark FAIR scheduler pools, not a sequential loop.

## Consequences

The long tail stops paying segment-fan-out overhead it never needed, and the head still gets distributed
compaction. A tier-B commit conflict is treated as "rewrite results are stale" and loops back to re-plan and
re-execute, because the commit pins its conflict scan to the plan version and a blind re-commit would
deterministically refind the same conflict. `max_source_fragments` caps fragments consumed per run for
incremental compaction of head datasets. The binding gap (default options on tier-B commit) means inline index
remap always runs there, which is acceptable given [0010](0010-stable-row-ids-rejected.md) rejected the
alternative. Index policy is size-aware: `num_partitions` clamps to roughly sqrt(rows) and vector indexing is
skipped below a row floor where flat KNN suffices.
