# 0002. Two-tier compaction orchestration for the 30k-org power law

Status: Accepted

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
