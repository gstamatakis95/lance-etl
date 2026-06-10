# ADR 0026 — Three-job isolation: separate packages, CLIs, and DAGs

**Status**: Accepted

**Date**: 2026-06-10

---

## Context

The pipeline has three distinct operational jobs: ETL (Iceberg-to-Lance incremental routing),
maintenance (TTL expiration, compaction, version cleanup), and index (vector and full-text index
builds).  Until this change all three shared a single Python module (`etl.py`, `indexing.py`,
`maintenance.py`) and a single aggregate CLI entry point (`cli.py`) that could dispatch any job.
That structure created three problems.

First, any deploy-time import error in one job blocked the other two from starting, because
importing the aggregate CLI transitively imported all three job implementations.

Second, the shared-file layout invited cross-job coupling.  Helper functions added to serve one
job were available to any other job without any package boundary enforcing the intended isolation.

Third, a single Airflow DAG file that encoded the full pipeline order (optimize-iceberg >> etl >>
maintenance >> index) made it impossible to schedule the three jobs independently, adjust their
retry policies separately, or run a one-off index rebuild without triggering ETL.

The three jobs must share no files and must never talk to each other at runtime.  Each job reads
from and writes to Lance datasets independently.  Their only coordination surface is the Lance
dataset version history and the commit-conflict retry protocol already present in each job.

---

## Decision

Each job is a separate Python package with its own `__init__.py`, `cli.py`, and `__main__.py`:

- `lance_etl.etl` — ETL job.  Entry point `lance-etl-etl`.  Run via `python -m lance_etl.etl`.
- `lance_etl.indexing` — Index job.  Entry point `lance-etl-index`.  Run via `python -m lance_etl.indexing`.
- `lance_etl.maintenance` — Maintenance job.  Entry point `lance-etl-maintenance`.  Run via `python -m lance_etl.maintenance`.

A fourth package, `lance_etl.tools`, holds the operator-only subcommands (recall audit, namespace
migration, Iceberg optimization) that are not scheduled.  Its entry point is `lance-etl-tools`.

Shared argument-parsing helpers (telemetry config, dataset URI loading, common argument groups)
live in `lance_etl.cliutil` and are imported by each CLI independently.

Each job has its own Airflow DAG file:

- `lance_etl_etl_dag.py` — ETL DAG.
- `lance_etl_maintenance_dag.py` — Maintenance DAG.
- `lance_etl_index_dag.py` — Index DAG.  `max_active_runs=1` serializes index-commit concurrency.

Common DAG utilities (default args, environment variable wiring, task-factory helpers) live in
`lance_etl_common.py` and are imported by each DAG file.

### Skip-check design

Both the index job and the maintenance job perform a derived-state skip check before doing any
write work.  The checks are zero-write and zero-scan: they read only from the open manifest.

`index_skip_reason` (in `lance_etl.indexing.runner`) calls `dataset.describe_indices()` and reads
`index_stats.num_unindexed_fragments` and `index_stats.num_indices` for each configured column.
If every configured column has an index and zero unindexed fragments, the function returns a
non-None skip reason string and the job does nothing.

`compaction_skip_reason` (in `lance_etl.maintenance.job`) reads `dataset_stats.num_fragments`.
If the fragment count is at or below the small-tier target-files threshold, the function returns a
non-None skip reason string and the compaction step is skipped.

Both skip checks are evaluated on the open dataset object without issuing additional object-store
requests beyond the manifest read.  They stay low-cardinality and do not scan row data.

### Conflict safety

Overlap between job runs is safe.  Concurrent ETL and index runs are handled by the
commit-conflict retry loop in `commit_with_retries`: the retry re-reads the dataset at the latest
version before each attempt so it never operates on stale metadata.  Stale-fragment errors from
the index build are handled by the replan loop in `build_and_commit_segments`, which discards
index segments built against fragments that no longer exist and rebuilds only the live shards.
The `max_active_runs=1` setting on the index DAG is the one hard serialization requirement: two
concurrent index builds on the same dataset would both reach the `commit_existing_index_segments`
step, and lance raises on a second commit that references the same index UUID already committed.

---

## Consequences

**Import isolation**: an import error in the indexing package does not prevent the ETL or
maintenance packages from loading.

**Independent scheduling**: each DAG can be paused, backfilled, or given distinct retry policies
without affecting the other two.  The staggered-schedule recommendation (ETL then maintenance then
index, with time gaps) is an operational practice, not a correctness requirement.

**No aggregate CLI**: the old `lance-etl` single-binary entry point is gone.  Operators and DAGs
call `lance-etl-etl`, `lance-etl-index`, `lance-etl-maintenance`, or `lance-etl-tools`.

**Test updates required**: tests that imported directly from `lance_etl.cli` must be updated to
import from the new per-package CLIs or from `lance_etl.cliutil`.  This is a one-time mechanical
change with no behavior difference.
