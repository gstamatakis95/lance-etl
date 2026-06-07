# 0009. Index-vs-compaction coexistence and the orphan-race guard

Status: Accepted

## Context

Ingestion (merge_insert and delete), compaction, and indexing must coexist, running concurrently against the
same datasets, with zero data loss and eventual convergence to a healthy fragment count and full index coverage.
A research pass mapped the Lance transaction conflict matrix: index builds commute with ingestion, version
cleanup never conflicts but can delete transaction files an in-flight committer needs to rebase from, and a
distributed compaction commit pins its conflict scan to the plan version so it must re-plan rather than
re-commit on conflict.

## Decision

Adopt the coexistence strategy in `market-research/concurrency-and-coexistence.md`: index builds need no ordering
against merge, compaction re-plans on conflict, and cleanup runs with a horizon (floor of several hours) that
exceeds the longest head-org job so it never deletes a transaction file a live committer needs.

Guard the one genuine race found by the coexistence stress test: an index segment build plans over a fragment
set, compaction concurrently rewrites those fragments away, and committing the segments would orphan
now-deleted fragments. `commit_existing_index_segments` raises a plain `ValueError` ("would orphan fragments"),
which is not the retryable conflict class. A shared `is_stale_fragment_error` predicate detects it across vector,
scalar, and FTS paths. On a stale-but-live plan the builder re-reads at latest, re-resolves the fragment set
(dropping dead fragments), rebuilds, and re-commits within a budget. On a corrupt index already holding dead
fragments it drops and cleanly rebuilds. It never silently skips indexing.

## Consequences

The coexistence stress test runs three concurrent actors (ingester, compactor, indexer) over a head-sized
dataset and several tail datasets and asserts exact final content, full index coverage, and a fragment count in
the target band. It passes 40-plus consecutive runs. The `defer_index_remap` default is False on the pinned
build because deferred remap leaves indexed vector queries broken until the remap runs. This guard, not
[0010](0010-stable-row-ids-rejected.md), is how the remap race is contained. Stable row IDs would have made
the dead-fragment case impossible but were rejected for the concurrency panic.
