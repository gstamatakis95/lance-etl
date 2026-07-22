# `lance_etl.reconciler` — local control loop

This package is the process that turns durable PostgreSQL state into Lance work and back again. It
owns the one local Spark session for the reconciliation process, and it is the only code that
executes `INGEST`, `PUBLISH`, and `REBUILD` claims read from
[`lance_etl.state`](../state/README.md). Its entry point is the installed `lance-etl-reconcile`
command and the `python -m lance_etl.reconciler` module.

For the reconciliation cycle at a glance and the shared pipeline story (Iceberg reads, replay-safe
ingestion, the segment-API index recipe), see the package [README](../README.md). For the hard
coding rules that govern this package, see [AGENTS.md](../AGENTS.md) and the repository-root
`AGENTS.md`. For the control-plane transaction design this package drives, see
[ADR 0042](../../../docs/adr/postgresql-dataset-control-plane.md). For the historical orchestration
decisions this package's execution model descends from, see
[fleet-orchestration-and-maintenance.md](../../../docs/adr/fleet-orchestration-and-maintenance.md)
(ADR 0028 task-based orchestration, ADR 0035 per-dataset failure isolation, ADR 0018 retention, ADR
0039 commit-retry hardening). `prewarm.py`'s local exact-version check is the reconciler-side
predecessor of the Rust search service's own Prewarm RPC documented in
[ADR 0007](../../../docs/adr/caching-and-observability.md) — the two are related but distinct: this
package's prewarm runs inside the local process before every publish, while the search service's
RPC warms its own persistent cache once a caller invokes it against a serving replica.

## Module map

| Module | Responsibility |
|---|---|
| `cli.py` | The installed `lance-etl-reconcile` command: argument parsing, `migrate` / `run-once` / `run` / `status` / `repair` dispatch, and the continuous poll loop |
| `config.py` | Process bootstrap: PostgreSQL URL validation (psycopg 3, local-vs-remote TLS), local Spark master/catalog/warehouse settings, `ReconcilerSettings` re-export |
| `runtime.py` | Fresh-process wiring: builds the local Spark session, the PostgreSQL engine and repository, resolves or bootstraps the Iceberg source registration, and assembles every collaborator into `ReconcilerApplication` (full run) or `ReconcilerOperator` (status/repair only, no Spark) |
| `service.py` | `ReconcilerApplication.run_once`, `BoundedDispatcher` (bounded claim/execute/reconcile loop), `ResultReconciler` (typed result -> repository transition), retention-floor and SLO evaluation |
| `iceberg.py` | Spark Iceberg metadata adapter, table-contract and baseline qualification, direct-parent snapshot lineage, `DurableSourcePlanProvider` |
| `planning.py` | `SourcePlanEnqueuer` — maps one side-effect-free `SourcePlan` into idempotent `enqueue_source_snapshot` repository calls |
| `workers.py` | `DistributedIngestRunner` and `ConfiguredPublicationRunner` — the fenced Spark executor work for each phase, plus `FencedWorkExecutor` claim dispatch and `LeaseHeartbeat` |
| `prewarm.py` | `LocalExactVersionPrewarmer` — opens the exact qualified candidate version on one executor immediately before publish |
| `retention.py` | `PublicationRetentionSweep` — deletes retired candidate pins and manifest artifacts on executors, then prunes control-plane audit rows |
| `results.py` | `WorkResult` / `ResultKind` — the closed, self-validating set of worker outcomes the dispatcher and reconciler understand |
| `telemetry.py` | `TelemetrySloEmitter` — infallible low-cardinality Datadog gauges for reconciler health |
| `migrations.py` | `AlembicMigrationRunner` — wraps `alembic upgrade head` for the `migrate` command. See the [migrations README](../../../migrations/README.md) |
| `__main__.py` | `python -m lance_etl.reconciler` entry point, equivalent to the installed `lance-etl-reconcile` command |
| `__init__.py` | Re-exports the package's public surface |

## The run-once cycle, phase by phase

`ReconcilerApplication.run_once()` runs five steps in a fixed order, matching the numbered list in
the package [README](../README.md):

1. **`plan_and_enqueue_snapshots`** — repeatedly calls `SourcePlanProvider.plan()` (backed by
   `DurableSourcePlanProvider` in `iceberg.py`) for the next accepted or durably rejected Iceberg
   snapshot, and `SourcePlanEnqueuer.enqueue` to persist it and its touched datasets, up to
   `settings.max_snapshots_per_plan` passes or until a pass enqueues nothing new.
2. **`run_due_dataset_work`** — delegates to `BoundedDispatcher.run()`, described below.
3. **`reconcile_results`** — delegates to `PublicationRetentionSweep.reconcile()` via the
   `ExternalResultSweep` protocol slot (despite the name, in the current local runtime this step
   performs bounded external cleanup of retired publications, not ambiguous result reconciliation —
   worker results are reconciled synchronously inside the dispatcher in step 2).
4. **`gate_source_retention`** — reads `ControlPlaneStatus` once and derives the exact source
   snapshot floor (`RetentionDecision`) that Iceberg expiration must still preserve.
5. **`emit_slo_status`** — evaluates `evaluate_slo` against the same status snapshot and emits it
   through `SloEmitter`.

`run_once` returns a `RunOnceSummary` bundling all five results. `lance-etl-reconcile run` calls
`run_once` in a loop separated by `poll_interval` (or an explicit `--poll-seconds`) until
interrupted. It introduces no separate scheduler or retry database, per hard rule 9.

## Worker execution and the publish-gate qualification

`FencedWorkExecutor.execute(claim)` resolves a live `WorkExecutionContext` for the claim (`None`
means the claim's fence has already expired, which becomes an immediate `RETRY`), starts a
`LeaseHeartbeat`, and dispatches to `DistributedIngestRunner.run` for `INGEST` claims or
`ConfiguredPublicationRunner.run` for `PUBLISH`/`REBUILD` claims.

`DistributedIngestRunner` (`workers.py`) normalizes one source window's mutations against the
frozen specification, collapses terminal per-key mutations, computes the streaming source digest,
writes replay-safe merges through `etl/replay_sink.py`, verifies the completion marker, and returns
`INGEST_SUCCEEDED` with the exact committed Lance version, row count, and digest — or a `RETRY` for
a transient failure.

`ConfiguredPublicationRunner` (`workers.py`) is the compaction-through-publish pipeline: it resolves
or creates the isolated candidate (rewriting the exact applied source version for REBUILD), runs
maintenance if `spec.compaction_enabled`, builds every required index through the segment API,
pins the qualified candidate version, computes exact total/distinct/live/distinct-live row counts,
and qualifies schema and per-index fragment coverage. A publication is returned as
`PUBLISH_SUCCEEDED` only when qualification finds **zero unindexed fragments for every required
index** — any gap blocks or retries the claim instead. A failed compaction becomes a transient
`MAINTENANCE_FAILED` retry and a failed index build a transient `INDEX_BUILD_FAILED` retry, rather
than a terminal block, so both are exercised directly in `tests/test_reconciler_classifier.py`. The
observed index kind that actually gets persisted as evidence is resolved by
`resolved_actual_index_kind`, which works around a known pylance `Unknown`-kind quirk on FTS indexes
by consulting `stats.index_stats(name)["index_type"]` — see `src/lance_etl/AGENTS.md` for the full
API-ground-truth note. When `spec.prewarm_required`, the candidate is prewarmed (see below) before
the result is returned. A failed prewarm is a transient retry, never a block, since the underlying
candidate is still valid and worth retrying.

## Lease-renewal heartbeat

Because `DistributedIngestRunner.run` and `ConfiguredPublicationRunner.run` can run for far longer
than one PostgreSQL lease, `FencedWorkExecutor.execute` wraps every claim in a `LeaseHeartbeat`
(`workers.py`): a daemon thread that calls `repository.renew_lease(claim, lease_duration)` every
`settings.lease_heartbeat_interval` until the worker finishes. A renewal extends
`lease_expires_at` without touching the owning dataset's `fence_epoch` — see
[`state/README.md`](../state/README.md#work-claiming-for-update-skip-locked-lease-and-fence) for
why that distinction is load-bearing. If a renewal call fails or returns `False` (the lease was
lost — reclaimed by another worker after expiry), the heartbeat sets `lost = True` and stops
itself. `FencedWorkExecutor.execute` checks that flag after the worker returns and converts an
otherwise-successful result into a `LEASE_LOST` retry, so a result computed under a lease that may
have already been reassigned is never applied as if it still owned the claim.

`ReconcilerSettings.lease_heartbeat_interval` must stay strictly shorter than `lease_duration`
(validated in `state/settings.py`), so a healthy worker always renews well before its own lease can
expire.

## The bounded dispatcher

`BoundedDispatcher.run()` (`service.py`) drains due work for up to `settings.max_drain_batches`
iterations. Each iteration claims up to `settings.claim_batch_size` dataset-disjoint claims via
`repository.claim_due_work`, executes every claim through `execute_isolated` (which converts an
unexpected exception in the worker into an `UNEXPECTED_WORKER_FAILURE` retry result rather than
letting it escape and abort the whole batch — the per-dataset failure isolation from ADR 0035), and
reconciles each result through `ResultReconciler.reconcile`. The loop stops early once a batch
returns fewer claims than requested (the queue is drained) or the batch limit is reached, and
returns a constant-size `DispatchSummary` (claimed/succeeded/advanced/retried/blocked/stale) rather
than per-work-item detail, keeping the summary safe to log at any cardinality.

`ResultReconciler.reconcile` maps each `ResultKind` to the matching fenced repository transition:
`INGEST_SUCCEEDED` -> `complete_ingest`, `PUBLISH_SUCCEEDED` -> first `advance_publish_phases`
(checkpoints every fixed phase from the claim's current phase through `PREWARM` so a crash after a
long-running publish still resumes past completed work) then `publish_dataset`, `PHASE_ADVANCED` ->
`advance_phase` followed by a zero-delay `retry_work` (so a phase checkpoint alone does not block
the claim, it just yields it back to the queue), `RETRY` -> `retry_work` with the code-owned
jittered backoff delay, and `BLOCKED` -> `block_work`. Any transition whose fence has moved on
(a stale claim) returns `False` and the dispatcher counts it as `stale` rather than raising, since a
stale result is expected under concurrent reclaim and not a bug.

## Restricted repair and status

`lance-etl-reconcile status` and `repair` never start Spark — they run against `ReconcilerOperator`,
a PostgreSQL-only surface built by `runtime.build_runtime_operator`. `repair --action retry-blocked
--work-id ...` calls `repository.retry_blocked_work` directly. `repair --action rebuild --tenant-id
... --namespace ... --org-id ... --request-id ...` calls `repository.enqueue_rebuild` with an
operator-supplied idempotency key, so repeating the same repair call is a no-op rather than a second
rebuild. Both repairs accept `--dry-run` to validate input without mutating any state.

## Invariants a maintainer must not break

- **The driver plans, executors do heavy work.** `DistributedIngestRunner` and
  `ConfiguredPublicationRunner` push Lance reads/writes, index segment builds, and compaction into
  Spark executor closures (`mapInArrow`/`map`/`parallelize(...).map`). Never open a `lance.dataset`
  on the driver for row-level work, including under `local[*]`, which still uses real executor
  tasks.
- **No external scheduler or reconciler manifests.** This process is the only orchestrator. Do not
  add a remote Spark submission wrapper, a second retry database, or an operator/container image
  for the reconciler — hard rule 9.
- **A publish only succeeds with zero unindexed fragments across every required index.** Do not
  weaken `ConfiguredPublicationRunner`'s qualification gate to accept partial index coverage.
- **The heartbeat renews the lease, never the fence.** Only `claim_due_work`'s `claim_locked_row`
  path may advance `fence_epoch`.
- **Failure isolation stays per-claim.** `execute_isolated` must keep converting an unexpected
  worker exception into a retry result for that one claim, not letting it abort the whole dispatch
  batch.
- **Segment-API indexing only**, per hard rule 6 — `ConfiguredPublicationRunner`'s index build path
  must keep using the sanctioned segment-API and streaming-bootstrap recipes documented in the root
  `AGENTS.md` and `src/lance_etl/AGENTS.md`.

## Testing

| Test file | Covers |
|---|---|
| `tests/test_reconciler.py` | `ResultReconciler`, `BoundedDispatcher`, the run-once cycle, retention-floor and SLO evaluation, and CLI dispatch, using a fake Spark and injected repository |
| `tests/test_reconciler_classifier.py` | The `MAINTENANCE_FAILED` / `INDEX_BUILD_FAILED` retry gates in `ConfiguredPublicationRunner.run`, driven against a real candidate dataset with stubbed maintenance and indexing collaborators |
| `tests/test_reconciler_prewarm.py` | `LocalExactVersionPrewarmer` |
| `tests/test_reconciler_retention.py` | `PublicationRetentionSweep`'s pin and manifest deletion plus audit pruning |
| `tests/test_state_postgres.py` | Real-PostgreSQL: the durable side of claim/lease/fence and terminal transitions this package drives (see [`state/README.md`](../state/README.md#testing)) |
| `tests/test_postgres_queue_load.py` | Real-PostgreSQL: bounded claim contention under concurrent local workers |

Most of this package's own tests run against a fake Spark session and an in-memory or mocked
repository and need no PostgreSQL. The real-PostgreSQL tests it shares with `state/` are gated on
`LANCE_ETL_TEST_DATABASE_URL`:

```bash
export LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl_test'
.venv/bin/pytest tests/test_reconciler.py tests/test_reconciler_classifier.py \
  tests/test_reconciler_prewarm.py tests/test_reconciler_retention.py -m "not integration"
```
