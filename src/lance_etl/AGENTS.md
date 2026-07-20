# AGENTS.md — Python package (`src/lance_etl/`)

The repository-root `AGENTS.md` is the canonical rulebook. Its ten **Hard coding rules** apply here
in full and are not repeated. This file adds the Python-package layout, pylance API ground truth,
build and test commands, telemetry conventions, and commit-retry constants. The ninth rule makes
the PostgreSQL-backed reconciler a local process. The tenth fixes the normalized configuration and
publication model.

The load-bearing Python rule is hard rule 6 (segment-API-only index builds). Its full per-type
recipe for Vector / BTREE / BITMAP / ZONEMAP / FTS lives in the root `AGENTS.md`. The pylance API
facts those recipes depend on are documented under **API ground truth** below.

For an operator- and developer-facing tour of the jobs, see `README.md` in this directory. For
architecture decisions, see `../../docs/adr/README.md`.

---

## Layout

```
src/lance_etl/            Python package
  reconciler/             Local PostgreSQL-backed control loop
    cli.py                Installed lance-etl-reconcile command
    config.py             Local process bootstrap and Spark settings
    runtime.py            PostgreSQL, local Spark, worker, and prewarm wiring
    service.py            One-shot and looping orchestration over durable state
    iceberg.py            Spark Iceberg metadata adapter and baseline qualification
    planning.py           Source-plan to PostgreSQL work mapping
    workers.py            Fenced ingest, maintenance, indexing, validation, and publication
    prewarm.py            Local executor-owned exact-version verification
    retention.py          Publication and audit cleanup
  state/                  PostgreSQL control plane
    tables.py             SQLAlchemy Core metadata for the exact 9-table control plane
    specs.py              Immutable field, ingestion, compaction, index, and publication policy
    settings.py           Environment-sourced local loop limits and retention bounds
    types.py              Validated routing, source, plan, claim, status, and serving values
    repository.py         Visible transactions, leases, cursors, and catalog publication
  source/                 Iceberg source contract and side-effect-free planning
    contract.py           Fixed table UUID and partition specification validation
    lineage.py            Direct-parent snapshot chronology
    manifests.py          Physical-change classification and touched-target discovery
    planner.py            Pinned baseline and incremental window planning
    scans.py              Exact Spark snapshot scan construction
  etl/                    Shared ETL primitives composed by the reconciler
    replay_sink.py        Source-sequenced replay-safe Lance merge
    completion.py         Monotonic Lance completion marker
    digest.py             Canonical mutation and source digests
    mutation.py           Operation normalization and terminal mutation collapse
    pivot.py              Map projection, schema alignment, and Arrow casts
    sink.py               Executor-side content-routed idempotent Lance merge sink
  indexing/               Segment-API index planning, build, commit, and maintenance libraries
  maintenance/            Retention, compaction, cleanup, and tag libraries
  publication/            Exact candidate manifests and publication workflow helpers
  recall/                 Offline recall audit libraries
  tools/                  Uninstalled operator library CLI
  cliutil.py              Shared local Spark and CLI helpers
  telemetry.py            Datadog facade, Lance event bridge, and commit retries
  cloud_storage.py        PyArrow filesystem resolution and dataset discovery
  iceberg_optimize.py     Iceberg table maintenance library
  migrate_namespace.py    Namespace copy and optimization library
```

The `group_by_routing` sorted-run split is not a production symbol. It exists only as a test-only
oracle in `tests/conftest.py`. Production streaming routing uses `stream_routing_groups`.

### Adjacent trees

```
bench/                  Benchmark package (python -m bench). See bench/README.md for the full guide.
  cli.py                Subcommand dispatch: download / prepare / search / report / e2e / experiment / qualify
  e2e.py                Reconciler-driven end-to-end run over the PostgreSQL control plane
  reconcile.py          Source and spec registration plus reconciler invocation for the e2e run
  experiment.py         Agent loop iteration: prepare + spawn server + e2e + sizes + sweep -> metrics.json + experiments.jsonl
  qualification.py      Deterministic mutation-collapse, shuffle-width, and external scale-gate evidence
  server.py             ServerHandle: build/spawn/health-check/restart/stop the search-api binary
  sizes.py              On-disk data/index/meta byte measurement across the Lance fleet
  config.py             BenchConfig dataclass + full flag set
  datasets.py           DatasetAdapter registry: Sift1mAdapter, BigannAdapter
  download.py           Corpus acquisition + checksum verification
  prepare.py            Iceberg table + prepared artifacts (queries, ground truth, vocab)
  search.py             Recall / FTS / hybrid / load / clusters / prewarm search legs
  report.py             summary.md, recall.csv, results.csv, pareto.png aggregation
  grpc_client.py        gRPC stub helpers for the search legs
  results.py            Phase artifact I/O (save_phase, load_phase, read_json, write_json)
migrations/             Alembic environment and PostgreSQL revisions
tests/                  pytest suite, including isolated-schema PostgreSQL tests
```

The normalized PostgreSQL entity list and transaction boundaries are ADR 0042 in
`../../docs/adr/postgresql-dataset-control-plane.md`. Runtime code must read an active immutable
dataset specification revision by identity. The database carries typed columns for every supported
data-path option. Do not introduce an untyped configuration document or a second scheduling state
machine.

Author configuration through the repository lifecycle APIs. A complete typed graph is inserted as
DRAFT in one transaction, its digest is recomputed, and activation retires the former ACTIVE
revision. Database triggers freeze ACTIVE and RETIRED parent, field, and index rows. IVF_RQ and
INVERTED options are nullable columns on `index_definitions`, frozen by the same index trigger and
gated by per-index-type CHECK constraints. Source defaults and dataset desired revisions must
resolve to ACTIVE revisions. Assigning a different revision to materialized data creates
deterministic REBUILD work. The claimed `dataset_work` row records a `launcher_kind` audit label
only.

---

## API ground truth and known API notes

The lance checkout at `/Users/gstamatakis/IdeaProjects/lance` is the pylance API ground truth. When
you are unsure whether an API exists or what its signature is, read that checkout. Do not guess.

- `lance.lance.indices.build_rq_model(dimension, num_bits=1, dtype="float32")` is a real API
  returning a JSON string. The vector dimension must be divisible by 8.
- Streaming k-means (`streaming_sample_rate`, `streaming_coreset_rate`,
  `streaming_refine_passes`) is exposed only through the committed `create_index` path. The
  distributed segment path refuses internal training and requires precomputed centroids.
- `create_index_uncommitted(..., rabitq_model=str)` is validated. Passing a wrong JSON raises
  `ValueError`. The same string must reach every executor shard.
- `CommitConflictError` is not reliably importable from `lance` directly. Use the fallback chain
  in `telemetry.py`. Conflicts surface as `OSError` or `RuntimeError` from lance internals.
- `defer_index_remap=True` builds a `__lance_frag_reuse` system index at commit time through
  the options passed to `Compaction.commit`. pylance 8.0.0 carries the `options` parameter.
- The FTS path requires a Lance field id (not a pyarrow schema index) for `Index(fields=[...])`.
  Resolve it with `lance_field_id(dataset, column)` from `indexing/segments.py` — the single
  documented helper for that internal access, per hard rule 1. Never inline the underlying
  `_ds.lance_schema` lookup at call sites.
- Iceberg 1.10 rejects `start-timestamp` / `end-timestamp` outside changelog scans. The
  `reconciler/iceberg.py` snapshot ledger and `source/scans.py` resolve windows to
  `start-snapshot-id` / `end-snapshot-id` from the `{table}.snapshots` metadata table before
  reading.
- KNOWN pylance 8.0.0 REGRESSION: concurrent `merge_insert` against a dataset carrying BTREE
  index deltas can raise the internal error `RowAddrTreeMap::from_sorted_iter called with
  non-sorted input`. The failure is loud (the merge errors and retries surface it, no silent
  corruption), and the coexistence stress test is marked xfail with this reason. Re-test and
  drop the marker when an upstream fix ships.
- KNOWN pylance BEHAVIOR: `describe_indices()` reports `index_type` as `Unknown` for an
  INVERTED index published through the FTS atomic `CreateIndex` swap, because that hand-built
  `Index` record carries no index details. `stats.index_stats(name)["index_type"]` still reports
  the true `Inverted` type. This holds on every supported pylance major: on the 9.x checkout
  `index_stats` derives `index_type` from the index's own plugin statistics, so it reports
  `Inverted` there too. The publication qualification gate (`reconciler/workers.py`) therefore
  resolves the effective kind through `resolved_actual_index_kind`, which is data-driven and
  version-independent: whenever `describe_indices` reports `Unknown` it takes the kind from the
  stats type, which carries the real kind, so genuinely wrong types are still rejected. Consulting
  the returned stats value is data inspection, not attribute probing, so the compatibility rule is
  respected. `lance_major_version()` is used only to emit an operator warning when the mislabeling
  persists on a major at or beyond 9, never to gate correctness, so a malformed `lance.__version__`
  is reported as an unknown major rather than failing a publish. Scalar and vector segments
  committed with `commit_existing_index_segments` are not affected. The resolved observed kind (not
  the configured type) is what `publication_evidence` persists, so `validate_publication_indexes`
  is a real cross-check against the frozen specification.
- KNOWN retention behavior: tombstone rows carry the delete mutation's event `ts` (not a null `ts`)
  and the maintenance retention predicate expires a tombstone only when its `ts` is past both
  `record_retention_seconds` and the source `replay_horizon_seconds`, so its source-sequence
  anti-resurrection watermark outlives every replayable window. An unbounded replay horizon means
  tombstones are never expired. `MaintenanceConfig.deleted_column` opts a dataset into this
  tombstone-aware predicate. Operator-library and namespace-migration callers leave it unset and
  keep the plain `ts < cutoff` predicate.
- V2 manifest paths default on (`enable_v2_manifest_paths=True` at dataset creation). New datasets
  use V2. Existing datasets migrate via `migrate_manifest_paths_v2`. V2 makes every dataset open
  a single object-store request regardless of version-history depth.
- `lance.indices.IvfModel.save(uri, *, storage_options=)` / `IvfModel.load(uri, *,
  storage_options=)` persist and read IVF centroids through lance's own object-store layer in a
  single-file format. This is the centroid sidecar mechanism (ADR 0040). Never use
  `create_index`'s `ivf_centroids_file` parameter, which bypasses `storage_options`.

---

## Build and test commands

```bash
# Synchronize the exact locked development environment
uv sync --locked --group dev --python 3.14.0

# Install bench dependencies
uv sync --locked --group dev --group bench --python 3.14.0

# Lint and format (must pass before any commit)
uvx ruff format src/ tests/ bench/ migrations/
uvx ruff check src/ tests/ bench/ migrations/

# Run tests
.venv/bin/pytest -m "not integration"
```

pylance `==8.0.0` installs from PyPI (8.0.0 released 2026-07-01, superseding the
build-from-checkout requirement of the 8.0.0b6 era):

```bash
uv pip install "pylance==8.0.0"
```

The Rust service sources the lance crates from crates.io at the same version. Bump the two
together (see `../../rust/search-api/AGENTS.md` for the crate-version coupling).

---

## Telemetry conventions (Python)

- `Telemetry.create(config)` must be called once per process (driver and each executor). Never
  pickle a `Telemetry` object into a closure. Pickle only the `TelemetryConfig` dataclass.
- Metrics are namespaced under `config.metric_prefix` (default `lance.pipeline`) and tagged with
  `env:`, `service:`, and optional constant tags.
- Lance trace events are bridged to Datadog automatically on the first `Telemetry.create` call per
  process via `attach_lance_event_bridge`.

The Rust service's telemetry conventions (`search_api.*` metrics, infallible emitters, the
`object_store.*` span attributes, low-cardinality rule) are documented in
`../../rust/search-api/AGENTS.md`.

---

## Commit-conflict retry pattern

All commits (ETL merge_insert, index commit, compaction commit) must go through
`commit_with_retries` from `telemetry.py`. Retry budgets are named constants in `telemetry.py`:
`DEFAULT_CONFLICT_RETRIES` (10) for ETL, `DEFAULT_COMMIT_RETRIES` (20) for index and compaction,
and `DEFAULT_LARGE_COMMIT_RETRIES` (2) for the compaction `Compaction.commit`. The retry loop
re-reads the dataset before each attempt so it operates against the latest version.
