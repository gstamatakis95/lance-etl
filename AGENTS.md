# AGENTS.md — AI coding agent guide for lance-etl

This file is the canonical reference for any AI agent working in this repository. Read it in full
before touching any file.

---

## Repository layout

```
lance-etl/
  src/lance_etl/          Python package (production sources)
    etl/                  ETL job package (python -m lance_etl.etl)
      __init__.py         Re-exports: IcebergToLanceETL, ETLConfig, ROUTING_COLS, apply_merge, and helpers
      cli.py              Entry point for lance-etl-etl script and python -m lance_etl.etl
      __main__.py         Calls cli.main()
      job.py              IcebergToLanceETL: read_increment, collapse, spark_batches split, merge fan-out, hourly interval-tag stamp (stamp_interval_tags)
      pivot.py            ETLConfig, ROUTING_COLS, pivot_map_columns (returns column roles), group_by_routing, apply_fsl_cast, apply_ttl_cast
      sink.py             The Lance sink seam: apply_merge, table_chunks, build_update_condition, dataset_uri (format 2.1 bootstrap, role writes)
    indexing/             Indexing job package (python -m lance_etl.indexing)
      __init__.py         Re-exports: LanceIndexer, IndexJobConfig, all handlers, segments, optimize helpers
      cli.py              Entry point for lance-etl-index script and python -m lance_etl.indexing
      __main__.py         Calls cli.main()
      config.py           IndexJobConfig, METRIC_TO_DISTANCE, FTS_OPTIONAL_PARAMS, index-name helpers
      handlers.py         IndexHandler, VectorIndexHandler, BTreeIndexHandler, BitmapIndexHandler, FtsIndexHandler, commit_fts_index, publish_fts_index
      optimize.py         load_vector_config, write_vector_config, drop_existing_index, optimize_existing_index, merge_index_deltas, maintain_index_locally
      runner.py           LanceIndexer fleet phases: plan_dataset_indexes, bootstrap_vector_index (streaming k-means), resolve_vector_artifacts, build_one_shard, commit_one_index, merge_deltas_if_needed, role-based target discovery
      segments.py         build_vector_segment, build_scalar_segment, commit_segments, split_evenly, stale-fragment guards
    maintenance/          Maintenance job package (python -m lance_etl.maintenance)
      __init__.py         Re-exports: MaintenanceJob, MaintenanceConfig, plan_one_dataset, commit_one_dataset, fan_out_per_dataset, update_serving_tag, compaction_skip_reason, and helpers
      cli.py              Entry point for lance-etl-maintenance script and python -m lance_etl.maintenance
      __main__.py         Calls cli.main()
      job.py              MaintenanceJob fleet phases: plan_one_dataset, execute_rewrite_task, commit_one_dataset, cleanup_dataset, run_ttl_on_open_dataset, compaction_skip_reason (derived-state skip: dataset_stats num_fragments)
      tools.py            update_serving_tag, update_serving_tags, migrate_dataset_manifest_paths, migrate_manifest_paths, prune_interval_tags, prune_interval_tags_fleet
    pipeline/             Unified pipeline job package (python -m lance_etl.pipeline)
      __init__.py         Re-exports: PipelineJob, PipelineConfig, prune_interval_tags, prune_interval_tags_fleet, stamp_eligible
      cli.py              Entry point for lance-etl-pipeline script and python -m lance_etl.pipeline
      __main__.py         Calls cli.main()
      job.py              PipelineJob, PipelineConfig: prune -> maintenance -> index -> stamp serialized fleet phases
    tools/                Operator tools package (python -m lance_etl.tools)
      __init__.py         Package marker
      cli.py              Entry point for lance-etl-tools script and python -m lance_etl.tools
      __main__.py         Calls cli.main()
    cliutil.py            Shared CLI helpers: add_common_arguments, add_dataset_arguments, add_index_column_arguments, build_spark (memory-safe SQL defaults), build_telemetry_config, load_dataset_uris, parse_* helpers
    column_roles.py       Column-role metadata (lance-etl.columns): load_column_roles, merge_column_roles
    fanout.py             Shared per-dataset Spark fan-out (fan_out_per_dataset) used by the maintenance, indexing, and operator-tool fleet phases
    recall.py             RecallAuditJob, RecallJobConfig, DatadogSpanSource: replay Datadog spans, score recall@k/nDCG@k/MRR
    telemetry.py          Telemetry, TelemetryConfig, LanceRuntimeConfig, commit_with_retries
    cloud_storage.py      resolve_filesystem + discover_datasets for pyarrow filesystem I/O
    iceberg_optimize.py   IcebergOptimizer + IcebergOptimizeConfig: source Iceberg table maintenance via CALL procedures (rewrite_data_files, rewrite_manifests, expire_snapshots, opt-in remove_orphan_files)
    migrate_namespace.py  NamespaceMigrator + MigrateConfig: one-off namespace copy/optimize utility
  bench/                  Benchmark package (python -m bench)
    cli.py                Subcommand dispatch: download / prepare / ingest / index / compact
                          / search / report / e2e / experiment / all
    experiment.py         Agent loop iteration: prepare + spawn server + e2e + sizes + sweep -> metrics.json + experiments.jsonl
    server.py             ServerHandle: build/spawn/health-check/restart/stop the search-api binary
    sizes.py              On-disk data/index/meta byte measurement across the Lance fleet
    config.py             BenchConfig dataclass + full flag set
    datasets.py           DatasetAdapter registry: Sift1mAdapter, BigannAdapter
    download.py           Corpus acquisition + checksum verification
    prepare.py            Iceberg table + prepared artifacts (queries, ground truth, vocab)
    ingest.py             Real ETL run via LanceIndexer / IcebergToLanceETL
    indexes.py            Index build phase
    compaction.py         Compaction phase
    search.py             Recall / FTS / hybrid / load / clusters / prewarm search legs
    report.py             summary.md, recall.csv, results.csv, pareto.png aggregation
    grpc_client.py        gRPC stub helpers for the search legs
    results.py            Phase artifact I/O (save_phase, load_phase, read_json, write_json)
  rust/search-api/        Rust gRPC search service (tonic, lance crate)
    proto/                lance_etl/v1/lance_etl.proto (one file: SearchService + IntakeService, shared DatasetTarget)
    src/domain/           Transport-agnostic types and traits
      target.rs           DatasetTarget, DatasetRef — dataset addressing (one dataset per request)
      query.rs            VectorQuery, TextQuery, HybridQuery, Hit, FusedHit
      filter.rs           Typed predicate AST (no raw SQL)
      backend.rs          SearchBackend trait
      prewarm.rs          PrewarmSpec, PrewarmReport, Prewarmer trait
      clusters.rs         ClusterSpec, ClusterReport, ClusterReader trait
      fusion.rs           FusionSpec (Rrf and Weighted variants) and within-dataset fusion logic
      rerank.rs           Reranker seam, IdentityReranker (no-op default)
      intake.rs           IntakeBatch, Record, RecordWrite, WriteOp, RecordSink trait, StdoutSink placeholder
      error.rs            SearchError
    src/cache/            Persistent two-tier caching layer (index + metadata, no raw data), pluggable disk/redis backends
      layout.rs           Versioned stamp naming, key hashing, framing, atomic writes, TTL/budget sweep
      entry_store.rs      EntryStore trait: the persistent byte-store seam beneath both tiers
      disk_store.rs       Local-disk EntryStore (the default backend, prefixes.json registry)
      redis_store.rs      Shared-Redis EntryStore (hash-per-dir keys, native TTL, registry hygiene)
      index_cache.rs      HybridIndexCacheBackend: Moka hot tier + pluggable persistent CacheBackend
      store_cache.rs      Read-through byte cache for immutable metadata of wrapped stores
      janitor.rs          Periodic TTL + budget sweep over the disk tiers (redis needs none)
    src/lance/            Lance backend implementations
      backend.rs          LanceSearchBackend — single-dataset dispatch, post-fusion rerank
      provider.rs         DatasetProvider trait, CachingDatasetProvider (shared session + LRU)
      filter.rs           filter_to_expr: domain Filter -> DataFusion Expr
      text.rs             Domain text query tree -> Lance FTS parameters
      rows.rs             Arrow record batch -> JSON row conversion
      prewarm.rs          Prewarmer impl over Lance prewarm APIs
      index_reader.rs     IVF centroid extraction, ClusterReader impl
      error.rs            Lance error classification into SearchError
    src/grpc/             Tonic transport
      mod.rs              SearchGrpc<B>: tonic service adapter (search) + IntakeGrpc<S> (intake)
      convert.rs          Proto <-> domain conversion for the search service
      intake.rs           IntakeGrpc<S>: tonic adapter over any RecordSink
      intake_convert.rs   Proto <-> domain conversion for the intake service
    src/telemetry/        Datadog observability
      traces.rs           OTLP span export, JSON stdout logs with trace correlation
      metrics.rs          Typed DogStatsD facade (Metrics struct + Rpc + IntakeRpc tag enums)
      recall.rs           Deterministic sampled-query capture into recall.* span attributes
    src/config.rs         Config from env vars
    src/lib.rs            Crate root
    src/main.rs           Binary entry point
    Cargo.toml            Workspace root for the crate
  airflow/
    lance_etl_common.py        Shared DAG helpers: default_args, common environment variables, and task-factory utilities
    lance_etl_etl_dag.py       Airflow DAG for the ETL job (optimize-iceberg [optional] >> etl)
    lance_etl_pipeline_dag.py  Airflow DAG for the unified pipeline (prune >> maintenance >> index >> stamp, max_active_runs=1)
  tests/                  pytest suite (conftest.py + test_*.py)
  docs/
    adr/                  Architecture decisions, consolidated into six thematic documents plus a numbered index (README.md)
  market-research/        Detailed evaluation notes, plans, and evidence underlying the ADRs
  claude/                 Original reference artifacts — IMMUTABLE, never edit
  pyproject.toml          Build, dependencies, ruff config
```

The lance checkout at `/Users/gstamatakis/IdeaProjects/lance` is the API ground truth. When you
are unsure whether a pylance API exists or what its signature is, read that checkout. Do not guess.

---

## Hard coding rules

These are non-negotiable. A review that finds a violation must fix it before marking the change
done.

### 1. No leading underscores on any defined name

Do not define names that begin with `_` or `__` anywhere in `src/`, `tests/`, or `airflow/`.
Third-party internals accessed through a leading underscore (e.g. `dataset._ds`) must go through a
single, documented helper function. Never scatter bare `_attr` accesses across the codebase. Note:
the `__version__` dunder was removed from `src/lance_etl/__init__.py` precisely because it violated
this rule.

### 2. No inline comments — use docstrings only

Python: every module, class, and function must have a Google-style docstring. No `# ...` inline
comments anywhere — if code needs explanation, restructure it or put the explanation in the
docstring. Rust: `///` doc comments only on public items. No `//` inline comments in production
code paths.

### 3. Type hints on every signature

Every Python function signature must carry complete type hints: parameters and return type. Use
builtin generics (`list[str]`, `dict[str, int]`, `tuple[str, ...]`) — never `List`, `Dict`,
`Tuple` from `typing`. Use `X | None` instead of `Optional[X]`. `from __future__ import
annotations` is required in every module.

### 4. ruff is the formatter and linter — line length is 120

ALWAYS run both commands after any Python change (a PostToolUse hook in `.claude/settings.json`
also runs them automatically after every file edit):

```bash
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/
```

The enabled rule sets are `E, W, F, I, B, UP, SIM, ARG, PLC0415, D, ANN, C901` (see
`pyproject.toml`). Google docstring style, complete signature annotations, and the mccabe
complexity cap of 12 are therefore lint-enforced, not just conventions (`ANN401` is ignored
because `Any` is deliberate for Spark/Arrow/gRPC engine objects, and `C901` is relaxed for
`tests/`). Both commands must exit 0. Do not suppress warnings without a written justification
in the PR description.

### 4b. All imports at the top of the file, always

No imports inside functions, methods, or conditional branches — enforced by `E402` and `PLC0415`.
Lazy imports for optional dependencies are not an accepted exception. Put the dependency in the
appropriate dependency group instead.

### 4c. No prose semicolons in documentation

In any Markdown file (README.md, AGENTS.md, CLAUDE.md, or docs/) do not use `;` as a prose
punctuation character. Split compound sentences into two sentences instead. Code spans and code
blocks are exempt.

### 5. Spark: heavy work in executors only

The driver plans, broadcasts read-only artifacts (IVF centroids, RaBitQ model, version pins), and
commits. All heavy I/O and compute — Lance dataset reads/writes, index segment builds, compaction
task execution — run inside executor closures (`mapInArrow`, `mapPartitions`, `map`). Never open a
`lance.dataset` on the driver for row-level work.

### 6. Lance indexes use the segment API

The only correct distributed index paths are:

**Vector (IVF_RQ):**
1. Bootstrap (index absent, rebuild, or artifact triggers): ONE executor task runs a committed
   `dataset.create_index(column, "IVF_RQ", name=, metric=, replace=True, num_partitions=,
   num_bits=, rabitq_model=<minted via build_rq_model>, streaming_sample_rate=,
   streaming_refine_passes=)`, whose internal streaming k-means trains the centroids with
   bounded memory, then stores the artifact config (ADR 0030). This is the one sanctioned
   non-segment vector build, and only with the explicit rotation plus stored config.
2. Increment: executor reads centroids back via `get_ivf_model` plus the stored `rabitq_model`,
   then `dataset.create_index_uncommitted(column, "IVF_RQ", name=, num_partitions=,
   num_bits=, ivf_centroids=, rabitq_model=, fragment_ids=shard)` per shard.
3. Commit fan-out: `dataset.merge_existing_index_segments(segments)` then
   `dataset.commit_existing_index_segments(name, column, [merged])` on an executor.
   The segment path hard-requires precomputed centroids, so training never happens there.

**BTREE / BITMAP:**
Same shard/commit flow but no `index_uuid`. Do not call `create_scalar_index(fragment_ids=)` or
`merge_index_metadata` for these types — both raise on current lance main. Scalar segments are
committed unmerged (no `merge_existing_index_segments` call). Lance unions them at query time and
the streaming delta-merge pass (`optimize_indices`) consolidates them on an executor later.

**FTS (INVERTED only):**
1. Driver mints one shared `index_uuid = str(uuid.uuid4())`.
2. Executor: `dataset.create_scalar_index(column, "INVERTED", name=, replace=False,
   index_uuid=shared, fragment_ids=[...], **fts_params)`.
3. Driver: `dataset.merge_index_metadata(index_uuid, index_type="INVERTED")`.
4. Driver: `LanceDataset.commit(uri, LanceOperation.CreateIndex(...), read_version=...)`.

Never call `merge_index_metadata` for BTREE, BITMAP, or vector types. The call raises.

### 7. No raw SQL strings in the gRPC filter API

The `Filter` type in `src/domain/filter.rs` is a typed AST. Column names are validated against the
dataset schema and the allowlist `[A-Za-z_][A-Za-z0-9_]*`. Literals become typed DataFusion `lit`
expressions via `filter_to_expr`. Do not accept, construct, or pass raw SQL strings anywhere in
the gRPC or domain layers.

### 8. No stable row IDs — they are rejected, not deferred

Move-stable row IDs (`enable_stable_row_ids`) were evaluated and rejected because
`merge_insert + delete + concurrent compaction` trips the `RowIdIndex` overlapping-chunk invariant,
risking silent data corruption on release builds. Do not add `enable_stable_row_ids=True` to any
dataset creation or compaction path. See ADR 0010 in `docs/adr/rejected-and-operator-tools.md` for the full
decision. Revisiting requires a fresh ADR.

### 9. The `claude/` directory is immutable

The files in `claude/` are the original reference artifacts that informed the current
implementation. Never edit, delete, or add files there. They are checked into git as-is.

---

## Build and test commands

### Python

```bash
# Install (editable) with dev dependencies
uv pip install -e ".[dev]"

# Install bench extras
uv pip install --group bench

# Lint and format (must pass before any commit)
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/

# Run tests
.venv/bin/pytest
```

pylance `>=8.0.0` installs from PyPI (8.0.0 released 2026-07-01, superseding the
build-from-checkout requirement of the 8.0.0b6 era):

```bash
uv pip install "pylance>=8.0.0"
```

### Rust (rust/search-api)

```bash
cd rust/search-api
cargo fmt                    # format
cargo clippy -- -D warnings  # lint (must be clean)
cargo build                  # compile
cargo test                   # unit tests
```

The Redis cache-backend integration tests (`tests/redis_cache.rs`) spawn a throwaway local
`redis-server` per test and self-skip with a message when the binary is not installed, so
`cargo test` stays green without Redis. Install `redis-server` to run them unskipped.

The lance crates are sourced from crates.io (`lance = "8.0.0"` and friends in
`rust/search-api/Cargo.toml`). Bump them together with the pylance dependency and the
`LANCE_CACHE_STAMP` in `src/cache/layout.rs`, which wipes the on-disk caches across lance
versions because the cache codec format is unstable between releases. The same stamp also
namespaces the Redis cache keys, so after a bump the old generation of keys simply ages out
through its TTLs (ADR 0031).

---

## API ground truth and known API notes

Key facts to internalize:

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
  Resolve it with `dataset._ds.lance_schema.field_case_insensitive(col).id()`.
- Iceberg 1.10 rejects `start-timestamp` / `end-timestamp` outside changelog scans. Use
  `snapshot_id_bounds` in `etl.py` to resolve wall-clock windows to `start-snapshot-id` /
  `end-snapshot-id` from the `{table}.snapshots` metadata table before reading.
- KNOWN pylance 8.0.0 REGRESSION: concurrent `merge_insert` against a dataset carrying BTREE
  index deltas can raise the internal error `RowAddrTreeMap::from_sorted_iter called with
  non-sorted input`. The failure is loud (the merge errors and retries surface it, no silent
  corruption), and the coexistence stress test is marked xfail with this reason. Re-test and
  drop the marker when an upstream fix ships.
- V2 manifest paths default on (`enable_v2_manifest_paths=True` at dataset creation). New datasets
  use V2. Existing datasets migrate via `migrate_manifest_paths_v2`. V2 makes every dataset open
  a single object-store request regardless of version-history depth.

---

## Telemetry conventions

- `Telemetry.create(config)` must be called once per process (driver and each executor). Never
  pickle a `Telemetry` object into a closure. Pickle only the `TelemetryConfig` dataclass.
- Metrics are namespaced under `config.metric_prefix` (default `lance.pipeline`) and tagged with
  `env:`, `service:`, and optional constant tags.
- Lance trace events are bridged to Datadog automatically on the first `Telemetry.create` call per
  process via `attach_lance_event_bridge`.
- The Rust service emits `search_api.*` metrics via the typed `Metrics` facade. All metric emitters
  are infallible: an unreachable Datadog Agent never panics and never fails a request.
- Per-query-leg spans (`lance.vector_query` / `lance.text_query`) carry `object_store.*`
  attributes sourced from Lance execution-stats events: `object_store.requests`,
  `object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`,
  `object_store.indices_loaded`. These are aggregate counts only (no per-method GET/HEAD/LIST split, as
  Lance does not expose that in production builds). They stay low cardinality: no org, tenant, or
  version identifier is attached to span attributes.

---

## Commit-conflict retry pattern

All commits (ETL merge_insert, index commit, compaction commit) must go through
`commit_with_retries` from `telemetry.py`. Retry budgets are defined as named constants in
`telemetry.py`: `DEFAULT_CONFLICT_RETRIES` (10) for ETL, `DEFAULT_COMMIT_RETRIES` (20) for index
and compaction, and `DEFAULT_LARGE_COMMIT_RETRIES` (2) for the compaction `Compaction.commit`. The retry
loop re-reads the dataset before each attempt so it operates against the latest version.
