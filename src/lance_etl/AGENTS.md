# AGENTS.md — Python package (`src/lance_etl/`)

The repository-root `AGENTS.md` is the canonical rulebook. Its eight **Hard coding rules** (no
leading underscores, no inline comments, complete type hints, ruff formatting and imports-at-top,
executors-only heavy work, segment-API-only index builds, no raw SQL in the filter API, no stable
row IDs) apply here in full and are not repeated. This file adds the Python-package specifics: the
detailed layout, the pylance API ground truth, Python build and test commands, Python telemetry
conventions, and the commit-retry constants.

The load-bearing Python rule is hard rule 6 (segment-API-only index builds). Its full per-type
recipe for Vector / BTREE / BITMAP / ZONEMAP / FTS lives in the root `AGENTS.md`. The pylance API
facts those recipes depend on are documented under **API ground truth** below.

For an operator- and developer-facing tour of the jobs, see `README.md` in this directory. For
architecture decisions, see `../../docs/adr/README.md`.

---

## Layout

```
src/lance_etl/          Python package (production sources)
  etl/                  ETL job package (python -m lance_etl.etl)
    __init__.py         Re-exports: IcebergToLanceETL, ETLConfig, ROUTING_COLS, apply_merge, apply_ttl_cast, dataset_uri, derive_bulk_schemas, pivot_map_columns, plan_bulk_append, snapshot_id_bounds
    cli.py              Entry point for lance-etl-etl script and python -m lance_etl.etl
    __main__.py         Calls cli.main()
    job.py              IcebergToLanceETL: read_increment, collapse, adaptive-plan routing (explicit N salted shuffle + partition sort), production-disabled bulk qualification path, streaming merge fan-out (merge_partition), hourly interval-tag stamp (stamp_interval_tags)
    plan.py             ETLConfig-driven adaptive routing: RoutingPlan, compute_routing_plan (per-trio count aggregation), apply_salted_shuffle (big-dataset key-hash sub-bucketing), bucket_count/shuffle_partition_count sizing
    bulk.py             Production-disabled bulk-append qualification path for big NEW or empty datasets. It remains testable through explicit ETLConfig opt-in until MUTATION-01 deletes the raw append path.
    pivot.py            ETLConfig, ROUTING_COLS, pivot_map_columns (returns column roles, canonical vector dims), align_to_schema (null-fill/reorder/cast a slice to the driver-derived canonical schema), stream_routing_groups (streaming sorted-run group split), routing_stats_schema/routing_stats_ddl (shared with bulk.py), apply_fsl_cast, apply_ttl_cast
    sink.py             The Lance sink seam: apply_merge, table_chunks, build_update_condition, dataset_uri (format 2.1 bootstrap, role writes), open_or_bootstrap (used by the bulk-append bootstrap)
  indexing/             Indexing job package (python -m lance_etl.indexing)
    __init__.py         Re-exports: LanceIndexer, IndexJobConfig, all handlers, segments, optimize helpers
    cli.py              Entry point for lance-etl-index script and python -m lance_etl.indexing
    __main__.py         Calls cli.main()
    config.py           IndexJobConfig, METRIC_TO_DISTANCE, FTS_OPTIONAL_PARAMS, growth_exceeds_retrain_factor, index-name helpers
    handlers.py         IndexHandler, VectorIndexHandler, BTreeIndexHandler, BitmapIndexHandler, ZonemapIndexHandler, FtsIndexHandler, commit_fts_index, publish_fts_index. Each handler exposes prepare / build_segment / merges().
    optimize.py         load_vector_config, write_vector_config, optimize_existing_index, merge_index_deltas, maintain_index_locally
    runner.py           LanceIndexer fleet phases: make_handler (kind -> IndexHandler dispatch), plan_dataset_indexes, bootstrap_vector_index (streaming k-means), persist_bootstrap_centroids, build_one_shard, commit_one_index, role-based target discovery
    segments.py         build_vector_segment, build_scalar_segment, commit_segments, split_evenly, lance_field_id, stale-fragment guards
  maintenance/          Maintenance job package (python -m lance_etl.maintenance)
    __init__.py         Re-exports: MaintenanceJob, MaintenanceConfig, plan_one_dataset, commit_one_dataset, fan_out_per_dataset, update_serving_tag, and helpers
    cli.py              Entry point for lance-etl-maintenance script and python -m lance_etl.maintenance
    __main__.py         Calls cli.main()
    job.py              MaintenanceJob fleet phases: plan_one_dataset, execute_rewrite_task, commit_one_dataset, cleanup_dataset, run_ttl_on_open_dataset, compaction_skip_reason (derived-state skip: dataset_stats num_fragments)
    tools.py            update_serving_tag / update_serving_tags (both take tags: Sequence[str], default ("HEAD",), and flip every named tag in ONE dataset open), flip_one_tag, migrate_dataset_manifest_paths, migrate_manifest_paths, prune_interval_tags, prune_interval_tags_fleet
    cluster.py          Internal clustered-rewrite qualification phases. Production maintenance exposes no CLI enablement and MaintenanceConfig defaults the path off.
  pipeline/             Unified pipeline job package (python -m lance_etl.pipeline)
    __init__.py         Re-exports: PipelineJob, PipelineConfig, prune_interval_tags, prune_interval_tags_fleet, stamp_eligible
    cli.py              Entry point for lance-etl-pipeline script and python -m lance_etl.pipeline
    __main__.py         Calls cli.main()
    job.py              PipelineJob, PipelineConfig: prune -> maintenance -> index -> stamp serialized fleet phases. The temporary stamp phase writes interval tags only and never publishes HEAD.
  tools/                Operator tools package (python -m lance_etl.tools)
    __init__.py         Package marker
    cli.py              Entry point for lance-etl-tools script and python -m lance_etl.tools
    __main__.py         Calls cli.main()
  cliutil.py            Shared CLI helpers: add_common_arguments, add_dataset_arguments, add_index_column_arguments, build_spark (memory-safe SQL defaults), build_telemetry_config, load_dataset_uris, parse_* helpers, run_cli_main (the shared per-job main shell reused by all five cli.py mains: parse, logging, dispatch, exit-code mapping)
  column_roles.py       Column-role metadata (lance-etl.columns): load_column_roles, merge_column_roles
  fanout.py             Shared Spark fleet helpers used by the maintenance, indexing, and operator-tool fleet phases: fan_out_per_dataset, run_fleet_fanout (single-phase driver shell: span, empty guard, timed fan-out, gauge, log), run_flat_tagged_job (flat parallelize/collect with per-dataset ok/error isolation, FLAT_OK/FLAT_ERROR markers), derive_partitions, TAG_FANOUT_PARTITIONS
  recall/               Offline recall audit package (invoked via python -m lance_etl.tools recall)
    __init__.py         Re-exports the externally-used set: RecallAuditJob, RecallJobConfig, RecallSample, RecallReport, DatadogSpanSource, InMemorySpanSource, plus the scoring, query-translation, and reporting helpers listed in __all__
    config.py           RecallJobConfig, identifier/path allowlists, BM25 params, scanner batch size
    source.py           SpanSource, DatadogSpanSource, InMemorySpanSource: fetch and parse recall.* span attributes into RecallSample
    queries.py          filter_ast_to_sql, text_query_field_queries, tokenize_text, fuse_legs: span-to-query replay translation
    scoring.py          brute_force_top_k, bm25_top_k, grade_against_reference, grade_hybrid_reference: recall@k/nDCG@k/MRR scoring
    job.py              RecallAuditJob: two-tier Spark fan-out that scores every sample and renders the aggregate report
  telemetry.py          Telemetry, TelemetryConfig, LanceRuntimeConfig, commit_with_retries
  cloud_storage.py      resolve_filesystem + discover_datasets (driver walk or executor-fanned listing) for pyarrow filesystem I/O
  iceberg_optimize.py   IcebergOptimizer + IcebergOptimizeConfig: source Iceberg table maintenance via CALL procedures (rewrite_data_files, rewrite_manifests, expire_snapshots, opt-in remove_orphan_files)
  migrate_namespace.py  NamespaceMigrator + MigrateConfig: one-off namespace copy/optimize utility
```

The `group_by_routing` sorted-run split is not a production symbol. It exists only as a test-only
oracle in `tests/conftest.py`. Production streaming routing uses `stream_routing_groups`.

### Adjacent Python trees

```
bench/                  Benchmark package (python -m bench). See bench/README.md for the full guide.
  cli.py                Subcommand dispatch: download / prepare / ingest / index / compact / search / report / e2e / experiment / all
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
airflow/
  lance_etl_common.py          Shared fixed Spark and retry policy
  lance_etl_reconciler_dag.py Sole serialized five-phase reconciler DAG
tests/                  pytest suite (conftest.py + test_*.py)
```

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
- Iceberg 1.10 rejects `start-timestamp` / `end-timestamp` outside changelog scans. Use
  `snapshot_id_bounds` in `etl/` to resolve wall-clock windows to `start-snapshot-id` /
  `end-snapshot-id` from the `{table}.snapshots` metadata table before reading.
- KNOWN pylance 8.0.0 REGRESSION: concurrent `merge_insert` against a dataset carrying BTREE
  index deltas can raise the internal error `RowAddrTreeMap::from_sorted_iter called with
  non-sorted input`. The failure is loud (the merge errors and retries surface it, no silent
  corruption), and the coexistence stress test is marked xfail with this reason. Re-test and
  drop the marker when an upstream fix ships.
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

# Install airflow dependencies (only needed to run tests/test_airflow_dags.py unskipped)
uv sync --locked --group dev --group airflow --python 3.14.0

# Lint and format (must pass before any commit)
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/

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
