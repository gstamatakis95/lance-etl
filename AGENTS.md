# AGENTS.md — AI coding agent guide for lance-etl

This file is the canonical reference for any AI agent working in this repository. Read it in full
before touching any file.

---

## Repository layout

```
lance-etl/
  src/lance_etl/          Python package (production sources)
    etl.py                IcebergToLanceETL: read, collapse, repartition, merge_insert
    indexing.py           LanceIndexer + per-type handlers (VectorIndex, BTree, Bitmap, Fts)
    compaction.py         LanceCompactor: Compaction.plan / Compaction.commit
    telemetry.py          Telemetry, TelemetryConfig, LanceRuntimeConfig, commit_with_retries
    cloud_storage.py      resolve_filesystem + discover_datasets for pyarrow filesystem I/O
    arrow_types.py        resolve_arrow_type / resolve_type_map (CLI type specs)
    cli.py                Entry point: etl / compact / index subcommands
  bench/                  Benchmark package (python -m bench)
    cli.py                Subcommand dispatch: download / prepare / ingest / index / compact
                          / search / report / all
    config.py             BenchConfig dataclass + full flag set
    datasets.py           DatasetAdapter registry: Sift1mAdapter, SyntheticAdapter
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
    proto/                lance_etl/search/v1/search.proto
    src/domain/           Transport-agnostic types and traits
      target.rs           DatasetTarget, DateRange — dataset addressing
      query.rs            VectorQuery, TextQuery, HybridQuery, Hit, FusedHit
      filter.rs           Typed predicate AST (no raw SQL)
      backend.rs          SearchBackend trait
      prewarm.rs          PrewarmSpec, PrewarmReport, Prewarmer trait
      clusters.rs         ClusterSpec, ClusterReport, ClusterReader trait
      fusion.rs           FusionSpec, RrfFusion
      merge.rs            Dedup-by-id fan-out merge
      error.rs            SearchError
    src/cache/            Persistent two-tier caching layer
      layout.rs           Versioned stamp dir, key hashing, atomic writes, TTL/budget sweep
      disk_cache.rs       Hybrid disk + Moka CacheBackend for the Lance index cache
      store_cache.rs      Read-through byte cache for immutable metadata of wrapped stores
      janitor.rs          Periodic TTL + budget sweep over both cache tiers
    src/lance/            Lance backend implementations
      backend.rs          LanceSearchBackend — date-range fan-out, dedup, RRF
      provider.rs         DatasetProvider trait, CachingDatasetProvider (shared session + LRU)
      filter.rs           filter_to_expr: domain Filter -> DataFusion Expr
      text.rs             Domain text query tree -> Lance FTS parameters
      rows.rs             Arrow record batch -> JSON row conversion
      prewarm.rs          Prewarmer impl over Lance prewarm APIs
      index_reader.rs     IVF centroid extraction, ClusterReader impl
      error.rs            Lance error classification into SearchError
    src/grpc/             Tonic transport
      mod.rs              SearchGrpc<B>: tonic service adapter
      convert.rs          Proto <-> domain conversion
    src/telemetry/        Datadog observability
      traces.rs           OTLP span export, JSON stdout logs with trace correlation
      metrics.rs          Typed DogStatsD facade (Metrics struct + tag enums)
    src/config.rs         Config from env vars
    src/lib.rs            Crate root
    src/main.rs           Binary entry point
    Cargo.toml            Workspace root for the crate
  airflow/
    lance_etl_dag.py      Configurable-schedule Airflow DAG (etl -> index -> compact)
  tests/                  pytest suite (conftest.py + test_*.py)
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
single, documented helper function. Never scatter bare `_attr` accesses across the codebase.

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

The enabled rule sets are `E, W, F, I, B, UP, SIM, ARG, PLC0415` (see `pyproject.toml`). Both
commands must exit 0. Do not suppress warnings without a written justification in the PR
description.

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
1. Driver: `IndicesBuilder.train_ivf(...)` to get IVF centroids.
2. Driver: `lance.lance.indices.build_rq_model(dimension, num_bits)` to get the RaBitQ model JSON.
3. Broadcast both artifacts to executors.
4. Executor: `dataset.create_index_uncommitted(column, "IVF_RQ", name=, num_partitions=,
   num_bits=, ivf_centroids=, rabitq_model=, fragment_ids=shard)`.
5. Driver: `dataset.merge_existing_index_segments(segments)` then
   `dataset.commit_existing_index_segments(name, column, [merged])`.

**BTREE / BITMAP:**
Same shard/commit flow but no `index_uuid`. Do not call `create_scalar_index(fragment_ids=)` or
`merge_index_metadata` for these types — both raise on current lance main.

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

### 8. The `claude/` directory is immutable

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

pylance `>=8.0.0b6` must be built from the lance checkout until released on PyPI:

```bash
cd /Users/gstamatakis/IdeaProjects/lance
maturin develop --release -m python/Cargo.toml
```

### Rust (rust/search-api)

```bash
cd rust/search-api
cargo fmt                    # format
cargo clippy -- -D warnings  # lint (must be clean)
cargo build                  # compile
cargo test                   # unit tests
```

The lance crates are sourced via path dependencies pointing at
`/Users/gstamatakis/IdeaProjects/lance/rust/*`. If the checkout moves, update the paths in
`rust/search-api/Cargo.toml`.

---

## API ground truth and known API notes

Key facts to internalize:

- `lance.lance.indices.build_rq_model(dimension, num_bits=1, dtype="float32")` is a real API
  returning a JSON string. The vector dimension must be divisible by 8.
- `create_index_uncommitted(..., rabitq_model=str)` is validated. Passing a wrong JSON raises
  `ValueError`. The same string must reach every executor shard.
- `CommitConflictError` is not reliably importable from `lance` directly. Use the fallback chain
  in `telemetry.py`. Conflicts surface as `OSError` or `RuntimeError` from lance internals.
- `defer_index_remap=True` on the small-dataset tier builds a `__lance_frag_reuse` system index.
  On the current lance build this leaves indexed vector queries failing with a missing fragment-id
  error until the remap runs. The production default is `False` (opt-in via `--defer-index-remap`).
  The large-dataset tier ignores `defer_index_remap` entirely because the Python `Compaction.commit`
  binding hard-codes default options and always remaps inline.
- The FTS path requires a Lance field id (not a pyarrow schema index) for `Index(fields=[...])`.
  Resolve it with `dataset._ds.lance_schema.field_case_insensitive(col).id()`.
- Iceberg 1.10 rejects `start-timestamp` / `end-timestamp` outside changelog scans. Use
  `snapshot_id_bounds` in `etl.py` to resolve wall-clock windows to `start-snapshot-id` /
  `end-snapshot-id` from the `{table}.snapshots` metadata table before reading.

---

## Telemetry conventions

- `Telemetry.create(config)` must be called once per process (driver and each executor). Never
  pickle a `Telemetry` object into a closure. Pickle only the `TelemetryConfig` dataclass.
- Metrics are namespaced under `config.metric_prefix` (default `lance.pipeline`) and tagged with
  `env:`, `service:`, and optional constant tags.
- Lance trace events are bridged to Datadog automatically on the first `Telemetry.create` call per
  process via `attach_lance_event_bridge`.

---

## Commit-conflict retry pattern

All commits (ETL merge_insert, index commit, compaction commit) must go through
`commit_with_retries` from `telemetry.py`. Default retry budgets: 10 for ETL, 20 for index and
compaction. The retry loop re-reads the dataset before each attempt so it operates against the
latest version.
