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
    cloud_storage.py      resolve_filesystem for pyarrow (driver-only artifact I/O)
    arrow_types.py        resolve_arrow_type / resolve_type_map (CLI type specs)
    cli.py                Entry point: etl / compact / index subcommands
  rust/search-api/        Rust gRPC search service (tonic, lance crate)
    proto/                lance_etl/search/v1/search.proto
    src/domain/           Transport-agnostic types and traits
    src/lance/            Lance backend, DatasetProvider, filter -> DataFusion Expr
    src/grpc/             Tonic adapter (proto <-> domain)
    src/config.rs         Config from env vars
    Cargo.toml            Workspace root for the crate
  airflow/
    lance_etl_dag.py      Daily Airflow DAG (etl -> index -> compact)
  tests/                  pytest suite (conftest.py + test_*.py)
  docs/
    verification-report.md  API verification against lance main @ 466405f47 — read this
                             to understand why APIs are used the way they are
  claude/                 Original reference artifacts — IMMUTABLE, never edit
  pyproject.toml          Build, dependencies, ruff config
```

The lance checkout at `/Users/gstamatakis/IdeaProjects/lance` is the API ground truth. When you
are unsure whether a pylance API exists or what its signature is, read that checkout; do not guess.

---

## Hard coding rules

These are non-negotiable. A review that finds a violation must fix it before marking the change
done.

### 1. No leading underscores on any defined name

Do not define names that begin with `_` or `__` anywhere in `src/`, `tests/`, or `airflow/`.
Third-party internals accessed through a leading underscore (e.g. `dataset._ds`) must go through a
single, documented helper function. Never scatter bare `_attr` accesses across the codebase.

### 2. No inline comments; use docstrings only

Python: every module, class, and function must have a Google-style docstring. No `# ...` inline
comments anywhere — if code needs explanation, restructure it or put the explanation in the
docstring. Rust: `///` doc comments only on public items; no `//` inline comments in production
code paths.

### 3. Type hints on every signature

Every Python function signature must carry complete type hints: parameters and return type. Use
builtin generics (`list[str]`, `dict[str, int]`, `tuple[str, ...]`) — never `List`, `Dict`,
`Tuple` from `typing`. Use `X | None` instead of `Optional[X]`. `from __future__ import
annotations` is required in every module.

### 4. ruff is the formatter and linter; line length is 120

Before committing any Python change:

```bash
uvx ruff format src/ tests/ airflow/
uvx ruff check src/ tests/ airflow/
```

The enabled rule sets are `E, W, F, I, B, UP, SIM, ARG` (see `pyproject.toml`). Both commands
must exit 0. Do not suppress warnings without a written justification in the PR description.

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

Never call `merge_index_metadata` for BTREE, BITMAP, or vector types; the call raises.

### 7. No raw SQL strings in the gRPC filter API

The `Filter` type in `src/domain/filter.rs` is a typed AST. Column names are validated against the
dataset schema and the allowlist `[A-Za-z_][A-Za-z0-9_]*`; literals become typed DataFusion `lit`
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

# Lint and format (must pass before any commit)
uvx ruff format src/ tests/ airflow/
uvx ruff check src/ tests/ airflow/

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

See `docs/verification-report.md` for a detailed, line-cited verification of every API used by
this project against lance main @ `466405f47` (pylance `8.0.0-beta.6`). Key facts to internalize:

- `lance.lance.indices.build_rq_model(dimension, num_bits=1, dtype="float32")` is a real API
  returning a JSON string. The vector dimension must be divisible by 8.
- `create_index_uncommitted(..., rabitq_model=str)` is validated; passing a wrong JSON raises
  `ValueError`. The same string must reach every executor shard.
- `CommitConflictError` is not reliably importable from `lance` directly; use the fallback chain
  in `telemetry.py`. Conflicts surface as `OSError` or `RuntimeError` from lance internals.
- `defer_index_remap=True` in compaction builds a `__lance_frag_reuse` index via
  `compact_files`/`Compaction.execute` (the small-dataset tier), but is currently ignored by the
  distributed `Compaction.commit` binding (see `optimize.rs:566-568` TODO).
- The FTS path requires a Lance field id (not a pyarrow schema index) for `Index(fields=[...])`.
  Resolve it with `dataset._ds.lance_schema.field_case_insensitive(col).id()`.

---

## Telemetry conventions

- `Telemetry.create(config)` must be called once per process (driver and each executor). Never
  pickle a `Telemetry` object into a closure; pickle only the `TelemetryConfig` dataclass.
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
