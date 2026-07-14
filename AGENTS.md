# AGENTS.md — AI coding agent guide for lance-etl

This file is the canonical repo-wide rulebook for any AI agent working in this repository. Read it
in full before touching any file. It carries the rules that apply everywhere. The package-specific
detail — the detailed layout, API ground truth, build and test commands, and per-language telemetry
— lives in two sub-guides. Read the one that covers what you are changing:

- **`src/lance_etl/AGENTS.md`** — the Python package (Spark ETL, indexing, maintenance, pipeline,
  tools, recall). The full segment-API index recipe's pylance API facts, Python build/test, Python
  telemetry, and the commit-retry constants.
- **`rust/search-api/AGENTS.md`** — the Rust gRPC search service. Crate layout, cargo commands, the
  lance-crate version-bump coupling, Rust telemetry, the filter-AST rule detail, and proto surface
  notes.

For a developer-facing tour of the jobs and service see the per-directory `README.md` files. For
architecture decisions see `docs/adr/README.md`.

---

## Repository layout

```
lance-etl/
  src/lance_etl/     Python package: Spark ETL, indexing, maintenance, pipeline, tools, recall (detail: src/lance_etl/AGENTS.md)
  rust/search-api/   Rust gRPC search service: tonic transport over the Lance crate (detail: rust/search-api/AGENTS.md)
  bench/             End-to-end benchmark package, python -m bench (detail: bench/README.md)
  airflow/           Two Airflow DAGs: the ETL DAG and the unified pipeline DAG
  tests/             pytest suite (conftest.py + test_*.py)
  docs/adr/          Architecture decisions, six thematic documents plus a numbered index (docs/adr/README.md)
  market-research/   Detailed evaluation notes, plans, and evidence underlying the ADRs
  pyproject.toml     Build, dependencies, ruff config
```

The lance checkout at `/Users/gstamatakis/IdeaProjects/lance` is the API ground truth. When you are
unsure whether a pylance API exists or what its signature is, read that checkout. Do not guess. The
checkout tracks lance main and can be ahead of the pin this repo actually ships: at the time of
writing the checkout is at `9.0.0-beta.20` while `pyproject.toml` pins `pylance>=8.0.0,<9`. When a
behavior difference between major versions could matter, verify the API against the pinned major
(read the installed `pylance` package in `etl/venv`, or the release notes) rather than assuming the
checkout's behavior applies unchanged.

---

## Hard coding rules

These are non-negotiable and repo-wide. A review that finds a violation must fix it before marking
the change done.

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
4. Clustered-rewrite rebuild (ADR 0041): after a clustered Overwrite drops the IVF_RQ index, it
   is rebuilt with the PRESERVED centroids and stored `rabitq_model` via
   `create_index_uncommitted(..., ivf_centroids=, rabitq_model=, fragment_ids=shard)` per shard,
   then `merge_existing_index_segments`, then `commit_existing_index_segments`. No training
   occurs.

**BTREE / BITMAP / ZONEMAP:**
Same shard/commit flow but no `index_uuid`. Do not call `create_scalar_index(fragment_ids=)` or
`merge_index_metadata` for any of these types — all three raise on current lance main. BTREE and
BITMAP segments are committed unmerged (no `merge_existing_index_segments` call). Lance unions them
at query time and the streaming delta-merge pass (`optimize_indices`) consolidates them on an
executor later. ZONEMAP is the exception: its per-shard segments ARE merged with
`dataset.merge_existing_index_segments(segments)` before `dataset.commit_existing_index_segments(name,
column, [merged])` — it is the only scalar type that merges before commit. Zonemap segment merging
requires lance 8 (upstream commits e8748a405 and cc657c5e3), which this repo already pins
(`pylance>=8.0.0,<9`).

**FTS (INVERTED only):**
1. Driver mints one shared `index_uuid = str(uuid.uuid4())`.
2. Executor: `dataset.create_scalar_index(column, "INVERTED", name=, replace=True,
   index_uuid=shared, fragment_ids=[...], **fts_params)`. Pylance 8 checks committed same-name
   metadata even on the uncommitted fragment path, so `replace=False` is valid only for an initial
   build whose name does not exist. Atomic rebuilds must use `replace=True` or every shard raises
   before building.
3. Driver: `dataset.merge_index_metadata(index_uuid, index_type="INVERTED")`.
4. Driver: re-list the old same-name segments and commit one
   `LanceOperation.CreateIndex(new_indices=[rebuilt], removed_indices=old_segments)` transaction.
   The old index remains queryable until this atomic swap commits.

Never call `merge_index_metadata` for BTREE, BITMAP, or vector types. The call raises.

The pylance API facts these recipes depend on (`build_rq_model`, `get_ivf_model`,
`IvfModel.save/load`, `lance_field_id`, and so on) are in `src/lance_etl/AGENTS.md`.

### 7. No raw SQL strings in the gRPC filter API

The `Filter` type in `rust/search-api/src/domain/filter.rs` is a typed AST. Column names are
validated against the dataset schema and the allowlist `[A-Za-z_][A-Za-z0-9_]*`. Literals become
typed DataFusion `lit` expressions via `filter_to_expr`. Do not accept, construct, or pass raw SQL
strings anywhere in the gRPC or domain layers. The full detail is in `rust/search-api/AGENTS.md`.

### 8. No stable row IDs — they are rejected, not deferred

Move-stable row IDs (`enable_stable_row_ids`) were evaluated and rejected because
`merge_insert + delete + concurrent compaction` trips the `RowIdIndex` overlapping-chunk invariant,
risking silent data corruption on release builds. Do not add `enable_stable_row_ids=True` to any
dataset creation or compaction path. See ADR 0010 in `docs/adr/rejected-and-operator-tools.md` for the full
decision. Revisiting requires a fresh ADR.

---

## Telemetry conventions (repo-wide)

Telemetry is Datadog throughout. `Telemetry.create(config)` runs once per process on the Python
side, and the Rust service emits `search_api.*` metrics through a typed facade. All metric emitters
are infallible: an unreachable Datadog Agent never fails a request or a job. Span attributes and
metric tags stay low cardinality — no org, tenant, or version identifier is attached. The
per-language specifics (Python metric prefix and event bridge, the Rust `object_store.*` span
attributes and typed tag enums) are in `src/lance_etl/AGENTS.md` and `rust/search-api/AGENTS.md`.

---

## Commit-conflict retry pattern

All Python commits (ETL merge_insert, index commit, compaction commit) must go through
`commit_with_retries` from `src/lance_etl/telemetry.py`, which re-reads the dataset before each
attempt so it operates against the latest version. The named retry-budget constants are documented
in `src/lance_etl/AGENTS.md`.
