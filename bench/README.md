# Local BIGANN and SIFT1M benchmark

The `bench` package qualifies the production PostgreSQL reconciler path end to end on one machine:
Iceberg ingestion, Lance indexing, compaction, publication, search, and report. It is an
experimental harness. The reconciler it drives is the normal write path.

## Install

```bash
uv sync --locked --group dev --group bench --python 3.14.0
```

Large `merge_insert` batches benefit from additional DataFusion memory headroom:

```bash
export LANCE_MEM_POOL_SIZE=4294967296
```

## Commands

| Command | Purpose |
|---|---|
| `download` | Download and checksum a corpus |
| `prepare` | Create local Iceberg input and query artifacts |
| `search` | Measure vector, text, hybrid, load, cluster, and prewarm behavior |
| `report` | Aggregate phase artifacts into tables and plots |
| `e2e` | Run the reconciler-driven end-to-end qualification path |
| `experiment` | Run one local build, search, size, and parameter-sweep iteration |
| `qualify` | Emit deterministic mutation-collapse, shuffle-width, and external scale-gate evidence |
| `fuzz` | Randomized CRUD fuzz: seeded op sequences reconciled end-to-end, verified against an in-memory oracle with full row-content comparison |

Use `python -m bench COMMAND --help` for the complete current flag set.

`python -m bench` starts through an import-light launcher. Spark-bearing commands (`prepare`, `e2e`,
`experiment`, and `fuzz`) replace that process with the environment's `spark-submit` before importing
Lance, Arrow, or telemetry libraries. Other commands replace it with the same Python interpreter.

## Corpus cache

Raw corpus files are stored under `--corpus-root`, default `bench/corpora`. The cache is independent
of `--workspace`, so a fresh experiment can reuse a verified download.

```bash
python -m bench download --dataset sift1m
python -m bench e2e \
  --dataset sift1m \
  --corpus-root bench/corpora \
  --workspace bench/workspace \
  --results-root bench/results
```

Prepared queries, ground truth, and manifests remain workspace-specific because their shape
depends on row limit, route count, seed, and text settings.

## SIFT1M smoke run

SIFT1M is the smallest real corpus and includes published nearest-neighbor ground truth.

```bash
python -m bench e2e \
  --dataset sift1m \
  --batches 2 \
  --num-partitions 128 \
  --workspace bench/workspace \
  --results-root bench/results
```

For a download-free integration fixture:

```bash
.venv/bin/pytest tests/test_bench_e2e_tagged.py -x -q -m integration
```

The test creates tiny local BIGANN-format files, runs Spark locally, and uses real adapter IO.

## BIGANN qualification

Download one official prefix:

```bash
python -m bench download --dataset bigann --limit 100000000
```

Official ground truth exists for 1M, 2M, 5M, 10M, 20M, 50M, 100M, 200M, 500M, and 1000M rows.
Other limits use exact brute-force preparation and are practical only at small scale.

Run the local end-to-end path:

```bash
python -m bench e2e \
  --dataset bigann \
  --limit 100000000 \
  --batches 4 \
  --no-text \
  --rows-per-slice 1000000 \
  --etl-partitions 16 \
  --num-partitions 4096 \
  --num-shards 16 \
  --vector-row-floor 1024 \
  --seed 42 \
  --workspace bench/workspace \
  --results-root bench/results
```

Scale to 1B rows by changing the local storage paths and bounded parallelism:

```bash
python -m bench e2e \
  --dataset bigann \
  --limit 1000000000 \
  --batches 10 \
  --no-text \
  --rows-per-slice 5000000 \
  --etl-partitions 64 \
  --num-partitions 16384 \
  --num-shards 32 \
  --workspace /Volumes/bench/workspace \
  --results-root /Volumes/bench/results
```

## Local search measurement

`e2e`'s control plane lives in a randomly named, ephemeral PostgreSQL schema for the duration of
one run (`bench/reconcile.py:isolated_control_plane`), dropped when the run finishes. An externally
started `search-api` server connects with the default `search_path` and can never see that schema's
published rows. `e2e` therefore self-hosts its own `search-api` subprocess *inside* the isolation
window instead of dialing an external one: it spawns the release binary pointed at the exact
isolated schema, waits for gRPC health `SERVING`, measures recall/FTS/hybrid/latency, and tears the
subprocess down before the schema is dropped (`bench/e2e.py:catalog_search_leg`,
`bench/search_server.py:self_hosted_search_api`). There is no `--endpoint` flag on `e2e` at all —
build the binary once and `e2e` handles the rest:

```bash
cd rust/search-api
cargo build --release
cd ../..

python -m bench e2e \
  --dataset sift1m --batches 2 --num-partitions 128 \
  --workspace bench/workspace --results-root bench/results
```

`--search-api-binary` (default `rust/search-api/target/release/search-api`) points `e2e` at the
binary. Pass an empty string to disable the leg explicitly. An absent default binary also records
`{"status": "NOT_RUN"}` on `final_catalog_grpc` without failing the run. `expected_versions` is
resolved directly from the live control-plane repository inside the isolation window, so no
`--search-expected-versions-path` evidence file is needed for `e2e`.

### Standalone `search` (ad hoc, after `e2e`)

The standalone `search` command needs a server resolved one of two ways, and does not itself create
or drop any PostgreSQL schema:

```bash
# self-host against a control plane an e2e run kept alive
python -m bench e2e --keep-control-plane --dataset sift1m --batches 2 \
  --workspace bench/workspace --results-root bench/results
python -m bench search \
  --control-plane-url "$(python -c "import json,sys; print(json.load(open(sys.argv[1]))['database_url'])" \
    bench/results/<run_id>/control_plane.json)" \
  --workspace bench/workspace --results-root bench/results

# or dial an already-running server started separately, pointed at the same schema
LANCE_ETL_BASE_URI="$PWD/bench/workspace/lance" \
LANCE_ETL_DATABASE_URL='<the kept schema URL>' \
SEARCH_API_TELEMETRY_DISABLED=true \
cargo run --locked --manifest-path rust/search-api/Cargo.toml &
python -m bench search --endpoint 127.0.0.1:8080 \
  --workspace bench/workspace --results-root bench/results
```

`--endpoint` wins when both are set. `--search-expected-versions-path` remains required for the
`--endpoint` path (`bench/grpc_client.py:load_expected_versions`), since a dialed-in server has no
in-process repository to resolve versions from. The self-hosting path does not need it. Both paths
measure the same recall/FTS/hybrid/load legs (`bench/search.py:run_search_against_endpoint`).

## Fuzz CRUD qualification

`fuzz` reconciles a seeded, randomized sequence of insert/update/delete operations end to end
through the same reconciler-driven control plane as `e2e`, then verifies the terminal state
against an in-memory oracle with full row-content comparison rather than row counts alone.

```bash
python -m bench fuzz --seed 42 --workspace bench/workspace --results-root bench/results
```

Key flags and their defaults: `--seed` (no default, pass explicitly for reproducibility),
`--fuzz-ops` (total randomized CRUD ops, default 200), `--fuzz-snapshots` (Iceberg append
snapshots, first seeds every org, default 4), `--fuzz-keyspace` (distinct record-id pool shared
across orgs, default 80), `--fuzz-mix` (insert:update:delete relative weights, default
`60:25:15`), `--fuzz-dim` (synthetic vector dimension, divisible by 8, default 32). Additional
flags cover exact-redelivery duplication (`--fuzz-dup-probability`), retention-window behavior
(`--fuzz-retention-mode`, `--fuzz-retention-seconds`), and same-snapshot conflict injection
(`--fuzz-conflict`). Run `python -m bench fuzz --help` for the complete current set.

## Artifacts

Each phase writes structured JSON under `--results-root`. Reports may include:

- `summary.md`
- `recall.csv`
- `results.csv`
- `pareto.png`
- per-phase JSON and timing files
- `metrics.json` and `experiments.jsonl` for experiment runs

Keep build and search evidence tied to the same exact Lance versions. Do not attribute a search
result to index parameters when the catalog served another publication.

## Reproducibility

Record these inputs with every result:

- git revision and pinned `pylance` or Lance crate version
- dataset name, row limit, batch count, and seed
- corpus checksum manifest
- workspace and result paths
- ingestion partitions, fragment sizing, index partitions, and index shards
- exact published Lance version for each route
- machine CPU, memory, local storage, and Spark version

Use a fresh workspace when comparing incompatible schema or index configurations. A shared corpus
cache is safe because downloads are checksummed and immutable.
