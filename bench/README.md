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

Use `python -m bench COMMAND --help` for the complete current flag set.

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

Build and start `rust/search-api` in local mode when measuring the catalog-backed read path. Publish
the benchmark datasets through the reconciler first so PostgreSQL contains an active publication
for every route. The request cannot choose a version. The harness checks each response's
`served_version` against the expected publication evidence.

```bash
cd rust/search-api
SEARCH_API_LOCAL_MODE=true \
LANCE_ETL_BASE_URI="$PWD/../../bench/workspace/lance" \
LANCE_ETL_DATABASE_URL='postgresql://localhost/lance_etl' \
SEARCH_API_TELEMETRY_DISABLED=true \
cargo run --locked
```

Run the search leg from the repository root with the local endpoint and the command's current
expected-version options from `--help`.

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
