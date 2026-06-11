# bench — BIGANN / SIFT1B end-to-end benchmark runbook

This runbook covers the reproducible commands for running the benchmark at 1M (smoke), 100M (local), and 1B (remote) scales, the server build and start procedure, disk and time budgets, checksum semantics, and the cold-vs-warm measurement recipe.

---

## Prerequisites

```bash
# Install Python dependencies (editable + bench extras)
uv pip install -e ".[dev]"
uv pip install --group bench

# Build pylance from the local checkout (required until >=8.0.0b6 ships on PyPI)
cd /Users/gstamatakis/IdeaProjects/lance
maturin develop --release -m python/Cargo.toml
```

---

The ETL now chunks `merge_insert` sources in bounded row batches (`merge_batch_rows`,
default 250,000 rows per chunk). This makes `LANCE_MEM_POOL_SIZE` optional headroom rather
than a hard requirement. For multi-batch runs at 1M scale or larger the env var is still
recommended to give DataFusion extra spill budget:

```bash
export LANCE_MEM_POOL_SIZE=4294967296
```

---

## Smoke run — sift1m (~160 MB download, official ground truth)

The smallest corpus with published ground truth. Downloads the IRISA SIFT1M tarball once
and caches it under `bench/workspace/sift/`.

```bash
python -m bench all \
  --dataset sift1m \
  --batches 2 \
  --etl-partitions 4 \
  --num-partitions 128 \
  --endpoint localhost:50051 \
  --workspace bench/workspace \
  --results-root bench/results
```

For an offline fixture run without any download, use the pytest integration tests:

```bash
.venv/bin/pytest tests/test_bench_e2e.py tests/test_bench_e2e_tagged.py -x -q -m integration
```

The tests write minimal bigann u8bin files directly into a temp workspace so no network
access is needed. They exercise the full real adapter IO path through Spark local mode.

---

## Local 100M run — BIGANN

Requires about 10 GB of compressed download traffic, 12.8 GB for the converted base artifact, and 30–80 GB total disk.

### Step 1 — Download the corpus prefix

```bash
python -m bench download \
  --dataset bigann \
  --limit 100000000 \
  --workspace bench/workspace
```

The corpus is the original IRISA corpus-texmex distribution (`http://corpus-texmex.irisa.fr/`), with a HuggingFace HTTPS mirror as fallback. The base file `bigann_base.bvecs.gz` is streamed and decompressed on the fly. Only the compressed bytes needed for the first `--limit` vectors are transferred (about 10 GB for 100M, instead of the full 98 GB archive), and the vectors are written locally in u8bin layout. An interrupted transfer resumes from the compressed `.gz.partial` sidecar without refetching. The query file `bigann_query.bvecs.gz` (10K vectors, ~1 MB) is fetched in full. The ground-truth tarball `bigann_gnd.tar.gz` is fetched once when the limit matches a published prefix size. A checksum manifest `checksums-100000000.json` is written under `bench/workspace/bigann/`.

Official ground truth ships for exactly ten prefix sizes:

| Prefix | Tar member | Prefix | Tar member |
|---|---|---|---|
| 1M | `gnd/idx_1M.ivecs` | 50M | `gnd/idx_50M.ivecs` |
| 2M | `gnd/idx_2M.ivecs` | 100M | `gnd/idx_100M.ivecs` |
| 5M | `gnd/idx_5M.ivecs` | 200M | `gnd/idx_200M.ivecs` |
| 10M | `gnd/idx_10M.ivecs` | 500M | `gnd/idx_500M.ivecs` |
| 20M | `gnd/idx_20M.ivecs` | 1000M | `gnd/idx_1000M.ivecs` |

Any other `--limit` falls back to exact brute-force ground truth computed during the prepare phase (only sensible at small limits).

### Step 2 — Start the search server

```bash
cd rust/search-api
cargo build --release

LANCE_ETL_BASE_URI="bench/workspace/lance" \
SEARCH_API_PORT=50051 \
./target/release/search-api
```

Key environment variables (see `rust/search-api/src/config.rs` for the full list):

| Variable | Default | Purpose |
|---|---|---|
| `LANCE_ETL_BASE_URI` | (required) | Base directory of the Lance datasets |
| `SEARCH_API_PORT` | `8080` | TCP port for the gRPC server |
| `SEARCH_API_INDEX_CACHE_BYTES` | `1073741824` | In-process index cache budget |
| `SEARCH_API_METADATA_CACHE_BYTES` | `268435456` | Metadata cache budget |
| `SEARCH_API_DATASET_CACHE_CAPACITY` | `16384` | Open-dataset-handle LRU capacity (weighted units) |
| `SEARCH_API_DISK_CACHE_DIR` | `/tmp/rust-search/cache` | Persistent disk cache root |

### Step 3 — Run the e2e benchmark

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
  --endpoint localhost:50051 \
  --prewarm \
  --workspace bench/workspace \
  --results-root bench/results
```

This runs ETL and then a unified ``PipelineJob`` (compaction, index build, and interval-tag stamping in one serialized fleet run) for each of the 4 batches in sequence, then verifies all historical tags and runs tag-pinned recall measurements via gRPC. Per-stage index timings are not emitted by the e2e path because all index types run inside a single ``LanceIndexer.run`` call within the pipeline job.

---

## Remote 1B run

Only the `--limit`, `--num-partitions`, and performance-scaling knobs change. Everything else is identical to the 100M command.

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
  --vector-row-floor 1024 \
  --seed 42 \
  --endpoint <remote-host>:50051 \
  --prewarm \
  --workspace /mnt/nvme/bench/workspace \
  --results-root /mnt/nvme/bench/results
```

---

## Phase-major `all` command (for comparison)

The `all` subcommand runs phases in order (full ingest, then full index, then compact). Use it for SIFT1M baselines:

```bash
python -m bench all \
  --dataset sift1m \
  --batches 2 \
  --etl-partitions 4 \
  --num-partitions 128 \
  --endpoint localhost:50051 \
  --workspace bench/workspace \
  --results-root bench/results
```

---

## Memory sizing for the 1B run

The Spark driver process must hold more than just JVM overhead during the index-build phase.

**BITMAP segments (no driver merge):** Executors build per-shard bitmap segments and the
driver only commits them, unmerged, exactly like BTREE. Lance unions the segments in
parallel at query time. Consolidation happens through the delta-merge pass
(`optimize_indices`), which streams on an executor and never materialises all bitmaps at
once. The driver therefore has no bitmap-related heap requirement during index builds.

**Compaction index remap (driver heap):** When `defer_index_remap=False` (the default),
`Compaction.commit` remaps the bitmap index inline by loading all bitmaps into memory.
The peak is the same order of magnitude as the merge: 8–16 GB for a 1B-row bitmap index.
Set `defer_index_remap=True` in `MaintenanceConfig` to defer remapping through the
frag-reuse index instead. Both the `--num-shards 32` 1B run and any compaction pass on a
1B dataset require at least 32 GB of available driver RAM, with `defer_index_remap=True`
strongly recommended.

**IVF training sample (executor heap):** The `train_sample_memory_budget_bytes` field in
`IndexJobConfig` (default 8 GB) caps the IVF centroid training sample that lands on a single
executor. At 16384 partitions and 256 samples per partition over 128-dimensional float32
vectors, the sample is `16384 * 256 * 128 * 4 bytes` = 2.1 GB. This is within the default
budget for the 1B run at `--num-partitions 16384`. Each Spark executor needs at least 4 GB
heap to cover the training sample plus framework overhead.

---

## Disk and time budget (100M BIGANN, --no-text, 4 batches)

| Artifact | Approximate size | Notes |
|---|---|---|
| Compressed transfer (bvecs.gz prefix) | ~10 GB | Deleted after conversion completes |
| Converted base prefix (u8bin) | 12.8 GB | 100M rows × 128 bytes |
| Query file (u8bin) | 1.3 MB | 10K queries × 128 bytes |
| Ground-truth tarball (ivecs) | ~470 MB | All ten published prefix sizes |
| Iceberg warehouse | ~30 GB | Parquet partitioned by org |
| Lance datasets (per batch) | ~3–4 GB per batch | FSL float32, indexed |
| IVF index segments | ~2–5 GB | 4096 partitions, IVF_RQ |
| Total peak disk | ~50–80 GB | After compaction |

Tagged versions are exempt from Lance cleanup pruning. Retained tags inflate the Lance dataset footprint proportionally to the number of retained versions.

Approximate wall times on a 16-core workstation with NVMe storage:

| Phase | Per batch | 4 batches total |
|---|---|---|
| ETL (ingest) | 5–15 min | 20–60 min |
| Index build | 10–30 min | 40–120 min (incremental after batch 1) |
| Compaction | 2–10 min | 8–40 min |
| gRPC recall | < 1 min | < 4 min |

---

## checksums.json semantics

After a successful download, `bench/workspace/bigann/checksums-{limit}.json` records the sha256 digest of:

- `base`: the converted local base u8bin file (only the streamed prefix).
- `query`: the converted query u8bin file.
- `ground_truth_tarball`: the official IRISA `bigann_gnd.tar.gz` (when the limit matches a published prefix size).

Re-running `download` when the files already exist skips the network fetch and reports `skipped: true`. To force a re-download, delete the files manually or use `--force`.

To pin a checksum and detect corruption on future runs, pass `--sha256 <hex>` where `<hex>` is the base file's digest from `checksums-{limit}.json`. The download phase raises an error if the digest does not match.

---

## Cold-vs-warm measurement recipe

The e2e flow records `cold_ms` and `warm_ms` for the first query pair at each tag via the gRPC legs. These are client-measured single-stream latencies:

- `cold_ms`: the first query this client sends to the server for a given org at the given tag, after prewarming (when `--prewarm` is set) or without it.
- `warm_ms`: the immediately following identical query, benefiting from OS page cache and the server's in-process index cache.

To measure a true cold start (empty page cache and empty server cache):

1. Start a fresh server with an empty `SEARCH_API_DISK_CACHE_DIR`.
2. Run the e2e benchmark without `--prewarm`. Record `cold_ms` from the e2e artifact.
3. Stop and restart the server with the same empty cache directory.
4. Run the e2e benchmark with `--prewarm`. Record `cold_ms` again. This measures first-query latency after the Prewarm RPC has loaded metadata and index segments into the in-process cache.

The difference between the two `cold_ms` values quantifies the benefit of prewarming. The `warm_ms` in both runs measures the steady-state in-process cache hit latency.

---

## Tag naming and serve-tag semantics

Each batch's serve tag is named after the batch window's end time in UTC, formatted as `%Y%m%dT%H%M%SZ` (e.g. `20240101T060000Z`). Tag names contain only `[A-Za-z0-9._-]` to satisfy the Lance tag name constraint.

A tagged version is exempt from Lance version cleanup: `cleanup_dataset` never prunes a version that has a tag pointing at it. Serve tags therefore act as explicit retention pins, keeping historical snapshots readable across maintenance operations until the tag is explicitly moved or deleted.

The gRPC server's `version_ref` oneof on `VectorSearchRequest`, `TextSearchRequest`, `HybridSearchRequest`, and `PrewarmRequest` accepts either an exact `version` (uint64 committed-version id) or a `tag` (string) that the server resolves at request time. Pinning a search to a tag makes the result set reproducible across compaction and index rebuilds as long as the tag remains.

---

## Running the tests

```bash
# Fast bigann_io unit tests (no Spark, no network)
.venv/bin/pytest tests/test_bigann_io.py -x -q

# Full offline e2e test (Spark local, requires ~4 GB RAM)
.venv/bin/pytest tests/test_bench_e2e.py tests/test_bench_e2e_tagged.py -x -q -m integration
```

---

## Local telemetry capture

Pass `--capture-telemetry` to any `e2e` run to record metrics, traces, and logs to plain files
under `{workspace}/telemetry/` without a Datadog account or Docker.  Add `opentelemetry-proto`
to the bench dependency group first:

```bash
uv pip install --group bench
```

Then run with capture enabled (bigann 1M prefix, download once first):

```bash
python -m bench download --dataset bigann --limit 1000000 --workspace bench/workspace
python -m bench e2e --dataset bigann --limit 1000000 --no-text --capture-telemetry \
  --workspace bench/workspace --results-root bench/results
```

Telemetry files written:

- `bench/workspace/telemetry/metrics.jsonl` — DogStatsD metrics from Python jobs and the Rust service
- `bench/workspace/telemetry/traces.jsonl` — OTLP spans from the Rust service
- `bench/workspace/telemetry/search-api.log` — Rust service JSON logs (redirect server stdout there manually)

See `bench/TELEMETRY.md` for the full reference: env vars for each emitter, the server redirect
recipe, jq one-liners for common questions, and the self-improvement loop.
