# bench — BIGANN / SIFT1B end-to-end benchmark runbook

This runbook covers the reproducible commands for running the benchmark at 1M (smoke), 100M (local), and 1B (remote) scales, the server build and start procedure, disk and time budgets, checksum semantics, and the cold-vs-warm measurement recipe.

---

## Prerequisites

```bash
# Install Python dependencies (editable + bench extras)
uv pip install -e ".[dev]"
uv pip install --group bench

# pylance >=8.0.0 installs from PyPI with the dev extras above
```

---

The ETL chunks `merge_insert` sources by byte budget (`merge_batch_bytes`, default 64 MiB per
chunk). The rows-per-chunk is derived from the table's actual mean row width, so float32, uint8,
and other dtypes all stay within the DataFusion default pool (~100 MB) automatically. This makes
`LANCE_MEM_POOL_SIZE` optional headroom rather than a hard requirement for any corpus dtype. For
multi-batch runs at 1M scale or larger the env var is still recommended to give DataFusion extra
spill budget:

```bash
export LANCE_MEM_POOL_SIZE=4294967296
```

---

## Corpus cache — download once, reuse across workspaces

Raw corpus files (fvecs, u8bin, tarballs) land in a shared cache directory controlled by
`--corpus-root` (default `bench/corpora`).  The cache is independent of `--workspace`, so
switching to a fresh workspace never re-downloads data that is already present.

```bash
python -m bench download --dataset sift1m
python -m bench download --dataset sift1m --workspace bench/workspace-new
```

Both commands read from (and write to) `bench/corpora/sift/` by default.  To share a
cache across multiple checkout roots, point both runs at the same absolute path:

```bash
python -m bench download --dataset sift1m --corpus-root /data/bench-corpora
python -m bench e2e --dataset sift1m --corpus-root /data/bench-corpora --workspace /tmp/run1
```

Prepared artifacts (queries.npy, ground_truth.npz, manifests) remain workspace-scoped
under `{workspace}/prepared/` because they depend on `--limit`, `--tenants`, and other
shape flags that can differ between runs.

---

## Smoke run — sift1m (~160 MB download, official ground truth)

The smallest corpus with published ground truth. Downloads the IRISA SIFT1M tarball once
and caches it under `bench/corpora/sift/`.

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
  --limit 100000000
```

All files land in `bench/corpora/bigann/` by default.  Pass `--corpus-root` to redirect to
a different location shared across workspaces.

The corpus is the original IRISA corpus-texmex distribution (`http://corpus-texmex.irisa.fr/`), with a HuggingFace HTTPS mirror as fallback. The base file `bigann_base.bvecs.gz` is streamed and decompressed on the fly. Only the compressed bytes needed for the first `--limit` vectors are transferred (about 10 GB for 100M, instead of the full 98 GB archive), and the vectors are written locally in u8bin layout. An interrupted transfer resumes from the compressed `.gz.partial` sidecar without refetching. The query file `bigann_query.bvecs.gz` (10K vectors, ~1 MB) is fetched in full. The ground-truth tarball `bigann_gnd.tar.gz` is fetched once when the limit matches a published prefix size. A checksum manifest `checksums-100000000.json` is written under `bench/corpora/bigann/`.

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
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | Persistent disk cache root |

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

**IVF training (executor memory):** Vector bootstrap builds train centroids with lance's
streaming k-means (ADR 0030), which loads at most `num_partitions * streaming_sample_rate`
vectors per step instead of one giant sample, so training memory stays bounded regardless of
the partition count. No training memory budget needs tuning.

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

After a successful download, `bench/corpora/bigann/checksums-{limit}.json` records the sha256 digest of:

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

1. Start a fresh server with an empty `SEARCH_API_CACHE_DIR`.
2. Run the e2e benchmark without `--prewarm`. Record `cold_ms` from the e2e artifact.
3. Stop and restart the server with the same empty cache directory.
4. Run the e2e benchmark with `--prewarm`. Record `cold_ms` again. This measures first-query latency after the Prewarm RPC has loaded metadata and index segments into the in-process cache.

The difference between the two `cold_ms` values quantifies the benefit of prewarming. The `warm_ms` in both runs measures the steady-state in-process cache hit latency.

---

## Agent experiment loop — `python -m bench experiment`

One command runs a complete, measurable iteration: knobs in, `metrics.json` out. It chains
download and prepare when the corpus shape is missing (cached afterwards), wipes the Lance root
so every iteration is a clean build of the configured knobs, spawns and owns the `search-api`
server, runs the batch-major e2e body (real ETL, pipeline compaction, indexing, hour tags),
measures the on-disk footprint, restarts the server for a true cold first query, runs the full
`nprobes x refine_factors` recall sweep, and appends a one-line summary to
`{results_root}/experiments.jsonl`.

```bash
python -m bench experiment \
  --dataset sift1m --run-id iter-001 \
  --num-partitions 256 --target-rows-per-fragment 1048576 \
  --nprobes 1,10,25,50 --refine-factors none,5 \
  --server-env SEARCH_API_CACHE_BACKEND=disk
```

Server lifecycle flags: `--server-bin PATH` (default: the release build, then the debug build),
`--build-server` (run `cargo build --release` first), `--no-spawn-server` (measure an external
server at `--endpoint`), and repeatable `--server-env KEY=VALUE` for server-side knobs (cache
backend, cache budgets). Without any binary the run still completes and records the sweep as
skipped, like the other server-dependent legs. The server's output lands in
`{run_dir}/server.log`.

`metrics.json` schema (stable keys, everything an agent needs to compare iterations):

| Key | Contents |
|---|---|
| `run_id`, `knobs` | The full configuration dump, paths as strings |
| `server` | Spawned binary, endpoint, and env, or the skip reason |
| `build` | Total and per-batch ETL and pipeline wall seconds |
| `sizes` | Per-dataset and fleet `data_bytes` / `index_bytes` / `meta_bytes` / `total_bytes` and the index-to-data ratio |
| `sweep.points` | One record per `(nprobes, refine_factor)`: recall@1/10/100, mean/p50/p95/p99 ms, single-stream QPS |
| `sweep.first_query` | Per-org cold and warm first-query ms, cold measured after a server restart |
| `tags` | Tags created and whether historical-tag verification passed |
| `headline` | The distilled comparison numbers: best recall@10 point, the fastest point at recall@10 >= 0.95 (the knee), cold first-query ms, build seconds, and bytes |
| `baseline_delta` | Per-metric `{baseline, current, delta}` when `--baseline RUN_ID` was given |

The loop an agent runs is: pick knobs, run with a fresh `--run-id`, read `metrics.json`, adjust
knobs, run again with `--baseline <previous run id>`, and steer on the headline deltas. The
`experiments.jsonl` history holds one line per iteration (run id, knob vector, headline), so
the whole tuning trajectory is greppable:

```bash
python -m bench experiment --run-id iter-002 --num-partitions 512 --baseline iter-001
jq -c '{run: .run_id, knobs: .knobs.ivf_partitions, r10: .headline.best_recall_at_10, p95: .headline.knee_p95_ms, bytes: .headline.total_bytes}' \
  < bench/results/experiments.jsonl
```

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
python -m bench download --dataset bigann --limit 1000000
python -m bench e2e --dataset bigann --limit 1000000 --no-text --capture-telemetry \
  --workspace bench/workspace --results-root bench/results
```

Telemetry files written:

- `bench/workspace/telemetry/metrics.jsonl` — DogStatsD metrics from Python jobs and the Rust service
- `bench/workspace/telemetry/traces.jsonl` — OTLP spans from the Rust service
- `bench/workspace/telemetry/search-api.log` — Rust service JSON logs (redirect server stdout there manually)

See `bench/TELEMETRY.md` for the full reference: env vars for each emitter, the server redirect
recipe, jq one-liners for common questions, and the self-improvement loop.
