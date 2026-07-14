# bench — BIGANN / SIFT1M end-to-end benchmark runbook

This runbook covers reproducible offline build qualification at 1M, 100M, and 1B scales plus an
optional authenticated measurement against an externally managed production search fleet. The
harness never starts a private search binary or bypasses TLS, JWT authentication, or PostgreSQL
catalog resolution.

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
  --workspace bench/workspace \
  --results-root bench/results
```

For an offline fixture run without any download, use the pytest integration tests:

```bash
.venv/bin/pytest tests/test_bench_e2e.py tests/test_bench_e2e_tagged.py -x -q -m integration
```

The tests write minimal BIGANN u8bin files directly into a temporary workspace so no network
access is needed. They exercise the full real adapter IO path through Spark local mode. Search
evidence is recorded as `NOT_RUN` unless production credentials are supplied. This status is not
a built-server search result.

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

### Step 2 — Run offline build qualification

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

This run verifies the local Iceberg, ETL, Lance index, compaction, and historical-version path. It
does not combine those measurements with an unrelated external catalog route.

### Step 3 — Prepare the production search gate

Deploy the release image through the production manifests and publish the exact benchmark targets
through the reconciler. Obtain the fleet CA and one short-lived JWT for every exact target. The
token directory uses this fixed naming convention:

```text
tokens/tenant0--ns--org0.jwt
tokens/tenant0--ns--org1.jwt
```

Each JWT must authorize exactly the tenant, namespace, and organization named by its file. Token
files are read again for every RPC so an operator can rotate them during a long run.

Record the exact catalog publication expected for every target. Extra or missing targets and
non-positive versions are rejected.

```json
{
  "tenant0/ns/org0": 1042,
  "tenant0/ns/org1": 998
}
```

### Step 4 — Run the bound service benchmark

```bash
python -m bench search \
  --dataset bigann \
  --limit 100000000 \
  --no-text \
  --seed 42 \
  --endpoint search.production.example:443 \
  --search-ca-path /run/secrets/search/ca.pem \
  --search-token-dir /run/secrets/search/tokens \
  --search-expected-versions-path /run/release/bigann-expected-versions.json \
  --workspace bench/workspace \
  --results-root bench/results
```

The search request cannot select a tag or version. The benchmark instead validates every response's
`served_version` against the operator-owned publication file and fails on any mismatch. Build and
service artifacts remain separate, so an external result cannot be attributed to local index knobs.

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

The e2e flow records `cold_ms` and `warm_ms` for the first query pair against the final catalog
publication. These are client-measured single-stream latencies:

- `cold_ms`: the first query this client sends to the server for a given org.
- `warm_ms`: the immediately following identical query, benefiting from OS page cache and the server's in-process index cache.

The harness reports the first request sent by the benchmark client as `cold_ms`. It does not claim
that this is a fleet cold start because the production service and shared object-store caches may
already be warm. To measure a controlled fleet cold start:

1. Drain and replace the isolated benchmark fleet through the normal deployment workflow.
2. Confirm its prewarm and serving-catalog state through production controls.
3. Run the authenticated benchmark and record `cold_ms` from the e2e artifact.

The `warm_ms` value measures the immediately repeated in-process cache latency. Prewarming is an
internal publication gate and is not exposed to benchmark users through the search API.

---

## Agent experiment loop — `python -m bench experiment`

One command runs a complete, measurable offline build iteration: knobs in, `metrics.json` out. It chains
download and prepare when the corpus shape is missing (cached afterwards), wipes the Lance root
so every iteration is a clean build of the configured knobs, runs the batch-major e2e body, measures
the on-disk footprint and appends a one-line summary to
`{results_root}/experiments.jsonl`.

```bash
python -m bench experiment \
  --dataset sift1m --run-id iter-001 \
  --num-partitions 256 --target-rows-per-fragment 1048576
```

The experiment command is deliberately offline-only. Run the standalone `search` command with
`--endpoint`, `--search-ca-path`, `--search-token-dir`, and
`--search-expected-versions-path` after publishing the qualified candidate. A locally built binary
is never discovered or started, so CI cannot present an insecure process smoke test as production
search evidence.

`metrics.json` schema (stable keys, everything an agent needs to compare iterations):

| Key | Contents |
|---|---|
| `run_id`, `knobs` | The full configuration dump, paths as strings |
| `search_service` | External endpoint mode and `MEASURED`, `FAILED`, or `NOT_RUN` status |
| `build` | Total and per-batch ETL and pipeline wall seconds |
| `sizes` | Per-dataset and fleet `data_bytes` / `index_bytes` / `meta_bytes` / `total_bytes` and the index-to-data ratio |
| `sweep.points` | One record per `(nprobes, refine_factor)`: recall@1/10/100, mean/p50/p95/p99 ms, single-stream QPS |
| `sweep.first_query` | Per-org first and immediately repeated query latency |
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

The public gRPC API accepts only the logical tenant, namespace, and organization identity. The
server resolves the exact URI, Lance version, and release profile from PostgreSQL. Every response
returns `served_version` so benchmark evidence names the version that actually served the query.

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
