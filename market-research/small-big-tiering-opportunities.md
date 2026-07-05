# Small/big dataset tiering — where else it has merit

Read-only investigation. The two-tier pattern is already applied in indexing
(`src/lance_etl/indexing.py`) and compaction (`src/lance_etl/compaction.py`): classify by fragment
count, batch tiny datasets one-per-executor-task, fan out segment builds for big ones, drive big
ones concurrently from a driver thread pool on FAIR pools. This report finds where the same
dichotomy is missing, where it is already handled, and grades each.

## The established pattern (reference)

- Compaction `classify_or_compact` opens each dataset, reads `num_fragments`, and routes below
  `large_dataset_fragment_threshold` (default 128) to in-process `Compaction.execute` plus inline
  cleanup, above it to the distributed plan/execute/commit triad
  (`src/lance_etl/compaction.py:510-527`, `:469-507`, `:618-701`).
- Indexing `classify` runs one distributed fragment-count job, splits at
  `small_dataset_fragment_threshold` (default 32), batches small datasets into one Spark job
  (`run_small_tier` -> `index_dataset_locally`, one whole dataset per task,
  `src/lance_etl/indexing.py:1796-1880`, `:1966-1997`) and keeps the segment fan-out for big ones
  driven by a driver thread pool (`run_large_tier`, `:1999-2044`).

---

## 1. Recall brute-force scoring job (`src/lance_etl/recall.py`) — NOT size-aware. Clear win.

**Current behavior.** `RecallAuditJob.run` groups samples by `(dataset_uri, dataset_version)` and
fans out with `parallelize(items, len(items))` — exactly one Spark task per group
(`src/lance_etl/recall.py:1979-2003`). Each task runs `score_version_group`, which opens the
dataset once and scores every sample in-process (`:1689-1721`). The vector ground truth comes from
`brute_force_top_k_scored`, which streams the entire dataset through one task, keeping a running
top-k merged per batch (`:1059-1087`). BM25 ground truth (`bm25_top_k`) likewise materializes all
candidate text rows in the one task (`:1299` onward).

**Inefficiency at both extremes.** This is the one job that mirrors `etl.py`/`indexing.py`
fan-out in the docstring (`:12-15`) but skips their tiering entirely.

- Small extreme: the power-law tail produces thousands of tiny `(uri, version)` groups, each
  getting its own Spark task that pays scheduling plus a cold dataset open to brute-force a few
  thousand rows. Task overhead dominates useful work.
- Big extreme: a huge org's group brute-forces its whole dataset (potentially tens of millions of
  vectors) single-threaded in ONE task, for every sample in the group. Cost is
  `O(rows * samples)` with no per-fragment fan-out. This is the straggler the other jobs explicitly
  avoid.

**Proposed small/big treatment.** Classify groups by dataset size (reuse the existing
fragment-count probe pattern). Small tier: pack many tiny groups into a bounded number of Spark
slices (`small_tier_slices`-style) so one task scores many datasets, amortizing open and
scheduling cost. Big tier: fan the scan out per-fragment, compute a partial top-k per fragment,
and reduce. The reduce is the same stable-merge already used across batches
(`recall.py:1082-1086`), so exactness is preserved — the per-fragment partial-topk merge is
algebraically identical to the per-batch merge already in place.

**Expected win.** Removes the per-huge-tenant straggler that bounds wall-clock, and cuts task
count for the tail by 1-2 orders of magnitude. **Effort: M.** **Risk: M** — must keep the
brute-force/BM25 reference bit-identical; mitigated because the merge primitive already exists.

---

## 2. Rust serving — open-handle LRU capacity for a 30k-tenant tail (`rust/search-api`). Worth doing.

**Current behavior.** The open-`Dataset` handle cache is a flat Moka LRU with a count capacity of
`dataset_cache_capacity`, default **1024** (`src/config.rs:13`, `:202`; used at
`src/lance/provider.rs:90`, `:158`). The byte-budgeted index cache (1 GiB memory / 8 GiB disk) and
metadata cache (256 MiB) are shared and URI/index-UUID-prefixed, so they are size-fair across
tenants (`src/lance/provider.rs:56-76`, `:79-100`).

**Inefficiency at the small extreme.** With up to 30,000 tenants and a 1024-handle cap, the long
tail of tiny tenants churns through cold opens constantly even though a tiny-dataset handle is
cheap (manifest + schema + index listing). The cap is a uniform count regardless of handle weight,
so 1024 huge-tenant handles and 1024 tiny-tenant handles cost the same slot budget. A few huge
tenants' index pages can also evict the entire tail's hot index pages from the byte cache.

**Proposed small/big treatment.** Either raise the default handle capacity substantially (tiny
handles are cheap, so a 30k-tenant fleet wants far more than 1024 resident), or weight the Moka LRU
by per-handle cost so cheap tiny handles are retained longer than expensive huge ones. Optionally
give the tail and the few huge tenants separate cache segments so a hot whale cannot evict the
tail wholesale.

**Expected win.** Fewer cold opens on the tail -> lower P99 on tail-tenant queries. **Effort: S**
(raise default) to **M** (Moka weigher / segmentation). **Risk: low-M** — bounded by handle memory
footprint, which is small.

---

## 3. ETL merge routing (`src/lance_etl/etl.py`) — mostly already handled, residual is marginal.

**Current behavior.** After collapse, `run_on_dataframe` does
`collapsed.repartition(config.num_partitions, *routing)` — a fixed 512-way hash shuffle by routing
key (`src/lance_etl/etl.py:811`, `num_partitions` default 512 at `:251`). `merge_partition` then
groups each partition by routing key and runs one keyed, idempotent `merge_insert` per dataset
(`:814-838`, `apply_merge` at `:430-549`). The module docstring already frames this as the
small/big design (`:37-45`).

**Honest assessment — largely handled, with two residual edges.**

- Tiny orgs are already cheap: a tiny org is a small group merged in process at near-zero cost, an
  org with no rows in the window produces no group and touches no dataset, and bootstrap is a
  single empty append plus merge (`apply_merge`, `:433-445`). This is genuinely the small tier and
  needs nothing.
- Small-side residual (marginal): the shuffle width is a static 512 regardless of increment size.
  A window carrying only a handful of tiny orgs still fans into 512 partitions, most empty, paying
  512 task launches. Deriving `num_partitions` from observed distinct routing keys or input row
  count (or relying on Spark AQE coalescing, not currently configured — no `spark.sql.adaptive`
  reference exists) would trim empty-task overhead. **Effort: S. Risk: low. Win: small.**
- Big-side residual (largely fundamental): all rows of ONE hot routing key hash to ONE partition,
  so a whale org's window increment is one giant `merge_insert` in one task while peers idle — a
  classic skew straggler. This is NOT fixable by tiering: `merge_insert` is a single-writer,
  single-commit operation per dataset, so one key's rows cannot be split across tasks without
  commit conflicts. A "dedicated partition per whale" buys nothing because the merge is already the
  atomic unit. Document it rather than build it.

**Verdict: the ETL is already the small/big design it claims to be.** Only the adaptive
partition-count nit is actionable, and it is marginal.

---

## Already handled well (do not build)

- **Version cleanup is already a batched in-process sweep for tiny datasets.** There is no
  standalone cleanup job (`cli.py` has no cleanup subcommand — cleanup is bundled into compaction).
  Small-tier `compact_small_dataset` runs `cleanup_dataset` in the same executor task that
  compacted the dataset (`src/lance_etl/compaction.py:503`), driven by the batched
  `fan_out_per_dataset` small tier (`:739-769`). Large datasets clean on the driver after commit
  (`:649`, `:676`). Tiny datasets therefore already pay cleanup once, in a batched in-process
  sweep, exactly as requested. The only nit is cleanup runs even when nothing was rewritten, but
  `cleanup_old_versions` is cheap on shallow history.

- **Serving flat-KNN vs IVF is already size-correct, handled by Lance.** `run_vector_query` calls
  `scanner.nearest(...)` (`src/lance/backend.rs:436`). Lance uses the vector index when one exists
  and does an exact flat scan when it does not. Tiny datasets are deliberately left without a vector
  index (skipped below `vector_min_rows` = 50k, `src/lance_etl/indexing.py:1206-1218`,
  `:1826-1837`), so `nearest()` flat-scans them automatically — which is the correct cheap path for
  a tiny dataset. No server-side size branch is needed. (`bypass_vector_index` /
  `scanner.use_index(false)` at `backend.rs:460-462` is a client override, not size logic.)

- **IVF training is already degraded on tiny datasets.** `derive_num_partitions` clamps to
  `sqrt(rows)` and `degrade_num_partitions` lowers the partition count when the dataset cannot
  supply `num_partitions * sample_rate` training rows
  (`src/lance_etl/indexing.py:238-271`, applied at `:1427-1431`, `:1843-1844`). The vector index
  is skipped entirely below the row floor. Sample availability on tiny datasets is handled.

- **Index small-tier vs large-tier** is the reference implementation itself — no gap.

---

## Ranked "worth doing" list

| Rank | Opportunity | Side | Merit | Effort | File |
|------|-------------|------|-------|--------|------|
| 1 | Recall job two-tier (batch tiny groups / per-fragment partial-topk reduce for huge) | both | **Clear win** | M | `src/lance_etl/recall.py:1979-2003`, `:1689-1721`, `:1059-1087` |
| 2 | Rust handle-LRU sizing/weighting for 30k tail | small | Worth doing | S–M | `rust/search-api/src/config.rs:13`, `src/lance/provider.rs:90,158` |
| 3 | ETL adaptive shuffle-partition count | small | Marginal | S | `src/lance_etl/etl.py:811,251` |
| — | Rust prewarm/fanout concurrency size-awareness | both | Marginal (per-RPC, bounded defaults fine) | S | `rust/search-api/src/config.rs:37-40` |
| — | Version cleanup batched sweep | small | **Already handled** | — | `src/lance_etl/compaction.py:503,649` |
| — | Serving flat-KNN vs IVF | both | **Already handled** (by Lance) | — | `rust/search-api/src/lance/backend.rs:436` |
| — | IVF train degradation on tiny | small | **Already handled** | — | `src/lance_etl/indexing.py:238-271` |

## Top 3

1. **Recall brute-force scoring** — the only heavy job with no size tiering. Tiny groups waste a
   task each, a huge group brute-forces its whole dataset in one task. Add the same classify +
   batch-small / fan-out-big shape the indexing and compaction jobs already use. The partial-topk
   reduce reuses the existing per-batch merge, so exactness is free.
2. **Rust serving open-handle LRU** — a flat 1024-handle count cap churns cold opens across a
   30k-tiny-tenant tail. Raise/weight the LRU so cheap tiny handles persist and a whale cannot
   evict the tail.
3. **ETL adaptive partition count** — marginal: derive the shuffle width from increment size so a
   tiny window does not fan into 512 mostly-empty tasks. The big-side merge straggler is
   fundamental to single-writer `merge_insert` and should be documented, not engineered around.

## Already handled (confirmed, do not build)

Version cleanup (batched in the compaction small tier), serving flat-KNN vs IVF selection (Lance
does it automatically and tiny datasets are intentionally index-free), and IVF training
degradation on tiny datasets.
