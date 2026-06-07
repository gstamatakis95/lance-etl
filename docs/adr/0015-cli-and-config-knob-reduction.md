# 0015. CLI and config knob reduction: opinionated defaults

Status: Accepted

## Context

The Python CLI and Rust `Config` struct had accumulated roughly 30 configurable knobs beyond what any deployment
actually varied. Each extra knob is a surface area cost: it appears in `--help`, must be documented, must be
tested, and creates an implicit contract between operator config and code behavior. Knobs that are never varied
in practice are a maintenance liability, not a feature.

The knobs fell into two categories:

**Tier 1 (values that are universally correct and never varied):** constants like `num_bits=1` (the only value
IVF_RQ accepts), `materialize_deletions=True`, `run_cleanup=True`, `enable_v2_manifest_paths=True`, `compaction_mode="try_binary_copy"`,
`path_component_pattern`, `train_sample_rate=256`, `train_max_iters=50`, `retrain_growth_factor=4.0`,
`reuse_artifacts=True`, and the Rust constants `disk_cache_ttl_secs=7d`,
`store_cache_max_range_bytes=4MiB`, `disk_cache_sweep_secs=300s`, `io_block_size_bytes=256KiB`, and
`object_store_timeout_secs=120s`. These were exposed as env vars or dataclass fields, but no deployment ever
changed them.

**Tier 2 (CLI flags for dataclass fields that should not be CLI-configurable):** `--key-col`, `--vectors-col`,
`--metadata-col`, `--ts-col`, `--op-col`, `--delete-op-value`, `--window-column`, `--ingested-at-col`,
`--fts-lower-case`, `--fts-stem`, `--fts-remove-stop-words`, `--fts-ascii-folding`. These are schema
column names or fine-grained tokenizer toggles: they are appropriate as dataclass fields tunable in code for
test or custom deployments, but they should not be CLI flags that operators can vary per invocation, because
incorrect values here break ingestion semantics or tokenizer consistency.

**Retry budgets:** The retry budgets (`conflict_retries`, `commit_retries`, `large_commit_retries`) were
duplicated as literals across `ETLConfig`, `IndexJobConfig`, and `CompactionConfig` with no single canonical
home.

**Rust env knobs superseded by [0014](0014-drop-by-date-partitioning.md):** `SEARCH_API_FANOUT_CONCURRENCY` and
`SEARCH_API_ID_COLUMN` were removed with the fan-out feature itself. They are listed here as part of the
complete accounting of knob removals.

## Decision

**Tier 1 hardcoded constants (Python):**

Move these values from configurable dataclass fields to module-level constants with docstrings explaining why
they are baked:

- `IVF_RQ_NUM_BITS = 1` in `indexing.py` (IVF_RQ supports only 1).
- `TRAIN_SAMPLE_RATE = 256` in `indexing.py` (well-calibrated training constant).
- `TRAIN_MAX_ITERS = 50` in `indexing.py` (Lance k-means converges within this bound).
- `RETRAIN_GROWTH_FACTOR = 4.0` in `indexing.py` (triggers full rebuild on 4x row growth).
- `COMPACTION_MODE = "try_binary_copy"` in `compaction.py` (the only production-safe mode).
- `INGESTED_AT_COLUMN = "_ingested_at"` in `etl.py` (standardized payload column name).
- `PATH_COMPONENT_PATTERN = r"^[A-Za-z0-9._-]+"` in `etl.py` (security invariant, must not be weakenable by config).

The following dataclass fields become non-configurable by adopting the constant directly in the implementation:

- `IndexJobConfig.num_bits` removed. `IVF_RQ_NUM_BITS` used directly.
- `IndexJobConfig.train_sample_rate` removed. `TRAIN_SAMPLE_RATE` used directly.
- `IndexJobConfig.train_max_iters` removed. `TRAIN_MAX_ITERS` used directly.
- `IndexJobConfig.retrain_growth_factor` removed. `RETRAIN_GROWTH_FACTOR` used directly.
- `IndexJobConfig.reuse_artifacts` removed. Artifacts are always reused when not rebuilding.
- `CompactionConfig.compaction_mode` removed. `COMPACTION_MODE` used directly.
- `CompactionConfig.materialize_deletions` removed. Deletions are always materialized.
- `CompactionConfig.run_cleanup` removed. Cleanup always runs after a compaction commit.
- `ETLConfig.ingested_at_col` removed. `INGESTED_AT_COLUMN` used directly.
- `ETLConfig.path_component_pattern` removed. `PATH_COMPONENT_PATTERN` used directly.
- `ETLConfig.enable_v2_manifest_paths` removed. V2 manifest paths are always enabled (consistent with [0012](0012-v2-manifest-paths.md)).

**Tier 1 hardcoded constants (Rust):**

Remove these from `Config` struct and from `Config::from_env()`. They remain as named module constants for
documentation and internal use:

- `SEARCH_API_DISK_CACHE_TTL_SECS` removed. `DEFAULT_DISK_CACHE_TTL_SECS` (7 days) is always used.
- `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES` removed. `DEFAULT_STORE_CACHE_MAX_RANGE_BYTES` (4 MiB) is always used.
- `SEARCH_API_DISK_CACHE_SWEEP_SECS` removed. `DEFAULT_DISK_CACHE_SWEEP_SECS` (300 s) is always used.
- `SEARCH_API_IO_BLOCK_SIZE_BYTES` removed. `DEFAULT_IO_BLOCK_SIZE_BYTES` (256 KiB) is always used.
- `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS` removed. `DEFAULT_OBJECT_STORE_TIMEOUT_SECS` (120 s) is always used.

The `SEARCH_API_STATSD_ADDR` env knob is kept (statsd host and port vary by deployment and are infrastructure
config, not application tuning). All other remaining env knobs are kept.

**Tier 2 dropped CLI flags:**

Remove from `argparse` and from `run_etl` / `run_index` argument wiring:

- `--key-col` (schema constant, always `vector_id`).
- `--vectors-col` (schema constant, always `vectors`).
- `--metadata-col` (schema constant, always `metadata`).
- `--ts-col` (schema constant, always `timestamp`).
- `--op-col` (schema constant, always `op`).
- `--delete-op-value` (schema constant list, always `["delete", "DELETE", "d"]`).
- `--window-column` (schema constant, always `updated_at`).
- `--ingested-at-col` (superseded by `INGESTED_AT_COLUMN` constant).
- `--partition-derive` (removed with by-date partitioning, see [0014](0014-drop-by-date-partitioning.md)).
- `--fts-lower-case` (fine-grained tokenizer toggle, tunable in code but not per invocation).
- `--fts-stem` (same).
- `--fts-remove-stop-words` (same).
- `--fts-ascii-folding` (same).

**Retry budgets consolidated:**

Four literals are promoted to named constants in `telemetry.py` as the single canonical home:

- `DEFAULT_CONFLICT_RETRIES = 10` (ETL `merge_insert` commit loop).
- `DEFAULT_RETRY_TIMEOUT = timedelta(seconds=120)` (total ETL conflict-retry time budget).
- `DEFAULT_COMMIT_RETRIES = 20` (index and compaction commits, shared by `IndexJobConfig` and `CompactionConfig`).
- `DEFAULT_LARGE_COMMIT_RETRIES = 2` (tier-B `Compaction.commit` retry budget in `CompactionConfig`).

## Consequences

The `etl` CLI subcommand exposes fewer flags: `--key-col`, `--vectors-col`, `--metadata-col`, `--ts-col`,
`--op-col`, `--delete-op-value`, `--window-column`, `--ingested-at-col`, and `--partition-derive` are gone.
The `index` subcommand loses `--fts-lower-case`, `--fts-stem`, `--fts-remove-stop-words`, and
`--fts-ascii-folding`. Operators who previously overrode these flags must instead set the values in the
`ETLConfig` or `IndexJobConfig` dataclass in code.

The Rust search-api binary no longer reads `SEARCH_API_DISK_CACHE_TTL_SECS`, `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`,
`SEARCH_API_DISK_CACHE_SWEEP_SECS`, `SEARCH_API_IO_BLOCK_SIZE_BYTES`, or `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS`.
Setting these variables in deployment config has no effect. `SEARCH_API_STATSD_ADDR` is kept.

`COMPACTION_MODES` (a tuple of accepted modes) is replaced by `COMPACTION_MODE` (a single string constant).
The `execute_options` method no longer validates the mode field because the field no longer exists.

Retry budgets in `ETLConfig`, `IndexJobConfig`, and `CompactionConfig` reference the constants from `telemetry.py`
rather than hard-coded literals. A single edit in `telemetry.py` now propagates to all three callers.

The net change is 1718 lines deleted, 366 inserted, a reduction of roughly 74% in diff lines relative to the
previous baseline. The simplification is irreversible by design. If a deployment ever needs a non-default value
for a baked constant, the constant is changed in code with a rationale comment, not promoted back to a CLI flag.
