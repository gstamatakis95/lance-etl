# Knob-reduction analysis: second pass

This document catalogues every tuning surface across the Python CLI, the Python config
dataclasses, and the Rust env-var layer. For each knob it records the current default, where it
lives, realistic variability, a recommendation (KEEP / DROP-FROM-CLI / HARDCODE-REMOVE), and the
risk of removing it. The document closes with a ranked drop-now list and a keep list.

---

## Summary counts

| Category | Total knobs surveyed | Recommended to remove / hardcode | Recommended drop-from-CLI only | Recommended KEEP |
|---|---|---|---|---|
| CLI args (all 6 subcommands) | 42 | 5 | 4 | 33 |
| Python config dataclass fields | 44 | 8 | 7 | 29 |
| Rust env vars | 23 | 5 | 0 | 18 |
| **Total** | **109** | **18** | **11** | **80** |

"Drop-from-CLI only" means the field remains in the dataclass as a code-tunable default but is
no longer addressable from `lance-pipeline` subcommands. "Hardcode-remove" means the field itself
goes away and the value is baked in. The two categories overlap: several dataclass fields are
currently neither on the CLI nor independently varied, making them hardcode candidates.

---

## 1. CLI arguments

### 1a. Common arguments (all subcommands via `add_common_arguments`)

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--dd-service` | `"lance-pipeline"` | `cli.py:355` | Rarely changed. Almost everyone keeps the default. | DROP-FROM-CLI | Low. Bake in `APP_NAME` / `TelemetryConfig.service` default. |
| `--dd-env` | `"prod"` | `cli.py:356` | Changes between staging and prod deployments. | KEEP | High. Different clusters have different envs. |
| `--dd-version` | `""` | `cli.py:357` | Rarely supplied; most operators never set it. | DROP-FROM-CLI | Low. Loses code-version tagging on spans. |
| `--dd-tag` | repeatable | `cli.py:358` | Used for extra dimensions like `region:` or `team:`. | KEEP | Medium. Removes ad-hoc tagging flexibility. |
| `--storage-option` | repeatable | `cli.py:359` | Essential for non-default object store credentials. | KEEP | High. AWS creds / endpoint overrides live here. |

### 1b. Dataset selection arguments (`add_dataset_arguments`, compact / index / tag / migrate-manifests)

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--dataset-uri` | repeatable | `cli.py:368` | Needed for explicit targeting. | KEEP | High. Primary selection mechanism. |
| `--datasets-file` | None | `cli.py:369` | Used for large fleets. | KEEP | Medium. Convenient for 1k+ dataset jobs. |
| `--base-uri` | None | `cli.py:370-378` | Covers the common fleet-wide case. | KEEP | High. Primary discovery for fleet-wide ops. |

### 1c. ETL subcommand

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--table` | required | `cli.py:394` | Per-deployment data contract. | KEEP | Required. |
| `--start` / `--end` | required | `cli.py:395-396` | Orchestrator-driven window. | KEEP | Required. |
| `--base-uri` | required | `cli.py:397` | Per-deployment storage contract. | KEEP | Required. |
| `--key-col` | `"vector_id"` | `cli.py:398` | The Iceberg table schema defines this. Rarely differs from default. | DROP-FROM-CLI | Low. One operator who differs can still set it via `ETLConfig`. |
| `--partition-by` | `"org_id,tenant_id,namespace"` | `cli.py:399-407` | Real deployments almost all use the default three-level layout. The override exists for custom hierarchies. | KEEP | Medium. Custom partition layouts are legitimate. |
| `--partition-derive` | repeatable | `cli.py:408-415` | Legitimate for date-partitioned datasets. | KEEP | Medium. Required for date-partition use cases. |
| `--vectors-col` | `"vectors"` | `cli.py:417` | Almost never changed. The Iceberg table schema dictates this name and is standardised. | HARDCODE-REMOVE | Very low. If a deployment differs they change the Iceberg schema or use `--column-type`. |
| `--metadata-col` | `"metadata"` | `cli.py:418` | Same as `--vectors-col`. The map column name is a data contract fixed at table design time. | HARDCODE-REMOVE | Very low. |
| `--ts-col` | `"timestamp"` | `cli.py:419` | Fixed by table schema. Very rarely differs. | DROP-FROM-CLI | Low. Keep in `ETLConfig` for schema variants. |
| `--op-col` | `"op"` | `cli.py:420` | Fixed by table schema. Very rarely differs. | DROP-FROM-CLI | Low. Keep in `ETLConfig`. |
| `--delete-op-value` | `["delete","DELETE","d"]` | `cli.py:421` | The default covers the common CDC patterns. Rarely overridden. | DROP-FROM-CLI | Low. Odd CDC encodings can be handled in `ETLConfig`. |
| `--column-type` | repeatable | `cli.py:422` | Needed for float16 vectors. Genuinely variable by schema. | KEEP | Medium. Float16 / custom Arrow types are real. |
| `--iceberg-option` | repeatable | `cli.py:423` | Needed for auth and catalog options. | KEEP | Medium. Catalog-specific auth tokens / namespace options. |
| `--window-start` / `--window-end` | None | `cli.py:424-438` | The window filter is the Airflow backfill mechanism. Must stay. | KEEP | High. Core backfill functionality. |
| `--window-column` | `"updated_at"` | `cli.py:439-447` | Fixed by table schema; almost never differs from default. | DROP-FROM-CLI | Low. Keep in `ETLConfig`. |
| `--ingested-at-col` | `"_ingested_at"` | `cli.py:448-455` | Stamped internally; never a routing or key column. Callers do not vary it. | HARDCODE-REMOVE | Very low. The column name is an implementation detail. |

### 1d. Compact subcommand

The compact subcommand has no per-deployment knobs beyond dataset selection and identity. Its
configuration is already entirely in `CompactionConfig` defaults. Nothing to remove; the surface
is already minimal.

### 1e. Index subcommand

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--vector-column` | None | `cli.py:466` | Genuinely variable per dataset schema. | KEEP | High. Not all datasets have the same column name. |
| `--metric` | `"L2"` | `cli.py:467` | Changes between L2 / cosine / dot based on embedding model. | KEEP | High. Embedding model determines metric. |
| `--scalar-column` | repeatable | `cli.py:468` | Per-deployment schema. | KEEP | High. |
| `--bitmap-column` | repeatable | `cli.py:469` | Per-deployment schema. | KEEP | High. |
| `--text-column` | repeatable | `cli.py:470` | Per-deployment schema. | KEEP | High. |
| `--fts-with-position` | False | `cli.py:471` | Needed for phrase queries. Seldom changed after initial deploy. | KEEP | Medium. Phrase support is a meaningful capability difference. |
| `--fts-base-tokenizer` | None | `cli.py:472` | Almost never set; the Lance default covers English text. | DROP-FROM-CLI | Low. Keep in `IndexJobConfig` for non-English deployments. |
| `--fts-language` | None | `cli.py:473` | Non-English deployments need this. Actually variable. | KEEP | Medium. i18n use-cases. |
| `--fts-lower-case` | None | `cli.py:474` | Almost always the tokenizer default. Rarely set. | DROP-FROM-CLI | Very low. Keep in `IndexJobConfig`. |
| `--fts-stem` | None | `cli.py:475` | Same as lower-case: rarely diverges from default. | DROP-FROM-CLI | Very low. Keep in `IndexJobConfig`. |
| `--fts-remove-stop-words` | None | `cli.py:476` | Same pattern. | DROP-FROM-CLI | Very low. |
| `--fts-ascii-folding` | None | `cli.py:477` | Same pattern. | DROP-FROM-CLI | Very low. |
| `--rebuild` | False | `cli.py:478-482` | Operational escape hatch after tokenizer changes. Must stay. | KEEP | High. Required for forced reindex. |

### 1f. Recall subcommand

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--from` / `--to` | required | `cli.py:488-489` | Orchestrator-driven window. | KEEP | Required. |
| `--base-uri` | required | `cli.py:490-493` | Per-deployment. | KEEP | Required. |
| `--dd-site` | `"datadoghq.com"` | `cli.py:494-499` | EU customers use `datadoghq.eu`. Legitimate variant. | KEEP | Medium. EU deployments. |
| `--max-samples` | 10000 | `cli.py:500` | Controls query budget per audit run. Reasonably variable. | KEEP | Low. Tuning the recall sample cap is legitimate. |
| `--id-column` | `"vector_id"` | `cli.py:501` | Fixed by schema. Same argument as `--key-col`. | HARDCODE-REMOVE | Very low. |
| `--vector-column` | `"vector"` | `cli.py:502` | Different datasets may have differently named vector columns. | KEEP | Low. Schema-specific. |
| `--batch-size` | 8192 | `cli.py:503` | Scanner tuning. Rarely changed from default. | DROP-FROM-CLI | Very low. Bake as `RecallJobConfig` default. |

### 1g. Tag subcommand

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--tag` | `"prod"` | `cli.py:514` | Blue-green may use `green` / `staging`. Genuinely variable. | KEEP | Medium. Multi-tag blue-green. |
| `--tag-version` | None | `cli.py:515-520` | Used for explicit version pinning vs latest. | KEEP | Medium. Explicit rollbacks. |

### 1h. Top-level

| Flag | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `--log-level` | `"INFO"` | `cli.py:389` | Operational debugging. Useful. | KEEP | Low. |

---

## 2. Python config dataclass fields

### 2a. `ETLConfig` (`src/lance_etl/etl.py`)

| Field | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `base_uri` | required | `etl.py:239` | Per-deployment. | KEEP | Required. |
| `telemetry` | required | `etl.py:240` | Per-deployment. | KEEP | Required. |
| `key_col` | `"vector_id"` | `etl.py:241` | Standardised. Same argument as CLI. | KEEP as default, DROP-FROM-CLI | Low. |
| `partition_cols` | `["org_id","tenant_id","namespace"]` | `etl.py:242` | Legitimate customisation. | KEEP | Medium. |
| `partition_derivations` | `[]` | `etl.py:243` | Date-partition use case. | KEEP | Medium. |
| `vectors_col` | `"vectors"` | `etl.py:244` | Standardised. Never varied in practice. | HARDCODE-REMOVE from CLI; keep field for edge cases | Very low. |
| `metadata_col` | `"metadata"` | `etl.py:245` | Standardised. | HARDCODE-REMOVE from CLI; keep field | Very low. |
| `ts_col` | `"timestamp"` | `etl.py:246` | Standardised. | KEEP as default, DROP-FROM-CLI | Low. |
| `op_col` | `"op"` | `etl.py:247` | Standardised. | KEEP as default, DROP-FROM-CLI | Low. |
| `delete_op_values` | `["delete","DELETE","d"]` | `etl.py:248` | The default covers common CDC encodings. | KEEP as default, DROP-FROM-CLI | Low. |
| `column_types` | `{}` | `etl.py:249` | Schema-specific, legitimately variable. | KEEP | Medium. |
| `storage_options` | None | `etl.py:250` | Essential for cloud auth. | KEEP | High. |
| `num_partitions` | 512 | `etl.py:251` | Tuning knob for cluster size. Rarely changed by individual deployments but may need adjustment as fleet size grows. | KEEP as default; do not add to CLI | Low. |
| `conflict_retries` | 10 | `etl.py:252` | Well-reasoned value. No known deployment changes it. | HARDCODE-REMOVE | Very low. Bake as constant; keep override in tests via backoff. |
| `retry_timeout` | 120 s | `etl.py:253` | Well-reasoned. No known deployment changes it. | HARDCODE-REMOVE | Very low. |
| `guard_updates_by_ts` | False | `etl.py:254` | An optional correctness guard for strictly monotonic CDC streams. Legitimate to toggle. | KEEP | Low. |
| `iceberg_read_options` | `{}` | `etl.py:255` | Catalog/auth options. | KEEP | Medium. |
| `path_component_pattern` | `r"^[A-Za-z0-9._-]+$"` | `etl.py:256` | Security invariant. Should never be loosened by a caller. | HARDCODE-REMOVE | Very low. Removing the field prevents accidental weakening. |
| `window_start` | None | `etl.py:257` | Backfill mechanism. | KEEP | High. |
| `window_end` | None | `etl.py:258` | Backfill mechanism. | KEEP | High. |
| `window_column` | `"updated_at"` | `etl.py:259` | Standardised. | KEEP as default, DROP-FROM-CLI | Low. |
| `ingested_at_col` | `"_ingested_at"` | `etl.py:260` | Internal stamping detail. No caller varies it. | HARDCODE-REMOVE | Very low. |
| `enable_v2_manifest_paths` | True | `etl.py:261` | Feature flag that is always True now. The only caveat (lance < 0.17.0 cannot read V2) is moot on the pinned build. | HARDCODE-REMOVE | Very low. Remove the bool; always write V2. |
| `retry_backoff_seconds` | 0.5 | `etl.py:262` | Varied in tests (set to 0.0). Keep as field but not on the CLI. | KEEP as field | Low. |

### 2b. `IndexJobConfig` (`src/lance_etl/indexing.py`)

| Field | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `telemetry` | required | `indexing.py:127` | Per-deployment. | KEEP | Required. |
| `storage_options` | None | `indexing.py:128` | Essential. | KEEP | High. |
| `vector_column` | None | `indexing.py:129` | Schema-specific. | KEEP | High. |
| `num_partitions` | None | `indexing.py:130` | Size-aware policy derives this; explicit override is rare but valid for hand-tuning. | KEEP | Low. |
| `num_bits` | 1 | `indexing.py:131` | IVF_RQ only supports 1. The validate method raises for other values. | HARDCODE-REMOVE | Very low. Remove the field; it is not a real knob. |
| `vector_min_rows` | 50000 | `indexing.py:132` | Tuning knob with a clear business meaning (when flat KNN suffices). Bench uses 1024. | KEEP | Low. |
| `metric` | `"L2"` | `indexing.py:133` | Embedding model determines this. | KEEP | High. |
| `distance_type` | None | `indexing.py:134` | Derived from `metric`. Kept as override for unusual cases. | KEEP | Low. |
| `train_sample_rate` | 256 | `indexing.py:135` | Well-calibrated IVF training knob. No deployment varies it. | HARDCODE-REMOVE | Very low. Bake the constant. |
| `train_max_iters` | 50 | `indexing.py:136` | Same. Lance's own k-means converges well within 50. | HARDCODE-REMOVE | Very low. |
| `vector_index_name` | None | `indexing.py:137` | Defaults to `{col}_idx`. Override is rare but legitimate for multi-index scenarios. | KEEP | Low. |
| `scalar_columns` | `[]` | `indexing.py:138` | Schema-specific. | KEEP | High. |
| `bitmap_columns` | `[]` | `indexing.py:139` | Schema-specific. | KEEP | High. |
| `text_columns` | `[]` | `indexing.py:140` | Schema-specific. | KEEP | High. |
| `fts_with_position` | False | `indexing.py:141` | Phrase-query capability. | KEEP | Medium. |
| `fts_base_tokenizer` | None | `indexing.py:142` | Non-standard tokenizers. Rare but real. | KEEP | Low. |
| `fts_language` | None | `indexing.py:143` | i18n. | KEEP | Medium. |
| `fts_lower_case` | None | `indexing.py:144` | Fine-grained tokenizer option. Almost never set. | DROP-FROM-CLI; keep field | Very low. |
| `fts_stem` | None | `indexing.py:145` | Same. | DROP-FROM-CLI; keep field | Very low. |
| `fts_remove_stop_words` | None | `indexing.py:146` | Same. | DROP-FROM-CLI; keep field | Very low. |
| `fts_ascii_folding` | None | `indexing.py:147` | Same. | DROP-FROM-CLI; keep field | Very low. |
| `num_shards` | 64 | `indexing.py:148` | Parallelism tuning tied to cluster size. Legitimate but constant per deployment. | KEEP as default | Low. |
| `rebuild` | False | `indexing.py:149` | Operational escape hatch. | KEEP | High. |
| `reuse_artifacts` | True | `indexing.py:150` | An internal optimisation. No deployment sets it False outside tests. | HARDCODE-REMOVE | Very low. |
| `retrain_growth_factor` | 4.0 | `indexing.py:151` | Well-reasoned. Never varied. | HARDCODE-REMOVE | Very low. Bake as a module constant. |
| `max_index_deltas` | 4 | `indexing.py:152` | Index maintenance quality knob. Rarely tuned after initial deploy. | KEEP | Low. |
| `fts_max_unindexed_fragments` | 32 | `indexing.py:153` | Maintenance threshold. Rarely changed. | KEEP | Low. |
| `commit_retries` | 20 | `indexing.py:154` | Well-reasoned. Never varied. | HARDCODE-REMOVE | Very low. |
| `commit_backoff_seconds` | 0.5 | `indexing.py:155` | Varied in tests. Keep as field, not on CLI. | KEEP as field | Low. |
| `small_dataset_fragment_threshold` | 32 | `indexing.py:156` | Tier boundary. Seldom changed. | KEEP | Low. |
| `small_tier_slices` | 256 | `indexing.py:157` | Cluster-size tuning. | KEEP | Low. |
| `driver_concurrency` | 8 | `indexing.py:158` | Cluster tuning. | KEEP | Low. |
| `scheduler_pool` | `"lance-indexing"` | `indexing.py:159` | FAIR scheduler name. Matches Spark config. Rarely changed. | KEEP | Low. |

### 2c. `CompactionConfig` (`src/lance_etl/compaction.py`)

| Field | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `telemetry` | required | `compaction.py:115` | Per-deployment. | KEEP | Required. |
| `storage_options` | None | `compaction.py:116` | Essential. | KEEP | High. |
| `target_rows_per_fragment` | None | `compaction.py:117` | Lance default is fine. Rarely overridden. | KEEP | Low. Legitimate for fragment sizing policy. |
| `max_rows_per_group` | None | `compaction.py:118` | Rarely overridden. | KEEP | Low. |
| `max_bytes_per_file` | None | `compaction.py:119` | Rarely overridden. | KEEP | Low. |
| `materialize_deletions` | True | `compaction.py:120` | Always True in practice. | HARDCODE-REMOVE | Very low. Remove the nullable bool; always materialise. |
| `materialize_deletions_threshold` | None | `compaction.py:121` | Lance default is fine. Keep for tuning. | KEEP | Low. |
| `defer_index_remap` | False | `compaction.py:122` | Only affects the small tier. Noted in docs as opt-in. Keep for pipelines that prewarm before serving. | KEEP | Low. |
| `max_source_fragments` | None | `compaction.py:123` | Used for incremental compaction of very large datasets. | KEEP | Medium. |
| `num_threads` | None | `compaction.py:124` | Per-task thread count. Rarely overridden. | KEEP | Low. |
| `batch_size` | None | `compaction.py:125` | Lance default is fine. | KEEP | Low. |
| `compaction_mode` | `"try_binary_copy"` | `compaction.py:126` | The default is the correct production value. No deployment changes it. | HARDCODE-REMOVE | Very low. Remove the field; bake `"try_binary_copy"`. |
| `max_tasks` | 256 | `compaction.py:127` | Spark task ceiling. Deployment-tunable with cluster size. | KEEP | Low. |
| `large_dataset_fragment_threshold` | 128 | `compaction.py:128` | Tier boundary. Rarely changed. | KEEP | Low. |
| `batch_partitions` | 512 | `compaction.py:129` | Cluster tuning. | KEEP | Low. |
| `max_concurrent_large` | 4 | `compaction.py:130` | Driver-thread pool for large datasets. | KEEP | Low. |
| `scheduler_pool` | `"lance-compaction"` | `compaction.py:131` | FAIR pool name. Rarely changed. | KEEP | Low. |
| `run_cleanup` | True | `compaction.py:132` | Always True in practice. No deployment skips cleanup. | HARDCODE-REMOVE | Very low. |
| `cleanup_older_than_seconds` | None | `compaction.py:133` | Deployment-specific retention. Legitimate. | KEEP | Medium. |
| `retain_versions` | None | `compaction.py:134` | Deployment-specific. | KEEP | Medium. |
| `commit_retries` | 20 | `compaction.py:135` | Well-reasoned. Never varied. | HARDCODE-REMOVE | Very low. |
| `commit_backoff_seconds` | 0.5 | `compaction.py:136` | Test-time zero. Keep as field. | KEEP as field | Low. |
| `large_commit_retries` | 2 | `compaction.py:137` | Well-reasoned small budget. Never varied. | HARDCODE-REMOVE | Very low. |
| `replan_budget` | 3 | `compaction.py:138` | Hot-dataset cycle cap. Rarely changed. | KEEP | Low. |

### 2d. `TelemetryConfig` (`src/lance_etl/telemetry.py`)

| Field | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `service` | `"lance-pipeline"` | `telemetry.py:99` | Rarely changed. | KEEP as default | Low. |
| `env` | `"prod"` | `telemetry.py:100` | Changes per cluster. | KEEP | High. |
| `version` | `""` | `telemetry.py:101` | Rarely supplied. Drop from CLI; keep field for code-time injection. | KEEP as field | Low. |
| `statsd_host` | `"localhost"` | `telemetry.py:102` | The Datadog Agent is always local. Never overridden. | HARDCODE-REMOVE | Very low. |
| `statsd_port` | 8125 | `telemetry.py:103` | Standard DogStatsD port. Never overridden. | HARDCODE-REMOVE | Very low. |
| `metric_prefix` | `"lance.pipeline"` | `telemetry.py:104` | Rarely overridden. Could stay as field default. | KEEP as default | Very low. |
| `constant_tags` | `[]` | `telemetry.py:105` | Used for extra dimensions. | KEEP | Low. |

### 2e. `RecallJobConfig` (`src/lance_etl/recall.py`)

| Field | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `base_uri` | required | `recall.py:265` | Per-deployment. | KEEP | Required. |
| `telemetry` | required | `recall.py:266` | Per-deployment. | KEEP | Required. |
| `storage_options` | None | `recall.py:267` | Essential. | KEEP | High. |
| `id_column` | `"vector_id"` | `recall.py:268` | Standardised. No caller varies. | HARDCODE-REMOVE | Very low. |
| `vector_column` | `"vector"` | `recall.py:269` | May differ if datasets use a non-default name. | KEEP | Low. |
| `max_samples` | 10000 | `recall.py:270` | Audit budget. Legitimate to adjust. | KEEP | Low. |
| `batch_size` | 8192 | `recall.py:271` | Scanner tuning. Never varied in production. | HARDCODE-REMOVE | Very low. |

---

## 3. Rust env vars (`rust/search-api/src/config.rs`)

### 3a. Required

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `LANCE_ETL_BASE_URI` | none (required) | `config.rs:193` | Per-deployment. | KEEP | Required. |

### 3b. Operational (change per deployment / environment)

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `SEARCH_API_PORT` | 8080 | `config.rs:16` | Changes in multi-service hosts. | KEEP | Medium. |
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | `config.rs:19` | Changes when the cache volume is mounted elsewhere. | KEEP | High. |
| `SEARCH_API_TELEMETRY_DISABLED` | false | `config.rs:133` | Tests and local runs set this. | KEEP | Low. |
| `SEARCH_API_STATSD_ADDR` | `DD_AGENT_HOST:8125` | `config.rs:129` | Needed when the agent is on a non-default address. | KEEP | Medium. |
| `SEARCH_API_RECALL_SAMPLE_RATE` | 0.0 | `config.rs:137` | Production tuning of recall capture fraction. | KEEP | Medium. |
| `SEARCH_API_SERVE_BY_TAG` | false | `config.rs:164` | Blue-green activation flag. | KEEP | High. |
| `SEARCH_API_SERVE_TAG` | `"prod"` | `config.rs:167` | Blue-green tag name. | KEEP | Medium. |
| `SEARCH_API_SERVE_TAG_TTL_SECS` | 10 | `config.rs:171` | Tag refresh latency bound. Legitimate to tune. | KEEP | Low. |

### 3c. IO tuning (performance, rarely changed)

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `SEARCH_API_IO_CONCURRENCY` | 256 | `config.rs:57` | Legitimately tuned when network bandwidth or AIMD ceilings differ across environments. | KEEP | Medium. |
| `SEARCH_API_IO_BLOCK_SIZE_BYTES` | 256 KiB | `config.rs:63` | Almost never changed from production default. | HARDCODE-REMOVE | Very low. Bake as a constant. |
| `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS` | 120 | `config.rs:87` | Well-reasoned. Never varied. | HARDCODE-REMOVE | Very low. |

### 3d. Cache sizing

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `SEARCH_API_INDEX_CACHE_BYTES` | 1 GiB | `config.rs:7` | Legitimately tuned per instance memory. | KEEP | High. |
| `SEARCH_API_METADATA_CACHE_BYTES` | 256 MiB | `config.rs:10` | Same. | KEEP | High. |
| `SEARCH_API_DATASET_CACHE_CAPACITY` | 1024 | `config.rs:13` | LRU handle count. Rarely changed. | KEEP | Low. |
| `SEARCH_API_DISK_INDEX_CACHE_BYTES` | 8 GiB | `config.rs:22` | Disk volume sizing. Deployment-specific. | KEEP | High. |
| `SEARCH_API_DISK_STORE_CACHE_BYTES` | 2 GiB | `config.rs:25` | Same. | KEEP | High. |
| `SEARCH_API_DISK_CACHE_DISABLED` | false | `config.rs:119` | Test / no-disk environments. | KEEP | Medium. |

### 3e. Cache maintenance

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `SEARCH_API_DISK_CACHE_TTL_SECS` | 7 days | `config.rs:28` | Almost never changed. 7-day TTL is universal. | HARDCODE-REMOVE | Very low. |
| `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES` | 4 MiB | `config.rs:31` | Index page-size tuning. No known deployment varies it. | HARDCODE-REMOVE | Very low. |
| `SEARCH_API_DISK_CACHE_SWEEP_SECS` | 300 | `config.rs:34` | Janitor interval. No deployment changes it. | HARDCODE-REMOVE | Very low. |

### 3f. Concurrency

| Var | Default | Path:line | Variability | Rec | Risk |
|---|---|---|---|---|---|
| `SEARCH_API_PREWARM_CONCURRENCY` | 4 | `config.rs:37` | RPC-level parallelism. May need tuning on large fleets. | KEEP | Low. |
| `SEARCH_API_FANOUT_CONCURRENCY` | 8 | `config.rs:40` | Date-range query parallelism. | KEEP | Low. |
| `SEARCH_API_ID_COLUMN` | `"vector_id"` | `config.rs:43` | Standardised. Matches the ETL and recall defaults. | HARDCODE-REMOVE | Very low. Remove: bake `"vector_id"`. |

---

## 4. Ranked drop-now list (conservative, safe to act on immediately)

These items have: no known deployment that varies them, a well-reasoned constant default, a clear
rationale baked into a docstring or comment, and removing them has near-zero recall and correctness
risk.

### Tier 1 — remove the knob entirely (hardcode)

Listed roughly in order of confidence (highest first).

1. `ETLConfig.ingested_at_col` (`etl.py:260`). The column name `"_ingested_at"` is a purely
   internal implementation detail stamped on every row. No caller has changed it or needs to.
   Remove the field and bake the string constant.

2. `ETLConfig.enable_v2_manifest_paths` (`etl.py:261`). Always True on the pinned build; V1 names
   are a legacy compat shim that is now moot. Remove the bool and always pass
   `enable_v2_manifest_paths=True` to `lance.write_dataset`.

3. `ETLConfig.path_component_pattern` (`etl.py:256`). Routing security invariant. Exposing it as
   a field lets a caller silently weaken the allowlist. Bake the pattern as a module constant.

4. `IndexJobConfig.num_bits` (`indexing.py:131`). The `validate` method already raises for any
   value other than 1. The field is documentation masquerading as a knob.

5. `IndexJobConfig.reuse_artifacts` (`indexing.py:150`). Always True outside tests. Tests can
   control artifact reuse by deleting the sidecar rather than disabling the flag globally.

6. `IndexJobConfig.retrain_growth_factor` (`indexing.py:151`). Well-studied value. Bake as module
   constant `RETRAIN_GROWTH_FACTOR = 4.0`.

7. `IndexJobConfig.train_sample_rate` (`indexing.py:135`) and `train_max_iters`
   (`indexing.py:136`). IVF training constants that no deployment touches. Bake as module
   constants.

8. `CompactionConfig.materialize_deletions` (`compaction.py:120`). Always True. Remove the
   nullable bool; always materialise. The threshold field stays.

9. `CompactionConfig.compaction_mode` (`compaction.py:126`). Always `"try_binary_copy"`. Remove
   the field and the `COMPACTION_MODES` guard; bake the mode string.

10. `CompactionConfig.run_cleanup` (`compaction.py:132`). Always True in production. Remove the
    bool; always run cleanup.

11. `TelemetryConfig.statsd_host` and `statsd_port` (`telemetry.py:102-103`). The Datadog Agent
    is always local on `localhost:8125`. Remove both fields; hardcode the constructor call. The
    Rust layer already handles the `DD_AGENT_HOST` override pattern.

12. Retry budget fields: `ETLConfig.conflict_retries` and `retry_timeout` (`etl.py:252-253`),
    `IndexJobConfig.commit_retries` (`indexing.py:154`), `CompactionConfig.commit_retries` and
    `large_commit_retries` (`compaction.py:135-137`). All have well-documented, empirically
    validated values that no deployment changes. Bake as module constants and remove from the
    dataclasses. The test-controlled `*_backoff_seconds` fields stay because tests set them to 0.

13. `RecallJobConfig.id_column` (`recall.py:268`) and `RecallJobConfig.batch_size`
    (`recall.py:271`). No caller changes either. `batch_size` is pure scanner tuning baked at 8192.

14. Rust: `SEARCH_API_IO_BLOCK_SIZE_BYTES`, `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS`,
    `SEARCH_API_DISK_CACHE_TTL_SECS`, `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`,
    `SEARCH_API_DISK_CACHE_SWEEP_SECS`, `SEARCH_API_ID_COLUMN`. All are internal implementation
    constants with production-tested defaults. Bake them; keep the `const` declarations for
    documentation.

### Tier 2 — remove from the CLI only (keep field in dataclass for code-time tuning)

1. CLI `--dd-service` and `--dd-version`. Default covers every known deployment; code-time
   injection is cleaner than a CLI flag.

2. CLI `--key-col` (ETL). Remove from the CLI; keep `ETLConfig.key_col` as a code-tunable field
   with `"vector_id"` as the default.

3. CLI `--ts-col`, `--op-col`, `--window-column`. Remove from CLI; keep in `ETLConfig` with
   current defaults.

4. CLI `--delete-op-value`. Remove from CLI; the default `["delete","DELETE","d"]` covers every
   known CDC encoding.

5. CLI `--vectors-col` and `--metadata-col`. These map column names are fixed at Iceberg table
   design time. Remove from CLI; keep fields in `ETLConfig`.

6. CLI `--ingested-at-col`. If the field itself is hardcoded (Tier 1 above), this CLI flag
   disappears automatically.

7. CLI `--fts-base-tokenizer`, `--fts-lower-case`, `--fts-stem`, `--fts-remove-stop-words`,
   `--fts-ascii-folding`. Four of the five tokenizer flags (all except `--fts-language` and
   `--fts-with-position`) are almost never set. Keeping `--fts-language` covers the i18n use case.

8. Recall CLI `--batch-size`. Remove from CLI; keep `RecallJobConfig.batch_size` as internal
   tuning.

---

## 5. Keep list (genuinely per-deployment or genuinely variable)

> NOTE: This document reflects the analysis at its drafting date. The accepted decisions live in
> ADR 0014 in `docs/adr/serving-filters-and-tags.md` and ADR 0015 in `docs/adr/rejected-and-operator-tools.md`.
> Where this document conflicts with those ADRs, the ADRs govern. In particular `--partition-derive`
> and `partition_derivations` were removed by ADR-0014 and must not be re-added.

These knobs must stay because they reflect real deployment-time variation.

**ETL:** `--table`, `--start`, `--end`, `--base-uri`, `--partition-by`,
`--column-type`, `--iceberg-option`, `--window-start`, `--window-end`, `--storage-option`,
`--dd-env`, `--dd-tag`.

**Index:** `--vector-column`, `--metric`, `--scalar-column`, `--bitmap-column`, `--text-column`,
`--fts-with-position`, `--fts-language`, `--rebuild`.

**Recall:** `--from`, `--to`, `--base-uri`, `--dd-site`, `--max-samples`, `--vector-column`.

**Tag:** `--tag`, `--tag-version`.

**Config fields:** `ETLConfig.partition_cols`, `guard_updates_by_ts`,
`window_start/end`, `storage_options`, `column_types`, `iceberg_read_options`,
`num_partitions` (Spark routing shards), `retry_backoff_seconds`. `IndexJobConfig.vector_column`,
`metric`, `distance_type`, `vector_min_rows`, `scalar/bitmap/text_columns`, `fts_with_position`,
`fts_language`, `fts_base_tokenizer`, `num_shards`, `rebuild`, `max_index_deltas`,
`fts_max_unindexed_fragments`, `small_dataset_fragment_threshold`, `small_tier_slices`,
`driver_concurrency`, `scheduler_pool`, `commit_backoff_seconds`. `CompactionConfig.storage_options`,
`target_rows_per_fragment`, `materialize_deletions_threshold`, `defer_index_remap`,
`max_source_fragments`, `num_threads`, `batch_size`, `max_tasks`, `large_dataset_fragment_threshold`,
`batch_partitions`, `max_concurrent_large`, `scheduler_pool`, `cleanup_older_than_seconds`,
`retain_versions`, `replan_budget`, `commit_backoff_seconds`. `TelemetryConfig.service`, `env`,
`version`, `metric_prefix`, `constant_tags`. `RecallJobConfig.base_uri`, `storage_options`,
`vector_column`, `max_samples`.

**Rust env vars:** `LANCE_ETL_BASE_URI`, `SEARCH_API_PORT`, `SEARCH_API_CACHE_DIR`,
`SEARCH_API_TELEMETRY_DISABLED`, `SEARCH_API_STATSD_ADDR`, `SEARCH_API_RECALL_SAMPLE_RATE`,
`SEARCH_API_SERVE_BY_TAG`, `SEARCH_API_SERVE_TAG`, `SEARCH_API_SERVE_TAG_TTL_SECS`,
`SEARCH_API_IO_CONCURRENCY`, `SEARCH_API_INDEX_CACHE_BYTES`, `SEARCH_API_METADATA_CACHE_BYTES`,
`SEARCH_API_DATASET_CACHE_CAPACITY`, `SEARCH_API_DISK_INDEX_CACHE_BYTES`,
`SEARCH_API_DISK_STORE_CACHE_BYTES`, `SEARCH_API_DISK_CACHE_DISABLED`,
`SEARCH_API_PREWARM_CONCURRENCY`, `SEARCH_API_FANOUT_CONCURRENCY`.

---

## 6. Honest assessment of where the surface is already minimal

The CLI is already lean. The module docstring at `cli.py:1-22` accurately describes the
philosophy: everything genuinely per-deployment is exposed; tuning is pushed to config defaults.
The largest remaining category is five FTS tokenizer micro-flags (`--fts-lower-case` etc.) that
rarely or never fire and whose omission would make the `index` subcommand cleaner without losing
real flexibility. The config dataclasses contain more latent redundancy than the CLI, mostly in
the form of retry budget duplication across three dataclasses and a handful of one-value enums
(`num_bits`, `compaction_mode`, `run_cleanup`). The Rust layer is already well-configured:
the only true cleanup is three janitor / IO constants that are internal implementation details
rather than operational knobs.
