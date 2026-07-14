# lance_etl — the Spark ETL, indexing, and maintenance jobs

`src/lance_etl/` is the PySpark write path of the project. It ingests embeddings and text from an
Apache Iceberg table into per-tenant Lance vector datasets, builds distributed IVF_RQ / scalar /
FTS indices over them, and compacts and cleans them up. The companion Rust gRPC service under
`../../rust/search-api/` serves vector, full-text, and hybrid search over the datasets these jobs
produce.

Scale target: up to 1 billion vectors spread across up to 30,000 organisations with a power-law
size distribution.

See also: the repository-root `../../README.md` (system overview), `../../AGENTS.md` (repo-wide
rules), `./AGENTS.md` (Python-package agent guide with the layout and pylance API notes), and
`../../docs/adr/README.md` (architecture decisions).

---

## The jobs and how they compose

Production installs one reconciler entry point and one restricted operator entry point. The legacy
job modules remain directly importable for implementation reuse and tests. They are not scheduled
and have no console scripts because direct invocation bypasses durable work fencing.

| Entry point | Module | Role |
|---|---|---|
| `lance-etl-reconcile` | `lance_etl.reconciler` | Run one closed durable production reconciliation action |
| `lance-etl-tools` | `lance_etl.tools` | Unscheduled operator subcommands: `recall`, `migrate-namespace`, `optimize-iceberg` |

Production uses one durable reconciler DAG. PostgreSQL owns source-window, target-lane, lease,
phase, and serving-catalog state. Each scheduled process runs one closed action and can be retried
without inventing a new identity or accepting stale output.

---

## Driver/executor split

Every job follows the same shape: the driver plans the rounds, broadcasts read-only artifacts (IVF
centroids, the RaBitQ model, version pins, global-bucket maps), and commits. All heavy I/O and
compute — Lance dataset reads and writes, index segment builds, compaction task execution — run
inside executor closures (`mapInArrow`, `mapPartitions`, `map`). The driver never opens a
`lance.dataset` for row-level work. Every dataset size follows the same task shape. A small dataset
is simply the one-task case. The shared fleet helpers in `fanout.py` (`fan_out_per_dataset`,
`run_fleet_fanout`, `run_flat_tagged_job`) back the per-dataset fan-out for the maintenance,
indexing, and operator-tool phases, isolating each dataset's success or failure.

---

## ETL: adaptive routing and the disabled bulk path

Routing is the fixed trio `org_id/tenant_id/namespace`. Each row lands in the dataset
`base_uri/{org_id}/{tenant_id}/{namespace}.lance`, and because every key lives in exactly one
dataset the per-dataset `merge_insert` is the sole dedup mechanism. Every key in the source
`vectors`, `texts`, and `metadata` maps is pivoted automatically into a concrete typed column of
that dataset (dynamic map pivot, ADR 0024). No field declarations or type casts are needed, and a
key absent from a row yields NULL. The driver computes a per-trio count aggregation
(`compute_routing_plan`) and picks a routing plan: an explicit N-way salted shuffle plus a
partition sort sub-buckets the biggest datasets by key hash (`apply_salted_shuffle`) so no single
task carries a whole large org.

The retained bulk-append qualification path in `bulk.py` can short-circuit absent or empty
datasets through parallel `write_fragments` and one `commit_batch`. Production configuration
defaults it off because a raw append cannot distinguish a failed commit from an ambiguously
successful commit and therefore cannot guarantee duplicate-free retry. Routine ETL sends every
target through the replay-safe merge path.

---

## Indexing: the fleet phases and the segment API

`LanceIndexer` runs rounds of `plan -> artifacts -> build -> commit`:

- `plan_dataset_indexes` — executor fan-out that discovers per-dataset index targets from the
  `lance-etl.columns` role metadata the ETL writes and emits shard specs.
- `bootstrap_vector_index` — one executor task trains IVF centroids with streaming k-means and
  stores the artifact config, then `persist_bootstrap_centroids` writes the centroid sidecar.
- `build_one_shard` — one flat Spark job that builds vector, scalar, and FTS segments per shard
  through `make_handler(kind, ...)`, whose handler exposes `prepare` / `build_segment` / `merges()`.
- `commit_one_index` — executor fan-out that merges (vector and ZONEMAP) and publishes.
- `merge_index_deltas` — bounds accumulated BTREE/BITMAP deltas with a later `optimize_indices`
  pass on an executor.

Index builds are segment-API only. The full per-type recipe (Vector IVF_RQ, BTREE, BITMAP, ZONEMAP,
FTS INVERTED) is hard rule 6 in `../../AGENTS.md`, and the pylance API facts it depends on are in
`./AGENTS.md`. In short: fresh vector builds bootstrap once through a committed `create_index` with
streaming k-means and a stored `rabitq_model`, increments and rebuilds go through
`create_index_uncommitted` per shard with precomputed centroids, BTREE and BITMAP segments commit
unmerged, ZONEMAP segments merge before commit, and FTS uses the shared `index_uuid` +
`merge_index_metadata` path. A retrain is triggered when growth crosses
`growth_exceeds_retrain_factor`.

When no column flags are given the indexer discovers targets automatically (vector roles get
IVF_RQ, scalar roles BTREE, text roles BM25 INVERTED). Explicit column flags override discovery for
the run.

---

## Maintenance: compaction, TTL, cleanup, and clustered rewrite

`MaintenanceJob` runs the ordered per-dataset steps `plan -> execute -> commit`: per-row TTL
expiration (when `--ttl-column` is set), the unified plan-execute-commit compaction (one flat Spark
job over every dataset's rewrite tasks), and version cleanup. TTL deletes expired rows before
compaction so the compaction reclaims that storage. `compaction_skip_reason` skips datasets whose
derived state (fragment count) does not warrant a rewrite.

The retained clustered-rewrite qualification path (`cluster.py`, ADR 0041) physically reclusters
a dataset by IVF centroid. Production maintenance does not expose it on the command line and its
configuration defaults off. The Overwrite can clobber an overlapping writer and temporarily drops
indexes, so it cannot become a routine production path until those boundaries are qualified.

---

## Serving and interval tags

Production serving is pinned to the `HEAD` tag at a concrete dataset version. Tagged versions are
exempt from version cleanup. The `update_serving_tag` and `update_serving_tags` helpers take
`tags: Sequence[str]` and flip every named tag in one dataset open. Any call containing `HEAD`
requires an explicit target version. The temporary pipeline stamps hourly interval tags only and
never publishes `HEAD`.

Hourly interval tagging (ADR 0032): the ETL run stamps every dataset it wrote with the Lance tag of
the truncated UTC hour (format `%Y%m%dT%H%M%SZ`, for example `20260611T120000Z`). The stamp is
create-or-move, so a later run in the same hour advances that hour's tag to the newest version. The
pipeline prunes old interval tags (keep-last 48 by default). The search service can pin a query to
any such tag via `version_ref`.

The reconciler owns the safe sequence. It builds and validates an exact candidate, creates an
immutable candidate pin, prewarms every required replica at that exact version, commits the serving
catalog through PostgreSQL compare-and-swap, then mirrors `HEAD` on a best-effort basis.

---

## Production CLI

`lance-etl-reconcile` exposes the five parameter-free scheduled actions listed under Airflow and
one restricted `repair` command. It deliberately has no date, dataset, index, compaction, TTL,
cache, concurrency, or Spark tuning flags. Those choices belong to the versioned release profile.

The ETL, maintenance, indexing, and pipeline modules have no installed console scripts. Direct
module execution is a development surface and must not be placed in a production scheduler.

### `recall` — offline recall audit

Fetches Datadog-sampled vector, text, and hybrid search spans, replays each query as an exact
brute-force or exact BM25 scan against the dataset version that served it, and reports recall@k,
nDCG@k, and MRR per RPC-parameter bucket and per organisation. `DD_API_KEY` and `DD_APP_KEY` must be
set in the environment.

```bash
lance-etl-tools recall \
  --from 2024-01-15T00:00:00 \
  --to 2024-01-16T00:00:00 \
  --base-uri s3://my-bucket/lance \
  --dd-service lance-pipeline --dd-env prod
```

| Flag | Default | Purpose |
|---|---|---|
| `--from` | (required) | Window start (ISO 8601 or epoch ms) |
| `--to` | (required) | Window end (ISO 8601 or epoch ms) |
| `--base-uri` | (required) | Root URI for per-tenant Lance datasets |
| `--dd-site` | `datadoghq.com` | Datadog site domain for the Spans search API |
| `--max-samples` | `10000` | Cap on sampled spans fetched |
| `--vector-column` | `vector` | Fixed-size-list vector column for brute-force distances |

What it measures: **vector queries** are graded with an exact brute-force nearest-neighbor scan at
the pinned version (recall@k, nDCG@k, MRR against the true distance-ordered ranking). **Text
queries** (`recall.query_type=text`) are graded with exact Okapi BM25 ranking, primarily a
staleness and version-correctness signal since FTS returns exact results. **Hybrid queries**
(`recall.query_type=hybrid`) recompute exact vector and exact BM25 top-k, fuse with the recorded
strategy (RRF or weighted), and grade the served ids.

### `migrate-namespace` — copy a whole namespace to a new name

```bash
lance-etl-tools migrate-namespace \
  --source-namespace legacy \
  --target-namespace v2 \
  --base-uri s3://my-bucket/lance \
  --vector-column vector --metric cosine \
  --dd-service lance-pipeline --dd-env prod
```

Copies every dataset whose namespace component equals `--source-namespace` to the same address with
the namespace component replaced by `--target-namespace`. Source datasets are never deleted, so an
operator can verify the new namespace and flip serving through the blue-green tag helpers before
removing the source. Each target is recompacted and reindexed in production pipeline order after
copying. This is a one-off operator tool and is not scheduled in the Airflow DAG.

| Flag | Default | Purpose |
|---|---|---|
| `--source-namespace` | (required) | Namespace value to copy from |
| `--target-namespace` | (required) | Namespace value to copy to |
| `--base-uri` | (required) | Root URI under which per-tenant datasets live |
| `--partition-by` | `org_id,tenant_id,namespace` | Partition columns building the dataset path |
| `--no-recompact` | off | Skip compaction of target datasets after copying |
| `--no-reindex` | off | Skip index rebuild on target datasets after copying |
| `--overwrite-target` | off | Allow overwriting target datasets that already exist |

The index column flags (`--vector-column`, `--scalar-column`, `--bitmap-column`, `--text-column`,
`--metric`, `--fts-base-tokenizer`, `--fts-language`) are shared with the `index` entry point and
are optional. When none are given, reindexing is skipped with a warning.

### `optimize-iceberg` — optimize the upstream Iceberg source table

Runs Iceberg's own table maintenance procedures on the source Iceberg table, a separate store from
the Lance datasets maintained by the `maintenance` subcommand.

```bash
lance-etl-tools optimize-iceberg \
  --table prod.vectors.events \
  --dd-service lance-pipeline --dd-env prod
```

Four steps run in a fixed safe order. `rewrite_data_files` bin-packs small data files into larger
ones (default on). `rewrite_manifests` rewrites the manifest list to align with the new file layout
(default on, runs after the rewrite to stay consistent). `expire_snapshots` prunes snapshot history
beyond a retention horizon — at least the last 5 snapshots are always kept regardless of age, and
snapshots older than 7 days beyond that count are expired only with `--expire-snapshots`. Scheduled
production calls require the durable source retention gate first. `remove_orphan_files`
deletes files no live snapshot references — opt-in because it is the only step that can delete data
files outright. Iceberg's own three-day safety horizon is respected so an in-flight write is never
mistaken for an orphan. Heavy work runs distributed in Spark.

| Flag | Default | Purpose |
|---|---|---|
| `--table` | (required) | Fully-qualified Iceberg source table: `catalog.namespace.table` |
| `--no-rewrite-data-files` | off | Skip the bin-pack rewrite of small data files |
| `--no-rewrite-manifests` | off | Skip the manifest rewrite |
| `--expire-snapshots` | off | Expire snapshot history after the durable retention gate authorizes it |
| `--remove-orphan-files` | off (opt-in) | Delete files no live snapshot references |
| `--expire-retain-last` | `5` | Snapshots always retained regardless of age |
| `--expire-older-than-days` | `7` | Age horizon in days for snapshot expiration |

---

## Airflow deployment

Deploy `../../airflow/lance_etl_common.py` and `../../airflow/lance_etl_reconciler_dag.py`. The DAG
has exactly five serial tasks and `max_active_runs=1`:

1. `plan_and_enqueue_window`
2. `run_due_target_work`
3. `reconcile_results`
4. `gate_source_retention`
5. `emit_slo_status`

Every task has 24 Airflow retries. Durable target work has no attempt ceiling. A lease token and
target fence reject late workers, while periodic lease renewal protects long Spark phases. Work is
claimed one target at a time because execution is synchronous.

The DAG accepts no params, `dag_run.conf` tuning, date windows, dataset lists, index flags, TTL
flags, or cache knobs. Release policy lives in `DeploymentProfile`. Deployment supplies only
secrets and identities through the process environment:

| Environment value | Purpose |
|---|---|
| `LANCE_ETL_DATABASE_URL` | PostgreSQL control-plane URL using the psycopg 3 driver |
| `LANCE_ETL_LANCE_BASE_URI` | Deployment-owned Lance base URI |
| `LANCE_ETL_SOURCE_TABLE` | Two- or three-part Iceberg table identifier |
| `LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID` | Explicit first-start snapshot that is executor-qualified before acceptance |
| `DD_SERVICE`, `DD_ENV` | Low-cardinality telemetry identity |
| `LANCE_ETL_SPARK_CONN_ID` | Airflow Spark connection, default `spark_default` |

Iceberg is partitioned by `tenant_id`, `namespace`, `org_id`, and `hours(processing_timestamp)`.
Planning reads exact snapshot metadata and manifests. It never derives correctness from Airflow's
wall-clock interval. Initial startup scans the exact configured baseline and proves that each
target and `vector_id` has at most one distinct canonical mutation digest.

Restricted repair is outside the scheduled DAG. `repair --action retry-blocked --work-id ...`
retries one durable blocked identity. `repair --action rollback` requires the exact tenant,
namespace, organization, and retained successful work ID. Rollback only enqueues fenced PREWARM
work and never mutates the serving catalog directly.

The `lance-etl` wheel must be installed on every executor. Either bake it into the cluster image or
ship it via `spark.submit.pyFiles` (see the module docstring in `../../airflow/lance_etl_common.py`).

---

## Telemetry

`Telemetry.create(config)` is called once per process (driver and each executor). Only the
`TelemetryConfig` dataclass is pickled into closures, never a `Telemetry` object. Metrics are
namespaced under `config.metric_prefix` (default `lance.pipeline`) and tagged with `env:`,
`service:`, and optional constant tags. Lance trace events bridge to Datadog automatically on the
first `Telemetry.create` call per process. All commits go through `commit_with_retries`, which
re-reads the dataset before each attempt. See `./AGENTS.md` for the retry-budget constants.

---

## Install and test

```bash
uv venv
source .venv/bin/activate
uv pip install -e . --group dev
uv pip install --group bench

# Lint and format (must pass before any commit)
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/

# Run tests
.venv/bin/pytest -m "not integration"
```

`dev`, `bench`, and `airflow` are PEP 735 dependency groups, not extras, so install them with
`--group`, not `.[dev]`. `pylance>=8.0.0,<9` installs from PyPI, so `uv pip install -e . --group dev`
suffices for the core suite. The `airflow` group (`uv pip install --group airflow`) is needed only
to run the DAG-parse smoke test `tests/test_airflow_dags.py`, which otherwise self-skips. CI runs
that test in a dedicated job with the group installed.
