# `tests/`

The pytest suite for the whole repository: `src/lance_etl/`, `bench/`, and (through the
Postgres-gated tests) `migrations/`. It is one flat directory of 73 `test_*.py` modules plus
`conftest.py` — no subpackages, no per-source-package test directories. A module's name names its
subject rather than mirroring a package path (`test_replay_sink.py`, not
`etl/test_replay_sink.py`), which keeps every test file one `grep` away regardless of which
`src/lance_etl/` package it exercises. See the root [AGENTS.md](../AGENTS.md) and
[src/lance_etl/AGENTS.md](../src/lance_etl/AGENTS.md) for the coding rules these tests are also
held to (no `#` inline comments, no leading underscores, complete type hints, `ruff` clean).

## Running the suite

```bash
uv sync --locked --group dev --python 3.14.0
uv sync --locked --group dev --group bench --python 3.14.0   # adds bench deps for test_bench_*.py
uvx ruff format src/ tests/ bench/ migrations/
uvx ruff check src/ tests/ bench/ migrations/
.venv/bin/pytest -m "not integration"
```

`tests/conftest.py` disables ddtrace agent flushing (`DD_TRACE_ENABLED=false`) before any module
under test imports `ddtrace`, and inserts the repository root onto `sys.path` so the un-packaged
top-level `bench` package is importable by the `test_bench_*` modules without an editable install.

## The `integration` marker

`pyproject.toml` declares exactly one marker: `integration: slow end-to-end tests that run Spark in
local mode`. `.venv/bin/pytest -m "not integration"` is the default fast loop. Drop the `-m` filter
(or use `-m integration`) to run everything, including real local Spark sessions. Six modules carry
it: `test_bench_e2e_tagged.py`, `test_iceberg_optimize.py`, `test_local_e2e.py`,
`test_postgres_queue_load.py`, `test_rebuild_integration.py`, and `test_v2_manifest_paths.py`.

`test_iceberg_optimize.py::test_optimizer_compacts_data_files` needs a Spark JVM gateway configured
with the Iceberg catalog plugin at launch, so it skips itself when an earlier Spark test module in
the same pytest process already started a gateway. Run it in its own process for real coverage:

```bash
.venv/bin/pytest tests/test_iceberg_optimize.py -m integration
```

## The PostgreSQL gate

There is no `LANCE_ETL_TEST_DATABASE_URL` pytest fixture in `conftest.py`. Instead, every module
that needs a real control-plane database declares its own module-level
`POSTGRES_URL_ENV: str = "LANCE_ETL_TEST_DATABASE_URL"` constant and calls `pytest.skip(...)` at the
top of each test (or a shared per-module setup helper) when the variable is unset, for example
`test_state_postgres.py`, `test_local_e2e.py`, `test_postgres_queue_load.py`, and
`test_bench_e2e_tagged.py`. Point it at a disposable local PostgreSQL database to run those tests:

```bash
export LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl_test'
.venv/bin/pytest -m integration
```

Not every Postgres-gated module is also `integration`-marked (`test_state_postgres.py`,
`test_state_specs.py`, `test_state_types.py`, `test_reconciler_retention.py`,
`test_state_rebuild_publish_wedge.py` skip individually without the marker), so setting the
environment variable before a plain `pytest -m "not integration"` run still exercises them.

## `conftest.py` fixtures and fakes

| Name | Purpose |
|---|---|
| `telemetry_config` / `telemetry` | A `TelemetryConfig`/`Telemetry` pair pointed at a local (fire-and-forget UDP) statsd sink, with the Lance event bridge detached, safe to use offline |
| `make_vector_table` | Builds a small `id`/`vector`/`category`/`text` PyArrow table with reproducible random vectors |
| `write_fragmented_dataset` | Writes a table as a Lance dataset split into multiple fragments via `max_rows_per_file` |
| `compact_dataset_inline` | Runs one full `Compaction.execute`/commit/cleanup cycle in-process (no Spark), the harness the concurrency suites race against merges and index builds from a plain thread |
| `group_by_routing` | A non-streaming equivalence oracle for `lance_etl.etl.pivot.stream_routing_groups`, test-only per hard rule 1 — never a production symbol |
| `FakeBroadcast`, `FakeRdd`, `FakeSparkContext`, `FakeSpark` | An in-process stand-in for `SparkContext.parallelize().map()`/`mapPartitions()`/`partitionBy()`/`collect()` and broadcast variables, used throughout the suite (recall, fanout, and fleet-orchestration tests) to drive real driver-side fan-out code without a JVM |

Individual test modules add narrower fakes and fixtures next to the tests that need them
(`test_reconciler_classifier.py` and `test_source_planner.py` each define their own small fixture
classes, for example) rather than growing `conftest.py` further.

## Suite layout by subject

| Area | Representative modules |
|---|---|
| ETL mutation, replay, and merge | `test_mutation_identity.py`, `test_completion_marker.py`, `test_replay_sink.py`, `test_replay_sink_faults.py`, `test_etl_streaming_merge.py`, `test_etl_concurrency.py`, `test_merge_conflict_metric.py`, `test_btree_delta_coexistence.py`, `test_v2_manifest_paths.py`, `test_column_roles.py`, `test_partition_routing.py`, `test_schema_evolution.py` |
| Iceberg source planning | `test_source_planner.py` |
| Indexing (segment API) | `test_centroid_sidecar.py`, `test_cluster_assignment.py`, `test_cluster_rewrite.py`, `test_index_bootstrap_retry.py`, `test_index_maintenance.py`, `test_index_plan_phase.py`, `test_index_replan_guard.py`, `test_index_segment_paths.py`, `test_zonemap_handler.py`, `test_rebuild_integration.py`, `test_size_policy.py` |
| Maintenance (compaction, cleanup, tags, retention) | `test_maintenance.py`, `test_maintenance_fri.py`, `test_maintenance_replan.py`, `test_compaction_deletion_skip.py`, `test_cleanup_rotation.py`, `test_prune_interval_tags.py`, `test_validate_cleanup_horizon.py`, `test_serving_tag.py`, `test_serving_tag_idempotency.py`, `test_fleet_orchestration.py`, `test_fanout.py` |
| Reconciler and PostgreSQL control plane | `test_reconciler.py`, `test_reconciler_classifier.py`, `test_reconciler_prewarm.py`, `test_reconciler_retention.py`, `test_state_postgres.py`, `test_state_specs.py`, `test_state_types.py`, `test_state_rebuild_publish_wedge.py`, `test_postgres_queue_load.py`, `test_local_runtime.py`, `test_local_e2e.py`, `test_release_assets.py` |
| Publication | `test_publication.py` |
| Recall audit | `test_recall.py`, `test_recall_quality.py`, `test_recall_scoring.py`, `test_recall_tiering.py` |
| Operator tools CLI | `test_tools_cli_parsing.py`, `test_migrate_namespace.py`, `test_iceberg_optimize.py` |
| Telemetry | `test_telemetry_bridge.py`, `test_telemetry_capture.py`, `test_telemetry_retries.py` |
| Storage/discovery helpers | `test_dataset_discovery.py` |
| Bench package (`python -m bench`) | `test_bench_capacity.py`, `test_bench_cli.py`, `test_bench_corpus.py`, `test_bench_datasets.py`, `test_bench_e2e_tagged.py`, `test_bench_experiment.py`, `test_bench_fvecs.py`, `test_bench_groundtruth.py`, `test_bench_grpc_shapes.py`, `test_bench_recall_alignment.py`, `test_bench_reconcile.py`, `test_bench_run_experiment.py`, `test_bigann_io.py` |

`test_bench_e2e_tagged.py` and `test_bench_run_experiment.py` are the e2e/bench-tagged tests: they
drive `bench.e2e.run_e2e` (or `bench.experiment`) through the real reconciler over a real PostgreSQL
control plane, are both `integration`-marked (where applicable) and Postgres-gated, and write their
phase artifacts (`e2e.json`, `metrics.json`) under a `tmp_path` workspace rather than the repository.
`test_bench_grpc_shapes.py` and `test_bench_recall_alignment.py` cover the search-leg/report layer
against gRPC stub shapes without standing up the real search-api binary. `bench/README.md` covers
the full benchmark package these tests exercise.
